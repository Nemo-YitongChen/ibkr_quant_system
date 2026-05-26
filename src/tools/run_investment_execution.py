from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from ..analysis.investment_portfolio import InvestmentPaperConfig
from ..analysis.report import write_csv, write_json
from ..app.investment_engine import InvestmentExecutionEngine, InvestmentExecutionResult
from ..common.account_profile import load_account_profiles
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.cli_contracts import ArtifactBundle, InvestmentExecutionSummary
from ..common.config_layers import load_layered_config
from ..common.logger import get_logger
from ..common.market_structure import load_market_structure
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..offhours.ib_setup import connect_ib
from ..portfolio.investment_allocator import InvestmentExecutionConfig

log = get_logger("tools.run_investment_execution")
BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Execute investment rebalance orders via IBKR paper/live account.",
        command="ibkr-quant-execution",
        examples=[
            "ibkr-quant-execution --market HK --submit",
            "ibkr-quant-execution --market US --report_dir reports_investment_us/market_us",
        ],
        notes=[
            "Writes investment execution summary JSON, plan CSV, and investment_execution_report.md in the report directory.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="", help="Path to the IBKR runtime config yaml.")
    ap.add_argument("--execution_config", default="", help="Path to the investment execution config yaml.")
    ap.add_argument("--paper_config", default="", help="Path to investment paper config yaml.")
    ap.add_argument("--market_structure_config", default="", help="Path to market structure constraint yaml.")
    ap.add_argument("--account_profile_config", default="", help="Path to account profile yaml.")
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for execution snapshots.")
    ap.add_argument("--report_dir", default="", help="Explicit report directory that contains investment_candidates.csv.")
    ap.add_argument("--reports_root", default="reports_investment", help="Root directory used by investment reports.")
    ap.add_argument("--watchlist_yaml", default="", help="Use the same watchlist stem as the report generator.")
    ap.add_argument("--portfolio_id", default="", help="Stable identifier for one investment execution portfolio.")
    ap.add_argument("--submit", action="store_true", default=False, help="Actually submit paper/live orders instead of only planning.")
    ap.add_argument("--request_timeout_sec", type=float, default=10.0, help="IBKR request timeout in seconds.")
    return ap


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _load_yaml(path_str: str) -> Dict[str, Any]:
    import yaml

    with _resolve_project_path(path_str).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_paper_config_payload(path_str: str) -> Dict[str, Any]:
    return load_layered_config(
        BASE_DIR,
        path_str,
        default_paths=("config/investment_paper.yaml",),
    ).payload


def _load_execution_config_payload(path_str: str) -> Dict[str, Any]:
    return load_layered_config(
        BASE_DIR,
        path_str,
        default_paths=("config/investment_execution.yaml",),
    ).payload


def _slugify_report_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


def _infer_report_dir(args: argparse.Namespace, market: str) -> Path:
    if args.report_dir:
        return _resolve_project_path(args.report_dir)
    root = _resolve_project_path(args.reports_root)
    if args.watchlist_yaml:
        return root / _slugify_report_name(Path(str(args.watchlist_yaml)).stem)
    return root / f"market_{str(market or 'default').lower()}"


def _cli_summary_payload(result: Any, report_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    summary_contract = InvestmentExecutionSummary(
        market=str(getattr(result, "market", "") or "DEFAULT"),
        portfolio_id=str(getattr(result, "portfolio_id", "") or "-"),
        submitted=bool(getattr(result, "submitted", False)),
        account_profile=str(getattr(result, "account_profile_label", "") or "-"),
        order_count=int(getattr(result, "order_count", 0) or 0),
        gap_symbols=int(getattr(result, "gap_symbols", 0) or 0),
        gap_notional=f"{float(getattr(result, 'gap_notional', 0.0) or 0.0):.2f}",
    )
    artifacts = ArtifactBundle(
        summary_json=report_dir / "investment_execution_summary.json",
        plan_csv=report_dir / "investment_execution_plan.csv",
        report_md=report_dir / "investment_execution_report.md",
    )
    return summary_contract.to_dict(), artifacts.to_dict()


def _load_json_dict(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _broker_context_from_existing_artifacts(report_dir: Path) -> Dict[str, float]:
    for filename in ("investment_broker_snapshot_summary.json", "investment_execution_summary.json"):
        payload = _load_json_dict(report_dir / filename)
        if not payload:
            continue
        return {
            "broker_equity": float(payload.get("broker_equity", 0.0) or 0.0),
            "broker_cash": float(payload.get("broker_cash", 0.0) or 0.0),
        }
    return {"broker_equity": 0.0, "broker_cash": 0.0}


def _write_gateway_unavailable_artifacts(
    *,
    report_dir: Path,
    market: str,
    portfolio_id: str,
    account_id: str,
    submit_requested: bool,
    error: Exception,
) -> InvestmentExecutionResult:
    report_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    run_id = f"{str(market or 'DEFAULT').upper()}-exec-gateway-unavailable-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    error_text = f"{type(error).__name__}: {error}"
    broker_context = _broker_context_from_existing_artifacts(report_dir)
    primary_reason = "IBKR_GATEWAY_UNAVAILABLE"
    primary_action = "start_or_unlock_ib_gateway_paper_api"
    submit_guard_status = "BLOCKED_IBKR_GATEWAY_UNAVAILABLE"
    submit_guard_reason = (
        "IBKR connection failed before execution planning; no broker state was refreshed and no order was submitted. "
        f"error={error_text}"
    )
    diagnostic_rows = [
        {
            "section": "ibkr_connection",
            "check": "gateway_api_connection",
            "value": "FAILED",
            "threshold": "CONNECTED",
            "status": "BLOCKED",
            "note": error_text,
        },
        {
            "section": "paper_submit_readiness",
            "check": "paper_submit_state",
            "value": "BLOCKED",
            "threshold": "READY",
            "status": "BLOCKED",
            "note": "paper submit is blocked until the IBKR paper gateway API connection is available",
        },
    ]
    owner_rows = [
        {
            "step": "P0",
            "name": "ibkr_gateway_connection",
            "status": "BLOCKED",
            "evidence": error_text,
            "next_action": primary_action,
        },
        {
            "step": "P1",
            "name": "small_account_capital_profile",
            "status": "UNKNOWN",
            "evidence": "broker state was not refreshed",
            "next_action": "refresh broker snapshot after gateway reconnects",
        },
        {
            "step": "P2",
            "name": "paper_order_activation",
            "status": "BLOCKED",
            "evidence": "IBKR connection unavailable",
            "next_action": primary_action,
        },
        {
            "step": "P3",
            "name": "post_cost_edge_gate",
            "status": "INSUFFICIENT_SAMPLE",
            "evidence": "no new execution sample",
            "next_action": "rerun no-submit execution after gateway reconnects",
        },
        {
            "step": "P4",
            "name": "micro_live_acceptance",
            "status": "BLOCKED",
            "evidence": "paper evidence collection is blocked",
            "next_action": "do not enable live submit",
        },
        {
            "step": "P5",
            "name": "live_safety_controls",
            "status": "PASS",
            "evidence": "no live order path was invoked",
            "next_action": "keep live disabled",
        },
        {
            "step": "P6",
            "name": "investment_state_assessment",
            "status": "PAPER_BLOCKED",
            "evidence": primary_reason,
            "next_action": primary_action,
        },
    ]
    owner_progression = {
        "generated_at": ts,
        "overall_status": "PAPER_BLOCKED",
        "primary_no_order_reason": primary_reason,
        "rows": owner_rows,
        "open_blocker_count": 3,
    }
    no_order_diagnostics = {
        "generated_at": ts,
        "run_id": run_id,
        "market": str(market or "").upper(),
        "portfolio_id": str(portfolio_id or ""),
        "report_dir": str(report_dir),
        "submitted": False,
        "submit_requested": bool(submit_requested),
        "submit_effective": False,
        "submit_guard_status": submit_guard_status,
        "submit_guard_reason": submit_guard_reason,
        "broker_equity": float(broker_context["broker_equity"]),
        "broker_cash": float(broker_context["broker_cash"]),
        "target_equity": 0.0,
        "order_count": 0,
        "blocked_order_count": 0,
        "submit_blocking_order_count": 0,
        "paper_submit_ready": False,
        "paper_submit_readiness_status": "BLOCKED",
        "primary_no_order_reason": primary_reason,
        "primary_action": primary_action,
        "diagnostic_rows": diagnostic_rows,
        "paper_submit_readiness_rows": [row for row in diagnostic_rows if row["section"] == "paper_submit_readiness"],
        "progression_assessment": owner_progression,
    }
    summary = {
        "ts": ts,
        "generated_at": ts,
        "run_id": run_id,
        "market": str(market or "").upper(),
        "portfolio_id": str(portfolio_id or ""),
        "account_id": str(account_id or ""),
        "report_dir": str(report_dir),
        "submitted": False,
        "submit_requested": bool(submit_requested),
        "submit_effective": False,
        "submit_guard_status": submit_guard_status,
        "submit_guard_reason": submit_guard_reason,
        "ibkr_connection_status": "FAILED",
        "ibkr_connection_error": error_text,
        "broker_equity": float(broker_context["broker_equity"]),
        "broker_cash": float(broker_context["broker_cash"]),
        "target_equity": 0.0,
        "order_count": 0,
        "submitted_order_count": 0,
        "filled_order_count": 0,
        "blocked_order_count": 0,
        "submit_blocking_order_count": 0,
        "primary_no_order_reason": primary_reason,
        "no_order_primary_action": primary_action,
        "owner_progression_status": "PAPER_BLOCKED",
        "paper_submit_ready": False,
        "paper_submit_readiness_status": "BLOCKED",
        "gap_symbols": 0,
        "gap_notional": 0.0,
    }
    write_csv(str(report_dir / "investment_execution_plan.csv"), [])
    write_csv(str(report_dir / "investment_no_order_diagnostics.csv"), diagnostic_rows)
    write_json(str(report_dir / "investment_no_order_diagnostics.json"), no_order_diagnostics)
    write_csv(str(report_dir / "investment_owner_progression_assessment.csv"), owner_rows)
    write_json(str(report_dir / "investment_owner_progression_assessment.json"), owner_progression)
    write_json(str(report_dir / "investment_execution_summary.json"), summary)
    (report_dir / "investment_execution_report.md").write_text(
        "\n".join(
            [
                "# Investment Execution Report",
                "",
                f"- Generated: {ts}",
                f"- Market: {str(market or '').upper()}",
                f"- Portfolio: {portfolio_id}",
                f"- Status: {primary_reason}",
                f"- Submit requested: {int(bool(submit_requested))}",
                f"- Submit effective: 0",
                f"- Next action: {primary_action}",
                f"- Error: {error_text}",
                "",
                "No order was planned or submitted because the IBKR paper gateway API connection failed before broker state refresh.",
            ]
        ),
        encoding="utf-8",
    )
    return InvestmentExecutionResult(
        run_id=run_id,
        portfolio_id=str(portfolio_id or ""),
        market=str(market or "").upper(),
        report_dir=str(report_dir),
        submitted=False,
        broker_equity=float(broker_context["broker_equity"]),
        broker_cash=float(broker_context["broker_cash"]),
        target_equity=0.0,
        order_count=0,
        order_value=0.0,
        gap_symbols=0,
        gap_notional=0.0,
        account_profile_name="",
        account_profile_label="gateway_unavailable",
    )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required")

    ibkr_cfg_path = str(market_config_path(BASE_DIR, market, args.ibkr_config or None))
    ibkr_cfg = _load_yaml(ibkr_cfg_path)
    ibkr_mode = str(ibkr_cfg.get("mode", "paper")).strip().lower()
    if ibkr_mode != "paper" and not bool(args.submit):
        log.warning("IBKR config mode=%s; running in plan-only mode", ibkr_cfg.get("mode"))

    paper_cfg_path = str(
        _resolve_project_path(
            args.paper_config or str(ibkr_cfg.get("investment_paper_config", f"config/investment_paper_{market.lower()}.yaml"))
        )
    )
    execution_cfg_path = str(
        _resolve_project_path(
            args.execution_config or str(ibkr_cfg.get("investment_execution_config", f"config/investment_execution_{market.lower()}.yaml"))
        )
    )
    paper_cfg = InvestmentPaperConfig.from_dict(_load_paper_config_payload(paper_cfg_path).get("paper"))
    execution_cfg = InvestmentExecutionConfig.from_dict(_load_execution_config_payload(execution_cfg_path).get("execution"))
    if ibkr_mode != "paper" and bool(getattr(execution_cfg, "near_entry_paper_test_enabled", False)):
        log.warning("Disabling near-entry paper test because IBKR mode=%s is not paper.", ibkr_cfg.get("mode"))
        execution_cfg = replace(execution_cfg, near_entry_paper_test_enabled=False)
    if ibkr_mode != "paper" and bool(getattr(execution_cfg, "whole_share_missing_opportunity_paper_sample_enabled", False)):
        log.warning("Disabling missing-opportunity whole-share paper sample because IBKR mode=%s is not paper.", ibkr_cfg.get("mode"))
        execution_cfg = replace(execution_cfg, whole_share_missing_opportunity_paper_sample_enabled=False)
    if ibkr_mode != "paper" and bool(getattr(execution_cfg, "shadow_ml_allow_whole_share_paper_sample", False)):
        log.warning("Disabling whole-share shadow ML paper sample because IBKR mode=%s is not paper.", ibkr_cfg.get("mode"))
        execution_cfg = replace(execution_cfg, shadow_ml_allow_whole_share_paper_sample=False)
    market_structure = load_market_structure(
        BASE_DIR,
        market,
        str(
            args.market_structure_config
            or ibkr_cfg.get("market_structure_config", f"config/market_structure_{market.lower()}.yaml")
        ),
    )
    account_profiles = load_account_profiles(
        BASE_DIR,
        str(args.account_profile_config or ibkr_cfg.get("account_profile_config", "config/account_profiles.yaml")),
    )
    if not str(execution_cfg.lot_size_file or "").strip():
        execution_cfg.lot_size = max(int(execution_cfg.lot_size or 1), int(market_structure.order_rules.buy_lot_multiple or 1))

    report_dir = _infer_report_dir(args, market)
    portfolio_id = str(args.portfolio_id or f"{market}:{report_dir.name}")
    storage = Storage(str(_resolve_project_path(args.db)))
    try:
        ib = connect_ib(
            str(ibkr_cfg["host"]),
            int(ibkr_cfg["port"]),
            int(ibkr_cfg["client_id"]),
            request_timeout=float(args.request_timeout_sec),
        )
    except Exception as exc:
        log.warning("IBKR gateway unavailable; writing degraded execution artifacts: %s", exc)
        result = _write_gateway_unavailable_artifacts(
            report_dir=report_dir,
            market=market,
            portfolio_id=portfolio_id,
            account_id=str(ibkr_cfg.get("account_id", "")),
            submit_requested=bool(args.submit),
            error=exc,
        )
        summary_fields, artifact_fields = _cli_summary_payload(result, report_dir)
        emit_cli_summary(
            command="ibkr-quant-execution",
            headline="investment execution run degraded: ibkr gateway unavailable",
            summary=summary_fields,
            artifacts=artifact_fields,
        )
        return
    try:
        engine = InvestmentExecutionEngine(
            ib=ib,
            account_id=str(ibkr_cfg["account_id"]),
            storage=storage,
            market=market,
            portfolio_id=portfolio_id,
            paper_cfg=paper_cfg,
            execution_cfg=execution_cfg,
            market_structure=market_structure,
            account_profiles=account_profiles,
        )
        try:
            result = engine.run(report_dir=str(report_dir), submit=bool(args.submit))
        except ValueError as e:
            raise SystemExit(str(e))
        summary_fields, artifact_fields = _cli_summary_payload(result, report_dir)
        emit_cli_summary(
            command="ibkr-quant-execution",
            headline="investment execution run complete",
            summary=summary_fields,
            artifacts=artifact_fields,
        )
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
