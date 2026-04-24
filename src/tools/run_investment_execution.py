from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from ..analysis.investment_portfolio import InvestmentPaperConfig
from ..app.investment_engine import InvestmentExecutionEngine
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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required")

    ibkr_cfg_path = str(market_config_path(BASE_DIR, market, args.ibkr_config or None))
    ibkr_cfg = _load_yaml(ibkr_cfg_path)
    if str(ibkr_cfg.get("mode", "paper")).strip().lower() != "paper" and not bool(args.submit):
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
    ib = connect_ib(
        str(ibkr_cfg["host"]),
        int(ibkr_cfg["port"]),
        int(ibkr_cfg["client_id"]),
        request_timeout=float(args.request_timeout_sec),
    )
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
