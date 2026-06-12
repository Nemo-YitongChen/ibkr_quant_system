from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..analysis.report import write_json
from ..common.auto_order_readiness import (
    build_auto_order_readiness_summary,
    evaluate_auto_order_readiness,
    evaluate_auto_order_recovery_eligibility,
    normalize_auto_order_readiness_policy,
)
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.market_readiness import build_market_readiness_payload
from ..common.markets import market_config_path, resolve_market_code
from ..common.runtime_paths import resolve_repo_path

BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Review whether configured portfolios are ready for automated order submission.",
        command="ibkr-quant-auto-order-readiness",
        examples=[
            "ibkr-quant-auto-order-readiness --config config/supervisor.yaml",
            "ibkr-quant-auto-order-readiness --config config/supervisor.yaml --out_dir reports_supervisor",
        ],
        notes=[
            "This command is read-only. It does not submit orders or mutate strategy config.",
        ],
    )
    ap.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    ap.add_argument("--preflight_summary", default="", help="Optional preflight summary JSON override.")
    ap.add_argument("--weekly_summary", default="", help="Optional weekly review summary JSON override.")
    ap.add_argument("--market_readiness", default="", help="Optional market readiness JSON override.")
    ap.add_argument("--watchlist_expansion", default="", help="Optional watchlist expansion summary JSON override.")
    ap.add_argument("--runtime_root", default="runtime_data/paper_investment_only_duq152001", help="Runtime artifact root used to build market readiness if missing.")
    ap.add_argument("--out_dir", default="", help="Output directory. Defaults to supervisor summary_out_dir.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _resolve(raw_path: str) -> Path:
    return resolve_repo_path(BASE_DIR, str(raw_path or ""))


def _overlay_gateway_budget_evidence(
    weekly_summary: Dict[str, Any],
    gateway_budget_payload: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(weekly_summary or {})
    summary = dict((gateway_budget_payload or {}).get("summary") or {})
    rows = [
        dict(row)
        for row in list((gateway_budget_payload or {}).get("rows") or [])
        if isinstance(row, dict)
    ]
    if summary:
        merged["ibkr_gateway_budget"] = summary
    if rows:
        merged["ibkr_gateway_budget_rows"] = rows
    generated_at = str((gateway_budget_payload or {}).get("generated_at") or "").strip()
    if generated_at:
        merged["ibkr_gateway_budget_generated_at"] = generated_at
    return merged


def _report_portfolios(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for market_cfg_raw in list(cfg.get("markets", []) or []):
        market_cfg = dict(market_cfg_raw or {})
        market = resolve_market_code(str(market_cfg.get("market") or market_cfg.get("name") or ""))
        if not market:
            continue
        for item_raw in list(market_cfg.get("reports", []) or []):
            item = dict(item_raw or {})
            if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                continue
            watchlist_yaml = str(item.get("watchlist_yaml") or "").strip()
            ibkr_config = str(item.get("ibkr_config") or market_cfg.get("ibkr_config") or "").strip() or None
            ibkr_path = Path(market_config_path(BASE_DIR, market, ibkr_config)).resolve()
            ibkr_cfg = _load_yaml(ibkr_path)
            portfolio_id = str(item.get("portfolio_id") or f"{market}:{Path(watchlist_yaml).stem}").strip()
            rows.append(
                {
                    "market": market,
                    "watchlist": Path(watchlist_yaml).stem,
                    "portfolio_id": portfolio_id,
                    "account_mode": str(ibkr_cfg.get("mode") or "paper").strip().lower(),
                    "ibkr_config": str(ibkr_path),
                    "run_investment_execution": bool(item.get("run_investment_execution", False)),
                    "submit_investment_execution": bool(item.get("submit_investment_execution", False)),
                    "run_investment_guard": bool(item.get("run_investment_guard", False)),
                    "submit_investment_guard": bool(item.get("submit_investment_guard", False)),
                }
            )
    return rows


def _write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    rows = [dict(row) for row in list(payload.get("rows", []) or []) if isinstance(row, dict)]
    remediation_plan = [
        dict(row)
        for row in list(summary.get("remediation_plan") or [])
        if isinstance(row, dict)
    ]
    lines = [
        "# Auto Order Readiness",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Status: {summary.get('status', '-')}",
        f"- Summary: {summary.get('summary_text', '-')}",
        f"- Submit plan: {dict(summary.get('submit_plan') or {}).get('status', '-')} "
        f"({dict(summary.get('submit_plan') or {}).get('reason', '-')})",
        "",
        "## Remediation Plan",
        "",
        "| priority | severity | reason | affected | markets | remediation |",
        "| ---: | --- | --- | ---: | --- | --- |",
    ]
    for row in remediation_plan:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(int(row.get("priority", 0) or 0)),
                    str(row.get("severity") or "-"),
                    str(row.get("reason") or "-"),
                    str(int(row.get("affected_portfolio_count", 0) or 0)),
                    ",".join(str(value) for value in list(row.get("affected_markets") or [])) or "-",
                    str(row.get("remediation") or "-"),
                ]
            )
            + " |"
        )
    submit_plan = dict(summary.get("submit_plan") or {})
    frequency_plan = dict(summary.get("frequency_plan") or {})
    recovery_plan = dict(summary.get("recovery_plan") or {})
    recovery_eligibility = dict(summary.get("recovery_eligibility") or {})
    frontier_candidates = [
        dict(row)
        for row in list(submit_plan.get("frontier_candidates") or [])
        if isinstance(row, dict)
    ]
    lines.extend(
        [
            "",
            "## Frequency Plan",
            "",
            "| status | reason | primary action | seed proposals | markets | policy |",
            "| --- | --- | --- | ---: | --- | --- |",
            "| "
            + " | ".join(
                [
                    str(frequency_plan.get("status") or "-"),
                    str(frequency_plan.get("reason") or "-"),
                    str(frequency_plan.get("primary_action") or "-"),
                    str(int(frequency_plan.get("seed_proposal_count", 0) or 0)),
                    ",".join(str(value) for value in list(frequency_plan.get("seed_proposal_markets") or [])) or "-",
                    str(frequency_plan.get("submit_gate_policy") or "-"),
                ]
            )
            + " |",
            "",
            "## Minimum-Request Recovery Plan",
            "",
            f"- Status: {recovery_plan.get('status', '-')}",
            f"- Primary action: {recovery_plan.get('primary_action', '-')}",
            f"- Target: {recovery_plan.get('target_market', '-')}/{recovery_plan.get('target_portfolio_id', '-')}",
            f"- Request policy: {recovery_plan.get('request_policy', '-')}",
            f"- Eligibility: {recovery_eligibility.get('eligible', False)} "
            f"({recovery_eligibility.get('reason', '-')})",
            "",
            "| order | phase | action | market | portfolio | Gateway | condition |",
            "| ---: | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for step in list(recovery_plan.get("steps") or []):
        if not isinstance(step, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    str(int(step.get("order", 0) or 0)),
                    str(step.get("phase") or "-"),
                    str(step.get("action") or "-"),
                    str(step.get("market") or "-"),
                    str(step.get("portfolio_id") or "-"),
                    "yes" if bool(step.get("requires_ibkr_gateway", False)) else "no",
                    str(step.get("condition") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Submit Plan",
            "",
        "| status | reason | selected portfolio | orders | planned gross | symbols | mode |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
        "| "
        + " | ".join(
            [
                str(submit_plan.get("status") or "-"),
                str(submit_plan.get("reason") or "-"),
                ",".join(str(value) for value in list(submit_plan.get("selected_portfolio_ids") or []))
                or str(submit_plan.get("selected_portfolio_id") or "-"),
                str(
                    int(
                        submit_plan.get("selected_total_order_count", 0)
                        or submit_plan.get("selected_order_count", 0)
                        or 0
                    )
                ),
                f"{float(submit_plan.get('selected_total_planned_gross_order_value', 0.0) or submit_plan.get('selected_planned_gross_order_value', 0.0) or 0.0):.2f}",
                str(submit_plan.get("selected_planned_order_symbols") or "-"),
                str(submit_plan.get("submit_mode") or "-"),
            ]
            )
            + " |",
        ]
    )
    lines.extend(
        [
            "",
            "## Submit Frontier",
            "",
            "| rank | market | portfolio | status | frontier_reason | quality | net_edge_bps | margin_bps | orders | gross | symbols | hard_blocks | policy_rejects | next_action |",
            "| ---: | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
        ]
    )
    for idx, row in enumerate(frontier_candidates[:10], start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    str(row.get("market") or "-"),
                    str(row.get("portfolio_id") or "-"),
                    str(row.get("status") or "-"),
                    str(row.get("frontier_reason") or "-"),
                    str(row.get("submit_quality_tier") or row.get("submit_quality_status") or "-"),
                    f"{float(row.get('submit_quality_min_net_edge_bps', 0.0) or 0.0):.2f}",
                    f"{float(row.get('submit_quality_min_edge_margin_bps', 0.0) or 0.0):.2f}",
                    str(int(row.get("order_count", 0) or 0)),
                    f"{float(row.get('planned_gross_order_value', 0.0) or 0.0):.2f}",
                    str(row.get("planned_order_symbols") or "-"),
                    ",".join(str(value) for value in list(row.get("hard_blocks") or [])) or "-",
                    ",".join(str(value) for value in list(row.get("policy_reject_reasons") or [])) or "-",
                    str(row.get("next_action") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Portfolio Rows",
            "",
            "| market | portfolio | account | status | reason | market readiness | blocks | warnings |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("market") or "-"),
                    str(row.get("portfolio_id") or "-"),
                    str(row.get("account_mode") or "-"),
                    str(row.get("status") or "-"),
                    str(row.get("primary_reason") or "-"),
                    (
                        f"{row.get('market_readiness_status') or '-'};"
                        f"{row.get('market_readiness_artifact_health_status') or '-'};"
                        f"{row.get('market_readiness_feasibility_status') or '-'}"
                    ),
                    ",".join(str(value) for value in list(row.get("hard_blocks", []) or [])) or "-",
                    ",".join(str(value) for value in list(row.get("warnings", []) or [])) or "-",
                ]
            )
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_auto_order_readiness_payload(
    *,
    config_path: str = "config/supervisor.yaml",
    preflight_summary_path: str = "",
    weekly_summary_path: str = "",
    market_readiness_path: str = "",
    watchlist_expansion_path: str = "",
    runtime_root: str = "runtime_data/paper_investment_only_duq152001",
) -> Dict[str, Any]:
    cfg_path = _resolve(config_path)
    cfg = _load_yaml(cfg_path)
    policy = normalize_auto_order_readiness_policy(dict(cfg.get("auto_order_readiness") or {}))
    preflight_path = (
        _resolve(preflight_summary_path)
        if str(preflight_summary_path or "").strip()
        else _resolve(str(cfg.get("dashboard_preflight_dir", "reports_preflight") or "reports_preflight"))
        / "supervisor_preflight_summary.json"
    )
    weekly_path = (
        _resolve(weekly_summary_path)
        if str(weekly_summary_path or "").strip()
        else _resolve(str(cfg.get("dashboard_weekly_review_dir", "reports_investment_weekly") or "reports_investment_weekly"))
        / "weekly_review_summary.json"
    )
    market_readiness_json_path = (
        _resolve(market_readiness_path)
        if str(market_readiness_path or "").strip()
        else _resolve(str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor"))
        / "market_readiness.json"
    )
    watchlist_expansion_json_path = (
        _resolve(watchlist_expansion_path)
        if str(watchlist_expansion_path or "").strip()
        else _resolve(str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor"))
        / "watchlist_expansion"
        / "watchlist_expansion_summary.json"
    )
    preflight_summary = _load_json(preflight_path)
    weekly_summary = _load_json(weekly_path)
    gateway_budget_path = weekly_path.parent / "weekly_ibkr_gateway_budget_status.json"
    weekly_summary = _overlay_gateway_budget_evidence(
        weekly_summary,
        _load_json(gateway_budget_path),
    )
    market_readiness_summary = _load_json(market_readiness_json_path)
    watchlist_expansion_summary = _load_json(watchlist_expansion_json_path)
    if not market_readiness_summary:
        market_readiness_summary = build_market_readiness_payload(
            base_dir=BASE_DIR,
            supervisor_config=cfg,
            config_path=cfg_path,
            runtime_root=_resolve(runtime_root),
        )
    now = datetime.now(timezone.utc)
    rows = [
        evaluate_auto_order_readiness(
            portfolio,
            preflight_summary=preflight_summary,
            weekly_summary=weekly_summary,
            market_readiness_summary=market_readiness_summary,
            policy=policy,
            now=now,
        )
        for portfolio in _report_portfolios(cfg)
    ]
    summary = build_auto_order_readiness_summary(
        rows,
        policy=policy,
        watchlist_expansion_summary=watchlist_expansion_summary,
    )
    summary["recovery_eligibility"] = evaluate_auto_order_recovery_eligibility(
        summary.get("recovery_plan"),
        now=now,
    )
    return {
        "generated_at": now.isoformat(),
        "schema_version": "2026Q2.auto_order_readiness.v1",
        "config_path": str(cfg_path),
        "preflight_summary_path": str(preflight_path),
        "weekly_summary_path": str(weekly_path),
        "gateway_budget_path": str(gateway_budget_path),
        "market_readiness_path": str(market_readiness_json_path),
        "watchlist_expansion_path": str(watchlist_expansion_json_path),
        "policy": policy,
        "summary": summary,
        "rows": rows,
    }


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = _load_yaml(_resolve(str(args.config)))
    out_dir = _resolve(str(args.out_dir or cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor"))
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_auto_order_readiness_payload(
        config_path=str(args.config),
        preflight_summary_path=str(args.preflight_summary or ""),
        weekly_summary_path=str(args.weekly_summary or ""),
        market_readiness_path=str(args.market_readiness or ""),
        watchlist_expansion_path=str(args.watchlist_expansion or ""),
        runtime_root=str(args.runtime_root or "runtime_data/paper_investment_only_duq152001"),
    )
    json_path = out_dir / "auto_order_readiness.json"
    md_path = out_dir / "auto_order_readiness.md"
    write_json(str(json_path), payload)
    _write_markdown(md_path, payload)
    emit_cli_summary(
        command="ibkr-quant-auto-order-readiness",
        headline="auto order readiness review complete",
        summary=dict(payload.get("summary") or {}),
        artifacts={"summary_json": json_path, "markdown": md_path},
    )


if __name__ == "__main__":
    main()
