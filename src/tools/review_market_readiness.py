from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..analysis.report import write_csv, write_json
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.market_readiness import build_market_readiness_payload
from ..common.runtime_paths import resolve_repo_path

BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Review multi-market quantitative trading readiness from supervisor config and latest artifacts.",
        command="ibkr-quant-market-readiness",
        examples=[
            "ibkr-quant-market-readiness --config config/supervisor.yaml",
            "ibkr-quant-market-readiness --config config/supervisor.yaml --out_dir reports_supervisor",
        ],
        notes=[
            "Read-only. This command does not connect to IBKR and never submits orders.",
            "Use it before enabling paper submit for ASX, HK, XETRA, or other configured markets.",
        ],
    )
    ap.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    ap.add_argument("--runtime_root", default="runtime_data/paper_investment_only_duq152001", help="Runtime artifact root.")
    ap.add_argument("--out_dir", default="", help="Output directory. Defaults to supervisor summary_out_dir.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve(path: str) -> Path:
    return resolve_repo_path(BASE_DIR, str(path or ""))


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    summary = dict(payload.get("summary") or {})
    rows = [dict(row) for row in list(payload.get("rows") or []) if isinstance(row, dict)]
    plan = [dict(row) for row in list(payload.get("preparation_plan") or []) if isinstance(row, dict)]
    lines = [
        "# Market Readiness",
        "",
        f"- Generated at: {payload.get('generated_at', '')}",
        f"- Summary: {summary.get('summary_text', '-')}",
        "",
        "## Preparation Plan",
        "",
        "| rank | market | portfolio | tier | score | reason | next action |",
        "| ---: | --- | --- | --- | ---: | --- | --- |",
    ]
    for row in plan:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(int(row.get("preparation_rank", 0) or 0)),
                    str(row.get("market") or "-"),
                    str(row.get("portfolio_id") or "-"),
                    str(row.get("priority_tier") or "-"),
                    f"{float(row.get('priority_score', 0.0) or 0.0):.2f}",
                    str(row.get("primary_reason") or "-"),
                    str(row.get("next_action") or "-"),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Market Rows",
            "",
            "| market | portfolio | status | reason | artifact | profile | orders | planned gross | broker equity | small account | rules | next action |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in rows:
        rules = (
            f"lot={int(row.get('buy_lot_multiple', 1) or 1)}; "
            f"fee={float(row.get('fee_floor_one_side_bps', 0.0) or 0.0):.2f}bps; "
            f"odd_lot_risk={int(bool(row.get('odd_lot_discount_risk', False)))}"
        )
        small_account = (
            f"{row.get('small_account_feasibility_status', '-')}; "
            f"max_order={float(row.get('effective_max_order_value', 0.0) or 0.0):.2f}; "
            f"min_trade={float(row.get('effective_min_trade_value', 0.0) or 0.0):.2f}; "
            f"investable={float(row.get('effective_investable_equity', 0.0) or 0.0):.2f}"
        )
        artifact = (
            f"{row.get('artifact_health_status', '-')}; "
            f"age={float(row.get('execution_artifact_age_hours', 0.0) or 0.0):.1f}h; "
            f"cap={int(bool(row.get('equity_cap_applied', False)))}"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("market") or "-"),
                    str(row.get("portfolio_id") or "-"),
                    str(row.get("readiness_status") or "-"),
                    str(row.get("primary_reason") or "-"),
                    artifact,
                    str(row.get("account_profile_name") or "-"),
                    str(int(row.get("order_count", 0) or 0)),
                    f"{float(row.get('planned_gross_order_value', 0.0) or 0.0):.2f}",
                    f"{float(row.get('broker_equity', 0.0) or 0.0):.2f}",
                    small_account,
                    rules,
                    str(row.get("next_action") or "-"),
                ]
            )
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_payload(
    *,
    config_path: str = "config/supervisor.yaml",
    runtime_root: str = "runtime_data/paper_investment_only_duq152001",
) -> Dict[str, Any]:
    cfg_path = _resolve(config_path)
    cfg = _load_yaml(cfg_path)
    return build_market_readiness_payload(
        base_dir=BASE_DIR,
        supervisor_config=cfg,
        config_path=cfg_path,
        runtime_root=_resolve(runtime_root),
    )


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = _load_yaml(_resolve(str(args.config)))
    out_dir = _resolve(str(args.out_dir or cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor"))
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_payload(config_path=str(args.config), runtime_root=str(args.runtime_root))
    json_path = out_dir / "market_readiness.json"
    csv_path = out_dir / "market_readiness.csv"
    md_path = out_dir / "market_readiness.md"
    write_json(str(json_path), payload)
    write_csv(str(csv_path), [dict(row) for row in list(payload.get("rows") or []) if isinstance(row, dict)])
    _write_markdown(md_path, payload)
    emit_cli_summary(
        command="ibkr-quant-market-readiness",
        headline="market readiness review complete",
        summary=dict(payload.get("summary") or {}),
        artifacts={"summary_json": json_path, "summary_csv": csv_path, "markdown": md_path},
    )


if __name__ == "__main__":
    main()
