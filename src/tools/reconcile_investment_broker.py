from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..analysis.report import write_csv, write_json
from ..common.adaptive_strategy import (
    adaptive_strategy_effective_controls_human_note,
    adaptive_strategy_summary_fields,
    load_report_adaptive_strategy_payload,
)
from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.cli_contracts import ArtifactBundle, ReconciliationSummary
from ..common.logger import get_logger
from ..common.markets import add_market_args, resolve_market_code
from ..common.runtime_paths import resolve_repo_path

log = get_logger("tools.reconcile_investment_broker")
BASE_DIR = Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Reconcile local investment paper ledger against latest broker execution snapshot.",
        command="ibkr-quant-reconcile",
        examples=[
            "ibkr-quant-reconcile --market HK --portfolio_id HK:watchlist",
            "ibkr-quant-reconcile --market US --portfolio_id US:market_us --out_dir reports_investment_reconcile_us",
        ],
        notes=[
            "Writes broker reconciliation CSV, summary JSON, and broker_reconciliation.md under --out_dir.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for local and broker snapshots.")
    ap.add_argument("--portfolio_id", default="", help="Stable portfolio id to reconcile.")
    ap.add_argument("--out_dir", default="reports_investment_reconcile", help="Directory for reconciliation artifacts.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _rows_to_symbol_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if symbol:
            out[symbol] = dict(row)
    return out


def _load_report_json(report_dir: str, name: str) -> Dict[str, Any]:
    if not str(report_dir or "").strip():
        return {}
    path = Path(report_dir) / name
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}


def _strategy_effective_controls_note(*summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        note = str(
            payload.get("strategy_effective_controls_human_note")
            or payload.get("strategy_effective_controls_note")
            or ""
        ).strip()
        if note:
            return note
        controls = dict(payload.get("strategy_effective_controls") or {})
        note = adaptive_strategy_effective_controls_human_note(controls)
        if note:
            return note
    return ""


def _execution_gate_summary(execution_summary: Dict[str, Any]) -> str:
    execution_summary = dict(execution_summary or {})
    blocked_total = int(execution_summary.get("blocked_order_count", 0) or 0)
    if blocked_total <= 0:
        return ""
    labels = [
        ("流动性", int(execution_summary.get("blocked_liquidity_order_count", 0) or 0)),
        ("风险告警", int(execution_summary.get("blocked_risk_alert_order_count", 0) or 0)),
        ("人工复核", int(execution_summary.get("blocked_manual_review_order_count", 0) or 0)),
        ("机会过滤", int(execution_summary.get("blocked_opportunity_order_count", 0) or 0)),
        ("质量过滤", int(execution_summary.get("blocked_quality_order_count", 0) or 0)),
        ("热点惩罚", int(execution_summary.get("blocked_hotspot_penalty_order_count", 0) or 0)),
    ]
    detail = "，".join(f"{label} {count}" for label, count in labels if count > 0)
    if detail:
        return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发（{detail}）。"
    return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发。"


def build_reconciliation_rows(local_rows: List[Dict[str, Any]], broker_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    local_map = _rows_to_symbol_map(local_rows)
    broker_map = _rows_to_symbol_map(broker_rows)
    out: List[Dict[str, Any]] = []
    for symbol in sorted(set(local_map) | set(broker_map)):
        local = local_map.get(symbol, {})
        broker = broker_map.get(symbol, {})
        local_qty = float(local.get("qty") or 0.0)
        broker_qty = float(broker.get("qty") or 0.0)
        if abs(local_qty - broker_qty) < 1e-9:
            status = "MATCH"
        elif local_qty <= 0 and broker_qty > 0:
            status = "ONLY_BROKER"
        elif local_qty > 0 and broker_qty <= 0:
            status = "ONLY_LOCAL"
        else:
            status = "QTY_MISMATCH"
        out.append(
            {
                "symbol": symbol,
                "status": status,
                "local_qty": local_qty,
                "broker_qty": broker_qty,
                "qty_diff": broker_qty - local_qty,
                "local_weight": float(local.get("weight") or 0.0),
                "broker_weight": float(broker.get("weight") or 0.0),
                "local_value": float(local.get("market_value") or 0.0),
                "broker_value": float(broker.get("market_value") or 0.0),
            }
        )
    return out


def _write_md(path: Path, summary: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    strategy_name = str(summary.get("adaptive_strategy_display_name") or summary.get("adaptive_strategy_name") or "").strip()
    strategy_summary = str(summary.get("adaptive_strategy_summary") or "").strip()
    strategy_runtime_note = str(summary.get("adaptive_strategy_runtime_note") or "").strip()
    strategy_market_note = str(summary.get("adaptive_strategy_active_market_note") or "").strip()
    strategy_controls_note = str(summary.get("strategy_effective_controls_note") or "").strip()
    execution_gate_summary = str(summary.get("execution_gate_summary") or "").strip()
    lines = [
        "# Investment Broker Reconciliation",
        "",
        f"- Generated: {summary.get('ts', '')}",
        f"- Market: {summary.get('market', '')}",
        f"- Portfolio: {summary.get('portfolio_id', '')}",
        f"- Account: {summary.get('account_id', '')}",
        f"- Local run id: {summary.get('local_run_id', '')}",
        f"- Broker run id: {summary.get('broker_run_id', '')}",
        f"- Match rows: {int(summary.get('match_rows', 0) or 0)}",
        f"- Only local rows: {int(summary.get('only_local_rows', 0) or 0)}",
        f"- Only broker rows: {int(summary.get('only_broker_rows', 0) or 0)}",
        f"- Qty mismatch rows: {int(summary.get('qty_mismatch_rows', 0) or 0)}",
        "",
    ]
    if strategy_name or strategy_summary or strategy_runtime_note:
        lines.extend(
            [
                "## Strategy",
                f"- Framework: {strategy_name or '-'}",
            ]
        )
        if strategy_summary:
            lines.append(f"- Summary: {strategy_summary}")
        if strategy_market_note:
            lines.append(f"- Market profile: {strategy_market_note}")
        if strategy_runtime_note:
            lines.append(f"- Runtime: {strategy_runtime_note}")
        if strategy_controls_note:
            lines.append(f"- Effective controls: {strategy_controls_note}")
        if execution_gate_summary:
            lines.append(f"- Execution gates: {execution_gate_summary}")
        lines.append("")
    lines.append("## Reconciliation")
    if not rows:
        lines.append("- (no rows)")
    else:
        for row in rows:
            lines.append(
                f"- {row['symbol']} status={row['status']} "
                f"local_qty={float(row.get('local_qty', 0.0) or 0.0):.0f} "
                f"broker_qty={float(row.get('broker_qty', 0.0) or 0.0):.0f} "
                f"qty_diff={float(row.get('qty_diff', 0.0) or 0.0):.0f}"
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def _cli_summary_payload(summary: Dict[str, Any], out_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    summary_contract = ReconciliationSummary(
        market=str(summary.get("market") or "DEFAULT"),
        portfolio_id=str(summary.get("portfolio_id") or "-"),
        match_rows=int(summary.get("match_rows") or 0),
        only_local_rows=int(summary.get("only_local_rows") or 0),
        only_broker_rows=int(summary.get("only_broker_rows") or 0),
        qty_mismatch_rows=int(summary.get("qty_mismatch_rows") or 0),
    )
    artifacts = ArtifactBundle(
        rows_csv=out_dir / "broker_reconciliation.csv",
        summary_json=out_dir / "broker_reconciliation_summary.json",
        report_md=out_dir / "broker_reconciliation.md",
    )
    return summary_contract.to_dict(), artifacts.to_dict()


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    market = resolve_market_code(getattr(args, "market", ""))
    if not market:
        raise SystemExit("--market is required")
    portfolio_id = str(args.portfolio_id or "").strip()
    if not portfolio_id:
        raise SystemExit("--portfolio_id is required")

    db_path = _resolve_project_path(args.db)
    out_dir = _resolve_project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        local_run = conn.execute(
            "SELECT * FROM investment_runs WHERE market=? AND portfolio_id=? ORDER BY ts DESC, id DESC LIMIT 1",
            (market, portfolio_id),
        ).fetchone()
        broker_run = conn.execute(
            "SELECT * FROM investment_execution_runs WHERE market=? AND portfolio_id=? ORDER BY ts DESC, id DESC LIMIT 1",
            (market, portfolio_id),
        ).fetchone()
        local_rows: List[Dict[str, Any]] = []
        broker_rows: List[Dict[str, Any]] = []
        if local_run is not None:
            local_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT symbol, qty, market_value, weight FROM investment_positions WHERE run_id=? ORDER BY symbol ASC",
                    (str(local_run["run_id"]),),
                ).fetchall()
            ]
        if broker_run is not None:
            broker_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT symbol, qty, market_value, weight FROM investment_broker_positions "
                    "WHERE run_id=? AND source='after' ORDER BY symbol ASC",
                    (str(broker_run["run_id"]),),
                ).fetchall()
            ]
    finally:
        conn.close()

    rows = build_reconciliation_rows(local_rows, broker_rows)
    report_dir = ""
    if local_run is not None:
        report_dir = str(local_run["report_dir"] or "").strip()
    if not report_dir and broker_run is not None:
        report_dir = str(broker_run["report_dir"] or "").strip()
    strategy_fields = adaptive_strategy_summary_fields(
        load_report_adaptive_strategy_payload(Path(report_dir)) if report_dir else {}
    )
    paper_summary = _load_report_json(report_dir, "investment_paper_summary.json")
    execution_summary = _load_report_json(report_dir, "investment_execution_summary.json")
    strategy_controls_note = _strategy_effective_controls_note(execution_summary, paper_summary)
    execution_gate_summary = _execution_gate_summary(execution_summary)
    summary = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market": market,
        "portfolio_id": portfolio_id,
        "account_id": str(broker_run["account_id"]) if broker_run is not None else "",
        "local_run_id": str(local_run["run_id"]) if local_run is not None else "",
        "broker_run_id": str(broker_run["run_id"]) if broker_run is not None else "",
        "report_dir": report_dir,
        "match_rows": int(sum(1 for row in rows if row["status"] == "MATCH")),
        "only_local_rows": int(sum(1 for row in rows if row["status"] == "ONLY_LOCAL")),
        "only_broker_rows": int(sum(1 for row in rows if row["status"] == "ONLY_BROKER")),
        "qty_mismatch_rows": int(sum(1 for row in rows if row["status"] == "QTY_MISMATCH")),
        "strategy_effective_controls_applied": bool(
            execution_summary.get("strategy_effective_controls_applied")
            or paper_summary.get("strategy_effective_controls_applied")
        ),
        "strategy_effective_controls_note": strategy_controls_note,
        "execution_gate_summary": execution_gate_summary,
        "execution_blocked_order_count": int(execution_summary.get("blocked_order_count", 0) or 0),
        "execution_blocked_liquidity_order_count": int(execution_summary.get("blocked_liquidity_order_count", 0) or 0),
        "execution_blocked_risk_alert_order_count": int(execution_summary.get("blocked_risk_alert_order_count", 0) or 0),
        "execution_blocked_manual_review_order_count": int(execution_summary.get("blocked_manual_review_order_count", 0) or 0),
        "execution_blocked_opportunity_order_count": int(execution_summary.get("blocked_opportunity_order_count", 0) or 0),
        "execution_gap_symbols": int(execution_summary.get("gap_symbols", 0) or 0),
        "execution_gap_notional": float(execution_summary.get("gap_notional", 0.0) or 0.0),
    }
    summary.update(strategy_fields)

    write_csv(str(out_dir / "broker_reconciliation.csv"), rows)
    write_json(str(out_dir / "broker_reconciliation_summary.json"), summary)
    _write_md(out_dir / "broker_reconciliation.md", summary, rows)
    summary_fields, artifact_fields = _cli_summary_payload(summary, out_dir)
    emit_cli_summary(
        command="ibkr-quant-reconcile",
        headline="broker reconciliation complete",
        summary=summary_fields,
        artifacts=artifact_fields,
    )
    log.info(
        "Wrote broker reconciliation -> %s rows=%s local_only=%s broker_only=%s mismatches=%s",
        out_dir / "broker_reconciliation.md",
        len(rows),
        summary["only_local_rows"],
        summary["only_broker_rows"],
        summary["qty_mismatch_rows"],
    )


if __name__ == "__main__":
    main()
