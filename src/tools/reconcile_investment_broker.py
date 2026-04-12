from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..analysis.report import write_csv, write_json
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
        "## Reconciliation",
    ]
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
    summary = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "market": market,
        "portfolio_id": portfolio_id,
        "account_id": str(broker_run["account_id"]) if broker_run is not None else "",
        "local_run_id": str(local_run["run_id"]) if local_run is not None else "",
        "broker_run_id": str(broker_run["run_id"]) if broker_run is not None else "",
        "match_rows": int(sum(1 for row in rows if row["status"] == "MATCH")),
        "only_local_rows": int(sum(1 for row in rows if row["status"] == "ONLY_LOCAL")),
        "only_broker_rows": int(sum(1 for row in rows if row["status"] == "ONLY_BROKER")),
        "qty_mismatch_rows": int(sum(1 for row in rows if row["status"] == "QTY_MISMATCH")),
    }

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
