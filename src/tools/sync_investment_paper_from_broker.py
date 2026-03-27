from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from ..analysis.report import write_csv, write_json
from ..common.logger import get_logger
from ..common.markets import add_market_args, resolve_market_code
from ..common.storage import Storage

log = get_logger("tools.sync_investment_paper_from_broker")
BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Seed the local investment paper ledger from the latest broker snapshot.")
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--portfolio_id", default="", help="Stable portfolio id to sync.")
    ap.add_argument("--out_dir", default="reports_investment_sync")
    ap.add_argument("--note", default="broker_sync")
    return ap.parse_args()


def _resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, BASE_DIR / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _write_md(path: Path, summary: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
    lines = [
        "# Investment Paper Sync From Broker",
        "",
        f"- Generated: {summary.get('ts', '')}",
        f"- Market: {summary.get('market', '')}",
        f"- Portfolio: {summary.get('portfolio_id', '')}",
        f"- Account: {summary.get('account_id', '')}",
        f"- Broker run id: {summary.get('source_run_id', '')}",
        f"- Synced position count: {int(summary.get('position_count', 0) or 0)}",
        f"- Broker cash: {float(summary.get('cash_after', 0.0) or 0.0):.2f}",
        f"- Broker equity: {float(summary.get('equity_after', 0.0) or 0.0):.2f}",
        "",
        "## Positions",
    ]
    if not rows:
        lines.append("- (no positions)")
    else:
        for row in rows:
            lines.append(
                f"- {row['symbol']} qty={float(row.get('qty', 0.0) or 0.0):.0f} "
                f"last={float(row.get('last_price', 0.0) or 0.0):.2f} "
                f"mv={float(row.get('market_value', 0.0) or 0.0):.2f} "
                f"weight={float(row.get('weight', 0.0) or 0.0):.3f}"
            )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
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
        broker_run = conn.execute(
            "SELECT * FROM investment_execution_runs WHERE market=? AND portfolio_id=? ORDER BY ts DESC, id DESC LIMIT 1",
            (market, portfolio_id),
        ).fetchone()
        if broker_run is None:
            raise SystemExit(f"No investment_execution_runs found for {portfolio_id}")
        broker_rows = [
            dict(row)
            for row in conn.execute(
                "SELECT symbol, qty, avg_cost, market_price, market_value, weight FROM investment_broker_positions "
                "WHERE run_id=? AND source='after' ORDER BY symbol ASC",
                (str(broker_run["run_id"]),),
            ).fetchall()
        ]
        latest_local_run = conn.execute(
            "SELECT * FROM investment_runs WHERE market=? AND portfolio_id=? ORDER BY ts DESC, id DESC LIMIT 1",
            (market, portfolio_id),
        ).fetchone()
    finally:
        conn.close()

    storage = Storage(str(db_path))
    now = datetime.now(timezone.utc)
    run_id = f"{market}-broker-sync-{now.strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    report_dir = str(broker_run["report_dir"] or (latest_local_run["report_dir"] if latest_local_run else ""))
    cash_after = _to_float(broker_run["broker_cash"])
    equity_after = _to_float(broker_run["broker_equity"])
    details = {
        "source": "broker_sync",
        "source_run_id": str(broker_run["run_id"]),
        "account_id": str(broker_run["account_id"] or ""),
        "note": str(args.note or "broker_sync"),
        "report_dir": report_dir,
    }
    storage.insert_investment_run(
        {
            "run_id": run_id,
            "market": market,
            "portfolio_id": portfolio_id,
            "report_dir": report_dir,
            "rebalance_due": 0,
            "executed": 0,
            "cash_before": float(cash_after),
            "cash_after": float(cash_after),
            "equity_before": float(equity_after),
            "equity_after": float(equity_after),
            "details": json.dumps(details, ensure_ascii=False),
        }
    )

    position_rows: List[Dict[str, Any]] = []
    for row in broker_rows:
        qty = _to_float(row.get("qty"))
        if qty <= 0:
            continue
        position_row = {
            "symbol": str(row.get("symbol") or "").upper(),
            "qty": qty,
            "cost_basis": _to_float(row.get("avg_cost")),
            "last_price": _to_float(row.get("market_price")),
            "market_value": _to_float(row.get("market_value")),
            "weight": _to_float(row.get("weight")),
            "status": "OPEN",
        }
        position_rows.append(position_row)
        storage.insert_investment_position(
            {
                "run_id": run_id,
                "market": market,
                "portfolio_id": portfolio_id,
                **position_row,
                "details": json.dumps(details, ensure_ascii=False),
            }
        )

    summary = {
        "ts": now.isoformat(),
        "market": market,
        "portfolio_id": portfolio_id,
        "account_id": str(broker_run["account_id"] or ""),
        "source_run_id": str(broker_run["run_id"]),
        "position_count": int(len(position_rows)),
        "cash_after": float(cash_after),
        "equity_after": float(equity_after),
        "run_id": run_id,
    }

    write_csv(str(out_dir / "broker_sync_positions.csv"), position_rows)
    write_json(str(out_dir / "broker_sync_summary.json"), summary)
    _write_md(out_dir / "broker_sync_report.md", summary, position_rows)
    log.info(
        "Synced investment paper from broker -> %s positions=%s account=%s",
        out_dir / "broker_sync_report.md",
        len(position_rows),
        summary["account_id"],
    )


if __name__ == "__main__":
    main()
