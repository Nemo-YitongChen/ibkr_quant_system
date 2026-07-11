from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


EVIDENCE_INDEX_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "idx_fills_weekly_lookup",
        "table": "fills",
        "columns": ["ts", "portfolio_id", "system_kind", "execution_run_id"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_fills_weekly_lookup "
            "ON fills (ts DESC, portfolio_id, system_kind, execution_run_id)"
        ),
    },
    {
        "name": "idx_risk_events_weekly_lookup",
        "table": "risk_events",
        "columns": ["ts", "kind", "portfolio_id", "system_kind", "execution_run_id"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_risk_events_weekly_lookup "
            "ON risk_events (ts DESC, kind, portfolio_id, system_kind, execution_run_id)"
        ),
    },
    {
        "name": "idx_investment_positions_weekly_lookup",
        "table": "investment_positions",
        "columns": ["ts", "market", "portfolio_id"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_investment_positions_weekly_lookup "
            "ON investment_positions (ts DESC, market, portfolio_id)"
        ),
    },
    {
        "name": "idx_investment_trades_weekly_lookup",
        "table": "investment_trades",
        "columns": ["ts", "market", "portfolio_id"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_investment_trades_weekly_lookup "
            "ON investment_trades (ts DESC, market, portfolio_id)"
        ),
    },
    {
        "name": "idx_investment_candidate_snapshots_weekly_lookup",
        "table": "investment_candidate_snapshots",
        "columns": ["ts", "market", "portfolio_id", "stage", "symbol"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_investment_candidate_snapshots_weekly_lookup "
            "ON investment_candidate_snapshots (ts DESC, market, portfolio_id, stage, symbol)"
        ),
    },
    {
        "name": "idx_investment_candidate_outcomes_weekly_lookup",
        "table": "investment_candidate_outcomes",
        "columns": ["outcome_ts", "market", "portfolio_id", "symbol", "horizon_days"],
        "sql": (
            "CREATE INDEX IF NOT EXISTS idx_investment_candidate_outcomes_weekly_lookup "
            "ON investment_candidate_outcomes (outcome_ts DESC, market, portfolio_id, symbol, horizon_days)"
        ),
    },
]


def _connect(db_path: Path, *, readonly: bool, timeout_sec: float) -> sqlite3.Connection:
    if readonly:
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=max(0.001, float(timeout_sec)))
    return sqlite3.connect(str(db_path), timeout=max(0.001, float(timeout_sec)))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _index_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def inspect_evidence_indexes(
    db_path: str | Path,
    *,
    timeout_sec: float = 30.0,
    now: datetime | None = None,
) -> Dict[str, Any]:
    path = Path(db_path)
    generated_at = (now or datetime.now(timezone.utc)).isoformat()
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return {
            "schema_version": "2026Q3.evidence_index_maintenance.v1",
            "generated_at": generated_at,
            "db_path": str(path),
            "db_exists": False,
            "db_size_bytes": 0,
            "status": "missing_db",
            "missing_index_count": len(EVIDENCE_INDEX_DEFINITIONS),
            "ready_index_count": 0,
            "rows": rows,
        }
    with _connect(path, readonly=True, timeout_sec=timeout_sec) as conn:
        for definition in EVIDENCE_INDEX_DEFINITIONS:
            table = str(definition["table"])
            required_columns = [str(col) for col in list(definition.get("columns") or [])]
            row = {
                "index_name": str(definition["name"]),
                "table": table,
                "required_columns": ",".join(required_columns),
                "status": "missing",
                "missing_columns": "",
                "action_required": True,
            }
            if not _table_exists(conn, table):
                row["status"] = "table_missing"
                row["action_required"] = False
                rows.append(row)
                continue
            columns = _table_columns(conn, table)
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                row["status"] = "columns_missing"
                row["missing_columns"] = ",".join(missing_columns)
                row["action_required"] = False
                rows.append(row)
                continue
            if str(definition["name"]) in _index_names(conn, table):
                row["status"] = "present"
                row["action_required"] = False
            rows.append(row)
    missing_count = sum(1 for row in rows if str(row.get("status")) == "missing")
    present_count = sum(1 for row in rows if str(row.get("status")) == "present")
    blocked_count = sum(1 for row in rows if str(row.get("status")) in {"table_missing", "columns_missing"})
    return {
        "schema_version": "2026Q3.evidence_index_maintenance.v1",
        "generated_at": generated_at,
        "db_path": str(path),
        "db_exists": True,
        "db_size_bytes": int(path.stat().st_size),
        "status": "ready" if missing_count == 0 and blocked_count == 0 else "missing_indexes",
        "missing_index_count": int(missing_count),
        "ready_index_count": int(present_count),
        "blocked_index_count": int(blocked_count),
        "rows": rows,
    }


def apply_evidence_indexes(
    db_path: str | Path,
    *,
    timeout_sec: float = 30.0,
    now: datetime | None = None,
) -> Dict[str, Any]:
    path = Path(db_path)
    before = inspect_evidence_indexes(path, timeout_sec=timeout_sec, now=now)
    actions: List[Dict[str, Any]] = []
    if not path.exists():
        return {
            "schema_version": "2026Q3.evidence_index_maintenance_apply.v1",
            "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
            "db_path": str(path),
            "applied": False,
            "status": "missing_db",
            "before": before,
            "after": before,
            "actions": actions,
        }
    definitions_by_name = {str(row["name"]): dict(row) for row in EVIDENCE_INDEX_DEFINITIONS}
    with _connect(path, readonly=False, timeout_sec=timeout_sec) as conn:
        for row in list(before.get("rows") or []):
            if str(row.get("status")) != "missing":
                actions.append(
                    {
                        "index_name": str(row.get("index_name") or ""),
                        "table": str(row.get("table") or ""),
                        "action": "skipped",
                        "reason": str(row.get("status") or "not_missing"),
                        "elapsed_sec": 0.0,
                    }
                )
                continue
            definition = definitions_by_name.get(str(row.get("index_name") or ""), {})
            started = time.monotonic()
            conn.execute(str(definition.get("sql") or ""))
            conn.commit()
            actions.append(
                {
                    "index_name": str(row.get("index_name") or ""),
                    "table": str(row.get("table") or ""),
                    "action": "created",
                    "reason": "missing",
                    "elapsed_sec": round(time.monotonic() - started, 6),
                }
            )
    after = inspect_evidence_indexes(path, timeout_sec=timeout_sec, now=now)
    return {
        "schema_version": "2026Q3.evidence_index_maintenance_apply.v1",
        "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
        "db_path": str(path),
        "applied": True,
        "status": str(after.get("status") or ""),
        "before": before,
        "after": after,
        "actions": actions,
    }
