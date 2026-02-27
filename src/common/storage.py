import sqlite3
from typing import Any, Dict, Optional
from datetime import datetime

class Storage:
    """SQLite-backed audit/risk event store used by execution and risk modules."""

    def __init__(self, db_path: str = "audit.db"):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        # Keep connections short-lived; context managers commit/rollback automatically.
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._conn() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                account_id TEXT,
                symbol TEXT,
                exchange TEXT,
                currency TEXT,
                action TEXT,
                qty REAL,
                order_type TEXT,
                order_id INTEGER,
                parent_id INTEGER,
                status TEXT,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                order_id INTEGER,
                exec_id TEXT,
                symbol TEXT,
                action TEXT,
                qty REAL,
                price REAL,
                pnl REAL,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                kind TEXT,
                value REAL,
                details TEXT
            )""")

    def insert_order(self, row: Dict[str, Any]):
        # Accept sparse payloads so callers can evolve fields gradually.
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO orders ({cols}) VALUES ({qs})", list(row.values()))

    def update_order_status(self, order_id: int, status: str):
        with self._conn() as c:
            c.execute("UPDATE orders SET status=? WHERE order_id=?", (status, order_id))

    def insert_fill(self, row: Dict[str, Any]):
        # Mirror order insert path for execution/fill audit rows.
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO fills ({cols}) VALUES ({qs})", list(row.values()))

    def insert_risk_event(self, kind: str, value: float, details: str = ""):
        with self._conn() as c:
            c.execute(
                "INSERT INTO risk_events (ts, kind, value, details) VALUES (?, ?, ?, ?)",
                (datetime.utcnow().isoformat(), kind, value, details),
            )
