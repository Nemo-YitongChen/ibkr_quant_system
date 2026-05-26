import sqlite3
import threading
import time

from src.common.storage import Storage
from src.common.sqlite_utils import connect_sqlite


def test_connect_sqlite_applies_busy_timeout(tmp_path):
    db_path = tmp_path / "audit.db"
    with connect_sqlite(db_path, timeout_sec=7.5) as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 7500


def test_storage_retries_short_lived_sqlite_write_lock(tmp_path):
    db_path = tmp_path / "audit.db"
    storage = Storage(
        str(db_path),
        sqlite_timeout_sec=0.02,
        sqlite_write_retry_attempts=8,
        sqlite_write_retry_initial_delay_sec=0.05,
    )
    locker = sqlite3.connect(str(db_path), timeout=0.1)
    locker.execute("BEGIN IMMEDIATE")

    errors = []

    def _writer():
        try:
            storage.insert_investment_execution_run(
                {
                    "run_id": "lock-retry-run",
                    "market": "HK",
                    "portfolio_id": "paper-hk",
                    "submitted": 0,
                    "order_count": 0,
                }
            )
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=_writer)
    thread.start()
    time.sleep(0.15)
    locker.rollback()
    locker.close()
    thread.join(timeout=3.0)

    assert not thread.is_alive()
    assert errors == []
    with storage._conn() as conn:
        row = conn.execute(
            "SELECT run_id, market, portfolio_id FROM investment_execution_runs WHERE run_id=?",
            ("lock-retry-run",),
        ).fetchone()
    assert row == ("lock-retry-run", "HK", "paper-hk")
