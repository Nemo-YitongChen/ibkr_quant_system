from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def connect_sqlite(
    db_path: str | Path,
    *,
    timeout_sec: float = 30.0,
    row_factory: Any | None = None,
) -> sqlite3.Connection:
    """Open a SQLite connection with the project's lock-tolerant defaults."""

    timeout = max(0.001, float(timeout_sec or 30.0))
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    if row_factory is not None:
        conn.row_factory = row_factory
    try:
        conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    except sqlite3.Error:
        pass
    return conn
