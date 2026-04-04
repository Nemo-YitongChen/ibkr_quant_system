from __future__ import annotations

import importlib
import sqlite3

from src.app.engine import EngineConfig, TradingEngine
from src.common.storage import Storage


class DummyUniverseService:
    def build(self):
        return {"always_on": [], "short_candidates": []}


class DummyStrategy:
    pass


class DummyRunner:
    def tick(self):
        return None


class DummyIB:
    RequestTimeout = 0.0


def test_import_main_module_smoke() -> None:
    module = importlib.import_module("src.main")
    assert hasattr(module, "main")


def test_storage_smoke(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))

    storage.insert_signal_audit(
        {
            "symbol": "AAPL",
            "bar_end_time": "2026-04-05T10:00:00Z",
            "o": 1.0,
            "h": 1.1,
            "l": 0.9,
            "c": 1.05,
            "v": 1000.0,
            "last3_close": "[1.0, 1.02, 1.05]",
            "range20": 0.1,
            "mr_sig": 0.2,
            "bo_sig": 0.1,
            "short_sig": 0.16,
            "mid_scale": 0.5,
            "total_sig": 0.12,
            "threshold": 0.65,
            "should_trade": 0,
            "action": "",
            "reason": "smoke",
        }
    )
    storage.upsert_md_quality(
        day="2026-04-05",
        symbol="AAPL",
        buckets=1,
        duplicates=0,
        max_gap_sec=0,
        last_end_time="2026-04-05T10:00:00Z",
    )

    with sqlite3.connect(str(db_path)) as conn:
        signal_count = conn.execute("SELECT COUNT(*) FROM signals_audit").fetchone()[0]
        quality_count = conn.execute("SELECT COUNT(*) FROM md_quality").fetchone()[0]

    assert signal_count == 1
    assert quality_count == 1


def test_engine_instantiation_smoke() -> None:
    engine = TradingEngine(
        ib=DummyIB(),
        universe_svc=DummyUniverseService(),
        strategy=DummyStrategy(),
        runner=DummyRunner(),
        cfg=EngineConfig(),
    )

    assert hasattr(engine, "run_forever")
    assert hasattr(engine, "_update_quality")
    assert engine._states == {}
