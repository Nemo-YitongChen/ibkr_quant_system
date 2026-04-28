from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from src.app.engine import EngineConfig, TradingEngine
from src.common.storage import Storage

pytestmark = pytest.mark.guardrail


def test_src_main_imports_and_exposes_main() -> None:
    module = importlib.import_module("src.main")

    assert hasattr(module, "main")
    assert callable(module.main)


def test_storage_initializes_and_persists_core_rows(tmp_path) -> None:
    db_path = tmp_path / "smoke.db"
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
            "should_trade": 1,
            "risk_allowed": 1,
            "action": "BUY",
            "reason": "smoke",
        }
    )
    storage.upsert_md_quality(
        day="2026-04-12",
        symbol="AAPL",
        buckets=12,
        duplicates=1,
        max_gap_sec=300,
        last_end_time="2026-04-12T00:00:00+00:00",
    )

    with storage._conn() as conn:
        signal_count = conn.execute("SELECT COUNT(*) FROM signals_audit").fetchone()[0]
        quality_count = conn.execute("SELECT COUNT(*) FROM md_quality").fetchone()[0]

    assert signal_count == 1
    assert quality_count == 1


def test_trading_engine_smoke_instantiation_keeps_critical_methods() -> None:
    engine = TradingEngine(
        ib=SimpleNamespace(RequestTimeout=0.0),
        universe_svc=SimpleNamespace(build=lambda: {"always_on": [], "short_candidates": []}),
        strategy=SimpleNamespace(),
        runner=SimpleNamespace(tick=lambda: None),
        cfg=EngineConfig(),
    )

    assert callable(engine._update_quality)
    assert callable(engine.run_forever)
    assert callable(engine._sleep_with_runner)
    assert engine._states == {}
