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
        ib=object(),
        universe_svc=SimpleNamespace(build=lambda: {}),
        strategy=object(),
        runner=SimpleNamespace(),
        cfg=EngineConfig(),
    )

    assert callable(engine._update_quality)
    assert callable(engine.run_forever)
    assert callable(engine._sleep_with_runner)
