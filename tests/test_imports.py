from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from src.app.engine import EngineConfig, TradingEngine
from src.common.storage import Storage
from src.ibkr.account import AccountService
from src.ibkr.orders import OrderService

pytestmark = pytest.mark.guardrail


class _FakeEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class _FakeIB:
    def __init__(self) -> None:
        self.orderStatusEvent = _FakeEvent()
        self.errorEvent = _FakeEvent()
        self.client = SimpleNamespace(getReqId=lambda: 1000)

    def accountSummary(self):
        return []

    def qualifyContracts(self, contract):
        return [contract]


def test_main_module_imports() -> None:
    module = importlib.import_module("src.main")

    assert callable(module.main)


def test_storage_audit_methods_initialize_and_write(tmp_path) -> None:
    storage = Storage(str(tmp_path / "audit.db"))

    storage.insert_signal_audit(
        {
            "symbol": "AAPL",
            "bar_end_time": "2026-04-24T00:00:00+00:00",
            "should_trade": 0,
            "reason": "import-smoke",
        }
    )
    storage.upsert_md_quality(
        day="2026-04-24",
        symbol="AAPL",
        buckets=1,
        duplicates=0,
        max_gap_sec=0,
        last_end_time="2026-04-24T00:00:00+00:00",
    )

    with storage._conn() as conn:
        signal_count = conn.execute("SELECT COUNT(*) FROM signals_audit").fetchone()[0]
        quality_count = conn.execute("SELECT COUNT(*) FROM md_quality").fetchone()[0]

    assert signal_count == 1
    assert quality_count == 1


def test_engine_bootstrap_with_mock_services(tmp_path) -> None:
    ib = _FakeIB()
    storage = Storage(str(tmp_path / "audit.db"))
    account = AccountService(ib, "DU123")
    orders = OrderService(ib, "DU123", storage)
    engine = TradingEngine(
        ib=ib,
        universe_svc=SimpleNamespace(build=lambda: {"always_on": [], "short_candidates": []}),
        strategy=SimpleNamespace(execute=lambda *_args, **_kwargs: None, orders=orders),
        runner=SimpleNamespace(sleep=lambda _seconds: None),
        cfg=EngineConfig(cycle_sec=0),
        storage=storage,
    )

    assert hasattr(engine, "run_forever")
    assert callable(engine.run_forever)
    assert hasattr(engine, "_update_quality")
    assert account.get_netliq() is None
    engine._update_quality("AAPL", 1776988800, False)
    assert storage.get_md_quality("2026-04-24")
