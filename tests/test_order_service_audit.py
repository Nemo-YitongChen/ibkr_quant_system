from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.common.storage import Storage
from src.ibkr.investment_orders import InvestmentOrderParams, InvestmentOrderService

pytestmark = pytest.mark.guardrail


class _DummyEvent:
    def __iadd__(self, _handler):
        return self


class _FakeClient:
    def __init__(self, start: int = 1000):
        self._next = start

    def getReqId(self) -> int:
        self._next += 1
        return self._next


class _FakeIB:
    def __init__(self):
        self.orderStatusEvent = _DummyEvent()
        self.errorEvent = _DummyEvent()
        self.client = _FakeClient()
        self.qualified = []
        self.placed = []

    def qualifyContracts(self, contract):
        self.qualified.append(contract)
        return [contract]

    def placeOrder(self, contract, order):
        trade = SimpleNamespace(order=order, contract=contract)
        self.placed.append(trade)
        return trade


def test_investment_order_service_persists_order_and_execution_audit_rows(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))
    fake_ib = _FakeIB()
    service = InvestmentOrderService(
        fake_ib,
        "DUQ152001",
        storage,
        market="US",
        portfolio_id="US:test",
    )

    contract = SimpleNamespace(symbol="AAPL", exchange="SMART", currency="USD")
    plan_row = {
        "market": "US",
        "symbol": "AAPL",
        "action": "BUY",
        "current_qty": 0.0,
        "target_qty": 5.0,
        "delta_qty": 5.0,
        "ref_price": 100.0,
        "target_weight": 0.20,
        "order_value": 500.0,
        "reason": "rebalance_up",
        "expected_slippage_bps": 4.5,
    }

    trade = service.place_rebalance_order(
        contract,
        symbol="AAPL",
        action="BUY",
        qty=5.0,
        params=InvestmentOrderParams(order_type="LMT", ref_price=100.0, limit_price_buffer_bps=10.0),
        portfolio_id="US:test",
        execution_run_id="US-exec-001",
        plan_row=plan_row,
    )

    assert trade.order.orderId == 1001
    assert len(fake_ib.qualified) == 1
    assert len(fake_ib.placed) == 1

    order_meta = storage.get_order_by_order_id(1001)
    details_json = dict(order_meta.get("details_json") or {})

    assert order_meta["symbol"] == "AAPL"
    assert order_meta["portfolio_id"] == "US:test"
    assert order_meta["system_kind"] == "investment_execution"
    assert order_meta["execution_run_id"] == "US-exec-001"
    assert order_meta["status"] == "CREATED"
    assert details_json["expected_price"] == 100.0
    assert details_json["limit_price"] == 100.1
    assert details_json["portfolio_id"] == "US:test"
    assert details_json["execution_run_id"] == "US-exec-001"
    assert details_json["plan_row"]["reason"] == "rebalance_up"

    with storage._conn() as conn:
        execution_rows = conn.execute(
            """
            SELECT symbol, broker_order_id, status, details
            FROM investment_execution_orders
            WHERE run_id=?
            """,
            ("US-exec-001",),
        ).fetchall()

    assert len(execution_rows) == 1
    assert execution_rows[0][0] == "AAPL"
    assert execution_rows[0][1] == 1001
    assert execution_rows[0][2] == "CREATED"
    exec_details = json.loads(execution_rows[0][3])
    assert exec_details["expected_price"] == 100.0
    assert exec_details["plan_row"]["target_weight"] == 0.20
