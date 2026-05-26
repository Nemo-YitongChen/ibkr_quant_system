from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.common.storage import Storage
from src.ibkr.investment_orders import (
    InvestmentContractQualificationError,
    InvestmentOrderParams,
    InvestmentOrderService,
)
from src.ibkr.orders import BracketParams, OrderService

pytestmark = pytest.mark.guardrail


class _DummyEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class _FakeClient:
    def __init__(self, start: int = 1000, step: int = 1) -> None:
        self._next = start
        self._step = step

    def getReqId(self) -> int:
        current = self._next
        self._next += self._step
        return current


class _FakeIB:
    def __init__(self, client: _FakeClient | None = None) -> None:
        self.orderStatusEvent = _DummyEvent()
        self.errorEvent = _DummyEvent()
        self.client = client or _FakeClient(start=1001)
        self.qualified = []
        self.placed = []

    def qualifyContracts(self, contract):
        self.qualified.append(contract)
        return [contract]

    def placeOrder(self, contract, order):
        trade = SimpleNamespace(order=order, contract=contract)
        self.placed.append(trade)
        return trade


class _UnqualifiedFakeIB(_FakeIB):
    def qualifyContracts(self, contract):
        self.qualified.append(contract)
        return []


def _fetch_orders(db_path: Path):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT symbol, action, qty, order_type, order_id, parent_id, status, details FROM orders ORDER BY id ASC"
        ).fetchall()


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


def test_investment_order_service_blocks_unqualified_contract_before_order_creation(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))
    fake_ib = _UnqualifiedFakeIB()
    service = InvestmentOrderService(
        fake_ib,
        "DUQ152001",
        storage,
        market="US",
        portfolio_id="US:test",
    )

    contract = SimpleNamespace(symbol="SPLG", exchange="SMART", primaryExchange="ARCA", currency="USD")
    with pytest.raises(InvestmentContractQualificationError, match="contract qualification returned no match"):
        service.place_rebalance_order(
            contract,
            symbol="SPLG",
            action="BUY",
            qty=1.0,
            params=InvestmentOrderParams(order_type="LMT", ref_price=87.37, limit_price_buffer_bps=10.0),
            portfolio_id="US:test",
            execution_run_id="US-exec-unqualified",
            plan_row={
                "market": "US",
                "symbol": "SPLG",
                "action": "BUY",
                "delta_qty": 1.0,
                "ref_price": 87.37,
                "order_value": 87.37,
                "reason": "unit-test",
            },
        )

    assert len(fake_ib.qualified) == 1
    assert fake_ib.placed == []
    with storage._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM investment_execution_orders").fetchone()[0] == 0


def test_investment_order_warning_does_not_overwrite_active_broker_status(tmp_path) -> None:
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

    contract = SimpleNamespace(symbol="SCHX", exchange="SMART", primaryExchange="ARCA", currency="USD")
    trade = service.place_rebalance_order(
        contract,
        symbol="SCHX",
        action="BUY",
        qty=1.0,
        params=InvestmentOrderParams(order_type="LMT", ref_price=29.28, limit_price_buffer_bps=10.0),
        portfolio_id="US:test",
        execution_run_id="US-exec-warning",
        plan_row={
            "market": "US",
            "symbol": "SCHX",
            "action": "BUY",
            "delta_qty": 1.0,
            "ref_price": 29.28,
            "order_value": 29.28,
            "reason": "unit-test",
        },
    )
    trade.orderStatus = SimpleNamespace(status="PreSubmitted", filled=0.0, remaining=1.0)

    service._on_order_status(trade)
    service._on_error(
        trade.order.orderId,
        399,
        "Order Message: BUY 1 SCHX ARCA Warning: order will be placed at next open.",
        None,
    )

    assert storage.get_order_by_order_id(trade.order.orderId)["status"] == "PreSubmitted"
    with storage._conn() as conn:
        risk_events = conn.execute(
            "SELECT kind, order_id FROM risk_events WHERE kind='INVESTMENT_ORDER_WARNING'"
        ).fetchall()
    assert risk_events == [("INVESTMENT_ORDER_WARNING", trade.order.orderId)]


def test_place_bracket_persists_parent_and_children(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))
    ib = _FakeIB(client=_FakeClient(start=1000, step=3))
    service = OrderService(ib=ib, account_id="DU123456", storage=storage)

    contract = SimpleNamespace(symbol="AAPL", exchange="SMART", currency="USD")
    result = service.place_bracket(
        contract=contract,
        action="BUY",
        qty=10,
        entry_price=100.0,
        params=BracketParams(take_profit_pct=0.05, stop_loss_pct=0.02),
        signal_reason="unit-test",
        signal_tag="TEST",
        signal_source="UNIT",
    )

    assert len(result) == 3
    assert len(ib.placed) == 3
    assert ib.qualified == [contract]

    rows = _fetch_orders(db_path)
    assert len(rows) == 3

    parent, take_profit, stop_loss = rows

    assert parent[0] == "AAPL"
    assert parent[1] == "BUY"
    assert parent[2] == 10.0
    assert parent[3] == "LMT"
    assert parent[4] == 1000
    assert parent[5] == 0
    assert parent[6] == "CREATED"

    parent_details = json.loads(parent[7])
    assert parent_details["signal_reason"] == "unit-test"
    assert parent_details["signal_tag"] == "TEST"
    assert parent_details["signal_source"] == "UNIT"
    assert parent_details["leg"] == "parent"
    assert parent_details["take_profit_price"] == 105.0
    assert parent_details["stop_loss_price"] == 98.0

    assert take_profit[1] == "SELL"
    assert take_profit[3] == "LMT"
    assert take_profit[4] == 1001
    assert take_profit[5] == 1000

    tp_details = json.loads(take_profit[7])
    assert tp_details["leg"] == "take_profit"
    assert tp_details["expected_price"] == 105.0

    assert stop_loss[1] == "SELL"
    assert stop_loss[3] == "STP"
    assert stop_loss[4] == 1002
    assert stop_loss[5] == 1000

    sl_details = json.loads(stop_loss[7])
    assert sl_details["leg"] == "stop_loss"
    assert sl_details["expected_price"] == 98.0
