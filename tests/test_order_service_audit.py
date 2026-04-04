from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from src.common.storage import Storage
from src.ibkr.orders import BracketParams, OrderService


class DummyEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class DummyClient:
    def __init__(self) -> None:
        self._req_id = 1000

    def getReqId(self) -> int:
        rid = self._req_id
        self._req_id += 3
        return rid


class DummyIB:
    def __init__(self) -> None:
        self.client = DummyClient()
        self.orderStatusEvent = DummyEvent()
        self.errorEvent = DummyEvent()
        self.qualified_contracts = []
        self.placed_orders = []

    def qualifyContracts(self, contract) -> None:
        self.qualified_contracts.append(contract)

    def placeOrder(self, contract, order):
        self.placed_orders.append((contract, order))
        return SimpleNamespace(contract=contract, order=order)


def _fetch_orders(db_path: Path):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT symbol, action, qty, order_type, order_id, parent_id, status, details FROM orders ORDER BY id ASC"
        ).fetchall()


def test_place_bracket_persists_parent_and_children(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))
    ib = DummyIB()
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
    assert len(ib.placed_orders) == 3
    assert ib.qualified_contracts == [contract]

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
