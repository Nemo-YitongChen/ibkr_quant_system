from __future__ import annotations

import math

import pytest

from src.common.storage import Storage
from src.ibkr.fills import FillProcessor

pytestmark = pytest.mark.guardrail


class _DummyEvent:
    def __iadd__(self, _handler):
        return self


class _FakeIB:
    def __init__(self):
        self.execDetailsEvent = _DummyEvent()
        self.commissionReportEvent = _DummyEvent()


class _FakeGate:
    def __init__(self):
        self.calls = []

    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None:
        self.calls.append((trade_pnl, details))


class _Contract:
    symbol = "AAPL"
    exchange = "SMART"
    currency = "USD"


def _make_fill(order_id: int, exec_id: str, side: str, shares: float, price: float):
    class _Execution:
        def __init__(self):
            self.orderId = order_id
            self.execId = exec_id
            self.side = side
            self.shares = shares
            self.price = price

    class _Fill:
        contract = _Contract()
        execution = _Execution()

    return _Fill()


def _make_commission_report(value: float):
    class _Report:
        commission = value

    return _Report()


def test_fill_processor_persists_fill_risk_events_and_gate_callback_chain(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))
    fake_ib = _FakeIB()
    gate = _FakeGate()
    processor = FillProcessor(fake_ib, storage, gate)

    common_order_fields = {
        "account_id": "DUQ152001",
        "symbol": "AAPL",
        "exchange": "SMART",
        "currency": "USD",
        "qty": 1.0,
        "order_type": "LMT",
        "parent_id": 0,
        "status": "Filled",
        "portfolio_id": "US:test",
        "system_kind": "investment_execution",
        "execution_run_id": "US-exec-001",
    }

    storage.insert_order(
        {
            **common_order_fields,
            "order_id": 2001,
            "action": "BUY",
            "details": (
                '{"expected_price":100.0,"expected_slippage_bps":5.0,'
                '"risk_snapshot":{"event_risk_reason":"earnings","short_borrow_source":"ibkr"},'
                '"portfolio_id":"US:test","system_kind":"investment_execution","execution_run_id":"US-exec-001"}'
            ),
        }
    )
    storage.insert_order(
        {
            **common_order_fields,
            "order_id": 2002,
            "action": "SELL",
            "details": (
                '{"expected_price":101.0,"expected_slippage_bps":3.0,'
                '"risk_snapshot":{"event_risk_reason":"earnings","short_borrow_source":"ibkr"},'
                '"portfolio_id":"US:test","system_kind":"investment_execution","execution_run_id":"US-exec-001"}'
            ),
        }
    )

    processor._on_exec_details(None, _make_fill(2001, "exec-buy", "BOT", 1.0, 100.0))
    processor._on_exec_details(None, _make_fill(2002, "exec-sell", "SLD", 1.0, 101.0))
    processor._on_commission(None, _make_fill(2002, "exec-sell", "SLD", 1.0, 101.0), _make_commission_report(1.5))

    with storage._conn() as conn:
        fill_rows = conn.execute(
            """
            SELECT order_id, exec_id, symbol, action, pnl, actual_slippage_bps, portfolio_id, system_kind, execution_run_id
            FROM fills
            ORDER BY order_id ASC
            """
        ).fetchall()
        risk_rows = conn.execute(
            """
            SELECT kind, value, symbol, exec_id, portfolio_id, system_kind, execution_run_id
            FROM risk_events
            ORDER BY id ASC
            """
        ).fetchall()

    assert len(fill_rows) == 2
    assert fill_rows[0][0] == 2001
    assert fill_rows[0][2] == "AAPL"
    assert fill_rows[0][3] == "BUY"
    assert fill_rows[1][0] == 2002
    assert fill_rows[1][3] == "SELL"
    assert math.isclose(float(fill_rows[1][4]), 1.0, rel_tol=0.0, abs_tol=1e-9)
    assert fill_rows[1][6] == "US:test"
    assert fill_rows[1][7] == "investment_execution"
    assert fill_rows[1][8] == "US-exec-001"

    risk_kinds = [row[0] for row in risk_rows]
    assert risk_kinds.count("EXECUTION_SLIPPAGE_BPS") == 2
    assert risk_kinds.count("COMMISSION") == 1
    commission_row = next(row for row in risk_rows if row[0] == "COMMISSION")
    assert math.isclose(float(commission_row[1]), 1.5, rel_tol=0.0, abs_tol=1e-9)
    assert commission_row[2] == "AAPL"
    assert commission_row[3] == "exec-sell"
    assert commission_row[4] == "US:test"
    assert commission_row[5] == "investment_execution"
    assert commission_row[6] == "US-exec-001"

    assert len(gate.calls) == 1
    assert math.isclose(float(gate.calls[0][0]), -0.5, rel_tol=0.0, abs_tol=1e-9)
    assert "AAPL" in gate.calls[0][1]
    assert "commission=1.5" in gate.calls[0][1]
