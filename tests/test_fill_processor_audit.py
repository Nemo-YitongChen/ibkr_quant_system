from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.common.storage import Storage
from src.ibkr.fills import FillProcessor

pytestmark = pytest.mark.guardrail


class _DummyEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class _FakeIB:
    def __init__(self) -> None:
        self.execDetailsEvent = _DummyEvent()
        self.commissionReportEvent = _DummyEvent()


class _FakeGate:
    def __init__(self) -> None:
        self.calls = []

    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None:
        self.calls.append((trade_pnl, details))


class _Contract:
    symbol = "AAPL"
    exchange = "SMART"
    currency = "USD"


def _make_fill(order_id: int, exec_id: str, side: str, shares: float, price: float):
    class _Execution:
        def __init__(self) -> None:
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


def _fetch_one_fill(db_path: Path):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT symbol, action, qty, price, pnl, expected_price, expected_slippage_bps, "
            "actual_slippage_bps, slippage_bps_deviation, event_risk_reason, short_borrow_source, "
            "portfolio_id, system_kind, execution_run_id "
            "FROM fills ORDER BY id ASC LIMIT 1"
        ).fetchone()


def _fetch_risk_events(db_path: Path):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT kind, value, symbol, order_id, exec_id, expected_price, actual_price, "
            "expected_slippage_bps, actual_slippage_bps, slippage_bps_deviation, "
            "event_risk_reason, short_borrow_source, portfolio_id, system_kind, execution_run_id "
            "FROM risk_events ORDER BY id ASC"
        ).fetchall()


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


def test_fill_processor_persists_fill_and_risk_chain(tmp_path) -> None:
    db_path = tmp_path / "audit.db"
    storage = Storage(str(db_path))

    details = {
        "expected_price": 100.0,
        "expected_slippage_bps": 12.5,
        "risk_snapshot": {
            "event_risk_reason": "earnings_window",
            "short_borrow_source": "ibkr_stock_loan",
        },
        "portfolio_id": "growth_us",
        "system_kind": "paper",
        "execution_run_id": "run-001",
    }
    storage.insert_order(
        {
            "account_id": "DU123456",
            "symbol": "AAPL",
            "exchange": "SMART",
            "currency": "USD",
            "action": "BUY",
            "qty": 10,
            "order_type": "LMT",
            "order_id": 2001,
            "parent_id": 0,
            "status": "SUBMITTED",
            "portfolio_id": "growth_us",
            "system_kind": "paper",
            "execution_run_id": "run-001",
            "details": json.dumps(details, ensure_ascii=False),
        }
    )

    gate = _FakeGate()
    processor = FillProcessor(ib=_FakeIB(), storage=storage, gate=gate)

    trade = SimpleNamespace()
    fill = SimpleNamespace(
        contract=SimpleNamespace(symbol="AAPL", exchange="SMART", currency="USD"),
        execution=SimpleNamespace(orderId=2001, execId="exec-1", side="BOT", shares=10, price=101.0),
    )
    report = SimpleNamespace(commission=1.25)

    processor._on_exec_details(trade, fill)
    processor._on_commission(trade, fill, report)

    row = _fetch_one_fill(db_path)
    assert row is not None
    assert row[0] == "AAPL"
    assert row[1] == "BUY"
    assert row[2] == 10.0
    assert row[3] == 101.0
    assert row[4] == 0.0
    assert row[5] == 100.0
    assert row[6] == 12.5
    assert row[7] == 100.0
    assert row[8] == 87.5
    assert row[9] == "earnings_window"
    assert row[10] == "ibkr_stock_loan"
    assert row[11] == "growth_us"
    assert row[12] == "paper"
    assert row[13] == "run-001"

    risk_rows = _fetch_risk_events(db_path)
    assert len(risk_rows) == 2

    slippage_event, commission_event = risk_rows
    assert slippage_event[0] == "EXECUTION_SLIPPAGE_BPS"
    assert slippage_event[1] == 100.0
    assert slippage_event[2] == "AAPL"
    assert slippage_event[3] == 2001
    assert slippage_event[4] == "exec-1"
    assert slippage_event[5] == 100.0
    assert slippage_event[6] == 101.0
    assert slippage_event[7] == 12.5
    assert slippage_event[8] == 100.0
    assert slippage_event[9] == 87.5
    assert slippage_event[10] == "earnings_window"
    assert slippage_event[11] == "ibkr_stock_loan"
    assert slippage_event[12] == "growth_us"
    assert slippage_event[13] == "paper"
    assert slippage_event[14] == "run-001"

    assert commission_event[0] == "COMMISSION"
    assert commission_event[1] == 1.25
    assert commission_event[2] == "AAPL"
    assert commission_event[3] == 2001
    assert commission_event[4] == "exec-1"
    assert commission_event[12] == "growth_us"
    assert commission_event[13] == "paper"
    assert commission_event[14] == "run-001"

    assert len(gate.calls) == 1
    assert gate.calls[0][0] == -1.25
    assert gate.calls[0][1] == "AAPL execId=exec-1 commission=1.25"
