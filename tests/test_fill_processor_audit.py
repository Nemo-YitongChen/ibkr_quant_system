from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from src.common.storage import Storage
from src.ibkr.fills import FillProcessor


class DummyEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class DummyIB:
    def __init__(self) -> None:
        self.execDetailsEvent = DummyEvent()
        self.commissionReportEvent = DummyEvent()


class DummyGate:
    def __init__(self) -> None:
        self.closed = []

    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None:
        self.closed.append({"trade_pnl": trade_pnl, "details": details})


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

    gate = DummyGate()
    processor = FillProcessor(ib=DummyIB(), storage=storage, gate=gate)

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
    assert row[4] == 0.0  # opening trade -> realized gross stays zero in the ledger
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

    assert len(gate.closed) == 1
    assert gate.closed[0]["trade_pnl"] == -1.25
    assert "AAPL execId=exec-1 commission=1.25" == gate.closed[0]["details"]
