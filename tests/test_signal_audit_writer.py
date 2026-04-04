from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.common.signal_audit import SignalAuditContext, SignalAuditWriter
from src.risk.model import PreTradeRiskSnapshot


class _FakeStorage:
    def __init__(self):
        self.rows = []

    def insert_signal_audit(self, row):
        self.rows.append(row)


class _FakeBar:
    open = 100.0
    high = 101.0
    low = 99.5
    close = 100.5
    volume = 1000.0
    end_time = datetime(2026, 3, 30, 0, 5, tzinfo=timezone.utc)


def test_signal_audit_writer_persists_row() -> None:
    storage = _FakeStorage()
    writer = SignalAuditWriter(storage)
    snapshot = PreTradeRiskSnapshot(
        symbol="AAPL",
        action="BUY",
        entry_price=100.0,
        atr_stop=1.0,
        slippage_bps=5.0,
        gap_addon_pct=0.002,
        liquidity_haircut=0.0,
        slippage_addon_price=0.05,
        gap_addon_price=0.2,
        liquidity_addon_price=0.0,
        short_addon_price=0.0,
        stop_distance=1.25,
        take_profit_distance=2.25,
        stop_price=98.75,
        take_profit_price=102.25,
        event_risk="NONE",
        event_risk_reason="",
        short_borrow_fee_bps=0.0,
        short_borrow_source="",
        allowed=True,
        block_reasons=[],
        atr_pct=0.01,
        avg_bar_volume=10000.0,
        risk_per_share=1.25,
        expected_fill_price=100.05,
    )
    writer.write(
        SignalAuditContext(
            symbol="AAPL",
            bar=_FakeBar(),
            closes=[100.0, 100.2, 100.5],
            highs=[100.5, 100.7, 101.0],
            lows=[99.5, 99.8, 99.9],
            mr_sig=0.8,
            bo_sig=0.2,
            short_sig=0.56,
            mid_scale=0.7,
            total_sig=0.9,
            threshold_used=0.45,
            should_trade=True,
            action="BUY",
            channel="PURE_SHORT",
            qty_multiplier=1.0,
            can_trade_short=True,
            risk_snapshot=snapshot,
            regime_state_v2=SimpleNamespace(to_dict=lambda: {"state": "UPTREND"}),
            signal_decision=SimpleNamespace(to_dict=lambda: {"action": "BUY"}),
            risk_decision=SimpleNamespace(to_dict=lambda: {"allowed": True}),
            audit_tag="ALWAYS_ON",
            audit_source="REALTIME",
        )
    )

    assert len(storage.rows) == 1
    row = storage.rows[0]
    assert row["symbol"] == "AAPL"
    assert row["channel"] == "PURE_SHORT"
    assert row["reason"] == "ALWAYS_ON|REALTIME|PURE_SHORT|thr=0.450|qmul=1.00"
    assert row["risk_allowed"] == 1
    assert row["risk_gate"] == "OK"


def test_signal_audit_writer_uses_pretrade_allowed_for_risk_gate() -> None:
    storage = _FakeStorage()
    writer = SignalAuditWriter(storage)
    snapshot = PreTradeRiskSnapshot(
        symbol="AAPL",
        action="BUY",
        entry_price=100.0,
        atr_stop=1.0,
        slippage_bps=5.0,
        gap_addon_pct=0.002,
        liquidity_haircut=0.0,
        slippage_addon_price=0.05,
        gap_addon_price=0.2,
        liquidity_addon_price=0.0,
        short_addon_price=0.0,
        stop_distance=1.25,
        take_profit_distance=2.25,
        stop_price=98.75,
        take_profit_price=102.25,
        event_risk="EARNINGS",
        event_risk_reason="earnings blackout",
        short_borrow_fee_bps=0.0,
        short_borrow_source="",
        allowed=False,
        block_reasons=["event_risk"],
        atr_pct=0.01,
        avg_bar_volume=10000.0,
        risk_per_share=1.25,
        expected_fill_price=100.05,
    )
    writer.write(
        SignalAuditContext(
            symbol="AAPL",
            bar=_FakeBar(),
            closes=[100.0, 100.2, 100.5],
            highs=[100.5, 100.7, 101.0],
            lows=[99.5, 99.8, 99.9],
            mr_sig=0.8,
            bo_sig=0.2,
            short_sig=0.56,
            mid_scale=0.7,
            total_sig=0.9,
            threshold_used=0.45,
            should_trade=True,
            action="BUY",
            channel="PURE_SHORT",
            qty_multiplier=1.0,
            can_trade_short=True,
            risk_snapshot=snapshot,
            regime_state_v2=SimpleNamespace(to_dict=lambda: {"state": "UPTREND"}),
            signal_decision=SimpleNamespace(to_dict=lambda: {"action": "BUY"}),
            risk_decision=SimpleNamespace(to_dict=lambda: {"allowed": False}),
            audit_tag="ALWAYS_ON",
            audit_source="REALTIME",
        )
    )

    assert len(storage.rows) == 1
    row = storage.rows[0]
    assert row["can_trade_short"] == 1
    assert row["risk_allowed"] == 0
    assert row["risk_gate"] == "BLOCKED"
