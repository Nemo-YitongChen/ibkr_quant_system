from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.offhours.compute_short import compute_engine_signal_for_symbol
from src.portfolio.allocator import AllocatorConfig, PortfolioAllocator
from src.risk.model import TradeRiskConfig, TradeRiskModel
from src.risk.short_safety import ShortSafetyConfig, ShortSafetyGate
from src.strategies.engine_strategy import StrategyConfig


@dataclass
class _Bar:
    open: float
    high: float
    low: float
    close: float
    volume: float
    end_time: datetime


class _FakeAccount:
    def __init__(self, netliq: float):
        self._netliq = netliq

    def get_netliq(self) -> float:
        return float(self._netliq)


class _FakeIB:
    def positions(self):
        return []


class _FakeGate:
    def can_trade_short(self) -> bool:
        return True

    def event_risk_for(self, symbol: str) -> str:
        return "NONE"

    def event_risk_reason_for(self, symbol: str) -> str:
        return ""

    def short_borrow_fee_bps_for(self, symbol: str) -> float:
        return 25.0

    def short_borrow_source_for(self, symbol: str) -> str:
        return "unit_test"


class _FakeMD:
    def __init__(self, bars):
        self._bars = bars

    def get_5m_bars(self, symbol: str, need: int = 600):
        return list(self._bars[-int(need):])


class RiskPipelineTests(unittest.TestCase):
    def test_trade_risk_snapshot_builds_explicit_prices_and_components(self):
        model = TradeRiskModel(
            TradeRiskConfig(
                atr_window=5,
                atr_stop_mult=1.0,
                min_stop_loss_pct=0.01,
                liquidity_target_bar_volume=10_000.0,
                min_avg_bar_volume=1_000.0,
            )
        )
        snapshot = model.build_snapshot(
            symbol="TSLA",
            action="SELL",
            entry_price=100.0,
            highs=[101, 103, 102, 104, 105, 107],
            lows=[99, 97, 98, 96, 95, 94],
            closes=[100, 101, 100, 102, 99, 98],
            volumes=[2_000, 2_200, 2_100, 1_800, 1_700, 1_600],
            can_short=True,
            event_risk="NONE",
            short_borrow_fee_bps=120.0,
            short_borrow_source="desk",
        )

        self.assertGreater(snapshot.stop_distance, snapshot.atr_stop)
        self.assertGreater(snapshot.slippage_addon_price, 0.0)
        self.assertGreater(snapshot.gap_addon_price, 0.0)
        self.assertGreater(snapshot.liquidity_addon_price, 0.0)
        self.assertGreater(snapshot.short_addon_price, 0.0)
        self.assertGreater(snapshot.stop_price, snapshot.entry_price)
        self.assertLess(snapshot.take_profit_price, snapshot.entry_price)

    def test_allocator_uses_risk_per_share_and_liquidity_haircut(self):
        allocator = PortfolioAllocator(
            _FakeIB(),
            _FakeAccount(10_000.0),
            AllocatorConfig(risk_per_trade=0.02, max_open_positions=8, max_gross_leverage=2.0),
        )
        snapshot = TradeRiskModel().build_snapshot(
            symbol="AAPL",
            action="BUY",
            entry_price=100.0,
            highs=[101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
            lows=[99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85],
            closes=[100] * 15,
            volumes=[1_000] * 15,
            can_short=True,
        )
        snapshot.liquidity_haircut = 0.5
        snapshot.risk_per_share = 5.0

        qty = allocator.size_qty(requested_qty=100.0, entry_price=100.0, risk_snapshot=snapshot)
        self.assertEqual(qty, 40.0)

    def test_short_safety_blocks_unknown_ssr_borrow_and_earnings(self):
        gate = ShortSafetyGate(
            ShortSafetyConfig(
                require_locate=True,
                require_ssr_state=True,
                require_borrow_data=True,
                require_spread_data=True,
                min_avg_bar_volume=5_000.0,
            )
        )
        decision = gate.evaluate(
            "TSLA",
            now=datetime.now(ZoneInfo("UTC")),
            avg_bar_volume=1_000.0,
            action="SELL",
            enforce_timing=False,
            event_risk="HIGH",
            event_risk_reason="earnings:2026-03-10",
            short_borrow_fee_bps=0.0,
            short_borrow_source="unknown:ibkr_socket_api",
            locate_status="AVAILABLE",
            ssr_status="UNKNOWN",
            spread_bps=None,
            has_uptick_data=None,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("borrow_data_unknown", decision.blocked_reasons)
        self.assertIn("ssr_unknown", decision.blocked_reasons)
        self.assertIn("event_window_block", decision.blocked_reasons)
        self.assertIn("liquidity_below_min", decision.blocked_reasons)

    def test_compute_engine_signal_replays_shared_signal_kernel(self):
        base = datetime(2026, 3, 3, 9, 30, tzinfo=ZoneInfo("UTC"))
        bars = []
        price = 100.0
        for i in range(80):
            price += 0.25 if i % 7 else -0.5
            bars.append(
                _Bar(
                    open=price - 0.1,
                    high=price + 0.4,
                    low=price - 0.4,
                    close=price,
                    volume=20_000 + i * 50,
                    end_time=base + timedelta(minutes=5 * i),
                )
            )

        row = compute_engine_signal_for_symbol(
            symbol="AAPL",
            md=_FakeMD(bars),
            cfg=StrategyConfig(),
            gate=_FakeGate(),
            bars_need=80,
            tail_bars=30,
        )

        self.assertIsNotNone(row)
        self.assertIn("channel", row)
        self.assertIn("risk_snapshot", row)
        self.assertGreaterEqual(float(row["stability"]), 0.0)
        self.assertGreater(float(row["stop_price"]), 0.0)


if __name__ == "__main__":
    unittest.main()
