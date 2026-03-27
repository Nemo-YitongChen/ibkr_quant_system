from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from src.common.storage import Storage
from src.ibkr.fills import FillProcessor
from src.portfolio.allocator import AllocatorConfig, PortfolioAllocator
from src.risk.model import PreTradeRiskSnapshot
from src.risk.ledger import Ledger
from src.risk.short_data import fetch_remote_short_data
from src.risk.short_safety import ShortSafetyConfig, ShortSafetyGate
from src.strategies.engine_strategy import EngineStrategy, StrategyConfig, TradeSignal
from src.tools.paper_kpi_report import build_paper_kpi_report


class _FakeAccount:
    def __init__(self, netliq: float):
        self._netliq = netliq

    def get_netliq(self) -> float:
        return float(self._netliq)


class _FakeIB:
    def positions(self):
        return []


class _FakeGate:
    def __init__(self):
        self.closed = []

    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None:
        self.closed.append((trade_pnl, details))


class _FakeEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self


class _FakeIBWithEvents(_FakeIB):
    def __init__(self):
        self.execDetailsEvent = _FakeEvent()
        self.commissionReportEvent = _FakeEvent()


class _FakeOrders:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.placed = []

    def place_bracket(self, **kwargs):
        self.placed.append(kwargs)
        return ()


@dataclass
class _FakeRunner:
    watched: list

    def watch_entry_order(self, order_id: int, meta=None):
        self.watched.append((order_id, meta))


class PaperExecutionReportingTests(unittest.TestCase):
    def test_allocator_paper_min_qty_floor_preserves_sample_orders(self):
        allocator = PortfolioAllocator(
            _FakeIB(),
            _FakeAccount(50_000.0),
            AllocatorConfig(
                risk_per_trade=0.002,
                max_open_positions=8,
                max_gross_leverage=1.2,
                enable_min_order_qty_floor=True,
                min_order_qty=1.0,
            ),
        )
        snapshot = PreTradeRiskSnapshot(
            symbol="AAPL",
            action="BUY",
            entry_price=100.0,
            atr_stop=1.0,
            slippage_bps=5.0,
            gap_addon_pct=0.002,
            liquidity_haircut=0.75,
            slippage_addon_price=0.05,
            gap_addon_price=0.20,
            liquidity_addon_price=0.10,
            short_addon_price=0.0,
            stop_distance=2.0,
            take_profit_distance=3.6,
            stop_price=98.0,
            take_profit_price=103.6,
            event_risk="NONE",
            event_risk_reason="",
            short_borrow_fee_bps=0.0,
            short_borrow_source="",
            allowed=False,
            block_reasons=["liquidity_too_thin"],
            atr_pct=0.01,
            avg_bar_volume=100.0,
            risk_per_share=2.0,
            expected_fill_price=100.05,
        )

        qty = allocator.size_qty(requested_qty=1.0, entry_price=100.0, risk_snapshot=snapshot)
        self.assertEqual(qty, 1.0)

    def test_paper_kpi_report_groups_pipeline_by_market_and_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))

            storage.insert_signal_audit(
                {
                    "ts": "2026-03-04T01:00:00+00:00",
                    "symbol": "AAPL",
                    "should_trade": 1,
                    "risk_allowed": 1,
                    "action": "BUY",
                    "reason": "ALWAYS_ON|HIST|PURE_SHORT|thr=0.45|qmul=0.65",
                    "channel": "PURE_SHORT",
                }
            )
            storage.insert_signal_audit(
                {
                    "ts": "2026-03-04T01:00:00+00:00",
                    "symbol": "0700.HK",
                    "should_trade": 1,
                    "risk_allowed": 1,
                    "action": "SELL",
                    "reason": "ALWAYS_ON|REALTIME|PURE_SHORT|thr=0.45|qmul=1.00",
                    "channel": "PURE_SHORT",
                }
            )

            storage.insert_order(
                {
                    "ts": "2026-03-04T01:01:00+00:00",
                    "account_id": "DU123",
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "action": "BUY",
                    "qty": 1,
                    "order_type": "LMT",
                    "order_id": 1001,
                    "parent_id": 0,
                    "status": "Filled",
                    "details": json.dumps(
                        {
                            "leg": "parent",
                            "signal_tag": "ALWAYS_ON",
                            "signal_source": "HIST",
                            "signal_reason": "PURE_SHORT total=0.50",
                        }
                    ),
                }
            )
            storage.insert_fill(
                {
                    "ts": "2026-03-04T01:02:00+00:00",
                    "order_id": 1001,
                    "exec_id": "exec_aapl",
                    "symbol": "AAPL",
                    "action": "BOT",
                    "qty": 1.0,
                    "price": 100.10,
                    "pnl": 12.5,
                    "actual_slippage_bps": 7.5,
                    "slippage_bps_deviation": 2.0,
                }
            )
            storage.insert_risk_event(
                "COMMISSION",
                0.35,
                "execId=exec_aapl symbol=AAPL",
                ts="2026-03-04T01:02:01+00:00",
                symbol="AAPL",
                exec_id="exec_aapl",
            )
            storage.insert_risk_event(
                "SHORT_SAFETY_SHADOW_BLOCK",
                1.0,
                "symbol=0700.HK tag=ALWAYS_ON source=REALTIME channel=PURE_SHORT reasons=ssr_unknown",
                ts="2026-03-04T01:03:00+00:00",
                symbol="0700.HK",
            )
            storage.insert_risk_event(
                "SOURCE_EXEC_BLOCK",
                0.0,
                "symbol=0700.HK tag=ALWAYS_ON source=HIST channel=PURE_SHORT allowed_sources=REALTIME",
                ts="2026-03-04T01:02:30+00:00",
                symbol="0700.HK",
            )
            storage.insert_risk_event(
                "PRETRADE_RISK_BLOCK",
                1.0,
                "symbol=AAPL tag=ALWAYS_ON source=HIST channel=PURE_SHORT reasons=liquidity_too_thin",
                ts="2026-03-04T01:00:30+00:00",
                symbol="AAPL",
            )
            storage.insert_risk_event(
                "ALLOCATOR_QTY_ZERO",
                1.0,
                "symbol=0700.HK tag=ALWAYS_ON source=REALTIME channel=PURE_SHORT requested_qty=0.500 sized_qty=0.000",
                ts="2026-03-04T01:03:01+00:00",
                symbol="0700.HK",
            )

            report = build_paper_kpi_report(str(db_path), days=0)
            rows = {(row["market"], row["source"]): row for row in report["pipeline_by_market_source"]}

            self.assertIn(("US", "HIST"), rows)
            self.assertIn(("HK", "REALTIME"), rows)
            self.assertEqual(rows[("US", "HIST")]["parent_order_rows"], 1)
            self.assertEqual(rows[("US", "HIST")]["entry_fill_rows"], 1)
            self.assertAlmostEqual(rows[("US", "HIST")]["realized_net_pnl"], 12.15)
            self.assertEqual(rows[("US", "HIST")]["pretrade_risk_block_rows"], 1)
            self.assertEqual(rows[("HK", "REALTIME")]["short_shadow_block_rows"], 1)
            self.assertEqual(rows[("HK", "REALTIME")]["allocator_qty_zero_rows"], 1)
            self.assertEqual(rows[("HK", "HIST")]["source_exec_block_rows"], 1)

    def test_paper_kpi_report_supports_since_filter(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))

            storage.insert_signal_audit(
                {
                    "ts": "2026-03-03T23:59:00+00:00",
                    "symbol": "AAPL",
                    "should_trade": 1,
                    "risk_allowed": 1,
                    "action": "BUY",
                    "reason": "ALWAYS_ON|REALTIME|PURE_SHORT|thr=0.45|qmul=1.00",
                    "channel": "PURE_SHORT",
                }
            )
            storage.insert_signal_audit(
                {
                    "ts": "2026-03-04T00:01:00+00:00",
                    "symbol": "AAPL",
                    "should_trade": 1,
                    "risk_allowed": 1,
                    "action": "BUY",
                    "reason": "ALWAYS_ON|REALTIME|PURE_SHORT|thr=0.45|qmul=1.00",
                    "channel": "PURE_SHORT",
                }
            )

            report = build_paper_kpi_report(str(db_path), days=0, since="2026-03-04T00:00:00+00:00")
            self.assertEqual(report["overview"]["since_utc"], "2026-03-04T00:00:00+00:00")
            self.assertEqual(report["overview"]["signal_rows"], 1)

    @patch("src.risk.short_data.requests.get")
    def test_fetch_remote_short_data_supports_provider_sources(self, mock_get):
        class _FakeResponse:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        def _fake_get(url, headers=None, params=None, timeout=None):
            if "finance.yahoo.com" in url or "finance/quote" in url:
                return _FakeResponse(
                    {
                        "quoteResponse": {
                            "result": [
                                {"symbol": "AAPL", "bid": 100.0, "ask": 100.2, "marketState": "REGULAR"},
                            ]
                        }
                    }
                )
            if "iborrowdesk.com" in url:
                return _FakeResponse({"daily": [{"fee": 0.27, "available": 150000, "date": "2026-03-04"}]})
            raise AssertionError(url)

        mock_get.side_effect = _fake_get
        rows = fetch_remote_short_data(
            ["AAPL"],
            [
                {"enabled": True, "name": "iborrowdesk_borrow", "provider": "iborrowdesk"},
                {"enabled": True, "name": "yahoo_quote_spread", "provider": "yahoo_quote"},
            ],
            market="US",
        )
        self.assertIn("AAPL", rows)
        self.assertAlmostEqual(rows["AAPL"].borrow_fee_bps, 27.0)
        self.assertEqual(rows["AAPL"].borrow_source, "iborrowdesk_borrow")
        self.assertEqual(rows["AAPL"].locate_status, "AVAILABLE")
        self.assertEqual(rows["AAPL"].spread_source, "yahoo_quote_spread")
        self.assertIsNotNone(rows["AAPL"].spread_bps)

    def test_fill_processor_normalizes_bot_sld_actions_for_pnl(self):
        ledger = Ledger()
        self.assertEqual(ledger.on_fill("AAPL", "BOT", 1.0, 100.0), 0.0)
        self.assertEqual(ledger.on_fill("AAPL", "SLD", 1.0, 101.0), 1.0)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_order(
                {
                    "ts": "2026-03-04T01:00:00+00:00",
                    "account_id": "DU123",
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "action": "BUY",
                    "qty": 1,
                    "order_type": "LMT",
                    "order_id": 3001,
                    "parent_id": 0,
                    "status": "Filled",
                    "details": json.dumps({"expected_price": 100.0, "expected_slippage_bps": 5.0, "risk_snapshot": {}}),
                }
            )
            ib = _FakeIBWithEvents()
            processor = FillProcessor(ib, storage, _FakeGate())

            class _Execution:
                orderId = 3001
                execId = "exec_norm"
                side = "BOT"
                shares = 1
                price = 100.1

            class _Contract:
                symbol = "AAPL"

            class _Fill:
                contract = _Contract()
                execution = _Execution()

            processor._on_exec_details(None, _Fill())
            rows = storage._conn().execute("select action, actual_slippage_bps from fills").fetchall()
            self.assertEqual(rows[0][0], "BUY")
            self.assertGreater(float(rows[0][1]), 0.0)

    def test_engine_strategy_blocks_hist_execution_and_pretrade_risk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            orders = _FakeOrders(storage)
            strategy = EngineStrategy(
                orders=orders,
                gate=None,
                cfg=StrategyConfig(
                    runtime_mode="paper",
                    paper_allowed_execution_sources=["REALTIME"],
                    enforce_pretrade_risk_gate=True,
                ),
            )
            runner = _FakeRunner(watched=[])

            blocked_snapshot = PreTradeRiskSnapshot(
                symbol="AAPL",
                action="BUY",
                entry_price=100.0,
                atr_stop=1.0,
                slippage_bps=5.0,
                gap_addon_pct=0.002,
                liquidity_haircut=0.5,
                slippage_addon_price=0.05,
                gap_addon_price=0.2,
                liquidity_addon_price=0.1,
                short_addon_price=0.0,
                stop_distance=1.35,
                take_profit_distance=2.43,
                stop_price=98.65,
                take_profit_price=102.43,
                event_risk="NONE",
                event_risk_reason="",
                short_borrow_fee_bps=0.0,
                short_borrow_source="",
                allowed=False,
                block_reasons=["liquidity_too_thin"],
                atr_pct=0.01,
                avg_bar_volume=100.0,
                risk_per_share=1.35,
                expected_fill_price=100.05,
            )
            allowed_snapshot = PreTradeRiskSnapshot(
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
            sig = TradeSignal(
                should_trade=True,
                action="BUY",
                qty=1.0,
                entry_price=100.0,
                total_sig=0.7,
                short_sig=0.6,
                mid_scale=0.7,
                reason="test",
                channel="PURE_SHORT",
                audit_tag="ALWAYS_ON",
                audit_source="HIST",
                risk_snapshot=allowed_snapshot,
            )
            strategy.execute("AAPL", sig, runner)

            self.assertEqual(len(orders.placed), 0)
            rows = storage._conn().execute("select kind from risk_events order by id asc").fetchall()
            self.assertEqual(rows[0][0], "SOURCE_EXEC_BLOCK")

            realtime_sig = TradeSignal(
                should_trade=True,
                action="BUY",
                qty=1.0,
                entry_price=100.0,
                total_sig=0.7,
                short_sig=0.6,
                mid_scale=0.7,
                reason="test",
                channel="PURE_SHORT",
                audit_tag="ALWAYS_ON",
                audit_source="REALTIME",
                risk_snapshot=blocked_snapshot,
            )
            strategy.execute("AAPL", realtime_sig, runner)
            rows = storage._conn().execute("select kind from risk_events order by id asc").fetchall()
            self.assertEqual(rows[1][0], "PRETRADE_RISK_BLOCK")

    def test_engine_strategy_skips_shadow_sample_for_non_executable_hist_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            orders = _FakeOrders(storage)
            strategy = EngineStrategy(
                orders=orders,
                gate=None,
                cfg=StrategyConfig(
                    runtime_mode="paper",
                    paper_allowed_execution_sources=["REALTIME"],
                    enforce_pretrade_risk_gate=True,
                ),
                short_safety_gate=ShortSafetyGate(
                    ShortSafetyConfig(
                        shadow_mode=True,
                        require_locate=True,
                        require_borrow_data=True,
                        require_ssr_state=True,
                        require_spread_data=True,
                    )
                ),
            )
            runner = _FakeRunner(watched=[])
            allowed_snapshot = PreTradeRiskSnapshot(
                symbol="AAPL",
                action="SELL",
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
                expected_fill_price=99.95,
            )
            sig = TradeSignal(
                should_trade=True,
                action="SELL",
                qty=1.0,
                entry_price=100.0,
                total_sig=-0.7,
                short_sig=-0.6,
                mid_scale=0.7,
                reason="test",
                channel="PURE_SHORT",
                audit_tag="ALWAYS_ON",
                audit_source="HIST",
                risk_snapshot=allowed_snapshot,
            )
            strategy.execute("AAPL", sig, runner)
            rows = [row[0] for row in storage._conn().execute("select kind from risk_events order by id asc").fetchall()]
            self.assertEqual(rows, ["SOURCE_EXEC_BLOCK"])


if __name__ == "__main__":
    unittest.main()
