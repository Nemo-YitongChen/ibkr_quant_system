from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.common.storage import Storage
from src.ibkr.fills import FillProcessor
from src.ibkr.investment_orders import InvestmentOrderService
from src.tools.review_investment_execution import build_investment_execution_report


class InvestmentExecutionReportingTests(unittest.TestCase):
    def test_investment_order_service_persists_ibkr_health_events(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            service = InvestmentOrderService(
                FakeIB(),
                "DUQ152001",
                storage,
                market="US",
                portfolio_id="US:test",
            )
            service._on_error(-1, 322, "Maximum number of account summary requests exceeded", None)

            rows = list(
                storage._conn().execute(
                    "SELECT kind, value, portfolio_id, system_kind, details FROM risk_events WHERE kind='IBKR_HEALTH_EVENT'"
                )
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], "IBKR_HEALTH_EVENT")
            self.assertEqual(int(rows[0][1]), 322)
            self.assertEqual(rows[0][2], "US:test")
            self.assertEqual(rows[0][3], "investment_execution")
            self.assertIn("Maximum number of account summary requests exceeded", rows[0][4])

    def test_build_investment_execution_report_aggregates_orders_fills_and_commission(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))

            storage.insert_investment_execution_run(
                {
                    "run_id": "HK-exec-0",
                    "ts": "2026-03-12T06:00:00+00:00",
                    "market": "HK",
                    "portfolio_id": "HK:test",
                    "account_id": "DUQ152001",
                    "report_dir": "reports/demo",
                    "submitted": 1,
                    "order_count": 1,
                    "order_value": 29160.0,
                    "broker_equity": 1_001_197.08,
                    "broker_cash": 1_000_331.85,
                    "target_equity": 30_000.0,
                    "details": "{\"summary\":{\"gap_symbols\":1,\"gap_notional\":29160.0}}",
                }
            )
            storage.insert_investment_execution_order(
                {
                    "run_id": "HK-exec-0",
                    "ts": "2026-03-12T06:00:00+00:00",
                    "market": "HK",
                    "portfolio_id": "HK:test",
                    "symbol": "0883.HK",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 1000.0,
                    "delta_qty": 1000.0,
                    "ref_price": 29.16,
                    "target_weight": 1.0,
                    "order_value": 29160.0,
                    "order_type": "MKT",
                    "broker_order_id": 5,
                    "status": "Filled",
                    "reason": "rebalance_up_min_lot_override",
                    "details": "{}",
                }
            )
            storage.insert_investment_execution_run(
                {
                    "run_id": "HK-exec-1",
                    "ts": "2026-03-12T07:20:25+00:00",
                    "market": "HK",
                    "portfolio_id": "HK:test",
                    "account_id": "DUQ152001",
                    "report_dir": "reports/demo",
                    "submitted": 1,
                    "order_count": 1,
                    "order_value": 29160.0,
                    "broker_equity": 1_001_197.08,
                    "broker_cash": 1_000_331.85,
                    "target_equity": 30_000.0,
                    "details": "{\"summary\":{\"gap_symbols\":0,\"gap_notional\":0.0}}",
                }
            )
            storage.insert_investment_execution_order(
                {
                    "run_id": "HK-exec-1",
                    "ts": "2026-03-12T07:20:25+00:00",
                    "market": "HK",
                    "portfolio_id": "HK:test",
                    "symbol": "0883.HK",
                    "action": "SELL",
                    "current_qty": 1000.0,
                    "target_qty": 0.0,
                    "delta_qty": 1000.0,
                    "ref_price": 29.16,
                    "target_weight": 0.0,
                    "order_value": 29160.0,
                    "order_type": "MKT",
                    "broker_order_id": 5,
                    "status": "Filled",
                    "reason": "rebalance_down_min_lot_override",
                    "details": "{}",
                }
            )
            storage.insert_order(
                {
                    "ts": "2026-03-12T07:20:25+00:00",
                    "account_id": "DUQ152001",
                    "symbol": "0883.HK",
                    "exchange": "SEHK",
                    "currency": "HKD",
                    "action": "SELL",
                    "qty": 1000.0,
                    "order_type": "MKT",
                    "order_id": 5,
                    "parent_id": 0,
                    "status": "Filled",
                    "portfolio_id": "HK:test",
                    "system_kind": "investment_execution",
                    "execution_run_id": "HK-exec-1",
                    "details": "{}",
                }
            )
            storage.insert_fill(
                {
                    "ts": "2026-03-12T07:20:27+00:00",
                    "order_id": 5,
                    "exec_id": "exec_1",
                    "symbol": "883",
                    "action": "SELL",
                    "qty": 1000.0,
                    "price": 29.16,
                    "pnl": 120.0,
                    "actual_slippage_bps": -6.8,
                    "slippage_bps_deviation": -6.8,
                    "portfolio_id": "HK:test",
                    "system_kind": "investment_execution",
                    "execution_run_id": "HK-exec-1",
                }
            )
            storage.insert_risk_event(
                "COMMISSION",
                54.1591,
                "execId=exec_1 symbol=0883.HK",
                ts="2026-03-12T07:20:28+00:00",
                symbol="0883.HK",
                order_id=5,
                exec_id="exec_1",
                portfolio_id="HK:test",
                system_kind="investment_execution",
                execution_run_id="HK-exec-1",
            )
            storage.insert_investment_broker_position(
                {
                    "run_id": "HK-exec-1",
                    "ts": "2026-03-12T07:20:29+00:00",
                    "market": "HK",
                    "portfolio_id": "HK:test",
                    "symbol": "0883.HK",
                    "qty": 0.0,
                    "avg_cost": 29.17,
                    "market_price": 28.88,
                    "market_value": 0.0,
                    "weight": 0.0,
                    "source": "after",
                    "details": "{}",
                }
            )

            report = build_investment_execution_report(
                str(db_path),
                market="HK",
                days=0,
                portfolio_id="HK:test",
            )

            summary = report["summary"]
            self.assertEqual(summary["execution_run_rows"], 2)
            self.assertEqual(summary["submitted_order_rows"], 2)
            self.assertEqual(summary["filled_order_rows"], 2)
            self.assertEqual(summary["filled_with_audit_rows"], 1)
            self.assertEqual(summary["fill_audit_gap_rows"], 1)
            self.assertAlmostEqual(summary["fill_rate_status"], 1.0, places=6)
            self.assertAlmostEqual(summary["fill_rate_audit"], 0.5, places=6)
            self.assertEqual(summary["fill_rows"], 1)
            self.assertAlmostEqual(summary["commission_total"], 54.1591, places=4)
            self.assertAlmostEqual(summary["realized_net_pnl"], 65.8409, places=4)
            self.assertEqual(summary["weekly_rows"], 1)
            self.assertEqual(len(report["weekly_rows"]), 1)
            self.assertEqual(report["weekly_rows"][0]["filled_order_rows"], 2)
            self.assertEqual(report["weekly_rows"][0]["filled_with_audit_rows"], 1)
            self.assertEqual(report["weekly_rows"][0]["submitted_order_rows"], 2)
            self.assertAlmostEqual(report["weekly_rows"][0]["fill_rate_status"], 1.0, places=6)
            self.assertAlmostEqual(report["weekly_rows"][0]["fill_rate_audit"], 0.5, places=6)
            self.assertAlmostEqual(report["weekly_rows"][0]["commission_total"], 54.1591, places=4)
            self.assertEqual(report["order_rows"][0]["symbol"], "0883.HK")
            self.assertEqual(report["fill_rows"][0]["symbol"], "0883.HK")
            self.assertEqual(report["order_rows"][0]["fill_count"], 0)
            self.assertEqual(report["order_rows"][1]["fill_count"], 1)

    def test_fill_processor_normalizes_hk_exec_symbol_from_order_meta(self):
        normalized = FillProcessor._normalize_exec_symbol(
            "883",
            order_meta={"symbol": "0883.HK", "exchange": "SEHK", "currency": "HKD"},
            exchange="SEHK",
            currency="HKD",
        )
        self.assertEqual(normalized, "0883.HK")

    def test_storage_updates_only_latest_duplicate_order_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_order(
                {
                    "ts": "2026-03-12T06:00:00+00:00",
                    "symbol": "0883.HK",
                    "order_id": 5,
                    "status": "CREATED",
                }
            )
            storage.insert_order(
                {
                    "ts": "2026-03-12T07:00:00+00:00",
                    "symbol": "0883.HK",
                    "order_id": 5,
                    "status": "CREATED",
                }
            )
            storage.update_order_status(5, "Filled")

            rows = list(storage._conn().execute("SELECT ts, status FROM orders WHERE order_id=5 ORDER BY ts ASC"))
            self.assertEqual(rows[0][1], "CREATED")
            self.assertEqual(rows[1][1], "Filled")

    def test_execution_report_counts_blocked_opportunity_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_investment_execution_run(
                {
                    "run_id": "XETRA-exec-0",
                    "ts": "2026-03-12T07:00:00+00:00",
                    "market": "XETRA",
                    "portfolio_id": "XETRA:test",
                    "account_id": "DUQ152001",
                    "report_dir": "reports/demo",
                    "submitted": 0,
                    "order_count": 0,
                    "order_value": 0.0,
                    "broker_equity": 100000.0,
                    "broker_cash": 100000.0,
                    "target_equity": 85000.0,
                    "details": "{\"summary\":{\"gap_symbols\":1,\"gap_notional\":5369.92}}",
                }
            )
            storage.insert_investment_execution_order(
                {
                    "run_id": "XETRA-exec-0",
                    "ts": "2026-03-12T07:00:00+00:00",
                    "market": "XETRA",
                    "portfolio_id": "XETRA:test",
                    "symbol": "RWE.DE",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 97.0,
                    "delta_qty": 97.0,
                    "ref_price": 55.36,
                    "target_weight": 0.18,
                    "order_value": 5369.92,
                    "order_type": "LMT",
                    "broker_order_id": 0,
                    "status": "BLOCKED_OPPORTUNITY",
                    "reason": "rebalance_up|opportunity_wait_event",
                    "execution_intent_json": "{\"opportunity_status\":\"WAIT_EVENT\",\"opportunity_reason\":\"event risk\"}",
                    "details": "{\"opportunity_status\":\"WAIT_EVENT\",\"opportunity_reason\":\"event risk\"}",
                }
            )
            report = build_investment_execution_report(
                str(db_path),
                market="XETRA",
                days=0,
                portfolio_id="XETRA:test",
            )
            summary = report["summary"]
            self.assertEqual(summary["blocked_opportunity_rows"], 1)
            self.assertEqual(summary["opportunity_status_breakdown"], "WAIT_EVENT:1")
            self.assertEqual(report["order_rows"][0]["opportunity_status"], "WAIT_EVENT")

    def test_execution_report_filters_cross_market_broker_position_snapshots(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_investment_execution_run(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:test",
                    "account_id": "DUQ152001",
                    "report_dir": "reports/demo",
                    "submitted": 0,
                    "order_count": 0,
                    "order_value": 0.0,
                    "broker_equity": 100000.0,
                    "broker_cash": 95000.0,
                    "target_equity": 85000.0,
                    "details": "{}",
                }
            )
            storage.insert_investment_broker_position(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:01+00:00",
                    "market": "US",
                    "portfolio_id": "US:test",
                    "symbol": "SPY",
                    "qty": 10.0,
                    "avg_cost": 510.0,
                    "market_price": 512.0,
                    "market_value": 5120.0,
                    "weight": 0.0512,
                    "source": "after",
                    "details": "{}",
                }
            )
            storage.insert_investment_broker_position(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:02+00:00",
                    "market": "US",
                    "portfolio_id": "US:test",
                    "symbol": "0883.HK",
                    "qty": 1000.0,
                    "avg_cost": 29.17,
                    "market_price": 28.88,
                    "market_value": 28880.0,
                    "weight": 0.2888,
                    "source": "after",
                    "details": "{}",
                }
            )

            report = build_investment_execution_report(
                str(db_path),
                market="US",
                days=0,
                portfolio_id="US:test",
            )

            self.assertEqual([row["symbol"] for row in report["latest_broker_positions"]], ["SPY"])
            self.assertEqual(report["summary"]["latest_broker_holdings_count"], 1)


if __name__ == "__main__":
    unittest.main()
