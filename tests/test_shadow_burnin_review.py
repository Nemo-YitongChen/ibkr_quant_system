from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.storage import Storage
from src.tools.review_shadow_burnin import build_shadow_burnin_review, write_shadow_burnin_outputs


UTC = timezone.utc


class ShadowBurnInReviewTests(unittest.TestCase):
    def test_shadow_burnin_review_matches_shadow_events_to_short_entry_fills(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            storage = Storage(str(db_path))
            base = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

            storage.insert_order(
                {
                    "ts": base.isoformat(),
                    "account_id": "DU123",
                    "symbol": "TSLA",
                    "exchange": "SMART",
                    "currency": "USD",
                    "action": "SELL",
                    "qty": 10,
                    "order_type": "LMT",
                    "order_id": 1001,
                    "parent_id": 0,
                    "status": "FILLED",
                    "details": json.dumps({"leg": "parent"}),
                }
            )
            storage.insert_fill(
                {
                    "ts": (base + timedelta(minutes=2)).isoformat(),
                    "order_id": 1001,
                    "exec_id": "fill_tsla",
                    "symbol": "TSLA",
                    "action": "SLD",
                    "qty": 10.0,
                    "price": 100.0,
                    "pnl": -40.0,
                    "expected_price": 99.5,
                    "expected_slippage_bps": 8.0,
                    "actual_slippage_bps": 20.0,
                    "slippage_bps_deviation": 12.0,
                    "event_risk_reason": "macro_calendar_high",
                    "short_borrow_source": "feed_a",
                    "risk_snapshot_json": json.dumps({"risk_per_share": 2.0, "avg_bar_volume": 4000}),
                }
            )

            storage.insert_order(
                {
                    "ts": (base + timedelta(minutes=10)).isoformat(),
                    "account_id": "DU123",
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "action": "SELL",
                    "qty": 5,
                    "order_type": "LMT",
                    "order_id": 1002,
                    "parent_id": 0,
                    "status": "FILLED",
                    "details": json.dumps({"leg": "parent"}),
                }
            )
            storage.insert_fill(
                {
                    "ts": (base + timedelta(minutes=11)).isoformat(),
                    "order_id": 1002,
                    "exec_id": "fill_aapl",
                    "symbol": "AAPL",
                    "action": "SLD",
                    "qty": 5.0,
                    "price": 200.0,
                    "pnl": 15.0,
                    "expected_price": 199.8,
                    "expected_slippage_bps": 4.0,
                    "actual_slippage_bps": 5.0,
                    "slippage_bps_deviation": 1.0,
                    "event_risk_reason": "",
                    "short_borrow_source": "feed_a",
                    "risk_snapshot_json": json.dumps({"risk_per_share": 1.0, "avg_bar_volume": 12000}),
                }
            )

            storage.insert_risk_event(
                "SHORT_SAFETY_SHADOW_BLOCK",
                2.0,
                "symbol=TSLA reasons=spread_too_wide,borrow_data_unknown",
                ts=base.isoformat(),
                symbol="TSLA",
                expected_price=99.5,
                expected_slippage_bps=8.0,
                event_risk_reason="macro_calendar_high",
                short_borrow_source="feed_a",
                risk_snapshot_json=json.dumps({"risk_per_share": 2.0, "avg_bar_volume": 4000, "liquidity_haircut": 0.5}),
            )
            storage.insert_risk_event(
                "SHORT_SAFETY_SHADOW_BLOCK",
                1.5,
                "symbol=MSFT reasons=locate_unknown",
                ts=(base + timedelta(minutes=30)).isoformat(),
                symbol="MSFT",
                expected_price=300.0,
                expected_slippage_bps=6.0,
                event_risk_reason="earnings:2026-03-10",
                short_borrow_source="feed_b",
                risk_snapshot_json=json.dumps({"risk_per_share": 1.5, "avg_bar_volume": 10000}),
            )
            storage.insert_risk_event(
                "EXECUTION_SLIPPAGE_BPS",
                20.0,
                "execId=fill_tsla symbol=TSLA",
                ts=(base + timedelta(minutes=2)).isoformat(),
                symbol="TSLA",
                order_id=1001,
                exec_id="fill_tsla",
                expected_price=99.5,
                actual_price=100.0,
                expected_slippage_bps=8.0,
                actual_slippage_bps=20.0,
                slippage_bps_deviation=12.0,
            )

            review = build_shadow_burnin_review(
                str(db_path),
                days=0,
                match_window_min=15,
                min_sample=1,
                slippage_delta_bps_threshold=5.0,
            )

            overview = review["overview"]
            self.assertEqual(overview["shadow_event_count"], 2)
            self.assertEqual(overview["matched_shadow_fill_count"], 1)
            self.assertEqual(overview["unmatched_shadow_event_count"], 1)
            self.assertEqual(overview["short_entry_fill_count"], 2)
            self.assertEqual(overview["control_short_fill_count"], 1)
            self.assertAlmostEqual(overview["slippage_delta_bps"], 15.0)
            self.assertEqual(overview["recommendation"]["status"], "CANDIDATE_FOR_HARD_BLOCK_REVIEW")

            reason_map = {row["blocked_reason"]: row for row in review["blocked_reason_summary"]}
            self.assertEqual(reason_map["spread_too_wide"]["matched_fill_count"], 1)
            self.assertEqual(reason_map["locate_unknown"]["matched_fill_count"], 0)

    def test_shadow_burnin_review_writes_expected_output_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "audit.db"
            out_dir = Path(tmpdir) / "shadow_review"
            storage = Storage(str(db_path))
            base = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

            storage.insert_order(
                {
                    "ts": base.isoformat(),
                    "account_id": "DU123",
                    "symbol": "TSLA",
                    "exchange": "SMART",
                    "currency": "USD",
                    "action": "SELL",
                    "qty": 1,
                    "order_type": "LMT",
                    "order_id": 2001,
                    "parent_id": 0,
                    "status": "FILLED",
                    "details": json.dumps({"leg": "parent"}),
                }
            )
            storage.insert_fill(
                {
                    "ts": (base + timedelta(minutes=1)).isoformat(),
                    "order_id": 2001,
                    "exec_id": "fill_1",
                    "symbol": "TSLA",
                    "action": "SLD",
                    "qty": 1.0,
                    "price": 100.0,
                    "pnl": -2.0,
                    "actual_slippage_bps": 9.0,
                    "slippage_bps_deviation": 4.0,
                }
            )
            storage.insert_risk_event(
                "SHORT_SAFETY_SHADOW_BLOCK",
                1.0,
                "symbol=TSLA reasons=spread_too_wide",
                ts=base.isoformat(),
                symbol="TSLA",
            )

            review = build_shadow_burnin_review(str(db_path), days=0, match_window_min=5, min_sample=1)
            outputs = write_shadow_burnin_outputs(str(out_dir), review)

            self.assertTrue(Path(outputs["summary_json"]).exists())
            self.assertTrue(Path(outputs["markdown"]).exists())
            self.assertTrue(Path(outputs["event_csv"]).exists())
            self.assertTrue(Path(outputs["fill_csv"]).exists())


if __name__ == "__main__":
    unittest.main()
