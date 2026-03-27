from __future__ import annotations

import unittest
from tempfile import NamedTemporaryFile

from src.analysis.tracking import build_and_persist_analysis_chain
from src.common.storage import Storage


class InvestmentAnalysisTrackingTests(unittest.TestCase):
    def test_analysis_chain_records_watch_entry_add_exit_and_remove_events(self):
        with NamedTemporaryFile(suffix=".db") as tmp:
            storage = Storage(tmp.name)

            build_and_persist_analysis_chain(
                storage,
                market="US",
                portfolio_id="US:watchlist",
                report_dir="reports/watchlist",
                analysis_run_id="run-1",
                observed_ts="2026-03-12T08:00:00+00:00",
                ranked_rows=[
                    {"symbol": "AAA", "action": "WATCH", "score": 0.15, "regime_reason": "wait"},
                    {"symbol": "BBB", "action": "HOLD", "score": 0.42, "regime_reason": "hold core"},
                    {"symbol": "CCC", "action": "WATCH", "score": 0.10, "regime_reason": "watch"},
                ],
                opportunity_rows=[],
                broker_positions={"BBB": {"qty": 10.0}},
            )

            build_and_persist_analysis_chain(
                storage,
                market="US",
                portfolio_id="US:watchlist",
                report_dir="reports/watchlist",
                analysis_run_id="run-2",
                observed_ts="2026-03-12T09:00:00+00:00",
                ranked_rows=[
                    {"symbol": "AAA", "action": "ACCUMULATE", "score": 0.82, "regime_reason": "trend resumed"},
                    {"symbol": "BBB", "action": "WATCH", "score": 0.18, "regime_reason": "back to watch"},
                    {"symbol": "CCC", "action": "REDUCE", "score": -0.15, "regime_reason": "thesis weak"},
                ],
                opportunity_rows=[
                    {"symbol": "AAA", "action": "ACCUMULATE", "entry_status": "ENTRY_NOW", "entry_reason": "pullback reached"},
                ],
                broker_positions={},
            )

            build_and_persist_analysis_chain(
                storage,
                market="US",
                portfolio_id="US:watchlist",
                report_dir="reports/watchlist",
                analysis_run_id="run-3",
                observed_ts="2026-03-12T10:00:00+00:00",
                ranked_rows=[
                    {"symbol": "AAA", "action": "ACCUMULATE", "score": 0.88, "regime_reason": "can add"},
                ],
                opportunity_rows=[
                    {"symbol": "AAA", "action": "ACCUMULATE", "entry_status": "ADD_ON_PULLBACK", "entry_reason": "scale in"},
                ],
                broker_positions={"AAA": {"qty": 5.0}},
            )

            state_map = storage.get_investment_analysis_state_map("US", portfolio_id="US:watchlist")
            self.assertEqual(state_map["AAA"]["status"], "ADD_READY")
            self.assertEqual(state_map["BBB"]["status"], "REMOVED_FROM_WATCH")
            self.assertEqual(state_map["CCC"]["status"], "REMOVED_FROM_WATCH")

            event_kinds = {
                row["event_kind"]
                for row in storage.get_recent_investment_analysis_events("US", portfolio_id="US:watchlist", limit=20)
            }
            self.assertIn("WATCH_TO_ENTRY", event_kinds)
            self.assertIn("TO_ADD", event_kinds)
            self.assertIn("EXIT_TO_WATCH", event_kinds)
            self.assertIn("CANCEL_WATCH", event_kinds)
            self.assertIn("REMOVE_FROM_WATCH", event_kinds)


if __name__ == "__main__":
    unittest.main()
