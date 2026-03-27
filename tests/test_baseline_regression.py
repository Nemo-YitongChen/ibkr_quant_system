from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.tools.review_baseline_regression import _build_snapshot, _compare_snapshots


class BaselineRegressionTests(unittest.TestCase):
    def test_snapshot_and_compare_work_for_report_dir(self):
        with TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,score,action",
                        "AAA,0.5,ACCUMULATE",
                        "BBB,0.4,HOLD",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult",
                        "AAA,ACCUMULATE,0.8",
                        "BBB,HOLD,0.6",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_backtest.csv").write_text(
                "\n".join(
                    [
                        "symbol,bt_avg_ret_30d,bt_avg_ret_60d,bt_avg_ret_90d,bt_hit_rate_30d,bt_hit_rate_60d,bt_hit_rate_90d",
                        "AAA,0.1,0.15,0.2,0.6,0.7,0.8",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_paper_summary.json").write_text(json.dumps({"executed": True, "equity_after": 101000, "target_invested_weight": 0.55}), encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(json.dumps({"order_count": 1, "blocked_order_count": 0, "gap_symbols": 1, "gap_notional": 500.0, "target_invested_weight": 0.55}), encoding="utf-8")
            (report_dir / "investment_opportunity_summary.json").write_text(json.dumps({"entry_now_count": 1, "near_entry_count": 0, "wait_count": 1}), encoding="utf-8")
            (report_dir / "investment_report.md").write_text("# report", encoding="utf-8")

            current = _build_snapshot(report_dir, "US", "US:test")
            baseline = dict(current)
            baseline["avg_score_top10"] = 0.1
            comparison = _compare_snapshots(current, baseline)

            self.assertEqual(current["candidate_count"], 2)
            self.assertEqual(current["execution_order_count"], 1)
            self.assertEqual(len(comparison["deltas"]), 18)
            self.assertTrue(comparison["report_changed"] is False)


if __name__ == "__main__":
    unittest.main()
