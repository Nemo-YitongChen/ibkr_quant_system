from __future__ import annotations

import unittest

from src.tools.reconcile_investment_broker import build_reconciliation_rows


class InvestmentReconciliationTests(unittest.TestCase):
    def test_build_reconciliation_rows_detects_local_and_broker_only(self):
        rows = build_reconciliation_rows(
            [
                {"symbol": "0016.HK", "qty": 64.0, "weight": 0.09, "market_value": 8460.8},
                {"symbol": "0005.HK", "qty": 10.0, "weight": 0.01, "market_value": 500.0},
            ],
            [
                {"symbol": "0883.HK", "qty": 1000.0, "weight": 0.03, "market_value": 29300.0},
                {"symbol": "0005.HK", "qty": 10.0, "weight": 0.01, "market_value": 500.0},
            ],
        )
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertEqual(by_symbol["0016.HK"]["status"], "ONLY_LOCAL")
        self.assertEqual(by_symbol["0883.HK"]["status"], "ONLY_BROKER")
        self.assertEqual(by_symbol["0005.HK"]["status"], "MATCH")

    def test_build_reconciliation_rows_detects_qty_mismatch(self):
        rows = build_reconciliation_rows(
            [{"symbol": "0883.HK", "qty": 800.0, "weight": 0.02, "market_value": 24000.0}],
            [{"symbol": "0883.HK", "qty": 1000.0, "weight": 0.03, "market_value": 29300.0}],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "QTY_MISMATCH")
        self.assertEqual(rows[0]["qty_diff"], 200.0)


if __name__ == "__main__":
    unittest.main()
