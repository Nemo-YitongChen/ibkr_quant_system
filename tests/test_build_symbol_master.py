from __future__ import annotations

import unittest
from unittest.mock import patch

from src.tools import build_symbol_master


class BuildSymbolMasterTests(unittest.TestCase):
    def test_universe_rows_include_broad_watchlists_and_direct_symbols(self):
        cfg = {
            "asset_class": "equity",
            "name": "asx_market_core",
            "seed_symbols": ["CBA.AX"],
            "symbol_master_symbols": ["BHP.AX"],
            "seed_watchlist_yaml": "config/watchlists/asx_top_quality.yaml",
            "report_watchlist_yaml": "config/watchlists/asx_top_quality.yaml",
            "symbol_master_watchlists": ["config/watchlists/asx_market_core.yaml"],
        }

        with patch.object(build_symbol_master, "load_market_universe_config", return_value=cfg):
            with patch.object(
                build_symbol_master,
                "load_watchlist_symbols",
                side_effect=[
                    ["CSL.AX"],
                    ["WBC.AX"],
                    ["TLS.AX", "CBA.AX"],
                ],
            ):
                rows = build_symbol_master._universe_rows("ASX")

        symbols = [row[1] for row in rows]
        self.assertEqual(len(symbols), len(set(symbols)))
        self.assertIn("CBA.AX", symbols)
        self.assertIn("BHP.AX", symbols)
        self.assertIn("CSL.AX", symbols)
        self.assertIn("WBC.AX", symbols)
        self.assertIn("TLS.AX", symbols)


if __name__ == "__main__":
    unittest.main()
