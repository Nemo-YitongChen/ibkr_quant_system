from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from src.enrichment.providers import EnrichmentProviders
from src.enrichment.yfinance_history import fetch_daily_bars
from src.ibkr.market_data import OHLCVBar


class EnrichmentProvidersTests(unittest.TestCase):
    def test_fetch_daily_bars_can_fallback_to_stale_cache_when_online_history_empty(self):
        stale_rows = [
            OHLCVBar(
                time=datetime(2026, 3, 20, tzinfo=timezone.utc),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000.0,
            ),
            OHLCVBar(
                time=datetime(2026, 3, 21, tzinfo=timezone.utc),
                open=100.5,
                high=102.0,
                low=100.0,
                close=101.2,
                volume=1200.0,
            ),
        ]

        class _FakeTicker:
            def history(self, **kwargs):
                return []

        fake_yf = types.SimpleNamespace(Ticker=lambda symbol: _FakeTicker())
        with patch.dict(sys.modules, {"yfinance": fake_yf}):
            with patch("src.enrichment.yfinance_history._read_history_cache", return_value=[]):
                with patch("src.enrichment.yfinance_history._read_stale_history_cache", return_value=stale_rows):
                    rows = fetch_daily_bars("AAPL", 30, allow_stale_cache=True)
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(float(rows[-1].close), 101.2, places=6)

    def test_normalize_yfinance_symbol_maps_us_share_class(self):
        self.assertEqual(EnrichmentProviders._normalize_yfinance_symbol("BRK.B"), "BRK-B")
        self.assertEqual(EnrichmentProviders._normalize_yfinance_symbol("BF.B"), "BF-B")

    def test_normalize_yfinance_symbol_preserves_hk_and_index(self):
        self.assertEqual(EnrichmentProviders._normalize_yfinance_symbol("0700.HK"), "0700.HK")
        self.assertEqual(EnrichmentProviders._normalize_yfinance_symbol("^VIX"), "^VIX")

    def test_skip_earnings_lookup_for_etf_and_index(self):
        self.assertTrue(EnrichmentProviders._skip_earnings_lookup("SPY"))
        self.assertTrue(EnrichmentProviders._skip_earnings_lookup("QQQ"))
        self.assertTrue(EnrichmentProviders._skip_earnings_lookup("^VIX"))
        self.assertFalse(EnrichmentProviders._skip_earnings_lookup("AAPL"))
        self.assertFalse(EnrichmentProviders._skip_earnings_lookup("BRK.B"))

    def test_finnhub_symbol_variants_include_xetra_base_symbol(self):
        variants = EnrichmentProviders._finnhub_symbol_variants("SAP.DE")
        self.assertIn("SAP.DE", variants)
        self.assertIn("SAP", variants)
        self.assertIn("SAP-DE", variants)

    def test_symbol_market_hint_detects_cn_symbols(self):
        self.assertEqual(EnrichmentProviders._symbol_market_hint("600519.SS"), "CN")
        self.assertEqual(EnrichmentProviders._symbol_market_hint("000858.SZ"), "CN")
        self.assertEqual(EnrichmentProviders._symbol_market_hint("CN:600519"), "CN")

    def test_collect_includes_market_news(self):
        class _FakeProviders(EnrichmentProviders):
            def fetch_earnings_calendar(self, symbols, days_ahead=14):
                return {}

            def fetch_macro_calendar(self, days_ahead=7):
                return []

            def fetch_macro_indicators(self):
                return {}

            def fetch_market_snapshot(self, market="US"):
                return {"source": market}

            def fetch_market_news(self, market="US", max_items=8):
                return [{"symbol": "SAP.DE", "title": "Headline", "source": market}]

        bundle = _FakeProviders().collect(["SAP.DE"], market="XETRA")
        self.assertIn("market_news", bundle)
        self.assertEqual(bundle["market_news"][0]["source"], "XETRA")

    def test_fetch_recommendation_trends_scores_latest_snapshot(self):
        class _FakeProviders(EnrichmentProviders):
            @staticmethod
            def _finnhub_api_key() -> str:
                return "test"

            def _read_generic_cache(self, namespace, key, ttl_sec=0):
                return None

            def _write_generic_cache(self, namespace, key, value):
                return None

            def _finnhub_get(self, path, params, **kwargs):
                if path != "stock/recommendation":
                    raise AssertionError(path)
                return [
                    {
                        "period": "2026-03-01",
                        "strongBuy": 4,
                        "buy": 6,
                        "hold": 2,
                        "sell": 1,
                        "strongSell": 0,
                    }
                ]

        rows = _FakeProviders().fetch_recommendation_trends(["AAPL"], max_symbols=1)
        self.assertIn("AAPL", rows)
        self.assertEqual(rows["AAPL"]["recommendation_total"], 13)
        self.assertGreater(rows["AAPL"]["recommendation_score"], 0.0)

    def test_finnhub_is_disabled_for_non_us_symbols(self):
        class _FakeProviders(EnrichmentProviders):
            @staticmethod
            def _finnhub_api_key() -> str:
                return "test"

            def _finnhub_get(self, path, params, **kwargs):
                raise AssertionError("finnhub should be skipped for non-US symbols")

        self.assertFalse(_FakeProviders().fetch_recommendation_trends(["SAP.DE"], max_symbols=1))

    def test_fetch_market_news_non_us_skips_finnhub(self):
        class _FakeProviders(EnrichmentProviders):
            def __init__(self):
                super().__init__()
                self._has_yf = False

            @staticmethod
            def _finnhub_api_key() -> str:
                return "test"

            def _finnhub_get(self, path, params, **kwargs):
                raise AssertionError("finnhub should be skipped for non-US market news")

        self.assertEqual(_FakeProviders().fetch_market_news("HK", max_items=4), [])


if __name__ == "__main__":
    unittest.main()
