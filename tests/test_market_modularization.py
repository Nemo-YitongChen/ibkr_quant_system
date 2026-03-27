from __future__ import annotations

import unittest

from src.common.markets import market_timezone_name, resolve_market_code, symbol_matches_market
from src.ibkr.contracts import parse_stock_spec
from src.risk.limits import DailyRiskGate


class _DummyStorage:
    def insert_risk_event(self, *args, **kwargs):
        return None


class _DummyAccount:
    def get_netliq(self):
        return 100000.0


class _RecordingProviders:
    def __init__(self):
        self.calls = []

    def collect(self, *, symbols, market=""):
        self.calls.append({"symbols": list(symbols), "market": market})
        return {"earnings": {}, "macro_events": []}


class MarketModularizationTests(unittest.TestCase):
    def test_market_aliases_resolve_xetra(self):
        self.assertEqual(resolve_market_code("DE"), "XETRA")
        self.assertEqual(resolve_market_code("IBIS"), "XETRA")
        self.assertEqual(market_timezone_name("XETRA"), "Europe/Berlin")
        self.assertEqual(resolve_market_code("CN"), "CN")
        self.assertEqual(resolve_market_code("SSE"), "CN")
        self.assertEqual(market_timezone_name("CN"), "Asia/Shanghai")
        self.assertEqual(resolve_market_code("AU"), "ASX")
        self.assertEqual(market_timezone_name("ASX"), "Australia/Sydney")

    def test_symbol_matches_market_handles_us_hk_xetra(self):
        self.assertTrue(symbol_matches_market("AAPL", "US"))
        self.assertFalse(symbol_matches_market("600519.SS", "US"))
        self.assertFalse(symbol_matches_market("SAP.DE", "US"))
        self.assertFalse(symbol_matches_market("BHP.AX", "US"))
        self.assertTrue(symbol_matches_market("600519.SS", "CN"))
        self.assertTrue(symbol_matches_market("000858.SZ", "CN"))
        self.assertFalse(symbol_matches_market("AAPL", "CN"))
        self.assertTrue(symbol_matches_market("0700.HK", "HK"))
        self.assertFalse(symbol_matches_market("0700.HK", "XETRA"))
        self.assertTrue(symbol_matches_market("SAP.DE", "XETRA"))
        self.assertFalse(symbol_matches_market("AAPL", "XETRA"))
        self.assertTrue(symbol_matches_market("BHP.AX", "ASX"))
        self.assertFalse(symbol_matches_market("BHP.AX", "HK"))

    def test_parse_stock_spec_supports_xetra_and_lse(self):
        spec = parse_stock_spec("SAP.DE")
        self.assertEqual(spec.symbol, "SAP")
        self.assertEqual(spec.exchange, "IBIS")
        self.assertEqual(spec.currency, "EUR")

        spec = parse_stock_spec("AZN.L")
        self.assertEqual(spec.symbol, "AZN")
        self.assertEqual(spec.exchange, "LSE")
        self.assertEqual(spec.currency, "GBP")

        spec = parse_stock_spec("BHP.AX")
        self.assertEqual(spec.symbol, "BHP")
        self.assertEqual(spec.exchange, "ASX")
        self.assertEqual(spec.currency, "AUD")

    def test_parse_stock_spec_supports_cn_connect_symbols(self):
        spec = parse_stock_spec("600519.SS")
        self.assertEqual(spec.symbol, "600519")
        self.assertEqual(spec.exchange, "SEHKNTL")
        self.assertEqual(spec.currency, "CNH")

        spec = parse_stock_spec("000858.SZ")
        self.assertEqual(spec.symbol, "000858")
        self.assertEqual(spec.exchange, "SEHKSZSE")
        self.assertEqual(spec.currency, "CNH")

        spec = parse_stock_spec("688981.SS")
        self.assertEqual(spec.symbol, "688981")
        self.assertEqual(spec.exchange, "SEHKSTAR")
        self.assertEqual(spec.currency, "CNH")

    def test_daily_risk_gate_refreshes_context_with_market(self):
        providers = _RecordingProviders()
        gate = DailyRiskGate(
            storage=_DummyStorage(),
            account=_DummyAccount(),
            daily_loss_limit_short_pct=-0.01,
            max_consecutive_losses=3,
            providers=providers,
            market="XETRA",
        )

        gate.refresh_trade_context(["SAP.DE", "SIE.DE"])

        self.assertEqual(len(providers.calls), 1)
        self.assertEqual(providers.calls[0]["market"], "XETRA")
        self.assertEqual(providers.calls[0]["symbols"], ["SAP.DE", "SIE.DE"])


if __name__ == "__main__":
    unittest.main()
