from __future__ import annotations

import unittest
from types import SimpleNamespace

from src.ibkr.universe import UniverseConfig, UniverseService, scanner_location_codes_from_config


class UniverseScannerTests(unittest.TestCase):
    def test_scanner_location_codes_from_config_prefers_deduped_list(self):
        codes = scanner_location_codes_from_config(
            {
                "scanner_location_code": "STK.NYSE",
                "scanner_location_codes": ["STK.NYSE", "STK.NASDAQ.SCM", "STK.NYSE", "STK.AMEX"],
            },
            default="",
        )
        self.assertEqual(codes, ["STK.NYSE", "STK.NASDAQ.SCM", "STK.AMEX"])

    def test_scan_once_uses_blocking_scanner_data(self):
        class _FakeIB:
            def __init__(self):
                self.calls = []

            def reqScannerData(self, subscription, scannerSubscriptionOptions, scannerSubscriptionFilterOptions):
                self.calls.append(
                    {
                        "instrument": subscription.instrument,
                        "location": subscription.locationCode,
                        "scan_code": subscription.scanCode,
                        "rows": subscription.numberOfRows,
                    }
                )
                contract = SimpleNamespace(symbol="0700", exchange="SEHK", primaryExchange="SEHK", currency="HKD")
                details = SimpleNamespace(contract=contract)
                return [SimpleNamespace(contractDetails=details)]

        ib = _FakeIB()
        svc = UniverseService(
            ib,
            UniverseConfig(
                scanner_instrument="STOCK.HK",
                scanner_location_code="STK.HK.SEHK",
                scanner_enabled=True,
            ),
        )

        symbols = svc._scan_once("MOST_ACTIVE", 6)

        self.assertEqual(symbols, ["0700"])
        self.assertEqual(
            ib.calls,
            [
                {
                    "instrument": "STOCK.HK",
                    "location": "STK.HK.SEHK",
                    "scan_code": "MOST_ACTIVE",
                    "rows": 6,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
