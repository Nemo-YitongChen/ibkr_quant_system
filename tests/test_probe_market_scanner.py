from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.tools.probe_market_scanner import _load_probe_candidates, _probe_once


class ProbeMarketScannerTests(unittest.TestCase):
    def test_load_probe_candidates_reads_market_specific_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "scanner_probe_candidates.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        "markets:",
                        "  ASX:",
                        '    instrument: "STOCK.HK"',
                        "    location_codes:",
                        '      - "STK.HK.ASX"',
                        "    scanner_codes:",
                        '      - "HOT_BY_VOLUME"',
                    ]
                ),
                encoding="utf-8",
            )
            loaded = _load_probe_candidates(str(cfg_path), "ASX")
        self.assertEqual(loaded["instrument"], "STOCK.HK")
        self.assertEqual(loaded["location_codes"], ["STK.HK.ASX"])
        self.assertEqual(loaded["scanner_codes"], ["HOT_BY_VOLUME"])

    def test_probe_once_uses_blocking_scanner_data(self):
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
                contract = SimpleNamespace(symbol="BHP", exchange="SMART", primaryExchange="ASX", currency="AUD")
                details = SimpleNamespace(contract=contract)
                return [SimpleNamespace(contractDetails=details)]

        ib = _FakeIB()
        result = _probe_once(
            ib,
            instrument="STOCK.HK",
            location_code="STK.HK.ASX",
            scanner_code="HOT_BY_VOLUME",
            limit=5,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["symbols"][0]["symbol"], "BHP")
        self.assertEqual(
            ib.calls,
            [
                {
                    "instrument": "STOCK.HK",
                    "location": "STK.HK.ASX",
                    "scan_code": "HOT_BY_VOLUME",
                    "rows": 5,
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
