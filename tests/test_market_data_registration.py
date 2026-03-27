from __future__ import annotations

from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
import threading
import unittest

from src.ibkr.contracts import make_stock_contract
from src.ibkr.market_data import MarketDataService


class _FakeBar:
    def __init__(self, date, open_, high, low, close, volume):
        self.date = date
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


class _FakeIB:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.cancel_calls = 0

    def reqHistoricalData(self, **kwargs):
        self.calls += 1
        if not self._responses:
            raise RuntimeError("no_more_responses")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def cancelHistoricalData(self, raw):
        self.cancel_calls += 1


class MarketDataRegistrationTests(unittest.TestCase):
    def test_register_skips_identical_contract_re_registration(self):
        md = MarketDataService(ib=None)  # register path does not use ib
        contract = make_stock_contract("SPY")
        md.register("SPY", contract)
        md.register("SPY", contract)
        self.assertEqual(len(md._contracts), 1)

    def test_get_5m_bars_retries_historical_request_once(self):
        t0 = datetime(2026, 3, 13, 10, 0, tzinfo=timezone.utc)
        bars = [_FakeBar(t0 + timedelta(minutes=5 * i), 1, 2, 0.5, 1.5, 1000 + i) for i in range(4)]
        with TemporaryDirectory() as tmpdir:
            ib = _FakeIB([RuntimeError("timeout"), bars])
            md = MarketDataService(
                ib=ib,
                hist_retry_attempts=2,
                hist_retry_backoff_sec=0.0,
                hist_cache_dir=tmpdir,
            )
            contract = make_stock_contract("SPY")
            md.register("SPY", contract)

            out = md.get_5m_bars("SPY", need=3)

            self.assertEqual(ib.calls, 2)
            self.assertEqual(len(out), 3)
            self.assertEqual(out[-1].close, 1.5)

    def test_get_5m_bars_reuses_shared_cache_across_instances(self):
        t0 = datetime(2026, 3, 13, 10, 0, tzinfo=timezone.utc)
        bars = [_FakeBar(t0 + timedelta(minutes=5 * i), 10, 12, 9, 11 + i, 1000 + i) for i in range(3)]
        with TemporaryDirectory() as tmpdir:
            first_ib = _FakeIB([bars])
            first_md = MarketDataService(
                ib=first_ib,
                hist_retry_attempts=1,
                hist_retry_backoff_sec=0.0,
                hist_cache_dir=tmpdir,
            )
            contract = make_stock_contract("SPY")
            first_md.register("SPY", contract)
            first_out = first_md.get_5m_bars("SPY", need=3)
            self.assertEqual(first_ib.calls, 1)
            self.assertEqual(len(first_out), 3)

            second_ib = _FakeIB([RuntimeError("should_not_be_used")])
            second_md = MarketDataService(
                ib=second_ib,
                hist_retry_attempts=1,
                hist_retry_backoff_sec=0.0,
                hist_cache_dir=tmpdir,
            )
            second_md.register("SPY", contract)
            second_out = second_md.get_5m_bars("SPY", need=3)
            self.assertEqual(second_ib.calls, 0)
            self.assertEqual(len(second_out), 3)
            self.assertEqual(second_out[-1].close, 13.0)

    def test_get_daily_bars_rejects_sync_ib_requests_from_non_owner_thread(self):
        t0 = datetime(2026, 3, 13, 10, 0, tzinfo=timezone.utc)
        bars = [_FakeBar(t0 + timedelta(days=i), 10, 12, 9, 11 + i, 1000 + i) for i in range(3)]
        ib = _FakeIB([bars])
        md = MarketDataService(ib=ib)
        contract = make_stock_contract("SPY")
        md.register("SPY", contract)

        result: dict[str, object] = {}

        def _worker() -> None:
            try:
                md.get_daily_bars("SPY", days=5)
            except Exception as e:
                result["error"] = e

        thread = threading.Thread(target=_worker, name="worker-no-loop")
        thread.start()
        thread.join(timeout=5)

        self.assertIn("error", result)
        self.assertIsInstance(result["error"], RuntimeError)
        self.assertIn("owner thread", str(result["error"]))
        self.assertEqual(ib.calls, 0)


if __name__ == "__main__":
    unittest.main()
