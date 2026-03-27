from __future__ import annotations

import unittest

from src.signals.fusion import fuse


class SignalFusionTests(unittest.TestCase):
    def test_long_signal_survives_when_short_disabled(self):
        total = fuse(short_sig=0.7, long_sig=0.0, mid_scale=0.8, can_trade_short=False)
        self.assertGreater(total, 0.0)

    def test_short_signal_is_clipped_when_short_disabled(self):
        total = fuse(short_sig=-0.8, long_sig=0.0, mid_scale=0.8, can_trade_short=False)
        self.assertGreaterEqual(total, 0.0)

    def test_enabled_short_matches_expected_negative_bias(self):
        total = fuse(short_sig=-0.8, long_sig=0.0, mid_scale=0.8, can_trade_short=True)
        self.assertLess(total, 0.0)


if __name__ == "__main__":
    unittest.main()
