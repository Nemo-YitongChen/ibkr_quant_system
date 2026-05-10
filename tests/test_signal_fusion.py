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

    def test_configured_fusion_weights_are_respected(self):
        total = fuse(
            short_sig=0.5,
            long_sig=0.5,
            mid_scale=0.5,
            can_trade_short=True,
            short_base_weight=1.0,
            short_mid_weight=0.0,
            long_weight=0.0,
            mid_bias_weight=0.0,
        )
        self.assertEqual(total, 0.5)

    def test_configured_momentum_block_threshold_controls_risk_off_chasing(self):
        blocked = fuse(
            short_sig=0.7,
            long_sig=0.0,
            mid_scale=0.2,
            can_trade_short=True,
            momentum_block_mid_threshold=0.25,
            momentum_block_short_threshold=0.6,
        )
        allowed = fuse(
            short_sig=0.7,
            long_sig=0.0,
            mid_scale=0.2,
            can_trade_short=True,
            momentum_block_mid_threshold=0.10,
            momentum_block_short_threshold=0.6,
        )
        self.assertEqual(blocked, 0.0)
        self.assertGreater(allowed, 0.0)


if __name__ == "__main__":
    unittest.main()
