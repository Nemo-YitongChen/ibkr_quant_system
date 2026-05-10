from __future__ import annotations

from src.strategies.mid_regime import RegimeConfig, evaluate_regime
from src.strategies.short_breakout import BOConfig, signal as breakout_signal


def test_breakout_signal_marks_upside_breakout() -> None:
    cfg = BOConfig(lookback=3, confirm=1)
    high = [10.0, 11.0, 12.0, 13.0]
    low = [9.0, 9.5, 10.5, 12.0]
    close = [9.5, 10.5, 11.5, 12.5]

    assert breakout_signal(high, low, close, cfg) == 1.0


def test_breakout_signal_marks_downside_breakout() -> None:
    cfg = BOConfig(lookback=3, confirm=1)
    high = [12.0, 11.0, 10.0, 9.0]
    low = [10.0, 9.0, 8.0, 7.5]
    close = [11.0, 10.0, 9.0, 7.8]

    assert breakout_signal(high, low, close, cfg) == -1.0


def test_mid_regime_scale_drops_in_high_vol_deep_drawdown() -> None:
    cfg = RegimeConfig(
        ma_fast=2,
        ma_slow=3,
        momentum_lookback=3,
        vol_lookback=3,
        drawdown_lookback=4,
        vol_elevated=0.001,
        vol_extreme=0.002,
        drawdown_warn=-0.01,
        drawdown_stop=-0.03,
        risk_on_threshold=0.70,
        hard_risk_off_threshold=0.30,
        scale_floor=0.15,
        scale_neutral=0.50,
        scale_bull=0.85,
        scale_bear=0.35,
    )
    calm_uptrend = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    volatile_drawdown = [100.0, 110.0, 90.0, 95.0, 70.0, 72.0]

    good = evaluate_regime(calm_uptrend, cfg)
    bad = evaluate_regime(volatile_drawdown, cfg)

    assert good.scale > bad.scale
    assert bad.risk_on is False
    assert bad.state in {"RISK_OFF", "HARD_RISK_OFF"}
