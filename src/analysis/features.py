from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..strategies.mid_regime import RegimeConfig, evaluate_regime


def _pct(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a


def _sma(xs: List[float]) -> float:
    return sum(xs) / max(1, len(xs))


def _atr_proxy(highs: List[float], lows: List[float], closes: List[float], n: int = 14) -> float:
    """ATR proxy using True Range, sufficient for ranking and rough sizing."""
    if len(closes) < n + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    return _sma(trs[-n:])


@dataclass
class FeatureConfig:
    daily_lookback_days: int = 180
    min_daily_bars: int = 60
    ret_lookback_short: int = 5
    ret_lookback_long: int = 20
    ma_fast_window: int = 20
    ma_slow_window: int = 60
    atr_window: int = 14
    intraday_need_bars: int = 240
    intraday_window_bars: int = 60
    min_last_price: float = 2.0
    min_avg_daily_volume: float = 0.0
    min_avg_daily_dollar_volume: float = 0.0
    min_short_avg_bar_volume: float = 0.0

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "FeatureConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def compute_features_for_symbol(
    md,
    symbol: str,
    cfg: FeatureConfig | None = None,
    regime_cfg: RegimeConfig | None = None,
) -> Optional[Dict[str, Any]]:
    """Compute tradability features from daily plus recent intraday data."""
    cfg = cfg or FeatureConfig()

    d = md.get_daily_bars(symbol, days=int(cfg.daily_lookback_days))
    if not d or len(d) < int(cfg.min_daily_bars):
        return None

    closes = [float(b.close) for b in d if getattr(b, "close", None) is not None]
    highs = [float(b.high) for b in d if getattr(b, "high", None) is not None]
    lows = [float(b.low) for b in d if getattr(b, "low", None) is not None]
    volumes = [float(getattr(b, "volume", 0.0) or 0.0) for b in d if getattr(b, "close", None) is not None]
    min_bars_needed = max(
        int(cfg.min_daily_bars),
        int(cfg.ma_fast_window),
        int(cfg.ma_slow_window),
        int(cfg.atr_window) + 1,
        int(cfg.ret_lookback_long) + 1,
        int(cfg.ret_lookback_short) + 1,
    )
    if len(closes) < min_bars_needed or len(highs) != len(closes) or len(lows) != len(closes) or len(volumes) != len(closes):
        return None

    last = closes[-1]
    if last < float(cfg.min_last_price):
        return None

    avg_daily_volume = _sma(volumes[-int(cfg.ma_fast_window):])
    avg_daily_dollar_volume = _sma([closes[i] * volumes[i] for i in range(len(closes) - int(cfg.ma_fast_window), len(closes))])
    if avg_daily_volume < float(cfg.min_avg_daily_volume):
        return None
    if avg_daily_dollar_volume < float(cfg.min_avg_daily_dollar_volume):
        return None

    ret_1d = _pct(closes[-2], closes[-1])
    ret_5d = _pct(closes[-(int(cfg.ret_lookback_short) + 1)], closes[-1]) if len(closes) >= int(cfg.ret_lookback_short) + 1 else 0.0
    ret_20d = _pct(closes[-(int(cfg.ret_lookback_long) + 1)], closes[-1]) if len(closes) >= int(cfg.ret_lookback_long) + 1 else 0.0

    ma20 = _sma(closes[-int(cfg.ma_fast_window):])
    ma60 = _sma(closes[-int(cfg.ma_slow_window):])
    trend = _pct(ma60, ma20)

    atr14 = _atr_proxy(highs, lows, closes, int(cfg.atr_window))
    vol_norm = (atr14 / last) if last > 0 else 0.0
    regime_state = evaluate_regime(closes, regime_cfg or RegimeConfig())

    try:
        b5 = md.get_5m_bars(symbol, need=int(cfg.intraday_need_bars))
    except Exception:
        b5 = None

    short_range = 0.0
    short_vol = 0.0
    if b5 and len(b5) >= int(cfg.intraday_window_bars):
        h5 = [float(b.high) for b in b5[-int(cfg.intraday_window_bars):]]
        l5 = [float(b.low) for b in b5[-int(cfg.intraday_window_bars):]]
        v5 = [float(getattr(b, "volume", 0.0) or 0.0) for b in b5[-int(cfg.intraday_window_bars):]]
        short_range = (max(h5) - min(l5)) / (last if last > 0 else 1.0)
        short_vol = sum(v5) / max(1.0, float(len(v5)))
        if short_vol < float(cfg.min_short_avg_bar_volume):
            return None
    elif float(cfg.min_short_avg_bar_volume) > 0:
        return None

    return {
        "symbol": symbol,
        "last": float(last),
        "ret_1d": float(ret_1d),
        "ret_5d": float(ret_5d),
        "ret_20d": float(ret_20d),
        "ma20": float(ma20),
        "ma60": float(ma60),
        "trend": float(trend),
        "atr14": float(atr14),
        "vol_norm": float(vol_norm),
        "risk_on": bool(regime_state.risk_on),
        "mid_scale": float(regime_state.scale),
        "regime_state": str(regime_state.state),
        "regime_reason": str(regime_state.reason),
        "regime_composite": float(regime_state.composite),
        "regime_trend_score": float(regime_state.trend_score),
        "regime_momentum_score": float(regime_state.momentum_score),
        "regime_vol_score": float(regime_state.vol_score),
        "regime_drawdown_score": float(regime_state.drawdown_score),
        "avg_daily_volume": float(avg_daily_volume),
        "avg_daily_dollar_volume": float(avg_daily_dollar_volume),
        "short_range": float(short_range),
        "short_vol": float(short_vol),
    }
