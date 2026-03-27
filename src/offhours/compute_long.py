from __future__ import annotations

from typing import Dict, Any, Optional, List
import math


def _max_drawdown(xs: List[float]) -> float:
    if not xs:
        return 0.0
    peak = xs[0]
    mdd = 0.0
    for v in xs:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v - peak) / peak)
    return float(mdd)


def compute_long_for_symbol(symbol: str, md, years: int = 5) -> Optional[Dict[str, Any]]:
    """Compute a simple long-term score using daily closes (trend vs drawdown).

    We avoid relying on a project-specific 'long_signal' function to keep this robust.
    """
    bars = md.get_daily_bars(symbol, days=252 * years)
    return compute_long_from_bars(symbol, bars, years=years)


def compute_long_from_bars(symbol: str, bars: List[Any], years: int = 5) -> Optional[Dict[str, Any]]:
    """Compute a simple long-term score from preloaded daily bars."""
    if not bars or len(bars) < 200:
        return None
    lookback = max(200, 252 * int(years))
    closes = [float(b.close) for b in list(bars[-lookback:]) if getattr(b, "close", None) is not None]
    if len(closes) < 200:
        return None

    last = float(closes[-1])
    ma200 = sum(closes[-200:]) / 200.0
    trend = (last - ma200) / ma200 if ma200 > 0 else 0.0
    mdd = _max_drawdown(closes[-252:])  # last year drawdown

    # Score: trend minus drawdown penalty
    score = float(trend) + float(mdd)  # mdd is negative

    return {
        "symbol": symbol,
        "long_score": float(score),
        "trend_vs_ma200": float(trend),
        "mdd_1y": float(mdd),
        "last_close": float(last),
        "bars": int(len(bars)),
        "rebalance_flag": 1 if trend < -0.08 or mdd < -0.25 else 0,
    }
