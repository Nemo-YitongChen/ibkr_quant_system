from __future__ import annotations

from typing import Dict, Any, Optional, List
import math

from ..strategies.mid_regime import RegimeConfig, evaluate_regime, to_regime_state_v2


def _trend_slope(closes: List[float]) -> float:
    """Simple slope proxy for trend (normalized)."""
    if len(closes) < 30:
        return 0.0
    y0 = float(closes[0])
    y1 = float(closes[-1])
    if y0 <= 0:
        return 0.0
    return (y1 - y0) / y0


def compute_mid_for_symbol(
    symbol: str,
    md,
    lookback_days: int = 180,
    regime_cfg: RegimeConfig | None = None,
) -> Optional[Dict[str, Any]]:
    """Compute mid-term regime + trend features using daily bars."""
    bars = md.get_daily_bars(symbol, days=lookback_days)
    return compute_mid_from_bars(symbol, bars, regime_cfg=regime_cfg)


def compute_mid_from_bars(
    symbol: str,
    bars: List[Any],
    *,
    regime_cfg: RegimeConfig | None = None,
    lookback_days: int = 180,
) -> Optional[Dict[str, Any]]:
    """Compute mid-term regime + trend features from preloaded daily bars."""
    if not bars or len(bars) < 50:
        return None
    window = list(bars[-max(50, int(lookback_days)) :])
    closes = [float(b.close) for b in window if getattr(b, "close", None) is not None]
    if len(closes) < 50:
        return None

    cfg = regime_cfg or RegimeConfig()
    regime_state = evaluate_regime(closes, cfg)
    regime_state_v2 = to_regime_state_v2(regime_state)

    slope = _trend_slope(closes[-60:])  # last ~3 months

    return {
        "symbol": symbol,
        "mid_scale": float(regime_state.scale),
        "risk_on": bool(regime_state.risk_on),
        "regime_state": str(regime_state.state),
        "regime_reason": str(regime_state.reason),
        "regime_composite": float(regime_state.composite),
        "regime_state_v2": regime_state_v2.to_dict(),
        "trend_slope_60d": float(slope),
        "last_close": float(closes[-1]),
        "bars": int(len(bars)),
    }
