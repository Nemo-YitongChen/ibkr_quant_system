from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Any, List, Optional
import math
import time

from ..strategies.engine_strategy import EngineStrategy, StrategyConfig


def _stability_penalty(sig_series: List[float]) -> float:
    """Compute a penalty based on signal flip count (more flips => less stable)."""
    if len(sig_series) < 3:
        return 1.0
    flips = 0
    prev = 0
    for x in sig_series:
        s = 1 if x > 0 else (-1 if x < 0 else 0)
        if prev != 0 and s != 0 and s != prev:
            flips += 1
        if s != 0:
            prev = s
    # Penalty in (0,1]; more flips => smaller
    return 1.0 / (1.0 + flips)


def _core_signal_value(sig) -> float:
    channel = str(getattr(sig, "channel", "") or "").upper()
    if channel == "PURE_SHORT":
        return float(getattr(sig, "short_sig", 0.0) or 0.0)
    total_sig = float(getattr(sig, "total_sig", 0.0) or 0.0)
    short_sig = float(getattr(sig, "short_sig", 0.0) or 0.0)
    return total_sig if abs(total_sig) >= abs(short_sig) else short_sig


def _direction(sig) -> str:
    action = str(getattr(sig, "action", "") or "").upper()
    if action == "BUY":
        return "LONG"
    if action == "SELL":
        return "SHORT"
    return "WAIT"


def compute_engine_signal_for_symbol(
    *,
    symbol: str,
    md,
    cfg: StrategyConfig,
    gate,
    bars_need: int = 600,
    tail_bars: int = 60,
) -> Optional[Dict[str, Any]]:
    """Replay EngineStrategy and expose the shared signal language used by live trading."""
    try:
        bars = md.get_5m_bars(symbol, need=bars_need)
    except Exception:
        raise

    if not bars or len(bars) < 30:
        return None

    strat = EngineStrategy(orders=None, gate=gate, cfg=cfg)
    last_sig = None
    for b in bars:
        last_sig = strat.evaluate_from_bar(symbol, b)

    if last_sig is None:
        return None

    strat2 = EngineStrategy(orders=None, gate=gate, cfg=cfg)
    sig_series: List[float] = []
    for b in bars[-min(len(bars), tail_bars):]:
        s = strat2.evaluate_from_bar(symbol, b)
        if s is not None:
            sig_series.append(float(_core_signal_value(s)))

    stability = _stability_penalty(sig_series)
    core_signal = float(_core_signal_value(last_sig))
    strength = abs(core_signal)
    confidence = float(strength) * float(stability) * max(0.25, float(getattr(last_sig, "mid_scale", 0.0) or 0.0))
    risk_snapshot = getattr(last_sig, "risk_snapshot", None)
    risk_payload = risk_snapshot.to_dict() if (risk_snapshot is not None and hasattr(risk_snapshot, "to_dict")) else {}

    return {
        "symbol": symbol,
        "engine_score": float(confidence),
        "signal_strength": float(strength),
        "signal_value": float(core_signal),
        "direction": _direction(last_sig),
        "action": str(getattr(last_sig, "action", "") or ""),
        "should_trade": bool(getattr(last_sig, "should_trade", False)),
        "channel": str(getattr(last_sig, "channel", "") or ""),
        "threshold_used": float(getattr(last_sig, "threshold_used", 0.0) or 0.0),
        "short_sig": float(getattr(last_sig, "short_sig", 0.0) or 0.0),
        "total_sig": float(getattr(last_sig, "total_sig", 0.0) or 0.0),
        "mid_scale": float(getattr(last_sig, "mid_scale", 0.0) or 0.0),
        "stability": float(stability),
        "risk_on": bool(getattr(last_sig, "risk_on", True)),
        "regime_state": str(getattr(last_sig, "regime_state", "") or ""),
        "regime_reason": str(getattr(last_sig, "regime_reason", "") or ""),
        "entry_price": float(getattr(last_sig, "entry_price", 0.0) or 0.0),
        "risk_allowed": bool(getattr(risk_snapshot, "allowed", True)) if risk_snapshot is not None else True,
        "risk_per_share": float(risk_payload.get("risk_per_share", 0.0) or 0.0),
        "stop_price": float(risk_payload.get("stop_price", 0.0) or 0.0),
        "take_profit_price": float(risk_payload.get("take_profit_price", 0.0) or 0.0),
        "stop_distance": float(risk_payload.get("stop_distance", 0.0) or 0.0),
        "take_profit_distance": float(risk_payload.get("take_profit_distance", 0.0) or 0.0),
        "liquidity_haircut": float(risk_payload.get("liquidity_haircut", 0.0) or 0.0),
        "avg_bar_volume": float(risk_payload.get("avg_bar_volume", 0.0) or 0.0),
        "event_risk": str(risk_payload.get("event_risk", "") or ""),
        "event_risk_reason": str(risk_payload.get("event_risk_reason", "") or ""),
        "short_borrow_fee_bps": float(risk_payload.get("short_borrow_fee_bps", 0.0) or 0.0),
        "short_borrow_source": str(risk_payload.get("short_borrow_source", "") or ""),
        "reason": getattr(last_sig, "reason", ""),
        "bar_end_time": getattr(getattr(bars[-1], "end_time", None), "isoformat", lambda: "")(),
        "bars": int(len(bars)),
        "risk_snapshot": risk_payload,
    }


def compute_short_for_symbol(
    *,
    symbol: str,
    md,
    cfg: StrategyConfig,
    gate,
    bars_need: int = 600,
    tail_bars: int = 60,
) -> Optional[Dict[str, Any]]:
    """Compute next-day short watch score using 5m bars replay.

    We replay bars through EngineStrategy (same logic used in live engine),
    but we do NOT execute orders here. We only evaluate signals.
    """
    engine_row = compute_engine_signal_for_symbol(
        symbol=symbol,
        md=md,
        cfg=cfg,
        gate=gate,
        bars_need=bars_need,
        tail_bars=tail_bars,
    )
    if engine_row is None:
        return None

    return {
        "symbol": symbol,
        "score": float(engine_row["engine_score"]),
        "short_sig": float(engine_row["short_sig"]),
        "total_sig": float(engine_row["total_sig"]),
        "mid_scale": float(engine_row["mid_scale"]),
        "channel": str(engine_row["channel"]),
        "direction": str(engine_row["direction"]),
        "reason": str(engine_row["reason"]),
        "bar_end_time": str(engine_row["bar_end_time"]),
        "stability": float(engine_row["stability"]),
        "bars": int(engine_row["bars"]),
    }
