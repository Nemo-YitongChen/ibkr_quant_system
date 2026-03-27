from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class TradePlanConfig:
    vix_elevated_threshold: float = 18.0
    vix_high_threshold: float = 25.0
    size_mult_normal: float = 1.0
    size_mult_elevated: float = 0.8
    size_mult_high: float = 0.6
    long_stop_atr_mult: float = 1.2
    long_take_profit_atr_mult: float = 2.0
    short_stop_atr_mult: float = 1.2
    short_take_profit_atr_mult: float = 2.0
    min_atr_abs: float = 0.01

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "TradePlanConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def make_trade_plan(
    row: Dict[str, Any],
    feat: Dict[str, Any],
    *,
    vix: float,
    cfg: TradePlanConfig | None = None,
) -> Dict[str, Any]:
    """Convert ranking output into a structured, risk-aware trade suggestion."""
    cfg = cfg or TradePlanConfig()
    sym = str(row["symbol"]).upper()
    direction = row["direction"]
    last = float(feat["last"])
    atr = max(float(feat["atr14"]), float(cfg.min_atr_abs))
    vol_norm = float(feat["vol_norm"])

    size_mult = float(cfg.size_mult_normal)
    if vix >= float(cfg.vix_high_threshold):
        size_mult = float(cfg.size_mult_high)
    elif vix >= float(cfg.vix_elevated_threshold):
        size_mult = float(cfg.size_mult_elevated)

    liquidity_haircut = float(row.get("liquidity_haircut", row.get("risk_snapshot", {}).get("liquidity_haircut", 0.0)) or 0.0)
    if liquidity_haircut > 0:
        size_mult *= max(0.25, 1.0 - liquidity_haircut)
    if str(row.get("tradable_status", "") or "").upper() == "REDUCED":
        size_mult *= 0.5

    stop = ""
    tp = ""
    entry = "NEXT_OPEN"
    notes = []
    risk_snapshot = dict(row.get("risk_snapshot", {}) or {})
    explicit_stop = float(row.get("stop_price", risk_snapshot.get("stop_price", 0.0)) or 0.0)
    explicit_tp = float(row.get("take_profit_price", risk_snapshot.get("take_profit_price", 0.0)) or 0.0)
    stop_distance = float(row.get("stop_distance", risk_snapshot.get("stop_distance", 0.0)) or 0.0)
    take_profit_distance = float(row.get("take_profit_distance", risk_snapshot.get("take_profit_distance", 0.0)) or 0.0)

    if explicit_stop > 0 and explicit_tp > 0:
        stop = f"{explicit_stop:.2f}"
        tp = f"{explicit_tp:.2f}"
        notes.append("Risk model uses ATR, slippage, gap, liquidity, and short-side add-ons to set exit levels.")
    elif direction == "LONG":
        stop = f"{last - float(cfg.long_stop_atr_mult) * atr:.2f}"
        tp = f"{last + float(cfg.long_take_profit_atr_mult) * atr:.2f}"
        notes.append("Entry: next open; optionally wait for pullback or confirmation.")
    elif direction == "SHORT":
        stop = f"{last + float(cfg.short_stop_atr_mult) * atr:.2f}"
        tp = f"{last - float(cfg.short_take_profit_atr_mult) * atr:.2f}"
        notes.append("Short only if permitted by your risk gate and account settings.")
    else:
        notes.append("No clear edge; keep on watch.")

    return {
        "symbol": sym,
        "direction": direction,
        "entry": entry,
        "stop": stop,
        "take_profit": tp,
        "size_mult_suggest": round(size_mult, 2),
        "vol_norm": round(vol_norm, 4),
        "risk_on": bool(row.get("risk_on", True)),
        "mid_scale": round(float(row.get("mid_scale", 0.5) or 0.5), 3),
        "regime_state": str(row.get("regime_state", "")),
        "regime_reason": str(row.get("regime_reason", "")),
        "tradable_status": str(row.get("tradable_status", "")),
        "blocked_reason": str(row.get("blocked_reason", "")),
        "channel": str(row.get("channel", "")),
        "stability": round(float(row.get("stability", 0.0) or 0.0), 3),
        "risk_per_share": round(float(row.get("risk_per_share", risk_snapshot.get("risk_per_share", 0.0)) or 0.0), 4),
        "stop_distance": round(stop_distance, 4),
        "take_profit_distance": round(take_profit_distance, 4),
        "notes": " ".join(notes),
    }
