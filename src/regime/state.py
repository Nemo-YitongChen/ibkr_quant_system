from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict


@dataclass
class RegimeStateV2:
    market: str
    state: str
    trend_state: str
    vol_state: str
    breadth_state: str
    liquidity_state: str
    event_state: str
    risk_budget_scale: float
    long_allowed: bool
    short_allowed: bool
    composite: float = 0.0
    reason: str = ""
    components: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def classify_regime_v2(
    *,
    market: str,
    state: str,
    composite: float,
    trend_score: float,
    momentum_score: float,
    vol_score: float,
    drawdown_score: float,
    reason: str,
    event_state: str = "NONE",
    breadth_state: str = "UNKNOWN",
    liquidity_state: str = "NORMAL",
) -> RegimeStateV2:
    state_norm = str(state or "").upper()
    trend_state = "UP" if trend_score >= 0.6 else "DOWN" if trend_score <= 0.4 else "SIDEWAYS"
    vol_state = "CALM" if vol_score >= 0.7 else "STRESSED" if vol_score <= 0.3 else "ELEVATED"
    event_state_norm = str(event_state or "NONE").upper()
    liquidity_state_norm = str(liquidity_state or "NORMAL").upper()
    long_allowed = state_norm not in {"HARD_RISK_OFF"}
    short_allowed = state_norm in {"RISK_OFF", "HARD_RISK_OFF"}
    return RegimeStateV2(
        market=str(market or "").upper(),
        state=state_norm,
        trend_state=trend_state,
        vol_state=vol_state,
        breadth_state=str(breadth_state or "UNKNOWN").upper(),
        liquidity_state=liquidity_state_norm,
        event_state=event_state_norm,
        risk_budget_scale=float(risk_budget_scale_from_state(state_norm, composite)),
        long_allowed=bool(long_allowed),
        short_allowed=bool(short_allowed),
        composite=float(composite),
        reason=str(reason or ""),
        components={
            "trend": float(trend_score),
            "momentum": float(momentum_score),
            "volatility": float(vol_score),
            "drawdown": float(drawdown_score),
        },
    )


def risk_budget_scale_from_state(state: str, composite: float) -> float:
    state_norm = str(state or "").upper()
    if state_norm == "HARD_RISK_OFF":
        return 0.15
    if state_norm == "RISK_OFF":
        return max(0.25, min(0.60, float(composite)))
    if state_norm == "RISK_ON":
        return max(0.60, min(0.85, float(composite)))
    if state_norm == "BULL":
        return max(0.75, min(1.0, float(composite)))
    return max(0.40, min(0.65, float(composite) if composite else 0.50))
