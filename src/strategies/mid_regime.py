from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List

from ..regime.state import RegimeStateV2, classify_regime_v2


@dataclass
class RegimeConfig:
    ma_fast: int = 24
    ma_slow: int = 72
    momentum_lookback: int = 48
    vol_lookback: int = 72
    drawdown_lookback: int = 72

    vol_elevated: float = 0.010
    vol_extreme: float = 0.018
    drawdown_warn: float = -0.03
    drawdown_stop: float = -0.06

    scale_floor: float = 0.15
    scale_neutral: float = 0.50
    scale_bull: float = 0.85
    scale_bear: float = 0.35

    trend_weight: float = 0.45
    momentum_weight: float = 0.20
    volatility_weight: float = 0.20
    drawdown_weight: float = 0.15

    risk_on_threshold: float = 0.50
    hard_risk_off_threshold: float = 0.25


@dataclass
class RegimeState:
    risk_on: bool
    scale: float
    composite: float
    state: str
    reason: str
    trend_score: float
    momentum_score: float
    vol_score: float
    drawdown_score: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def sma(xs: List[float], n: int) -> float:
    if len(xs) < n or n <= 0:
        return 0.0
    w = xs[-n:]
    return sum(w) / float(n)


def stdev_ret(close: List[float], n: int) -> float:
    if len(close) < n + 1 or n <= 1:
        return 0.0
    rets = []
    for i in range(-n, 0):
        prev = float(close[i - 1])
        cur = float(close[i])
        if prev <= 0:
            continue
        rets.append((cur / prev) - 1.0)
    if len(rets) < 2:
        return 0.0
    m = sum(rets) / len(rets)
    v = sum((r - m) ** 2 for r in rets) / max(1, len(rets) - 1)
    return v**0.5


def _momentum(close: List[float], lookback: int) -> float:
    if len(close) < lookback + 1 or lookback <= 0:
        return 0.0
    prev = float(close[-(lookback + 1)])
    cur = float(close[-1])
    if prev <= 0:
        return 0.0
    return (cur / prev) - 1.0


def _drawdown(close: List[float], lookback: int) -> float:
    if len(close) < max(2, lookback):
        return 0.0
    window = [float(x) for x in close[-lookback:]]
    peak = max(window) if window else 0.0
    last = float(window[-1]) if window else 0.0
    if peak <= 0:
        return 0.0
    return (last / peak) - 1.0


def _trend_score(close: List[float], cfg: RegimeConfig) -> float:
    fast = sma(close, cfg.ma_fast)
    slow = sma(close, cfg.ma_slow)
    last = float(close[-1]) if close else 0.0
    if last <= 0 or fast <= 0 or slow <= 0:
        return 0.5

    ma_cross = 1.0 if fast >= slow else 0.0
    dist_fast = _clip((last / fast) - 1.0, -0.05, 0.05)
    dist_slow = _clip((fast / slow) - 1.0, -0.05, 0.05)
    bias = 0.5 + 5.0 * (0.6 * dist_fast + 0.4 * dist_slow)
    return _clip(0.5 * ma_cross + 0.5 * bias, 0.0, 1.0)


def _momentum_score(close: List[float], cfg: RegimeConfig) -> float:
    mom = _momentum(close, cfg.momentum_lookback)
    return _clip(0.5 + 5.0 * _clip(mom, -0.10, 0.10), 0.0, 1.0)


def _vol_score(close: List[float], cfg: RegimeConfig) -> float:
    vol = stdev_ret(close, cfg.vol_lookback)
    if vol <= cfg.vol_elevated:
        return 1.0
    if vol >= cfg.vol_extreme:
        return 0.0
    span = max(1e-9, cfg.vol_extreme - cfg.vol_elevated)
    return _clip(1.0 - ((vol - cfg.vol_elevated) / span), 0.0, 1.0)


def _drawdown_score(close: List[float], cfg: RegimeConfig) -> float:
    dd = _drawdown(close, cfg.drawdown_lookback)
    if dd >= cfg.drawdown_warn:
        return 1.0
    if dd <= cfg.drawdown_stop:
        return 0.0
    span = max(1e-9, cfg.drawdown_warn - cfg.drawdown_stop)
    return _clip((dd - cfg.drawdown_stop) / span, 0.0, 1.0)


def evaluate_regime(close: List[float], cfg: RegimeConfig) -> RegimeState:
    """Return explainable mid-term regime state."""
    need = max(cfg.ma_slow + 2, cfg.vol_lookback + 1, cfg.momentum_lookback + 1, cfg.drawdown_lookback)
    if len(close) < need:
        return RegimeState(
            risk_on=True,
            scale=float(cfg.scale_neutral),
            composite=0.5,
            state="WARMUP",
            reason=f"warmup need={need} have={len(close)}",
            trend_score=0.5,
            momentum_score=0.5,
            vol_score=0.5,
            drawdown_score=0.5,
        )

    trend_score = _trend_score(close, cfg)
    momentum_score = _momentum_score(close, cfg)
    vol_score = _vol_score(close, cfg)
    drawdown_score = _drawdown_score(close, cfg)

    composite = (
        float(cfg.trend_weight) * trend_score
        + float(cfg.momentum_weight) * momentum_score
        + float(cfg.volatility_weight) * vol_score
        + float(cfg.drawdown_weight) * drawdown_score
    )
    composite = _clip(composite, 0.0, 1.0)

    risk_on = composite >= float(cfg.risk_on_threshold)
    if composite <= float(cfg.hard_risk_off_threshold):
        scale = float(cfg.scale_floor)
        state = "HARD_RISK_OFF"
    elif composite >= 0.75:
        scale = float(cfg.scale_bull)
        state = "BULL"
    elif composite >= float(cfg.risk_on_threshold):
        scale = float(cfg.scale_neutral + 0.5 * (cfg.scale_bull - cfg.scale_neutral))
        state = "RISK_ON"
    else:
        scale = float(cfg.scale_bear + 0.5 * (cfg.scale_neutral - cfg.scale_bear) * (composite / max(1e-9, cfg.risk_on_threshold)))
        state = "RISK_OFF"

    scale = _clip(scale, float(cfg.scale_floor), 1.0)
    parts = [
        ("trend", trend_score),
        ("momentum", momentum_score),
        ("vol", vol_score),
        ("drawdown", drawdown_score),
    ]
    weakest = min(parts, key=lambda x: x[1])
    strongest = max(parts, key=lambda x: x[1])
    reason = (
        f"{state} comp={composite:.3f} "
        f"trend={trend_score:.2f} mom={momentum_score:.2f} "
        f"vol={vol_score:.2f} dd={drawdown_score:.2f} "
        f"weak={weakest[0]} strong={strongest[0]}"
    )
    return RegimeState(
        risk_on=risk_on,
        scale=scale,
        composite=composite,
        state=state,
        reason=reason,
        trend_score=trend_score,
        momentum_score=momentum_score,
        vol_score=vol_score,
        drawdown_score=drawdown_score,
    )


def regime(close: List[float], cfg: RegimeConfig) -> tuple[bool, float]:
    st = evaluate_regime(close, cfg)
    return st.risk_on, st.scale


def to_regime_state_v2(st: RegimeState, *, market: str = "", event_state: str = "NONE") -> RegimeStateV2:
    return classify_regime_v2(
        market=market,
        state=st.state,
        composite=st.composite,
        trend_score=st.trend_score,
        momentum_score=st.momentum_score,
        vol_score=st.vol_score,
        drawdown_score=st.drawdown_score,
        reason=st.reason,
        event_state=event_state,
    )
