from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ReportScoringConfig:
    engine_score_weight: float = 1.0
    overlay_score_weight: float = 0.35
    trend_ret_20d_weight: float = 0.6
    trend_ret_5d_weight: float = 0.4
    trend_slope_weight: float = 0.3
    mr_dist_ma20_weight: float = -0.25
    tradable_range_weight: float = 0.2
    tradable_volume_weight: float = 0.2
    short_range_scale: float = 10.0
    short_vol_scale: float = 1_000_000.0
    vix_elevated_threshold: float = 18.0
    vix_high_threshold: float = 25.0
    vix_elevated_penalty: float = 0.20
    vix_high_penalty: float = 0.40
    earnings_penalty: float = 0.35
    macro_penalty: float = 0.25
    blocked_penalty: float = 0.35
    reduced_penalty: float = 0.15
    borrow_fee_penalty_scale: float = 150.0
    long_threshold: float = 0.25
    short_threshold: float = -0.25

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "ReportScoringConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def overlay_symbol(
    feat: Dict[str, Any],
    *,
    vix: float,
    earnings_in_14d: bool,
    macro_high_risk: bool,
    tradable_status: str = "",
    blocked_reason: str = "",
    short_borrow_fee_bps: float = 0.0,
    cfg: ReportScoringConfig | None = None,
) -> Dict[str, Any]:
    """Compute a small overlay around the main EngineStrategy replay score."""
    cfg = cfg or ReportScoringConfig()
    last = float(feat["last"])
    ma20 = float(feat["ma20"])
    dist_ma20 = (last - ma20) / ma20 if ma20 > 0 else 0.0

    alpha_trend = (
        float(cfg.trend_ret_20d_weight) * float(feat["ret_20d"])
        + float(cfg.trend_ret_5d_weight) * float(feat["ret_5d"])
        + float(cfg.trend_slope_weight) * float(feat["trend"])
    )
    alpha_mr = float(cfg.mr_dist_ma20_weight) * float(dist_ma20)
    alpha_tradable = (
        float(cfg.tradable_range_weight) * min(1.0, float(feat["short_range"]) * float(cfg.short_range_scale))
        + float(cfg.tradable_volume_weight) * min(1.0, float(feat["short_vol"]) / float(cfg.short_vol_scale))
    )
    alpha = alpha_trend + alpha_mr + alpha_tradable

    risk = 0.0
    if vix >= float(cfg.vix_high_threshold):
        risk += float(cfg.vix_high_penalty)
    elif vix >= float(cfg.vix_elevated_threshold):
        risk += float(cfg.vix_elevated_penalty)

    if earnings_in_14d:
        risk += float(cfg.earnings_penalty)
    if macro_high_risk:
        risk += float(cfg.macro_penalty)
    tradable_status = str(tradable_status or "").upper()
    if tradable_status == "BLOCKED" or blocked_reason:
        risk += float(cfg.blocked_penalty)
    elif tradable_status == "REDUCED":
        risk += float(cfg.reduced_penalty)
    if float(short_borrow_fee_bps or 0.0) > 0:
        risk += min(0.5, float(short_borrow_fee_bps) / max(1.0, float(cfg.borrow_fee_penalty_scale)))

    return {
        "symbol": feat["symbol"],
        "overlay_score": float(alpha - risk),
        "overlay_alpha": float(alpha),
        "overlay_risk": float(risk),
        "dist_ma20": float(dist_ma20),
        "risk_on": bool(feat.get("risk_on", True)),
        "mid_scale": float(feat.get("mid_scale", 0.5) or 0.5),
        "regime_state": str(feat.get("regime_state", "")),
        "regime_reason": str(feat.get("regime_reason", "")),
        "regime_composite": float(feat.get("regime_composite", 0.0) or 0.0),
    }


def score_symbol(
    feat: Dict[str, Any],
    *,
    vix: float,
    earnings_in_14d: bool,
    macro_high_risk: bool,
    cfg: ReportScoringConfig | None = None,
) -> Dict[str, Any]:
    """Backward-compatible wrapper retained for callers that still expect a single score field."""
    overlay = overlay_symbol(
        feat,
        vix=vix,
        earnings_in_14d=earnings_in_14d,
        macro_high_risk=macro_high_risk,
        cfg=cfg,
    )
    score = float(overlay["overlay_score"])
    direction = "WAIT"
    if score > float((cfg or ReportScoringConfig()).long_threshold):
        direction = "LONG"
    elif score < float((cfg or ReportScoringConfig()).short_threshold):
        direction = "SHORT"
    out = dict(overlay)
    out["score"] = score
    out["direction"] = direction
    out["alpha"] = float(overlay["overlay_alpha"])
    out["risk"] = float(overlay["overlay_risk"])
    return out


def rank_symbols(
    features: List[Dict[str, Any]],
    *,
    vix: float,
    earnings_map: Dict[str, bool],
    macro_high_risk: bool,
    top_n: int = 15,
    cfg: ReportScoringConfig | None = None,
) -> List[Dict[str, Any]]:
    """Backward-compatible ranking using only overlay scoring."""
    cfg = cfg or ReportScoringConfig()
    out: List[Dict[str, Any]] = []
    for f in features:
        sym = str(f["symbol"]).upper()
        out.append(
            score_symbol(
                f,
                vix=vix,
                earnings_in_14d=bool(earnings_map.get(sym, False)),
                macro_high_risk=macro_high_risk,
                cfg=cfg,
            )
        )
    out.sort(key=lambda x: float(x["score"]), reverse=True)
    return out[:top_n]
