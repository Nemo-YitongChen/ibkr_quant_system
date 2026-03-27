from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from ..events.models import SignalDecision

COMMODITY_PROXY_THEMES = {
    "GLD": "gold",
    "SLV": "silver",
    "USO": "oil",
}


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


@dataclass
class InvestmentScoringConfig:
    long_score_weight: float = 1.0
    trend_vs_ma200_weight: float = 0.6
    mid_scale_weight: float = 0.35
    regime_composite_weight: float = 0.35
    trend_slope_weight: float = 0.25
    drawdown_penalty_weight: float = 0.40
    rebalance_penalty: float = 0.35
    risk_off_penalty: float = 0.15
    earnings_penalty: float = 0.12
    macro_penalty: float = 0.10
    vix_elevated_threshold: float = 18.0
    vix_high_threshold: float = 25.0
    vix_elevated_penalty: float = 0.08
    vix_high_penalty: float = 0.18
    accumulate_threshold: float = 0.35
    hold_threshold: float = 0.10
    reduce_threshold: float = -0.05
    min_mid_scale_accumulate: float = 0.55
    valuation_weight: float = 0.12
    profit_margin_weight: float = 0.18
    operating_margin_weight: float = 0.10
    revenue_growth_weight: float = 0.10
    roe_weight: float = 0.10
    recommendation_weight: float = 0.08
    market_sentiment_weight: float = 0.10
    data_quality_weight: float = 0.12
    source_coverage_weight: float = 0.06
    missing_ratio_penalty_weight: float = 0.18
    low_data_quality_threshold: float = 0.55
    low_data_quality_penalty: float = 0.10
    valuation_anchor_pe: float = 20.0
    trailing_pe_cap: float = 45.0
    forward_pe_cap: float = 35.0
    profit_margin_floor: float = 0.05
    operating_margin_floor: float = 0.08
    revenue_growth_floor: float = 0.03
    roe_floor: float = 0.10
    execution_alpha_weight: float = 0.35
    execution_market_sentiment_weight: float = 0.22
    execution_mid_scale_weight: float = 0.20
    execution_trend_weight: float = 0.12
    execution_recommendation_weight: float = 0.06
    execution_data_quality_weight: float = 0.18
    execution_drawdown_penalty_weight: float = 0.10
    execution_event_penalty: float = 0.10
    execution_macro_penalty: float = 0.08
    execution_risk_off_penalty: float = 0.10
    execution_rebalance_penalty: float = 0.08
    execution_missing_ratio_penalty_weight: float = 0.12
    execution_low_data_quality_penalty: float = 0.12
    execution_ready_threshold: float = 0.08
    execution_min_data_quality: float = 0.50
    execution_max_missing_ratio: float = 0.40
    cost_penalty_weight: float = 0.12
    cost_penalty_bps_scale: float = 120.0
    execution_cost_penalty_weight: float = 0.16
    execution_cost_penalty_bps_scale: float = 90.0
    high_expected_cost_bps: float = 45.0
    microstructure_weight: float = 0.10
    execution_microstructure_weight: float = 0.16
    returns_risk_penalty_weight: float = 0.08
    execution_returns_risk_penalty_weight: float = 0.10
    returns_ewma_vol_soft_limit: float = 0.035
    returns_downside_vol_soft_limit: float = 0.025

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentScoringConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class InvestmentPlanConfig:
    size_mult_normal: float = 1.0
    size_mult_elevated: float = 0.85
    size_mult_high: float = 0.70
    vix_elevated_threshold: float = 18.0
    vix_high_threshold: float = 25.0
    min_allocation_mult: float = 0.35
    max_allocation_mult: float = 1.25
    staged_entry_parts: int = 3
    accumulate_pullback_pct: float = 0.03
    rebalance_window_days: int = 30
    review_window_days: int = 90
    trim_fraction: float = 0.25

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentPlanConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def score_investment_candidate(
    long_row: Dict[str, Any],
    mid_row: Dict[str, Any],
    *,
    vix: float,
    earnings_in_14d: bool,
    macro_high_risk: bool,
    market_sentiment_score: float = 0.0,
    data_quality_score: float = 1.0,
    source_coverage: float = 1.0,
    missing_ratio: float = 0.0,
    history_source: str = "",
    expected_cost_bps: float = 0.0,
    spread_proxy_bps: float = 0.0,
    slippage_proxy_bps: float = 0.0,
    commission_proxy_bps: float = 0.0,
    liquidity_score: float = 1.0,
    avg_daily_dollar_volume: float = 0.0,
    avg_daily_volume: float = 0.0,
    atr_pct: float = 0.0,
    micro_breakout_5m: float = 0.0,
    micro_reversal_5m: float = 0.0,
    micro_volume_burst_5m: float = 0.0,
    microstructure_score: float = 0.0,
    intraday_history_source: str = "",
    intraday_bar_count: float = 0.0,
    returns_ewma_vol_20d: float = 0.0,
    returns_downside_vol_20d: float = 0.0,
    fundamentals: Dict[str, Any] | None = None,
    recommendation: Dict[str, Any] | None = None,
    cfg: InvestmentScoringConfig | None = None,
) -> Dict[str, Any]:
    cfg = cfg or InvestmentScoringConfig()
    symbol = str(long_row["symbol"]).upper()
    fundamentals = dict(fundamentals or {})
    recommendation = dict(recommendation or {})
    market = str(long_row.get("market") or mid_row.get("market") or fundamentals.get("market") or "").upper()
    asset_class = str(fundamentals.get("asset_class", "") or "").strip().lower()
    asset_theme = str(fundamentals.get("asset_theme", "") or "").strip().lower()
    if not asset_class and symbol in COMMODITY_PROXY_THEMES:
        asset_class = "commodity_proxy"
    if not asset_theme and symbol in COMMODITY_PROXY_THEMES:
        asset_theme = COMMODITY_PROXY_THEMES[symbol]
    is_commodity_proxy = asset_class == "commodity_proxy"
    data_quality_score = _clamp(float(data_quality_score or 0.0), 0.0, 1.0)
    source_coverage = _clamp(float(source_coverage or 0.0), 0.0, 1.0)
    missing_ratio = _clamp(float(missing_ratio or 0.0), 0.0, 1.0)
    expected_cost_bps = max(0.0, float(expected_cost_bps or 0.0))
    spread_proxy_bps = max(0.0, float(spread_proxy_bps or 0.0))
    slippage_proxy_bps = max(0.0, float(slippage_proxy_bps or 0.0))
    commission_proxy_bps = max(0.0, float(commission_proxy_bps or 0.0))
    liquidity_score = _clamp(float(liquidity_score or 0.0), 0.0, 1.0)
    avg_daily_dollar_volume = max(0.0, float(avg_daily_dollar_volume or 0.0))
    avg_daily_volume = max(0.0, float(avg_daily_volume or 0.0))
    atr_pct = max(0.0, float(atr_pct or 0.0))
    micro_breakout_5m = _clamp(float(micro_breakout_5m or 0.0), -1.0, 1.0)
    micro_reversal_5m = _clamp(float(micro_reversal_5m or 0.0), -1.0, 1.0)
    micro_volume_burst_5m = _clamp(float(micro_volume_burst_5m or 0.0), -1.0, 1.0)
    microstructure_score = _clamp(float(microstructure_score or 0.0), -1.0, 1.0)
    intraday_bar_count = max(0.0, float(intraday_bar_count or 0.0))
    returns_ewma_vol_20d = max(0.0, float(returns_ewma_vol_20d or 0.0))
    returns_downside_vol_20d = max(0.0, float(returns_downside_vol_20d or 0.0))
    centered_data_quality = _clamp((data_quality_score - 0.5) * 2.0, -1.0, 1.0)
    centered_source_coverage = _clamp((source_coverage - 0.5) * 2.0, -1.0, 1.0)
    returns_vol_penalty = max(
        0.0,
        (returns_ewma_vol_20d - float(cfg.returns_ewma_vol_soft_limit)) / max(float(cfg.returns_ewma_vol_soft_limit), 1e-6),
    )
    returns_downside_penalty = max(
        0.0,
        (returns_downside_vol_20d - float(cfg.returns_downside_vol_soft_limit))
        / max(float(cfg.returns_downside_vol_soft_limit), 1e-6),
    )

    long_score = float(long_row.get("long_score", 0.0) or 0.0)
    trend_vs_ma200 = float(long_row.get("trend_vs_ma200", 0.0) or 0.0)
    mdd_1y = float(long_row.get("mdd_1y", 0.0) or 0.0)
    rebalance_flag = int(long_row.get("rebalance_flag", 0) or 0)

    mid_scale = float(mid_row.get("mid_scale", 0.5) or 0.5)
    trend_slope_60d = float(mid_row.get("trend_slope_60d", 0.0) or 0.0)
    regime_composite = float(mid_row.get("regime_composite", 0.0) or 0.0)
    regime_state = str(mid_row.get("regime_state", "") or "")
    regime_reason = str(mid_row.get("regime_reason", "") or "")
    risk_on = bool(mid_row.get("risk_on", True))
    trailing_pe = float(fundamentals.get("trailing_pe", 0.0) or 0.0)
    forward_pe = float(fundamentals.get("forward_pe", 0.0) or 0.0)
    profit_margin = float(fundamentals.get("profit_margin", 0.0) or 0.0)
    operating_margin = float(fundamentals.get("operating_margin", 0.0) or 0.0)
    revenue_growth = float(fundamentals.get("revenue_growth", 0.0) or 0.0)
    roe = float(fundamentals.get("roe", fundamentals.get("return_on_equity", 0.0)) or 0.0)
    analyst_recommendation_score = float(recommendation.get("recommendation_score", 0.0) or 0.0)
    strong_buy = int(recommendation.get("strong_buy", 0) or 0)
    buy = int(recommendation.get("buy", 0) or 0)
    hold = int(recommendation.get("hold", 0) or 0)
    sell = int(recommendation.get("sell", 0) or 0)
    strong_sell = int(recommendation.get("strong_sell", 0) or 0)
    recommendation_total = int(recommendation.get("recommendation_total", 0) or 0)

    pe_for_scoring = forward_pe if forward_pe > 0 else trailing_pe
    valuation_score = 0.0
    if pe_for_scoring > 0 and not is_commodity_proxy:
        valuation_score = _clamp((float(cfg.valuation_anchor_pe) - pe_for_scoring) / float(cfg.valuation_anchor_pe), -1.0, 1.0)
    if is_commodity_proxy:
        # Commodity proxy ETFs do not map cleanly to equity-style PE/margin/ROE fields.
        # Treat missing fundamentals as neutral instead of structurally bearish.
        margin_score = 0.0
        operating_margin_score = 0.0
        revenue_growth_score = 0.0
        roe_score = 0.0
    else:
        margin_score = _clamp((profit_margin - float(cfg.profit_margin_floor)) / max(float(cfg.profit_margin_floor), 0.01), -1.0, 1.0)
        operating_margin_score = _clamp(
            (operating_margin - float(cfg.operating_margin_floor)) / max(float(cfg.operating_margin_floor), 0.01),
            -1.0,
            1.0,
        )
        revenue_growth_score = _clamp(
            (revenue_growth - float(cfg.revenue_growth_floor)) / max(abs(float(cfg.revenue_growth_floor)), 0.01),
            -1.0,
            1.0,
        )
        roe_score = _clamp((roe - float(cfg.roe_floor)) / max(float(cfg.roe_floor), 0.01), -1.0, 1.0)

    alpha = (
        float(cfg.long_score_weight) * long_score
        + float(cfg.trend_vs_ma200_weight) * trend_vs_ma200
        + float(cfg.mid_scale_weight) * (mid_scale - 0.5)
        + float(cfg.regime_composite_weight) * regime_composite
        + float(cfg.trend_slope_weight) * trend_slope_60d
        + float(cfg.valuation_weight) * valuation_score
        + float(cfg.profit_margin_weight) * margin_score
        + float(cfg.operating_margin_weight) * operating_margin_score
        + float(cfg.revenue_growth_weight) * revenue_growth_score
        + float(cfg.roe_weight) * roe_score
        + float(cfg.recommendation_weight) * analyst_recommendation_score
        + float(cfg.market_sentiment_weight) * float(market_sentiment_score)
        + float(cfg.data_quality_weight) * centered_data_quality
        + float(cfg.source_coverage_weight) * centered_source_coverage
        + float(cfg.microstructure_weight) * microstructure_score
    )

    risk = float(cfg.drawdown_penalty_weight) * abs(min(0.0, mdd_1y))
    risk += float(cfg.missing_ratio_penalty_weight) * missing_ratio
    if rebalance_flag:
        risk += float(cfg.rebalance_penalty)
    if not risk_on or regime_state.upper() in {"RISK_OFF", "HARD_RISK_OFF"}:
        risk += float(cfg.risk_off_penalty)
    if earnings_in_14d:
        risk += float(cfg.earnings_penalty)
    if macro_high_risk:
        risk += float(cfg.macro_penalty)
    if vix >= float(cfg.vix_high_threshold):
        risk += float(cfg.vix_high_penalty)
    elif vix >= float(cfg.vix_elevated_threshold):
        risk += float(cfg.vix_elevated_penalty)
    if trailing_pe > float(cfg.trailing_pe_cap) and trailing_pe > 0 and not is_commodity_proxy:
        risk += min(0.25, (trailing_pe - float(cfg.trailing_pe_cap)) / max(float(cfg.trailing_pe_cap), 1.0))
    if forward_pe > float(cfg.forward_pe_cap) and forward_pe > 0 and not is_commodity_proxy:
        risk += min(0.25, (forward_pe - float(cfg.forward_pe_cap)) / max(float(cfg.forward_pe_cap), 1.0))
    if data_quality_score < float(cfg.low_data_quality_threshold):
        risk += float(cfg.low_data_quality_penalty) * (
            (float(cfg.low_data_quality_threshold) - data_quality_score) / max(float(cfg.low_data_quality_threshold), 1e-6)
        )
    risk += float(cfg.returns_risk_penalty_weight) * min(1.5, 0.60 * returns_vol_penalty + 0.40 * returns_downside_penalty)

    # 这里保留“成本前分数”和“成本后分数”两套口径。
    # 前者反映纯信号强弱，后者才是更接近真实可执行收益的分数。
    score_before_cost = float(alpha - risk)
    cost_penalty = float(cfg.cost_penalty_weight) * _clamp(
        expected_cost_bps / max(float(cfg.cost_penalty_bps_scale), 1e-6),
        0.0,
        1.5,
    )
    score = float(score_before_cost - cost_penalty)
    model_recommendation_score = float(score)
    execution_base = (
        float(cfg.execution_alpha_weight) * _clamp(score_before_cost, -1.0, 1.0)
        + float(cfg.execution_market_sentiment_weight) * _clamp(float(market_sentiment_score), -1.0, 1.0)
        + float(cfg.execution_mid_scale_weight) * _clamp((mid_scale - 0.5) * 2.0, -1.0, 1.0)
        + float(cfg.execution_trend_weight) * _clamp(trend_vs_ma200, -1.0, 1.0)
        + float(cfg.execution_recommendation_weight) * _clamp(analyst_recommendation_score, -1.0, 1.0)
        + float(cfg.execution_data_quality_weight) * centered_data_quality
        + float(cfg.execution_microstructure_weight) * microstructure_score
    )
    execution_penalty = float(cfg.execution_drawdown_penalty_weight) * abs(min(0.0, mdd_1y))
    execution_penalty += float(cfg.execution_missing_ratio_penalty_weight) * missing_ratio
    execution_cost_penalty = float(cfg.execution_cost_penalty_weight) * _clamp(
        expected_cost_bps / max(float(cfg.execution_cost_penalty_bps_scale), 1e-6),
        0.0,
        1.5,
    )
    execution_penalty += execution_cost_penalty
    if rebalance_flag:
        execution_penalty += float(cfg.execution_rebalance_penalty)
    if not risk_on or regime_state.upper() in {"RISK_OFF", "HARD_RISK_OFF"}:
        execution_penalty += float(cfg.execution_risk_off_penalty)
    if earnings_in_14d:
        execution_penalty += float(cfg.execution_event_penalty)
    if macro_high_risk:
        execution_penalty += float(cfg.execution_macro_penalty)
    if data_quality_score < float(cfg.low_data_quality_threshold):
        execution_penalty += float(cfg.execution_low_data_quality_penalty) * (
            (float(cfg.low_data_quality_threshold) - data_quality_score) / max(float(cfg.low_data_quality_threshold), 1e-6)
        )
    execution_penalty += float(cfg.execution_returns_risk_penalty_weight) * min(
        1.5,
        0.55 * returns_vol_penalty + 0.45 * returns_downside_penalty,
    )
    execution_score_before_cost = _clamp(execution_base - (execution_penalty - execution_cost_penalty), -1.0, 1.0)
    execution_score = _clamp(execution_base - execution_penalty, -1.0, 1.0)
    action = "WATCH"
    if rebalance_flag or score <= float(cfg.reduce_threshold):
        action = "REDUCE"
    elif (
        score >= float(cfg.accumulate_threshold)
        and mid_scale >= float(cfg.min_mid_scale_accumulate)
        and trend_vs_ma200 > 0.0
    ):
        action = "ACCUMULATE"
    elif score >= float(cfg.hold_threshold):
        action = "HOLD"
    execution_ready = bool(
        action in {"ACCUMULATE", "HOLD"}
        and execution_score >= float(cfg.execution_ready_threshold)
        and data_quality_score >= float(cfg.execution_min_data_quality)
        and missing_ratio <= float(cfg.execution_max_missing_ratio)
        and not (earnings_in_14d and action == "ACCUMULATE")
    )
    gates_blocked = []
    if earnings_in_14d:
        gates_blocked.append("earnings_window")
    if data_quality_score < float(cfg.execution_min_data_quality):
        gates_blocked.append("low_data_quality")
    if missing_ratio > float(cfg.execution_max_missing_ratio):
        gates_blocked.append("history_missing")
    if expected_cost_bps >= float(cfg.high_expected_cost_bps):
        gates_blocked.append("high_expected_cost")

    decision = SignalDecision(
        symbol=symbol,
        market=market,
        strategy="investment_scoring",
        long_score=float(long_score),
        short_score=0.0,
        total_score=float(score),
        regime_state=dict(mid_row.get("regime_state_v2", {}) or {}),
        gates_passed=[],
        gates_blocked=gates_blocked,
        action=action,
        reasons=[str(regime_reason or "")],
        context={
            "alpha": float(alpha),
            "risk": float(risk),
            "execution_score": float(execution_score),
            "execution_ready": bool(execution_ready),
            "macro_high_risk": bool(macro_high_risk),
            "earnings_in_14d": bool(earnings_in_14d),
            "asset_class": asset_class,
            "asset_theme": asset_theme,
            "score_before_cost": float(score_before_cost),
            "cost_penalty": float(cost_penalty),
            "execution_score_before_cost": float(execution_score_before_cost),
            "execution_cost_penalty": float(execution_cost_penalty),
            "expected_cost_bps": float(expected_cost_bps),
            "spread_proxy_bps": float(spread_proxy_bps),
            "slippage_proxy_bps": float(slippage_proxy_bps),
            "commission_proxy_bps": float(commission_proxy_bps),
            "liquidity_score": float(liquidity_score),
            "avg_daily_dollar_volume": float(avg_daily_dollar_volume),
            "avg_daily_volume": float(avg_daily_volume),
            "atr_pct": float(atr_pct),
            "micro_breakout_5m": float(micro_breakout_5m),
            "micro_reversal_5m": float(micro_reversal_5m),
            "micro_volume_burst_5m": float(micro_volume_burst_5m),
            "microstructure_score": float(microstructure_score),
            "intraday_history_source": str(intraday_history_source or ""),
            "intraday_bar_count": float(intraday_bar_count),
            "returns_ewma_vol_20d": float(returns_ewma_vol_20d),
            "returns_downside_vol_20d": float(returns_downside_vol_20d),
            "market_sentiment_score": float(market_sentiment_score),
            "data_quality_score": float(data_quality_score),
            "source_coverage": float(source_coverage),
            "missing_ratio": float(missing_ratio),
            "history_source": str(history_source or ""),
        },
    )

    return {
        "symbol": symbol,
        "score": float(score),
        "score_before_cost": float(score_before_cost),
        "model_recommendation_score": float(model_recommendation_score),
        "execution_score": float(execution_score),
        "execution_score_before_cost": float(execution_score_before_cost),
        "execution_ready": int(bool(execution_ready)),
        "action": action,
        "alpha": float(alpha),
        "risk": float(risk),
        "cost_penalty": float(cost_penalty),
        "execution_penalty": float(execution_penalty),
        "execution_cost_penalty": float(execution_cost_penalty),
        "expected_cost_bps": float(expected_cost_bps),
        "spread_proxy_bps": float(spread_proxy_bps),
        "slippage_proxy_bps": float(slippage_proxy_bps),
        "commission_proxy_bps": float(commission_proxy_bps),
        "liquidity_score": float(liquidity_score),
        "avg_daily_dollar_volume": float(avg_daily_dollar_volume),
        "avg_daily_volume": float(avg_daily_volume),
        "atr_pct": float(atr_pct),
        "micro_breakout_5m": float(micro_breakout_5m),
        "micro_reversal_5m": float(micro_reversal_5m),
        "micro_volume_burst_5m": float(micro_volume_burst_5m),
        "microstructure_score": float(microstructure_score),
        "intraday_history_source": str(intraday_history_source or ""),
        "intraday_bar_count": int(intraday_bar_count),
        "returns_ewma_vol_20d": float(returns_ewma_vol_20d),
        "returns_downside_vol_20d": float(returns_downside_vol_20d),
        "long_score": float(long_score),
        "trend_vs_ma200": float(trend_vs_ma200),
        "mdd_1y": float(mdd_1y),
        "rebalance_flag": int(rebalance_flag),
        "mid_scale": float(mid_scale),
        "trend_slope_60d": float(trend_slope_60d),
        "regime_state": regime_state,
        "regime_reason": regime_reason,
        "regime_composite": float(regime_composite),
        "risk_on": bool(risk_on),
        "earnings_in_14d": bool(earnings_in_14d),
        "macro_high_risk": bool(macro_high_risk),
        "last_close": float(long_row.get("last_close", mid_row.get("last_close", 0.0)) or 0.0),
        "valuation_score": float(valuation_score),
        "margin_score": float(margin_score),
        "operating_margin_score": float(operating_margin_score),
        "revenue_growth_score": float(revenue_growth_score),
        "roe_score": float(roe_score),
        "recommendation_score": float(analyst_recommendation_score),
        "analyst_recommendation_score": float(analyst_recommendation_score),
        "market_sentiment_score": float(market_sentiment_score),
        "data_quality_score": float(data_quality_score),
        "source_coverage": float(source_coverage),
        "missing_ratio": float(missing_ratio),
        "history_source": str(history_source or ""),
        "strong_buy": int(strong_buy),
        "buy": int(buy),
        "hold": int(hold),
        "sell": int(sell),
        "strong_sell": int(strong_sell),
        "recommendation_total": int(recommendation_total),
        "trailing_pe": float(trailing_pe),
        "forward_pe": float(forward_pe),
        "profit_margin": float(profit_margin),
        "operating_margin": float(operating_margin),
        "revenue_growth": float(revenue_growth),
        "roe": float(roe),
        "sector": str(fundamentals.get("sector", "") or ""),
        "industry": str(fundamentals.get("industry", "") or ""),
        "asset_class": asset_class,
        "asset_theme": asset_theme,
        "country": str(fundamentals.get("country", "") or ""),
        "market": market,
        "signal_decision": decision.to_dict(),
    }


def make_investment_plan(
    row: Dict[str, Any],
    *,
    vix: float,
    cfg: InvestmentPlanConfig | None = None,
) -> Dict[str, Any]:
    cfg = cfg or InvestmentPlanConfig()
    action = str(row.get("action", "WATCH") or "WATCH").upper()
    direction = str(row.get("direction", "LONG") or "LONG").upper()
    base_mult = float(cfg.size_mult_normal)
    if vix >= float(cfg.vix_high_threshold):
        base_mult = float(cfg.size_mult_high)
    elif vix >= float(cfg.vix_elevated_threshold):
        base_mult = float(cfg.size_mult_elevated)

    execution_quality = _clamp(
        1.0 + 0.20 * float(row.get("execution_score", 0.0) or 0.0),
        0.80,
        1.15,
    )
    quality_mult = _clamp(
        (0.6 + float(row.get("mid_scale", 0.5) or 0.5)) * execution_quality,
        float(cfg.min_allocation_mult),
        float(cfg.max_allocation_mult),
    )
    allocation_mult = 0.0
    entry_style = "WAIT"
    notes = []

    if action == "ACCUMULATE":
        allocation_mult = round(base_mult * quality_mult, 2)
        if direction == "SHORT":
            entry_style = f"SHORT_STAGGER_{max(1, int(cfg.staged_entry_parts))}X"
            notes.append("Scale into the short gradually and avoid chasing breakdown candles after an extended move.")
        else:
            entry_style = f"STAGGER_{max(1, int(cfg.staged_entry_parts))}X"
            notes.append(
                f"Use staged entries and prefer pullbacks of about {float(cfg.accumulate_pullback_pct) * 100.0:.1f}% from recent strength."
            )
    elif action == "HOLD":
        allocation_mult = round(min(1.0, base_mult * quality_mult), 2)
        entry_style = "HOLD_SHORT" if direction == "SHORT" else "HOLD_CORE"
        notes.append(
            "Maintain the short and only press it if borrow, liquidity, and market tone remain supportive."
            if direction == "SHORT"
            else "Keep the position and only add selectively if market conditions remain supportive."
        )
    elif action == "REDUCE":
        allocation_mult = 0.0
        entry_style = (
            f"COVER_{int(round(float(cfg.trim_fraction) * 100.0))}PCT"
            if direction == "SHORT"
            else f"TRIM_{int(round(float(cfg.trim_fraction) * 100.0))}PCT"
        )
        notes.append(
            "Short quality has weakened or squeeze risk is rising; cover part of the position and review the thesis."
            if direction == "SHORT"
            else "Trend or drawdown quality has weakened; reduce exposure and review the thesis."
        )
    else:
        notes.append(
            "No short allocation change until downside setup and borrow conditions improve."
            if direction == "SHORT"
            else "No allocation change until trend and regime quality improve."
        )

    if str(row.get("regime_state", "")).upper() in {"RISK_OFF", "HARD_RISK_OFF"}:
        notes.append("Market regime is defensive; keep pacing conservative.")
    if bool(row.get("earnings_in_14d", False)):
        notes.append("Upcoming earnings raise event risk; avoid oversized adds before the event.")
    if bool(row.get("macro_high_risk", False)):
        notes.append("Macro calendar risk is elevated; prefer phased execution.")
    if not bool(row.get("execution_ready", False)) and action in {"ACCUMULATE", "HOLD"}:
        notes.append("Execution quality gate is not fully open; wait for stronger timing or better liquidity.")
    if float(row.get("data_quality_score", 1.0) or 1.0) < 0.55:
        notes.append("Data quality is below the preferred threshold; confirm history coverage before sizing up.")
    if float(row.get("expected_cost_bps", 0.0) or 0.0) >= 45.0:
        notes.append("交易成本代理偏高，优先分批执行并避免在流动性偏薄时追价。")
    if float(row.get("weekly_feedback_score_penalty", 0.0) or 0.0) > 0.0:
        notes.append(
            "Weekly feedback flagged this setup as a repeated weak signal; keep sizing conservative until the signal quality improves."
        )
    if float(row.get("weekly_feedback_expected_cost_bps_add", 0.0) or 0.0) > 0.0:
        notes.append("最近的执行复盘显示该标的存在热点成本偏差，下一轮应更保守地看待执行质量。")

    return {
        "symbol": str(row["symbol"]).upper(),
        "direction": direction,
        "action": action,
        "entry_style": entry_style,
        "allocation_mult": float(allocation_mult),
        "score": float(row.get("score", 0.0) or 0.0),
        "score_before_cost": float(row.get("score_before_cost", row.get("score", 0.0)) or 0.0),
        "model_recommendation_score": float(row.get("model_recommendation_score", row.get("score", 0.0)) or 0.0),
        "execution_score": float(row.get("execution_score", 0.0) or 0.0),
        "execution_score_before_cost": float(row.get("execution_score_before_cost", row.get("execution_score", 0.0)) or 0.0),
        "execution_ready": int(bool(row.get("execution_ready", False))),
        "market_sentiment_score": float(row.get("market_sentiment_score", 0.0) or 0.0),
        "data_quality_score": float(row.get("data_quality_score", 1.0) or 1.0),
        "source_coverage": float(row.get("source_coverage", 1.0) or 1.0),
        "missing_ratio": float(row.get("missing_ratio", 0.0) or 0.0),
        "history_source": str(row.get("history_source", "") or ""),
        "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
        "spread_proxy_bps": float(row.get("spread_proxy_bps", 0.0) or 0.0),
        "slippage_proxy_bps": float(row.get("slippage_proxy_bps", 0.0) or 0.0),
        "commission_proxy_bps": float(row.get("commission_proxy_bps", 0.0) or 0.0),
        "cost_penalty": float(row.get("cost_penalty", 0.0) or 0.0),
        "execution_cost_penalty": float(row.get("execution_cost_penalty", 0.0) or 0.0),
        "liquidity_score": float(row.get("liquidity_score", 0.0) or 0.0),
        "avg_daily_dollar_volume": float(row.get("avg_daily_dollar_volume", 0.0) or 0.0),
        "avg_daily_volume": float(row.get("avg_daily_volume", 0.0) or 0.0),
        "atr_pct": float(row.get("atr_pct", 0.0) or 0.0),
        "history_bar_count": int(row.get("history_bar_count", 0) or 0),
        "history_coverage_ratio": float(row.get("history_coverage_ratio", 0.0) or 0.0),
        "freshness_score": float(row.get("freshness_score", 0.0) or 0.0),
        "shadow_ml_enabled": int(bool(row.get("shadow_ml_enabled", False))),
        "shadow_ml_score": float(row.get("shadow_ml_score", 0.0) or 0.0),
        "shadow_ml_return": float(row.get("shadow_ml_return", 0.0) or 0.0),
        "shadow_ml_positive_prob": float(row.get("shadow_ml_positive_prob", 0.0) or 0.0),
        "shadow_ml_horizon_days": int(row.get("shadow_ml_horizon_days", 0) or 0),
        "shadow_ml_training_samples": int(row.get("shadow_ml_training_samples", 0) or 0),
        "shadow_ml_reason": str(row.get("shadow_ml_reason", "") or ""),
        "weekly_feedback_applied": int(bool(row.get("weekly_feedback_applied", False))),
        "weekly_feedback_score_penalty": float(row.get("weekly_feedback_score_penalty", 0.0) or 0.0),
        "weekly_feedback_execution_penalty": float(row.get("weekly_feedback_execution_penalty", 0.0) or 0.0),
        "weekly_feedback_reason": str(row.get("weekly_feedback_reason", "") or ""),
        "weekly_feedback_repeat_count": int(row.get("weekly_feedback_repeat_count", 0) or 0),
        "weekly_feedback_cooldown_days": int(row.get("weekly_feedback_cooldown_days", 0) or 0),
        "weekly_feedback_expected_cost_bps_add": float(row.get("weekly_feedback_expected_cost_bps_add", 0.0) or 0.0),
        "weekly_feedback_slippage_proxy_bps_add": float(row.get("weekly_feedback_slippage_proxy_bps_add", 0.0) or 0.0),
        "weekly_feedback_penalty_kind": str(row.get("weekly_feedback_penalty_kind", "") or ""),
        "review_window_days": int(cfg.review_window_days),
        "rebalance_window_days": int(cfg.rebalance_window_days),
        "last_close": float(row.get("last_close", 0.0) or 0.0),
        "regime_state": str(row.get("regime_state", "") or ""),
        "regime_reason": str(row.get("regime_reason", "") or ""),
        "mid_scale": float(row.get("mid_scale", 0.5) or 0.5),
        "trend_vs_ma200": float(row.get("trend_vs_ma200", 0.0) or 0.0),
        "mdd_1y": float(row.get("mdd_1y", 0.0) or 0.0),
        "rebalance_flag": int(row.get("rebalance_flag", 0) or 0),
        "signal_decision": dict(row.get("signal_decision", {}) or {}),
        "notes": " ".join(notes),
    }
