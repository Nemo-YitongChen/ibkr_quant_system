from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from math import floor
from typing import Any, Dict, List, Tuple


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts))
    except Exception:
        return None


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


@dataclass
class InvestmentPaperConfig:
    initial_cash: float = 100000.0
    rebalance_frequency: str = "weekly"
    rebalance_weekday: int = 4
    max_holdings: int = 8
    max_single_weight: float = 0.22
    max_sector_weight: float = 0.40
    max_country_weight: float = 1.00
    max_market_weight: float = 1.00
    max_net_exposure: float = 1.00
    max_gross_exposure: float = 1.00
    max_short_exposure: float = 0.35
    correlation_soft_limit: float = 0.62
    correlation_hard_limit: float = 0.82
    correlation_weight_floor: float = 0.35
    dynamic_exposure_floor: float = 0.55
    dynamic_short_exposure_floor: float = 0.60
    stress_loss_soft_limit: float = 0.085
    portfolio_atr_soft_limit: float = 0.055
    portfolio_liquidity_soft_floor: float = 0.42
    market_sentiment_soft_floor: float = -0.15
    sector_concentration_soft_limit: float = 0.40
    stress_index_drop_pct: float = 0.08
    stress_volatility_spike_pct: float = 0.06
    stress_liquidity_shock_pct: float = 0.04
    min_position_weight: float = 0.05
    hold_weight_multiplier: float = 0.80
    short_hold_weight_multiplier: float = 0.75
    allow_fractional_qty: bool = False
    fractional_qty_decimals: int = 4

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentPaperConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def is_rebalance_due(
    last_ts: str | None,
    now: datetime,
    *,
    frequency: str,
    rebalance_weekday: int,
    force: bool = False,
) -> bool:
    if force:
        return True
    last = _parse_iso(last_ts)
    if last is None:
        return True
    freq = str(frequency or "weekly").strip().lower()
    if freq == "monthly":
        return (now.year, now.month) != (last.year, last.month)
    if now.weekday() != int(rebalance_weekday):
        return False
    return (now.date() - last.date()).days >= 7


def _bucket_key(row: Dict[str, Any], field: str) -> str:
    text = str(row.get(field) or "").strip()
    return text or "UNKNOWN"


def _parse_return_series(row: Dict[str, Any]) -> List[float]:
    raw = row.get("return_series_60d_json", row.get("return_series_json", ""))
    if isinstance(raw, list):
        values = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        values = parsed if isinstance(parsed, list) else []
    else:
        return []
    out: List[float] = []
    for value in values:
        number = _to_float(value, 0.0)
        if abs(number) > 0.60:
            continue
        out.append(float(number))
    return out


def _pair_correlation_from_returns(lhs: Dict[str, Any], rhs: Dict[str, Any]) -> float | None:
    lhs_returns = _parse_return_series(lhs)
    rhs_returns = _parse_return_series(rhs)
    sample_size = min(len(lhs_returns), len(rhs_returns))
    if sample_size < 20:
        return None
    lhs_slice = lhs_returns[-sample_size:]
    rhs_slice = rhs_returns[-sample_size:]
    lhs_mean = sum(lhs_slice) / sample_size
    rhs_mean = sum(rhs_slice) / sample_size
    cov = 0.0
    lhs_var = 0.0
    rhs_var = 0.0
    for lhs_value, rhs_value in zip(lhs_slice, rhs_slice):
        lhs_delta = float(lhs_value - lhs_mean)
        rhs_delta = float(rhs_value - rhs_mean)
        cov += lhs_delta * rhs_delta
        lhs_var += lhs_delta * lhs_delta
        rhs_var += rhs_delta * rhs_delta
    if lhs_var <= 1e-12 or rhs_var <= 1e-12:
        return None
    return _clamp(cov / ((lhs_var ** 0.5) * (rhs_var ** 0.5)), -0.98, 0.98)


def _pair_correlation_proxy(lhs: Dict[str, Any], rhs: Dict[str, Any]) -> float:
    # 这里用“市场/国家/行业/波动”做代理相关性。
    # 它不是严格协方差矩阵，但足够在组合构建阶段先拦住明显拥挤的同质仓位。
    if str(lhs.get("symbol") or "").upper() == str(rhs.get("symbol") or "").upper():
        return 1.0
    corr = 0.14
    if _bucket_key(lhs, "market") == _bucket_key(rhs, "market") != "UNKNOWN":
        corr += 0.16
    if _bucket_key(lhs, "country") == _bucket_key(rhs, "country") != "UNKNOWN":
        corr += 0.08
    if _bucket_key(lhs, "sector") == _bucket_key(rhs, "sector") != "UNKNOWN":
        corr += 0.26
    if _bucket_key(lhs, "industry") == _bucket_key(rhs, "industry") != "UNKNOWN":
        corr += 0.12
    if _bucket_key(lhs, "asset_class") == _bucket_key(rhs, "asset_class") != "UNKNOWN":
        corr += 0.05
    if _bucket_key(lhs, "asset_theme") == _bucket_key(rhs, "asset_theme") != "UNKNOWN":
        corr += 0.05
    atr_mix = min(0.08, (_to_float(lhs.get("atr_pct")) + _to_float(rhs.get("atr_pct"))) * 0.90)
    corr += atr_mix
    corr += max(0.0, 0.65 - _to_float(lhs.get("data_quality_score"), 1.0)) * 0.05
    corr += max(0.0, 0.65 - _to_float(rhs.get("data_quality_score"), 1.0)) * 0.05
    return _clamp(corr, 0.05, 0.98)


def _pair_correlation(lhs: Dict[str, Any], rhs: Dict[str, Any]) -> float:
    # returns-based 相关性优先用于真实收益率足够的标的；数据不够时再回退到代理相关性。
    returns_corr = _pair_correlation_from_returns(lhs, rhs)
    if returns_corr is not None:
        return float(returns_corr)
    return _pair_correlation_proxy(lhs, rhs)


def _weighted_pair_correlation(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
) -> tuple[float, float]:
    symbols = [str(symbol).upper() for symbol, weight in weights.items() if abs(_to_float(weight)) > 0.0]
    if len(symbols) <= 1:
        return 0.0, 0.0
    weighted_sum = 0.0
    weight_total = 0.0
    max_pair = 0.0
    for idx, lhs_symbol in enumerate(symbols):
        lhs_weight = abs(_to_float(weights.get(lhs_symbol), 0.0))
        lhs_row = rows_by_symbol.get(lhs_symbol, {})
        for rhs_symbol in symbols[idx + 1 :]:
            rhs_weight = abs(_to_float(weights.get(rhs_symbol), 0.0))
            rhs_row = rows_by_symbol.get(rhs_symbol, {})
            pair_weight = lhs_weight * rhs_weight
            if pair_weight <= 0.0:
                continue
            pair_corr = _pair_correlation(lhs_row, rhs_row)
            weighted_sum += pair_corr * pair_weight
            weight_total += pair_weight
            max_pair = max(max_pair, pair_corr)
    if weight_total <= 0.0:
        return 0.0, max_pair
    return float(weighted_sum / weight_total), float(max_pair)


def _symbol_correlation_to_portfolio(
    symbol: str,
    row: Dict[str, Any],
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
) -> float:
    weighted_sum = 0.0
    weight_total = 0.0
    for other_symbol, other_weight in weights.items():
        if str(other_symbol).upper() == str(symbol).upper():
            continue
        abs_weight = abs(_to_float(other_weight, 0.0))
        if abs_weight <= 0.0:
            continue
        pair_corr = _pair_correlation(row, rows_by_symbol.get(str(other_symbol).upper(), {}))
        weighted_sum += pair_corr * abs_weight
        weight_total += abs_weight
    if weight_total <= 0.0:
        return 0.0
    return float(weighted_sum / weight_total)


def _returns_based_portfolio_metrics(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    series_map: Dict[str, List[float]] = {}
    for symbol, weight in weights.items():
        if abs(_to_float(weight, 0.0)) <= 0.0:
            continue
        series = _parse_return_series(rows_by_symbol.get(str(symbol).upper(), {}))
        if len(series) >= 20:
            series_map[str(symbol).upper()] = series
    if not series_map:
        return {
            "enabled": False,
            "symbol_count": 0,
            "portfolio_ewma_vol_1d": 0.0,
            "portfolio_downside_vol_1d": 0.0,
            "portfolio_var_95_1d": 0.0,
        }
    sample_size = min(len(series) for series in series_map.values())
    sample_size = min(sample_size, 40)
    if sample_size < 20:
        return {
            "enabled": False,
            "symbol_count": len(series_map),
            "portfolio_ewma_vol_1d": 0.0,
            "portfolio_downside_vol_1d": 0.0,
            "portfolio_var_95_1d": 0.0,
        }
    portfolio_returns: List[float] = []
    for idx in range(sample_size):
        portfolio_ret = 0.0
        for symbol, series in series_map.items():
            portfolio_ret += _to_float(weights.get(symbol), 0.0) * float(series[-sample_size + idx])
        portfolio_returns.append(float(portfolio_ret))
    ewma_lambda = 0.94
    ewma_var = 0.0
    ewma_weight = 0.0
    downside_sq = 0.0
    downside_count = 0
    for ret in portfolio_returns:
        ewma_var = float(ewma_lambda * ewma_var + (1.0 - ewma_lambda) * (ret * ret))
        ewma_weight = float(ewma_lambda * ewma_weight + (1.0 - ewma_lambda))
        if ret < 0.0:
            downside_sq += float(ret * ret)
            downside_count += 1
    ewma_vol = (ewma_var / ewma_weight) ** 0.5 if ewma_weight > 0.0 else 0.0
    downside_vol = (downside_sq / max(1, downside_count)) ** 0.5 if downside_count > 0 else 0.0
    sorted_returns = sorted(portfolio_returns)
    tail_index = max(0, int(len(sorted_returns) * 0.05) - 1)
    var_95 = max(0.0, -float(sorted_returns[tail_index])) if sorted_returns else 0.0
    return {
        "enabled": True,
        "symbol_count": len(series_map),
        "sample_size": int(sample_size),
        "portfolio_ewma_vol_1d": float(ewma_vol),
        "portfolio_downside_vol_1d": float(downside_vol),
        "portfolio_var_95_1d": float(var_95),
    }


def _merge_stress_with_returns_metrics(stress: Dict[str, Any], returns_metrics: Dict[str, Any]) -> Dict[str, Any]:
    if not bool(returns_metrics.get("enabled", False)):
        return stress
    scenarios = {str(key): dict(value) for key, value in dict(stress.get("scenarios", {}) or {}).items()}
    ewma_vol = float(returns_metrics.get("portfolio_ewma_vol_1d", 0.0) or 0.0)
    downside_vol = float(returns_metrics.get("portfolio_downside_vol_1d", 0.0) or 0.0)
    var_95 = float(returns_metrics.get("portfolio_var_95_1d", 0.0) or 0.0)
    scenario_floors = {
        "index_drop": max(var_95 * 1.10, downside_vol * 1.35),
        "volatility_spike": max(ewma_vol * 1.85, var_95),
        "liquidity_shock": max(ewma_vol * 1.15, var_95 * 0.85),
    }
    worst_name = str(stress.get("worst_scenario", "") or "")
    worst_loss = float(stress.get("worst_loss", 0.0) or 0.0)
    for scenario, floor_loss in scenario_floors.items():
        row = scenarios.setdefault(scenario, {"label": "", "loss": 0.0, "long_loss": 0.0, "short_loss": 0.0})
        row["loss"] = float(max(_to_float(row.get("loss"), 0.0), float(floor_loss)))
        if float(row.get("loss", 0.0) or 0.0) > worst_loss:
            worst_name = scenario
            worst_loss = float(row.get("loss", 0.0) or 0.0)
    return {
        "scenarios": scenarios,
        "worst_loss": float(worst_loss),
        "worst_scenario": worst_name,
        "worst_scenario_label": str(scenarios.get(worst_name, {}).get("label", "") or ""),
    }


def _stress_loss_pct(row: Dict[str, Any], scenario: str, cfg: InvestmentPaperConfig) -> float:
    # stress 场景先用现有候选字段做代理损失。
    # 目标是给组合层一个稳定、可解释的收缩信号，而不是替代正式风险引擎。
    atr_pct = _clamp(_to_float(row.get("atr_pct"), 0.0), 0.0, 0.25)
    mdd_1y = _clamp(abs(min(0.0, _to_float(row.get("mdd_1y"), 0.0))), 0.0, 0.60)
    liquidity_score = _clamp(_to_float(row.get("liquidity_score"), 0.60), 0.0, 1.0)
    data_quality_score = _clamp(_to_float(row.get("data_quality_score"), 0.80), 0.0, 1.0)
    market_sentiment_score = _clamp(_to_float(row.get("market_sentiment_score"), 0.0), -1.0, 1.0)
    expected_cost = max(0.0, _to_float(row.get("expected_cost_bps"), 0.0)) / 10000.0
    direction = str(row.get("direction") or "LONG").strip().upper()
    illiquid_penalty = max(0.0, 0.60 - liquidity_score)
    low_quality_penalty = max(0.0, 0.65 - data_quality_score)
    defensive_penalty = max(0.0, -market_sentiment_score)
    if scenario == "index_drop":
        loss = float(cfg.stress_index_drop_pct) * (
            0.70
            + 2.20 * atr_pct
            + 0.55 * mdd_1y
            + 0.25 * defensive_penalty
        )
        return _clamp(loss, 0.0, 0.25)
    if scenario == "volatility_spike":
        loss = float(cfg.stress_volatility_spike_pct) * (
            0.60
            + 4.00 * atr_pct
            + 0.45 * mdd_1y
            + 0.30 * low_quality_penalty
        )
        if direction == "SHORT":
            loss *= 1.15
        return _clamp(loss, 0.0, 0.22)
    loss = float(cfg.stress_liquidity_shock_pct) + 1.60 * expected_cost + 0.10 * illiquid_penalty + 0.06 * low_quality_penalty
    if direction == "SHORT":
        loss *= 1.10
    return _clamp(loss, 0.0, 0.18)


def _evaluate_stress_scenarios(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
    cfg: InvestmentPaperConfig,
) -> Dict[str, Any]:
    scenarios = {
        "index_drop": "指数下跌",
        "volatility_spike": "波动抬升",
        "liquidity_shock": "流动性恶化",
    }
    results: Dict[str, Dict[str, Any]] = {}
    worst_name = ""
    worst_loss = 0.0
    for scenario, label in scenarios.items():
        pnl = 0.0
        long_loss = 0.0
        short_loss = 0.0
        for symbol, signed_weight in weights.items():
            row = rows_by_symbol.get(str(symbol).upper(), {})
            loss_pct = _stress_loss_pct(row, scenario, cfg)
            abs_weight = abs(_to_float(signed_weight, 0.0))
            direction = str(row.get("direction") or ("SHORT" if signed_weight < 0 else "LONG")).strip().upper()
            if scenario == "index_drop":
                contribution = abs_weight * loss_pct if direction == "SHORT" else -abs_weight * loss_pct
            else:
                contribution = -abs_weight * loss_pct
            pnl += contribution
            if direction == "SHORT":
                short_loss += abs_weight * loss_pct
            else:
                long_loss += abs_weight * loss_pct
        loss = max(0.0, -pnl)
        if loss > worst_loss:
            worst_loss = loss
            worst_name = scenario
        results[scenario] = {
            "label": label,
            "loss": float(loss),
            "long_loss": float(long_loss),
            "short_loss": float(short_loss),
        }
    return {
        "scenarios": results,
        "worst_loss": float(worst_loss),
        "worst_scenario": worst_name,
        "worst_scenario_label": str(results.get(worst_name, {}).get("label", "") or ""),
    }


def _largest_bucket_share(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
    field: str,
) -> float:
    bucket_weights: Dict[str, float] = {}
    gross = 0.0
    known_gross = 0.0
    for symbol, signed_weight in weights.items():
        abs_weight = abs(_to_float(signed_weight, 0.0))
        gross += abs_weight
        key = _bucket_key(rows_by_symbol.get(str(symbol).upper(), {}), field)
        if key == "UNKNOWN":
            continue
        known_gross += abs_weight
        bucket_weights[key] = float(bucket_weights.get(key, 0.0) + abs_weight)
    if gross <= 0.0 or known_gross <= 0.0:
        return 0.0
    return max(bucket_weights.values(), default=0.0) / known_gross


def _portfolio_signal_metrics(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    weights: Dict[str, float],
) -> Dict[str, float]:
    gross = sum(abs(_to_float(weight, 0.0)) for weight in weights.values())
    if gross <= 0.0:
        return {
            "avg_atr_pct": 0.0,
            "avg_liquidity_score": 0.0,
            "avg_market_sentiment_score": 0.0,
            "avg_data_quality_score": 0.0,
        }
    weighted_atr = 0.0
    weighted_liquidity = 0.0
    weighted_sentiment = 0.0
    weighted_quality = 0.0
    for symbol, signed_weight in weights.items():
        row = rows_by_symbol.get(str(symbol).upper(), {})
        abs_weight = abs(_to_float(signed_weight, 0.0))
        weighted_atr += abs_weight * _to_float(row.get("atr_pct"), 0.0)
        weighted_liquidity += abs_weight * _to_float(row.get("liquidity_score"), 0.60)
        weighted_sentiment += abs_weight * _to_float(row.get("market_sentiment_score"), 0.0)
        weighted_quality += abs_weight * _to_float(row.get("data_quality_score"), 0.80)
    return {
        "avg_atr_pct": float(weighted_atr / gross),
        "avg_liquidity_score": float(weighted_liquidity / gross),
        "avg_market_sentiment_score": float(weighted_sentiment / gross),
        "avg_data_quality_score": float(weighted_quality / gross),
    }


def _provisional_weights(
    chosen: List[Tuple[str, float, float, str]],
    cfg: InvestmentPaperConfig,
) -> Dict[str, float]:
    raw_total = sum(item[2] for item in chosen)
    if raw_total <= 0.0:
        return {}
    weights: Dict[str, float] = {}
    for symbol, _score, raw_weight, direction in chosen:
        desired_weight = min(float(cfg.max_single_weight), raw_weight / raw_total)
        weights[symbol] = float(-desired_weight if str(direction).upper() == "SHORT" else desired_weight)
    return weights


def _build_risk_overlay(
    rows_by_symbol: Dict[str, Dict[str, Any]],
    chosen: List[Tuple[str, float, float, str]],
    cfg: InvestmentPaperConfig,
) -> Dict[str, Any]:
    # 先对“准备入选”的候选股做一次组合级体检，再决定这轮最多能给多少净/总敞口。
    # 这样可以把相关性、stress、流动性这些组合问题，提前折算成可执行的资金预算。
    provisional = _provisional_weights(chosen, cfg)
    avg_pair_correlation, max_pair_correlation = _weighted_pair_correlation(rows_by_symbol, provisional)
    returns_metrics = _returns_based_portfolio_metrics(rows_by_symbol, provisional)
    stress = _merge_stress_with_returns_metrics(_evaluate_stress_scenarios(rows_by_symbol, provisional, cfg), returns_metrics)
    signal_metrics = _portfolio_signal_metrics(rows_by_symbol, provisional)
    top_sector_share = _largest_bucket_share(rows_by_symbol, provisional, "sector")
    corr_penalty = max(
        0.0,
        (avg_pair_correlation - float(cfg.correlation_soft_limit)) / max(1.0 - float(cfg.correlation_soft_limit), 1e-6),
    )
    stress_penalty = max(
        0.0,
        (float(stress.get("worst_loss", 0.0) or 0.0) - float(cfg.stress_loss_soft_limit))
        / max(1.0 - float(cfg.stress_loss_soft_limit), 1e-6),
    )
    atr_penalty = max(
        0.0,
        (float(signal_metrics.get("avg_atr_pct", 0.0) or 0.0) - float(cfg.portfolio_atr_soft_limit))
        / max(float(cfg.portfolio_atr_soft_limit), 1e-6),
    )
    liquidity_penalty = max(
        0.0,
        (float(cfg.portfolio_liquidity_soft_floor) - float(signal_metrics.get("avg_liquidity_score", 0.0) or 0.0))
        / max(float(cfg.portfolio_liquidity_soft_floor), 1e-6),
    )
    sentiment_penalty = max(
        0.0,
        (float(cfg.market_sentiment_soft_floor) - float(signal_metrics.get("avg_market_sentiment_score", 0.0) or 0.0))
        / max(abs(float(cfg.market_sentiment_soft_floor)), 1e-6),
    )
    concentration_penalty = max(
        0.0,
        (top_sector_share - float(cfg.sector_concentration_soft_limit))
        / max(1.0 - float(cfg.sector_concentration_soft_limit), 1e-6),
    )
    scale_penalty = (
        0.26 * corr_penalty
        + 0.30 * stress_penalty
        + 0.14 * atr_penalty
        + 0.12 * liquidity_penalty
        + 0.08 * sentiment_penalty
        + 0.10 * concentration_penalty
    )
    dynamic_scale = _clamp(1.0 - scale_penalty, float(cfg.dynamic_exposure_floor), 1.0)
    dynamic_short_scale = _clamp(
        1.0 - (0.45 * stress_penalty + 0.25 * liquidity_penalty + 0.10 * corr_penalty),
        float(cfg.dynamic_short_exposure_floor),
        1.0,
    )
    notes: List[str] = []
    if avg_pair_correlation > float(cfg.correlation_soft_limit):
        notes.append("相关性偏高，降低组合总敞口。")
    if float(stress.get("worst_loss", 0.0) or 0.0) > float(cfg.stress_loss_soft_limit):
        notes.append(f"最差 stress 场景为{str(stress.get('worst_scenario_label') or '未知')}，降低组合总敞口。")
    if float(signal_metrics.get("avg_liquidity_score", 0.0) or 0.0) < float(cfg.portfolio_liquidity_soft_floor):
        notes.append("组合平均流动性偏弱，降低总敞口并保留现金。")
    if float(signal_metrics.get("avg_market_sentiment_score", 0.0) or 0.0) < float(cfg.market_sentiment_soft_floor):
        notes.append("市场情绪偏弱，降低净多头敞口。")
    if bool(returns_metrics.get("enabled", False)):
        notes.append("returns-based 风险度量已启用，优先用真实收益率修正相关性与 stress。")
    return {
        "enabled": True,
        "candidate_count": int(len(chosen)),
        "returns_based_enabled": bool(returns_metrics.get("enabled", False)),
        "returns_based_symbol_count": int(returns_metrics.get("symbol_count", 0) or 0),
        "returns_based_sample_size": int(returns_metrics.get("sample_size", 0) or 0),
        "correlation_source": "returns+proxy_fallback" if bool(returns_metrics.get("enabled", False)) else "proxy_only",
        "returns_based_portfolio_vol_1d": float(returns_metrics.get("portfolio_ewma_vol_1d", 0.0) or 0.0),
        "returns_based_downside_vol_1d": float(returns_metrics.get("portfolio_downside_vol_1d", 0.0) or 0.0),
        "returns_based_var_95_1d": float(returns_metrics.get("portfolio_var_95_1d", 0.0) or 0.0),
        "avg_pair_correlation": float(avg_pair_correlation),
        "max_pair_correlation": float(max_pair_correlation),
        "top_sector_share": float(top_sector_share),
        "avg_atr_pct": float(signal_metrics.get("avg_atr_pct", 0.0) or 0.0),
        "avg_liquidity_score": float(signal_metrics.get("avg_liquidity_score", 0.0) or 0.0),
        "avg_market_sentiment_score": float(signal_metrics.get("avg_market_sentiment_score", 0.0) or 0.0),
        "avg_data_quality_score": float(signal_metrics.get("avg_data_quality_score", 0.0) or 0.0),
        "dynamic_scale": float(dynamic_scale),
        "dynamic_short_scale": float(dynamic_short_scale),
        "dynamic_net_exposure": float(max(0.0, float(cfg.max_net_exposure) * dynamic_scale)),
        "dynamic_gross_exposure": float(max(0.0, float(cfg.max_gross_exposure) * dynamic_scale)),
        "dynamic_short_exposure": float(
            max(0.0, min(float(cfg.max_gross_exposure) * dynamic_scale, float(cfg.max_short_exposure) * dynamic_short_scale))
        ),
        "stress_scenarios": dict(stress.get("scenarios", {}) or {}),
        "stress_worst_loss": float(stress.get("worst_loss", 0.0) or 0.0),
        "stress_worst_scenario": str(stress.get("worst_scenario", "") or ""),
        "stress_worst_scenario_label": str(stress.get("worst_scenario_label", "") or ""),
        "notes": notes,
    }


def build_target_allocations(
    ranked_rows: List[Dict[str, Any]],
    plan_rows: List[Dict[str, Any]],
    *,
    cfg: InvestmentPaperConfig,
    return_details: bool = False,
) -> Dict[str, float] | tuple[Dict[str, float], Dict[str, Any]]:
    by_symbol = {str(row["symbol"]).upper(): dict(row) for row in ranked_rows}
    candidates: List[Tuple[str, float, float, str]] = []
    for plan in plan_rows:
        symbol = str(plan["symbol"]).upper()
        action = str(plan.get("action", "WATCH") or "WATCH").upper()
        if action not in {"ACCUMULATE", "HOLD"}:
            continue
        allocation = float(plan.get("allocation_mult", 0.0) or 0.0)
        if allocation <= 0.0:
            continue
        direction = str(by_symbol.get(symbol, {}).get("direction", plan.get("direction", "LONG")) or "LONG").upper()
        if direction == "SHORT" and not _is_truthy(by_symbol.get(symbol, {}).get("execution_ready", plan.get("execution_ready", False))):
            continue
        score = float(
            by_symbol.get(symbol, {}).get(
                "model_recommendation_score",
                by_symbol.get(symbol, {}).get("score", 0.0),
            )
            or 0.0
        )
        if direction == "SHORT":
            mult = float(cfg.short_hold_weight_multiplier) if action == "HOLD" else 1.0
        else:
            mult = float(cfg.hold_weight_multiplier) if action == "HOLD" else 1.0
        candidates.append((symbol, abs(score), allocation * mult, direction))

    candidates.sort(key=lambda item: item[1], reverse=True)
    chosen = candidates[: max(1, int(cfg.max_holdings))]
    raw_total = sum(item[2] for item in chosen)
    if raw_total <= 0:
        empty = {}
        return (empty, {"enabled": False, "notes": ["没有满足条件的候选股，风险层未启用。"]}) if return_details else empty

    weights: Dict[str, float] = {}
    sector_weights: Dict[str, float] = {}
    country_weights: Dict[str, float] = {}
    market_weights: Dict[str, float] = {}
    risk_overlay = _build_risk_overlay(by_symbol, chosen, cfg)
    # 这里不直接改原始配置，而是把动态敞口当作本次组合构建的临时预算。
    # 这样 dashboard 和周报能看到“为什么这一轮没打满”，协作者也不容易把静态参数当成最终真相。
    max_net_exposure = max(0.0, min(1.5, _to_float(risk_overlay.get("dynamic_net_exposure"), float(cfg.max_net_exposure))))
    max_gross_exposure = max(0.0, min(2.0, _to_float(risk_overlay.get("dynamic_gross_exposure"), float(cfg.max_gross_exposure))))
    max_short_exposure = max(
        0.0,
        min(
            max_gross_exposure,
            _to_float(risk_overlay.get("dynamic_short_exposure"), float(cfg.max_short_exposure)),
        ),
    )
    long_exposure = 0.0
    short_exposure = 0.0
    gross_exposure = 0.0
    correlation_reduced_symbols: List[str] = []
    for symbol, _score, raw_weight, direction in chosen:
        row = by_symbol.get(symbol, {})
        sector = str(row.get("sector") or "UNKNOWN").strip() or "UNKNOWN"
        country = str(row.get("country") or "UNKNOWN").strip() or "UNKNOWN"
        market = str(row.get("market") or "UNKNOWN").strip() or "UNKNOWN"
        desired_weight = min(float(cfg.max_single_weight), raw_weight / raw_total)
        pair_correlation = _symbol_correlation_to_portfolio(symbol, row, by_symbol, weights)
        if pair_correlation > float(cfg.correlation_soft_limit):
            correlation_scale = _clamp(
                1.0
                - (
                    (pair_correlation - float(cfg.correlation_soft_limit))
                    / max(1.0 - float(cfg.correlation_soft_limit), 1e-6)
                ),
                float(cfg.correlation_weight_floor),
                1.0,
            )
            desired_weight *= correlation_scale
            correlation_reduced_symbols.append(symbol)
        if pair_correlation >= float(cfg.correlation_hard_limit):
            desired_weight *= max(float(cfg.correlation_weight_floor) * 0.70, 0.20)
        remaining_sector = 1.0 if sector == "UNKNOWN" else max(
            0.0,
            float(cfg.max_sector_weight) - float(sector_weights.get(sector, 0.0)),
        )
        remaining_country = 1.0 if country == "UNKNOWN" else max(
            0.0,
            float(cfg.max_country_weight) - float(country_weights.get(country, 0.0)),
        )
        remaining_market = 1.0 if market == "UNKNOWN" else max(
            0.0,
            float(cfg.max_market_weight) - float(market_weights.get(market, 0.0)),
        )
        remaining_gross = max(0.0, max_gross_exposure - gross_exposure)
        if direction == "SHORT":
            remaining_direction = max(0.0, max_short_exposure - short_exposure)
            signed_weight = -min(desired_weight, remaining_sector, remaining_country, remaining_market, remaining_gross, remaining_direction)
        else:
            remaining_direction = max(0.0, max_net_exposure - long_exposure)
            signed_weight = min(desired_weight, remaining_sector, remaining_country, remaining_market, remaining_gross, remaining_direction)
        if abs(signed_weight) >= float(cfg.min_position_weight):
            weights[symbol] = float(signed_weight)
            if sector != "UNKNOWN":
                sector_weights[sector] = float(sector_weights.get(sector, 0.0) + abs(signed_weight))
            if country != "UNKNOWN":
                country_weights[country] = float(country_weights.get(country, 0.0) + abs(signed_weight))
            if market != "UNKNOWN":
                market_weights[market] = float(market_weights.get(market, 0.0) + abs(signed_weight))
            gross_exposure += abs(signed_weight)
            if signed_weight < 0:
                short_exposure += abs(signed_weight)
            else:
                long_exposure += abs(signed_weight)

    final_weights = {symbol: float(weight) for symbol, weight in weights.items() if abs(float(weight)) > 0.0}
    final_avg_corr, final_max_corr = _weighted_pair_correlation(by_symbol, final_weights)
    final_returns_metrics = _returns_based_portfolio_metrics(by_symbol, final_weights)
    final_stress = _merge_stress_with_returns_metrics(_evaluate_stress_scenarios(by_symbol, final_weights, cfg), final_returns_metrics)
    risk_overlay.update(
        {
            "applied_net_exposure": float(long_exposure),
            "applied_short_exposure": float(short_exposure),
            "applied_gross_exposure": float(gross_exposure),
            "final_avg_pair_correlation": float(final_avg_corr),
            "final_max_pair_correlation": float(final_max_corr),
            "final_returns_based_enabled": bool(final_returns_metrics.get("enabled", False)),
            "final_returns_based_symbol_count": int(final_returns_metrics.get("symbol_count", 0) or 0),
            "final_returns_based_sample_size": int(final_returns_metrics.get("sample_size", 0) or 0),
            "final_returns_based_portfolio_vol_1d": float(final_returns_metrics.get("portfolio_ewma_vol_1d", 0.0) or 0.0),
            "final_returns_based_downside_vol_1d": float(final_returns_metrics.get("portfolio_downside_vol_1d", 0.0) or 0.0),
            "final_returns_based_var_95_1d": float(final_returns_metrics.get("portfolio_var_95_1d", 0.0) or 0.0),
            "final_stress_scenarios": dict(final_stress.get("scenarios", {}) or {}),
            "final_stress_worst_loss": float(final_stress.get("worst_loss", 0.0) or 0.0),
            "final_stress_worst_scenario": str(final_stress.get("worst_scenario", "") or ""),
            "final_stress_worst_scenario_label": str(final_stress.get("worst_scenario_label", "") or ""),
            "correlation_reduced_symbols": correlation_reduced_symbols[:12],
            "selected_symbols": list(final_weights.keys()),
        }
    )
    return (final_weights, risk_overlay) if return_details else final_weights


def _mark_positions(
    positions: Dict[str, Dict[str, Any]],
    price_map: Dict[str, float],
) -> Tuple[List[Dict[str, Any]], float]:
    rows: List[Dict[str, Any]] = []
    equity = 0.0
    for symbol, pos in positions.items():
        qty = float(pos.get("qty", 0.0) or 0.0)
        if abs(qty) <= 0:
            continue
        last_price = float(price_map.get(symbol, pos.get("last_price", pos.get("cost_basis", 0.0)) or 0.0))
        market_value = qty * last_price
        equity += market_value
        rows.append(
            {
                "symbol": symbol,
                "qty": float(qty),
                "cost_basis": float(pos.get("cost_basis", 0.0) or 0.0),
                "last_price": float(last_price),
                "market_value": float(market_value),
            }
        )
    return rows, float(equity)


def _floor_trade_qty(raw_qty: float, cfg: InvestmentPaperConfig) -> float:
    if bool(cfg.allow_fractional_qty):
        precision = max(0, int(cfg.fractional_qty_decimals or 0))
        scale = float(10**precision)
        return float(floor(max(0.0, float(raw_qty)) * scale) / scale)
    return float(floor(max(0.0, float(raw_qty))))


def simulate_rebalance(
    positions: Dict[str, Dict[str, Any]],
    *,
    cash: float,
    price_map: Dict[str, float],
    target_weights: Dict[str, float],
    cfg: InvestmentPaperConfig | None = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], float, float]:
    cfg = cfg or InvestmentPaperConfig()
    positions = {str(sym).upper(): dict(pos) for sym, pos in (positions or {}).items()}
    trades: List[Dict[str, Any]] = []

    marked_rows, current_market_value = _mark_positions(positions, price_map)
    total_equity = float(cash + current_market_value)
    target_qty_map: Dict[str, float] = {}
    for symbol, weight in target_weights.items():
        price = float(price_map.get(symbol, 0.0) or 0.0)
        if price <= 0.0:
            continue
        raw_qty = abs(float(total_equity) * float(weight)) / price
        qty = _floor_trade_qty(raw_qty, cfg)
        target_qty_map[str(symbol).upper()] = float(qty if float(weight) >= 0.0 else -qty)

    def _reason(current_qty: float, target_qty: float, action: str) -> str:
        if action == "SELL":
            if current_qty <= 0 and target_qty < current_qty:
                return "target_add_short"
            if current_qty > 0 and target_qty < 0:
                return "flip_to_short"
            return "rebalance_down" if target_qty > 0 else "exit_or_short"
        if current_qty < 0 and target_qty >= 0:
            return "cover_or_flip_long"
        if current_qty < 0 and target_qty > current_qty:
            return "cover_partial"
        return "target_add"

    for symbol in sorted(set(positions) | set(target_qty_map)):
        price = float(price_map.get(symbol, positions.get(symbol, {}).get("last_price", positions.get(symbol, {}).get("cost_basis", 0.0)) or 0.0))
        if price <= 0.0:
            continue
        pos = positions.setdefault(symbol, {"qty": 0.0, "cost_basis": price, "last_price": price})
        current_qty = float(pos.get("qty", 0.0) or 0.0)
        target_qty = float(target_qty_map.get(symbol, 0.0) or 0.0)
        sell_delta = current_qty - target_qty
        if sell_delta <= 0.0:
            pos["last_price"] = price
            continue
        sell_qty = _floor_trade_qty(sell_delta, cfg)
        if sell_qty <= 0.0:
            pos["last_price"] = price
            continue
        cash += sell_qty * price
        pos["qty"] = float(current_qty - sell_qty)
        pos["last_price"] = price
        if abs(float(pos["qty"])) <= 1e-9:
            pos["qty"] = 0.0
        trades.append(
            {
                "symbol": symbol,
                "action": "SELL",
                "qty": float(sell_qty),
                "price": float(price),
                "trade_value": float(sell_qty * price),
                "reason": _reason(current_qty, target_qty, "SELL"),
            }
        )

    for symbol in sorted(set(positions) | set(target_qty_map)):
        price = float(price_map.get(symbol, positions.get(symbol, {}).get("last_price", positions.get(symbol, {}).get("cost_basis", 0.0)) or 0.0))
        if price <= 0.0:
            continue
        pos = positions.setdefault(symbol, {"qty": 0.0, "cost_basis": price, "last_price": price})
        current_qty = float(pos.get("qty", 0.0) or 0.0)
        target_qty = float(target_qty_map.get(symbol, 0.0) or 0.0)
        buy_delta = target_qty - current_qty
        if buy_delta <= 0.0:
            pos["last_price"] = price
            continue
        buy_qty = _floor_trade_qty(min(buy_delta, cash / price if price > 0 else 0.0), cfg)
        if buy_qty <= 0.0:
            pos["last_price"] = price
            continue
        trade_value = buy_qty * price
        new_qty = float(current_qty + buy_qty)
        if current_qty >= 0.0 and new_qty > 0.0:
            old_cost = current_qty * float(pos.get("cost_basis", price) or price)
            pos["cost_basis"] = float((old_cost + trade_value) / new_qty) if new_qty > 0 else float(price)
        elif current_qty < 0.0 and new_qty < 0.0:
            pos["cost_basis"] = float(pos.get("cost_basis", price) or price)
        else:
            pos["cost_basis"] = float(price)
        pos["qty"] = new_qty
        pos["last_price"] = price
        cash -= trade_value
        trades.append(
            {
                "symbol": symbol,
                "action": "BUY",
                "qty": float(buy_qty),
                "price": float(price),
                "trade_value": float(trade_value),
                "reason": _reason(current_qty, target_qty, "BUY"),
            }
        )

    final_rows, final_market_value = _mark_positions(positions, price_map)
    final_equity = float(cash + final_market_value)
    if final_equity > 0:
        for row in final_rows:
            row["weight"] = float(row["market_value"] / final_equity)
            positions[row["symbol"]]["weight"] = row["weight"]
    return positions, trades, float(cash), float(final_equity)
