from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        data = json.loads(value)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _signal_decision_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
    direct = row.get("signal_decision")
    if isinstance(direct, dict):
        return dict(direct)
    return _parse_json_dict(row.get("signal_decision_json"))


def _execution_intent_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
    direct = row.get("execution_intent")
    if isinstance(direct, dict):
        return dict(direct)
    return _parse_json_dict(row.get("execution_intent_json"))


def _decision_reason_text(decision: Dict[str, Any]) -> str:
    reasons = [str(x).strip() for x in list(decision.get("reasons", []) or []) if str(x).strip()]
    return " | ".join(reasons[:3])


def _decision_gate_text(decision: Dict[str, Any]) -> str:
    passed = [str(x).strip() for x in list(decision.get("gates_passed", []) or []) if str(x).strip()]
    blocked = [str(x).strip() for x in list(decision.get("gates_blocked", []) or []) if str(x).strip()]
    parts: List[str] = []
    if passed:
        parts.append("passed=" + ",".join(passed[:4]))
    if blocked:
        parts.append("blocked=" + ",".join(blocked[:4]))
    return " ; ".join(parts)


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8") as f:
            f.write("")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            fieldnames.append(str(key))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_md(
    path: str,
    title: str,
    ranked: List[Dict[str, Any]],
    plans: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> None:
    def regime_explainer(state: str) -> str:
        state = str(state or "").upper()
        if state == "BULL":
            return "中期环境偏顺风，趋势与动量更支持延续，但仍需关注波动是否放大。"
        if state == "RISK_ON":
            return "环境整体可交易，说明趋势、动量、波动、回撤四项综合后仍偏正面。"
        if state == "RISK_OFF":
            return "环境偏防守，通常意味着波动、回撤或趋势至少一项在拖累胜率，仓位宜保守。"
        if state == "HARD_RISK_OFF":
            return "环境明显逆风，趋势承接弱或波动与回撤压力较大，应优先控制风险。"
        if state == "WARMUP":
            return "样本仍在预热，当前环境判断只可作弱参考。"
        return "这是对趋势、动量、波动、回撤四项的综合解释，不代表单一参数或固定阈值。"

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [f"# {title}", "", f"- Generated: {ts}", ""]

    summary = context.get("summary", {})
    market_profile = dict(context.get("market_profile", {}) or {})
    market_structure = dict(context.get("market_structure", {}) or {})
    adaptive_strategy = dict(context.get("adaptive_strategy", {}) or {})
    lines.append("## Market Summary")
    lines.append(
        f"- 市场波动: VIX={float(summary.get('vix', 0.0) or 0.0):.2f}；"
        f"宏观风险={'高' if summary.get('macro_high_risk') else '正常'}；"
        f"财报窗口标记={int(summary.get('earnings_risk_count', 0) or 0)}"
    )
    lines.append(
        f"- 样本覆盖: candidates={int(summary.get('candidate_count', 0) or 0)}；"
        f"features_ok={int(summary.get('features_ok', 0) or 0)}；"
        f"ranked={int(summary.get('ranked_count', 0) or 0)}；"
        f"plans={int(summary.get('plan_count', 0) or 0)}"
    )
    lines.append(
        "- Regime 说明: 报告中的环境判断只解释四个维度的综合含义"
        " 趋势、动量、波动、回撤；不展示具体阈值、权重或调参细节。"
    )
    lines.append("")
    lines.append("## Top Candidates (Ranked)")
    if not ranked:
        lines.append("- (no candidates)")
    else:
        for r in ranked:
            lines.append(
                f"- **{r['symbol']}** score={r['score']:.3f} dir={r['direction']} "
                f"channel={r.get('channel', '')} "
                f"short_sig={float(r.get('short_sig', 0.0) or 0.0):.3f} "
                f"total_sig={float(r.get('total_sig', 0.0) or 0.0):.3f} "
                f"stability={float(r.get('stability', 0.0) or 0.0):.3f} "
                f"alpha={r['alpha']:.3f} risk={r['risk']:.3f} "
                f"market_view={r.get('regime_state', '')} "
                f"tradable={r.get('tradable_status', '') or 'N/A'}"
            )
            lines.append(f"  环境解释: {regime_explainer(str(r.get('regime_state', '')))}")
            if r.get("blocked_reason"):
                lines.append(f"  执行限制: {r.get('blocked_reason', '')}")
    lines.append("")
    lines.append("## Trade Plans")
    if not plans:
        lines.append("- (no plans)")
    else:
        for p in plans:
            lines.append(
                f"- **{p['symbol']}** {p['direction']} entry={p['entry']} stop={p['stop']} "
                f"tp={p['take_profit']} size_mult={p['size_mult_suggest']} "
                f"channel={p.get('channel', '')} stability={float(p.get('stability', 0.0) or 0.0):.3f} "
                f"market_view={p.get('regime_state', '')} tradable={p.get('tradable_status', '') or 'N/A'}  \\\n"
                f"  {p['notes']} 环境解释: {regime_explainer(str(p.get('regime_state', '')))}"
            )
            if p.get("blocked_reason"):
                lines.append(f"  执行限制: {p.get('blocked_reason', '')}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_investment_md(
    path: str,
    title: str,
    ranked: List[Dict[str, Any]],
    plans: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> None:
    def regime_explainer(state: str) -> str:
        state = str(state or "").upper()
        if state == "BULL":
            return "中期环境偏顺风，趋势与动量更支持资产继续走强。"
        if state == "RISK_ON":
            return "环境整体可承受风险，说明趋势、动量、波动、回撤综合后仍偏正面。"
        if state == "RISK_OFF":
            return "环境偏防守，适合降低加仓速度并提高持仓质量要求。"
        if state == "HARD_RISK_OFF":
            return "环境明显逆风，应优先保护本金并收缩风险暴露。"
        if state == "WARMUP":
            return "样本仍在预热，环境判断暂时只作弱参考。"
        return "这是对趋势、动量、波动、回撤四个维度的综合解释，不展示具体参数。"

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [f"# {title}", "", f"- Generated: {ts}", ""]

    summary = context.get("summary", {})
    market_profile = dict(context.get("market_profile", {}) or {})
    market_structure = dict(context.get("market_structure", {}) or {})
    adaptive_strategy = dict(context.get("adaptive_strategy", {}) or {})
    lines.append("## Market Summary")
    lines.append(
        f"- 市场波动: VIX={float(summary.get('vix', 0.0) or 0.0):.2f}；"
        f"宏观风险={'高' if summary.get('macro_high_risk') else '正常'}；"
        f"财报窗口标记={int(summary.get('earnings_risk_count', 0) or 0)}"
    )
    lines.append(
        f"- 样本覆盖: candidates={int(summary.get('candidate_count', 0) or 0)}；"
        f"mid_ok={int(summary.get('mid_ok', 0) or 0)}；"
        f"long_ok={int(summary.get('long_ok', 0) or 0)}；"
        f"ranked={int(summary.get('ranked_count', 0) or 0)}；"
        f"plans={int(summary.get('plan_count', 0) or 0)}；"
        f"recommendation_coverage={int(summary.get('recommendation_coverage', 0) or 0)}"
    )
    if summary.get("broad_ranked_count") or summary.get("deep_pool_count") or summary.get("enrichment_count"):
        lines.append(
            f"- 分层扫描: broad_ranked={int(summary.get('broad_ranked_count', 0) or 0)}；"
            f"deep_pool={int(summary.get('deep_pool_count', 0) or 0)}；"
            f"enrichment={int(summary.get('enrichment_count', 0) or 0)}；"
            f"scanner_added={int(summary.get('scanner_candidate_count', 0) or 0)}；"
            f"history_workers={int(summary.get('history_workers', 0) or 0)}"
        )
    if (
        summary.get("avg_data_quality_score") is not None
        or summary.get("avg_source_coverage") is not None
        or summary.get("avg_missing_ratio") is not None
    ):
        lines.append(
            f"- 数据质量: avg_score={float(summary.get('avg_data_quality_score', 0.0) or 0.0):.2f}；"
            f"source_cov={float(summary.get('avg_source_coverage', 0.0) or 0.0):.2f}；"
            f"missing_ratio={float(summary.get('avg_missing_ratio', 0.0) or 0.0):.2f}；"
            f"low_quality={int(summary.get('low_quality_count', 0) or 0)}；"
            f"top_low_quality={int(summary.get('ranked_low_quality_count', 0) or 0)}"
        )
    if (
        summary.get("avg_expected_cost_bps") is not None
        or summary.get("high_cost_count") is not None
        or summary.get("low_liquidity_count") is not None
    ):
        lines.append(
            f"- 交易成本代理: avg_cost={float(summary.get('avg_expected_cost_bps', 0.0) or 0.0):.1f}bps；"
            f"spread={float(summary.get('avg_spread_proxy_bps', 0.0) or 0.0):.1f}bps；"
            f"slippage={float(summary.get('avg_slippage_proxy_bps', 0.0) or 0.0):.1f}bps；"
            f"commission={float(summary.get('avg_commission_proxy_bps', 0.0) or 0.0):.1f}bps；"
            f"high_cost={int(summary.get('high_cost_count', 0) or 0)}；"
            f"low_liquidity={int(summary.get('low_liquidity_count', 0) or 0)}"
        )
    if summary.get("shadow_ml_enabled") or summary.get("shadow_ml_reason"):
        lines.append(
            f"- Shadow ML: enabled={'Y' if bool(summary.get('shadow_ml_enabled')) else 'N'}；"
            f"version={summary.get('shadow_ml_model_version', '') or '-'}；"
            f"features={int(summary.get('shadow_ml_feature_count', 0) or 0)}；"
            f"reason={summary.get('shadow_ml_reason', '') or '-'}；"
            f"samples={int(summary.get('shadow_ml_training_samples', 0) or 0)}；"
            f"horizon={int(summary.get('shadow_ml_horizon_days', 0) or 0)}d；"
            f"avg_score={float(summary.get('shadow_ml_avg_score', 0.0) or 0.0):.2f}；"
            f"avg_ret={float(summary.get('shadow_ml_avg_return', 0.0) or 0.0):.3f}；"
            f"avg_prob={float(summary.get('shadow_ml_avg_positive_prob', 0.0) or 0.0):.2f}"
        )
    if (
        summary.get("avg_microstructure_score") is not None
        or summary.get("avg_returns_ewma_vol_20d") is not None
    ):
        lines.append(
            f"- 微观结构/收益风险: micro={float(summary.get('avg_microstructure_score', 0.0) or 0.0):.2f}；"
            f"breakout_5m={float(summary.get('avg_micro_breakout_5m', 0.0) or 0.0):.2f}；"
            f"reversal_5m={float(summary.get('avg_micro_reversal_5m', 0.0) or 0.0):.2f}；"
            f"volume_burst_5m={float(summary.get('avg_micro_volume_burst_5m', 0.0) or 0.0):.2f}；"
            f"ewma_vol_20d={float(summary.get('avg_returns_ewma_vol_20d', 0.0) or 0.0):.3f}；"
            f"downside_vol_20d={float(summary.get('avg_returns_downside_vol_20d', 0.0) or 0.0):.3f}"
        )
    if summary.get("weekly_feedback_enabled") or summary.get("weekly_feedback_penalty_symbols"):
        lines.append(
            f"- Weekly Feedback: enabled={'Y' if bool(summary.get('weekly_feedback_enabled')) else 'N'}；"
            f"penalty_symbols={int(summary.get('weekly_feedback_penalty_symbols', 0) or 0)}；"
            f"signal_symbols={int(summary.get('weekly_feedback_signal_penalty_symbols', summary.get('configured_signal_penalty_symbols', 0)) or 0)}；"
            f"execution_symbols={int(summary.get('weekly_feedback_execution_penalty_symbols', summary.get('configured_execution_penalty_symbols', 0)) or 0)}；"
            f"applied_candidates={int(summary.get('weekly_feedback_applied_candidates', 0) or 0)}"
        )
    if summary.get("short_candidate_count"):
        lines.append(f"- 短书扫描: short_candidates={int(summary.get('short_candidate_count', 0) or 0)}")
    data_warning = str(summary.get("data_warning", "") or "").strip()
    if data_warning:
        lines.append(f"- 数据提醒: {data_warning}")
    if summary.get("market_leaders") or summary.get("market_laggards"):
        lines.append(
            f"- 市场风向: leaders={summary.get('market_leaders', '') or 'N/A'}；"
            f"laggards={summary.get('market_laggards', '') or 'N/A'}"
        )
    if summary.get("market_sentiment_label") or summary.get("market_sentiment_guidance"):
        lines.append(
            f"- 市场情绪: {summary.get('market_sentiment_label', '') or 'N/A'} "
            f"score={float(summary.get('market_sentiment_score', 0.0) or 0.0):.2f}；"
            f"{summary.get('market_sentiment_guidance', '') or ''}"
        )
    if (
        summary.get("breadth_positive_ratio") is not None
        or summary.get("breadth_dispersion_1d") is not None
        or summary.get("leadership_spread_5d") is not None
    ):
        lines.append(
            f"- 市场结构: breadth_up_ratio={float(summary.get('breadth_positive_ratio', 0.0) or 0.0):.2f}；"
            f"dispersion_1d={float(summary.get('breadth_dispersion_1d', 0.0) or 0.0):.3f}；"
            f"leadership_spread_5d={float(summary.get('leadership_spread_5d', 0.0) or 0.0):.3f}"
        )
    market_news = list(summary.get("market_news", []) or [])
    if market_news:
        digest = []
        for item in market_news[:3]:
            title = str(item.get("title", "") or "").strip()
            publisher = str(item.get("publisher", "") or "").strip()
            if title:
                digest.append(f"{publisher + ':' if publisher else ''}{title}")
        if digest:
            lines.append("- 市场消息: " + " | ".join(digest))
    macro_indicators = dict(summary.get("macro_indicators", {}) or {})
    if macro_indicators:
        macro_parts = []
        for key in ("fed_funds", "unemployment_rate", "cpi"):
            if key in macro_indicators and macro_indicators.get(key) is not None:
                macro_parts.append(f"{key}={float(macro_indicators.get(key) or 0.0):.2f}")
        if macro_parts:
            lines.append(f"- 宏观指标: {'; '.join(macro_parts)}")
    if market_profile:
        market_name = str(market_profile.get("name", "") or "").strip()
        benchmark = str(market_profile.get("benchmark_symbol", "") or "").strip()
        market_tz = str(market_profile.get("timezone", "") or "").strip()
        style_bias = str(market_profile.get("style_bias", "") or "").strip()
        if market_name or benchmark or market_tz or style_bias:
            lines.append(
                f"- 市场画像: name={market_name or 'N/A'}；benchmark={benchmark or 'N/A'}；"
                f"timezone={market_tz or 'N/A'}；style_bias={style_bias or 'N/A'}"
            )
        notes = [str(note).strip() for note in list(market_profile.get("notes", []) or []) if str(note).strip()]
        if notes:
            lines.append("- 市场备注: " + " ".join(notes))
    if market_structure:
        costs = dict(market_structure.get("costs", {}) or {})
        order_rules = dict(market_structure.get("order_rules", {}) or {})
        account_rules = dict(market_structure.get("account_rules", {}) or {})
        prefs = dict(market_structure.get("portfolio_preferences", {}) or {})
        total_one_side_bps = float(costs.get("total_one_side_bps", 0.0) or 0.0)
        lines.append(
            f"- 市场约束: scope={market_structure.get('market_scope', '') or 'N/A'}；"
            f"settlement={account_rules.get('standard_settlement_cycle', '') or 'N/A'}；"
            f"buy_lot={int(order_rules.get('buy_lot_multiple', 0) or 0) or 1}；"
            f"day_turnaround={'Y' if bool(order_rules.get('day_turnaround_allowed', True)) else 'N'}；"
            f"price_limit={float(order_rules.get('price_limit_pct', 0.0) or 0.0):.1f}%"
            if float(order_rules.get("price_limit_pct", 0.0) or 0.0) > 0.0
            else f"- 市场约束: scope={market_structure.get('market_scope', '') or 'N/A'}；"
            f"settlement={account_rules.get('standard_settlement_cycle', '') or 'N/A'}；"
            f"buy_lot={int(order_rules.get('buy_lot_multiple', 0) or 0) or 1}；"
            f"day_turnaround={'Y' if bool(order_rules.get('day_turnaround_allowed', True)) else 'N'}；"
            "price_limit=N/A"
        )
        if total_one_side_bps > 0.0:
            lines.append(
                f"- 市场费用底座: one_side={total_one_side_bps:.2f}bps；"
                f"broker={float(costs.get('broker_commission_bps', 0.0) or 0.0):.2f}；"
                f"stamp={float(costs.get('stamp_duty_bps_per_side', 0.0) or 0.0):.2f}；"
                f"trading_fee={float(costs.get('trading_fee_bps_per_side', 0.0) or 0.0):.3f}；"
                f"settlement_fee={float(costs.get('settlement_fee_bps_per_side', 0.0) or 0.0):.3f}"
            )
        if float(account_rules.get("pdt_margin_equity_min", 0.0) or 0.0) > 0.0:
            lines.append(
                f"- 账户门槛: intraday_margin_equity_min={float(account_rules.get('pdt_margin_equity_min', 0.0) or 0.0):.0f}"
            )
        if prefs:
            lines.append(
                f"- 组合偏好: instruments={','.join(str(x) for x in list(prefs.get('preferred_instruments', []) or [])) or 'N/A'}；"
                f"signal_freq={prefs.get('recommended_signal_frequency', '') or 'N/A'}；"
                f"rebalance_freq={prefs.get('recommended_rebalance_frequency', '') or 'N/A'}；"
                f"max_rebalances_per_week={int(prefs.get('max_rebalances_per_week', 0) or 0)}"
            )
        ms_notes = [str(note).strip() for note in list(market_structure.get("notes", []) or []) if str(note).strip()]
        if ms_notes:
            lines.append("- 市场结构备注: " + " ".join(ms_notes))
    if adaptive_strategy:
        regime = dict(adaptive_strategy.get("regime", {}) or {})
        relative_strength = dict(adaptive_strategy.get("relative_strength", {}) or {})
        pullback = dict(adaptive_strategy.get("pullback", {}) or {})
        defensive = dict(adaptive_strategy.get("defensive", {}) or {})
        execution = dict(adaptive_strategy.get("execution", {}) or {})
        lines.append(
            f"- 策略框架: {adaptive_strategy.get('name', '') or 'N/A'}；"
            f"{adaptive_strategy.get('display_name', '') or 'N/A'}；"
            f"bias={adaptive_strategy.get('implementation_bias', '') or 'N/A'}"
        )
        if adaptive_strategy.get("objective"):
            lines.append(f"- 策略目标: {adaptive_strategy.get('objective', '')}")
        lines.append(
            f"- Regime 规则: uptrend=Close>MA{int(regime.get('long_ma_window', 0) or 0)} & "
            f"MA{int(regime.get('short_ma_window', 0) or 0)}>MA{int(regime.get('long_ma_window', 0) or 0)}；"
            f"sideways_high_vol=|Close/MA{int(regime.get('long_ma_window', 0) or 0)}-1|<={float(regime.get('near_long_ma_band_pct', 0.0) or 0.0) * 100.0:.1f}% "
            f"& Vol{int(regime.get('short_vol_window', 0) or 0)}>{float(regime.get('high_vol_multiple_vs_long', 0.0) or 0.0):.2f}xVol{int(regime.get('long_vol_window', 0) or 0)}；"
            f"downtrend=Close<MA{int(regime.get('long_ma_window', 0) or 0)} & MA{int(regime.get('short_ma_window', 0) or 0)}<MA{int(regime.get('long_ma_window', 0) or 0)}"
        )
        lines.append(
            f"- 相对强弱: R{int(relative_strength.get('lookback_long', 0) or 0)} / "
            f"R{int(relative_strength.get('lookback_mid', 0) or 0)} / "
            f"Vol{int(relative_strength.get('volatility_window', 0) or 0)}；"
            f"weights={float(relative_strength.get('long_weight', 0.0) or 0.0):.2f}/"
            f"{float(relative_strength.get('mid_weight', 0.0) or 0.0):.2f}/"
            f"-{float(relative_strength.get('volatility_penalty_weight', 0.0) or 0.0):.2f}；"
            f"price_filter=MA{int(relative_strength.get('price_filter_ma_window', 0) or 0)}"
        )
        lines.append(
            f"- 回撤模块: above_MA{int(pullback.get('trend_ma_window', 0) or 0)}；"
            f"R{int(pullback.get('long_strength_lookback', 0) or 0)} top "
            f"{float(pullback.get('long_strength_top_pct', 0.0) or 0.0) * 100.0:.0f}% ; "
            f"R{int(pullback.get('short_pullback_lookback', 0) or 0)} bottom "
            f"{float(pullback.get('short_pullback_bottom_pct', 0.0) or 0.0) * 100.0:.0f}% ; "
            f"size_scale={float(pullback.get('size_scale_vs_trend', 0.0) or 0.0):.2f}"
        )
        lines.append(
            f"- 防守模式: small={float(defensive.get('small_max_gross', 0.0) or 0.0) * 100.0:.0f}% gross；"
            f"medium={float(defensive.get('medium_max_gross', 0.0) or 0.0) * 100.0:.0f}% gross；"
            f"large={float(defensive.get('large_max_gross', 0.0) or 0.0) * 100.0:.0f}% gross；"
            f"entry_threshold_raise={float(defensive.get('raise_entry_threshold_pct', 0.0) or 0.0) * 100.0:.0f}%"
        )
        lines.append(
            f"- 执行节奏: signal={execution.get('signal_frequency', '') or 'N/A'}；"
            f"rebalance={execution.get('rebalance_frequency', '') or 'N/A'}；"
            f"high_vol_max={int(execution.get('max_rebalances_per_week_high_vol', 0) or 0)}/week；"
            f"entry_delay={int(execution.get('entry_delay_min_minutes', 0) or 0)}-"
            f"{int(execution.get('entry_delay_max_minutes', 0) or 0)}m after open"
        )
        rollout = [dict(item) for item in list(adaptive_strategy.get("rollout", []) or []) if isinstance(item, dict)]
        if rollout:
            lines.append("- 实施顺序: " + " | ".join(f"{row.get('name', '')}:{row.get('scope', '')}" for row in rollout[:3]))
        strategy_notes = [str(note).strip() for note in list(adaptive_strategy.get("notes", []) or []) if str(note).strip()]
        if strategy_notes:
            lines.append("- 策略备注: " + " ".join(strategy_notes))
    lines.append(
        "- Regime 说明: 这里只解释趋势、动量、波动、回撤四个维度的综合含义，不展示具体参数和阈值。"
    )
    lines.append("")

    lines.append("## Top Investment Candidates")
    if not ranked:
        lines.append("- (no candidates)")
    else:
        for r in ranked:
            signal_decision = _signal_decision_for_row(r)
            lines.append(
                f"- **{r['symbol']}** score={float(r.get('score', 0.0) or 0.0):.3f} "
                f"raw={float(r.get('score_before_cost', r.get('score', 0.0)) or 0.0):.3f} "
                f"dir={str(r.get('direction', 'LONG') or 'LONG').upper()} "
                f"model={float(r.get('model_recommendation_score', r.get('score', 0.0)) or 0.0):.3f} "
                f"exec={float(r.get('execution_score', 0.0) or 0.0):.3f} "
                f"cost={float(r.get('expected_cost_bps', 0.0) or 0.0):.1f}bps "
                f"cost_pen={float(r.get('cost_penalty', 0.0) or 0.0):.3f} "
                f"ready={'Y' if bool(r.get('execution_ready', False)) else 'N'} "
                f"action={r.get('action', 'WATCH')} "
                f"long_score={float(r.get('long_score', 0.0) or 0.0):.3f} "
                f"trend_vs_ma200={float(r.get('trend_vs_ma200', 0.0) or 0.0):.3f} "
                f"mid_scale={float(r.get('mid_scale', 0.0) or 0.0):.3f} "
                f"mdd_1y={float(r.get('mdd_1y', 0.0) or 0.0):.3f} "
                f"dq={float(r.get('data_quality_score', 0.0) or 0.0):.2f} "
                f"src_cov={float(r.get('source_coverage', 0.0) or 0.0):.2f} "
                f"miss={float(r.get('missing_ratio', 0.0) or 0.0):.2f} "
                f"ml={float(r.get('shadow_ml_score', 0.0) or 0.0):.2f} "
                f"ml_prob={float(r.get('shadow_ml_positive_prob', 0.0) or 0.0):.2f} "
                f"fb_pen={float(r.get('weekly_feedback_score_penalty', 0.0) or 0.0):.2f} "
                f"source={str(r.get('history_source', '') or '-')}"
                " "
                f"market_view={r.get('regime_state', '')}"
            )
            lines.append(f"  环境解释: {regime_explainer(str(r.get('regime_state', '')))}")
            if float(r.get("weekly_feedback_score_penalty", 0.0) or 0.0) > 0.0:
                lines.append(
                    f"  Weekly Feedback: penalty={float(r.get('weekly_feedback_score_penalty', 0.0) or 0.0):.2f} "
                    f"repeat={int(r.get('weekly_feedback_repeat_count', 0) or 0)} "
                    f"cooldown_days={int(r.get('weekly_feedback_cooldown_days', 0) or 0)} "
                    f"reason={str(r.get('weekly_feedback_reason', '') or '-')}"
                )
            if float(r.get("expected_cost_bps", 0.0) or 0.0) > 0.0:
                lines.append(
                    "  成本代理: "
                    f"spread={float(r.get('spread_proxy_bps', 0.0) or 0.0):.1f}bps "
                    f"slippage={float(r.get('slippage_proxy_bps', 0.0) or 0.0):.1f}bps "
                    f"commission={float(r.get('commission_proxy_bps', 0.0) or 0.0):.1f}bps "
                    f"liq={float(r.get('avg_daily_dollar_volume', 0.0) or 0.0):.0f}"
                )
            gate_text = _decision_gate_text(signal_decision)
            reason_text = _decision_reason_text(signal_decision)
            if gate_text:
                lines.append(f"  决策门控: {gate_text}")
            if reason_text:
                lines.append(f"  决策原因: {reason_text}")
            if int(r.get("bt_signal_samples", 0) or 0) > 0:
                lines.append(
                    "  历史持有期: "
                    f"30d avg={float(r.get('bt_avg_ret_30d', 0.0) or 0.0):.3f} hit={float(r.get('bt_hit_rate_30d', 0.0) or 0.0):.2f}; "
                    f"60d avg={float(r.get('bt_avg_ret_60d', 0.0) or 0.0):.3f} hit={float(r.get('bt_hit_rate_60d', 0.0) or 0.0):.2f}; "
                    f"90d avg={float(r.get('bt_avg_ret_90d', 0.0) or 0.0):.3f} hit={float(r.get('bt_hit_rate_90d', 0.0) or 0.0):.2f}"
                )
            sector = str(r.get("sector", "") or "").strip()
            industry = str(r.get("industry", "") or "").strip()
            asset_class = str(r.get("asset_class", "") or "").strip()
            asset_theme = str(r.get("asset_theme", "") or "").strip()
            market_cap = float(r.get("market_cap", 0.0) or 0.0)
            trailing_pe = float(r.get("trailing_pe", 0.0) or 0.0)
            if asset_class or asset_theme or sector or industry or market_cap > 0 or trailing_pe > 0:
                lines.append(
                    f"  基本面/资产概览: class={asset_class or 'equity'} theme={asset_theme or 'N/A'} "
                    f"sector={sector or 'N/A'} industry={industry or 'N/A'} "
                    f"market_cap={market_cap:.0f} trailing_pe={trailing_pe:.2f} "
                    f"profit_margin={float(r.get('profit_margin', 0.0) or 0.0):.2f} "
                    f"operating_margin={float(r.get('operating_margin', 0.0) or 0.0):.2f} "
                    f"revenue_growth={float(r.get('revenue_growth', 0.0) or 0.0):.2f}"
                )
            if int(r.get("recommendation_total", 0) or 0) > 0:
                lines.append(
                    "  分析师预期: "
                    f"rec_score={float(r.get('recommendation_score', 0.0) or 0.0):.2f} "
                    f"SB={int(r.get('strong_buy', 0) or 0)} "
                    f"B={int(r.get('buy', 0) or 0)} "
                    f"H={int(r.get('hold', 0) or 0)} "
                    f"S={int(r.get('sell', 0) or 0)} "
                    f"SS={int(r.get('strong_sell', 0) or 0)}"
                )
            if r.get("earnings_in_14d"):
                lines.append("  事件提示: 财报窗口临近，建议降低单次加仓幅度。")
            if r.get("rebalance_flag"):
                lines.append("  调仓提示: 长期趋势或回撤已触发调仓复核。")
    lines.append("")

    lines.append("## Investment Plans")
    if not plans:
        lines.append("- (no plans)")
    else:
        for p in plans:
            signal_decision = _signal_decision_for_row(p)
            lines.append(
                f"- **{p['symbol']}** action={p['action']} "
                f"dir={str(p.get('direction', 'LONG') or 'LONG').upper()} "
                f"model={float(p.get('model_recommendation_score', p.get('score', 0.0)) or 0.0):.3f} "
                f"exec={float(p.get('execution_score', 0.0) or 0.0):.3f} "
                f"raw={float(p.get('score_before_cost', p.get('score', 0.0)) or 0.0):.3f} "
                f"cost={float(p.get('expected_cost_bps', 0.0) or 0.0):.1f}bps "
                f"ready={'Y' if bool(p.get('execution_ready', False)) else 'N'} "
                f"entry_style={p['entry_style']} "
                f"allocation_mult={float(p.get('allocation_mult', 0.0) or 0.0):.2f} "
                f"dq={float(p.get('data_quality_score', 0.0) or 0.0):.2f} "
                f"src_cov={float(p.get('source_coverage', 0.0) or 0.0):.2f} "
                f"miss={float(p.get('missing_ratio', 0.0) or 0.0):.2f} "
                f"ml={float(p.get('shadow_ml_score', 0.0) or 0.0):.2f} "
                f"ml_prob={float(p.get('shadow_ml_positive_prob', 0.0) or 0.0):.2f} "
                f"review_days={int(p.get('review_window_days', 0) or 0)} "
                f"rebalance_days={int(p.get('rebalance_window_days', 0) or 0)} "
                f"market_view={p.get('regime_state', '')}"
            )
            lines.append(f"  {p.get('notes', '')} 环境解释: {regime_explainer(str(p.get('regime_state', '')))}")
            gate_text = _decision_gate_text(signal_decision)
            reason_text = _decision_reason_text(signal_decision)
            if gate_text:
                lines.append(f"  决策门控: {gate_text}")
            if reason_text:
                lines.append(f"  决策原因: {reason_text}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
