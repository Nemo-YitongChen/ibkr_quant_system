from __future__ import annotations

from typing import Any, Callable, Dict, List

from ..common.adaptive_strategy import (
    adaptive_strategy_context,
    adaptive_strategy_effective_controls_human_note,
    load_adaptive_strategy,
)
from ..common.account_profile import load_account_profiles, resolved_account_profile_summary
from ..common.market_structure import load_market_structure, market_structure_summary
from ..common.markets import resolve_market_code
from .review_weekly_common_support import (
    BASE_DIR,
    _attribution_control_split_text,
    _runtime_config_paths_for_market,
)


def _strategy_effective_controls_note(*summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        note = str(
            payload.get("strategy_effective_controls_human_note")
            or payload.get("strategy_effective_controls_note")
            or ""
        ).strip()
        if note:
            return note
        controls = dict(payload.get("strategy_effective_controls") or {})
        note = adaptive_strategy_effective_controls_human_note(controls)
        if note:
            return note
    return ""


def _active_market_profile_note(*summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        note = str(payload.get("adaptive_strategy_active_market_note") or "").strip()
        if note:
            return note
    return ""


def _active_market_strategy_field(field_name: str, *summaries: Dict[str, Any]) -> str:
    for summary in summaries:
        payload = dict(summary or {})
        value = str(payload.get(field_name) or "").strip()
        if value:
            return value
    return ""


def _execution_gate_summary(execution_summary: Dict[str, Any]) -> str:
    execution_summary = dict(execution_summary or {})
    blocked_total = int(execution_summary.get("blocked_order_count", 0) or 0)
    if blocked_total <= 0:
        return ""
    labels = [
        ("边际收益", int(execution_summary.get("blocked_edge_order_count", 0) or 0)),
        ("流动性", int(execution_summary.get("blocked_liquidity_order_count", 0) or 0)),
        ("风险告警", int(execution_summary.get("blocked_risk_alert_order_count", 0) or 0)),
        ("人工复核", int(execution_summary.get("blocked_manual_review_order_count", 0) or 0)),
        ("机会过滤", int(execution_summary.get("blocked_opportunity_order_count", 0) or 0)),
        ("质量过滤", int(execution_summary.get("blocked_quality_order_count", 0) or 0)),
        ("热点惩罚", int(execution_summary.get("blocked_hotspot_penalty_order_count", 0) or 0)),
    ]
    detail = "，".join(f"{label} {count}" for label, count in labels if count > 0)
    if detail:
        return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发（{detail}）。"
    return f"另外有 {blocked_total} 笔计划单因执行 gate 暂未下发。"


def _weekly_strategy_note(
    *,
    market_rules: Dict[str, Any],
    account_profile: Dict[str, Any],
    adaptive_strategy: Dict[str, Any],
    opportunity_summary: Dict[str, Any],
    market_sentiment: Dict[str, Any],
    strategy_effective_controls_note: str = "",
    execution_gate_summary: str = "",
) -> str:
    if bool(market_rules.get("research_only", False)):
        return "当前市场仍以研究为主，周度结论优先用于研究跟踪，不直接放大自动交易动作。"
    defensive_wait_count = int(opportunity_summary.get("adaptive_strategy_wait_count", 0) or 0)
    control_note = str(strategy_effective_controls_note or "").strip()
    gate_note = str(execution_gate_summary or "").strip()
    if control_note:
        parts = [control_note]
        if defensive_wait_count > 0:
            parts.append(f"同时有 {defensive_wait_count} 个新开仓机会因防守环境被降级为观察。")
        if gate_note:
            parts.append(gate_note)
        return " ".join(parts)
    if defensive_wait_count > 0:
        note = f"本周有 {defensive_wait_count} 个新开仓机会因防守环境被降级为观察，先不把回撤信号直接转成加仓动作。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    sentiment_label = str(market_sentiment.get("label", "") or "").strip().upper()
    if sentiment_label == "DEFENSIVE":
        note = "本周市场处于防守环境，周报应优先解释仓位保护、减速加仓和执行保守化。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    if bool(market_rules.get("small_account_rule_active", False)):
        preferred = "/".join(
            str(item).upper()
            for item in list(market_rules.get("small_account_preferred_asset_classes", []) or [])
            if str(item).strip()
        ) or "ETF"
        note = f"当前账户仍在小资金规则范围内，本周先按 {preferred} 优先级解释机会与执行，不扩展到低流动性单股。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    profile_label = str(account_profile.get("label", "") or account_profile.get("name", "") or "").strip()
    if profile_label:
        note = f"当前按 {profile_label} 档位运行，周报优先关注这档账户适配的仓位节奏、持仓数和执行密度。"
        if gate_note:
            return f"{note} {gate_note}"
        return note
    strategy_name = str(adaptive_strategy.get("name", "") or "ACM-RS").strip()
    note = f"当前按 {strategy_name} 自适应中频框架运行，周报优先复盘市场状态、执行成本和信号质量。"
    if gate_note:
        return f"{note} {gate_note}"
    return note


def _augment_summary_rows_with_strategy_context(
    summary_rows: List[Dict[str, Any]],
    *,
    broker_summary_rows: List[Dict[str, Any]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    latest_report_dir_fn: Callable[[Dict[str, List[Dict[str, Any]]], str], str],
    load_market_sentiment_fn: Callable[[str], Dict[str, Any]],
    report_json_fn: Callable[[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    broker_summary_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(broker_summary_rows or [])}
    market_cache: Dict[str, Dict[str, Any]] = {}
    context_rows: List[Dict[str, Any]] = []
    for row in list(summary_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "")
        market = resolve_market_code(str(row.get("market") or ""))
        report_dir = latest_report_dir_fn(runs_by_portfolio, portfolio_id)
        market_sentiment = load_market_sentiment_fn(report_dir)
        opportunity_summary = report_json_fn(report_dir, "investment_opportunity_summary.json")
        paper_summary = report_json_fn(report_dir, "investment_paper_summary.json")
        execution_summary = report_json_fn(report_dir, "investment_execution_summary.json")
        if market not in market_cache:
            runtime_paths = _runtime_config_paths_for_market(market)
            market_cache[market] = {
                "market_structure": load_market_structure(BASE_DIR, market, str(runtime_paths["market_structure"])),
                "account_profiles": load_account_profiles(BASE_DIR, str(runtime_paths["account_profile"])),
                "adaptive_strategy": load_adaptive_strategy(BASE_DIR, str(runtime_paths["adaptive_strategy"])),
            }
        cached = market_cache[market]
        broker_summary = dict(broker_summary_map.get(portfolio_id) or {})
        broker_equity = float(
            broker_summary.get("latest_broker_equity")
            or row.get("latest_equity")
            or row.get("start_equity")
            or 0.0
        )
        market_rules = market_structure_summary(cached["market_structure"], broker_equity=broker_equity)
        account_profile = (
            resolved_account_profile_summary(cached["account_profiles"], broker_equity=broker_equity)
            if broker_equity > 0.0
            else {}
        )
        adaptive_strategy = adaptive_strategy_context(cached["adaptive_strategy"])
        controls_note = _strategy_effective_controls_note(execution_summary, paper_summary)
        gate_summary = _execution_gate_summary(execution_summary)
        strategy_note = _weekly_strategy_note(
            market_rules=market_rules,
            account_profile=account_profile,
            adaptive_strategy=adaptive_strategy,
            opportunity_summary=opportunity_summary,
            market_sentiment=market_sentiment,
            strategy_effective_controls_note=controls_note,
            execution_gate_summary=gate_summary,
        )
        row["market_rules_summary"] = str(market_rules.get("summary_text", "") or "")
        row["account_profile_label"] = str(account_profile.get("label", "") or account_profile.get("name", "") or "")
        row["account_profile_summary"] = str(account_profile.get("summary", "") or "")
        row["adaptive_strategy_name"] = str(adaptive_strategy.get("name", "") or "")
        row["adaptive_strategy_summary"] = str(adaptive_strategy.get("summary_text", "") or "")
        row["adaptive_strategy_active_market_profile"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_profile",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_plan_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_plan_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_regime_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_regime_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_active_market_execution_summary"] = _active_market_strategy_field(
            "adaptive_strategy_active_market_execution_summary",
            execution_summary,
            paper_summary,
        )
        row["adaptive_strategy_market_profile_note"] = _active_market_profile_note(execution_summary, paper_summary)
        row["strategy_effective_controls_applied"] = bool(
            execution_summary.get("strategy_effective_controls_applied")
            or paper_summary.get("strategy_effective_controls_applied")
        )
        row["strategy_effective_controls_note"] = controls_note
        row["execution_gate_summary"] = gate_summary
        row["execution_blocked_order_count"] = int(execution_summary.get("blocked_order_count", 0) or 0)
        row["weekly_strategy_note"] = strategy_note
        context_rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "report_dir": report_dir,
                "market_rules_summary": row["market_rules_summary"],
                "account_profile_label": row["account_profile_label"],
                "account_profile_summary": row["account_profile_summary"],
                "adaptive_strategy_name": row["adaptive_strategy_name"],
                "adaptive_strategy_summary": row["adaptive_strategy_summary"],
                "adaptive_strategy_active_market_profile": row["adaptive_strategy_active_market_profile"],
                "adaptive_strategy_active_market_plan_summary": row["adaptive_strategy_active_market_plan_summary"],
                "adaptive_strategy_active_market_regime_summary": row["adaptive_strategy_active_market_regime_summary"],
                "adaptive_strategy_active_market_execution_summary": row["adaptive_strategy_active_market_execution_summary"],
                "adaptive_strategy_market_profile_note": row["adaptive_strategy_market_profile_note"],
                "strategy_effective_controls_applied": row["strategy_effective_controls_applied"],
                "strategy_effective_controls_note": row["strategy_effective_controls_note"],
                "execution_gate_summary": row["execution_gate_summary"],
                "execution_blocked_order_count": row["execution_blocked_order_count"],
                "weekly_strategy_note": row["weekly_strategy_note"],
                "market_sentiment_label": str(market_sentiment.get("label", "") or ""),
                "adaptive_strategy_wait_count": int(opportunity_summary.get("adaptive_strategy_wait_count", 0) or 0),
            }
        )
    return context_rows

def _sector_top_weight(rows: List[Dict[str, Any]], portfolio_id: str) -> tuple[str, float]:
    ordered = [row for row in rows if str(row.get("portfolio_id") or "") == portfolio_id]
    ordered.sort(key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    if not ordered:
        return "", 0.0
    first = ordered[0]
    return str(first.get("sector") or ""), float(first.get("weight") or 0.0)

def _build_attribution_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    sector_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]] | None = None,
    execution_gate_rows: List[Dict[str, Any]] | None = None,
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    latest_report_dir_fn: Callable[[Dict[str, List[Dict[str, Any]]], str], str],
    load_market_sentiment_fn: Callable[[str], Dict[str, Any]],
    report_json_fn: Callable[[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    execution_map = {str(row.get("portfolio_id") or ""): dict(row) for row in execution_effect_rows}
    planned_execution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(planned_execution_cost_rows or [])
    }
    execution_gate_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(execution_gate_rows or [])
    }
    out: List[Dict[str, Any]] = []
    neutral_exposure_ratio = 0.60
    for summary in summary_rows:
        portfolio_id = str(summary.get("portfolio_id") or "")
        latest_positions = list(latest_rows_by_portfolio.get(portfolio_id) or [])
        latest_equity = float(summary.get("latest_equity") or 0.0)
        holdings_value = float(sum(float(row.get("market_value") or 0.0) for row in latest_positions))
        invested_ratio = 0.0
        if latest_equity > 0.0:
            invested_ratio = max(0.0, min(1.5, holdings_value / latest_equity))
        elif float(summary.get("cash_after") or 0.0) > 0.0:
            invested_ratio = max(
                0.0,
                min(
                    1.5,
                    1.0 - float(summary.get("cash_after") or 0.0) / max(float(summary.get("start_equity") or 1.0), 1.0),
                ),
            )

        report_dir = latest_report_dir_fn(runs_by_portfolio, portfolio_id)
        market_sentiment = load_market_sentiment_fn(report_dir)
        paper_summary = report_json_fn(report_dir, "investment_paper_summary.json")
        execution_summary = report_json_fn(report_dir, "investment_execution_summary.json")
        market_proxy_return = float(market_sentiment.get("benchmark_ret5d", 0.0) or 0.0)
        market_contribution = market_proxy_return * neutral_exposure_ratio
        sizing_contribution = market_proxy_return * (invested_ratio - neutral_exposure_ratio)

        execution_effect = dict(execution_map.get(portfolio_id) or {})
        planned_effect = dict(planned_execution_map.get(portfolio_id) or {})
        gate_effect = dict(execution_gate_map.get(portfolio_id) or {})
        execution_cost_total = float(execution_effect.get("execution_cost_total", 0.0) or 0.0)
        planned_execution_cost_total = float(planned_effect.get("planned_execution_cost_total", 0.0) or 0.0)
        execution_contribution = -execution_cost_total / latest_equity if latest_equity > 0.0 else 0.0
        execution_cost_gap = float(execution_cost_total - planned_execution_cost_total)

        strategy_controls = dict(
            execution_summary.get("strategy_effective_controls")
            or paper_summary.get("strategy_effective_controls")
            or {}
        )
        strategy_base_target = float(
            strategy_controls.get(
                "base_effective_target_invested_weight",
                strategy_controls.get("base_target_invested_weight", 0.0),
            )
            or 0.0
        )
        strategy_effective_target = float(
            strategy_controls.get("effective_target_invested_weight", strategy_base_target) or strategy_base_target
        )
        strategy_control_weight_delta = max(0.0, strategy_base_target - strategy_effective_target)

        risk_source = dict(paper_summary or {})
        risk_source.update(dict(execution_summary or {}))
        risk_net_tightening = max(
            0.0,
            float(
                risk_source.get(
                    "risk_net_exposure_tightening",
                    max(
                        0.0,
                        float(risk_source.get("risk_base_net_exposure", 0.0) or 0.0)
                        - float(risk_source.get("risk_dynamic_net_exposure", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_gross_tightening = max(
            0.0,
            float(
                risk_source.get(
                    "risk_gross_exposure_tightening",
                    max(
                        0.0,
                        float(risk_source.get("risk_base_gross_exposure", 0.0) or 0.0)
                        - float(risk_source.get("risk_dynamic_gross_exposure", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_overlay_weight_delta = max(risk_net_tightening, risk_gross_tightening)
        risk_market_profile_budget_weight_delta = max(
            0.0,
            float(
                risk_source.get(
                    "risk_market_profile_budget_tightening",
                    max(
                        float(risk_source.get("risk_market_profile_budget_net_tightening", 0.0) or 0.0),
                        float(risk_source.get("risk_market_profile_budget_gross_tightening", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_throttle_weight_delta = max(
            0.0,
            float(
                risk_source.get(
                    "risk_throttle_weight_delta",
                    max(
                        float(risk_source.get("risk_throttle_net_tightening", 0.0) or 0.0),
                        float(risk_source.get("risk_throttle_gross_tightening", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_recovery_weight_credit = max(
            0.0,
            float(
                risk_source.get(
                    "risk_recovery_weight_credit",
                    max(
                        float(risk_source.get("risk_recovery_net_credit", 0.0) or 0.0),
                        float(risk_source.get("risk_recovery_gross_credit", 0.0) or 0.0),
                    ),
                )
                or 0.0
            ),
        )
        risk_layered_split_text = str(risk_source.get("risk_layered_throttle_text", "") or "")
        risk_dominant_throttle_layer = str(risk_source.get("risk_dominant_throttle_layer", "") or "")
        risk_dominant_throttle_layer_label = str(risk_source.get("risk_dominant_throttle_layer_label", "") or "")

        execution_gate_blocked_order_count = int(gate_effect.get("blocked_order_count", 0) or 0)
        execution_gate_blocked_order_value = float(gate_effect.get("blocked_order_value", 0.0) or 0.0)
        execution_gate_blocked_order_ratio = float(gate_effect.get("blocked_order_ratio", 0.0) or 0.0)
        execution_gate_blocked_weight = float(execution_gate_blocked_order_value / latest_equity) if latest_equity > 0.0 else 0.0
        control_split_text = _attribution_control_split_text(
            strategy_delta=strategy_control_weight_delta,
            risk_delta=risk_overlay_weight_delta,
            gate_weight=execution_gate_blocked_weight,
            gate_value=execution_gate_blocked_order_value,
            gate_ratio=execution_gate_blocked_order_ratio,
        )

        top_sector, top_sector_weight = _sector_top_weight(sector_rows, portfolio_id)
        residual_after_base = float(summary.get("weekly_return") or 0.0) - market_contribution - sizing_contribution - execution_contribution
        sector_strength = max(0.0, min(0.45, (top_sector_weight - 0.25) / 0.45))
        sector_contribution = residual_after_base * sector_strength
        selection_contribution = float(summary.get("weekly_return") or 0.0) - (
            market_contribution + sizing_contribution + execution_contribution + sector_contribution
        )

        contributions = {
            "selection": float(selection_contribution),
            "sizing": float(sizing_contribution),
            "sector": float(sector_contribution),
            "execution": float(execution_contribution),
            "market": float(market_contribution),
        }
        dominant_key = max(contributions, key=lambda key: abs(float(contributions[key])))
        diagnosis = {
            "selection": "收益主要由选股质量驱动，优先复盘信号与候选排序。",
            "sizing": "收益主要受仓位利用率影响，优先复盘资金闲置与加减仓节奏。",
            "sector": "收益主要受行业/主题倾斜影响，优先复盘行业暴露是否过强或过弱。",
            "execution": (
                "收益主要受执行损耗影响，优先复盘佣金、滑点和执行时机。"
                if execution_cost_gap <= 0.0
                else "收益主要受执行损耗影响，而且实际执行成本高于计划，优先复盘拆单节奏、时段风格和成交质量。"
            ),
            "market": "收益主要受市场方向影响，优先复盘 regime 与净敞口控制。",
        }[dominant_key]
        abs_total = sum(abs(float(value)) for value in contributions.values()) or max(abs(float(summary.get("weekly_return") or 0.0)), 1e-9)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(summary.get("market") or ""),
                "attribution_mode": "proxy_v1",
                "weekly_return": float(summary.get("weekly_return") or 0.0),
                "market_proxy_return": float(market_proxy_return),
                "invested_ratio": float(invested_ratio),
                "selection_contribution": float(selection_contribution),
                "sizing_contribution": float(sizing_contribution),
                "sector_contribution": float(sector_contribution),
                "execution_contribution": float(execution_contribution),
                "market_contribution": float(market_contribution),
                "selection_share": float(abs(selection_contribution) / abs_total),
                "sizing_share": float(abs(sizing_contribution) / abs_total),
                "sector_share": float(abs(sector_contribution) / abs_total),
                "execution_share": float(abs(execution_contribution) / abs_total),
                "market_share": float(abs(market_contribution) / abs_total),
                "top_sector": top_sector,
                "top_sector_weight": float(top_sector_weight),
                "execution_cost_total": float(execution_cost_total),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "planned_spread_cost_total": float(planned_effect.get("planned_spread_cost_total", 0.0) or 0.0),
                "planned_slippage_cost_total": float(planned_effect.get("planned_slippage_cost_total", 0.0) or 0.0),
                "planned_commission_cost_total": float(planned_effect.get("planned_commission_cost_total", 0.0) or 0.0),
                "avg_expected_cost_bps": planned_effect.get("avg_expected_cost_bps"),
                "planned_cost_basis": str(planned_effect.get("planned_cost_basis", "") or ""),
                "execution_style_breakdown": str(planned_effect.get("execution_style_breakdown", "") or ""),
                "execution_cost_gap": float(execution_cost_gap),
                "commission_total": float(execution_effect.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(execution_effect.get("slippage_cost_total", 0.0) or 0.0),
                "avg_actual_slippage_bps": execution_effect.get("avg_actual_slippage_bps"),
                "strategy_control_weight_delta": float(strategy_control_weight_delta),
                "risk_overlay_weight_delta": float(risk_overlay_weight_delta),
                "risk_market_profile_budget_weight_delta": float(risk_market_profile_budget_weight_delta),
                "risk_throttle_weight_delta": float(risk_throttle_weight_delta),
                "risk_recovery_weight_credit": float(risk_recovery_weight_credit),
                "risk_layered_split_text": str(risk_layered_split_text),
                "risk_dominant_throttle_layer": str(risk_dominant_throttle_layer),
                "risk_dominant_throttle_layer_label": str(risk_dominant_throttle_layer_label),
                "execution_gate_blocked_order_count": int(execution_gate_blocked_order_count),
                "execution_gate_blocked_order_value": float(execution_gate_blocked_order_value),
                "execution_gate_blocked_order_ratio": float(execution_gate_blocked_order_ratio),
                "execution_gate_blocked_weight": float(execution_gate_blocked_weight),
                "control_split_text": control_split_text,
                "dominant_driver": dominant_key.upper(),
                "diagnosis": diagnosis,
            }
        )
    out.sort(key=lambda row: abs(float(row.get("weekly_return", 0.0) or 0.0)), reverse=True)
    return out

def _build_risk_review_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] | None = None,
    *,
    risk_overlay_from_history_row_fn: Callable[[Dict[str, Any]], Dict[str, Any]],
    latest_risk_overlay_fn: Callable[[List[Dict[str, Any]]], Dict[str, Any]],
    risk_driver_and_diagnosis_fn: Callable[[Dict[str, Any]], tuple[str, str]],
    mean_fn: Callable[[List[float]], float],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    all_portfolios = set(runs_by_portfolio)
    all_portfolios.update((risk_history_by_portfolio or {}).keys())
    for portfolio_id in sorted(all_portfolios):
        runs = list(runs_by_portfolio.get(portfolio_id, []) or [])
        history_rows = list((risk_history_by_portfolio or {}).get(portfolio_id, []) or [])
        source_rows = history_rows or runs
        overlays = [risk for risk in (risk_overlay_from_history_row_fn(row) for row in source_rows) if risk]
        if not overlays:
            continue
        latest = latest_risk_overlay_fn(source_rows)
        avg_dynamic_net = mean_fn([float(item.get("dynamic_net_exposure", 0.0) or 0.0) for item in overlays])
        avg_dynamic_gross = mean_fn([float(item.get("dynamic_gross_exposure", 0.0) or 0.0) for item in overlays])
        avg_avg_corr = mean_fn([float(item.get("avg_pair_correlation", 0.0) or 0.0) for item in overlays])
        avg_worst_loss = mean_fn([float(item.get("stress_worst_loss", 0.0) or 0.0) for item in overlays])
        latest_scenarios = dict(latest.get("final_stress_scenarios", {}) or latest.get("stress_scenarios", {}) or {})
        latest_row = (source_rows[-1] if source_rows else {})
        source_kinds = sorted(
            {
                str(row.get("source_kind") or "").strip().lower()
                for row in history_rows
                if str(row.get("source_kind") or "").strip()
            }
        )
        row = {
            "portfolio_id": portfolio_id,
            "market": str(latest_row.get("market") or (runs[-1] if runs else {}).get("market") or ""),
            "risk_overlay_runs": int(len(overlays)),
            "risk_history_source": "normalized_table" if history_rows else "legacy_run_details",
            "risk_history_sources": ",".join(source_kinds),
            "avg_dynamic_net_exposure": float(avg_dynamic_net),
            "avg_dynamic_gross_exposure": float(avg_dynamic_gross),
            "avg_pair_correlation": float(avg_avg_corr),
            "avg_stress_worst_loss": float(avg_worst_loss),
            "latest_dynamic_scale": float(latest.get("dynamic_scale", 1.0) or 1.0),
            "latest_dynamic_net_exposure": float(latest.get("dynamic_net_exposure", 0.0) or 0.0),
            "latest_dynamic_gross_exposure": float(latest.get("dynamic_gross_exposure", 0.0) or 0.0),
            "latest_dynamic_short_exposure": float(latest.get("dynamic_short_exposure", 0.0) or 0.0),
            "latest_market_profile_net_exposure_budget": float(latest.get("market_profile_net_exposure_budget", 0.0) or 0.0),
            "latest_market_profile_gross_exposure_budget": float(latest.get("market_profile_gross_exposure_budget", 0.0) or 0.0),
            "latest_market_profile_budget_tightening": float(
                max(
                    float(latest.get("market_profile_budget_tightening_net", 0.0) or 0.0),
                    float(latest.get("market_profile_budget_tightening_gross", 0.0) or 0.0),
                )
            ),
            "latest_throttle_tightening": float(
                max(
                    float(latest.get("throttle_net_tightening", 0.0) or 0.0),
                    float(latest.get("throttle_gross_tightening", 0.0) or 0.0),
                )
            ),
            "latest_recovery_credit": float(
                max(
                    float(latest.get("recovery_net_credit", 0.0) or 0.0),
                    float(latest.get("recovery_gross_credit", 0.0) or 0.0),
                )
            ),
            "latest_dominant_throttle_layer": str(latest.get("dominant_throttle_layer", "") or ""),
            "latest_dominant_throttle_layer_label": str(latest.get("dominant_throttle_layer_label", "") or ""),
            "latest_layered_throttle_text": str(latest.get("layered_throttle_text", "") or ""),
            "latest_recovery_active": int(bool(latest.get("recovery_active", False))),
            "latest_avg_pair_correlation": float(
                latest.get("final_avg_pair_correlation", latest.get("avg_pair_correlation", 0.0)) or 0.0
            ),
            "latest_max_pair_correlation": float(
                latest.get("final_max_pair_correlation", latest.get("max_pair_correlation", 0.0)) or 0.0
            ),
            "latest_top_sector_share": float(latest.get("top_sector_share", 0.0) or 0.0),
            "latest_stress_index_drop_loss": float(latest_scenarios.get("index_drop", {}).get("loss", 0.0) or 0.0),
            "latest_stress_volatility_spike_loss": float(latest_scenarios.get("volatility_spike", {}).get("loss", 0.0) or 0.0),
            "latest_stress_liquidity_shock_loss": float(latest_scenarios.get("liquidity_shock", {}).get("loss", 0.0) or 0.0),
            "latest_stress_worst_loss": float(
                latest.get("final_stress_worst_loss", latest.get("stress_worst_loss", 0.0)) or 0.0
            ),
            "latest_stress_worst_scenario": str(
                latest.get("final_stress_worst_scenario", latest.get("stress_worst_scenario", "")) or ""
            ),
            "latest_stress_worst_scenario_label": str(
                latest.get("final_stress_worst_scenario_label", latest.get("stress_worst_scenario_label", "")) or ""
            ),
            "risk_notes": " | ".join(str(x).strip() for x in list(latest.get("notes", []) or []) if str(x).strip()),
            "correlation_reduced_symbols": ",".join(list(latest.get("correlation_reduced_symbols", []) or [])[:12]),
        }
        dominant_driver, diagnosis = risk_driver_and_diagnosis_fn(row)
        row["dominant_risk_driver"] = dominant_driver
        row["risk_diagnosis"] = diagnosis
        rows.append(row)
    rows.sort(
        key=lambda row: (
            0 if str(row.get("dominant_risk_driver", "") or "") == "STRESS" else 1 if str(row.get("dominant_risk_driver", "") or "") == "CORRELATION" else 2,
            -float(row.get("latest_stress_worst_loss", 0.0) or 0.0),
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return rows

def _build_market_profile_tuning_summary(
    strategy_context_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    attribution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(attribution_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    risk_feedback_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(risk_feedback_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    execution_feedback_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(execution_feedback_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    out: List[Dict[str, Any]] = []
    for raw in list(strategy_context_rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        attribution = dict(attribution_map.get(portfolio_id) or {})
        risk_feedback = dict(risk_feedback_map.get(portfolio_id) or {})
        execution_feedback = dict(execution_feedback_map.get(portfolio_id) or {})
        strategy_delta = float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0)
        risk_delta = float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0)
        gate_weight = float(attribution.get("execution_gate_blocked_weight", 0.0) or 0.0)
        gate_ratio = float(attribution.get("execution_gate_blocked_order_ratio", 0.0) or 0.0)
        blocked_edge_count = int(attribution.get("execution_gate_blocked_order_count", 0) or 0)
        split_text = str(attribution.get("control_split_text", "") or "").strip()
        risk_action = str(risk_feedback.get("risk_feedback_action", "") or "HOLD").upper()
        execution_action = str(execution_feedback.get("execution_feedback_action", "") or "HOLD").upper()

        tuning_target = "WATCH"
        tuning_target_label = "继续观察"
        tuning_bias = "NEUTRAL"
        tuning_bias_label = "暂无明显失配"
        tuning_action = "KEEP_BASELINE"
        note = "当前市场档案没有出现单一主导的失配信号，先继续观察。"
        no_trade_optimization_note = ""

        if gate_weight >= max(0.02, strategy_delta + 0.01, risk_delta + 0.01) and blocked_edge_count > 0:
            tuning_target = "EXECUTION_GATE"
            tuning_target_label = "执行门槛"
            tuning_bias = "TOO_TIGHT"
            tuning_bias_label = "执行 gate 偏紧"
            tuning_action = "REVIEW_EXECUTION_GATE"
            note = (
                "本周更明显的阻断来自 execution edge gate，优先复核 "
                "min_expected_edge_bps / edge_cost_buffer_bps，而不是继续收紧执行节奏。"
            )
            no_trade_prefix = (
                "本周几乎所有计划单都被 gate 阻断；"
                if gate_ratio >= 0.95
                else "即使当前 paper 成交样本不足，"
            )
            no_trade_optimization_note = (
                f"{no_trade_prefix}仍可用候选快照、被阻断订单的 5/20/60d outcome、"
                "expected_edge 与 required_edge gap、shadow/dry-run 回标做 counterfactual 校准；"
                "先在 paper/shadow 单字段放宽验证，不直接 live 生效。"
            )
            note = (
                f"{note}若本周没有 paper 成交，先用 counterfactual/outcome 回标验证被挡单，"
                "再做 paper/shadow 单字段放宽。"
            )
        elif strategy_delta >= max(0.05, risk_delta + 0.02, gate_weight + 0.02):
            tuning_target = "REGIME_PLAN"
            tuning_target_label = "Regime / 计划参数"
            tuning_bias = "TOO_TIGHT"
            tuning_bias_label = "策略参数偏紧"
            tuning_action = "REVIEW_REGIME_PLAN"
            note = (
                "本周压仓主要来自策略主动控仓，优先复核 risk_on / hard_risk_off、"
                "no_trade_band 和 turnover_penalty，而不是先改风险 overlay。"
            )
        elif risk_delta >= max(0.04, strategy_delta + 0.02, gate_weight + 0.02):
            tuning_target = "RISK_OVERLAY"
            tuning_target_label = "风险 Overlay"
            tuning_bias = "RISK_DRIVEN"
            tuning_bias_label = "风险层主导"
            tuning_action = "KEEP_RISK_OVERLAY"
            note = "本周压仓主要来自风险 overlay，先不要把问题误判成市场档案参数本身。"
        elif execution_action == "RELAX":
            tuning_target = "EXECUTION_TACTICS"
            tuning_target_label = "执行节奏"
            tuning_bias = "CAN_RELAX"
            tuning_bias_label = "执行节奏可放宽"
            tuning_action = "KEEP_EXECUTION_RELAX"
            note = "实际执行成本持续低于计划，可继续沿执行参与率/拆单参数做温和放宽。"
        elif risk_action == "RELAX":
            tuning_target = "RISK_BUDGET"
            tuning_target_label = "风险预算"
            tuning_bias = "CAN_RELAX"
            tuning_bias_label = "风险预算可放宽"
            tuning_action = "KEEP_RISK_RELAX"
            note = "组合风险预算相对保守，若后续样本持续稳定，可继续沿风险预算方向温和放宽。"

        if split_text:
            note = f"{note}（{split_text}）"

        summary_text = f"{tuning_target_label} / {tuning_bias_label}: {note}"
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "adaptive_strategy_active_market_profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "adaptive_strategy_active_market_plan_summary": str(row.get("adaptive_strategy_active_market_plan_summary") or ""),
                "adaptive_strategy_active_market_regime_summary": str(row.get("adaptive_strategy_active_market_regime_summary") or ""),
                "adaptive_strategy_active_market_execution_summary": str(row.get("adaptive_strategy_active_market_execution_summary") or ""),
                "adaptive_strategy_market_profile_note": str(row.get("adaptive_strategy_market_profile_note") or ""),
                "market_profile_tuning_target": tuning_target,
                "market_profile_tuning_target_label": tuning_target_label,
                "market_profile_tuning_bias": tuning_bias,
                "market_profile_tuning_bias_label": tuning_bias_label,
                "market_profile_tuning_action": tuning_action,
                "market_profile_tuning_note": note,
                "market_profile_tuning_summary": summary_text,
                "no_trade_optimization_note": no_trade_optimization_note,
                "counterfactual_optimization_available": int(bool(no_trade_optimization_note)),
                "strategy_control_weight_delta": float(strategy_delta),
                "risk_overlay_weight_delta": float(risk_delta),
                "execution_gate_blocked_weight": float(gate_weight),
                "execution_gate_blocked_order_ratio": float(gate_ratio),
                "execution_gate_blocked_order_count": int(blocked_edge_count),
                "risk_feedback_action": risk_action,
                "execution_feedback_action": execution_action,
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("market_profile_tuning_bias") or "") == "TOO_TIGHT" else 1 if str(row.get("market_profile_tuning_bias") or "") == "CAN_RELAX" else 2,
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out
