from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def _format_skip_reason_counts(reason_counts: Dict[str, Any]) -> str:
    parts: List[str] = []
    for reason, count in sorted(
        ((str(k or ""), int(v or 0)) for k, v in dict(reason_counts or {}).items()),
        key=lambda item: (-item[1], item[0]),
    ):
        if not reason or count <= 0:
            continue
        parts.append(f"{reason}:{count}")
    return ", ".join(parts)


def write_weekly_review_markdown(
    path: Path,
    summary_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    broker_diff_rows: List[Dict[str, Any]],
    reason_rows: List[Dict[str, Any]],
    shadow_summary_rows: List[Dict[str, Any]],
    shadow_feedback_rows: List[Dict[str, Any]],
    feedback_calibration_rows: List[Dict[str, Any]],
    feedback_automation_rows: List[Dict[str, Any]],
    feedback_effect_market_summary_rows: List[Dict[str, Any]],
    feedback_threshold_suggestion_rows: List[Dict[str, Any]],
    feedback_threshold_history_overview_rows: List[Dict[str, Any]],
    feedback_threshold_effect_overview_rows: List[Dict[str, Any]],
    feedback_threshold_cohort_overview_rows: List[Dict[str, Any]],
    feedback_threshold_trial_alert_rows: List[Dict[str, Any]],
    feedback_threshold_tuning_rows: List[Dict[str, Any]],
    labeling_summary: Dict[str, Any],
    labeling_skip_rows: List[Dict[str, Any]],
    outcome_spread_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
    blocked_edge_attribution_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    control_timeseries_rows: List[Dict[str, Any]],
    window_label: str,
    decision_evidence_summary_rows: List[Dict[str, Any]] | None = None,
    decision_evidence_history_overview_rows: List[Dict[str, Any]] | None = None,
    edge_calibration_rows: List[Dict[str, Any]] | None = None,
    slicing_calibration_rows: List[Dict[str, Any]] | None = None,
    risk_calibration_rows: List[Dict[str, Any]] | None = None,
    calibration_patch_suggestion_rows: List[Dict[str, Any]] | None = None,
    patch_governance_rows: List[Dict[str, Any]] | None = None,
) -> None:
    lines = [
        "# Weekly Investment Review",
        "",
        f"- Window: {window_label}",
        "",
        "## Local Paper Ledger Summary",
    ]
    if not summary_rows:
        lines.append("- (no portfolios)")
    else:
        for row in summary_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"weekly_return={float(row.get('weekly_return', 0.0) or 0.0):.3f} "
                f"max_drawdown={float(row.get('max_drawdown', 0.0) or 0.0):.3f} "
                f"rebalances={int(row.get('executed_rebalances', 0) or 0)} "
                f"turnover={float(row.get('turnover', 0.0) or 0.0):.3f} "
                f"equity={float(row.get('latest_equity', 0.0) or 0.0):.2f} "
                f"cash={float(row.get('cash_after', 0.0) or 0.0):.2f} "
                f"holdings={int(row.get('holdings_count', 0) or 0)}"
            )
            if row.get("top_holdings"):
                lines.append(f"  当前重仓: {row['top_holdings']}")
            if row.get("top_sectors"):
                lines.append(f"  行业暴露: {row['top_sectors']}")
            if row.get("holdings_change_summary"):
                lines.append(f"  持仓变化: {row['holdings_change_summary']}")
            if row.get("account_profile_label") or row.get("account_profile_summary"):
                profile_text = str(row.get("account_profile_label") or "-")
                if row.get("account_profile_summary"):
                    profile_text = f"{profile_text} / {row.get('account_profile_summary')}"
                lines.append(f"  账户档位: {profile_text}")
            if row.get("market_rules_summary"):
                lines.append(f"  市场约束: {row['market_rules_summary']}")
            if row.get("adaptive_strategy_name") or row.get("adaptive_strategy_summary"):
                strategy_text = str(row.get("adaptive_strategy_name") or "-")
                if row.get("adaptive_strategy_summary"):
                    strategy_text = f"{strategy_text} / {row.get('adaptive_strategy_summary')}"
                lines.append(f"  策略框架: {strategy_text}")
            if row.get("adaptive_strategy_market_profile_note"):
                lines.append(f"  市场档案: {row['adaptive_strategy_market_profile_note']}")
            if row.get("market_profile_tuning_note"):
                lines.append(f"  参数调优: {row['market_profile_tuning_note']}")
            if row.get("market_profile_readiness_summary"):
                lines.append(f"  建议状态: {row['market_profile_readiness_summary']}")
            if row.get("strategy_effective_controls_note"):
                lines.append(f"  策略控仓: {row['strategy_effective_controls_note']}")
            if row.get("execution_gate_summary"):
                lines.append(f"  执行阻断: {row['execution_gate_summary']}")
            if row.get("weekly_strategy_note"):
                lines.append(f"  周度解释: {row['weekly_strategy_note']}")
            lines.append(
                f"  交易拆分: buys={int(row.get('buy_count', 0) or 0)} "
                f"sells={int(row.get('sell_count', 0) or 0)} "
                f"buy_value={float(row.get('gross_buy_value', 0.0) or 0.0):.2f} "
                f"sell_value={float(row.get('gross_sell_value', 0.0) or 0.0):.2f}"
            )
            if int(row.get("broker_sync_runs", 0) or 0) > 0:
                lines.append(f"  说明: 已排除 {int(row.get('broker_sync_runs', 0) or 0)} 次 broker_sync 对绩效曲线的直接影响")
    lines.append("")
    lines.append("## Broker Execution Summary")
    if not broker_summary_rows:
        lines.append("- (no broker execution rows)")
    else:
        for row in broker_summary_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"broker_equity={float(row.get('latest_broker_equity', 0.0) or 0.0):.2f} "
                f"broker_cash={float(row.get('latest_broker_cash', 0.0) or 0.0):.2f} "
                f"broker_holdings={int(row.get('broker_holdings_count', 0) or 0)} "
                f"broker_value={float(row.get('broker_holdings_value', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  执行汇总: runs={int(row.get('execution_runs', 0) or 0)} "
                f"submitted_runs={int(row.get('submitted_runs', 0) or 0)} "
                f"submitted_orders={int(row.get('submitted_order_rows', 0) or 0)} "
                f"errors={int(row.get('error_order_rows', 0) or 0)} "
                f"gap_symbols={int(row.get('latest_gap_symbols', 0) or 0)} "
                f"gap_notional={float(row.get('latest_gap_notional', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  计划成本: basis={row.get('planned_cost_basis', '-') or '-'} "
                f"spread={float(row.get('planned_spread_cost_total', 0.0) or 0.0):.2f} "
                f"slippage={float(row.get('planned_slippage_cost_total', 0.0) or 0.0):.2f} "
                f"commission={float(row.get('planned_commission_cost_total', 0.0) or 0.0):.2f} "
                f"exec_cost_total={float(row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                f"avg_expected_bps={float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  执行成本: commission={float(row.get('commission_total', 0.0) or 0.0):.2f} "
                f"slippage_cost={float(row.get('slippage_cost_total', 0.0) or 0.0):.2f} "
                f"exec_cost_total={float(row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                f"avg_slippage_bps={float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  计划对比: actual_minus_plan={float(row.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                f"styles={row.get('execution_style_breakdown', '-') or '-'}"
            )
            if row.get("broker_top_holdings"):
                lines.append(f"  Broker 持仓: {row['broker_top_holdings']}")
            if row.get("status_breakdown"):
                lines.append(f"  执行状态: {row['status_breakdown']}")
            if row.get("error_statuses"):
                lines.append(f"  错误状态: {row['error_statuses']}")
            diff_row = next((r for r in broker_diff_rows if str(r.get("portfolio_id") or "") == str(row.get("portfolio_id") or "")), None)
            if diff_row:
                lines.append(
                    f"  账本差异: local_only={int(diff_row.get('local_only_count', 0) or 0)} "
                    f"broker_only={int(diff_row.get('broker_only_count', 0) or 0)}"
                )
                if diff_row.get("local_only_symbols"):
                    lines.append(f"  仅本地账本: {diff_row['local_only_symbols']}")
                if diff_row.get("broker_only_symbols"):
                    lines.append(f"  仅 Broker: {diff_row['broker_only_symbols']}")

    lines.append("")
    lines.append("## Weekly Outcome Labeling Coverage")
    if not labeling_summary and not labeling_skip_rows:
        lines.append("- (no outcome labeling summary)")
    else:
        lines.append(
            f"- labeled={int(labeling_summary.get('labeled_rows', 0) or 0)} "
            f"skipped={int(labeling_summary.get('skipped_rows', 0) or 0)} "
            f"horizons={','.join(str(x) for x in list(labeling_summary.get('horizons') or [])) or '-'}"
        )
        if labeling_summary.get("skip_reason_counts"):
            lines.append(f"  跳过原因汇总: {_format_skip_reason_counts(labeling_summary.get('skip_reason_counts', {}))}")
        for row in list(labeling_skip_rows or [])[:20]:
            lines.append(
                f"- **{row.get('portfolio_id', '-') or '-'}** market={row.get('market', '-') or '-'} "
                f"horizon={row.get('horizon_days', '-') or '-'} "
                f"reason={row.get('skip_reason_label', row.get('skip_reason', '-')) or '-'} "
                f"skipped={int(row.get('skip_count', 0) or 0)} "
                f"symbols={int(row.get('symbol_count', 0) or 0)}"
            )
            if row.get("sample_symbols"):
                lines.append(f"  示例标的: {row.get('sample_symbols', '')}")
            if row.get("oldest_snapshot_ts") or row.get("latest_snapshot_ts"):
                lines.append(
                    f"  时间范围: {str(row.get('oldest_snapshot_ts', '') or '-')[:19]} -> "
                    f"{str(row.get('latest_snapshot_ts', '') or '-')[:19]}"
                )
            if int(float(row.get("max_remaining_forward_bars", 0) or 0)) > 0:
                lines.append(
                    f"  预计成熟: 还差 {int(float(row.get('min_remaining_forward_bars', 0) or 0))}"
                    f"-{int(float(row.get('max_remaining_forward_bars', 0) or 0))} 个交易日；"
                    f"预计 {str(row.get('estimated_ready_start_ts', '') or '-')[:10]} -> "
                    f"{str(row.get('estimated_ready_end_ts', '') or '-')[:10]}"
                )

    lines.append("")
    lines.append("## Weekly Outcome Spread")
    if not outcome_spread_rows:
        lines.append("- (no outcome spread rows)")
    else:
        for row in outcome_spread_rows:
            lines.append(
                f"- **{row['portfolio_id']} / {int(row.get('horizon_days', 0) or 0)}d** market={row['market']} "
                f"selected_vs_unselected={float(row.get('selected_spread_vs_unselected_bps', 0.0) or 0.0):.1f}bps "
                f"top_vs_unselected={float(row.get('top_ranked_spread_vs_unselected_bps', 0.0) or 0.0):.1f}bps "
                f"executed_vs_blocked_edge={float(row.get('executed_spread_vs_blocked_edge_bps', 0.0) or 0.0):.1f}bps"
            )
            lines.append(
                f"  样本: universe={int(row.get('universe_sample_count', 0) or 0)} "
                f"selected={int(row.get('selected_sample_count', 0) or 0)} "
                f"executed={int(row.get('executed_sample_count', 0) or 0)} "
                f"blocked_edge={int(row.get('blocked_edge_sample_count', 0) or 0)}"
            )

    lines.append("")
    lines.append("## Weekly Proxy Attribution")
    if not attribution_rows:
        lines.append("- (no attribution rows)")
    else:
        for row in attribution_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} mode={row.get('attribution_mode', 'proxy_v1')} "
                f"weekly_return={float(row.get('weekly_return', 0.0) or 0.0):.3f} "
                f"selection={float(row.get('selection_contribution', 0.0) or 0.0):.3f} "
                f"sizing={float(row.get('sizing_contribution', 0.0) or 0.0):.3f} "
                f"sector={float(row.get('sector_contribution', 0.0) or 0.0):.3f} "
                f"execution={float(row.get('execution_contribution', 0.0) or 0.0):.3f} "
                f"market={float(row.get('market_contribution', 0.0) or 0.0):.3f}"
            )
            lines.append(
                f"  代理说明: dominant={row.get('dominant_driver', '')} "
                f"market_proxy={float(row.get('market_proxy_return', 0.0) or 0.0):.3f} "
                f"invested={float(row.get('invested_ratio', 0.0) or 0.0):.2f} "
                f"top_sector={row.get('top_sector', '-') or '-'}:{float(row.get('top_sector_weight', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  计划vs实际执行成本: plan={float(row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                f"actual={float(row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                f"gap={float(row.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                f"expected_bps={float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f} "
                f"actual_slippage_bps={float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}"
            )
            if row.get("control_split_text"):
                lines.append(f"  控制拆解: {row.get('control_split_text', '')}")
            if row.get("risk_layered_split_text"):
                lines.append(f"  风险分层: {row.get('risk_layered_split_text', '')}")
            lines.append(f"  建议: {row.get('diagnosis', '')}")

    lines.append("")
    lines.append("## Weekly Edge Realization")
    if not edge_realization_rows:
        lines.append("- (no edge realization rows)")
    else:
        for row in edge_realization_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"expected_edge={float(row.get('avg_expected_edge_bps', 0.0) or 0.0):.1f}bps "
                f"realized_cost={float(row.get('avg_realized_total_cost_bps', 0.0) or 0.0):.1f}bps "
                f"capture={float(row.get('avg_execution_capture_bps', 0.0) or 0.0):.1f}bps "
                f"fill_delay={float(row.get('avg_fill_delay_seconds', 0.0) or 0.0):.1f}s"
            )
            lines.append(
                f"  成熟结果: 5d={float(row.get('matured_5d_avg_realized_edge_bps', 0.0) or 0.0):.1f}bps "
                f"20d={float(row.get('matured_20d_avg_realized_edge_bps', 0.0) or 0.0):.1f}bps "
                f"60d={float(row.get('matured_60d_avg_realized_edge_bps', 0.0) or 0.0):.1f}bps"
            )

    lines.append("")
    lines.append("## Weekly Blocked Edge Attribution")
    if not blocked_edge_attribution_rows:
        lines.append("- (no blocked edge attribution rows)")
    else:
        for row in blocked_edge_attribution_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"blocked_value={float(row.get('blocked_edge_order_value', 0.0) or 0.0):.2f} "
                f"blocked_weight={float(row.get('blocked_edge_weight', 0.0) or 0.0):.2%} "
                f"expected_edge={float(row.get('avg_expected_edge_bps', 0.0) or 0.0):.1f}bps "
                f"required_gap={float(row.get('avg_required_gap_bps', 0.0) or 0.0):.1f}bps"
            )
            lines.append(
                f"  反事实: 5d={float(row.get('matured_5d_avg_counterfactual_edge_bps', 0.0) or 0.0):.1f}bps "
                f"20d={float(row.get('matured_20d_avg_counterfactual_edge_bps', 0.0) or 0.0):.1f}bps "
                f"60d={float(row.get('matured_60d_avg_counterfactual_edge_bps', 0.0) or 0.0):.1f}bps"
            )

    lines.append("")
    lines.append("## Weekly Decision Evidence")
    if not decision_evidence_summary_rows:
        lines.append("- (no decision evidence rows)")
    else:
        for row in decision_evidence_summary_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"rows={int(row.get('decision_evidence_row_count', 0) or 0)} "
                f"market_rule_blocked={int(row.get('decision_blocked_market_rule_order_count', 0) or 0)} "
                f"edge_blocked={int(row.get('decision_blocked_edge_order_count', 0) or 0)} "
                f"bucket={row.get('decision_primary_liquidity_bucket', '-') or '-'}"
            )
            lines.append(
                f"  决策参数: expected_edge={float(row.get('decision_avg_expected_edge_bps', 0.0) or 0.0):.1f}bps "
                f"expected_cost={float(row.get('decision_avg_expected_cost_bps', 0.0) or 0.0):.1f}bps "
                f"gate={float(row.get('decision_avg_edge_gate_threshold_bps', 0.0) or 0.0):.1f}bps "
                f"adv={float(row.get('decision_avg_dynamic_order_adv_pct', 0.0) or 0.0):.3f} "
                f"slices={float(row.get('decision_avg_slice_count', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  结果: slippage={float(row.get('decision_avg_realized_slippage_bps', 0.0) or 0.0):.1f}bps "
                f"realized_edge={float(row.get('decision_avg_realized_edge_bps', 0.0) or 0.0):.1f}bps "
                f"outcome_5/20/60={float(row.get('decision_avg_outcome_5d_bps', 0.0) or 0.0):.1f}/"
                f"{float(row.get('decision_avg_outcome_20d_bps', 0.0) or 0.0):.1f}/"
                f"{float(row.get('decision_avg_outcome_60d_bps', 0.0) or 0.0):.1f}bps"
            )

    lines.append("")
    lines.append("## Decision Evidence History")
    if not decision_evidence_history_overview_rows:
        lines.append("- (no decision evidence history rows)")
    else:
        for row in decision_evidence_history_overview_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"weeks={int(row.get('weeks_tracked', 0) or 0)} "
                f"latest={row.get('latest_week_label', '-') or '-'} "
                f"baseline={row.get('baseline_week_label', '-') or '-'} "
                f"bucket={row.get('latest_primary_liquidity_bucket', '-') or '-'}"
            )
            if row.get("liquidity_bucket_chain"):
                lines.append(f"  Bucket 演变: {row.get('liquidity_bucket_chain', '')}")
            lines.append(
                f"  结果趋势: slippage_delta={float(row.get('decision_avg_realized_slippage_bps_delta', 0.0) or 0.0):.1f}bps "
                f"({row.get('decision_slippage_trend', '-') or '-'}) "
                f"realized_edge_delta={float(row.get('decision_avg_realized_edge_bps_delta', 0.0) or 0.0):.1f}bps "
                f"({row.get('decision_realized_edge_trend', '-') or '-'}) "
                f"outcome20_delta={float(row.get('decision_avg_outcome_20d_bps_delta', 0.0) or 0.0):.1f}bps "
                f"({row.get('decision_outcome_20d_trend', '-') or '-'})"
            )
            lines.append(
                f"  Gate/执行: blocked_edge_delta={float(row.get('decision_blocked_edge_order_count_delta', 0.0) or 0.0):.1f} "
                f"({row.get('decision_blocked_edge_trend', '-') or '-'}) "
                f"market_rule_delta={float(row.get('decision_blocked_market_rule_order_count_delta', 0.0) or 0.0):.1f} "
                f"({row.get('decision_market_rule_block_trend', '-') or '-'}) "
                f"adv_delta={float(row.get('decision_avg_dynamic_order_adv_pct_delta', 0.0) or 0.0):.3f} "
                f"slice_delta={float(row.get('decision_avg_slice_count_delta', 0.0) or 0.0):.2f}"
            )

    lines.append("")
    lines.append("## Edge Calibration")
    if not edge_calibration_rows:
        lines.append("- (no edge calibration rows)")
    else:
        for row in edge_calibration_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"filled={int(row.get('filled_sample_count', 0) or 0)} "
                f"blocked_edge={int(row.get('blocked_edge_sample_count', 0) or 0)} "
                f"blocked_rule={int(row.get('blocked_market_rule_sample_count', 0) or 0)} "
                f"edge_quality={row.get('edge_gate_quality', '-') or '-'}"
            )
            lines.append(
                f"  Outcome20: filled={float(row.get('filled_avg_outcome_20d_bps', 0.0) or 0.0):.1f}bps "
                f"blocked_edge={float(row.get('blocked_edge_avg_outcome_20d_bps', 0.0) or 0.0):.1f}bps "
                f"gap={float(row.get('blocked_edge_vs_filled_outcome_20d_bps', 0.0) or 0.0):.1f}bps "
                f"rule_gap={float(row.get('blocked_market_rule_vs_filled_outcome_20d_bps', 0.0) or 0.0):.1f}bps"
            )
            if row.get("edge_calibration_note"):
                lines.append(f"  结论: {row.get('edge_calibration_note', '')}")

    lines.append("")
    lines.append("## Slicing Calibration")
    if not slicing_calibration_rows:
        lines.append("- (no slicing calibration rows)")
    else:
        for row in slicing_calibration_rows:
            lines.append(
                f"- **{row['portfolio_id']} / {row.get('dynamic_liquidity_bucket', '-') or '-'}** market={row['market']} "
                f"samples={int(row.get('sample_count', 0) or 0)} "
                f"filled={int(row.get('filled_sample_count', 0) or 0)} "
                f"assessment={row.get('slicing_assessment', '-') or '-'}"
            )
            lines.append(
                f"  执行: adv={float(row.get('avg_dynamic_order_adv_pct', 0.0) or 0.0):.3f} "
                f"slices={float(row.get('avg_slice_count', 0.0) or 0.0):.2f} "
                f"slippage={float(row.get('avg_realized_slippage_bps', 0.0) or 0.0):.1f}bps "
                f"delay={float(row.get('avg_fill_delay_seconds', 0.0) or 0.0):.1f}s"
            )
            if row.get("slicing_calibration_note"):
                lines.append(f"  结论: {row.get('slicing_calibration_note', '')}")

    lines.append("")
    lines.append("## Risk Calibration")
    if not risk_calibration_rows:
        lines.append("- (no risk calibration rows)")
    else:
        for row in risk_calibration_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"target={row.get('risk_calibration_target', '-') or '-'} "
                f"outcome20_delta={float(row.get('decision_avg_outcome_20d_bps_delta', 0.0) or 0.0):.1f}bps "
                f"realized_edge_delta={float(row.get('decision_avg_realized_edge_bps_delta', 0.0) or 0.0):.1f}bps"
            )
            lines.append(
                f"  风险拆分: budget={float(row.get('latest_budget_weight_delta', 0.0) or 0.0):.3f} "
                f"throttle={float(row.get('latest_throttle_weight_delta', 0.0) or 0.0):.3f} "
                f"recovery={float(row.get('latest_recovery_weight_credit', 0.0) or 0.0):.3f} "
                f"layer={row.get('latest_dominant_throttle_layer_label', row.get('latest_dominant_throttle_layer', '-')) or '-'}"
            )
            if row.get("risk_calibration_note"):
                lines.append(f"  结论: {row.get('risk_calibration_note', '')}")

    lines.append("")
    lines.append("## Calibration Patch Suggestions")
    if not calibration_patch_suggestion_rows:
        lines.append("- (no calibration patch suggestions)")
    else:
        for row in calibration_patch_suggestion_rows:
            lines.append(
                f"- **{row.get('portfolio_id', '-') or '-'}** market={row.get('market', '-') or '-'} "
                f"scope={row.get('scope_label', row.get('scope', '-')) or '-'} "
                f"field={row.get('field', '-') or '-'} "
                f"current={row.get('current_value', '-') if row.get('current_value', '-') not in (None, '') else '-'} "
                f"suggested={row.get('suggested_value', '-') if row.get('suggested_value', '-') not in (None, '') else '-'}"
            )
            lines.append(
                f"  配置: {row.get('config_path', '-') or '-'} "
                f"({row.get('change_hint_label', row.get('change_hint', '-')) or '-'}) "
                f"source={row.get('source_signal_label', row.get('source_signal', '-')) or '-'} "
                f"priority={row.get('priority_label', '-') or '-'}"
            )
            if row.get("source_note"):
                lines.append(f"  建议: {row.get('source_note', '')}")

    lines.append("")
    lines.append("## Patch Governance Summary")
    if not patch_governance_rows:
        lines.append("- (no patch governance rows)")
    else:
        for row in patch_governance_rows:
            avg_review_to_apply_weeks = row.get("avg_review_to_apply_weeks")
            latency_basis = str(row.get("review_latency_basis", "review_to_apply") or "review_to_apply")
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('patch_kind_label', '-') or '-'} / {row.get('field', '-') or '-'}** "
                f"scope={row.get('scope_label', '-') or '-'} "
                f"tracked={int(row.get('review_cycle_count', 0) or 0)} "
                f"open={int(row.get('open_cycle_count', 0) or 0)} "
                f"approved_pending={int(row.get('approved_not_applied_count', 0) or 0)} "
                f"approval={float(row.get('approval_rate', 0.0) or 0.0):.0%} "
                f"rejection={float(row.get('rejection_rate', 0.0) or 0.0):.0%} "
                f"apply={float(row.get('apply_rate', 0.0) or 0.0):.0%}"
            )
            avg_weeks_text = f"{float(avg_review_to_apply_weeks or 0.0):.1f}" if avg_review_to_apply_weeks is not None else "-"
            lines.append(
                f"  最近状态: {row.get('latest_week_label', '-') or '-'} / "
                f"{row.get('latest_status_label', '-') or '-'} "
                f"avg_{latency_basis}_weeks={avg_weeks_text}"
            )
            if row.get("examples"):
                lines.append(f"  示例: {row.get('examples', '')}")

    lines.append("")
    lines.append("## Weekly Risk Overlay Review")
    if not risk_review_rows:
        lines.append("- (no risk overlay rows)")
    else:
        for row in risk_review_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"driver={row.get('dominant_risk_driver', '')} "
                f"avg_net={float(row.get('avg_dynamic_net_exposure', 0.0) or 0.0):.2f} "
                f"avg_gross={float(row.get('avg_dynamic_gross_exposure', 0.0) or 0.0):.2f} "
                f"avg_corr={float(row.get('avg_pair_correlation', 0.0) or 0.0):.2f} "
                f"avg_worst_stress={float(row.get('avg_stress_worst_loss', 0.0) or 0.0):.3f}"
            )
            lines.append(
                f"  最新风险预算: scale={float(row.get('latest_dynamic_scale', 1.0) or 1.0):.2f} "
                f"net={float(row.get('latest_dynamic_net_exposure', 0.0) or 0.0):.2f} "
                f"gross={float(row.get('latest_dynamic_gross_exposure', 0.0) or 0.0):.2f} "
                f"corr={float(row.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f} "
                f"stress={row.get('latest_stress_worst_scenario_label', '-') or '-'}:{float(row.get('latest_stress_worst_loss', 0.0) or 0.0):.3f}"
            )
            if row.get("latest_layered_throttle_text"):
                lines.append(f"  分层 throttle: {row.get('latest_layered_throttle_text', '')}")
            if (
                float(row.get("latest_market_profile_budget_tightening", 0.0) or 0.0) > 0.0
                or float(row.get("latest_throttle_tightening", 0.0) or 0.0) > 0.0
                or float(row.get("latest_recovery_credit", 0.0) or 0.0) > 0.0
            ):
                lines.append(
                    f"  预算/收缩/恢复: budget={float(row.get('latest_market_profile_budget_tightening', 0.0) or 0.0):.2%} "
                    f"throttle={float(row.get('latest_throttle_tightening', 0.0) or 0.0):.2%} "
                    f"recovery={float(row.get('latest_recovery_credit', 0.0) or 0.0):.2%}"
                )
            if row.get("correlation_reduced_symbols"):
                lines.append(f"  相关性收缩标的: {row['correlation_reduced_symbols']}")
            if row.get("risk_notes"):
                lines.append(f"  风险备注: {row['risk_notes']}")
            lines.append(f"  建议: {row.get('risk_diagnosis', '')}")

    lines.append("")
    lines.append("## Weekly Execution Session Review")
    if not execution_session_rows:
        lines.append("- (no execution session rows)")
    else:
        for row in execution_session_rows:
            lines.append(
                f"- **{row['portfolio_id']} / {row.get('session_label', '-') or '-'}** market={row['market']} "
                f"plan={float(row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                f"actual={float(row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                f"gap={float(row.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                f"expected_bps={float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f} "
                f"actual_slippage_bps={float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  时段样本: submitted={int(row.get('submitted_order_rows', 0) or 0)} "
                f"fills={int(row.get('fill_count', 0) or 0)} "
                f"fill_notional={float(row.get('fill_notional', 0.0) or 0.0):.2f} "
                f"styles={row.get('execution_style_breakdown', '-') or '-'}"
            )

    lines.append("")
    lines.append("## Weekly Execution Hotspots")
    if not execution_hotspot_rows:
        lines.append("- (no execution hotspots)")
    else:
        for row in execution_hotspot_rows[:20]:
            lines.append(
                f"- **{row['portfolio_id']} / {row.get('symbol', '-') or '-'} / {row.get('session_label', '-') or '-'}** "
                f"action={row.get('hotspot_action', '')} "
                f"plan={float(row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                f"actual={float(row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                f"gap={float(row.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                f"expected_bps={float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f} "
                f"actual_slippage_bps={float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f} "
                f"pressure={float(row.get('pressure_score', 0.0) or 0.0):.2f}"
            )
            if row.get("execution_style_breakdown"):
                lines.append(f"  风格分布: {row.get('execution_style_breakdown', '')}")
            lines.append(f"  说明: {row.get('hotspot_reason', '')}")

    lines.append("")
    lines.append("## Weekly Control Timeseries")
    if not control_timeseries_rows:
        lines.append("- (no control timeseries rows)")
    else:
        by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
        for row in control_timeseries_rows:
            by_portfolio.setdefault(str(row.get("portfolio_id") or ""), []).append(dict(row))
        for portfolio_id, rows in sorted(by_portfolio.items()):
            lines.append(f"- **{portfolio_id}**")
            for row in rows[-4:]:
                lines.append(
                    f"  {row.get('week_label', '-') or '-'} "
                    f"strategy={float(row.get('strategy_control_weight_delta', 0.0) or 0.0):.2%} "
                    f"risk={float(row.get('risk_overlay_weight_delta', 0.0) or 0.0):.2%} "
                    f"execution={float(row.get('execution_gate_blocked_weight', 0.0) or 0.0):.2%} "
                    f"driver={row.get('dominant_driver', '-') or '-'}"
                )

    lines.append("")
    lines.append("## Shadow Review Weekly Summary")
    if not shadow_summary_rows:
        lines.append("- (no shadow review blocks)")
    else:
        for row in shadow_summary_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"shadow_reviews={int(row.get('shadow_review_count', 0) or 0)} "
                f"distinct_symbols={int(row.get('distinct_symbols', 0) or 0)} "
                f"repeats={int(row.get('repeated_symbol_count', 0) or 0)} "
                f"near_miss={int(row.get('near_miss_count', 0) or 0)} "
                f"far_below={int(row.get('far_below_count', 0) or 0)} "
                f"action={row.get('shadow_review_action', '')}"
            )
            lines.append(
                f"  平均影子指标: score={float(row.get('avg_shadow_score', 0.0) or 0.0):.3f} "
                f"prob={float(row.get('avg_shadow_prob', 0.0) or 0.0):.3f} "
                f"samples={float(row.get('avg_shadow_samples', 0.0) or 0.0):.1f}"
            )
            if row.get("repeated_symbols"):
                lines.append(f"  重复拦截: {row['repeated_symbols']}")
            lines.append(f"  建议: {row.get('shadow_review_reason', '')}")

    lines.append("")
    lines.append("## Weekly Feedback Outcome Calibration")
    if not feedback_calibration_rows:
        lines.append("- (no outcome calibration rows)")
    else:
        for row in feedback_calibration_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"samples={int(row.get('outcome_sample_count', 0) or 0)} "
                f"scope={row.get('selection_scope_label', '-') or '-'} "
                f"horizon={row.get('selected_horizon_days', '-') or '-'} "
                f"positive={float(row.get('outcome_positive_rate', 0.0) or 0.0):.2f} "
                f"broken={float(row.get('outcome_broken_rate', 0.0) or 0.0):.2f} "
                f"avg_return={float(row.get('avg_future_return', 0.0) or 0.0):.3f} "
                f"avg_drawdown={float(row.get('avg_max_drawdown', 0.0) or 0.0):.3f}"
            )
            lines.append(
                f"  校准分: signal={float(row.get('signal_quality_score', 0.0) or 0.0):.2f} "
                f"shadow_relax={float(row.get('shadow_threshold_relax_support', 0.0) or 0.0):.2f} "
                f"shadow_weak={float(row.get('shadow_weak_signal_support', 0.0) or 0.0):.2f} "
                f"risk_tighten={float(row.get('risk_tighten_support', 0.0) or 0.0):.2f} "
                f"risk_relax={float(row.get('risk_relax_support', 0.0) or 0.0):.2f} "
                f"execution={float(row.get('execution_support', 0.0) or 0.0):.2f}"
            )
            lines.append(
                f"  对齐度: score_alignment={float(row.get('score_alignment_score', 0.0) or 0.0):.2f} "
                f"gap={float(row.get('score_alignment_gap', 0.0) or 0.0):+.3f} "
                f"calibration_conf={float(row.get('calibration_confidence', 0.0) or 0.0):.2f}"
                f"({row.get('calibration_confidence_label', '')})"
            )
            lines.append(f"  说明: {row.get('calibration_reason', '')}")

    lines.append("")
    lines.append("## Weekly Feedback Calibration Automation")
    if not feedback_automation_rows:
        lines.append("- (no calibration automation rows)")
    else:
        for row in feedback_automation_rows:
            lines.append(
                f"- **{row['portfolio_id']} / {row.get('feedback_kind_label', '-') or '-'}** market={row['market']} "
                f"action={row.get('feedback_action', '-') or '-'} "
                f"mode={row.get('calibration_apply_mode_label', '-') or '-'} "
                f"basis={row.get('calibration_basis_label', '-') or '-'} "
                f"base={float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f} "
                f"calib={float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
                f"final={float(row.get('feedback_confidence', 0.0) or 0.0):.2f} "
                f"weekly_samples={int(row.get('feedback_sample_count', 0) or 0)} "
                f"outcome_samples={int(row.get('feedback_calibration_sample_count', 0) or 0)} "
                f"maturity={float(row.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}"
                f"({row.get('outcome_maturity_label', '') or 'UNKNOWN'})"
            )
            if int(row.get("outcome_pending_sample_count", 0) or 0) > 0:
                lines.append(
                    f"  样本成熟: pending={int(row.get('outcome_pending_sample_count', 0) or 0)} "
                    f"remaining={int(row.get('outcome_pending_min_remaining_bars', 0) or 0)}-"
                    f"{int(row.get('outcome_pending_max_remaining_bars', 0) or 0)} "
                    f"ready={str(row.get('outcome_ready_estimate_start_ts', '') or '-')[:10]}->"
                    f"{str(row.get('outcome_ready_estimate_end_ts', '') or '-')[:10]}"
                )
            if str(row.get("market_data_gate_label", "") or "").strip() not in {"", "IBKR正常", "未检查"}:
                lines.append(
                    f"  数据 gate: {row.get('market_data_gate_label', '-') or '-'} "
                    f"(probe={row.get('market_data_probe_status_label', '-') or '-'})"
                )
                lines.append(f"  数据 gate 说明: {row.get('market_data_gate_reason', '')}")
            lines.append(f"  自动化说明: {row.get('automation_reason', '')}")
            if row.get("feedback_reason"):
                lines.append(f"  周报结论: {row.get('feedback_reason', '')}")

    lines.append("")
    lines.append("## Weekly Feedback Auto-Apply Effect Summary")
    if not feedback_effect_market_summary_rows:
        lines.append("- (no active auto-apply effect rows)")
    else:
        for row in feedback_effect_market_summary_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"signal={row.get('summary_signal', '-') or '-'} "
                f"tracked={int(row.get('tracked_count', 0) or 0)} "
                f"latest_up={int(row.get('latest_improved_count', 0) or 0)} "
                f"latest_down={int(row.get('latest_deteriorated_count', 0) or 0)} "
                f"latest_flat={int(row.get('latest_stable_count', 0) or 0)} "
                f"avg_weeks={float(row.get('avg_active_weeks', 0.0) or 0.0):.1f}"
            )
            lines.append(
                f"  里程碑: W+1 up/down={int(row.get('w1_improved_count', 0) or 0)}/"
                f"{int(row.get('w1_deteriorated_count', 0) or 0)} "
                f"W+2 up/down={int(row.get('w2_improved_count', 0) or 0)}/"
                f"{int(row.get('w2_deteriorated_count', 0) or 0)} "
                f"W+4 up/down={int(row.get('w4_improved_count', 0) or 0)}/"
                f"{int(row.get('w4_deteriorated_count', 0) or 0)}"
            )
            lines.append(f"  代表组合: {row.get('top_portfolios_text', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Suggestions")
    if not feedback_threshold_suggestion_rows:
        lines.append("- (no threshold suggestions)")
    else:
        for row in feedback_threshold_suggestion_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"action={row.get('suggestion_label', '-') or '-'} "
                f"tracked={int(row.get('tracked_count', 0) or 0)} "
                f"avg_weeks={float(row.get('avg_active_weeks', 0.0) or 0.0):.1f} "
                f"signal={row.get('summary_signal', '-') or '-'}"
            )
            lines.append(
                f"  阈值建议: conf {float(row.get('base_auto_confidence', 0.0) or 0.0):.2f}->"
                f"{float(row.get('suggested_auto_confidence', 0.0) or 0.0):.2f} "
                f"base_conf {float(row.get('base_auto_base_confidence', 0.0) or 0.0):.2f}->"
                f"{float(row.get('suggested_auto_base_confidence', 0.0) or 0.0):.2f} "
                f"calib {float(row.get('base_auto_calibration_score', 0.0) or 0.0):.2f}->"
                f"{float(row.get('suggested_auto_calibration_score', 0.0) or 0.0):.2f} "
                f"maturity {float(row.get('base_auto_maturity_ratio', 0.0) or 0.0):.2f}->"
                f"{float(row.get('suggested_auto_maturity_ratio', 0.0) or 0.0):.2f}"
            )
            lines.append(f"  说明: {row.get('reason', '-') or '-'}")
            lines.append(f"  代表组合: {row.get('examples', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Suggestion Trends")
    if not feedback_threshold_history_overview_rows:
        lines.append("- (no threshold suggestion history)")
    else:
        for row in feedback_threshold_history_overview_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"action={row.get('current_label', '-') or '-'} "
                f"transition={row.get('transition', '-') or '-'} "
                f"trend={row.get('trend_bucket', '-') or '-'} "
                f"same_weeks={int(row.get('same_action_weeks', 0) or 0)} "
                f"tracked={int(row.get('weeks_tracked', 0) or 0)} "
                f"signal={row.get('summary_signal', '-') or '-'}"
            )
            lines.append(f"  阈值快照: {row.get('threshold_snapshot', '-') or '-'}")
            lines.append(f"  动作历史: {row.get('action_chain', '-') or '-'}")
            lines.append(f"  原因: {row.get('reason', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Trial Effects")
    if not feedback_threshold_effect_overview_rows:
        lines.append("- (no threshold trial effects)")
    else:
        for row in feedback_threshold_effect_overview_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"current={row.get('current_label', '-') or '-'} "
                f"signal={row.get('summary_signal', '-') or '-'} "
                f"effect={row.get('effect_label', '-') or '-'} "
                f"same_weeks={int(row.get('same_action_weeks', 0) or 0)} "
                f"tracked={int(row.get('weeks_tracked', 0) or 0)} "
                f"active_weeks={float(row.get('avg_active_weeks', 0.0) or 0.0):.1f}"
            )
            lines.append(f"  阈值快照: {row.get('threshold_snapshot', '-') or '-'}")
            lines.append(f"  动作历史: {row.get('action_chain', '-') or '-'}")
            lines.append(f"  试运行结论: {row.get('effect_reason', '-') or '-'}")
            lines.append(f"  说明: {row.get('reason', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Trial Cohorts")
    if not feedback_threshold_cohort_overview_rows:
        lines.append("- (no threshold trial cohorts)")
    else:
        for row in feedback_threshold_cohort_overview_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"cohort={row.get('cohort_label', '-') or '-'} "
                f"baseline={row.get('baseline_week', '-') or '-'} "
                f"weeks={int(row.get('cohort_weeks', 0) or 0)} "
                f"latest={row.get('latest_effect', '-') or '-'} "
                f"W+1={row.get('effect_w1', '-') or '-'} "
                f"W+2={row.get('effect_w2', '-') or '-'} "
                f"W+4={row.get('effect_w4', '-') or '-'}"
            )
            lines.append(f"  动作历史: {row.get('action_chain', '-') or '-'}")
            lines.append(f"  结论: {row.get('diagnosis', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Trial Alerts")
    if not feedback_threshold_trial_alert_rows:
        lines.append("- (no threshold trial alerts)")
    else:
        for row in feedback_threshold_trial_alert_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"stage={row.get('stage_label', '-') or '-'} "
                f"action={row.get('action_label', '-') or '-'} "
                f"baseline={row.get('baseline_week', '-') or '-'} "
                f"weeks={int(row.get('cohort_weeks', 0) or 0)} "
                f"latest={row.get('latest_effect', '-') or '-'} "
                f"W+1={row.get('effect_w1', '-') or '-'}"
            )
            lines.append(f"  观察重点: {row.get('next_check', '-') or '-'}")
            lines.append(f"  结论: {row.get('diagnosis', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Threshold Tuning Suggestions")
    if not feedback_threshold_tuning_rows:
        lines.append("- (no threshold tuning suggestions)")
    else:
        for row in feedback_threshold_tuning_rows:
            lines.append(
                f"- **{row.get('market', '-') or '-'} / {row.get('feedback_kind_label', '-') or '-'}** "
                f"suggestion={row.get('suggestion_label', '-') or '-'} "
                f"cohort={row.get('cohort_label', '-') or '-'} "
                f"baseline={row.get('baseline_week', '-') or '-'} "
                f"weeks={int(row.get('cohort_weeks', 0) or 0)} "
                f"latest={row.get('latest_effect', '-') or '-'} "
                f"W+1={row.get('effect_w1', '-') or '-'} "
                f"W+2={row.get('effect_w2', '-') or '-'} "
                f"W+4={row.get('effect_w4', '-') or '-'}"
            )
            lines.append(f"  cohort 结论: {row.get('diagnosis', '-') or '-'}")
            lines.append(f"  调参建议: {row.get('reason', '-') or '-'}")

    lines.append("")
    lines.append("## Weekly Feedback Auto-Apply")
    if not shadow_feedback_rows:
        lines.append("- (no auto-feedback suggestions)")
    else:
        for row in shadow_feedback_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"action={row.get('shadow_review_action', '')} "
                f"confidence={float(row.get('feedback_confidence', 0.0) or 0.0):.2f}"
                f"({row.get('feedback_confidence_label', '')}) "
                f"base={float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f} "
                f"calib={float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
                f"exec_score_delta={float(row.get('execution_shadow_score_delta', 0.0) or 0.0):+.3f} "
                f"exec_prob_delta={float(row.get('execution_shadow_prob_delta', 0.0) or 0.0):+.3f} "
                f"accumulate_delta={float(row.get('scoring_accumulate_threshold_delta', 0.0) or 0.0):+.3f} "
                f"exec_ready_delta={float(row.get('scoring_execution_ready_threshold_delta', 0.0) or 0.0):+.3f} "
                f"review_days_delta={int(row.get('plan_review_window_days_delta', 0) or 0):+d}"
            )
            if row.get("signal_penalty_symbols"):
                lines.append(f"  弱信号惩罚: {row['signal_penalty_symbols']}")
            lines.append(
                f"  Outcome 校准: samples={int(row.get('feedback_calibration_sample_count', 0) or 0)} "
                f"horizon={row.get('feedback_calibration_horizon_days', '-') or '-'} "
                f"scope={row.get('feedback_calibration_scope', '-') or '-'} "
                f"label={row.get('feedback_calibration_label', '-') or '-'}"
            )
            if row.get("feedback_calibration_reason"):
                lines.append(f"  校准说明: {row.get('feedback_calibration_reason', '')}")
            lines.append(f"  说明: {row.get('feedback_reason', '')}")

    lines.append("")
    lines.append("## Weekly Risk Auto-Apply")
    if not risk_feedback_rows:
        lines.append("- (no risk auto-feedback suggestions)")
    else:
        for row in risk_feedback_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"action={row.get('risk_feedback_action', '')} "
                f"confidence={float(row.get('feedback_confidence', 0.0) or 0.0):.2f}"
                f"({row.get('feedback_confidence_label', '')}) "
                f"base={float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f} "
                f"calib={float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
                f"single_delta={float(row.get('paper_max_single_weight_delta', 0.0) or 0.0):+.3f} "
                f"sector_delta={float(row.get('paper_max_sector_weight_delta', 0.0) or 0.0):+.3f} "
                f"net_delta={float(row.get('paper_max_net_exposure_delta', 0.0) or 0.0):+.3f} "
                f"gross_delta={float(row.get('paper_max_gross_exposure_delta', 0.0) or 0.0):+.3f} "
                f"short_delta={float(row.get('paper_max_short_exposure_delta', 0.0) or 0.0):+.3f}"
            )
            lines.append(
                f"  Outcome 校准: samples={int(row.get('feedback_calibration_sample_count', 0) or 0)} "
                f"horizon={row.get('feedback_calibration_horizon_days', '-') or '-'} "
                f"scope={row.get('feedback_calibration_scope', '-') or '-'} "
                f"label={row.get('feedback_calibration_label', '-') or '-'}"
            )
            if row.get("feedback_calibration_reason"):
                lines.append(f"  校准说明: {row.get('feedback_calibration_reason', '')}")
            lines.append(f"  说明: {row.get('feedback_reason', '')}")

    lines.append("")
    lines.append("## Weekly Execution Auto-Apply")
    if not execution_feedback_rows:
        lines.append("- (no execution auto-feedback suggestions)")
    else:
        for row in execution_feedback_rows:
            lines.append(
                f"- **{row['portfolio_id']}** market={row['market']} "
                f"action={row.get('execution_feedback_action', '')} "
                f"confidence={float(row.get('feedback_confidence', 0.0) or 0.0):.2f}"
                f"({row.get('feedback_confidence_label', '')}) "
                f"base={float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f} "
                f"calib={float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
                f"adv_delta={float(row.get('execution_adv_max_participation_pct_delta', 0.0) or 0.0):+.3f} "
                f"split_trigger_delta={float(row.get('execution_adv_split_trigger_pct_delta', 0.0) or 0.0):+.3f} "
                f"slice_delta={int(row.get('execution_max_slices_per_symbol_delta', 0) or 0):+d} "
                f"open_delta={float(row.get('execution_open_session_participation_scale_delta', 0.0) or 0.0):+.3f} "
                f"midday_delta={float(row.get('execution_midday_session_participation_scale_delta', 0.0) or 0.0):+.3f} "
                f"close_delta={float(row.get('execution_close_session_participation_scale_delta', 0.0) or 0.0):+.3f}"
            )
            lines.append(
                f"  Outcome 校准: samples={int(row.get('feedback_calibration_sample_count', 0) or 0)} "
                f"horizon={row.get('feedback_calibration_horizon_days', '-') or '-'} "
                f"scope={row.get('feedback_calibration_scope', '-') or '-'} "
                f"label={row.get('feedback_calibration_label', '-') or '-'}"
            )
            if row.get("feedback_calibration_reason"):
                lines.append(f"  校准说明: {row.get('feedback_calibration_reason', '')}")
            lines.append(
                f"  成本观察: plan={float(row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                f"actual={float(row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                f"gap={float(row.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                f"expected_bps={float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f} "
                f"actual_slippage_bps={float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}"
            )
            if row.get("execution_style_breakdown"):
                lines.append(f"  风格分布: {row.get('execution_style_breakdown', '')}")
            session_feedback_rows = []
            raw_session_feedback = str(row.get("execution_session_feedback_json", "") or "").strip()
            if raw_session_feedback:
                try:
                    parsed = json.loads(raw_session_feedback)
                    if isinstance(parsed, list):
                        session_feedback_rows = [dict(item) for item in parsed if isinstance(item, dict)]
                except Exception:
                    session_feedback_rows = []
            for session_row in session_feedback_rows:
                lines.append(
                    f"  时段反馈[{session_row.get('session_label', '-') or '-'}]: "
                    f"action={session_row.get('session_action', '')} "
                    f"scale_delta={float(session_row.get('scale_delta', 0.0) or 0.0):+.3f} "
                    f"plan={float(session_row.get('planned_execution_cost_total', 0.0) or 0.0):.2f} "
                    f"actual={float(session_row.get('execution_cost_total', 0.0) or 0.0):.2f} "
                    f"gap={float(session_row.get('execution_cost_gap', 0.0) or 0.0):.2f}"
                )
            hotspot_rows = []
            raw_hotspots = str(row.get("execution_hotspots_json", "") or "").strip()
            if raw_hotspots:
                try:
                    parsed = json.loads(raw_hotspots)
                    if isinstance(parsed, list):
                        hotspot_rows = [dict(item) for item in parsed if isinstance(item, dict)]
                except Exception:
                    hotspot_rows = []
            for hotspot in hotspot_rows[:3]:
                lines.append(
                    f"  热点[{hotspot.get('session_label', '-') or '-'} / {hotspot.get('symbol', '-') or '-'}]: "
                    f"pressure={float(hotspot.get('pressure_score', 0.0) or 0.0):.2f} "
                    f"gap={float(hotspot.get('execution_cost_gap', 0.0) or 0.0):.2f} "
                    f"actual_slippage_bps={float(hotspot.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}"
                )
            if row.get("execution_penalty_symbols"):
                lines.append(f"  执行热点惩罚: {row.get('execution_penalty_symbols', '')}")
            lines.append(f"  说明: {row.get('feedback_reason', '')}")

    lines.append("")
    lines.append("## Trade Reason Summary")
    if not reason_rows:
        lines.append("- (no trades)")
    else:
        for row in reason_rows[:20]:
            lines.append(
                f"- {row['portfolio_id']} {row['action']} reason={row['reason']} "
                f"count={int(row.get('trade_count', 0) or 0)} "
                f"value={float(row.get('trade_value', 0.0) or 0.0):.2f}"
            )

    lines.append("")
    lines.append("## Trade Log")
    if not trade_rows:
        lines.append("- (no trades)")
    else:
        for row in trade_rows[:50]:
            lines.append(
                f"- {row['ts']} {row['portfolio_id']} {row['action']} {row['symbol']} "
                f"qty={float(row.get('qty', 0.0) or 0.0):.0f} price={float(row.get('price', 0.0) or 0.0):.2f} "
                f"value={float(row.get('trade_value', 0.0) or 0.0):.2f} reason={row.get('reason', '')}"
            )
    path.write_text("\n".join(lines), encoding="utf-8")
