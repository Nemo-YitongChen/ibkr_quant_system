from pathlib import Path

from src.tools.generate_dashboard import (
    _build_health_overview,
    _build_market_data_health_overview,
    _dashboard_market_state_label,
    _dashboard_report_freshness_label,
)
from src.tools.review_investment_weekly import _weekly_strategy_note
from src.tools.review_weekly_io import read_csv_rows
from src.tools.review_weekly_markdown import write_weekly_review_markdown
from src.tools.review_weekly_thresholds import (
    build_feedback_threshold_suggestion_rows,
    load_feedback_threshold_overrides,
)


def test_load_feedback_threshold_overrides_normalizes_market_keys(tmp_path: Path):
    path = tmp_path / "thresholds.yaml"
    path.write_text(
        "markets:\n  us:\n    execution:\n      auto_confidence: 0.7\n",
        encoding="utf-8",
    )
    overrides = load_feedback_threshold_overrides(path)
    assert overrides == {"US": {"execution": {"auto_confidence": 0.7}}}


def test_build_feedback_threshold_suggestion_rows_relaxes_on_consistent_improvement():
    rows = build_feedback_threshold_suggestion_rows(
        [
            {
                "market": "US",
                "feedback_kind": "execution",
                "feedback_kind_label": "执行参数",
                "summary_signal": "持续改善",
                "tracked_count": 2,
                "avg_active_weeks": 3.0,
                "latest_improved_count": 1,
                "latest_deteriorated_count": 0,
                "w1_improved_count": 1,
                "w2_improved_count": 1,
                "w4_improved_count": 0,
                "w1_deteriorated_count": 0,
                "w2_deteriorated_count": 0,
                "w4_deteriorated_count": 0,
                "top_portfolios_text": "US:watchlist: 改善",
            }
        ]
    )
    assert rows[0]["suggestion_action"] == "RELAX_AUTO_APPLY"


def test_read_csv_rows_and_markdown_writer(tmp_path: Path):
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("symbol,score\nAAPL,1\n", encoding="utf-8")
    rows = read_csv_rows(csv_path)
    assert rows == [{"symbol": "AAPL", "score": "1"}]

    out_path = tmp_path / "weekly_review.md"
    write_weekly_review_markdown(
        out_path,
        summary_rows=[
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "weekly_return": 0.01,
                "max_drawdown": -0.02,
                "executed_rebalances": 1,
                "turnover": 0.12,
                "latest_equity": 100000.0,
                "cash_after": 10000.0,
                "holdings_count": 5,
                "account_profile_label": "小资金",
                "account_profile_summary": "先做 ETF 和高流动性基础工具，减少小额高频调仓。",
                "market_rules_summary": "settlement=T+1 | buy_lot=1 | ETF-first below 25000.00",
                "adaptive_strategy_name": "ACM-RS",
                "adaptive_strategy_summary": "ACM-RS | RS=126/63/20 | rebalance=weekly | entry_delay=15-30m",
                "adaptive_strategy_market_profile_note": "当前使用 US trend-first 市场档案；计划=staged=3x | no_trade_band=3.0%；regime=vol=1.00%/1.80% | risk_on=0.50；执行=min_edge=16.0bps | edge_buffer=5.0bps。",
                "market_profile_tuning_note": "本周压仓主要来自策略主动控仓，优先复核 risk_on / hard_risk_off、no_trade_band 和 turnover_penalty，而不是先改风险 overlay。（策略 6.0% | 风险 12.0% | 执行 0.0%）",
                "market_profile_readiness_summary": "当前仅连续 1 周维持同方向，先继续观察到至少 2 周再决定是否人工应用。",
                "strategy_effective_controls_note": "策略主动转入防守，按 中等资金 上限把有效目标仓位从 36% 收到 30%。",
                "execution_gate_summary": "另外有 2 笔计划单因执行 gate 暂未下发（流动性 1，人工复核 1）。",
                "weekly_strategy_note": "本周有 2 个新开仓机会因防守环境被降级为观察，先不把回撤信号直接转成加仓动作。",
            }
        ],
        trade_rows=[],
        broker_summary_rows=[],
        broker_diff_rows=[],
        reason_rows=[],
        shadow_summary_rows=[],
        shadow_feedback_rows=[],
        feedback_calibration_rows=[],
        feedback_automation_rows=[],
        feedback_effect_market_summary_rows=[],
        feedback_threshold_suggestion_rows=[],
        feedback_threshold_history_overview_rows=[],
        feedback_threshold_effect_overview_rows=[],
        feedback_threshold_cohort_overview_rows=[],
        feedback_threshold_trial_alert_rows=[],
        feedback_threshold_tuning_rows=[],
        labeling_summary={},
        labeling_skip_rows=[],
        outcome_spread_rows=[],
        edge_realization_rows=[],
        blocked_edge_attribution_rows=[],
        attribution_rows=[
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "attribution_mode": "proxy_v1",
                "weekly_return": 0.01,
                "selection_contribution": 0.004,
                "sizing_contribution": 0.001,
                "sector_contribution": 0.001,
                "execution_contribution": -0.001,
                "market_contribution": 0.005,
                "planned_execution_cost_total": 12.0,
                "execution_cost_total": 10.0,
                "execution_cost_gap": -2.0,
                "avg_expected_cost_bps": 10.0,
                "avg_actual_slippage_bps": 8.0,
                "control_split_text": "策略 6.0% | 风险 12.0% | 执行 0.0%",
                "diagnosis": "收益主要由选股质量驱动，优先复盘信号与候选排序。",
            }
        ],
        risk_review_rows=[],
        risk_feedback_rows=[],
        execution_session_rows=[],
        execution_hotspot_rows=[],
        execution_feedback_rows=[],
        control_timeseries_rows=[],
        window_label="2026-W13",
        calibration_patch_suggestion_rows=[
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "scope_label": "执行门槛",
                "field": "edge_cost_buffer_bps",
                "current_value": 5.0,
                "suggested_value": 4.0,
                "config_path": "market_profiles.US.edge_cost_buffer_bps",
                "change_hint_label": "按放松方向温和下调",
                "source_signal_label": "edge gate 偏紧",
                "priority_label": "先改低风险 buffer",
                "source_note": "被 edge gate 挡掉的单事后并不差，当前 edge floor/buffer 可能偏紧。",
            }
        ],
        patch_governance_rows=[
            {
                "market": "US",
                "patch_kind_label": "校准补丁",
                "field": "adv_split_trigger_pct",
                "scope_label": "执行切片",
                "review_cycle_count": 2,
                "open_cycle_count": 1,
                "approved_not_applied_count": 1,
                "approval_rate": 0.5,
                "rejection_rate": 0.0,
                "apply_rate": 0.5,
                "review_latency_basis": "review_to_apply",
                "avg_review_to_apply_weeks": 1.0,
                "latest_week_label": "2026-W13",
                "latest_status_label": "已批准",
                "examples": "US:watchlist:已批准",
            }
        ],
    )
    text = out_path.read_text(encoding="utf-8")
    assert "# Weekly Investment Review" in text
    assert "## Broker Execution Summary" in text
    assert "账户档位: 小资金" in text
    assert "市场约束: settlement=T+1" in text
    assert "策略框架: ACM-RS" in text
    assert "市场档案: 当前使用 US trend-first 市场档案" in text
    assert "参数调优: 本周压仓主要来自策略主动控仓" in text
    assert "建议状态: 当前仅连续 1 周维持同方向" in text
    assert "策略控仓: 策略主动转入防守" in text
    assert "执行阻断: 另外有 2 笔计划单因执行 gate 暂未下发" in text
    assert "周度解释: 本周有 2 个新开仓机会因防守环境被降级为观察" in text
    assert "控制拆解: 策略 6.0% | 风险 12.0% | 执行 0.0%" in text
    assert "## Calibration Patch Suggestions" in text
    assert "market_profiles.US.edge_cost_buffer_bps" in text
    assert "edge gate 偏紧" in text
    assert "## Patch Governance Summary" in text
    assert "adv_split_trigger_pct" in text
    assert "avg_review_to_apply_weeks=1.0" in text


def test_weekly_strategy_note_prefers_defensive_cap_message() -> None:
    note = _weekly_strategy_note(
        market_rules={"research_only": False, "small_account_rule_active": True, "small_account_preferred_asset_classes": ["etf"]},
        account_profile={"label": "小资金", "summary": "先做 ETF"},
        adaptive_strategy={"name": "ACM-RS"},
        opportunity_summary={"adaptive_strategy_wait_count": 2},
        market_sentiment={"label": "DEFENSIVE"},
        strategy_effective_controls_note="",
        execution_gate_summary="",
    )
    assert "2 个新开仓机会" in note


def test_weekly_strategy_note_prefers_strategy_control_and_execution_gate_messages() -> None:
    note = _weekly_strategy_note(
        market_rules={"research_only": False},
        account_profile={"label": "中等资金"},
        adaptive_strategy={"name": "ACM-RS"},
        opportunity_summary={"adaptive_strategy_wait_count": 1},
        market_sentiment={"label": "BALANCED"},
        strategy_effective_controls_note="策略主动转入防守，按 中等资金 上限把有效目标仓位从 36% 收到 30%。",
        execution_gate_summary="另外有 2 笔计划单因执行 gate 暂未下发（流动性 1，人工复核 1）。",
    )
    assert "策略主动转入防守" in note
    assert "1 个新开仓机会" in note
    assert "2 笔计划单因执行 gate" in note


def test_dashboard_report_freshness_label_marks_stale_when_generation_lags() -> None:
    label = _dashboard_report_freshness_label(
        market="US",
        report_date="2026-04-10",
        latest_generated_at="2026-04-13T09:30:00",
        as_of_date="2026-04-19",
    )
    assert "待刷新" in label
    assert "US" in label


def test_dashboard_report_freshness_label_marks_ready_when_generation_is_recent() -> None:
    label = _dashboard_report_freshness_label(
        market="HK",
        report_date="2026-04-18",
        latest_generated_at="2026-04-19T08:00:00",
        as_of_date="2026-04-19",
    )
    assert "已更新" in label
    assert "HK" in label


def test_dashboard_market_state_label_handles_missing_summary() -> None:
    label = _dashboard_market_state_label(None)
    assert label == "市场状态: 暂无数据"


def test_build_health_overview_flags_degraded_and_warning_states() -> None:
    rows = _build_health_overview(
        [
            {"status": "ready", "summary": "US 正常"},
            {"status": "warning", "summary": "HK 需复核"},
            {"status": "degraded", "summary": "CN 数据不完整"},
        ]
    )
    assert rows[0]["status"] == "degraded"
    assert "CN 数据不完整" in rows[0]["summary"]
    assert "HK 需复核" in rows[0]["summary"]


def test_build_market_data_health_overview_handles_empty_rows() -> None:
    rows = _build_market_data_health_overview([])
    assert rows[0]["status"] == "warning"
    assert "暂无市场数据健康检查结果" in rows[0]["summary"]
