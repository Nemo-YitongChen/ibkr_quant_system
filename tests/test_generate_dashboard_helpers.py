from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import src.tools.generate_dashboard as generate_dashboard
from src.tools.generate_dashboard import (
    _build_auto_order_readiness_health,
    _build_dashboard_status_rollout_summary,
    _build_evidence_focus_actions,
    _build_evidence_focus_summary,
    _build_health_overview,
    _build_market_data_health_overview,
    _build_gateway_runtime_summary,
    _build_card_evidence_action_summary,
    _build_evidence_action_summary,
    _build_market_evidence_action_summary,
    _build_overview,
    _build_ops_overview,
    _build_review_artifact_health_rows,
    _dashboard_v2_block_metrics_text,
    _load_weekly_blocked_vs_allowed_expost_rows,
    _load_weekly_unified_evidence_rows,
    _load_watchlist_expansion_payload,
    _simple_gateway_is_connected,
    _simple_gateway_runtime_text,
    _simple_market_evidence_action_rows,
    _simple_next_step_text,
    _simple_ops_overview_rows,
    _simple_weekly_strategy_context_rows,
    _dashboard_market_state_label,
    _dashboard_report_freshness_label,
    _translate_market_status_label_en,
    _translate_report_freshness_label_en,
)


def test_dashboard_report_freshness_label_keeps_legacy_calls() -> None:
    assert _dashboard_report_freshness_label("fresh") == "报告已更新"
    assert _dashboard_report_freshness_label("stale") == "报告待刷新"


def test_dashboard_report_freshness_label_supports_market_and_time_semantics() -> None:
    stale = _dashboard_report_freshness_label(
        market="US",
        report_date="2026-04-10",
        latest_generated_at="2026-04-13T09:30:00",
        as_of_date="2026-04-19",
    )
    fresh = _dashboard_report_freshness_label(
        market="HK",
        report_date="2026-04-19",
        latest_generated_at="2026-04-19T09:30:00",
        as_of_date="2026-04-19",
    )
    assert "US" in stale
    assert "待刷新" in stale
    assert "HK" in fresh
    assert "已更新" in fresh


def test_dashboard_market_state_label_supports_none() -> None:
    assert _dashboard_market_state_label(None) == "市场状态: 暂无数据"
    assert _dashboard_market_state_label(True) == "开市中"
    assert _dashboard_market_state_label(False) == "已闭市"


def test_dashboard_v2_block_metrics_text_renders_advanced_html_metrics() -> None:
    text = _dashboard_v2_block_metrics_text(
        {
            "metrics": {
                "market_count": 3,
                "portfolio_count": 7,
                "nested": {"ignored": True},
            }
        }
    )

    assert "market_count=3" in text
    assert "portfolio_count=7" in text
    assert "nested" not in text


def test_build_evidence_action_summary_extracts_evidence_quality_block() -> None:
    summary = _build_evidence_action_summary(
        [
            {"id": "ops_health", "metrics": {"ignored": 1}},
            {
                "id": "evidence_quality",
                "status": "warn",
                "summary": "action=Review gate thresholds",
                "metrics": {
                    "primary_action": "review_gate_thresholds",
                    "action_label": "Review gate thresholds",
                    "action_note": "Blocked rows outperformed allowed rows.",
                    "evidence_row_count": "12",
                    "blocked_review_count": 3,
                    "sample_ready_review_count": 2,
                    "insufficient_sample_count": 1,
                    "too_restrictive_count": 1,
                    "candidate_model_warning_count": 0,
                },
                "rows": {
                    "blocked_vs_allowed_label_summary": [
                        {
                            "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
                            "count": 1,
                            "action": "review_gate_thresholds",
                        }
                    ]
                },
            },
        ]
    )

    assert summary["status"] == "warn"
    assert summary["primary_action"] == "review_gate_thresholds"
    assert summary["action_label"] == "Review gate thresholds"
    assert summary["decision_basis"] == "blocked_outperformed_allowed"
    assert "too_restrictive=1" in summary["rationale"]
    assert summary["blocked_label_summary"][0]["review_label"] == "BLOCKED_OUTPERFORMED_ALLOWED"
    assert summary["evidence_row_count"] == 12
    assert summary["sample_ready_review_count"] == 2


def test_build_evidence_action_summary_handles_missing_block() -> None:
    summary = _build_evidence_action_summary([])

    assert summary["primary_action"] == ""
    assert summary["decision_basis"] == "no_unified_evidence"
    assert summary["evidence_row_count"] == 0


def test_build_card_evidence_action_summary_scopes_to_portfolio() -> None:
    summary = _build_card_evidence_action_summary(
        {"market": "US", "portfolio_id": "US:paper"},
        unified_evidence_rows=[
            {
                "market": "US",
                "portfolio_id": "US:paper",
                "symbol": "AAPL",
                "blocked_flag": 1,
                "join_quality": "order_fill_outcome",
            },
            {
                "market": "HK",
                "portfolio_id": "HK:paper",
                "symbol": "0700.HK",
                "blocked_flag": 1,
            },
        ],
        blocked_vs_allowed_rows=[
            {
                "market": "US",
                "portfolio_id": "US:paper",
                "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
            },
            {
                "market": "HK",
                "portfolio_id": "HK:paper",
                "review_label": "BLOCKING_HELPED",
            },
        ],
        candidate_model_rows=[],
        waterfall_rows=[],
    )

    assert summary["scope"] == "portfolio"
    assert summary["portfolio_id"] == "US:paper"
    assert summary["primary_action"] == "review_gate_thresholds"
    assert summary["decision_basis"] == "blocked_outperformed_allowed"
    assert summary["evidence_row_count"] == 1
    assert summary["too_restrictive_count"] == 1


def test_build_market_evidence_action_summary_groups_by_market() -> None:
    summary = _build_market_evidence_action_summary(
        ["US", "HK"],
        unified_evidence_rows=[
            {"market": "US", "portfolio_id": "US:paper", "symbol": "AAPL", "blocked_flag": 1},
            {"market": "HK", "portfolio_id": "HK:paper", "symbol": "0700.HK", "blocked_flag": 1},
        ],
        blocked_vs_allowed_rows=[
            {"market": "US", "portfolio_id": "US:paper", "review_label": "BLOCKED_OUTPERFORMED_ALLOWED"},
            {"market": "HK", "portfolio_id": "HK:paper", "review_label": "BLOCKING_HELPED"},
        ],
        candidate_model_rows=[],
        waterfall_rows=[],
    )

    assert summary["US"]["scope"] == "market"
    assert summary["US"]["primary_action"] == "review_gate_thresholds"
    assert summary["US"]["decision_basis"] == "blocked_outperformed_allowed"
    assert summary["HK"]["primary_action"] == "keep_gate_monitor_post_cost"
    assert summary["HK"]["decision_basis"] == "blocking_helped_post_cost"


def test_build_evidence_focus_actions_prioritizes_actionable_market_work() -> None:
    rows = _build_evidence_focus_actions(
        {
            "US": {
                "market": "US",
                "primary_action": "review_signal_expected_edge",
                "action_label": "Review signal expected edge",
                "basis_label": "Candidate model warning",
                "action_note": "Calibrate signal score to realized edge.",
                "evidence_row_count": 8,
            },
            "HK": {
                "market": "HK",
                "primary_action": "review_gate_thresholds",
                "action_label": "Review gate thresholds",
                "basis_label": "Blocked outperformed allowed",
                "action_note": "Review edge floor and buffers.",
                "evidence_row_count": 12,
            },
            "CN": {
                "market": "CN",
                "primary_action": "build_weekly_unified_evidence",
                "action_label": "Build unified evidence",
                "basis_label": "No unified evidence",
                "action_note": "Regenerate weekly evidence.",
            },
            "ASX": {
                "market": "ASX",
                "primary_action": "collect_more_outcome_samples",
                "action_label": "Collect more outcome samples",
                "basis_label": "Insufficient blocked-vs-allowed sample",
                "action_note": "Keep collecting candidate outcomes.",
            },
        },
        limit=3,
    )

    assert [row["market"] for row in rows] == ["HK", "US", "CN"]
    assert [row["priority_order"] for row in rows] == [10, 20, 30]
    assert rows[0]["evidence_row_count"] == 12


def test_build_evidence_focus_actions_skips_monitor_only_markets() -> None:
    rows = _build_evidence_focus_actions(
        {
            "US": {
                "primary_action": "keep_gate_monitor_post_cost",
                "action_label": "Keep gate and monitor",
            },
            "HK": {
                "primary_action": "monitor_evidence",
                "action_label": "Monitor evidence",
            },
            "bad": "legacy malformed summary",
        }
    )

    assert rows == []
    assert _build_evidence_focus_actions("legacy bad payload") == []


def test_build_evidence_focus_summary_uses_top_ranked_action() -> None:
    summary = _build_evidence_focus_summary(
        [
            {
                "market": "HK",
                "primary_action": "review_gate_thresholds",
                "action": "Review gate thresholds",
                "basis": "Blocked outperformed allowed",
                "detail": "Review edge floor and buffers.",
                "priority_order": 10,
                "evidence_row_count": 12,
            },
            {
                "market": "US",
                "primary_action": "collect_more_outcome_samples",
                "action": "Collect more outcome samples",
                "basis": "Insufficient blocked-vs-allowed sample",
                "priority_order": 60,
            },
        ]
    )

    assert summary["status"] == "warn"
    assert summary["primary_market"] == "HK"
    assert summary["primary_action"] == "review_gate_thresholds"
    assert summary["focus_action_count"] == 2
    assert summary["urgent_action_count"] == 1
    assert summary["gate_review_count"] == 1
    assert summary["sample_collection_count"] == 1
    assert summary["read_only"] is True
    assert "HK: Review gate thresholds" in summary["summary_text"]


def test_build_evidence_focus_summary_handles_empty_input() -> None:
    summary = _build_evidence_focus_summary([])

    assert summary["status"] == "ok"
    assert summary["primary_action"] == ""
    assert summary["focus_action_count"] == 0
    assert summary["summary_text"] == "No actionable evidence focus work."


def test_simple_gateway_connected_treats_limited_permissions_as_connected() -> None:
    health = {
        "status": "LIMITED",
        "status_detail": "perm=2 delayed=1",
        "permission_count": 2,
        "delayed_count": 1,
    }

    assert _simple_gateway_is_connected(health) is True
    next_step = _simple_next_step_text(
        mode="paper-auto-submit",
        is_dry_run_view=False,
        open_flag=True,
        report_fresh="fresh",
        gateway_status_label="LIMITED",
        gateway_connected=True,
        action_label="观察",
        action_detail="",
        recommendation_differs=False,
        recommended_execution_mode_label="-",
    )

    assert "IB Gateway 已连接" in next_step
    assert "先启动 IB Gateway" not in next_step


def test_simple_gateway_disconnected_only_for_unresolved_connectivity_break() -> None:
    health = {
        "status": "DEGRADED",
        "status_detail": "127.0.0.1:4002 not_listening",
        "connectivity_breaks": 1,
        "connectivity_restores": 0,
    }

    assert _simple_gateway_is_connected(health) is False
    assert _simple_next_step_text(
        mode="paper-auto-submit",
        is_dry_run_view=False,
        open_flag=True,
        report_fresh="fresh",
        gateway_status_label="DEGRADED",
        gateway_connected=False,
        action_label="观察",
        action_detail="",
        recommendation_differs=False,
        recommended_execution_mode_label="-",
    ) == "先启动 IB Gateway，并确认 paper/live 目标端口可连接。"


def test_simple_next_step_uses_evidence_action_when_no_higher_priority_blocker() -> None:
    text = _simple_next_step_text(
        mode="paper-auto-submit",
        is_dry_run_view=False,
        open_flag=True,
        report_fresh="fresh",
        gateway_status_label="OK",
        gateway_connected=True,
        action_label="观察",
        action_detail="",
        recommendation_differs=False,
        recommended_execution_mode_label="-",
        evidence_primary_action="collect_more_outcome_samples",
        evidence_action_label="Collect more outcome samples",
        evidence_action_note="Blocked-vs-allowed evidence is sample-starved.",
    )

    assert "Evidence 建议：Collect more outcome samples" in text
    assert "sample-starved" in text


def test_gateway_runtime_summary_treats_listening_port_as_idle_client() -> None:
    summary = _build_gateway_runtime_summary(
        {
            "checks": [
                {
                    "name": "ibkr_port:127.0.0.1:4002",
                    "status": "PASS",
                    "detail": "127.0.0.1:4002 listening markets=US",
                }
            ]
        },
        {
            "service": {"status": "running"},
            "actions": {"run_once_in_progress": False},
        },
    )

    assert summary["status"] == "client_idle"
    assert summary["api_client_state"] == "idle"
    assert "Gateway socket 已就绪" in summary["status_label"]
    assert "API客户端已断开" in summary["action"]


def test_gateway_runtime_summary_keeps_gateway_start_action_for_unavailable_port() -> None:
    summary = _build_gateway_runtime_summary(
        {
            "checks": [
                {
                    "name": "ibkr_port:127.0.0.1:4002",
                    "status": "WARN",
                    "detail": "127.0.0.1:4002 not_listening",
                }
            ]
        },
        {"service": {"status": "running"}, "actions": {}},
    )

    assert summary["status"] == "port_unavailable"
    assert summary["api_client_state"] == "disconnected"
    assert "先启动 IB Gateway" in summary["action"]


def test_simple_gateway_runtime_text_explains_idle_client() -> None:
    text = _simple_gateway_runtime_text(
        {
            "gateway_runtime_summary": {
                "status_label": "Gateway socket 已就绪，量化客户端空闲",
                "action": "需要执行时启动 supervisor 或点击 run_once。",
            }
        }
    )

    assert "量化客户端空闲" in text
    assert "run_once" in text


def test_simple_weekly_strategy_context_rows_include_no_trade_optimization_note() -> None:
    rows = _simple_weekly_strategy_context_rows(
        {
            "weekly_strategy_context": {
                "weekly_strategy_note": "本周没有成交。",
                "no_trade_optimization_note": "用候选快照和 shadow 回标继续校准。",
            }
        }
    )

    assert ["无成交优化", "用候选快照和 shadow 回标继续校准。"] in rows


def test_simple_weekly_strategy_context_rows_include_evidence_action() -> None:
    rows = _simple_weekly_strategy_context_rows(
        {
            "weekly_strategy_context": {
                "weekly_strategy_note": "本周没有成交。",
            },
            "evidence_action_summary": {
                "action_label": "Collect more outcome samples",
                "action_note": "Blocked-vs-allowed evidence is sample-starved.",
                "evidence_row_count": 8,
                "blocked_review_count": 2,
                "sample_ready_review_count": 0,
                "insufficient_sample_count": 2,
                "rationale": "Insufficient blocked-vs-allowed sample: evidence_rows=8, blocked_reviews=2, ready=0, insufficient=2.",
            },
        }
    )

    assert [
        "Evidence下一步",
        "Collect more outcome samples：Blocked-vs-allowed evidence is sample-starved.",
    ] in rows
    assert [
        "Evidence依据",
        "Insufficient blocked-vs-allowed sample: evidence_rows=8, blocked_reviews=2, ready=0, insufficient=2.",
    ] in rows
    assert ["Evidence样本", "rows=8 / blocked_reviews=2 / ready=0 / insufficient=2"] in rows


def test_simple_market_evidence_action_rows_use_market_summary() -> None:
    rows = _simple_market_evidence_action_rows(
        {
            "US": {
                "market": "US",
                "evidence_action_label": "Monitor evidence",
                "evidence_row_count": 2,
            },
        },
        {
            "US": {
                "action_label": "Review gate thresholds",
                "basis_label": "Blocked outperformed allowed",
                "action_note": "Blocked rows outperformed allowed rows.",
                "evidence_row_count": 8,
            },
        },
    )

    assert rows == [
        [
            "US",
            "Review gate thresholds",
            "Blocked outperformed allowed",
            "rows=8",
            "Blocked rows outperformed allowed rows.",
        ]
    ]


def test_simple_market_evidence_action_rows_fallback_to_market_views() -> None:
    rows = _simple_market_evidence_action_rows(
        {
            "HK": {
                "market": "HK",
                "evidence_action_label": "Collect more outcome samples",
                "evidence_basis_label": "Insufficient blocked-vs-allowed sample",
                "evidence_rationale": "Evidence sample is not ready yet.",
                "evidence_row_count": 3,
            },
            "bad": "legacy malformed row",
        }
    )

    assert rows == [
        [
            "HK",
            "Collect more outcome samples",
            "Insufficient blocked-vs-allowed sample",
            "rows=3",
            "Evidence sample is not ready yet.",
        ]
    ]


def test_build_health_overview_prefers_degraded_and_merges_summary() -> None:
    rows = _build_health_overview(
        [
            {"status": "ready", "summary": "整体正常"},
            {"status": "warning", "summary": "权限有波动"},
            {"status": "degraded", "summary": "连接短时降级"},
        ]
    )
    assert rows[0]["status"] == "degraded"
    assert "连接短时降级" in rows[0]["summary"]
    assert "权限有波动" in rows[0]["summary"]


def test_build_market_data_health_overview_empty_returns_warning_summary() -> None:
    rows = _build_market_data_health_overview([])
    assert rows[0]["status"] == "warning"
    assert rows[0]["summary"] == "暂无市场数据健康检查结果"


def test_dashboard_label_translation_helpers_support_dynamic_market_labels() -> None:
    assert _translate_report_freshness_label_en("US 已更新") == "US Report Fresh"
    assert _translate_report_freshness_label_en("HK 待刷新") == "HK Report Needs Refresh"
    assert _translate_market_status_label_en("市场状态: 暂无数据") == "Market State: No Data"


def test_build_overview_includes_freshness_and_health_fields() -> None:
    rows = _build_overview(
        [
            {
                "market": "US",
                "watchlist": "watchlist",
                "mode": "paper-auto-submit",
                "exchange_open": True,
                "market_state_label": "开市中",
                "report_freshness_label": "US 待刷新",
                "report_status_label": "US 待刷新",
                "health_overview": [{"status_label": "有降级", "summary": "连接短时降级"}],
                "market_data_health_overview": [{"status_label": "待排查", "summary": "IBKR 历史覆盖不足"}],
                "priority_order": 1,
                "recommended_action": "可执行调仓",
                "recommended_detail": "BUY AAPL",
                "paper_summary": {"equity_after": 100000.0, "cash_after": 10000.0},
                "execution_summary": {"broker_equity": 100500.0, "broker_cash": 9500.0},
                "opportunity_summary": {"entry_now_count": 1, "wait_count": 2},
                "health_summary": {"status": "DEGRADED"},
                "dashboard_view": "trade",
            }
        ]
    )
    assert rows[0]["market_state_label"] == "开市中"
    assert rows[0]["report_freshness_label"] == "US 待刷新"
    assert rows[0]["health_status_label"] == "有降级"
    assert rows[0]["market_data_status_label"] == "待排查"


def test_build_dashboard_status_rollout_summary_tracks_rollout_gaps() -> None:
    summary = _build_dashboard_status_rollout_summary(
        [
            {
                "market": "US",
                "exchange_open_raw": None,
                "report_freshness_label": "US 待刷新",
                "report_status": {"fresh": False},
                "health_overview": [{"status": "degraded", "status_label": "有降级", "summary": "连接短时降级"}],
                "market_data_health_overview": [{"status": "warning", "status_label": "待排查", "summary": "IBKR 历史覆盖不足"}],
            },
            {
                "market": "US",
                "exchange_open_raw": True,
                "report_freshness_label": "US 已更新",
                "report_status": {"fresh": True},
                "health_overview": [{"status": "ready", "status_label": "已就绪", "summary": "整体正常"}],
                "market_data_health_overview": [{"status": "ready", "status_label": "IBKR正常", "summary": "覆盖稳定"}],
            },
            {
                "market": "HK",
                "exchange_open_raw": False,
                "report_freshness_label": "HK 已更新",
                "report_status": {"fresh": True},
                "health_overview": [{"status": "warning", "status_label": "有告警", "summary": "权限有波动"}],
                "market_data_health_overview": [{"status": "warning", "status_label": "研究Fallback", "summary": "research-only fallback"}],
            },
        ]
    )
    assert summary["portfolio_count"] == 3
    assert summary["market_state_missing_count"] == 1
    assert summary["report_stale_count"] == 1
    assert summary["ops_degraded_count"] == 1
    assert summary["ops_warning_count"] == 1
    assert summary["data_attention_count"] == 1
    assert summary["data_research_fallback_count"] == 1
    assert summary["market_rows"][0]["market"] == "US"


def test_simple_ops_overview_rows_include_status_rollout_counts() -> None:
    rows = _simple_ops_overview_rows(
        {
            "preflight_pass_count": 3,
            "preflight_warn_count": 1,
            "preflight_fail_count": 0,
            "ibkr_port_warning_count": 1,
            "gateway_runtime_summary": {
                "status_label": "Gateway socket 已就绪，量化客户端空闲",
            },
            "market_state_missing_count": 2,
            "stale_report_count": 1,
            "degraded_health_count": 1,
            "data_attention_count": 0,
            "data_research_fallback_count": 1,
            "execution_mode_mismatch_count": 0,
            "evidence_focus_action_count": 2,
            "evidence_focus_urgent_count": 1,
            "evidence_focus_primary_market": "HK",
            "evidence_focus_primary_action": "Review gate thresholds",
            "auto_order_submit_plan_status": "READY_SINGLE_CANDIDATE",
            "auto_order_submit_selected_portfolio_id": "US:watchlist",
            "open_market_analysis_status_label": "开市分析可用",
            "open_market_portfolio_count": 1,
            "open_market_fresh_report_count": 1,
            "open_market_auto_ready_count": 1,
            "open_market_auto_blocked_count": 0,
            "control_service_status": "configured",
        }
    )
    assert any(row[0] == "市场状态缺口" and "2" in row[1] for row in rows)
    assert any(row[0] == "市场数据健康" and "研究Fallback" in row[1] for row in rows)
    assert any(row[0] == "量化客户端" and "量化客户端空闲" in row[1] for row in rows)
    assert any(row[0] == "自动下单" and "US:watchlist" in row[1] for row in rows)
    assert any(row[0] == "开市交易分析" and "auto_ready=1" in row[1] for row in rows)
    assert any(row[0] == "Evidence复核" and "HK Review gate thresholds" in row[1] for row in rows)


def test_build_ops_overview_surfaces_artifact_and_governance_alerts() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={
            "status": "degraded",
            "status_label": "有降级",
            "summary_text": "artifact 3 | degraded 1",
            "warning_count": 0,
            "degraded_count": 1,
        },
        governance_health_summary={
            "status": "warning",
            "status_label": "有告警",
            "summary_text": "pending governance action",
        },
    )

    categories = {row["category"]: row for row in overview["alert_rows"]}
    assert categories["ARTIFACT"]["status"] == "FAIL"
    assert categories["ARTIFACT"]["alert_class"] == "artifact_contract"
    assert categories["ARTIFACT"]["alert_severity"] == "fail"
    assert "artifact 3" in categories["ARTIFACT"]["detail"]
    assert categories["GOVERNANCE"]["status"] == "WARN"
    assert categories["GOVERNANCE"]["alert_class"] == "governance"
    assert "pending governance" in categories["GOVERNANCE"]["detail"]


def test_build_ops_overview_surfaces_urgent_evidence_focus_alert() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        evidence_focus_summary={
            "summary_text": "HK: Review gate thresholds; basis=Blocked outperformed allowed; urgent=1/2.",
            "primary_market": "HK",
            "primary_action_label": "Review gate thresholds",
            "focus_action_count": 2,
            "urgent_action_count": 1,
        },
    )

    categories = {row["category"]: row for row in overview["alert_rows"]}
    assert overview["evidence_focus_urgent_count"] == 1
    assert overview["evidence_focus_primary_market"] == "HK"
    assert categories["EVIDENCE"]["status"] == "WARN"
    assert categories["EVIDENCE"]["alert_class"] == "evidence"
    assert "Review gate thresholds" in categories["EVIDENCE"]["detail"]
    assert overview["alert_class_counts"]["evidence"] == 1


def test_build_ops_overview_keeps_sample_only_evidence_non_alerting() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        evidence_focus_summary={
            "summary_text": "HK: Collect more outcome samples; urgent=0/1.",
            "primary_market": "HK",
            "primary_action_label": "Collect more outcome samples",
            "focus_action_count": 1,
            "urgent_action_count": 0,
        },
    )

    assert overview["evidence_focus_action_count"] == 1
    assert overview["evidence_focus_urgent_count"] == 0
    assert all(row["category"] != "EVIDENCE" for row in overview["alert_rows"])


def test_build_ops_overview_surfaces_ibkr_gateway_budget_alert() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        ibkr_gateway_budget_summary={
            "status": "warning",
            "summary_text": "gateway_requests=2200 cache_hits=300 over_budget=1",
            "gateway_request_count": 2200,
            "cache_hit_count": 300,
            "cache_hit_ratio": 0.12,
            "over_budget_market_count": 1,
            "stale_telemetry_market_count": 0,
            "missing_telemetry_market_count": 0,
            "max_budget_usage_pct": 110.0,
        },
    )

    categories = {row["category"]: row for row in overview["alert_rows"]}
    assert overview["ibkr_gateway_budget_status"] == "warning"
    assert overview["ibkr_gateway_budget_gateway_request_count"] == 2200
    assert overview["ibkr_gateway_budget_over_budget_market_count"] == 1
    assert "gateway_budget=warning" in overview["summary_text"]
    assert categories["IBKR_GATEWAY"]["status"] == "WARN"
    assert "gateway_requests=2200" in categories["IBKR_GATEWAY"]["detail"]


def test_build_ops_overview_surfaces_auto_order_submit_plan_alert() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        auto_order_readiness={
            "summary": {
                "status": "blocked",
                "summary_text": "auto_order_readiness portfolios=2 ready=1 warning=0 blocked=1 disabled=0",
                "blocked_count": 1,
                "ready_count": 1,
                "primary_block_reason": "preflight_stale",
                "offline_recovery_required_count": 1,
                "offline_recovery_summary_text": "offline_recovery_required=1 markets=US top_reason=preflight_stale_after_offline_gap",
                "submit_plan": {
                    "status": "BLOCKED",
                    "ready": False,
                    "reason": "no_single_safe_submit_candidate",
                    "frontier_candidates": [{"portfolio_id": "US:watchlist"}],
                    "rejected_candidates": [{"portfolio_id": "HK:bluechip"}],
                },
            }
        },
    )

    categories = {row["category"]: row for row in overview["alert_rows"]}
    assert overview["auto_order_status"] == "blocked"
    assert overview["auto_order_submit_plan_status"] == "BLOCKED"
    assert overview["auto_order_submit_plan_reason"] == "no_single_safe_submit_candidate"
    assert overview["auto_order_frontier_candidate_count"] == 1
    assert overview["auto_order_rejected_candidate_count"] == 1
    assert overview["auto_order_offline_recovery_required_count"] == 1
    assert "auto_submit_plan=BLOCKED" in overview["summary_text"]
    assert "offline_recovery=1" in overview["summary_text"]
    assert categories["AUTO_ORDER"]["status"] == "WARN"
    assert categories["AUTO_ORDER"]["alert_class"] == "auto_order"
    assert "no_single_safe_submit_candidate" in categories["AUTO_ORDER"]["detail"]


def test_auto_order_readiness_health_warns_when_older_than_gateway_budget() -> None:
    health = _build_auto_order_readiness_health(
        {"generated_at": "2026-05-27T04:24:31+00:00", "summary": {"status": "blocked"}},
        weekly_ibkr_gateway_budget_payload={"generated_at": "2026-05-28T23:04:35+00:00"},
        max_age_hours=168,
        now=datetime(2026, 5, 29, 4, 0, tzinfo=timezone.utc),
    )

    assert health["status"] == "warning"
    assert health["reason"] == "older_than_gateway_budget"
    assert health["older_than_gateway_budget"] is True
    assert health["age_hours"] == 47.59
    assert "gateway_budget_generated_at=2026-05-28T23:04:35+00:00" in health["summary_text"]


def test_load_watchlist_expansion_payload_counts_selected_and_reject_reasons(tmp_path: Path) -> None:
    summary_dir = tmp_path / "reports_supervisor"
    expansion_dir = summary_dir / "watchlist_expansion"
    expansion_dir.mkdir(parents=True, exist_ok=True)
    (expansion_dir / "watchlist_expansion_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-28T00:00:00+00:00",
                "account_profile": {"name": "small", "account_equity": 1000.0},
                "policy": {"max_last_close": 100.0},
                "markets": [
                    {"market": "US", "candidate_row_count": 2, "selected_count": 1},
                    {"market": "HK", "candidate_row_count": 1, "selected_count": 0},
                ],
            }
        ),
        encoding="utf-8",
    )
    (expansion_dir / "watchlist_expansion_candidates.csv").write_text(
        "\n".join(
            [
                "symbol,market,selection_status,selection_reason",
                "SPTM,US,SELECTED,PASS",
                "2800.HK,HK,REJECTED,expected_cost_above_max",
            ]
        ),
        encoding="utf-8",
    )

    payload = _load_watchlist_expansion_payload(
        summary_dir,
        {},
        now=datetime(2026, 5, 28, 6, 0, tzinfo=timezone.utc),
    )

    assert payload["status"] == "ready"
    assert payload["selected_count"] == 1
    assert payload["candidate_row_count"] == 3
    assert payload["zero_selected_market_count"] == 1
    assert payload["age_hours"] == 6.0
    assert payload["reason_summary"] == [{"reason": "expected_cost_above_max", "count": 1}]
    assert payload["primary_recommendation_market"] == "HK"
    assert payload["primary_recommendation_reason"] == "expected_cost_above_max"
    assert payload["primary_recommendation_action"] == "calibrate_cost_or_expand_lower_cost_etfs"
    assert payload["market_recommendations"][0]["market"] == "HK"
    assert "primary_recommendation_action=calibrate_cost_or_expand_lower_cost_etfs" in payload["summary_text"]


def test_build_ops_overview_surfaces_auto_order_readiness_freshness_alert() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        auto_order_readiness={"summary": {"status": "ready", "submit_plan": {"status": "READY_SINGLE_CANDIDATE", "ready": True}}},
        auto_order_readiness_health={
            "status": "warning",
            "status_label": "自动下单证据过旧",
            "reason": "older_than_gateway_budget",
            "summary_text": "自动下单证据过旧: older_than_gateway_budget",
            "age_hours": 23.59,
            "max_age_hours": 168,
        },
    )

    categories = {(row["category"], row["name"]): row for row in overview["alert_rows"]}
    assert overview["auto_order_readiness_health_status"] == "warning"
    assert overview["auto_order_readiness_health_reason"] == "older_than_gateway_budget"
    assert "auto_order_health=warning" in overview["summary_text"]
    assert categories[("AUTO_ORDER", "readiness_freshness")]["status"] == "WARN"


def test_build_ops_overview_surfaces_open_market_analysis_alert() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={"pass_count": 1, "warn_count": 0, "fail_count": 0, "checks": []},
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
        open_market_analysis_summary={
            "status": "warning",
            "status_label": "开市门控证据缺失",
            "summary_text": "open_markets=1 auto_missing=1",
            "open_market_count": 1,
            "open_portfolio_count": 1,
            "fresh_open_report_count": 1,
            "auto_missing_open_count": 1,
            "primary_reason": "auto_order_readiness_missing",
        },
    )

    categories = {row["category"]: row for row in overview["alert_rows"]}
    assert overview["open_market_analysis_status"] == "warning"
    assert overview["open_market_auto_missing_count"] == 1
    assert "open_market_analysis=warning" in overview["summary_text"]
    assert categories["OPEN_MARKET"]["status"] == "WARN"
    assert categories["OPEN_MARKET"]["alert_class"] == "open_market_analysis"
    assert "auto_missing=1" in categories["OPEN_MARKET"]["detail"]


def test_build_ops_overview_classifies_preflight_gateway_port_alerts() -> None:
    overview = _build_ops_overview(
        [],
        preflight_summary={
            "pass_count": 2,
            "warn_count": 1,
            "fail_count": 0,
            "checks": [
                {
                    "name": "ibkr_port:127.0.0.1:4002",
                    "status": "WARN",
                    "detail": "127.0.0.1:4002 not_listening",
                }
            ],
        },
        control_payload={"service": {"status": "configured"}, "actions": {}},
        execution_mode_summary={"mismatch_count": 0},
        status_rollout_summary={
            "market_state_missing_count": 0,
            "data_attention_count": 0,
            "data_research_fallback_count": 0,
            "market_rows": [],
        },
        artifact_health_summary={"warning_count": 0, "degraded_count": 0},
        governance_health_summary={"status": "ready"},
    )

    alert = overview["alert_rows"][0]
    assert alert["alert_class"] == "gateway_port"
    assert alert["alert_severity"] == "warn"
    assert overview["alert_class_counts"]["gateway_port"] == 1
    assert overview["alert_severity_counts"]["warn"] == 1


def test_weekly_unified_evidence_loader_prefers_standalone_json(tmp_path) -> None:
    (tmp_path / "weekly_review_summary.json").write_text(
        json.dumps({"unified_evidence_rows": [{"portfolio_id": "summary"}]}),
        encoding="utf-8",
    )
    (tmp_path / "weekly_unified_evidence.json").write_text(
        json.dumps(
            {
                "artifact_type": "weekly_unified_evidence",
                "row_count": 1,
                "rows": [{"portfolio_id": "standalone", "symbol": "AAPL"}],
            }
        ),
        encoding="utf-8",
    )

    rows = _load_weekly_unified_evidence_rows(tmp_path)

    assert rows == [{"portfolio_id": "standalone", "symbol": "AAPL"}]


def test_weekly_unified_evidence_loader_skips_oversized_json(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(generate_dashboard, "DASHBOARD_WEEKLY_JSON_ARTIFACT_MAX_BYTES", 1)
    (tmp_path / "weekly_unified_evidence.json").write_text(
        json.dumps(
            {
                "artifact_type": "weekly_unified_evidence",
                "row_count": 1,
                "rows": [{"portfolio_id": "standalone", "symbol": "AAPL"}],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "weekly_unified_evidence.csv").write_text(
        "portfolio_id,symbol\ncsv,MSFT\n",
        encoding="utf-8",
    )

    rows = _load_weekly_unified_evidence_rows(tmp_path)

    assert rows == [{"portfolio_id": "csv", "symbol": "MSFT"}]


def test_review_artifact_health_uses_metadata_only_for_oversized_json(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(generate_dashboard, "DASHBOARD_WEEKLY_SUMMARY_MAX_BYTES", 1)
    (tmp_path / "weekly_review_summary.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-01T10:00:00+00:00",
                "schema_version": "test",
                "window_start": "2026-04-24T10:00:00+00:00",
                "window_end": "2026-05-01T10:00:00+00:00",
                "portfolio_count": 1,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "weekly_unified_evidence.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-01T10:00:00+00:00",
                "schema_version": "test",
                "artifact_type": "weekly_unified_evidence",
                "row_count": 2,
                "rows": [{"portfolio_id": "US:watchlist"}, {"portfolio_id": "HK:watchlist"}],
            }
        ),
        encoding="utf-8",
    )

    rows, _ = _build_review_artifact_health_rows(tmp_path)
    by_key = {row["artifact_key"]: row for row in rows}

    assert by_key["weekly_review_summary"]["source"] == "file:metadata_only"
    assert by_key["weekly_review_summary"]["missing_fields"] == []
    assert by_key["weekly_unified_evidence"]["source"] == "file:metadata_only"
    assert by_key["weekly_unified_evidence"]["row_count"] == 2
    assert by_key["weekly_unified_evidence"]["missing_fields"] == []


def test_weekly_blocked_vs_allowed_loader_prefers_standalone_json(tmp_path) -> None:
    (tmp_path / "weekly_review_summary.json").write_text(
        json.dumps({"blocked_vs_allowed_expost_review": [{"portfolio_id": "summary"}]}),
        encoding="utf-8",
    )
    (tmp_path / "weekly_blocked_vs_allowed_expost.json").write_text(
        json.dumps(
            {
                "artifact_type": "weekly_blocked_vs_allowed_expost",
                "row_count": 1,
                "rows": [{"portfolio_id": "standalone", "horizon": "20d"}],
            }
        ),
        encoding="utf-8",
    )

    rows = _load_weekly_blocked_vs_allowed_expost_rows(tmp_path)

    assert rows == [{"portfolio_id": "standalone", "horizon": "20d"}]
