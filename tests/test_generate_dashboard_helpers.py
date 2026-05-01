from __future__ import annotations

import json

from src.tools.generate_dashboard import (
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
    _dashboard_v2_block_metrics_text,
    _load_weekly_blocked_vs_allowed_expost_rows,
    _load_weekly_unified_evidence_rows,
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
            "control_service_status": "configured",
        }
    )
    assert any(row[0] == "市场状态缺口" and "2" in row[1] for row in rows)
    assert any(row[0] == "市场数据健康" and "研究Fallback" in row[1] for row in rows)
    assert any(row[0] == "量化客户端" and "量化客户端空闲" in row[1] for row in rows)


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
