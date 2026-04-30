from __future__ import annotations

from src.tools.dashboard_blocks import build_dashboard_v2_blocks


def test_dashboard_v2_blocks_include_control_market_and_evidence_layers():
    payload = {
        "ops_overview": {
            "summary_text": "ops ready",
            "preflight_fail_count": 0,
            "degraded_health_count": 1,
            "alert_rows": [{"status": "warn", "name": "stale"}],
        },
        "artifact_health_overview": {"fail_count": 0, "warn_count": 1},
        "governance_health_summary": {"blocked_count": 0, "warn_count": 1},
        "dashboard_control": {
            "service": {"status": "running"},
            "actions": {
                "last_action": "refresh_dashboard",
                "action_history": [
                    {"action": "run_once", "status": "completed"},
                    {"action": "refresh_dashboard", "status": "completed"},
                    {"action": "run_preflight", "status": "failed", "error_class": "transient_io"},
                ],
            },
        },
        "market_views": {
            "US": {"market": "US", "portfolio_count": 1, "open_count": 1},
            "HK": {"market": "HK", "portfolio_count": 1, "stale_report_count": 1},
            "CN": {"market": "CN", "portfolio_count": 0},
        },
        "unified_evidence_overview": {
            "row_count": 3,
            "allowed_row_count": 1,
            "blocked_row_count": 1,
            "candidate_only_row_count": 1,
            "outcome_labeled_row_count": 2,
            "partial_join_row_count": 1,
        },
        "blocked_vs_allowed_expost_review": [
            {"review_label": "BLOCKING_HELPED", "block_reason": "EDGE_GATE"},
        ],
        "candidate_model_review": [
            {"review_label": "SIGNAL_RANKING_WORKING", "portfolio_id": "US:watchlist"},
            {"review_label": "EXPECTED_EDGE_OVERSTATED", "portfolio_id": "HK:watchlist"},
        ],
        "weekly_attribution_waterfall": [{"component": "selection"}],
    }

    blocks = build_dashboard_v2_blocks(payload)
    by_id = {block["id"]: block for block in blocks}

    assert list(by_id) == [
        "ops_health",
        "dashboard_control_actions",
        "market_views",
        "evidence_quality",
    ]
    assert by_id["ops_health"]["metrics"]["degraded_health_count"] == 1
    assert by_id["dashboard_control_actions"]["metrics"]["history_count"] == 3
    assert by_id["dashboard_control_actions"]["metrics"]["transient_io_error_count"] == 1
    assert by_id["dashboard_control_actions"]["metrics"]["retryable_error_count"] == 1
    assert by_id["market_views"]["metrics"]["market_count"] == 3
    assert by_id["evidence_quality"]["metrics"]["evidence_row_count"] == 3
    assert by_id["evidence_quality"]["metrics"]["candidate_only_row_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["outcome_labeled_row_count"] == 2
    assert by_id["evidence_quality"]["metrics"]["blocked_review_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["blocking_helped_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["primary_action"] == "review_signal_expected_edge"
    assert by_id["evidence_quality"]["metrics"]["candidate_model_review_count"] == 2
    assert by_id["evidence_quality"]["metrics"]["candidate_model_warning_count"] == 1
    assert by_id["evidence_quality"]["status"] == "warn"
    assert by_id["evidence_quality"]["rows"]["blocked_vs_allowed_label_summary"][0] == {
        "review_label": "BLOCKING_HELPED",
        "count": 1,
        "action": "keep_gate_monitor_post_cost",
    }


def test_evidence_quality_block_marks_gate_review_when_blocked_outperforms():
    payload = {
        "unified_evidence_overview": {"row_count": 10},
        "blocked_vs_allowed_expost_review": [
            {
                "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
                "block_reason": "EDGE_GATE",
                "allowed_count": 6,
                "blocked_count": 7,
            },
        ],
        "candidate_model_review": [],
    }

    block = build_dashboard_v2_blocks(payload)[-1]

    assert block["status"] == "warn"
    assert block["metrics"]["too_restrictive_count"] == 1
    assert block["metrics"]["sample_ready_review_count"] == 1
    assert block["metrics"]["primary_action"] == "review_gate_thresholds"


def test_evidence_quality_block_keeps_insufficient_samples_non_warning():
    payload = {
        "unified_evidence_overview": {"row_count": 4},
        "blocked_vs_allowed_expost_review": [
            {
                "review_label": "INSUFFICIENT_OUTCOME_SAMPLE",
                "block_reason": "MARKET_RULE_GATE",
                "allowed_count": 1,
                "blocked_count": 2,
            },
        ],
        "candidate_model_review": [],
    }

    block = build_dashboard_v2_blocks(payload)[-1]

    assert block["status"] == "ok"
    assert block["metrics"]["insufficient_sample_count"] == 1
    assert block["metrics"]["sample_ready_review_count"] == 0
    assert block["metrics"]["primary_action"] == "collect_more_outcome_samples"
