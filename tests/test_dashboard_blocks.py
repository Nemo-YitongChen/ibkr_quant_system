from __future__ import annotations

from src.tools.dashboard_blocks import build_dashboard_v2_blocks


def test_dashboard_v2_blocks_include_control_market_and_evidence_layers():
    payload = {
        "ops_overview": {
            "summary_text": "ops ready",
            "preflight_fail_count": 0,
            "degraded_health_count": 1,
            "evidence_focus_action_count": 3,
            "evidence_focus_urgent_count": 2,
            "evidence_focus_primary_market": "US",
            "evidence_focus_primary_action": "Review gate thresholds",
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
        "market_evidence_action_summary": {
            "US": {
                "primary_action": "review_gate_thresholds",
                "action_label": "Review gate thresholds",
                "basis_label": "Blocked outperformed allowed",
                "evidence_row_count": 8,
            },
            "HK": {
                "primary_action": "collect_more_outcome_samples",
                "action_label": "Collect more outcome samples",
                "basis_label": "Insufficient blocked-vs-allowed sample",
                "evidence_row_count": 3,
            },
            "CN": {
                "primary_action": "build_weekly_unified_evidence",
                "action_label": "Build unified evidence",
                "basis_label": "No unified evidence",
                "evidence_row_count": 0,
            },
        },
        "evidence_focus_actions": [
            {
                "market": "US",
                "primary_action": "review_gate_thresholds",
                "action": "Review gate thresholds",
                "basis": "Blocked outperformed allowed",
                "priority_order": 10,
            },
            {
                "market": "CN",
                "primary_action": "build_weekly_unified_evidence",
                "action": "Build unified evidence",
                "basis": "No unified evidence",
                "priority_order": 30,
            },
            {
                "market": "HK",
                "primary_action": "collect_more_outcome_samples",
                "action": "Collect more outcome samples",
                "basis": "Insufficient blocked-vs-allowed sample",
                "priority_order": 60,
            },
        ],
        "evidence_focus_summary": {
            "status": "warn",
            "summary_text": "US: Review gate thresholds; basis=Blocked outperformed allowed; urgent=2/3.",
            "primary_market": "US",
            "primary_action": "review_gate_thresholds",
            "primary_action_label": "Review gate thresholds",
            "primary_basis": "Blocked outperformed allowed",
            "primary_detail": "Review edge floor and buffers.",
            "focus_action_count": 3,
            "urgent_action_count": 2,
            "read_only": True,
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
        "evidence_focus_actions",
        "evidence_quality",
    ]
    assert by_id["ops_health"]["metrics"]["degraded_health_count"] == 1
    assert by_id["ops_health"]["metrics"]["evidence_focus_action_count"] == 3
    assert by_id["ops_health"]["metrics"]["evidence_focus_urgent_count"] == 2
    assert by_id["ops_health"]["metrics"]["evidence_focus_primary_market"] == "US"
    assert by_id["ops_health"]["metrics"]["evidence_focus_primary_action"] == "Review gate thresholds"
    assert by_id["dashboard_control_actions"]["metrics"]["history_count"] == 3
    assert by_id["dashboard_control_actions"]["metrics"]["transient_io_error_count"] == 1
    assert by_id["dashboard_control_actions"]["metrics"]["retryable_error_count"] == 1
    assert by_id["market_views"]["metrics"]["market_count"] == 3
    assert by_id["market_views"]["metrics"]["evidence_action_market_count"] == 3
    assert by_id["market_views"]["metrics"]["evidence_row_market_count"] == 2
    assert by_id["market_views"]["metrics"]["evidence_attention_count"] == 2
    assert by_id["market_views"]["metrics"]["gate_review_market_count"] == 1
    assert by_id["market_views"]["metrics"]["missing_evidence_market_count"] == 1
    assert by_id["market_views"]["metrics"]["sample_collection_market_count"] == 1
    assert by_id["market_views"]["rows"][0]["evidence_primary_action"] == "build_weekly_unified_evidence"
    assert by_id["market_views"]["status"] == "warn"
    assert "evidence_attention=2" in by_id["market_views"]["summary"]
    assert by_id["evidence_focus_actions"]["status"] == "warn"
    assert by_id["evidence_focus_actions"]["metrics"]["focus_action_count"] == 3
    assert by_id["evidence_focus_actions"]["metrics"]["urgent_action_count"] == 2
    assert by_id["evidence_focus_actions"]["metrics"]["gate_review_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["missing_evidence_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["sample_collection_count"] == 1
    assert by_id["evidence_focus_actions"]["metrics"]["primary_market"] == "US"
    assert by_id["evidence_focus_actions"]["metrics"]["primary_action"] == "review_gate_thresholds"
    assert by_id["evidence_focus_actions"]["metrics"]["read_only"] is True
    assert by_id["evidence_focus_actions"]["rows"]["summary"]["primary_market"] == "US"
    assert by_id["evidence_focus_actions"]["rows"]["actions"][0]["market"] == "US"
    assert "US: Review gate thresholds" in by_id["evidence_focus_actions"]["summary"]
    assert by_id["evidence_quality"]["metrics"]["evidence_row_count"] == 3
    assert by_id["evidence_quality"]["metrics"]["candidate_only_row_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["outcome_labeled_row_count"] == 2
    assert by_id["evidence_quality"]["metrics"]["blocked_review_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["blocking_helped_count"] == 1
    assert by_id["evidence_quality"]["metrics"]["primary_action"] == "review_signal_expected_edge"
    assert by_id["evidence_quality"]["metrics"]["action_label"] == "Review signal expected edge"
    assert "Candidate model warning" in by_id["evidence_quality"]["metrics"]["action_note"]
    assert "action=Review signal expected edge" in by_id["evidence_quality"]["summary"]
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
    assert block["metrics"]["action_label"] == "Review gate thresholds"


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
    assert block["metrics"]["action_label"] == "Collect more outcome samples"
    assert "sample-starved" in block["metrics"]["action_note"]


def test_market_views_block_keeps_sample_collection_non_warning():
    payload = {
        "market_views": {
            "US": {
                "market": "US",
                "portfolio_count": 1,
                "open_count": 1,
                "evidence_action_label": "Collect more outcome samples",
                "evidence_basis_label": "Insufficient blocked-vs-allowed sample",
                "evidence_row_count": 4,
            },
        },
    }

    block = build_dashboard_v2_blocks(payload)[2]

    assert block["status"] == "ok"
    assert block["metrics"]["attention_count"] == 0
    assert block["metrics"]["evidence_attention_count"] == 0
    assert block["metrics"]["sample_collection_market_count"] == 1
    assert block["rows"][0]["evidence_primary_action"] == "collect_more_outcome_samples"


def test_market_views_block_handles_malformed_evidence_summary():
    payload = {
        "market_views": {
            "US": {
                "market": "US",
                "portfolio_count": 1,
                "evidence_action_label": "Review gate thresholds",
                "evidence_row_count": 2,
            },
        },
        "market_evidence_action_summary": {"US": "legacy bad summary"},
    }

    block = build_dashboard_v2_blocks(payload)[2]

    assert block["status"] == "warn"
    assert block["metrics"]["gate_review_market_count"] == 1
    assert block["rows"][0]["evidence_primary_action"] == "review_gate_thresholds"


def test_evidence_focus_actions_block_keeps_sample_collection_non_warning():
    payload = {
        "evidence_focus_actions": [
            {
                "market": "HK",
                "primary_action": "collect_more_outcome_samples",
                "priority_order": 60,
            },
            "legacy malformed row",
        ]
    }

    block = build_dashboard_v2_blocks(payload)[3]

    assert block["id"] == "evidence_focus_actions"
    assert block["status"] == "ok"
    assert block["metrics"]["focus_action_count"] == 1
    assert block["metrics"]["urgent_action_count"] == 0
    assert block["metrics"]["sample_collection_count"] == 1
    assert block["metrics"]["primary_market"] == "HK"
    assert block["rows"]["summary"]["primary_action"] == "collect_more_outcome_samples"
    assert block["rows"]["actions"][0]["market"] == "HK"
