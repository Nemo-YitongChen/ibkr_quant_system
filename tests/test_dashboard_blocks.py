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
        "unified_evidence_overview": {"row_count": 2, "allowed_row_count": 1, "blocked_row_count": 1},
        "blocked_vs_allowed_expost_review": [
            {"review_label": "BLOCKING_HELPED", "block_reason": "EDGE_GATE"},
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
    assert by_id["evidence_quality"]["metrics"]["evidence_row_count"] == 2
