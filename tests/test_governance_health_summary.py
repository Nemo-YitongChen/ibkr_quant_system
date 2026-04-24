from __future__ import annotations

from src.common.governance_health import build_governance_health_summary


def test_governance_health_summary_warns_on_pending_actions() -> None:
    cards = [
        {
            "market": "US",
            "watchlist": "watchlist",
            "dashboard_control": {
                "portfolio": {
                    "weekly_feedback_patch_governance_action_label": "优先处理已批准未应用 patch",
                    "weekly_feedback_market_profile_review_status": "APPROVED",
                    "weekly_feedback_market_profile_review_evidence_summary": "",
                    "weekly_feedback_market_profile_ready_for_manual_apply": True,
                }
            },
            "patch_review_history_rows": [
                {
                    "patch_kind": "market_profile",
                    "review_status": "APPROVED",
                    "ts": "2026-04-20T10:00:00+00:00",
                }
            ],
        }
    ]
    overview_rows = [
        {
            "rejection_rate": 0.0,
            "review_cycle_count": 1,
            "approved_not_applied_count": 1,
        }
    ]

    summary = build_governance_health_summary(cards, overview_rows)

    assert summary["status"] == "warning"
    assert summary["pending_action_count"] == 1
    assert summary["approved_not_applied_count"] == 1
    assert summary["ready_for_manual_apply_count"] == 1


def test_governance_health_summary_degrades_on_applied_without_evidence() -> None:
    cards = [
        {
            "market": "HK",
            "watchlist": "hk_top",
            "dashboard_control": {
                "portfolio": {
                    "weekly_feedback_patch_governance_action_label": "",
                    "weekly_feedback_calibration_patch_review_status": "APPLIED",
                    "weekly_feedback_calibration_patch_review_evidence_summary": "",
                    "weekly_feedback_calibration_patch_ready_for_manual_apply": False,
                }
            },
            "patch_review_history_rows": [],
        }
    ]

    summary = build_governance_health_summary(cards, [])

    assert summary["status"] == "degraded"
    assert summary["evidence_mismatch_count"] == 1


def test_governance_health_summary_degrades_live_applied_without_four_piece_governance() -> None:
    cards = [
        {
            "market": "US",
            "watchlist": "live_core",
            "account_mode": "live",
            "dashboard_control": {
                "portfolio": {
                    "weekly_feedback_patch_governance_action_label": "",
                    "weekly_feedback_market_profile_review_status": "APPLIED",
                    "weekly_feedback_market_profile_reviewed_ts": "2026-04-24T00:00:00+00:00",
                    "weekly_feedback_market_profile_review_evidence_summary": "post-cost evidence accepted",
                    "weekly_feedback_market_profile_review_evidence": {
                        "summary": "post-cost evidence accepted",
                        "approved_by": "operator",
                    },
                }
            },
            "patch_review_history_rows": [],
        }
    ]

    summary = build_governance_health_summary(cards, [])

    assert summary["status"] == "degraded"
    assert summary["live_change_governance_gap_count"] == 1
    assert summary["live_change_missing_component_count"] == 2
    assert summary["applied_live_change_gap_count"] == 1
    assert "missing rollback,effect_tracking" in summary["focus_items"][0]


def test_governance_health_summary_accepts_live_applied_with_four_piece_governance() -> None:
    cards = [
        {
            "market": "US",
            "watchlist": "live_core",
            "account_mode": "live",
            "dashboard_control": {
                "portfolio": {
                    "weekly_feedback_patch_governance_action_label": "",
                    "weekly_feedback_market_profile_review_status": "APPLIED",
                    "weekly_feedback_market_profile_reviewed_ts": "2026-04-24T00:00:00+00:00",
                    "weekly_feedback_market_profile_review_evidence_summary": "post-cost evidence accepted",
                    "weekly_feedback_market_profile_review_evidence": {
                        "summary": "post-cost evidence accepted",
                        "approved_by": "operator",
                        "rollback_plan": "restore previous overlay config and rerun supervisor cycle",
                        "effect_tracking_window": "next 3 weekly reviews",
                    },
                }
            },
            "patch_review_history_rows": [],
        }
    ]

    summary = build_governance_health_summary(cards, [])

    assert summary["status"] == "ready"
    assert summary["live_change_governance_gap_count"] == 0
