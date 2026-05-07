from __future__ import annotations

from src.common.evidence_focus_actions import (
    ACTION_STATUS_ACKNOWLEDGED,
    ACTION_STATUS_APPLIED,
    ACTION_STATUS_SUGGESTED,
    URGENCY_SAMPLE_COLLECTION,
    URGENCY_URGENT,
    apply_action_resolutions,
    build_action_id,
    build_evidence_focus_actions_from_expost,
    build_evidence_focus_actions_from_market_summaries,
    normalize_action_status,
    normalize_evidence_focus_action,
    normalize_urgency,
    summarize_evidence_focus_actions,
)


def test_normalize_evidence_focus_action_fills_defaults() -> None:
    action = normalize_evidence_focus_action(
        {
            "week": "2026W18",
            "market": "us",
            "primary_action": "review_gate_thresholds",
            "basis_label": "Blocked outperformed allowed",
            "action_label": "Review gate thresholds",
        }
    )

    assert action["action_id"].startswith("2026W18-US-market-review_gate_thresholds")
    assert action["market"] == "US"
    assert action["status"] == ACTION_STATUS_SUGGESTED
    assert action["urgency"] == URGENCY_URGENT
    assert action["read_only"] is True
    assert action["linked_evidence_artifact"] == "weekly_blocked_vs_allowed_expost.json"
    assert action["primary_action"] == "review_gate_thresholds"
    assert action["action"] == "Review gate thresholds"


def test_normalizers_reject_unknown_status_and_urgency() -> None:
    assert normalize_action_status("unknown") == ACTION_STATUS_SUGGESTED
    assert normalize_urgency("surprising") == "normal"


def test_build_action_id_is_deterministic_and_shortens_long_values() -> None:
    kwargs = {
        "week": "2026W18",
        "market": "HK",
        "portfolio_id": "portfolio-with-a-very-long-name-" * 4,
        "action_type": "review_gate_thresholds",
        "basis": "blocked-outperformed-allowed-" * 4,
    }

    first = build_action_id(**kwargs)
    second = build_action_id(**kwargs)

    assert first == second
    assert len(first) <= 96


def test_expost_blocked_outperformed_allowed_creates_urgent_gate_review() -> None:
    actions = build_evidence_focus_actions_from_expost(
        [
            {
                "market": "US",
                "portfolio_id": "US:paper",
                "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
                "horizon": "20d",
                "blocked_count": 7,
                "allowed_count": 8,
            }
        ],
        week="2026W18",
    )

    assert len(actions) == 1
    assert actions[0]["primary_action"] == "review_gate_thresholds"
    assert actions[0]["urgency"] == URGENCY_URGENT
    assert actions[0]["linked_evidence_key"] == "US|US:paper||20d"


def test_insufficient_sample_creates_sample_collection_action() -> None:
    actions = build_evidence_focus_actions_from_expost(
        [
            {
                "market": "HK",
                "portfolio_id": "HK:paper",
                "review_label": "INSUFFICIENT_SAMPLE",
                "horizon": "5d",
                "blocked_count": 1,
                "allowed_count": 2,
            }
        ],
        week="2026W18",
    )

    assert actions[0]["primary_action"] == "collect_more_outcome_samples"
    assert actions[0]["urgency"] == URGENCY_SAMPLE_COLLECTION


def test_summarize_evidence_focus_actions_prioritizes_urgent() -> None:
    actions = build_evidence_focus_actions_from_market_summaries(
        {
            "HK": {
                "market": "HK",
                "primary_action": "collect_more_outcome_samples",
                "action_label": "Collect more outcome samples",
                "basis_label": "Insufficient sample",
            },
            "US": {
                "market": "US",
                "primary_action": "review_gate_thresholds",
                "action_label": "Review gate thresholds",
                "basis_label": "Blocked outperformed allowed",
            },
        },
        week="2026W18",
    )
    summary = summarize_evidence_focus_actions(actions)

    assert [row["market"] for row in actions] == ["US", "HK"]
    assert summary["status"] == "warn"
    assert summary["primary_market"] == "US"
    assert summary["primary_action"] == "review_gate_thresholds"
    assert summary["urgent_action_count"] == 1
    assert summary["sample_collection_count"] == 1
    assert summary["actions"][0]["action_id"].startswith("2026W18-US-market-review_gate_thresholds")


def test_apply_action_resolutions_marks_dashboard_control_status() -> None:
    actions = [
        normalize_evidence_focus_action(
            {
                "week": "2026W18",
                "market": "US",
                "primary_action": "review_gate_thresholds",
                "basis": "Blocked outperformed allowed",
            }
        )
    ]
    action_id = actions[0]["action_id"]

    resolved = apply_action_resolutions(
        actions,
        [
            {
                "ts": "2026-05-08T09:00:00+10:00",
                "linked_evidence_action_id": action_id,
                "resolution_status": ACTION_STATUS_ACKNOWLEDGED,
                "resolution_note": "operator reviewed",
            },
            {
                "ts": "2026-05-08T10:00:00+10:00",
                "linked_evidence_action_id": action_id,
                "resolution_status": ACTION_STATUS_APPLIED,
                "resolution_note": "paper applied",
            },
        ],
    )

    assert resolved[0]["status"] == ACTION_STATUS_APPLIED
    assert resolved[0]["resolved_at"] == "2026-05-08T10:00:00+10:00"
    assert resolved[0]["resolution_source"] == "dashboard_control"
    assert resolved[0]["resolution_note"] == "paper applied"
    summary = summarize_evidence_focus_actions(resolved)
    assert summary["status"] == "ok"
    assert summary["urgent_action_count"] == 1
    assert summary["open_urgent_action_count"] == 0


def test_apply_action_resolutions_ignores_unknown_status() -> None:
    action = normalize_evidence_focus_action(
        {
            "week": "2026W18",
            "market": "HK",
            "primary_action": "review_gate_thresholds",
            "basis": "Blocked outperformed allowed",
        }
    )

    resolved = apply_action_resolutions(
        [action],
        [{"linked_evidence_action_id": action["action_id"], "resolution_status": "UNKNOWN"}],
    )

    assert resolved[0]["status"] == ACTION_STATUS_SUGGESTED
