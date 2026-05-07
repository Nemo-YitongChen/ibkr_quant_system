from __future__ import annotations

import json

from src.app.dashboard_control_audit import (
    append_dashboard_control_action_audit,
    attach_evidence_action_link,
    classify_dashboard_control_error,
    extract_evidence_action_link,
    normalize_resolution_status,
    redact_dashboard_control_text,
    sanitize_dashboard_control_action,
    summarize_evidence_action_audit_links,
)


def test_dashboard_control_action_audit_redacts_sensitive_text(tmp_path):
    row = sanitize_dashboard_control_action(
        {
            "ts": "2026-04-28T10:00:00+10:00",
            "action": "run_once",
            "status": "failed",
            "portfolio_id": "US:core",
            "detail": "path=/Users/nemo/project token=abc123 account=DUQ152001",
            "error": "RuntimeError: /Volumes/Data and Info/private secret=xyz",
        }
    )

    assert "/Users/nemo" not in row["detail"]
    assert "abc123" not in row["detail"]
    assert "DUQ152001" not in row["detail"]
    assert "/Volumes/Data and Info" not in row["error"]
    assert "xyz" not in row["error"]
    assert row["error_class"] == "exception"
    assert row["error_severity"] == "fail"

    audit_path = tmp_path / "dashboard_control_action_audit.jsonl"
    append_dashboard_control_action_audit(audit_path, row)

    stored = json.loads(audit_path.read_text(encoding="utf-8").strip())
    assert stored["action"] == "run_once"
    assert stored["error_class"] == "exception"


def test_dashboard_control_error_classification_buckets():
    assert classify_dashboard_control_error("") == "none"
    assert classify_dashboard_control_error("unsupported_field") == "validation"
    assert classify_dashboard_control_error("connection refused") == "transient_io"
    assert classify_dashboard_control_error("permission denied") == "permission"
    assert classify_dashboard_control_error("weekly_review_failed") == "task_failed"
    assert classify_dashboard_control_error("handler_exception: failed") == "exception"
    assert redact_dashboard_control_text("api_key=abc password=def") == "api_key=<redacted> password=<redacted>"


def test_dashboard_control_audit_links_evidence_action_resolution():
    payload = {
        "evidence_action_id": "2026W18-US-market-review_gate_thresholds",
        "market": "US",
        "portfolio_id": "US:core",
        "resolution_status": "applied",
        "resolution_note": "approved with token=abc account=DUQ152001",
    }

    link = extract_evidence_action_link(payload)
    row = sanitize_dashboard_control_action(
        attach_evidence_action_link(
            {
                "ts": "2026-05-08T10:00:00+10:00",
                "action": "review_market_profile_patch",
                "status": "completed",
            },
            payload,
        )
    )

    assert normalize_resolution_status("unexpected") == "ACKNOWLEDGED"
    assert link["resolution_status"] == "APPLIED"
    assert row["linked_evidence_action_id"] == "2026W18-US-market-review_gate_thresholds"
    assert row["linked_market"] == "US"
    assert row["linked_portfolio_id"] == "US:core"
    assert row["resolution_status"] == "APPLIED"
    assert "abc" not in row["resolution_note"]
    assert "DUQ152001" not in row["resolution_note"]


def test_dashboard_control_audit_summarizes_evidence_action_links():
    summary = summarize_evidence_action_audit_links(
        [
            {"action": "run_once", "status": "completed"},
            {
                "linked_evidence_action_id": "a1",
                "linked_market": "US",
                "linked_portfolio_id": "US:core",
                "resolution_status": "ACKNOWLEDGED",
            },
            {
                "linked_evidence_action_id": "a2",
                "linked_market": "HK",
                "linked_portfolio_id": "HK:core",
                "resolution_status": "REJECTED",
            },
        ]
    )

    assert summary["linked_action_history_count"] == 2
    assert summary["last_linked_evidence_action_id"] == "a2"
    assert summary["last_resolution_status"] == "REJECTED"
    assert summary["last_linked_market"] == "HK"
    assert summary["resolution_status_counts"] == {"ACKNOWLEDGED": 1, "REJECTED": 1}
