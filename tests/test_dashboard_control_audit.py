from __future__ import annotations

import json

from src.app.dashboard_control_audit import (
    append_dashboard_control_action_audit,
    classify_dashboard_control_error,
    redact_dashboard_control_text,
    sanitize_dashboard_control_action,
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
    assert redact_dashboard_control_text("api_key=abc password=def") == "api_key=<redacted> password=<redacted>"
