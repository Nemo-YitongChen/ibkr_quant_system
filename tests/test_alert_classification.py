from __future__ import annotations

from src.common.alert_classification import (
    alert_severity,
    classify_alert_row,
    classify_error_text,
    summarize_error_classes,
)


def test_classify_error_text_covers_control_failure_buckets() -> None:
    assert classify_error_text("unsupported_field") == "validation"
    assert classify_error_text("permission denied") == "permission"
    assert classify_error_text("connection refused") == "transient_io"
    assert classify_error_text("weekly_review_failed") == "task_failed"
    assert classify_error_text("handler_exception traceback") == "exception"


def test_classify_alert_row_maps_ops_categories() -> None:
    assert classify_alert_row({"category": "PREFLIGHT", "name": "ibkr_port:127.0.0.1:4002"}) == "gateway_port"
    assert classify_alert_row({"category": "REPORT", "detail": "stale"}) == "report_freshness"
    assert classify_alert_row({"category": "DATA", "detail": "history fallback"}) == "market_data"
    assert alert_severity({"status": "DEGRADED"}) == "fail"
    assert alert_severity({"status": "WARN"}) == "warn"


def test_summarize_error_classes_counts_retryable_and_primary_class() -> None:
    summary = summarize_error_classes(
        [
            {"status": "failed", "error_class": "transient_io"},
            {"status": "failed", "error_class": "transient_io"},
            {"status": "failed", "error_class": "validation"},
            {"status": "completed", "error_class": "none"},
        ]
    )

    assert summary["class_counts"]["transient_io"] == 2
    assert summary["retryable_count"] == 2
    assert summary["severity_counts"]["fail"] == 3
    assert summary["primary_error_class"] == "transient_io"
