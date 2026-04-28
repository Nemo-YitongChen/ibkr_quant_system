from __future__ import annotations

from typing import Any, Dict, Iterable


RETRYABLE_ERROR_CLASSES = {"transient_io", "task_failed", "unknown_failure"}


def classify_error_text(error: Any, *, status: str = "") -> str:
    text = str(error or "").strip().lower()
    status_text = str(status or "").strip().lower()
    if not text and status_text not in {"failed", "error"}:
        return "none"
    if any(token in text for token in ("missing_", "unsupported_", "not_found", "invalid")):
        return "validation"
    if any(token in text for token in ("permission", "denied", "auth", "forbidden")):
        return "permission"
    if any(token in text for token in ("timeout", "connection", "unreachable", "refused", "broken pipe")):
        return "transient_io"
    if any(token in text for token in ("handler_exception", "traceback", "runtimeerror", "exception")):
        return "exception"
    if text in {"weekly_review_failed", "dashboard_refresh_failed"} or text.endswith("_failed"):
        return "task_failed"
    if status_text in {"failed", "error"}:
        return "unknown_failure"
    return "none"


def error_severity(error_class: str, *, status: str = "") -> str:
    status_text = str(status or "").strip().lower()
    class_text = str(error_class or "none").strip().lower() or "none"
    if status_text in {"fail", "failed", "error"}:
        return "fail"
    if class_text in {"permission", "exception", "unknown_failure"}:
        return "fail"
    if class_text in {"validation", "transient_io", "task_failed"}:
        return "warn"
    return "ok"


def is_retryable_error(error_class: str) -> bool:
    return str(error_class or "").strip().lower() in RETRYABLE_ERROR_CLASSES


def summarize_error_classes(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    severity_counts = {"fail": 0, "warn": 0, "ok": 0}
    retryable_count = 0
    total = 0
    for raw in list(rows or []):
        row = dict(raw or {})
        error_class = str(row.get("error_class") or classify_error_text(row.get("error"), status=str(row.get("status") or "")))
        severity = str(row.get("error_severity") or error_severity(error_class, status=str(row.get("status") or "")))
        counts[error_class] = counts.get(error_class, 0) + 1
        severity_counts[severity if severity in severity_counts else "warn"] += 1
        retryable_count += int(is_retryable_error(error_class))
        total += 1
    primary_class = "none"
    actionable_counts = {key: value for key, value in counts.items() if key != "none" and value > 0}
    if actionable_counts:
        primary_class = sorted(actionable_counts.items(), key=lambda item: (-int(item[1]), str(item[0])))[0][0]
    return {
        "row_count": total,
        "class_counts": counts,
        "severity_counts": severity_counts,
        "retryable_count": retryable_count,
        "primary_error_class": primary_class,
    }


def classify_alert_row(row: Dict[str, Any] | None) -> str:
    raw = dict(row or {})
    category = str(raw.get("category") or "").strip().upper()
    name = str(raw.get("name") or "").strip().lower()
    detail = str(raw.get("detail") or "").strip()
    status = str(raw.get("status") or "").strip()
    if category == "PREFLIGHT" and name.startswith("ibkr_port:"):
        return "gateway_port"
    if category == "REPORT":
        return "report_freshness"
    if category == "HEALTH":
        return "ibkr_health"
    if category == "DATA":
        return "market_data"
    if category == "ARTIFACT":
        return "artifact_contract"
    if category == "GOVERNANCE":
        return "governance"
    if category == "MARKET_STATE":
        return "market_state"
    derived = classify_error_text(detail, status=status)
    return derived if derived != "none" else (category.lower() or "unknown")


def alert_severity(row: Dict[str, Any] | None) -> str:
    raw = dict(row or {})
    status = str(raw.get("status") or "").strip().lower()
    if status in {"fail", "failed", "error", "degraded"}:
        return "fail"
    if status in {"warn", "warning", "limited"}:
        return "warn"
    return "ok"
