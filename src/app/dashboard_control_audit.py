from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict


_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(token|secret|password|passwd|api[_-]?key|authorization)\s*[:=]\s*([^\s,;]+)"
)
_ACCOUNT_RE = re.compile(r"\b(DUQ|U|DU)[A-Z0-9]{4,}\b", re.IGNORECASE)
_USER_PATH_RE = re.compile(r"/Users/([^/\s]+)")
_VOLUME_PATH_RE = re.compile(r"/Volumes/([^\n\r\t]+)")


def classify_dashboard_control_error(error: Any, *, status: str = "") -> str:
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


def redact_dashboard_control_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = _SECRET_ASSIGNMENT_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = _ACCOUNT_RE.sub("<account>", text)
    text = _USER_PATH_RE.sub("/Users/<user>", text)
    text = _VOLUME_PATH_RE.sub("/Volumes/<path>", text)
    return text


def sanitize_dashboard_control_action(row: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = dict(row or {})
    sanitized = {
        "ts": redact_dashboard_control_text(raw.get("ts")),
        "action": redact_dashboard_control_text(raw.get("action")),
        "status": redact_dashboard_control_text(raw.get("status")),
        "portfolio_id": redact_dashboard_control_text(raw.get("portfolio_id")),
        "detail": redact_dashboard_control_text(raw.get("detail")),
        "error": redact_dashboard_control_text(raw.get("error")),
    }
    sanitized["error_class"] = classify_dashboard_control_error(
        sanitized.get("error"),
        status=str(sanitized.get("status") or ""),
    )
    return sanitized


def append_dashboard_control_action_audit(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sanitize_dashboard_control_action(row), ensure_ascii=False, sort_keys=True) + "\n")
