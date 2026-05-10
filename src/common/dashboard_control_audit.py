from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .alert_classification import classify_error_text, error_severity

RESOLUTION_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
RESOLUTION_STATUS_APPLIED = "APPLIED"
RESOLUTION_STATUS_REJECTED = "REJECTED"
RESOLUTION_STATUS_SUPERSEDED = "SUPERSEDED"

KNOWN_RESOLUTION_STATUSES = {
    RESOLUTION_STATUS_ACKNOWLEDGED,
    RESOLUTION_STATUS_APPLIED,
    RESOLUTION_STATUS_REJECTED,
    RESOLUTION_STATUS_SUPERSEDED,
}

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(token|secret|password|passwd|api[_-]?key|authorization|account(?:_id)?)\s*[:=]\s*([^\s,;]+)"
)
_ACCOUNT_RE = re.compile(r"\b(DUQ|U|DU)[A-Z0-9]{4,}\b", re.IGNORECASE)
_USER_PATH_RE = re.compile(r"/Users/([^/\s]+)")
_VOLUME_PATH_RE = re.compile(r"/Volumes/([^\n\r\t]+)")


def classify_dashboard_control_error(error: Any, *, status: str = "") -> str:
    return classify_error_text(error, status=status)


def redact_dashboard_control_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = _SECRET_ASSIGNMENT_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = _ACCOUNT_RE.sub("<account>", text)
    text = _USER_PATH_RE.sub("/Users/<user>", text)
    text = _VOLUME_PATH_RE.sub("/Volumes/<path>", text)
    return text


def _clean_text(value: Any, *, max_len: int = 240) -> str:
    text = redact_dashboard_control_text(value).strip()
    if max_len > 0 and len(text) > max_len:
        return text[: max_len - 3].rstrip() + "..."
    return text


def normalize_resolution_status(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return raw if raw in KNOWN_RESOLUTION_STATUSES else RESOLUTION_STATUS_ACKNOWLEDGED


def extract_evidence_action_link(payload: Dict[str, Any] | None) -> Dict[str, str]:
    raw = dict(payload or {})
    action_id = _clean_text(raw.get("evidence_action_id") or raw.get("linked_evidence_action_id"), max_len=160)
    market = _clean_text(raw.get("market") or raw.get("linked_market"), max_len=32)
    portfolio_id = _clean_text(raw.get("portfolio_id") or raw.get("linked_portfolio_id"), max_len=160)
    raw_status = raw.get("resolution_status")
    status = normalize_resolution_status(raw_status)
    note = _clean_text(raw.get("resolution_note"), max_len=240)
    if not any((action_id, market, portfolio_id, status, note)):
        return {}
    if action_id and not status:
        status = RESOLUTION_STATUS_ACKNOWLEDGED
    return {
        "linked_evidence_action_id": action_id,
        "linked_market": market,
        "linked_portfolio_id": portfolio_id,
        "resolution_status": status,
        "resolution_note": note,
    }


def extract_strategy_parameter_suggestion_link(payload: Dict[str, Any] | None) -> Dict[str, str]:
    raw = dict(payload or {})
    suggestion_id = _clean_text(
        raw.get("strategy_parameter_suggestion_id") or raw.get("linked_strategy_parameter_suggestion_id"),
        max_len=180,
    )
    field = _clean_text(
        raw.get("primary_field") or raw.get("strategy_parameter_field") or raw.get("linked_strategy_parameter_field"),
        max_len=120,
    )
    config_path = _clean_text(
        raw.get("config_path") or raw.get("strategy_parameter_config_path") or raw.get("linked_strategy_parameter_config_path"),
        max_len=220,
    )
    market = _clean_text(raw.get("market") or raw.get("linked_market"), max_len=32)
    portfolio_id = _clean_text(raw.get("portfolio_id") or raw.get("linked_portfolio_id"), max_len=160)
    status = normalize_resolution_status(raw.get("resolution_status"))
    note = _clean_text(raw.get("resolution_note"), max_len=240)
    if not any((suggestion_id, field, config_path)):
        return {}
    if suggestion_id and not status:
        status = RESOLUTION_STATUS_ACKNOWLEDGED
    out = {
        "linked_strategy_parameter_suggestion_id": suggestion_id,
        "linked_strategy_parameter_field": field,
        "linked_strategy_parameter_config_path": config_path,
        "resolution_status": status,
        "resolution_note": note,
    }
    if market:
        out["linked_market"] = market
    if portfolio_id:
        out["linked_portfolio_id"] = portfolio_id
    return out


def attach_evidence_action_link(record: Dict[str, Any] | None, payload: Dict[str, Any] | None) -> Dict[str, Any]:
    out = dict(record or {})
    link = extract_evidence_action_link(payload)
    if link:
        out.update(link)
    strategy_link = extract_strategy_parameter_suggestion_link(payload)
    if strategy_link:
        out.update(strategy_link)
    return out


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
    sanitized = attach_evidence_action_link(sanitized, raw)
    sanitized["error_class"] = classify_dashboard_control_error(
        sanitized.get("error"),
        status=str(sanitized.get("status") or ""),
    )
    sanitized["error_severity"] = error_severity(
        str(sanitized.get("error_class") or ""),
        status=str(sanitized.get("status") or ""),
    )
    return sanitized


def summarize_evidence_action_audit_links(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    source_rows = list(rows or [])
    linked_rows = [
        dict(row)
        for row in source_rows
        if isinstance(row, dict) and str(row.get("linked_evidence_action_id") or "").strip()
    ]
    last = linked_rows[-1] if linked_rows else {}
    status_counts: Dict[str, int] = {}
    for row in linked_rows:
        status = str(row.get("resolution_status") or "").strip().upper() or "UNKNOWN"
        status_counts[status] = int(status_counts.get(status, 0)) + 1
    strategy_rows = [
        dict(row)
        for row in source_rows
        if isinstance(row, dict) and str(row.get("linked_strategy_parameter_suggestion_id") or "").strip()
    ]
    last_strategy = strategy_rows[-1] if strategy_rows else {}
    strategy_status_counts: Dict[str, int] = {}
    for row in strategy_rows:
        status = str(row.get("resolution_status") or "").strip().upper() or "UNKNOWN"
        strategy_status_counts[status] = int(strategy_status_counts.get(status, 0)) + 1
    return {
        "linked_action_history_count": len(linked_rows),
        "last_linked_evidence_action_id": str(last.get("linked_evidence_action_id") or ""),
        "last_resolution_status": str(last.get("resolution_status") or ""),
        "last_linked_market": str(last.get("linked_market") or ""),
        "last_linked_portfolio_id": str(last.get("linked_portfolio_id") or ""),
        "resolution_status_counts": status_counts,
        "linked_strategy_parameter_suggestion_history_count": len(strategy_rows),
        "last_linked_strategy_parameter_suggestion_id": str(
            last_strategy.get("linked_strategy_parameter_suggestion_id") or ""
        ),
        "last_linked_strategy_parameter_field": str(last_strategy.get("linked_strategy_parameter_field") or ""),
        "last_strategy_parameter_resolution_status": str(last_strategy.get("resolution_status") or ""),
        "strategy_parameter_resolution_status_counts": strategy_status_counts,
    }


def append_dashboard_control_action_audit(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(sanitize_dashboard_control_action(row), ensure_ascii=False, sort_keys=True) + "\n")


def read_dashboard_control_action_audit(path: Path, *, max_rows: int = 1000) -> List[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(sanitize_dashboard_control_action(payload))
    except OSError:
        return []
    limit = max(1, int(max_rows))
    return rows[-limit:]
