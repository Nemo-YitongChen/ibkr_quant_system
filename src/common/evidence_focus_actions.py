from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
from typing import Any, Dict, Iterable, List

from .markets import resolve_market_code

ACTION_STATUS_SUGGESTED = "SUGGESTED"
ACTION_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
ACTION_STATUS_APPLIED = "APPLIED"
ACTION_STATUS_REJECTED = "REJECTED"
ACTION_STATUS_SUPERSEDED = "SUPERSEDED"
ACTION_STATUS_EXPIRED = "EXPIRED"

KNOWN_ACTION_STATUSES = {
    ACTION_STATUS_SUGGESTED,
    ACTION_STATUS_ACKNOWLEDGED,
    ACTION_STATUS_APPLIED,
    ACTION_STATUS_REJECTED,
    ACTION_STATUS_SUPERSEDED,
    ACTION_STATUS_EXPIRED,
}
RESOLVED_ACTION_STATUSES = {
    ACTION_STATUS_APPLIED,
    ACTION_STATUS_REJECTED,
    ACTION_STATUS_SUPERSEDED,
}
HANDLED_ACTION_STATUSES = RESOLVED_ACTION_STATUSES | {ACTION_STATUS_ACKNOWLEDGED}
NON_STALE_ACTION_STATUSES = HANDLED_ACTION_STATUSES | {ACTION_STATUS_EXPIRED}

URGENCY_URGENT = "urgent"
URGENCY_NORMAL = "normal"
URGENCY_SAMPLE_COLLECTION = "sample_collection"

KNOWN_URGENCIES = {URGENCY_URGENT, URGENCY_NORMAL, URGENCY_SAMPLE_COLLECTION}

EVIDENCE_FOCUS_ACTION_PRIORITY = {
    "review_gate_thresholds": 10,
    "review_signal_expected_edge": 20,
    "build_weekly_unified_evidence": 30,
    "hold_parameters_collect_more_evidence": 40,
    "collect_more_outcome_samples": 60,
    "continue_sample_collection": 60,
    "keep_gate_policy": 90,
    "keep_gate_monitor_post_cost": 90,
    "monitor_evidence": 99,
    "review_evidence": 99,
}

ACTION_EVIDENCE_ARTIFACTS = {
    "build_weekly_unified_evidence": "weekly_unified_evidence.json",
    "review_gate_thresholds": "weekly_blocked_vs_allowed_expost.json",
    "review_signal_expected_edge": "weekly_candidate_model_review.csv",
    "collect_more_outcome_samples": "weekly_blocked_vs_allowed_expost.json",
    "continue_sample_collection": "weekly_blocked_vs_allowed_expost.json",
    "keep_gate_policy": "weekly_blocked_vs_allowed_expost.json",
    "keep_gate_monitor_post_cost": "weekly_blocked_vs_allowed_expost.json",
}

BLOCKED_VS_ALLOWED_EXPOST_ARTIFACT = "weekly_blocked_vs_allowed_expost.json"
BLOCKED_VS_ALLOWED_REVIEW_LABEL_ACTIONS = {
    "BLOCKED_OUTPERFORMED_ALLOWED": ("review_gate_thresholds", URGENCY_URGENT),
    "INSUFFICIENT_OUTCOME_SAMPLE": ("collect_more_outcome_samples", URGENCY_SAMPLE_COLLECTION),
    "INSUFFICIENT_SAMPLE": ("collect_more_outcome_samples", URGENCY_SAMPLE_COLLECTION),
    "GATE_OK": ("keep_gate_monitor_post_cost", URGENCY_NORMAL),
    "BLOCKING_HELPED": ("keep_gate_monitor_post_cost", URGENCY_NORMAL),
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _hours_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds() / 3600.0), 2)


def normalize_action_status(status: Any) -> str:
    normalized = str(status or "").strip().upper()
    return normalized if normalized in KNOWN_ACTION_STATUSES else ACTION_STATUS_SUGGESTED


def normalize_urgency(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in KNOWN_URGENCIES else URGENCY_NORMAL


def urgency_for_action(action_type: str) -> str:
    normalized = str(action_type or "").strip()
    priority = int(EVIDENCE_FOCUS_ACTION_PRIORITY.get(normalized, 99))
    if normalized in {"collect_more_outcome_samples", "continue_sample_collection"}:
        return URGENCY_SAMPLE_COLLECTION
    if priority < 60:
        return URGENCY_URGENT
    return URGENCY_NORMAL


def build_action_id(
    *,
    week: str,
    market: str,
    portfolio_id: str,
    action_type: str,
    basis: str,
) -> str:
    raw_id = "-".join(
        part
        for part in (
            str(week or "unknown-week").strip(),
            resolve_market_code(str(market or "")) or str(market or "GLOBAL").strip().upper(),
            str(portfolio_id or "market").strip(),
            str(action_type or "monitor_evidence").strip(),
            str(basis or "no_basis").strip(),
        )
        if part
    )
    normalized = "".join(ch if ch.isalnum() or ch in {"-", "_", ":"} else "-" for ch in raw_id)
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    normalized = normalized.strip("-")
    if len(normalized) <= 96:
        return normalized
    digest = sha1(normalized.encode("utf-8")).hexdigest()[:10]
    return f"{normalized[:84].rstrip('-')}-{digest}"


def _linked_artifact(action_type: str) -> str:
    return ACTION_EVIDENCE_ARTIFACTS.get(str(action_type or "").strip(), "weekly_unified_evidence.json")


def normalize_evidence_focus_action(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    source = dict(raw or {})
    market = resolve_market_code(str(source.get("market") or "")) or str(source.get("market") or "").strip().upper()
    portfolio_id = str(source.get("portfolio_id") or "").strip()
    action_type = str(source.get("action_type") or source.get("primary_action") or "monitor_evidence").strip()
    basis = str(source.get("basis") or source.get("decision_basis") or source.get("basis_label") or "").strip()
    week = str(source.get("week") or source.get("week_label") or "").strip()
    priority_order = _safe_int(
        source.get("priority_order"),
        int(EVIDENCE_FOCUS_ACTION_PRIORITY.get(action_type, 99)),
    )
    urgency = normalize_urgency(source.get("urgency") or urgency_for_action(action_type))
    created_at = str(source.get("created_at") or "").strip()
    updated_at = str(source.get("updated_at") or created_at).strip()
    action_label = str(source.get("action") or source.get("action_label") or action_type).strip()
    detail = str(source.get("detail") or source.get("action_note") or source.get("rationale") or basis or "-").strip()
    linked_evidence_key = str(
        source.get("linked_evidence_key") or "|".join(part for part in (market, portfolio_id, basis) if part)
    ).strip()
    action_id = str(source.get("action_id") or "").strip() or build_action_id(
        week=week,
        market=market,
        portfolio_id=portfolio_id,
        action_type=action_type,
        basis=basis,
    )
    return {
        "action_id": action_id,
        "market": market,
        "portfolio_id": portfolio_id,
        "action_type": action_type,
        "primary_action": action_type,
        "action": action_label,
        "basis": basis,
        "urgency": urgency,
        "status": normalize_action_status(source.get("status")),
        "created_at": created_at,
        "updated_at": updated_at,
        "owner": str(source.get("owner") or "").strip(),
        "linked_evidence_artifact": str(source.get("linked_evidence_artifact") or _linked_artifact(action_type)),
        "linked_evidence_key": linked_evidence_key,
        "read_only": bool(source.get("read_only", True)),
        "summary": str(source.get("summary") or detail),
        "detail": detail,
        "resolved_at": str(source.get("resolved_at") or "").strip(),
        "resolution_source": str(source.get("resolution_source") or "").strip(),
        "resolution_note": str(source.get("resolution_note") or "").strip(),
        "priority_order": priority_order,
        "evidence_row_count": _safe_int(source.get("evidence_row_count")),
        "blocked_review_count": _safe_int(source.get("blocked_review_count")),
        "sample_ready_review_count": _safe_int(source.get("sample_ready_review_count")),
        "insufficient_sample_count": _safe_int(source.get("insufficient_sample_count")),
    }


def action_from_blocked_vs_allowed_review_label(review_label: str) -> tuple[str, str]:
    label = str(review_label or "").strip().upper()
    if label in BLOCKED_VS_ALLOWED_REVIEW_LABEL_ACTIONS:
        return BLOCKED_VS_ALLOWED_REVIEW_LABEL_ACTIONS[label]
    if label.startswith("INSUFFICIENT"):
        return BLOCKED_VS_ALLOWED_REVIEW_LABEL_ACTIONS["INSUFFICIENT_SAMPLE"]
    return "review_evidence", URGENCY_NORMAL


def build_evidence_focus_actions_from_expost(
    rows: Iterable[Dict[str, Any]] | None,
    *,
    week: str,
    artifact_missing: bool = False,
) -> List[Dict[str, Any]]:
    if rows is None or artifact_missing:
        return [
            normalize_evidence_focus_action(
                {
                    "week": week,
                    "market": "GLOBAL",
                    "portfolio_id": "",
                    "action_type": "build_weekly_unified_evidence",
                    "action": "build_weekly_unified_evidence",
                    "basis": "MISSING_BLOCKED_VS_ALLOWED_EXPOST_ARTIFACT",
                    "urgency": URGENCY_URGENT,
                    "linked_evidence_artifact": BLOCKED_VS_ALLOWED_EXPOST_ARTIFACT,
                    "linked_evidence_key": f"missing|{BLOCKED_VS_ALLOWED_EXPOST_ARTIFACT}",
                    "summary": (
                        "weekly_blocked_vs_allowed_expost artifact missing; "
                        "rebuild weekly evidence before gate calibration."
                    ),
                    "detail": (
                        "Blocked-vs-allowed ex-post review is unavailable, so gate calibration should "
                        "wait for weekly evidence regeneration."
                    ),
                }
            )
        ]
    actions: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        action_type, urgency = action_from_blocked_vs_allowed_review_label(str(row.get("review_label") or ""))
        action = normalize_evidence_focus_action(
            {
                "week": week,
                "market": row.get("market"),
                "portfolio_id": row.get("portfolio_id"),
                "action_type": action_type,
                "action": action_type,
                "basis": str(row.get("review_label") or ""),
                "urgency": urgency,
                "linked_evidence_artifact": BLOCKED_VS_ALLOWED_EXPOST_ARTIFACT,
                "linked_evidence_key": "|".join(
                    str(part or "")
                    for part in (
                        row.get("market"),
                        row.get("portfolio_id"),
                        row.get("block_reason"),
                        row.get("horizon"),
                    )
                ).strip("|"),
                "summary": str(row.get("recommendation") or row.get("review_label") or ""),
                "evidence_row_count": _safe_int(row.get("blocked_count")) + _safe_int(row.get("allowed_count")),
                "blocked_review_count": 1,
            }
        )
        actions.append(action)
    actions.sort(
        key=lambda row: (
            int(row.get("priority_order", 99) or 99),
            str(row.get("market") or ""),
            str(row.get("action_id") or ""),
        )
    )
    return actions


def build_evidence_focus_actions_from_market_summaries(
    market_evidence_action_summary: Dict[str, Any],
    *,
    week: str = "",
    limit: int = 5,
) -> List[Dict[str, Any]]:
    summaries = dict(market_evidence_action_summary) if isinstance(market_evidence_action_summary, dict) else {}
    actions: List[Dict[str, Any]] = []
    for market_key, raw_summary in sorted(summaries.items(), key=lambda part: str(part[0])):
        if not isinstance(raw_summary, dict):
            continue
        summary = dict(raw_summary)
        action_type = str(summary.get("primary_action") or "").strip()
        priority = int(EVIDENCE_FOCUS_ACTION_PRIORITY.get(action_type, 99))
        if priority >= 90:
            continue
        market = resolve_market_code(str(summary.get("market") or market_key or "")) or str(market_key or "").upper()
        action = normalize_evidence_focus_action(
            {
                "week": week,
                "market": market,
                "portfolio_id": summary.get("portfolio_id"),
                "action_type": action_type,
                "action": str(summary.get("action_label") or action_type).strip(),
                "basis": str(summary.get("basis_label") or summary.get("decision_basis") or "").strip(),
                "detail": str(
                    summary.get("action_note") or summary.get("rationale") or summary.get("basis_label") or "-"
                ).strip(),
                "priority_order": priority,
                "evidence_row_count": summary.get("evidence_row_count"),
                "blocked_review_count": summary.get("blocked_review_count"),
                "sample_ready_review_count": summary.get("sample_ready_review_count"),
                "insufficient_sample_count": summary.get("insufficient_sample_count"),
            }
        )
        actions.append(action)
    actions.sort(key=lambda row: (int(row.get("priority_order", 99) or 99), str(row.get("market", ""))))
    limit_int = max(0, int(limit))
    return actions[:limit_int] if limit_int else []


def summarize_evidence_focus_actions(actions: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [normalize_evidence_focus_action(row) for row in list(actions or []) if isinstance(row, dict)]
    urgent_count = sum(1 for row in rows if str(row.get("urgency") or "") == URGENCY_URGENT)
    open_urgent_count = sum(
        1
        for row in rows
        if str(row.get("urgency") or "") == URGENCY_URGENT
        and str(row.get("status") or ACTION_STATUS_SUGGESTED) == ACTION_STATUS_SUGGESTED
    )
    sample_collection_count = sum(1 for row in rows if str(row.get("urgency") or "") == URGENCY_SAMPLE_COLLECTION)
    gate_review_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "review_gate_thresholds")
    signal_review_count = sum(
        1 for row in rows if str(row.get("primary_action") or "") == "review_signal_expected_edge"
    )
    missing_evidence_count = sum(
        1 for row in rows if str(row.get("primary_action") or "") == "build_weekly_unified_evidence"
    )
    primary = rows[0] if rows else {}
    primary_action = str(primary.get("primary_action") or "")
    primary_label = str(primary.get("action") or "")
    primary_market = str(primary.get("market") or "")
    primary_basis = str(primary.get("basis") or "")
    primary_detail = str(primary.get("detail") or "")
    if not rows:
        summary_text = "No actionable evidence focus work."
    else:
        summary_text = (
            f"{primary_market or '-'}: {primary_label or primary_action or '-'}; "
            f"basis={primary_basis or '-'}; urgent={open_urgent_count}/{len(rows)}."
        )
    return {
        "status": "warn" if open_urgent_count else "ok",
        "summary_text": summary_text,
        "primary_market": primary_market,
        "primary_action": primary_action,
        "primary_action_label": primary_label,
        "primary_basis": primary_basis,
        "primary_detail": primary_detail,
        "focus_action_count": len(rows),
        "action_count": len(rows),
        "urgent_action_count": urgent_count,
        "urgent_count": urgent_count,
        "open_urgent_action_count": open_urgent_count,
        "gate_review_count": gate_review_count,
        "signal_review_count": signal_review_count,
        "missing_evidence_count": missing_evidence_count,
        "sample_collection_count": sample_collection_count,
        "basis_counts": _basis_counts(rows),
        "read_only": True,
        "actions": rows,
    }


def _basis_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        basis = str(row.get("basis") or "").strip() or "unknown"
        counts[basis] = counts.get(basis, 0) + 1
    return counts


def apply_action_resolutions(
    actions: Iterable[Dict[str, Any]],
    audit_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    normalized_actions = [normalize_evidence_focus_action(row) for row in list(actions or []) if isinstance(row, dict)]
    latest_by_action_id: Dict[str, tuple[int, str, Dict[str, Any]]] = {}
    for idx, raw in enumerate(list(audit_rows or [])):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        action_id = str(row.get("linked_evidence_action_id") or "").strip()
        if not action_id:
            continue
        status = str(row.get("resolution_status") or "").strip().upper()
        if status not in {
            ACTION_STATUS_ACKNOWLEDGED,
            ACTION_STATUS_APPLIED,
            ACTION_STATUS_REJECTED,
            ACTION_STATUS_SUPERSEDED,
        }:
            continue
        ts = str(row.get("ts") or row.get("timestamp") or row.get("updated_at") or "").strip()
        existing = latest_by_action_id.get(action_id)
        if existing is None or (ts, idx) >= (existing[1], existing[0]):
            latest_by_action_id[action_id] = (idx, ts, row)

    resolved: List[Dict[str, Any]] = []
    for action in normalized_actions:
        action_id = str(action.get("action_id") or "").strip()
        match = latest_by_action_id.get(action_id)
        if not match:
            resolved.append(action)
            continue
        _, ts, audit = match
        updated = dict(action)
        updated["status"] = str(audit.get("resolution_status") or ACTION_STATUS_ACKNOWLEDGED).strip().upper()
        updated["resolved_at"] = ts
        updated["resolution_source"] = "dashboard_control"
        updated["resolution_note"] = str(audit.get("resolution_note") or "").strip()
        resolved.append(updated)
    return resolved


def build_evidence_focus_effectiveness_summary(
    actions: Iterable[Dict[str, Any]],
    *,
    now_iso: str,
    stale_after_days: int = 7,
) -> Dict[str, Any]:
    """Summarize whether evidence focus actions were handled after creation."""
    rows = [normalize_evidence_focus_action(row) for row in list(actions or []) if isinstance(row, dict)]
    now_dt = _parse_iso_datetime(now_iso) or datetime.now(timezone.utc)
    stale_hours = max(0.0, float(stale_after_days) * 24.0)
    status_counts: Dict[str, int] = {}
    stale_urgent_action_ids: List[str] = []
    resolution_hours: List[float] = []

    for row in rows:
        status = normalize_action_status(row.get("status"))
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        created_dt = _parse_iso_datetime(row.get("created_at") or row.get("updated_at"))
        resolved_dt = _parse_iso_datetime(row.get("resolved_at"))
        hours = _hours_between(created_dt, resolved_dt)
        action_age_hours = _hours_between(created_dt, now_dt)
        if hours is not None and status in HANDLED_ACTION_STATUSES:
            resolution_hours.append(hours)
        if (
            str(row.get("urgency") or "") == URGENCY_URGENT
            and status not in NON_STALE_ACTION_STATUSES
            and created_dt is not None
            and action_age_hours is not None
            and action_age_hours >= stale_hours
        ):
            stale_urgent_action_ids.append(str(row.get("action_id") or ""))

    urgent_count = sum(1 for row in rows if str(row.get("urgency") or "") == URGENCY_URGENT)
    open_urgent_count = sum(
        1
        for row in rows
        if str(row.get("urgency") or "") == URGENCY_URGENT
        and normalize_action_status(row.get("status")) not in NON_STALE_ACTION_STATUSES
    )
    sample_collection_count = sum(1 for row in rows if str(row.get("urgency") or "") == URGENCY_SAMPLE_COLLECTION)
    resolved_count = sum(
        1
        for row in rows
        if normalize_action_status(row.get("status")) in RESOLVED_ACTION_STATUSES
    )
    avg_resolution_hours = round(sum(resolution_hours) / len(resolution_hours), 2) if resolution_hours else 0.0
    summary_text = (
        f"actions={len(rows)} urgent={urgent_count} open_urgent={open_urgent_count} "
        f"resolved={resolved_count} stale_urgent={len(stale_urgent_action_ids)} "
        f"avg_resolution_hours={avg_resolution_hours:.2f}"
    )
    return {
        "status": "warn" if stale_urgent_action_ids else "ok",
        "summary_text": summary_text,
        "new_action_count": len(rows),
        "action_count": len(rows),
        "urgent_action_count": urgent_count,
        "open_urgent_action_count": open_urgent_count,
        "resolved_action_count": resolved_count,
        "acknowledged_action_count": int(status_counts.get(ACTION_STATUS_ACKNOWLEDGED, 0)),
        "applied_action_count": int(status_counts.get(ACTION_STATUS_APPLIED, 0)),
        "rejected_action_count": int(status_counts.get(ACTION_STATUS_REJECTED, 0)),
        "superseded_action_count": int(status_counts.get(ACTION_STATUS_SUPERSEDED, 0)),
        "sample_collection_count": sample_collection_count,
        "stale_urgent_action_count": len(stale_urgent_action_ids),
        "avg_resolution_hours": avg_resolution_hours,
        "stale_after_days": int(stale_after_days),
        "status_counts": status_counts,
        "stale_urgent_action_ids": [action_id for action_id in stale_urgent_action_ids if action_id][:10],
        "read_only": True,
    }
