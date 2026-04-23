from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping


def _parse_ts(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _status_label(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw == "degraded":
        return "有降级"
    if raw == "warning":
        return "有告警"
    return "已就绪"


def build_governance_health_summary(
    cards: Iterable[Mapping[str, Any]],
    governance_rows: Iterable[Mapping[str, Any]],
    *,
    now: datetime | None = None,
) -> Dict[str, Any]:
    now_dt = now or datetime.now(timezone.utc)
    pending_action_count = 0
    ready_for_manual_apply_count = 0
    approved_not_applied_count = 0
    evidence_mismatch_count = 0
    focus_items: List[str] = []
    oldest_pending_days = 0.0

    for raw_card in list(cards or []):
        card = dict(raw_card or {})
        control = dict(dict(card.get("dashboard_control") or {}).get("portfolio") or {})
        governance_action_label = str(control.get("weekly_feedback_patch_governance_action_label") or "").strip()
        if governance_action_label:
            pending_action_count += 1
            focus_items.append(
                f"{str(card.get('market') or '-')}/{str(card.get('watchlist') or '-')}: {governance_action_label}"
            )
        for prefix in ("market_profile", "calibration_patch"):
            ready = bool(control.get(f"weekly_feedback_{prefix}_ready_for_manual_apply", False))
            if ready:
                ready_for_manual_apply_count += 1
            review_status = str(control.get(f"weekly_feedback_{prefix}_review_status") or "").strip().upper()
            review_evidence_summary = str(control.get(f"weekly_feedback_{prefix}_review_evidence_summary") or "").strip()
            if review_status == "APPROVED":
                approved_not_applied_count += 1
            if review_status == "APPLIED" and not review_evidence_summary:
                evidence_mismatch_count += 1
            if review_status in {"APPROVED", "READY"}:
                patch_kind = "market_profile" if prefix == "market_profile" else "calibration"
                latest_ts = None
                for raw_history in list(card.get("patch_review_history_rows", []) or []):
                    history = dict(raw_history or {})
                    if str(history.get("patch_kind") or "").strip().lower() != patch_kind:
                        continue
                    ts = _parse_ts(str(history.get("ts") or ""))
                    if ts is not None and (latest_ts is None or ts > latest_ts):
                        latest_ts = ts
                if latest_ts is not None:
                    pending_days = max(0.0, (now_dt - latest_ts).total_seconds() / 86400.0)
                    oldest_pending_days = max(oldest_pending_days, pending_days)

    rejection_hotspot_count = sum(
        1
        for row in list(governance_rows or [])
        if float(row.get("rejection_rate", 0.0) or 0.0) >= 0.5 and int(row.get("review_cycle_count", 0) or 0) >= 2
    )
    approved_not_applied_overview_count = sum(
        int(row.get("approved_not_applied_count", 0) or 0)
        for row in list(governance_rows or [])
    )
    approved_not_applied_count = max(approved_not_applied_count, approved_not_applied_overview_count)

    status = "ready"
    if evidence_mismatch_count > 0 or (approved_not_applied_count > 0 and oldest_pending_days >= 14.0):
        status = "degraded"
    elif pending_action_count > 0 or approved_not_applied_count > 0 or ready_for_manual_apply_count > 0 or rejection_hotspot_count > 0:
        status = "warning"

    summary_bits = [
        f"pending {pending_action_count}",
        f"approved_not_applied {approved_not_applied_count}",
        f"ready_for_manual_apply {ready_for_manual_apply_count}",
    ]
    if rejection_hotspot_count > 0:
        summary_bits.append(f"reject_hotspots {rejection_hotspot_count}")
    if evidence_mismatch_count > 0:
        summary_bits.append(f"evidence_mismatch {evidence_mismatch_count}")
    if oldest_pending_days > 0:
        summary_bits.append(f"oldest_pending_days {round(oldest_pending_days, 1)}")

    return {
        "status": status,
        "status_label": _status_label(status),
        "summary_text": " | ".join(summary_bits),
        "pending_action_count": int(pending_action_count),
        "approved_not_applied_count": int(approved_not_applied_count),
        "ready_for_manual_apply_count": int(ready_for_manual_apply_count),
        "rejection_hotspot_count": int(rejection_hotspot_count),
        "evidence_mismatch_count": int(evidence_mismatch_count),
        "oldest_pending_days": round(oldest_pending_days, 2) if oldest_pending_days > 0 else None,
        "focus_items": focus_items[:5],
    }
