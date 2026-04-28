from __future__ import annotations

from typing import Any, Dict, List

from ..common.alert_classification import summarize_error_classes


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _rows(value: Any, *, limit: int = 20) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value[:limit] if isinstance(row, dict)]


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _block_status_from_rows(rows: List[Dict[str, Any]]) -> str:
    statuses = {str(row.get("status") or "").strip().lower() for row in rows}
    if "fail" in statuses or "error" in statuses:
        return "fail"
    if "warn" in statuses or "warning" in statuses or "degraded" in statuses:
        return "warn"
    return "ok"


def build_ops_health_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    ops = _dict(payload.get("ops_overview"))
    artifact = _dict(payload.get("artifact_health_overview"))
    governance = _dict(payload.get("governance_health_summary"))
    alert_rows = _rows(ops.get("alert_rows"), limit=25)
    status = str(ops.get("status") or "").strip().lower() or _block_status_from_rows(alert_rows)
    return {
        "id": "ops_health",
        "title": "Ops Health",
        "status": status,
        "summary": str(ops.get("summary_text") or ""),
        "metrics": {
            "preflight_fail_count": _int(ops.get("preflight_fail_count")),
            "preflight_warn_count": _int(ops.get("preflight_warn_count")),
            "degraded_health_count": _int(ops.get("degraded_health_count")),
            "stale_report_count": _int(ops.get("stale_report_count")),
            "artifact_fail_count": _int(artifact.get("fail_count")),
            "artifact_warn_count": _int(artifact.get("warn_count")),
            "governance_blocked_count": _int(governance.get("blocked_count")),
            "governance_warn_count": _int(governance.get("warn_count")),
        },
        "rows": alert_rows,
    }


def build_control_actions_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    control = _dict(payload.get("dashboard_control"))
    service = _dict(control.get("service"))
    actions = _dict(control.get("actions"))
    history = list(reversed(_rows(actions.get("action_history"), limit=50)))[:20]
    failed_count = sum(1 for row in history if str(row.get("status") or "").lower() == "failed")
    error_summary = summarize_error_classes(history)
    return {
        "id": "dashboard_control_actions",
        "title": "Dashboard Control Actions",
        "status": "fail" if failed_count else str(service.get("status") or "disabled"),
        "summary": (
            f"service={service.get('status') or 'disabled'} "
            f"last_action={actions.get('last_action') or '-'} "
            f"failed_recent={failed_count} "
            f"primary_error={error_summary.get('primary_error_class') or 'none'}"
        ),
        "metrics": {
            "history_count": len(history),
            "failed_count": failed_count,
            "retryable_error_count": _int(error_summary.get("retryable_count")),
            "validation_error_count": _int(dict(error_summary.get("class_counts") or {}).get("validation")),
            "permission_error_count": _int(dict(error_summary.get("class_counts") or {}).get("permission")),
            "transient_io_error_count": _int(dict(error_summary.get("class_counts") or {}).get("transient_io")),
            "task_failed_error_count": _int(dict(error_summary.get("class_counts") or {}).get("task_failed")),
            "exception_error_count": _int(dict(error_summary.get("class_counts") or {}).get("exception")),
            "run_once_in_progress": int(bool(actions.get("run_once_in_progress"))),
            "preflight_in_progress": int(bool(actions.get("preflight_in_progress"))),
            "weekly_review_in_progress": int(bool(actions.get("weekly_review_in_progress"))),
        },
        "rows": history,
    }


def build_market_views_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    market_views = _dict(payload.get("market_views"))
    rows = [
        dict(row)
        for _, row in sorted(market_views.items(), key=lambda part: str(part[0]))
        if isinstance(row, dict)
    ]
    attention_count = sum(_int(row.get("stale_report_count")) + _int(row.get("degraded_health_count")) for row in rows)
    return {
        "id": "market_views",
        "title": "US/HK/CN Market Views",
        "status": "warn" if attention_count else "ok",
        "summary": f"markets={len(rows)} attention={attention_count}",
        "metrics": {
            "market_count": len(rows),
            "portfolio_count": sum(_int(row.get("portfolio_count")) for row in rows),
            "open_count": sum(_int(row.get("open_count")) for row in rows),
            "attention_count": attention_count,
        },
        "rows": rows,
    }


def build_evidence_quality_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence_overview = _dict(payload.get("unified_evidence_overview"))
    blocked_review_rows = _rows(payload.get("blocked_vs_allowed_expost_review"), limit=20)
    waterfall_rows = _rows(payload.get("weekly_attribution_waterfall"), limit=30)
    too_restrictive_count = sum(
        1
        for row in blocked_review_rows
        if str(row.get("review_label") or "").strip().upper() == "BLOCKED_OUTPERFORMED_ALLOWED"
    )
    return {
        "id": "evidence_quality",
        "title": "Trading Quality Evidence",
        "status": "warn" if too_restrictive_count else "ok",
        "summary": (
            f"evidence_rows={_int(evidence_overview.get('row_count'))} "
            f"candidate_only={_int(evidence_overview.get('candidate_only_row_count'))} "
            f"blocked_reviews={len(blocked_review_rows)}"
        ),
        "metrics": {
            "evidence_row_count": _int(evidence_overview.get("row_count")),
            "blocked_row_count": _int(evidence_overview.get("blocked_row_count")),
            "allowed_row_count": _int(evidence_overview.get("allowed_row_count")),
            "candidate_only_row_count": _int(evidence_overview.get("candidate_only_row_count")),
            "outcome_labeled_row_count": _int(evidence_overview.get("outcome_labeled_row_count")),
            "partial_join_row_count": _int(evidence_overview.get("partial_join_row_count")),
            "too_restrictive_count": too_restrictive_count,
            "waterfall_row_count": len(waterfall_rows),
        },
        "rows": {
            "blocked_vs_allowed": blocked_review_rows,
            "weekly_attribution_waterfall": waterfall_rows,
        },
    }


def build_dashboard_v2_blocks(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        build_ops_health_block(payload),
        build_control_actions_block(payload),
        build_market_views_block(payload),
        build_evidence_quality_block(payload),
    ]
