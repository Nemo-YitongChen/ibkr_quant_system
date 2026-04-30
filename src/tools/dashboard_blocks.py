from __future__ import annotations

from typing import Any, Dict, List

from ..common.alert_classification import summarize_error_classes


EVIDENCE_ACTION_DETAILS = {
    "build_weekly_unified_evidence": {
        "label": "Build unified evidence",
        "note": "Weekly evidence is missing; regenerate weekly review before changing parameters.",
    },
    "review_gate_thresholds": {
        "label": "Review gate thresholds",
        "note": "Blocked rows outperformed allowed rows; review edge floor, buffers, and market-rule handling.",
    },
    "review_signal_expected_edge": {
        "label": "Review signal expected edge",
        "note": "Candidate model warning is active; calibrate signal score to expected and realized edge first.",
    },
    "collect_more_outcome_samples": {
        "label": "Collect more outcome samples",
        "note": "Blocked-vs-allowed evidence is sample-starved; keep collecting candidate/outcome labels.",
    },
    "hold_parameters_collect_more_evidence": {
        "label": "Hold parameters",
        "note": "Evidence is mixed; avoid changing multiple gates until the sample stabilizes.",
    },
    "keep_gate_monitor_post_cost": {
        "label": "Keep gate and monitor",
        "note": "Blocking helped on post-cost outcomes; keep current gate and monitor future windows.",
    },
    "monitor_evidence": {
        "label": "Monitor evidence",
        "note": "No actionable warning yet; continue monitoring evidence quality and post-cost outcomes.",
    },
}


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


def _blocked_review_label(row: Dict[str, Any]) -> str:
    return str(row.get("review_label") or "").strip().upper()


def _count_labels(rows: List[Dict[str, Any]], labels: set[str]) -> int:
    return sum(1 for row in rows if _blocked_review_label(row) in labels)


def _count_insufficient_sample(rows: List[Dict[str, Any]]) -> int:
    return sum(1 for row in rows if _blocked_review_label(row).startswith("INSUFFICIENT"))


def _blocked_review_label_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in rows:
        label = _blocked_review_label(row) or "UNKNOWN"
        counts[label] = counts.get(label, 0) + 1
    return [
        {
            "review_label": label,
            "count": count,
            "action": _blocked_review_label_action(label),
        }
        for label, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _blocked_review_label_action(label: str) -> str:
    normalized = str(label or "").strip().upper()
    if normalized == "BLOCKED_OUTPERFORMED_ALLOWED":
        return "review_gate_thresholds"
    if normalized in {"BLOCKING_HELPED", "GATE_OK"}:
        return "keep_gate_monitor_post_cost"
    if normalized.startswith("INSUFFICIENT"):
        return "collect_more_outcome_samples"
    if normalized in {"MIXED", "NEUTRAL"}:
        return "hold_parameters_collect_more_evidence"
    return "monitor_evidence"


def _evidence_action_details(action: str) -> Dict[str, str]:
    details = EVIDENCE_ACTION_DETAILS.get(str(action or ""))
    if details:
        return dict(details)
    return dict(EVIDENCE_ACTION_DETAILS["monitor_evidence"])


def _evidence_primary_action(
    *,
    evidence_row_count: int,
    blocked_review_count: int,
    too_restrictive_count: int,
    model_warning_count: int,
    insufficient_sample_count: int,
    blocking_helped_count: int,
    mixed_review_count: int,
) -> str:
    if evidence_row_count <= 0:
        return "build_weekly_unified_evidence"
    if too_restrictive_count > 0:
        return "review_gate_thresholds"
    if model_warning_count > 0:
        return "review_signal_expected_edge"
    if blocked_review_count > 0 and insufficient_sample_count >= blocked_review_count:
        return "collect_more_outcome_samples"
    if mixed_review_count > 0:
        return "hold_parameters_collect_more_evidence"
    if blocking_helped_count > 0:
        return "keep_gate_monitor_post_cost"
    return "monitor_evidence"


def _block_status_from_rows(rows: List[Dict[str, Any]]) -> str:
    statuses = {str(row.get("status") or "").strip().lower() for row in rows}
    if "fail" in statuses or "error" in statuses:
        return "fail"
    if "warn" in statuses or "warning" in statuses or "degraded" in statuses:
        return "warn"
    return "ok"


ACTION_BY_LABEL = {
    str(details.get("label") or "").strip().lower(): action
    for action, details in EVIDENCE_ACTION_DETAILS.items()
}


def _market_evidence_summary(row: Dict[str, Any], summaries: Dict[str, Any]) -> Dict[str, Any]:
    market = str(row.get("market") or "").strip().upper()
    raw_summary = summaries.get(market) if market else {}
    summary = dict(raw_summary) if isinstance(raw_summary, dict) else {}
    action = str(summary.get("primary_action") or "").strip()
    if not action:
        action_label = str(row.get("evidence_action_label") or "").strip().lower()
        action = ACTION_BY_LABEL.get(action_label, "")
    return {
        "primary_action": action,
        "decision_basis": str(summary.get("decision_basis") or "").strip(),
        "action_label": str(summary.get("action_label") or row.get("evidence_action_label") or ""),
        "basis_label": str(summary.get("basis_label") or row.get("evidence_basis_label") or ""),
        "rationale": str(summary.get("rationale") or row.get("evidence_rationale") or ""),
        "evidence_row_count": _int(summary.get("evidence_row_count", row.get("evidence_row_count"))),
    }


def _enrich_market_view_row(row: Dict[str, Any], evidence: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(row)
    if evidence.get("primary_action"):
        enriched["evidence_primary_action"] = evidence.get("primary_action")
    if evidence.get("decision_basis"):
        enriched["evidence_decision_basis"] = evidence.get("decision_basis")
    if evidence.get("action_label"):
        enriched["evidence_action_label"] = evidence.get("action_label")
    if evidence.get("basis_label"):
        enriched["evidence_basis_label"] = evidence.get("basis_label")
    if evidence.get("rationale"):
        enriched["evidence_rationale"] = evidence.get("rationale")
    enriched["evidence_row_count"] = evidence.get("evidence_row_count", 0)
    return enriched


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
    evidence_summaries = _dict(payload.get("market_evidence_action_summary"))
    rows = []
    evidence_actions: List[str] = []
    for _, row in sorted(market_views.items(), key=lambda part: str(part[0])):
        if not isinstance(row, dict):
            continue
        evidence = _market_evidence_summary(row, evidence_summaries)
        evidence_actions.append(str(evidence.get("primary_action") or ""))
        rows.append(_enrich_market_view_row(dict(row), evidence))
    attention_count = sum(_int(row.get("stale_report_count")) + _int(row.get("degraded_health_count")) for row in rows)
    missing_evidence_count = sum(1 for action in evidence_actions if action == "build_weekly_unified_evidence")
    gate_review_count = sum(1 for action in evidence_actions if action == "review_gate_thresholds")
    signal_review_count = sum(1 for action in evidence_actions if action == "review_signal_expected_edge")
    sample_collection_count = sum(1 for action in evidence_actions if action == "collect_more_outcome_samples")
    evidence_attention_count = missing_evidence_count + gate_review_count + signal_review_count
    return {
        "id": "market_views",
        "title": "US/HK/CN Market Views",
        "status": "warn" if attention_count or evidence_attention_count else "ok",
        "summary": (
            f"markets={len(rows)} attention={attention_count} "
            f"evidence_attention={evidence_attention_count}"
        ),
        "metrics": {
            "market_count": len(rows),
            "portfolio_count": sum(_int(row.get("portfolio_count")) for row in rows),
            "open_count": sum(_int(row.get("open_count")) for row in rows),
            "attention_count": attention_count,
            "evidence_action_market_count": sum(1 for action in evidence_actions if action),
            "evidence_row_market_count": sum(1 for row in rows if _int(row.get("evidence_row_count")) > 0),
            "evidence_attention_count": evidence_attention_count,
            "missing_evidence_market_count": missing_evidence_count,
            "gate_review_market_count": gate_review_count,
            "signal_review_market_count": signal_review_count,
            "sample_collection_market_count": sample_collection_count,
        },
        "rows": rows,
    }


def build_evidence_quality_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence_overview = _dict(payload.get("unified_evidence_overview"))
    blocked_review_rows = _rows(payload.get("blocked_vs_allowed_expost_review"), limit=20)
    candidate_model_rows = _rows(payload.get("candidate_model_review"), limit=20)
    waterfall_rows = _rows(payload.get("weekly_attribution_waterfall"), limit=30)
    evidence_row_count = _int(evidence_overview.get("row_count"))
    too_restrictive_count = _count_labels(blocked_review_rows, {"BLOCKED_OUTPERFORMED_ALLOWED"})
    blocking_helped_count = _count_labels(blocked_review_rows, {"BLOCKING_HELPED", "GATE_OK"})
    insufficient_sample_count = _count_insufficient_sample(blocked_review_rows)
    mixed_review_count = _count_labels(blocked_review_rows, {"MIXED", "NEUTRAL"})
    blocked_review_count = len(blocked_review_rows)
    sample_ready_review_count = max(0, blocked_review_count - insufficient_sample_count)
    model_warning_count = sum(
        1
        for row in candidate_model_rows
        if str(row.get("review_label") or "").strip().upper()
        in {"SIGNAL_RANKING_INVERTED", "EXPECTED_EDGE_OVERSTATED"}
    )
    primary_action = _evidence_primary_action(
        evidence_row_count=evidence_row_count,
        blocked_review_count=blocked_review_count,
        too_restrictive_count=too_restrictive_count,
        model_warning_count=model_warning_count,
        insufficient_sample_count=insufficient_sample_count,
        blocking_helped_count=blocking_helped_count,
        mixed_review_count=mixed_review_count,
    )
    action_details = _evidence_action_details(primary_action)
    return {
        "id": "evidence_quality",
        "title": "Trading Quality Evidence",
        "status": "warn" if too_restrictive_count or model_warning_count else "ok",
        "summary": (
            f"evidence_rows={evidence_row_count} "
            f"candidate_only={_int(evidence_overview.get('candidate_only_row_count'))} "
            f"model_reviews={len(candidate_model_rows)} "
            f"blocked_reviews={blocked_review_count} "
            f"action={action_details.get('label')}"
        ),
        "metrics": {
            "primary_action": primary_action,
            "action_label": action_details.get("label"),
            "action_note": action_details.get("note"),
            "evidence_row_count": evidence_row_count,
            "blocked_review_count": blocked_review_count,
            "sample_ready_review_count": sample_ready_review_count,
            "insufficient_sample_count": insufficient_sample_count,
            "too_restrictive_count": too_restrictive_count,
            "blocking_helped_count": blocking_helped_count,
            "mixed_review_count": mixed_review_count,
            "blocked_row_count": _int(evidence_overview.get("blocked_row_count")),
            "allowed_row_count": _int(evidence_overview.get("allowed_row_count")),
            "candidate_only_row_count": _int(evidence_overview.get("candidate_only_row_count")),
            "outcome_labeled_row_count": _int(evidence_overview.get("outcome_labeled_row_count")),
            "partial_join_row_count": _int(evidence_overview.get("partial_join_row_count")),
            "candidate_model_review_count": len(candidate_model_rows),
            "candidate_model_warning_count": model_warning_count,
            "waterfall_row_count": len(waterfall_rows),
        },
        "rows": {
            "blocked_vs_allowed_label_summary": _blocked_review_label_summary(blocked_review_rows),
            "candidate_model_review": candidate_model_rows,
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
