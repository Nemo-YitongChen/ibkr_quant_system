from __future__ import annotations

from typing import Any, Dict, List

from ..common.alert_classification import summarize_error_classes
from ..common.auto_order_readiness import (
    build_auto_order_recovery_plan,
    build_stale_execution_refresh_plan,
    evaluate_auto_order_recovery_eligibility,
)
from ..common.dashboard_control_audit import summarize_evidence_action_audit_links
from ..common.watchlist_expansion import (
    WatchlistExpansionPolicy,
    selection_reason_summary,
    summarize_seed_promotion_quality,
    summarize_watchlist_expansion,
)


DASHBOARD_BLOCK_CATEGORY_HOME = "home"
DASHBOARD_BLOCK_CATEGORY_ADVANCED = "advanced"

HOME_DASHBOARD_BLOCK_IDS = [
    "ops_health",
    "open_market_analysis",
    "auto_order_readiness",
    "evidence_focus_actions",
    "evidence_quality",
    "dashboard_control_actions",
]
ADVANCED_DASHBOARD_BLOCK_IDS = [
    "market_views",
    "watchlist_expansion",
    "walk_forward_acceptance",
    "strategy_parameter_governance",
    "weekly_attribution_waterfall",
    "unified_evidence_overview",
    "blocked_vs_allowed_expost",
    "dashboard_control_action_history",
]

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


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _build_outcome_trial_gate_plan(
    trial_rows: List[Dict[str, Any]],
    readiness_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Map outcome-supported paper trial contracts to the current submit gates."""
    readiness_by_key = {
        (
            str(row.get("market") or "").strip().upper(),
            str(row.get("portfolio_id") or "").strip(),
        ): dict(row)
        for row in readiness_rows
        if str(row.get("market") or "").strip() and str(row.get("portfolio_id") or "").strip()
    }
    rows: List[Dict[str, Any]] = []
    for raw in trial_rows:
        trial = dict(raw or {})
        trial_status = str(trial.get("trial_status") or "").strip().upper()
        if trial_status and trial_status != "READY_FOR_MANUAL_REVIEW":
            continue
        market = str(trial.get("market") or "").strip().upper()
        portfolio_id = str(trial.get("portfolio_id") or "").strip()
        if not market or not portfolio_id:
            continue
        readiness = readiness_by_key.get((market, portfolio_id), {})
        hard_blocks = [
            str(value).strip()
            for value in list(readiness.get("hard_blocks") or [])
            if str(value).strip()
        ]
        gate_blockers: List[str] = []
        if not readiness:
            gate_blockers.append("market_readiness_row_missing")
        if bool(trial.get("requires_gateway_budget_ok", True)) and "gateway_budget_degraded" in hard_blocks:
            gate_blockers.append("gateway_budget_degraded")
        readiness_status = str(readiness.get("market_readiness_status") or "").strip().upper()
        artifact_status = str(readiness.get("market_readiness_artifact_health_status") or "").strip().upper()
        if bool(trial.get("requires_fresh_buy_plan", True)):
            if readiness_status != "READY_FOR_PAPER_REVIEW" or artifact_status == "STALE":
                gate_blockers.append("fresh_buy_plan_required")
            order_count = _int(readiness.get("market_readiness_order_count"))
            buy_value = _float(readiness.get("market_readiness_planned_buy_order_value"))
            if order_count <= 0 or buy_value <= 0.0:
                gate_blockers.append("buy_plan_missing")
        if bool(trial.get("requires_submit_quality_pass", True)):
            submit_quality = str(readiness.get("submit_quality_status") or "").strip().upper()
            if submit_quality != "PASS":
                gate_blockers.append("submit_quality_not_pass")
        if "strategy_suggestion_stale" in hard_blocks:
            gate_blockers.append("strategy_suggestion_stale")
        unique_blockers = list(dict.fromkeys(gate_blockers))
        ready = not unique_blockers
        rows.append(
            {
                "market": market,
                "portfolio_id": portfolio_id,
                "priority": str(trial.get("priority") or ""),
                "trial_type": str(trial.get("trial_type") or ""),
                "primary_field": str(trial.get("primary_field") or ""),
                "suggested_value": trial.get("suggested_value", ""),
                "suggested_value_unit": str(trial.get("suggested_value_unit") or ""),
                "candidate_symbols": str(trial.get("candidate_symbols") or ""),
                "status": "READY_FOR_OPERATOR_PAPER_TRIAL" if ready else "BLOCKED_BY_CURRENT_GATES",
                "primary_blocker": unique_blockers[0] if unique_blockers else "",
                "gate_blockers": unique_blockers,
                "current_readiness_status": readiness_status,
                "current_readiness_reason": str(readiness.get("market_readiness_reason") or ""),
                "current_submit_quality_status": str(readiness.get("submit_quality_status") or ""),
                "current_submit_quality_reason": str(readiness.get("submit_quality_reason") or ""),
                "current_order_count": _int(readiness.get("market_readiness_order_count")),
                "current_planned_buy_order_value": _float(readiness.get("market_readiness_planned_buy_order_value")),
                "paper_only": True,
                "auto_apply": False,
                "submit_orders": False,
                "does_not_relax_submit_gates": True,
            }
        )
    priority_rank = {"P1": 0, "P2": 1, "P3": 2}
    rows.sort(
        key=lambda row: (
            0 if str(row.get("status") or "") == "READY_FOR_OPERATOR_PAPER_TRIAL" else 1,
            priority_rank.get(str(row.get("priority") or ""), 9),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("trial_type") or ""),
        )
    )
    ready_rows = [row for row in rows if str(row.get("status") or "") == "READY_FOR_OPERATOR_PAPER_TRIAL"]
    primary = dict(rows[0]) if rows else {}
    if ready_rows:
        status = "READY_FOR_OPERATOR_PAPER_TRIAL"
        reason = "outcome_trial_gates_pass"
        primary_action = "review_and_run_one_paper_trial_without_relaxing_gates"
    elif rows:
        status = "BLOCKED_BY_CURRENT_GATES"
        reason = str(primary.get("primary_blocker") or "trial_gate_blocked")
        primary_action = "resolve_trial_gate_blockers_before_paper_trial"
    else:
        status = "NO_OUTCOME_TRIALS"
        reason = "no_ready_manual_trial_contracts"
        primary_action = "collect_more_outcome_evidence"
    return {
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "trial_count": int(len(rows)),
        "ready_trial_count": int(len(ready_rows)),
        "blocked_trial_count": int(len(rows) - len(ready_rows)),
        "primary_market": str(primary.get("market") or ""),
        "primary_portfolio_id": str(primary.get("portfolio_id") or ""),
        "primary_trial_type": str(primary.get("trial_type") or ""),
        "primary_blocker": str(primary.get("primary_blocker") or ""),
        "paper_only": True,
        "auto_apply": False,
        "submit_orders": False,
        "does_not_relax_submit_gates": True,
        "rows": rows[:20],
    }


def _blocked_review_label(row: Dict[str, Any]) -> str:
    return str(row.get("review_label") or "").strip().upper()


def _count_labels(rows: List[Dict[str, Any]], labels: set[str]) -> int:
    return sum(1 for row in rows if _blocked_review_label(row) in labels)


def _metric_or_count(metrics: Dict[str, Any], key: str, fallback: int) -> int:
    return _int(metrics.get(key)) if key in metrics else int(fallback)


def _status_count(rows: List[Dict[str, Any]], status: str) -> int:
    normalized = str(status or "").strip().upper()
    return sum(1 for row in rows if str(row.get("status") or "").strip().upper() == normalized)


def _followup_verdict_count(rows: List[Dict[str, Any]], verdict: str) -> int:
    normalized = str(verdict or "").strip().upper()
    return sum(1 for row in rows if str(row.get("followup_verdict") or "").strip().upper() == normalized)


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


def _with_dashboard_block_layout(block: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(block)
    block_id = str(enriched.get("id") or "")
    if block_id in ADVANCED_DASHBOARD_BLOCK_IDS:
        enriched["category"] = DASHBOARD_BLOCK_CATEGORY_ADVANCED
        enriched["advanced_only"] = True
    else:
        enriched["category"] = DASHBOARD_BLOCK_CATEGORY_HOME
        enriched["advanced_only"] = False
    return enriched


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
            "evidence_focus_action_count": _int(ops.get("evidence_focus_action_count")),
            "evidence_focus_urgent_count": _int(ops.get("evidence_focus_urgent_count")),
            "evidence_focus_primary_market": str(ops.get("evidence_focus_primary_market") or ""),
            "evidence_focus_primary_action": str(ops.get("evidence_focus_primary_action") or ""),
            "ibkr_gateway_budget_status": str(ops.get("ibkr_gateway_budget_status") or "ok"),
            "ibkr_gateway_budget_gateway_request_count": _int(ops.get("ibkr_gateway_budget_gateway_request_count")),
            "ibkr_gateway_budget_cache_hit_count": _int(ops.get("ibkr_gateway_budget_cache_hit_count")),
            "ibkr_gateway_budget_cache_hit_ratio": float(ops.get("ibkr_gateway_budget_cache_hit_ratio", 0.0) or 0.0),
            "ibkr_gateway_budget_max_usage_pct": float(ops.get("ibkr_gateway_budget_max_usage_pct", 0.0) or 0.0),
            "ibkr_gateway_budget_over_budget_market_count": _int(
                ops.get("ibkr_gateway_budget_over_budget_market_count")
            ),
            "ibkr_gateway_budget_stale_telemetry_market_count": _int(
                ops.get("ibkr_gateway_budget_stale_telemetry_market_count")
            ),
            "ibkr_gateway_budget_missing_telemetry_market_count": _int(
                ops.get("ibkr_gateway_budget_missing_telemetry_market_count")
            ),
            "auto_order_status": str(ops.get("auto_order_status") or ""),
            "auto_order_readiness_health_status": str(ops.get("auto_order_readiness_health_status") or "ready"),
            "auto_order_readiness_health_reason": str(ops.get("auto_order_readiness_health_reason") or ""),
            "auto_order_readiness_age_hours": ops.get("auto_order_readiness_age_hours"),
            "auto_order_blocked_count": _int(ops.get("auto_order_blocked_count")),
            "auto_order_ready_count": _int(ops.get("auto_order_ready_count")),
            "auto_order_primary_block_reason": str(ops.get("auto_order_primary_block_reason") or ""),
            "auto_order_submit_plan_status": str(ops.get("auto_order_submit_plan_status") or ""),
            "auto_order_submit_plan_reason": str(ops.get("auto_order_submit_plan_reason") or ""),
            "auto_order_submit_selected_portfolio_id": str(
                ops.get("auto_order_submit_selected_portfolio_id") or ""
            ),
            "auto_order_offline_recovery_required_count": _int(
                ops.get("auto_order_offline_recovery_required_count")
            ),
            "supervisor_shutdown_status": str(ops.get("supervisor_shutdown_status") or ""),
            "supervisor_shutdown_health_status": str(ops.get("supervisor_shutdown_health_status") or ""),
            "supervisor_shutdown_reason": str(ops.get("supervisor_shutdown_reason") or ""),
            "supervisor_shutdown_last_signal_name": str(ops.get("supervisor_shutdown_last_signal_name") or ""),
            "supervisor_shutdown_pid": _int(ops.get("supervisor_shutdown_pid")),
            "supervisor_shutdown_liveness_status": str(ops.get("supervisor_shutdown_liveness_status") or ""),
            "supervisor_code_revision_status": str(ops.get("supervisor_code_revision_status") or ""),
            "supervisor_code_revision": str(ops.get("supervisor_code_revision") or ""),
            "dashboard_code_revision": str(ops.get("dashboard_code_revision") or ""),
            "supervisor_shutdown_event_count": _int(ops.get("supervisor_shutdown_event_count")),
        },
        "rows": alert_rows,
    }


def build_auto_order_readiness_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    auto_order = _dict(payload.get("auto_order_readiness"))
    health = _dict(payload.get("auto_order_readiness_health"))
    ops = _dict(payload.get("ops_overview"))
    summary = _dict(auto_order.get("summary"))
    submit_plan = _dict(summary.get("submit_plan"))
    submit_capacity_plan = _dict(submit_plan.get("submit_capacity_plan"))
    frequency_plan = _auto_order_frequency_plan_with_watchlist_fallback(
        _dict(summary.get("frequency_plan")),
        payload,
    )
    rows = _rows(auto_order.get("rows"), limit=50)
    stale_execution_refresh_plan = _dict(summary.get("stale_execution_refresh_plan"))
    if not stale_execution_refresh_plan and rows:
        stale_execution_refresh_plan = build_stale_execution_refresh_plan(rows)
        stale_execution_refresh_plan["source"] = "dashboard_legacy_readiness_fallback"
    hard_block_counts = _dict(summary.get("hard_block_counts"))
    supervisor_code_status = str(ops.get("supervisor_code_revision_status") or "").strip().lower()
    supervisor_state = str(ops.get("supervisor_shutdown_status") or "").strip().lower()
    ops_revision_block_reason = ""
    if supervisor_state == "running" and supervisor_code_status in {"missing", "mismatch"}:
        ops_revision_block_reason = (
            "supervisor_code_revision_missing"
            if supervisor_code_status == "missing"
            else "supervisor_code_revision_mismatch"
        )
        hard_block_counts[ops_revision_block_reason] = max(
            _int(hard_block_counts.get(ops_revision_block_reason)),
            _int(summary.get("blocked_count")) or 1,
        )
    recovery_plan = _dict(summary.get("recovery_plan"))
    if ops_revision_block_reason and str(recovery_plan.get("status") or "") != "runtime_restart_required":
        recovery_plan = {}
    if not recovery_plan:
        recovery_plan = build_auto_order_recovery_plan(
            rows,
            submit_plan=submit_plan,
            stale_execution_refresh_plan=stale_execution_refresh_plan,
            global_hard_blocks=hard_block_counts,
        )
    recovery_eligibility = _dict(
        summary.get("recovery_eligibility")
    ) or evaluate_auto_order_recovery_eligibility(recovery_plan)
    if ops_revision_block_reason and str(recovery_plan.get("status") or "") == "runtime_restart_required":
        recovery_eligibility = evaluate_auto_order_recovery_eligibility(recovery_plan)
    execution_evidence_maintenance = _dict(
        auto_order.get("execution_evidence_maintenance")
    )
    opportunity_outcome_validation = _dict(payload.get("opportunity_outcome_validation"))
    opportunity_outcome_summary = _dict(opportunity_outcome_validation.get("summary"))
    calibration_suggestion_summary = _dict(
        opportunity_outcome_validation.get("calibration_suggestion_summary")
    )
    calibration_trial_plan_summary = _dict(
        opportunity_outcome_validation.get("calibration_trial_plan_summary")
    )
    calibration_suggestions = _rows(
        opportunity_outcome_validation.get("calibration_suggestions"),
        limit=20,
    )
    calibration_trial_plan = _rows(
        opportunity_outcome_validation.get("calibration_trial_plan"),
        limit=20,
    )
    outcome_trial_gate_plan = _build_outcome_trial_gate_plan(calibration_trial_plan, rows)
    remediation_plan = _rows(summary.get("remediation_plan"), limit=20)
    post_cost_rows = [
        {
            "market": str(row.get("market") or ""),
            "portfolio_id": str(row.get("portfolio_id") or ""),
            "status": str(row.get("post_cost_calibration_status") or ""),
            "reason": str(row.get("post_cost_calibration_reason") or ""),
            "primary_action": str(row.get("post_cost_primary_action") or ""),
            "candidate_count": _int(row.get("post_cost_candidate_count")),
            "high_cost_candidate_count": _int(row.get("post_cost_high_cost_candidate_count")),
            "positive_post_cost_edge_count": _int(row.get("post_cost_positive_edge_count")),
            "high_cost_positive_edge_count": _int(row.get("post_cost_high_cost_positive_edge_count")),
            "avg_expected_cost_bps": float(row.get("post_cost_avg_expected_cost_bps", 0.0) or 0.0),
            "avg_post_cost_edge_bps": float(row.get("post_cost_avg_post_cost_edge_bps", 0.0) or 0.0),
            "top_post_cost_symbols": str(row.get("post_cost_top_symbols") or ""),
        }
        for row in rows
        if str(row.get("post_cost_calibration_status") or "").strip()
    ]
    post_cost_review_statuses = {"COST_THRESHOLD_REVIEW", "COST_DRAG_DOMINANT", "EDGE_AFTER_COST_WEAK"}
    post_cost_review_rows = [
        row for row in post_cost_rows if str(row.get("status") or "").strip().upper() in post_cost_review_statuses
    ]
    wait_pullback_rows = [
        {
            "market": str(row.get("market") or ""),
            "portfolio_id": str(row.get("portfolio_id") or ""),
            "status": str(row.get("wait_pullback_calibration_status") or ""),
            "reason": str(row.get("wait_pullback_calibration_reason") or ""),
            "primary_action": str(row.get("wait_pullback_primary_action") or ""),
            "wait_pullback_count": _int(row.get("wait_pullback_count")),
            "close_wait_pullback_count": _int(row.get("wait_pullback_close_count")),
            "near_candidate_count": _int(row.get("wait_pullback_near_candidate_count")),
            "avg_entry_anchor_gap_pct": float(row.get("wait_pullback_avg_gap_pct", 0.0) or 0.0),
            "min_entry_anchor_gap_pct": float(row.get("wait_pullback_min_gap_pct", 0.0) or 0.0),
            "dominant_anchor_component": str(row.get("wait_pullback_dominant_anchor_component") or ""),
            "top_wait_symbols": str(row.get("wait_pullback_top_symbols") or ""),
        }
        for row in rows
        if str(row.get("wait_pullback_calibration_status") or "").strip()
    ]
    wait_pullback_review_statuses = {
        "REVIEW_NEAR_ENTRY",
        "REVIEW_ANCHOR",
        "MA_ANCHOR_CONSERVATIVE",
        "MISSING_ASSET_CLASS",
    }
    wait_pullback_review_rows = [
        row for row in wait_pullback_rows if str(row.get("status") or "").strip().upper() in wait_pullback_review_statuses
    ]
    frontier_candidates = _rows(submit_plan.get("frontier_candidates"), limit=20)
    top_frontier = dict(frontier_candidates[0]) if frontier_candidates else {}
    frontier_quality_pass_count = sum(
        1 for row in frontier_candidates if str(row.get("submit_quality_status") or "").strip().upper() == "PASS"
    )
    frontier_high_quality_count = sum(
        1 for row in frontier_candidates if str(row.get("submit_quality_tier") or "").strip().upper() == "HIGH"
    )
    submit_policy = _dict(submit_plan.get("policy"))
    submit_status = str(submit_plan.get("status") or "").strip().upper()
    summary_status = str(summary.get("status") or "").strip().lower()
    health_status = str(health.get("status") or "ready").strip().lower()
    health_reason = str(health.get("reason") or "").strip()
    health_summary = str(health.get("summary_text") or "").strip()
    if not auto_order:
        status = "warn"
        summary_text = "auto_order_readiness artifact missing"
    elif submit_status in {"READY_SINGLE_CANDIDATE", "READY_MULTI_CANDIDATE"} and bool(submit_plan.get("ready", False)):
        status = "ok"
        summary_text = str(summary.get("summary_text") or "safe paper submit candidate ready")
    elif submit_status == "DISABLED":
        status = "warn"
        summary_text = str(summary.get("summary_text") or "auto order readiness policy disabled")
    else:
        status = "fail" if summary_status in {"fail", "failed", "error", "degraded"} else "warn"
        summary_text = str(summary.get("summary_text") or f"submit_plan={submit_status or 'missing'}")
    if health_status == "degraded":
        status = "fail"
    elif health_status == "warning" and status == "ok":
        status = "warn"
    if health_status in {"warning", "degraded"} and health_summary:
        summary_text = f"{summary_text} | {health_summary}"
    return {
        "id": "auto_order_readiness",
        "title": "Auto Order Submit Gate",
        "status": status,
        "summary": (
            f"{summary_text} | submit_plan={submit_status or '-'} "
            f"reason={submit_plan.get('reason') or summary.get('primary_block_reason') or '-'}"
        ),
        "metrics": {
            "portfolio_count": _int(summary.get("portfolio_count")),
            "ready_count": _int(summary.get("ready_count")),
            "warning_count": _int(summary.get("warning_count")),
            "blocked_count": _int(summary.get("blocked_count")),
            "disabled_count": _int(summary.get("disabled_count")),
            "primary_block_reason": str(summary.get("primary_block_reason") or ""),
            "supervisor_code_revision_missing_count": _int(
                hard_block_counts.get("supervisor_code_revision_missing")
            ),
            "supervisor_code_revision_mismatch_count": _int(
                hard_block_counts.get("supervisor_code_revision_mismatch")
            ),
            "supervisor_revision_block_count": _int(
                hard_block_counts.get("supervisor_code_revision_missing")
            )
            + _int(hard_block_counts.get("supervisor_code_revision_mismatch")),
            "offline_recovery_required_count": _int(summary.get("offline_recovery_required_count")),
            "offline_recovery_markets": list(summary.get("offline_recovery_markets") or []),
            "offline_recovery_summary_text": str(summary.get("offline_recovery_summary_text") or ""),
            "readiness_health_status": health_status,
            "readiness_health_reason": health_reason,
            "readiness_health_summary_text": health_summary,
            "readiness_generated_at": str(health.get("generated_at") or ""),
            "readiness_age_hours": health.get("age_hours"),
            "readiness_max_age_hours": float(health.get("max_age_hours", 0.0) or 0.0),
            "gateway_budget_generated_at": str(health.get("gateway_budget_generated_at") or ""),
            "readiness_older_than_gateway_budget": int(bool(health.get("older_than_gateway_budget", False))),
            "readiness_secondary_reasons": list(health.get("secondary_reasons") or []),
            "submit_plan_status": submit_status,
            "submit_plan_ready": bool(submit_plan.get("ready", False)),
            "submit_plan_reason": str(submit_plan.get("reason") or ""),
            "candidate_supply_status": str(summary.get("candidate_supply_status") or frequency_plan.get("status") or ""),
            "candidate_supply_reason": str(summary.get("candidate_supply_reason") or frequency_plan.get("reason") or ""),
            "candidate_supply_primary_action": str(
                summary.get("candidate_supply_primary_action") or frequency_plan.get("primary_action") or ""
            ),
            "frequency_seed_proposal_count": _int(frequency_plan.get("seed_proposal_count")),
            "frequency_manual_seed_proposal_count": _int(frequency_plan.get("manual_seed_proposal_count")),
            "frequency_seed_proposal_markets": list(frequency_plan.get("seed_proposal_markets") or []),
            "frequency_seed_intake_plan_count": _int(frequency_plan.get("seed_intake_plan_count")),
            "frequency_seed_source_candidate_count": _int(frequency_plan.get("seed_source_candidate_count")),
            "frequency_seed_source_markets": list(frequency_plan.get("seed_source_markets") or []),
            "frequency_seed_intake_external_source_count": _int(
                frequency_plan.get("seed_intake_external_source_count")
            ),
            "frequency_seed_promotion_quality_rejected_count": _int(
                frequency_plan.get("seed_promotion_quality_rejected_count")
            ),
            "frequency_seed_promotion_primary_quality_reason": str(
                frequency_plan.get("seed_promotion_primary_quality_reason") or ""
            ),
            "frequency_seed_replacement_primary_action": str(
                frequency_plan.get("seed_replacement_primary_action") or ""
            ),
            "frequency_seed_promotion_quality_reason_counts": _dict(
                frequency_plan.get("seed_promotion_quality_reason_counts")
            ),
            "frequency_seed_evidence_queue_count": _int(frequency_plan.get("seed_evidence_queue_count")),
            "frequency_seed_evidence_ready_job_count": _int(
                frequency_plan.get("seed_evidence_ready_job_count")
            ),
            "frequency_seed_evidence_primary_market": str(
                frequency_plan.get("seed_evidence_primary_market") or ""
            ),
            "frequency_seed_evidence_primary_symbols": ",".join(
                str(symbol)
                for symbol in list(frequency_plan.get("seed_evidence_primary_symbols") or [])
                if str(symbol).strip()
            ),
            "frequency_seed_evidence_mode": str(frequency_plan.get("seed_evidence_mode") or ""),
            "frequency_plan_does_not_change_submit_decision": int(
                bool(frequency_plan.get("does_not_change_submit_decision", False))
            ),
            "stale_execution_refresh_status": str(stale_execution_refresh_plan.get("status") or ""),
            "stale_execution_refresh_target_count": _int(stale_execution_refresh_plan.get("target_count")),
            "stale_execution_refresh_primary_market": str(stale_execution_refresh_plan.get("primary_market") or ""),
            "stale_execution_refresh_primary_portfolio_id": str(
                stale_execution_refresh_plan.get("primary_portfolio_id") or ""
            ),
            "stale_execution_refresh_primary_score": float(
                stale_execution_refresh_plan.get("primary_score", 0.0) or 0.0
            ),
            "stale_execution_refresh_submit_orders": int(
                bool(stale_execution_refresh_plan.get("submit_orders", False))
            ),
            "submit_capacity_status": str(submit_capacity_plan.get("status") or ""),
            "submit_capacity_reason": str(submit_capacity_plan.get("reason") or ""),
            "submit_capacity_scale_allowed": int(bool(submit_capacity_plan.get("scale_allowed", False))),
            "submit_capacity_scale_stage": str(submit_capacity_plan.get("scale_stage") or ""),
            "submit_capacity_fill_count": _int(submit_capacity_plan.get("fill_count")),
            "submit_capacity_matured_5d_sample_count": _int(
                submit_capacity_plan.get("matured_5d_sample_count")
            ),
            "submit_capacity_evidence_market_count": _int(
                submit_capacity_plan.get("evidence_market_count")
            ),
            "submit_capacity_effective_max_portfolios": _int(
                submit_capacity_plan.get("effective_max_submit_portfolios_per_run")
            ),
            "submit_capacity_effective_max_total_gross": float(
                submit_capacity_plan.get("effective_max_submit_total_gross_order_value", 0.0) or 0.0
            ),
            "recovery_plan_status": str(recovery_plan.get("status") or ""),
            "recovery_plan_primary_action": str(recovery_plan.get("primary_action") or ""),
            "recovery_plan_target_market": str(recovery_plan.get("target_market") or ""),
            "recovery_plan_target_portfolio_id": str(recovery_plan.get("target_portfolio_id") or ""),
            "recovery_plan_target_symbols": str(recovery_plan.get("target_symbols") or ""),
            "recovery_plan_step_count": _int(recovery_plan.get("step_count")),
            "recovery_plan_gateway_refresh_portfolio_limit": _int(
                recovery_plan.get("gateway_refresh_portfolio_limit")
            ),
            "recovery_plan_does_not_submit_orders": int(
                bool(recovery_plan.get("does_not_submit_orders", False))
            ),
            "recovery_eligibility_active": int(bool(recovery_eligibility.get("active", False))),
            "recovery_eligibility_eligible": int(bool(recovery_eligibility.get("eligible", False))),
            "recovery_eligibility_reason": str(recovery_eligibility.get("reason") or ""),
            "execution_evidence_maintenance_status": str(
                execution_evidence_maintenance.get("status")
                or summary.get("execution_evidence_maintenance_status")
                or ""
            ),
            "execution_evidence_maintenance_reason": str(
                execution_evidence_maintenance.get("reason")
                or summary.get("execution_evidence_maintenance_reason")
                or ""
            ),
            "execution_evidence_maintenance_target_market": str(
                execution_evidence_maintenance.get("target_market")
                or summary.get("execution_evidence_maintenance_target_market")
                or ""
            ),
            "execution_evidence_maintenance_target_portfolio_id": str(
                execution_evidence_maintenance.get("target_portfolio_id")
                or summary.get("execution_evidence_maintenance_target_portfolio_id")
                or ""
            ),
            "execution_evidence_maintenance_submit_orders": int(
                bool(execution_evidence_maintenance.get("submit_orders", False))
            ),
            "post_cost_calibration_portfolio_count": len(post_cost_rows),
            "post_cost_review_portfolio_count": len(post_cost_review_rows),
            "post_cost_high_cost_candidate_count": sum(_int(row.get("high_cost_candidate_count")) for row in post_cost_rows),
            "post_cost_positive_edge_candidate_count": sum(
                _int(row.get("positive_post_cost_edge_count")) for row in post_cost_rows
            ),
            "post_cost_primary_status": (
                str(post_cost_review_rows[0].get("status") or "")
                if post_cost_review_rows
                else str(post_cost_rows[0].get("status") or "")
                if post_cost_rows
                else ""
            ),
            "post_cost_primary_action": (
                str(post_cost_review_rows[0].get("primary_action") or "")
                if post_cost_review_rows
                else str(post_cost_rows[0].get("primary_action") or "")
                if post_cost_rows
                else ""
            ),
            "wait_pullback_calibration_portfolio_count": len(wait_pullback_rows),
            "wait_pullback_review_portfolio_count": len(wait_pullback_review_rows),
            "wait_pullback_review_wait_count": sum(_int(row.get("close_wait_pullback_count")) for row in wait_pullback_rows),
            "wait_pullback_near_candidate_count": sum(_int(row.get("near_candidate_count")) for row in wait_pullback_rows),
            "wait_pullback_primary_status": (
                str(wait_pullback_review_rows[0].get("status") or "")
                if wait_pullback_review_rows
                else str(wait_pullback_rows[0].get("status") or "")
                if wait_pullback_rows
                else ""
            ),
            "wait_pullback_primary_action": (
                str(wait_pullback_review_rows[0].get("primary_action") or "")
                if wait_pullback_review_rows
                else str(wait_pullback_rows[0].get("primary_action") or "")
                if wait_pullback_rows
                else ""
            ),
            "opportunity_outcome_validation_count": _int(
                opportunity_outcome_summary.get("validation_count")
            ),
            "opportunity_outcome_matched_symbol_count": _int(
                opportunity_outcome_summary.get("matched_symbol_count")
            ),
            "opportunity_outcome_matured_5d_sample_count": _int(
                opportunity_outcome_summary.get("matured_5d_sample_count")
            ),
            "opportunity_outcome_matured_20d_sample_count": _int(
                opportunity_outcome_summary.get("matured_20d_sample_count")
            ),
            "opportunity_calibration_suggestion_count": _int(
                calibration_suggestion_summary.get("suggestion_count")
            ),
            "opportunity_calibration_p1_suggestion_count": _int(
                _dict(calibration_suggestion_summary.get("priority_counts")).get("P1")
            ),
            "opportunity_calibration_wait_pullback_anchor_review_count": _int(
                _dict(calibration_suggestion_summary.get("type_counts")).get("WAIT_PULLBACK_ANCHOR_REVIEW")
            ),
            "opportunity_calibration_hk_post_cost_review_count": _int(
                _dict(calibration_suggestion_summary.get("type_counts")).get("HK_POST_COST_THRESHOLD_REVIEW")
            ),
            "opportunity_calibration_trial_count": _int(
                calibration_trial_plan_summary.get("trial_count")
            ),
            "opportunity_calibration_trial_ready_count": _int(
                calibration_trial_plan_summary.get("ready_for_manual_review_count")
            ),
            "opportunity_calibration_trial_p1_ready_count": _int(
                calibration_trial_plan_summary.get("p1_ready_for_manual_review_count")
            ),
            "opportunity_calibration_trial_auto_apply_count": sum(
                1 for row in calibration_trial_plan if bool(row.get("auto_apply", False))
            ),
            "outcome_trial_gate_status": str(outcome_trial_gate_plan.get("status") or ""),
            "outcome_trial_gate_trial_count": _int(outcome_trial_gate_plan.get("trial_count")),
            "outcome_trial_gate_ready_count": _int(outcome_trial_gate_plan.get("ready_trial_count")),
            "outcome_trial_gate_blocked_count": _int(outcome_trial_gate_plan.get("blocked_trial_count")),
            "outcome_trial_gate_primary_market": str(outcome_trial_gate_plan.get("primary_market") or ""),
            "outcome_trial_gate_primary_portfolio_id": str(
                outcome_trial_gate_plan.get("primary_portfolio_id") or ""
            ),
            "outcome_trial_gate_primary_trial_type": str(outcome_trial_gate_plan.get("primary_trial_type") or ""),
            "outcome_trial_gate_primary_blocker": str(outcome_trial_gate_plan.get("primary_blocker") or ""),
            "outcome_trial_gate_submit_orders": int(bool(outcome_trial_gate_plan.get("submit_orders", False))),
            "candidate_count": _int(submit_plan.get("candidate_count")),
            "frontier_candidate_count": _int(submit_plan.get("frontier_candidate_count"))
            or len(frontier_candidates),
            "frontier_quality_pass_count": int(frontier_quality_pass_count),
            "frontier_high_quality_count": int(frontier_high_quality_count),
            "frontier_top_submit_quality_status": str(top_frontier.get("submit_quality_status") or ""),
            "frontier_top_submit_quality_tier": str(top_frontier.get("submit_quality_tier") or ""),
            "frontier_top_submit_quality_min_net_edge_bps": float(
                top_frontier.get("submit_quality_min_net_edge_bps", 0.0) or 0.0
            ),
            "frontier_top_submit_quality_min_edge_margin_bps": float(
                top_frontier.get("submit_quality_min_edge_margin_bps", 0.0) or 0.0
            ),
            "account_growth_profile": str(submit_policy.get("account_growth_profile") or ""),
            "account_growth_primary_action": str(submit_policy.get("account_growth_primary_action") or ""),
            "account_growth_submit_frequency_mode": str(
                submit_policy.get("account_growth_submit_frequency_mode") or ""
            ),
            "account_growth_max_orders_per_run": _int(
                submit_policy.get("account_growth_max_orders_per_run")
            ),
            "account_growth_max_order_value": float(
                submit_policy.get("account_growth_max_order_value", 0.0) or 0.0
            ),
            "selected_market": str(submit_plan.get("selected_market") or ""),
            "selected_markets": list(submit_plan.get("selected_markets") or []),
            "selected_portfolio_id": str(submit_plan.get("selected_portfolio_id") or ""),
            "selected_portfolio_ids": list(submit_plan.get("selected_portfolio_ids") or []),
            "selected_order_count": _int(submit_plan.get("selected_order_count")),
            "selected_total_order_count": _int(submit_plan.get("selected_total_order_count")),
            "selected_planned_gross_order_value": float(
                submit_plan.get("selected_planned_gross_order_value", 0.0) or 0.0
            ),
            "selected_total_planned_gross_order_value": float(
                submit_plan.get("selected_total_planned_gross_order_value", 0.0) or 0.0
            ),
            "selected_planned_order_symbols": str(submit_plan.get("selected_planned_order_symbols") or ""),
            "rejected_candidate_count": len(_rows(submit_plan.get("rejected_candidates"), limit=100)),
        },
        "rows": {
            "submit_plan": submit_plan,
            "submit_capacity_plan": submit_capacity_plan,
            "remediation_plan": remediation_plan,
            "frequency_plan": frequency_plan,
            "stale_execution_refresh_plan": stale_execution_refresh_plan,
            "recovery_plan": recovery_plan,
            "recovery_eligibility": recovery_eligibility,
            "execution_evidence_maintenance": execution_evidence_maintenance,
            "opportunity_outcome_validation": opportunity_outcome_validation,
            "opportunity_calibration_suggestions": calibration_suggestions,
            "opportunity_calibration_trial_plan": calibration_trial_plan,
            "outcome_trial_gate_plan": outcome_trial_gate_plan,
            "post_cost_calibration": post_cost_rows,
            "wait_pullback_calibration": wait_pullback_rows,
            "frontier_candidates": frontier_candidates,
            "portfolios": rows,
        },
    }


def _auto_order_frequency_plan_with_watchlist_fallback(
    frequency_plan: Dict[str, Any],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """Backfill display-only seed metrics for legacy readiness artifacts."""
    merged = dict(frequency_plan or {})
    expansion = _dict(payload.get("watchlist_expansion_summary"))
    seed_proposals = _rows(expansion.get("seed_proposals"), limit=50)
    seed_intake_plan = _rows(expansion.get("seed_intake_plan"), limit=50)
    seed_evidence_queue = _rows(expansion.get("seed_evidence_queue"), limit=20)
    seed_promotion_review = _rows(expansion.get("seed_promotion_review"), limit=50)
    if not seed_proposals and not seed_intake_plan and not seed_evidence_queue and not seed_promotion_review:
        return merged
    ready_seed_jobs = [
        row for row in seed_evidence_queue if str(row.get("status") or "").strip().upper() == "READY"
    ]
    primary_seed_job = dict(ready_seed_jobs[0]) if ready_seed_jobs else {}
    has_seed_source_metrics = any(
        key in merged
        for key in (
            "seed_intake_plan_count",
            "seed_source_candidate_count",
            "seed_intake_external_source_count",
        )
    )
    if not has_seed_source_metrics:
        merged.update(
            {
                "seed_proposal_count": len(seed_proposals),
                "manual_seed_proposal_count": sum(1 for row in seed_proposals if not bool(row.get("auto_apply"))),
                "seed_proposal_markets": [
                    str(row.get("market") or "")
                    for row in seed_proposals
                    if str(row.get("market") or "").strip()
                ],
                "seed_intake_plan_count": len(seed_intake_plan),
                "seed_source_candidate_count": sum(
                    _int(row.get("source_candidate_count"))
                    for row in seed_intake_plan
                ),
                "seed_source_markets": [
                    str(row.get("market") or "")
                    for row in seed_intake_plan
                    if _int(row.get("source_candidate_count")) > 0 and str(row.get("market") or "").strip()
                ],
                "seed_intake_external_source_count": sum(
                    1
                    for row in seed_intake_plan
                    if str(row.get("intake_status") or "") == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
                ),
            }
        )
    if "seed_promotion_quality_rejected_count" not in merged:
        quality_feedback = _dict(expansion.get("seed_quality_feedback")) or summarize_seed_promotion_quality(
            seed_promotion_review
        )
        merged["seed_quality_feedback"] = quality_feedback
        merged["seed_promotion_quality_rejected_count"] = _int(quality_feedback.get("quality_rejected_count"))
        merged["seed_promotion_quality_reason_counts"] = _dict(quality_feedback.get("quality_reason_counts"))
        merged["seed_promotion_primary_quality_reason"] = str(
            quality_feedback.get("primary_quality_reason") or ""
        )
        merged["seed_replacement_primary_action"] = str(quality_feedback.get("primary_action") or "")
    if "seed_evidence_queue_count" not in merged:
        merged["seed_evidence_queue_count"] = len(seed_evidence_queue)
    if "seed_evidence_ready_job_count" not in merged:
        merged["seed_evidence_ready_job_count"] = len(ready_seed_jobs)
    if "seed_evidence_primary_market" not in merged:
        merged["seed_evidence_primary_market"] = str(
            expansion.get("seed_evidence_primary_market")
            or primary_seed_job.get("market")
            or ""
        )
    if "seed_evidence_primary_symbols" not in merged:
        merged["seed_evidence_primary_symbols"] = list(
            expansion.get("seed_evidence_primary_symbols")
            or primary_seed_job.get("symbols")
            or []
        )
    if "seed_evidence_mode" not in merged:
        merged["seed_evidence_mode"] = str(
            expansion.get("seed_evidence_mode")
            or primary_seed_job.get("evidence_mode")
            or primary_seed_job.get("mode")
            or ""
        )
    merged["does_not_change_submit_decision"] = bool(
        merged.get("does_not_change_submit_decision", True)
    )
    return merged


def build_open_market_analysis_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = _dict(payload.get("open_market_analysis_summary"))
    rows = _rows(summary.get("rows"), limit=50)
    market_rows = _rows(summary.get("market_rows"), limit=20)
    status = str(summary.get("status") or "").strip().lower() or "warn"
    if not summary:
        status = "warn"
    return {
        "id": "open_market_analysis",
        "title": "Open Market Trading Analysis",
        "status": "fail" if status == "degraded" else "warn" if status == "warning" else "ok",
        "summary": str(summary.get("summary_text") or "open_market_analysis missing"),
        "metrics": {
            "open_market_count": _int(summary.get("open_market_count")),
            "open_portfolio_count": _int(summary.get("open_portfolio_count")),
            "fresh_open_report_count": _int(summary.get("fresh_open_report_count")),
            "stale_open_report_count": _int(summary.get("stale_open_report_count")),
            "actionable_open_count": _int(summary.get("actionable_open_count")),
            "submit_enabled_open_count": _int(summary.get("submit_enabled_open_count")),
            "auto_order_artifact_present": int(bool(summary.get("auto_order_artifact_present", False))),
            "auto_ready_open_count": _int(summary.get("auto_ready_open_count")),
            "auto_blocked_open_count": _int(summary.get("auto_blocked_open_count")),
            "auto_missing_open_count": _int(summary.get("auto_missing_open_count")),
            "data_attention_open_count": _int(summary.get("data_attention_open_count")),
            "missing_market_state_count": _int(summary.get("missing_market_state_count")),
            "primary_reason": str(summary.get("primary_reason") or ""),
        },
        "rows": {
            "market_rows": market_rows,
            "open_portfolios": rows,
            "primary_reason_counts": _dict(summary.get("primary_reason_counts")),
        },
    }


def build_control_actions_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    control = _dict(payload.get("dashboard_control"))
    service = _dict(control.get("service"))
    actions = _dict(control.get("actions"))
    raw_history = _rows(actions.get("action_history"), limit=50)
    history = list(reversed(raw_history))[:20]
    failed_count = sum(1 for row in history if str(row.get("status") or "").lower() == "failed")
    error_summary = summarize_error_classes(history)
    link_summary = _dict(actions.get("evidence_action_link_summary"))
    if not link_summary:
        link_summary = summarize_evidence_action_audit_links(raw_history)
    return {
        "id": "dashboard_control_actions",
        "title": "Governance / Control Actions",
        "status": "fail" if failed_count else str(service.get("status") or "disabled"),
        "summary": (
            f"service={service.get('status') or 'disabled'} "
            f"last_action={actions.get('last_action') or '-'} "
            f"failed_recent={failed_count} "
            f"primary_error={error_summary.get('primary_error_class') or 'none'} "
            f"linked_actions={_int(link_summary.get('linked_action_history_count'))} "
            f"linked_strategy_params={_int(link_summary.get('linked_strategy_parameter_suggestion_history_count'))} "
            f"last_resolution={link_summary.get('last_resolution_status') or '-'}"
        ),
        "metrics": {
            "history_count": len(history),
            "failed_count": failed_count,
            "linked_action_history_count": _int(link_summary.get("linked_action_history_count")),
            "last_linked_evidence_action_id": str(link_summary.get("last_linked_evidence_action_id") or ""),
            "last_resolution_status": str(link_summary.get("last_resolution_status") or ""),
            "linked_strategy_parameter_suggestion_history_count": _int(
                link_summary.get("linked_strategy_parameter_suggestion_history_count")
            ),
            "last_linked_strategy_parameter_suggestion_id": str(
                link_summary.get("last_linked_strategy_parameter_suggestion_id") or ""
            ),
            "last_linked_strategy_parameter_field": str(
                link_summary.get("last_linked_strategy_parameter_field") or ""
            ),
            "last_strategy_parameter_resolution_status": str(
                link_summary.get("last_strategy_parameter_resolution_status") or ""
            ),
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


def build_dashboard_control_action_history_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    control = _dict(payload.get("dashboard_control"))
    actions = _dict(control.get("actions"))
    raw_history = _rows(actions.get("action_history"), limit=100)
    history = list(reversed(raw_history))[:50]
    failed_count = sum(1 for row in history if str(row.get("status") or "").lower() == "failed")
    error_summary = summarize_error_classes(history)
    link_summary = _dict(actions.get("evidence_action_link_summary"))
    if not link_summary:
        link_summary = summarize_evidence_action_audit_links(raw_history)
    return {
        "id": "dashboard_control_action_history",
        "title": "Dashboard Control Action History",
        "status": "fail" if failed_count else "ok",
        "summary": (
            f"history={len(history)} failed={failed_count} "
            f"linked_actions={_int(link_summary.get('linked_action_history_count'))} "
            f"linked_strategy_params={_int(link_summary.get('linked_strategy_parameter_suggestion_history_count'))} "
            f"primary_error={error_summary.get('primary_error_class') or 'none'}"
        ),
        "metrics": {
            "history_count": len(history),
            "raw_history_count": len(raw_history),
            "failed_count": failed_count,
            "linked_action_history_count": _int(link_summary.get("linked_action_history_count")),
            "last_linked_evidence_action_id": str(link_summary.get("last_linked_evidence_action_id") or ""),
            "last_resolution_status": str(link_summary.get("last_resolution_status") or ""),
            "linked_strategy_parameter_suggestion_history_count": _int(
                link_summary.get("linked_strategy_parameter_suggestion_history_count")
            ),
            "last_linked_strategy_parameter_suggestion_id": str(
                link_summary.get("last_linked_strategy_parameter_suggestion_id") or ""
            ),
            "last_linked_strategy_parameter_field": str(
                link_summary.get("last_linked_strategy_parameter_field") or ""
            ),
            "last_strategy_parameter_resolution_status": str(
                link_summary.get("last_strategy_parameter_resolution_status") or ""
            ),
            "retryable_error_count": _int(error_summary.get("retryable_count")),
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


def build_watchlist_expansion_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    summary = _dict(payload.get("watchlist_expansion_summary"))
    market_rows = _rows(summary.get("markets"), limit=20)
    candidate_rows = _rows(summary.get("candidate_rows"), limit=100)
    selected_candidates = [row for row in candidate_rows if str(row.get("selection_status") or "").upper() == "SELECTED"]
    policy = WatchlistExpansionPolicy.from_mapping(_dict(summary.get("policy")))
    fallback_summary = (
        summarize_watchlist_expansion(candidate_rows, market_rows=market_rows, policy=policy)
        if (candidate_rows or market_rows)
        else {}
    )
    reason_summary = (
        _rows(summary.get("reason_summary"), limit=20)
        or _rows(fallback_summary.get("reason_summary"), limit=20)
        or selection_reason_summary(candidate_rows)[:20]
    )
    market_recommendations = _rows(summary.get("market_recommendations"), limit=20) or _rows(
        fallback_summary.get("market_recommendations"),
        limit=20,
    )
    seed_proposals = _rows(summary.get("seed_proposals"), limit=20) or _rows(fallback_summary.get("seed_proposals"), limit=20)
    seed_intake_plan = _rows(summary.get("seed_intake_plan"), limit=20) or _rows(
        fallback_summary.get("seed_intake_plan"),
        limit=20,
    )
    seed_promotion_review = _rows(summary.get("seed_promotion_review"), limit=50) or _rows(
        fallback_summary.get("seed_promotion_review"),
        limit=50,
    )
    seed_evidence_queue = _rows(summary.get("seed_evidence_queue"), limit=20) or _rows(
        fallback_summary.get("seed_evidence_queue"),
        limit=20,
    )
    account_growth_tier_plan = _dict(summary.get("account_growth_tier_plan")) or _dict(
        fallback_summary.get("account_growth_tier_plan")
    )
    seed_quality_feedback = _dict(summary.get("seed_quality_feedback")) or summarize_seed_promotion_quality(
        seed_promotion_review
    )
    primary_market = str(summary.get("primary_recommendation_market") or fallback_summary.get("primary_recommendation_market") or "")
    primary_reason = str(summary.get("primary_recommendation_reason") or fallback_summary.get("primary_recommendation_reason") or "")
    primary_action = str(summary.get("primary_recommendation_action") or fallback_summary.get("primary_recommendation_action") or "")
    selected_count = _int(summary.get("selected_count"))
    if selected_count <= 0 and market_rows:
        selected_count = sum(_int(row.get("selected_count")) for row in market_rows)
    candidate_count = _int(summary.get("candidate_row_count"))
    if candidate_count <= 0 and market_rows:
        candidate_count = sum(_int(row.get("candidate_row_count")) for row in market_rows)
    zero_selected_market_count = _int(summary.get("zero_selected_market_count"))
    if zero_selected_market_count <= 0 and market_rows:
        zero_selected_market_count = sum(
            1
            for row in market_rows
            if _int(row.get("candidate_row_count")) > 0 and _int(row.get("selected_count")) <= 0
        )
    seed_intake_external_source_count = sum(
        1
        for row in seed_intake_plan
        if str(row.get("intake_status") or "") == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
    )
    seed_intake_manual_review_count = sum(
        1 for row in seed_intake_plan if str(row.get("intake_status") or "") == "MANUAL_REVIEW_REQUIRED"
    )
    seed_source_candidate_count = sum(int(row.get("source_candidate_count", 0) or 0) for row in seed_intake_plan)
    seed_source_market_count = sum(1 for row in seed_intake_plan if int(row.get("source_candidate_count", 0) or 0) > 0)
    status_raw = str(summary.get("status") or "").strip().lower()
    if not summary:
        status = "warn"
    elif status_raw == "degraded":
        status = "fail"
    elif (
        status_raw == "warning"
        or (candidate_count > 0 and selected_count <= 0)
        or zero_selected_market_count > 0
        or seed_intake_external_source_count > 0
    ):
        status = "warn"
    else:
        status = "ok"
    account_profile = _dict(summary.get("account_profile"))
    return {
        "id": "watchlist_expansion",
        "title": "Watchlist Expansion Quality",
        "status": status,
        "summary": str(
            summary.get("summary_text")
            or f"markets={len(market_rows)} candidates={candidate_count} selected={selected_count}"
        ),
        "metrics": {
            "market_count": len(market_rows),
            "candidate_row_count": candidate_count,
            "selected_count": selected_count,
            "zero_selected_market_count": zero_selected_market_count,
            "rejected_count": _int(summary.get("rejected_count") or fallback_summary.get("rejected_count")),
            "stale": int(str(summary.get("reason") or "") == "stale_watchlist_expansion"),
            "age_hours": summary.get("age_hours"),
            "max_age_hours": float(summary.get("max_age_hours", 0.0) or 0.0),
            "account_profile": str(account_profile.get("name") or ""),
            "account_equity": float(account_profile.get("account_equity", 0.0) or 0.0),
            "top_reject_reason": str(reason_summary[0].get("reason") if reason_summary else ""),
            "primary_recommendation_market": primary_market,
            "primary_recommendation_reason": primary_reason,
            "primary_recommendation_action": primary_action,
            "primary_expansion_target": str(market_recommendations[0].get("expansion_target") if market_recommendations else ""),
            "preferred_asset_class_gap_count": sum(1 for row in market_recommendations if bool(row.get("preferred_asset_class_gap"))),
            "seed_proposal_count": len(seed_proposals),
            "manual_seed_proposal_count": sum(1 for row in seed_proposals if not bool(row.get("auto_apply"))),
            "primary_seed_proposal_action": str(seed_proposals[0].get("proposal_action") if seed_proposals else ""),
            "seed_intake_plan_count": len(seed_intake_plan),
            "seed_intake_external_source_count": seed_intake_external_source_count,
            "seed_intake_manual_review_count": seed_intake_manual_review_count,
            "seed_source_candidate_count": seed_source_candidate_count,
            "seed_source_market_count": seed_source_market_count,
            "seed_promotion_review_count": len(seed_promotion_review),
            "seed_promotion_ready_count": sum(
                1
                for row in seed_promotion_review
                if str(row.get("promotion_status") or "") == "PROMOTION_REVIEW_READY"
            ),
            "seed_promotion_mapping_required_count": sum(
                1
                for row in seed_promotion_review
                if str(row.get("promotion_status") or "") == "BROKER_MAPPING_REQUIRED"
            ),
            "seed_promotion_candidate_report_required_count": sum(
                1
                for row in seed_promotion_review
                if str(row.get("promotion_status") or "") == "CANDIDATE_REPORT_REQUIRED"
            ),
            "seed_promotion_quality_rejected_count": sum(
                1
                for row in seed_promotion_review
                if str(row.get("promotion_status") or "") == "QUALITY_REJECTED"
            ),
            "seed_promotion_primary_quality_reason": str(
                seed_quality_feedback.get("primary_quality_reason") or ""
            ),
            "seed_replacement_primary_action": str(seed_quality_feedback.get("primary_action") or ""),
            "seed_quality_feedback_status": str(seed_quality_feedback.get("status") or ""),
            "seed_evidence_queue_count": len(seed_evidence_queue),
            "seed_evidence_ready_job_count": sum(
                1
                for row in seed_evidence_queue
                if str(row.get("status") or "") == "READY"
            ),
            "seed_evidence_primary_market": str(
                summary.get("seed_evidence_primary_market")
                or fallback_summary.get("seed_evidence_primary_market")
                or ""
            ),
            "seed_evidence_primary_symbols": ",".join(
                str(symbol)
                for symbol in list(
                    summary.get("seed_evidence_primary_symbols")
                    or fallback_summary.get("seed_evidence_primary_symbols")
                    or []
                )
            ),
            "seed_evidence_mode": str(
                summary.get("seed_evidence_mode")
                or fallback_summary.get("seed_evidence_mode")
                or ""
            ),
            "primary_seed_intake_status": str(seed_intake_plan[0].get("intake_status") if seed_intake_plan else ""),
            "primary_seed_intake_next_action": str(seed_intake_plan[0].get("next_action") if seed_intake_plan else ""),
            "account_growth_profile": str(account_growth_tier_plan.get("profile") or ""),
            "account_growth_primary_action": str(account_growth_tier_plan.get("primary_action") or ""),
            "account_growth_expansion_mode": str(account_growth_tier_plan.get("expansion_mode") or ""),
            "account_growth_submit_frequency_mode": str(account_growth_tier_plan.get("submit_frequency_mode") or ""),
            "account_growth_max_orders_per_run": _int(account_growth_tier_plan.get("max_orders_per_run")),
            "account_growth_max_order_value": float(account_growth_tier_plan.get("max_order_value", 0.0) or 0.0),
            "selected_symbols": ",".join(
                str(row.get("symbol") or "").strip()
                for row in selected_candidates[:10]
                if str(row.get("symbol") or "").strip()
            ),
        },
        "rows": {
            "markets": market_rows,
            "selected_candidates": selected_candidates[:20],
            "reason_summary": reason_summary,
            "market_recommendations": market_recommendations,
            "seed_proposals": seed_proposals,
            "seed_intake_plan": seed_intake_plan,
            "seed_promotion_review": seed_promotion_review,
            "seed_evidence_queue": seed_evidence_queue,
            "account_growth_tier_plan": account_growth_tier_plan,
            "seed_quality_feedback": seed_quality_feedback,
            "policy": _dict(summary.get("policy")),
        },
    }


def build_evidence_focus_actions_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _rows(payload.get("evidence_focus_actions"), limit=20)
    summary = _dict(payload.get("evidence_focus_summary"))
    primary = rows[0] if rows else {}
    gate_review_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "review_gate_thresholds")
    signal_review_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "review_signal_expected_edge")
    missing_evidence_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "build_weekly_unified_evidence")
    hold_review_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "hold_parameters_collect_more_evidence")
    sample_collection_count = sum(1 for row in rows if str(row.get("primary_action") or "") == "collect_more_outcome_samples")
    urgent_count = sum(1 for row in rows if _int(row.get("priority_order")) < 60)
    open_urgent_count = _int(summary.get("open_urgent_action_count", urgent_count))
    primary_market = str(summary.get("primary_market") or primary.get("market") or "")
    primary_action = str(summary.get("primary_action") or primary.get("primary_action") or "")
    primary_action_label = str(summary.get("primary_action_label") or primary.get("action") or "")
    primary_basis = str(summary.get("primary_basis") or primary.get("basis") or "")
    primary_detail = str(summary.get("primary_detail") or primary.get("detail") or "")
    summary_text = str(summary.get("summary_text") or "").strip() or (
        f"actions={len(rows)} urgent={urgent_count} "
        f"gate={gate_review_count} signal={signal_review_count} "
        f"missing_evidence={missing_evidence_count} sample_collection={sample_collection_count}"
    )
    status = str(summary.get("status") or "").strip().lower() or ("warn" if urgent_count else "ok")
    return {
        "id": "evidence_focus_actions",
        "title": "Evidence Focus Actions",
        "status": status,
        "summary": summary_text,
        "metrics": {
            "primary_market": primary_market,
            "primary_action": primary_action,
            "primary_action_label": primary_action_label,
            "primary_basis": primary_basis,
            "focus_action_count": len(rows),
            "urgent_action_count": urgent_count,
            "open_urgent_action_count": open_urgent_count,
            "gate_review_count": gate_review_count,
            "signal_review_count": signal_review_count,
            "missing_evidence_count": missing_evidence_count,
            "hold_review_count": hold_review_count,
            "sample_collection_count": sample_collection_count,
            "read_only": bool(summary.get("read_only", True)),
        },
        "rows": {
            "summary": {
                "primary_market": primary_market,
                "primary_action": primary_action,
                "primary_action_label": primary_action_label,
                "primary_basis": primary_basis,
                "primary_detail": primary_detail,
                "summary_text": summary_text,
                "read_only": bool(summary.get("read_only", True)),
            },
            "actions": rows,
        },
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
        "title": "Execution Quality",
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


def build_weekly_attribution_waterfall_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _rows(payload.get("weekly_attribution_waterfall"), limit=50)
    component_count = len({str(row.get("component") or "").strip() for row in rows if row.get("component")})
    market_count = len({str(row.get("market") or "").strip() for row in rows if row.get("market")})
    return {
        "id": "weekly_attribution_waterfall",
        "title": "Weekly Attribution Waterfall",
        "status": "ok",
        "summary": f"rows={len(rows)} components={component_count} markets={market_count}",
        "metrics": {
            "row_count": len(rows),
            "component_count": component_count,
            "market_count": market_count,
        },
        "rows": rows,
    }


def build_walk_forward_acceptance_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    acceptance = _dict(payload.get("walk_forward_acceptance"))
    stability = _dict(payload.get("walk_forward_market_stability"))
    acceptance_rows = _rows(acceptance.get("rows"), limit=50)
    stability_rows = _rows(stability.get("rows"), limit=50)
    recommend_count = sum(1 for row in acceptance_rows if str(row.get("status") or "") == "RECOMMEND_PATCH")
    watch_count = sum(1 for row in acceptance_rows if str(row.get("status") or "") == "WATCH")
    rejected_count = sum(
        1
        for row in acceptance_rows
        if str(row.get("acceptance_failed_rules") or "").strip()
        or str(row.get("status") or "") in {"KEEP_BASELINE", "INSUFFICIENT_HISTORY"}
    )
    stable_market_count = sum(
        1 for row in stability_rows if _int(row.get("consecutive_stable_windows")) >= _int(row.get("min_consecutive_stable_windows"))
    )
    return {
        "id": "walk_forward_acceptance",
        "title": "Walk-Forward Acceptance",
        "status": "warn" if watch_count or rejected_count else "ok",
        "summary": (
            f"markets={len(acceptance_rows)} recommend={recommend_count} "
            f"watch={watch_count} rejected_or_baseline={rejected_count}"
        ),
        "metrics": {
            "market_count": len(acceptance_rows),
            "recommend_patch_count": recommend_count,
            "watch_count": watch_count,
            "rejected_or_baseline_count": rejected_count,
            "stable_market_count": stable_market_count,
            "stability_row_count": len(stability_rows),
        },
        "rows": {
            "acceptance": acceptance_rows,
            "market_stability": stability_rows,
        },
    }


def build_strategy_parameter_governance_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    suggestions = _rows(payload.get("strategy_parameter_suggestions"), limit=50)
    followups = _rows(payload.get("strategy_parameter_suggestion_followup"), limit=50)
    effectiveness = _dict(payload.get("strategy_parameter_suggestion_effectiveness"))
    suggestion_count = _metric_or_count(effectiveness, "suggestion_count", len(suggestions))
    open_count = _metric_or_count(effectiveness, "open_suggestion_count", _status_count(suggestions, "SUGGESTED"))
    handled_count = _metric_or_count(
        effectiveness,
        "handled_suggestion_count",
        _status_count(suggestions, "ACKNOWLEDGED")
        + _status_count(suggestions, "APPLIED")
        + _status_count(suggestions, "REJECTED")
        + _status_count(suggestions, "SUPERSEDED"),
    )
    resolved_count = _metric_or_count(
        effectiveness,
        "resolved_suggestion_count",
        _status_count(suggestions, "APPLIED")
        + _status_count(suggestions, "REJECTED")
        + _status_count(suggestions, "SUPERSEDED"),
    )
    applied_count = _metric_or_count(effectiveness, "applied_suggestion_count", _status_count(suggestions, "APPLIED"))
    stale_count = _metric_or_count(effectiveness, "stale_suggestion_count", 0)
    auto_apply_count = _metric_or_count(
        effectiveness,
        "auto_apply_count",
        sum(1 for row in suggestions if _int(row.get("auto_apply")) != 0),
    )
    followup_count = _metric_or_count(effectiveness, "followup_count", len(followups))
    improved_followup_count = _metric_or_count(
        effectiveness,
        "improved_followup_count",
        _followup_verdict_count(followups, "IMPROVED"),
    )
    degraded_followup_count = _metric_or_count(
        effectiveness,
        "degraded_followup_count",
        _followup_verdict_count(followups, "DEGRADED"),
    )
    insufficient_followup_sample_count = _metric_or_count(
        effectiveness,
        "insufficient_followup_sample_count",
        _followup_verdict_count(followups, "INSUFFICIENT_FOLLOWUP_SAMPLE"),
    )
    no_clear_change_followup_count = _metric_or_count(
        effectiveness,
        "no_clear_change_followup_count",
        _followup_verdict_count(followups, "NO_CLEAR_CHANGE"),
    )
    primary = suggestions[0] if suggestions else {}
    primary_market = str(effectiveness.get("primary_market") or primary.get("market") or "")
    primary_portfolio_id = str(effectiveness.get("primary_portfolio_id") or primary.get("portfolio_id") or "")
    primary_field = str(effectiveness.get("primary_field") or primary.get("primary_field") or "")
    raw_status = str(effectiveness.get("status") or "").strip().lower()
    status = "warn" if open_count or stale_count or auto_apply_count or degraded_followup_count else (raw_status or "ok")
    if raw_status in {"fail", "failed", "error"}:
        status = "fail"
    summary_text = str(effectiveness.get("summary_text") or "").strip() or (
        f"suggestions={suggestion_count} open={open_count} handled={handled_count} "
        f"resolved={resolved_count} followups={followup_count} "
        f"improved={improved_followup_count} degraded={degraded_followup_count}"
    )
    return {
        "id": "strategy_parameter_governance",
        "title": "Strategy Parameter Governance",
        "status": status,
        "summary": summary_text,
        "metrics": {
            "suggestion_count": suggestion_count,
            "open_suggestion_count": open_count,
            "handled_suggestion_count": handled_count,
            "resolved_suggestion_count": resolved_count,
            "applied_suggestion_count": applied_count,
            "stale_suggestion_count": stale_count,
            "auto_apply_count": auto_apply_count,
            "followup_count": followup_count,
            "improved_followup_count": improved_followup_count,
            "degraded_followup_count": degraded_followup_count,
            "no_clear_change_followup_count": no_clear_change_followup_count,
            "insufficient_followup_sample_count": insufficient_followup_sample_count,
            "avg_resolution_hours": float(effectiveness.get("avg_resolution_hours", 0.0) or 0.0),
            "primary_market": primary_market,
            "primary_portfolio_id": primary_portfolio_id,
            "primary_field": primary_field,
            "read_only": bool(effectiveness.get("read_only", True)),
        },
        "rows": {
            "effectiveness": effectiveness,
            "suggestions": suggestions,
            "followup": followups,
        },
    }


def build_unified_evidence_overview_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    overview = _dict(payload.get("unified_evidence_overview"))
    sample_rows = _rows(payload.get("unified_evidence_rows"), limit=20)
    row_count = _int(overview.get("row_count"))
    return {
        "id": "unified_evidence_overview",
        "title": "Unified Evidence Overview",
        "status": "warn" if row_count <= 0 else "ok",
        "summary": (
            f"rows={row_count} allowed={_int(overview.get('allowed_row_count'))} "
            f"blocked={_int(overview.get('blocked_row_count'))} "
            f"candidate_only={_int(overview.get('candidate_only_row_count'))}"
        ),
        "metrics": {
            "row_count": row_count,
            "allowed_row_count": _int(overview.get("allowed_row_count")),
            "blocked_row_count": _int(overview.get("blocked_row_count")),
            "candidate_only_row_count": _int(overview.get("candidate_only_row_count")),
            "outcome_labeled_row_count": _int(overview.get("outcome_labeled_row_count")),
            "partial_join_row_count": _int(overview.get("partial_join_row_count")),
            "sample_row_count": len(sample_rows),
        },
        "rows": {
            "overview": overview,
            "sample_rows": sample_rows,
        },
    }


def build_blocked_vs_allowed_expost_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _rows(payload.get("blocked_vs_allowed_expost_review"), limit=50)
    too_restrictive_count = _count_labels(rows, {"BLOCKED_OUTPERFORMED_ALLOWED"})
    blocking_helped_count = _count_labels(rows, {"BLOCKING_HELPED", "GATE_OK"})
    insufficient_sample_count = _count_insufficient_sample(rows)
    mixed_review_count = _count_labels(rows, {"MIXED", "NEUTRAL"})
    return {
        "id": "blocked_vs_allowed_expost",
        "title": "Blocked vs Allowed Ex-Post Review",
        "status": "warn" if too_restrictive_count else "ok",
        "summary": (
            f"reviews={len(rows)} too_restrictive={too_restrictive_count} "
            f"blocking_helped={blocking_helped_count} insufficient_sample={insufficient_sample_count}"
        ),
        "metrics": {
            "review_count": len(rows),
            "too_restrictive_count": too_restrictive_count,
            "blocking_helped_count": blocking_helped_count,
            "insufficient_sample_count": insufficient_sample_count,
            "mixed_review_count": mixed_review_count,
        },
        "rows": {
            "label_summary": _blocked_review_label_summary(rows),
            "reviews": rows,
        },
    }


def build_dashboard_v2_blocks(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks = [
        build_ops_health_block(payload),
        build_open_market_analysis_block(payload),
        build_auto_order_readiness_block(payload),
        build_evidence_focus_actions_block(payload),
        build_evidence_quality_block(payload),
        build_control_actions_block(payload),
        build_market_views_block(payload),
        build_watchlist_expansion_block(payload),
        build_walk_forward_acceptance_block(payload),
        build_strategy_parameter_governance_block(payload),
        build_weekly_attribution_waterfall_block(payload),
        build_unified_evidence_overview_block(payload),
        build_blocked_vs_allowed_expost_block(payload),
        build_dashboard_control_action_history_block(payload),
    ]
    return [_with_dashboard_block_layout(block) for block in blocks]
