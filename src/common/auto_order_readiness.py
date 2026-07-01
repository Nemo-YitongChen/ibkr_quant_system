from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from .freshness import age_hours_from_timestamp, parse_utc_datetime
from .supervisor_runtime_status import build_supervisor_runtime_status_from_payloads
from .watchlist_expansion import summarize_seed_promotion_quality

READY_STATUS = "READY"
BLOCKED_STATUS = "BLOCKED"
WARNING_STATUS = "WARNING"
DISABLED_STATUS = "DISABLED"

_BLOCK_REASON_PRIORITY = {
    "live_submit_not_allowed": 10,
    "ibkr_gateway_unavailable": 15,
    "supervisor_code_revision_missing": 18,
    "supervisor_code_revision_mismatch": 19,
    "preflight_missing": 20,
    "preflight_failed": 25,
    "preflight_stale": 30,
    "weekly_review_missing": 40,
    "weekly_review_stale": 45,
    "gateway_budget_degraded": 50,
    "market_readiness_missing": 55,
    "market_readiness_not_ready": 60,
    "submit_quality_not_pass": 65,
    "strategy_auto_apply_violation": 70,
    "strategy_followup_degraded": 75,
    "strategy_suggestion_stale": 80,
}

_WARNING_REASON_PRIORITY = {
    "preflight_warn": 100,
    "gateway_budget_warning": 110,
    "gateway_budget_research_degraded": 115,
    "market_readiness_missing": 120,
    "market_readiness_artifact_health": 130,
    "strategy_suggestions_open": 140,
}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _status(value: Any) -> str:
    return str(value or "").strip().lower()


def _market(value: Any) -> str:
    return str(value or "").strip().upper()


def _supervisor_code_revision_gate(
    supervisor_status: Mapping[str, Any] | None,
    current_code_revision: str,
) -> Dict[str, Any]:
    status = dict(supervisor_status or {})
    running_state = _status(status.get("status"))
    runtime = build_supervisor_runtime_status_from_payloads(
        summary_dir=Path("."),
        shutdown_status=status,
        current_revision=str(current_code_revision or "").strip(),
        pid_alive_func=lambda _pid: None,
    )
    supervisor_revision = str(runtime.get("supervisor_code_revision") or "").strip()
    current_revision = str(runtime.get("current_code_revision") or "").strip()
    revision_status = str(runtime.get("supervisor_code_revision_status") or "")
    runtime_fields = {
        "runtime_next_action": str(runtime.get("next_action") or ""),
        "runtime_restart_required": bool(runtime.get("restart_required")),
        "runtime_blocks_recovery_refresh": bool(runtime.get("blocks_recovery_refresh")),
        "runtime_request_policy": str(runtime.get("request_policy") or ""),
        "runtime_health_status": str(runtime.get("health_status") or ""),
    }
    if running_state not in {"running", "running_degraded"}:
        return {
            "revision_status": "not_running",
            "reason": "",
            "detail": "",
            "supervisor_code_revision": supervisor_revision,
            "current_code_revision": current_revision,
            **runtime_fields,
        }
    if revision_status == "missing":
        return {
            "revision_status": "missing",
            "reason": "supervisor_code_revision_missing",
            "detail": (
                f"status={running_state} pid={status.get('pid', '')} "
                "code_revision is missing"
            ),
            "supervisor_code_revision": supervisor_revision,
            "current_code_revision": current_revision,
            **runtime_fields,
        }
    if revision_status == "mismatch":
        return {
            "revision_status": "mismatch",
            "reason": "supervisor_code_revision_mismatch",
            "detail": (
                f"status={running_state} pid={status.get('pid', '')} "
                f"supervisor={supervisor_revision} current={current_revision}"
            ),
            "supervisor_code_revision": supervisor_revision,
            "current_code_revision": current_revision,
            **runtime_fields,
        }
    return {
        "revision_status": "match" if revision_status in {"", "unknown"} else revision_status,
        "reason": "",
        "detail": (
            f"status={running_state} pid={status.get('pid', '')} "
            f"supervisor={supervisor_revision} current={current_revision}"
        ),
        "supervisor_code_revision": supervisor_revision,
        "current_code_revision": current_revision,
        **runtime_fields,
    }


def normalize_auto_order_readiness_policy(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
    source = dict(raw or {})
    raw_excluded_markets = source.get("excluded_markets") or source.get("exclude_markets") or []
    if isinstance(raw_excluded_markets, str):
        raw_excluded_markets = raw_excluded_markets.split(",")
    excluded_markets = [
        _market(value)
        for value in list(raw_excluded_markets or [])
        if _market(value)
    ]
    return {
        "enabled": bool(source.get("enabled", False)),
        "allow_live_submit": bool(source.get("allow_live_submit", False)),
        "excluded_markets": excluded_markets,
        "max_preflight_age_hours": _float(source.get("max_preflight_age_hours", 24.0), 24.0),
        "max_weekly_review_age_hours": _float(source.get("max_weekly_review_age_hours", 168.0), 168.0),
        "local_dependency_refresh_enabled": bool(
            source.get("local_dependency_refresh_enabled", False)
        ),
        "preflight_refresh_interval_min": max(
            1,
            _int(source.get("preflight_refresh_interval_min"), 360),
        ),
        "market_readiness_refresh_interval_min": max(
            1,
            _int(source.get("market_readiness_refresh_interval_min"), 15),
        ),
        "dependency_refresh_retry_interval_min": max(
            1,
            _int(source.get("dependency_refresh_retry_interval_min"), 10),
        ),
        "dependency_refresh_timeout_sec": max(
            1.0,
            _float(source.get("dependency_refresh_timeout_sec"), 60.0),
        ),
        "max_offline_recovery_gap_hours": _float(
            source.get("max_offline_recovery_gap_hours", source.get("max_preflight_age_hours", 24.0)),
            24.0,
        ),
        "block_on_preflight_fail": bool(source.get("block_on_preflight_fail", True)),
        "block_on_missing_preflight": bool(source.get("block_on_missing_preflight", True)),
        "block_on_stale_preflight": bool(source.get("block_on_stale_preflight", True)),
        "block_on_missing_weekly_review": bool(source.get("block_on_missing_weekly_review", True)),
        "block_on_stale_weekly_review": bool(source.get("block_on_stale_weekly_review", True)),
        "block_on_gateway_budget_degraded": bool(source.get("block_on_gateway_budget_degraded", True)),
        "block_on_supervisor_code_revision_mismatch": bool(
            source.get("block_on_supervisor_code_revision_mismatch", True)
        ),
        "block_on_missing_market_readiness": bool(source.get("block_on_missing_market_readiness", False)),
        "block_on_market_readiness_not_ready": bool(source.get("block_on_market_readiness_not_ready", True)),
        "warn_on_missing_market_readiness": bool(source.get("warn_on_missing_market_readiness", False)),
        "max_submit_portfolios_per_run": max(1, _int(source.get("max_submit_portfolios_per_run"), 1)),
        "max_submit_portfolios_per_market": max(1, _int(source.get("max_submit_portfolios_per_market"), 1)),
        "max_submit_orders_per_portfolio": max(1, _int(source.get("max_submit_orders_per_portfolio"), 1)),
        "max_submit_gross_order_value": _float(source.get("max_submit_gross_order_value"), 100.0),
        "max_submit_total_gross_order_value": _float(
            source.get("max_submit_total_gross_order_value"),
            0.0,
        ),
        "evidence_scaled_submit_enabled": bool(source.get("evidence_scaled_submit_enabled", False)),
        "baseline_submit_portfolios_per_run": max(
            1,
            _int(source.get("baseline_submit_portfolios_per_run"), 1),
        ),
        "baseline_submit_total_gross_order_value": max(
            0.0,
            _float(source.get("baseline_submit_total_gross_order_value"), 100.0),
        ),
        "trial_submit_portfolios_per_run": max(
            1,
            _int(
                source.get("trial_submit_portfolios_per_run"),
                source.get("max_submit_portfolios_per_run", 1),
            ),
        ),
        "trial_submit_total_gross_order_value": max(
            0.0,
            _float(
                source.get("trial_submit_total_gross_order_value"),
                source.get("max_submit_total_gross_order_value", 0.0),
            ),
        ),
        "scale_min_filled_orders": max(1, _int(source.get("scale_min_filled_orders"), 5)),
        "scale_min_matured_edge_samples": max(
            1,
            _int(source.get("scale_min_matured_edge_samples"), 5),
        ),
        "scale_min_realized_edge_bps": _float(source.get("scale_min_realized_edge_bps"), 0.0),
        "scale_max_abs_slippage_bps": max(
            0.0,
            _float(source.get("scale_max_abs_slippage_bps"), 15.0),
        ),
        "scale_max_error_rate": max(
            0.0,
            min(1.0, _float(source.get("scale_max_error_rate"), 0.05)),
        ),
        "full_scale_min_filled_orders": max(
            1,
            _int(
                source.get("full_scale_min_filled_orders"),
                source.get("scale_min_filled_orders", 5),
            ),
        ),
        "full_scale_min_matured_edge_samples": max(
            1,
            _int(
                source.get("full_scale_min_matured_edge_samples"),
                source.get("scale_min_matured_edge_samples", 5),
            ),
        ),
        "full_scale_min_realized_edge_bps": _float(
            source.get("full_scale_min_realized_edge_bps"),
            source.get("scale_min_realized_edge_bps", 0.0),
        ),
        "full_scale_max_abs_slippage_bps": max(
            0.0,
            _float(
                source.get("full_scale_max_abs_slippage_bps"),
                source.get("scale_max_abs_slippage_bps", 15.0),
            ),
        ),
        "full_scale_max_error_rate": max(
            0.0,
            min(
                1.0,
                _float(
                    source.get("full_scale_max_error_rate"),
                    source.get("scale_max_error_rate", 0.05),
                ),
            ),
        ),
        "full_scale_min_evidence_markets": max(
            1,
            _int(source.get("full_scale_min_evidence_markets"), 1),
        ),
        "market_evidence_min_filled_orders": max(
            1,
            _int(source.get("market_evidence_min_filled_orders"), 1),
        ),
        "market_evidence_min_matured_edge_samples": max(
            1,
            _int(source.get("market_evidence_min_matured_edge_samples"), 1),
        ),
        "max_submit_unevidenced_markets_per_run": max(
            0,
            _int(source.get("max_submit_unevidenced_markets_per_run"), 1),
        ),
        "require_buy_order_for_submit": bool(source.get("require_buy_order_for_submit", False)),
        "block_on_submit_quality_not_pass": bool(source.get("block_on_submit_quality_not_pass", True)),
        "min_submit_net_edge_bps": _float(source.get("min_submit_net_edge_bps"), 8.0),
        "min_submit_edge_margin_bps": _float(source.get("min_submit_edge_margin_bps"), 3.0),
        "max_submit_expected_cost_bps": _float(source.get("max_submit_expected_cost_bps"), 35.0),
        "require_limit_order_for_submit": bool(source.get("require_limit_order_for_submit", True)),
        "max_submit_order_adv_pct": _float(source.get("max_submit_order_adv_pct"), 0.001),
        "high_quality_min_net_edge_bps": _float(source.get("high_quality_min_net_edge_bps"), 16.0),
        "high_quality_min_edge_margin_bps": _float(source.get("high_quality_min_edge_margin_bps"), 8.0),
        "high_quality_max_expected_cost_bps": _float(source.get("high_quality_max_expected_cost_bps"), 25.0),
        "block_on_strategy_auto_apply_violation": bool(
            source.get("block_on_strategy_auto_apply_violation", True)
        ),
        "block_on_degraded_strategy_followup": bool(source.get("block_on_degraded_strategy_followup", True)),
        "warn_on_open_strategy_suggestions": bool(source.get("warn_on_open_strategy_suggestions", True)),
    }


def _execution_evidence_metrics(
    session_rows: Iterable[Mapping[str, Any]],
    feedback_rows: Iterable[Mapping[str, Any]],
    edge_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    sessions = [dict(row) for row in list(session_rows or []) if isinstance(row, Mapping)]
    feedback = [dict(row) for row in list(feedback_rows or []) if isinstance(row, Mapping)]
    edges = [dict(row) for row in list(edge_rows or []) if isinstance(row, Mapping)]
    fill_count = sum(max(0, _int(row.get("fill_count"), 0)) for row in sessions)
    submitted_count = sum(max(0, _int(row.get("submitted_order_rows"), 0)) for row in sessions)
    error_count = sum(max(0, _int(row.get("error_order_rows"), 0)) for row in feedback)

    slippage_numerator = 0.0
    slippage_denominator = 0.0
    for row in sessions:
        slippage = _optional_float(row.get("avg_actual_slippage_bps"))
        if slippage is None or _int(row.get("fill_count"), 0) <= 0:
            continue
        weight = abs(_float(row.get("fill_notional"), 0.0))
        if weight <= 0.0:
            weight = float(max(1, _int(row.get("fill_count"), 0)))
        slippage_numerator += slippage * weight
        slippage_denominator += weight
    avg_slippage_bps = (
        slippage_numerator / slippage_denominator
        if slippage_denominator > 0.0
        else None
    )

    reported_matured_sample_count = sum(
        max(0, _int(row.get("matured_5d_sample_count"), 0))
        for row in edges
    )
    realized_edge_total = 0.0
    matured_sample_count = 0
    for row in edges:
        realized_edge = _optional_float(row.get("matured_5d_avg_realized_edge_bps"))
        row_sample_count = max(0, _int(row.get("matured_5d_sample_count"), 0))
        if realized_edge is None or row_sample_count <= 0:
            continue
        realized_edge_total += realized_edge * row_sample_count
        matured_sample_count += row_sample_count
    return {
        "fill_count": int(fill_count),
        "submitted_order_count": int(submitted_count),
        "error_order_count": int(error_count),
        "execution_error_rate": float(error_count / max(1, submitted_count)),
        "avg_realized_slippage_bps": avg_slippage_bps,
        "reported_matured_5d_sample_count": int(reported_matured_sample_count),
        "matured_5d_sample_count": int(matured_sample_count),
        "avg_matured_5d_realized_edge_bps": (
            realized_edge_total / matured_sample_count
            if matured_sample_count > 0
            else None
        ),
    }


def _evidence_quality_reasons(
    metrics: Mapping[str, Any],
    *,
    min_realized_edge_bps: float,
    max_abs_slippage_bps: float,
    max_error_rate: float,
) -> List[str]:
    reasons: List[str] = []
    realized_edge = metrics.get("avg_matured_5d_realized_edge_bps")
    realized_slippage = metrics.get("avg_realized_slippage_bps")
    if _int(metrics.get("fill_count"), 0) > 0 and realized_slippage is None:
        reasons.append("realized_slippage_missing")
    if (
        _int(metrics.get("reported_matured_5d_sample_count"), 0) > 0
        and realized_edge is None
    ):
        reasons.append("realized_edge_missing")
    if realized_edge is not None and _float(realized_edge) < min_realized_edge_bps:
        reasons.append("realized_edge_below_min")
    if realized_slippage is not None and abs(_float(realized_slippage)) > max_abs_slippage_bps:
        reasons.append("realized_slippage_above_max")
    if (
        _int(metrics.get("submitted_order_count"), 0) > 0
        and _float(metrics.get("execution_error_rate"), 0.0) > max_error_rate
    ):
        reasons.append("execution_error_rate_above_max")
    return reasons


def _market_evidence_rows(
    session_rows: Iterable[Mapping[str, Any]],
    feedback_rows: Iterable[Mapping[str, Any]],
    edge_rows: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    sessions = [dict(row) for row in list(session_rows or []) if isinstance(row, Mapping)]
    feedback = [dict(row) for row in list(feedback_rows or []) if isinstance(row, Mapping)]
    edges = [dict(row) for row in list(edge_rows or []) if isinstance(row, Mapping)]
    markets = sorted(
        {
            _market(row.get("market"))
            for row in sessions + feedback + edges
            if _market(row.get("market"))
        }
    )
    min_fills = max(1, _int(policy.get("market_evidence_min_filled_orders"), 1))
    min_matured = max(1, _int(policy.get("market_evidence_min_matured_edge_samples"), 1))
    rows: List[Dict[str, Any]] = []
    for market in markets:
        metrics = _execution_evidence_metrics(
            [row for row in sessions if _market(row.get("market")) == market],
            [row for row in feedback if _market(row.get("market")) == market],
            [row for row in edges if _market(row.get("market")) == market],
        )
        quality_reasons = _evidence_quality_reasons(
            metrics,
            min_realized_edge_bps=_float(policy.get("scale_min_realized_edge_bps"), 0.0),
            max_abs_slippage_bps=max(0.0, _float(policy.get("scale_max_abs_slippage_bps"), 15.0)),
            max_error_rate=max(0.0, _float(policy.get("scale_max_error_rate"), 0.05)),
        )
        sample_ready = (
            _int(metrics.get("fill_count"), 0) >= min_fills
            and _int(metrics.get("matured_5d_sample_count"), 0) >= min_matured
        )
        rows.append(
            {
                "market": market,
                **metrics,
                "sample_ready": bool(sample_ready),
                "quality_reasons": quality_reasons,
                "evidence_ready": bool(sample_ready and not quality_reasons),
            }
        )
    return rows


def build_auto_order_submit_capacity_plan(
    weekly_summary: Mapping[str, Any] | None,
    *,
    policy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Derive current paper-submit capacity from realized execution evidence."""
    normalized = normalize_auto_order_readiness_policy(policy)
    configured_portfolios = max(1, _int(normalized.get("max_submit_portfolios_per_run"), 1))
    configured_total = max(0.0, _float(normalized.get("max_submit_total_gross_order_value"), 0.0))
    baseline_portfolios = min(
        configured_portfolios,
        max(1, _int(normalized.get("baseline_submit_portfolios_per_run"), 1)),
    )
    baseline_total = max(0.0, _float(normalized.get("baseline_submit_total_gross_order_value"), 100.0))
    if configured_total > 0.0:
        baseline_total = min(configured_total, baseline_total)
    trial_portfolios = min(
        configured_portfolios,
        max(
            baseline_portfolios,
            _int(normalized.get("trial_submit_portfolios_per_run"), configured_portfolios),
        ),
    )
    trial_total = max(
        baseline_total,
        _float(normalized.get("trial_submit_total_gross_order_value"), configured_total),
    )
    if configured_total > 0.0:
        trial_total = min(configured_total, trial_total)
    if not bool(normalized.get("evidence_scaled_submit_enabled", False)):
        return {
            "status": "DISABLED",
            "reason": "evidence_scaled_submit_disabled",
            "scale_allowed": True,
            "scale_stage": "configured",
            "effective_max_submit_portfolios_per_run": configured_portfolios,
            "effective_max_submit_total_gross_order_value": configured_total,
            "configured_max_submit_portfolios_per_run": configured_portfolios,
            "configured_max_submit_total_gross_order_value": configured_total,
        }

    weekly = dict(weekly_summary or {})
    session_rows = [
        dict(row)
        for row in list(weekly.get("execution_session_summary") or [])
        if isinstance(row, Mapping)
    ]
    feedback_rows = [
        dict(row)
        for row in list(weekly.get("execution_feedback_summary") or [])
        if isinstance(row, Mapping)
    ]
    edge_rows = [
        dict(row)
        for row in list(weekly.get("edge_realization_summary") or [])
        if isinstance(row, Mapping)
    ]
    metrics = _execution_evidence_metrics(session_rows, feedback_rows, edge_rows)
    market_rows = _market_evidence_rows(
        session_rows,
        feedback_rows,
        edge_rows,
        policy=normalized,
    )
    evidence_market_count = sum(1 for row in market_rows if bool(row.get("evidence_ready", False)))
    min_fills = max(1, _int(normalized.get("scale_min_filled_orders"), 5))
    min_matured = max(1, _int(normalized.get("scale_min_matured_edge_samples"), 5))
    min_realized_edge = _float(normalized.get("scale_min_realized_edge_bps"), 0.0)
    max_abs_slippage = max(0.0, _float(normalized.get("scale_max_abs_slippage_bps"), 15.0))
    max_error_rate = max(0.0, _float(normalized.get("scale_max_error_rate"), 0.05))
    trial_insufficient = (
        _int(metrics.get("fill_count"), 0) < min_fills
        or _int(metrics.get("matured_5d_sample_count"), 0) < min_matured
    )
    trial_degraded_reasons = _evidence_quality_reasons(
        metrics,
        min_realized_edge_bps=min_realized_edge,
        max_abs_slippage_bps=max_abs_slippage,
        max_error_rate=max_error_rate,
    )

    full_min_fills = max(1, _int(normalized.get("full_scale_min_filled_orders"), min_fills))
    full_min_matured = max(
        1,
        _int(normalized.get("full_scale_min_matured_edge_samples"), min_matured),
    )
    full_min_markets = max(1, _int(normalized.get("full_scale_min_evidence_markets"), 1))
    full_insufficient_reasons: List[str] = []
    if _int(metrics.get("fill_count"), 0) < full_min_fills:
        full_insufficient_reasons.append("full_scale_fill_samples_below_min")
    if _int(metrics.get("matured_5d_sample_count"), 0) < full_min_matured:
        full_insufficient_reasons.append("full_scale_matured_edge_samples_below_min")
    if evidence_market_count < full_min_markets:
        full_insufficient_reasons.append("full_scale_market_evidence_below_min")
    full_degraded_reasons = _evidence_quality_reasons(
        metrics,
        min_realized_edge_bps=_float(
            normalized.get("full_scale_min_realized_edge_bps"),
            min_realized_edge,
        ),
        max_abs_slippage_bps=max(
            0.0,
            _float(normalized.get("full_scale_max_abs_slippage_bps"), max_abs_slippage),
        ),
        max_error_rate=max(
            0.0,
            _float(normalized.get("full_scale_max_error_rate"), max_error_rate),
        ),
    )
    if trial_insufficient:
        status = "BASELINE_INSUFFICIENT_EVIDENCE"
        reason = "collect_fill_slippage_and_matured_edge_samples"
        scale_allowed = False
        scale_stage = "baseline"
        effective_portfolios = baseline_portfolios
        effective_total = baseline_total
    elif trial_degraded_reasons:
        status = "HOLD_QUALITY_DEGRADED"
        reason = ",".join(trial_degraded_reasons)
        scale_allowed = False
        scale_stage = "baseline"
        effective_portfolios = baseline_portfolios
        effective_total = baseline_total
    elif not full_insufficient_reasons and not full_degraded_reasons:
        status = "FULL_SCALE_ALLOWED"
        reason = "full_scale_fill_slippage_post_cost_edge_and_market_coverage_pass"
        scale_allowed = True
        scale_stage = "full"
        effective_portfolios = configured_portfolios
        effective_total = configured_total
    else:
        status = "TRIAL_SCALE_ALLOWED"
        pending = full_insufficient_reasons + full_degraded_reasons
        reason = "trial_quality_pass_full_scale_pending:" + ",".join(pending)
        scale_allowed = True
        scale_stage = "trial"
        effective_portfolios = trial_portfolios
        effective_total = trial_total
    return {
        "status": status,
        "reason": reason,
        "scale_allowed": bool(scale_allowed),
        "scale_stage": scale_stage,
        "effective_max_submit_portfolios_per_run": effective_portfolios,
        "effective_max_submit_total_gross_order_value": effective_total,
        "configured_max_submit_portfolios_per_run": configured_portfolios,
        "configured_max_submit_total_gross_order_value": configured_total,
        "baseline_max_submit_portfolios_per_run": baseline_portfolios,
        "baseline_max_submit_total_gross_order_value": baseline_total,
        "trial_max_submit_portfolios_per_run": trial_portfolios,
        "trial_max_submit_total_gross_order_value": trial_total,
        "fill_count": _int(metrics.get("fill_count"), 0),
        "submitted_order_count": _int(metrics.get("submitted_order_count"), 0),
        "error_order_count": _int(metrics.get("error_order_count"), 0),
        "execution_error_rate": round(_float(metrics.get("execution_error_rate"), 0.0), 6),
        "avg_realized_slippage_bps": (
            round(_float(metrics.get("avg_realized_slippage_bps")), 6)
            if metrics.get("avg_realized_slippage_bps") is not None
            else None
        ),
        "matured_5d_sample_count": _int(metrics.get("matured_5d_sample_count"), 0),
        "avg_matured_5d_realized_edge_bps": (
            round(_float(metrics.get("avg_matured_5d_realized_edge_bps")), 6)
            if metrics.get("avg_matured_5d_realized_edge_bps") is not None
            else None
        ),
        "evidence_market_count": int(evidence_market_count),
        "market_evidence_rows": market_rows,
        "thresholds": {
            "trial": {
                "min_filled_orders": min_fills,
                "min_matured_edge_samples": min_matured,
                "min_realized_edge_bps": min_realized_edge,
                "max_abs_slippage_bps": max_abs_slippage,
                "max_error_rate": max_error_rate,
            },
            "full": {
                "min_filled_orders": full_min_fills,
                "min_matured_edge_samples": full_min_matured,
                "min_realized_edge_bps": _float(
                    normalized.get("full_scale_min_realized_edge_bps"),
                    min_realized_edge,
                ),
                "max_abs_slippage_bps": max(
                    0.0,
                    _float(normalized.get("full_scale_max_abs_slippage_bps"), max_abs_slippage),
                ),
                "max_error_rate": max(
                    0.0,
                    _float(normalized.get("full_scale_max_error_rate"), max_error_rate),
                ),
                "min_evidence_markets": full_min_markets,
            },
        },
    }


def _submit_plan_policy_snapshot(
    normalized_policy: Mapping[str, Any],
    *,
    max_portfolios: int,
    max_per_market: int,
    max_orders: int,
    max_value: float,
    max_total_value: float,
    require_buy: bool,
    account_growth_context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    account_context = dict(account_growth_context or {})
    return {
        "max_submit_portfolios_per_run": int(max_portfolios),
        "configured_max_submit_portfolios_per_run": max(
            1,
            _int(normalized_policy.get("max_submit_portfolios_per_run"), 1),
        ),
        "max_submit_portfolios_per_market": int(max_per_market),
        "max_submit_orders_per_portfolio": int(max_orders),
        "max_submit_gross_order_value": float(max_value),
        "configured_max_submit_gross_order_value": max(
            0.0,
            _float(normalized_policy.get("max_submit_gross_order_value"), 100.0),
        ),
        "max_submit_total_gross_order_value": float(max_total_value),
        "configured_max_submit_total_gross_order_value": max(
            0.0,
            _float(normalized_policy.get("max_submit_total_gross_order_value"), 0.0),
        ),
        "require_buy_order_for_submit": bool(require_buy),
        "excluded_markets": list(normalized_policy.get("excluded_markets") or []),
        "block_on_submit_quality_not_pass": bool(
            normalized_policy.get("block_on_submit_quality_not_pass", True)
        ),
        "evidence_scaled_submit_enabled": bool(
            normalized_policy.get("evidence_scaled_submit_enabled", False)
        ),
        "max_submit_unevidenced_markets_per_run": max(
            0,
            _int(normalized_policy.get("max_submit_unevidenced_markets_per_run"), 1),
        ),
        "account_growth_profile": str(account_context.get("profile") or ""),
        "account_growth_label": str(account_context.get("label") or ""),
        "account_growth_equity": _float(account_context.get("equity"), 0.0),
        "account_growth_expansion_mode": str(account_context.get("expansion_mode") or ""),
        "account_growth_submit_frequency_mode": str(account_context.get("submit_frequency_mode") or ""),
        "account_growth_primary_action": str(account_context.get("primary_action") or ""),
        "account_growth_max_orders_per_run": _int(account_context.get("max_orders_per_run"), 0),
        "account_growth_max_order_value": _float(account_context.get("max_order_value"), 0.0),
    }


def _block_detail(reason: str, severity: str, detail: str = "", remediation: str = "") -> Dict[str, str]:
    return {
        "reason": reason,
        "severity": severity,
        "detail": detail,
        "remediation": remediation,
    }


def _reason_priority(reason: str, *, warning: bool = False) -> int:
    reason_text = str(reason or "").strip()
    priorities = _WARNING_REASON_PRIORITY if warning else _BLOCK_REASON_PRIORITY
    return int(priorities.get(reason_text, 999))


def _primary_hard_block_reason(hard_blocks: Iterable[Any]) -> str:
    reasons = [str(reason or "").strip() for reason in list(hard_blocks or []) if str(reason or "").strip()]
    if not reasons:
        return ""
    return min(reasons, key=lambda reason: (_reason_priority(reason), reason))


def _preflight_check_relevant(check: Mapping[str, Any], portfolio: Mapping[str, Any]) -> bool:
    name = str(check.get("name") or "").strip()
    if not name:
        return True
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    watchlist = str(portfolio.get("watchlist") or "").strip()
    ibkr_config = str(portfolio.get("ibkr_config") or "").strip()
    global_names = {
        "config",
        "summary_out_dir",
        "dashboard_weekly_review_dir",
        "dashboard_execution_kpi_dir",
        "dashboard_db",
        "dashboard_control_state",
        "runtime_root",
    }
    if name in global_names:
        return True
    if name.startswith("ibkr_port:"):
        markets = {_market(value) for value in list(check.get("markets") or []) if _market(value)}
        config_paths = {str(value).strip() for value in list(check.get("ibkr_config_paths") or []) if str(value).strip()}
        return (market and market in markets) or (ibkr_config and ibkr_config in config_paths)
    prefix = name.split(":", 1)[0].strip().upper()
    if market and prefix != market:
        return False
    if portfolio_id and name.startswith(f"{portfolio_id}:"):
        return True
    if market and watchlist and name.startswith(f"{market}:{watchlist}:"):
        return True
    if market and name.startswith(f"{market}:"):
        return portfolio_id.count(":") <= 1 and watchlist == ""
    return True


def _preflight_relevant_checks(
    preflight_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
    statuses: Iterable[str],
) -> List[Dict[str, Any]]:
    wanted = {_status(value) for value in statuses}
    checks = [
        dict(check)
        for check in list((preflight_summary or {}).get("checks") or [])
        if isinstance(check, Mapping)
    ]
    return [
        check
        for check in checks
        if _status(check.get("status")) in wanted and _preflight_check_relevant(check, portfolio)
    ]


def _gateway_budget_row(weekly_summary: Mapping[str, Any] | None, portfolio: Mapping[str, Any]) -> Dict[str, Any]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("ibkr_gateway_budget_rows") or [])
        if isinstance(row, Mapping)
    ]
    market = _market(portfolio.get("market"))
    for row in rows:
        if _market(row.get("market")) == market:
            return row
    return {}


def _gateway_budget_status(weekly_summary: Mapping[str, Any] | None, portfolio: Mapping[str, Any]) -> str:
    row = _gateway_budget_row(weekly_summary, portfolio)
    if row:
        return _status(row.get("status"))
    summary = dict((weekly_summary or {}).get("ibkr_gateway_budget") or {})
    status = _status(summary.get("status"))
    return status


def _gateway_budget_submit_blocking(row: Mapping[str, Any], status: str) -> bool:
    normalized_status = _status(status)
    if normalized_status not in {"fail", "failed", "error", "degraded"}:
        return False
    if "submit_blocking" not in row:
        return True
    return bool(row.get("submit_blocking", False))


def _gateway_budget_detail(row: Mapping[str, Any], portfolio: Mapping[str, Any], status: str) -> str:
    reason = str(row.get("reason") or status).strip()
    budget = _int(row.get("weekly_gateway_request_budget"), 0)
    gateway_count = _int(row.get("gateway_request_count"), 0)
    usage_pct = _float(row.get("budget_usage_pct"), 0.0)
    top_kind = str(row.get("top_request_kind") or "").strip()
    top_tool = str(row.get("top_tool") or "").strip()
    recovery_days = _int(row.get("projected_recovery_days"), 0)
    recovery_at = str(row.get("projected_recovery_at") or "").strip()
    execution_count = _int(row.get("execution_gateway_request_count"), 0)
    execution_reserve = _int(row.get("execution_reserve_weekly_requests"), 0)
    research_recent = _int(row.get("research_recent_24h_request_count"), 0)
    research_daily = _int(row.get("research_daily_request_budget"), 0)
    short_count = _int(row.get("short_window_gateway_request_count"), 0)
    short_execution_count = _int(row.get("short_window_execution_request_count"), 0)
    short_limit = _int(row.get("short_window_request_limit"), 0)
    short_execution_reserve = _int(row.get("short_window_execution_reserve"), 0)
    parts = [f"market={portfolio.get('market', '')}", f"reason={reason}"]
    if budget > 0 or gateway_count > 0:
        parts.append(f"requests={gateway_count}/{budget}")
        parts.append(f"usage={usage_pct:.2f}%")
    if top_kind:
        parts.append(f"top_request_kind={top_kind}")
    if top_tool:
        parts.append(f"top_tool={top_tool}")
    if recovery_days > 0:
        parts.append(f"projected_recovery_days={recovery_days}")
    if recovery_at:
        parts.append(f"projected_recovery_at={recovery_at}")
    if execution_reserve > 0:
        parts.append(f"execution={execution_count}/{execution_reserve}")
    if research_daily > 0:
        parts.append(f"research_24h={research_recent}/{research_daily}")
    if short_limit > 0:
        parts.append(f"short_window={short_count}/{short_limit}")
    if short_execution_reserve > 0:
        parts.append(f"short_execution={short_execution_count}/{short_execution_reserve}")
    return " ".join(parts)


def _gateway_budget_remediation(row: Mapping[str, Any], *, blocked: bool) -> str:
    recovery_at = str(row.get("projected_recovery_at") or "").strip()
    top_tool = str(row.get("top_tool") or "").strip()
    top_kind = str(row.get("top_request_kind") or "").strip()
    action = "Keep high-request scans disabled and avoid submit until execution capacity recovers."
    if not blocked:
        action = "Throttle research requests while preserving execution and protective request capacity."
    if top_tool or top_kind:
        action += f" Highest load: {top_tool or 'unknown_tool'} / {top_kind or 'unknown_kind'}."
    if recovery_at:
        action += f" Re-check Gateway evidence after {recovery_at}."
    else:
        action += " Refresh the local Gateway budget artifact before the next scheduling cycle."
    return action


def _market_readiness_row(
    market_readiness_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> Dict[str, Any]:
    rows = [
        dict(row)
        for row in list((market_readiness_summary or {}).get("rows") or [])
        if isinstance(row, Mapping)
    ]
    if not rows:
        return {}
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    watchlist = str(portfolio.get("watchlist") or "").strip()
    for row in rows:
        if portfolio_id and str(row.get("portfolio_id") or "").strip() == portfolio_id:
            return row
    for row in rows:
        if market and _market(row.get("market")) == market and watchlist and str(row.get("watchlist") or "").strip() == watchlist:
            return row
    for row in rows:
        if market and _market(row.get("market")) == market:
            return row
    return {}


def _strategy_suggestion_rows_for_portfolio(
    weekly_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("strategy_parameter_suggestions") or [])
        if isinstance(row, Mapping)
    ]
    if not rows:
        return []
    market = str(portfolio.get("market") or "").strip().upper()
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        row_market = str(row.get("market") or "").strip().upper()
        row_portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id and row_portfolio_id:
            if row_portfolio_id == portfolio_id:
                matched.append(row)
            continue
        if market and row_market == market:
            matched.append(row)
    return matched


def _is_unresolved_strategy_suggestion(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or "SUGGESTED").strip().upper()
    return status not in {"APPLIED", "REJECTED", "SUPERSEDED", "RESOLVED"}


def _active_strategy_suggestion_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = "|".join(
            [
                str(row.get("portfolio_id") or "").strip(),
                str(row.get("primary_field") or row.get("field") or "").strip(),
                str(row.get("config_path") or "").strip(),
            ]
        )
        if not key.strip("|"):
            key = str(row.get("suggestion_id") or len(latest))
        current = latest.get(key)
        current_dt = parse_utc_datetime(current.get("created_at")) if current else None
        row_dt = parse_utc_datetime(row.get("created_at"))
        if current is None or (row_dt or datetime.min.replace(tzinfo=timezone.utc)) >= (
            current_dt or datetime.min.replace(tzinfo=timezone.utc)
        ):
            latest[key] = row
    return list(latest.values())


def _offline_recovery_state(
    *,
    preflight_age_hours: float | None,
    weekly_age_hours: float | None,
    market_readiness_present: bool,
    market_artifact_health_status: str,
    market_artifact_age_hours: float | None,
    gateway_status: str,
    gateway_row: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> Dict[str, Any]:
    max_gap = max(0.0, _float(policy.get("max_offline_recovery_gap_hours"), 24.0))
    max_weekly_age = max(0.0, _float(policy.get("max_weekly_review_age_hours"), 168.0))
    reasons: List[str] = []
    actions: List[str] = []

    def add(reason: str, action: str) -> None:
        if reason not in reasons:
            reasons.append(reason)
        if action and action not in actions:
            actions.append(action)

    if max_gap > 0.0:
        if preflight_age_hours is None and bool(policy.get("block_on_missing_preflight", True)):
            add("preflight_missing", "Run supervisor preflight before any automated paper submit.")
        elif preflight_age_hours is not None and preflight_age_hours > max_gap:
            add("preflight_stale_after_offline_gap", "Refresh supervisor preflight after reconnect.")

        if market_readiness_present:
            artifact_status = str(market_artifact_health_status or "").strip().upper()
            if artifact_status in {"STALE", "MISSING", "DEGRADED_GATEWAY"}:
                add(
                    f"market_readiness_artifact_{artifact_status.lower()}",
                    "Refresh investment report, paper execution dry-run, and market readiness after reconnect.",
                )
            elif market_artifact_age_hours is not None and market_artifact_age_hours > max_gap:
                add(
                    "execution_artifact_stale_after_offline_gap",
                    "Refresh execution artifact before ranking submit candidates.",
                )
        elif bool(policy.get("block_on_missing_market_readiness", False)) or bool(
            policy.get("warn_on_missing_market_readiness", False)
        ):
            add("market_readiness_missing", "Run market readiness before automated submit.")

    if weekly_age_hours is None and bool(policy.get("block_on_missing_weekly_review", True)):
        add("weekly_review_missing", "Run weekly review before automated submit.")
    elif weekly_age_hours is not None and max_weekly_age > 0.0 and weekly_age_hours > max_weekly_age:
        add("weekly_review_stale", "Refresh weekly review before automated submit.")

    gateway_reason = str(gateway_row.get("reason") or "").strip().lower()
    if gateway_reason == "stale_ibkr_request_telemetry":
        add("gateway_budget_stale_telemetry", "Refresh weekly review Gateway telemetry after reconnect.")
    elif _gateway_budget_submit_blocking(gateway_row, gateway_status):
        add("gateway_budget_degraded", "Let Gateway request budget recover before automated submit.")

    gap_values = [
        value
        for value in (preflight_age_hours, weekly_age_hours, market_artifact_age_hours)
        if value is not None
    ]
    return {
        "offline_recovery_required": bool(reasons),
        "offline_recovery_reason": ",".join(reasons),
        "offline_recovery_reasons": reasons,
        "offline_recovery_next_action": " ".join(actions),
        "offline_recovery_gap_hours": round(max(gap_values), 2) if gap_values else 0.0,
        "offline_recovery_max_gap_hours": float(max_gap),
    }


def _strategy_followup_rows_for_portfolio(
    weekly_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("strategy_parameter_suggestion_followup") or [])
        if isinstance(row, Mapping)
    ]
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        if portfolio_id and str(row.get("portfolio_id") or "").strip() == portfolio_id:
            matched.append(row)
            continue
        if market and _market(row.get("market")) == market:
            matched.append(row)
    return matched


def _account_growth_submit_context(account_growth_tier_plan: Mapping[str, Any] | None) -> Dict[str, Any]:
    plan = dict(account_growth_tier_plan or {})
    if not plan:
        return {}
    max_orders = _int(plan.get("max_orders_per_run"), 0)
    max_value = _float(plan.get("max_order_value"), 0.0)
    return {
        "profile": str(plan.get("profile") or "").strip().lower(),
        "label": str(plan.get("label") or ""),
        "equity": _float(plan.get("equity"), 0.0),
        "equity_band": str(plan.get("equity_band") or ""),
        "preferred_instruments": [
            str(value).strip()
            for value in list(plan.get("preferred_instruments") or [])
            if str(value).strip()
        ],
        "max_orders_per_run": max_orders,
        "max_order_value": max_value,
        "min_trade_value": _float(plan.get("min_trade_value"), 0.0),
        "expansion_mode": str(plan.get("expansion_mode") or ""),
        "submit_frequency_mode": str(plan.get("submit_frequency_mode") or ""),
        "primary_action": str(plan.get("primary_action") or ""),
        "quality_gate_policy": str(plan.get("quality_gate_policy") or "do_not_relax_submit_gates"),
        "read_only": bool(plan.get("read_only", True)),
    }


def _submit_policy_reject_reasons(
    order_count: int,
    planned_gross: float,
    *,
    max_orders: int,
    max_value: float,
    planned_buy: float = 0.0,
    require_buy: bool = False,
    account_growth_context: Mapping[str, Any] | None = None,
) -> List[str]:
    reject_reasons: List[str] = []
    account_context = dict(account_growth_context or {})
    account_max_orders = _int(account_context.get("max_orders_per_run"), 0)
    account_max_value = _float(account_context.get("max_order_value"), 0.0)
    if order_count <= 0:
        reject_reasons.append("no_planned_orders")
    if bool(require_buy) and float(planned_buy) <= 0.0:
        reject_reasons.append("no_buy_order_for_growth_submit")
    if order_count > int(max_orders):
        reject_reasons.append("order_count_exceeds_policy")
    if float(max_value) > 0.0 and float(planned_gross) > float(max_value) + 1e-9:
        reject_reasons.append("planned_gross_value_exceeds_policy")
    if account_max_orders > 0 and order_count > account_max_orders:
        reject_reasons.append("account_growth_order_count_exceeds_profile")
    if account_max_value > 0.0 and float(planned_gross) > account_max_value + 1e-9:
        reject_reasons.append("account_growth_order_value_exceeds_profile")
    return reject_reasons


def _submit_candidate_base(
    row: Mapping[str, Any],
    *,
    order_count: int,
    planned_gross: float,
    account_growth_context: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    account_context = dict(account_growth_context or {})
    return {
        "market": str(row.get("market") or ""),
        "portfolio_id": str(row.get("portfolio_id") or ""),
        "order_count": int(order_count),
        "planned_gross_order_value": float(planned_gross),
        "planned_buy_order_value": _float(row.get("market_readiness_planned_buy_order_value"), 0.0),
        "planned_sell_order_value": _float(row.get("market_readiness_planned_sell_order_value"), 0.0),
        "planned_net_cash_order_value": _float(row.get("market_readiness_planned_net_cash_order_value"), 0.0),
        "planned_order_symbols": str(row.get("market_readiness_planned_order_symbols") or ""),
        "market_readiness_reason": str(row.get("market_readiness_reason") or ""),
        "submit_quality_status": str(row.get("submit_quality_status") or ""),
        "submit_quality_tier": str(row.get("submit_quality_tier") or ""),
        "submit_quality_reason": str(row.get("submit_quality_reason") or ""),
        "submit_quality_min_net_edge_bps": _float(row.get("submit_quality_min_net_edge_bps"), 0.0),
        "submit_quality_min_edge_margin_bps": _float(row.get("submit_quality_min_edge_margin_bps"), 0.0),
        "submit_quality_max_expected_cost_bps": _float(row.get("submit_quality_max_expected_cost_bps"), 0.0),
        "submit_quality_order_types": str(row.get("submit_quality_order_types") or ""),
        "account_growth_profile": str(account_context.get("profile") or ""),
        "account_growth_max_orders_per_run": _int(account_context.get("max_orders_per_run"), 0),
        "account_growth_max_order_value": _float(account_context.get("max_order_value"), 0.0),
        "account_growth_primary_action": str(account_context.get("primary_action") or ""),
        "account_growth_submit_frequency_mode": str(account_context.get("submit_frequency_mode") or ""),
    }


def _submit_quality_rank(row: Mapping[str, Any]) -> int:
    tier = str(row.get("submit_quality_tier") or "").strip().upper()
    if tier == "HIGH":
        return 0
    if tier == "PASS":
        return 1
    if str(row.get("submit_quality_status") or "").strip().upper() == "PASS":
        return 1
    return 2


def _market_readiness_rank(status: str) -> int:
    normalized = str(status or "").strip().upper()
    if normalized == "READY_FOR_PAPER_REVIEW":
        return 0
    if normalized in {"PLANNED_MARKET_CLOSED", "WAIT_MARKET_SESSION", "WAITING_FOR_MARKET_SESSION"}:
        return 1
    if normalized:
        return 2
    return 3


def _first_remediation(row: Mapping[str, Any], *, preferred_reason: str = "") -> str:
    wanted = str(preferred_reason or "").strip()
    if wanted:
        for key in ("hard_block_details", "warning_details"):
            for detail in list(row.get(key) or []):
                if not isinstance(detail, Mapping):
                    continue
                if str(detail.get("reason") or "").strip() != wanted:
                    continue
                remediation = str(detail.get("remediation") or "").strip()
                if remediation:
                    return remediation
    for key in ("hard_block_details", "warning_details"):
        for detail in list(row.get(key) or []):
            if not isinstance(detail, Mapping):
                continue
            remediation = str(detail.get("remediation") or "").strip()
            if remediation:
                return remediation
    return str(row.get("market_readiness_next_action") or "").strip()


def _build_submit_frontier_candidates(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_orders: int,
    max_value: float,
    require_buy: bool = False,
    account_growth_context: Mapping[str, Any] | None = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    frontier: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw or {})
        if str(row.get("status") or "").strip().upper() == DISABLED_STATUS:
            continue
        if str(row.get("account_mode") or "paper").strip().lower() != "paper":
            continue
        order_count = _int(row.get("market_readiness_order_count"), 0)
        planned_gross = _float(row.get("market_readiness_planned_gross_order_value"), 0.0)
        planned_buy = _float(row.get("market_readiness_planned_buy_order_value"), 0.0)
        policy_reject_reasons = _submit_policy_reject_reasons(
            order_count,
            planned_gross,
            max_orders=max_orders,
            max_value=max_value,
            planned_buy=planned_buy,
            require_buy=require_buy,
            account_growth_context=account_growth_context,
        )
        hard_blocks = [str(value) for value in list(row.get("hard_blocks") or []) if str(value).strip()]
        warnings = [str(value) for value in list(row.get("warnings") or []) if str(value).strip()]
        readiness_status = str(row.get("market_readiness_status") or "").strip().upper()
        if bool(row.get("ready", False)) and readiness_status == "READY_FOR_PAPER_REVIEW" and not policy_reject_reasons:
            frontier_reason = "eligible_candidate"
        elif policy_reject_reasons and not hard_blocks:
            frontier_reason = policy_reject_reasons[0]
        elif hard_blocks:
            frontier_reason = _primary_hard_block_reason(hard_blocks)
        else:
            frontier_reason = str(row.get("primary_reason") or "not_ready").strip() or "not_ready"
        frontier.append(
            {
                **_submit_candidate_base(
                    row,
                    order_count=order_count,
                    planned_gross=planned_gross,
                    account_growth_context=account_growth_context,
                ),
                "status": str(row.get("status") or ""),
                "ready": bool(row.get("ready", False)),
                "frontier_reason": frontier_reason,
                "hard_blocks": hard_blocks,
                "warnings": warnings,
                "policy_reject_reasons": policy_reject_reasons,
                "market_readiness_status": readiness_status,
                "market_readiness_artifact_health_status": str(
                    row.get("market_readiness_artifact_health_status") or ""
                ).strip().upper(),
                "market_readiness_feasibility_status": str(
                    row.get("market_readiness_feasibility_status") or ""
                ).strip().upper(),
                "next_action": _first_remediation(row, preferred_reason=frontier_reason),
            }
        )
    frontier.sort(
        key=lambda item: (
            0
            if int(item.get("order_count", 0) or 0) > 0
            or float(item.get("planned_gross_order_value", 0.0) or 0.0) > 0.0
            else 1,
            _submit_quality_rank(item),
            -float(item.get("submit_quality_min_net_edge_bps", 0.0) or 0.0),
            -float(item.get("submit_quality_min_edge_margin_bps", 0.0) or 0.0),
            _market_readiness_rank(str(item.get("market_readiness_status") or "")),
            len(list(item.get("policy_reject_reasons") or [])),
            len(list(item.get("hard_blocks") or [])),
            float(item.get("planned_gross_order_value", 0.0) or 0.0),
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
        )
    )
    return frontier[: max(1, int(limit))]


def evaluate_auto_order_readiness(
    portfolio: Mapping[str, Any],
    *,
    preflight_summary: Mapping[str, Any] | None = None,
    weekly_summary: Mapping[str, Any] | None = None,
    market_readiness_summary: Mapping[str, Any] | None = None,
    supervisor_status: Mapping[str, Any] | None = None,
    current_code_revision: str = "",
    policy: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> Dict[str, Any]:
    """Evaluate whether one portfolio is allowed to submit automated orders."""
    normalized_policy = normalize_auto_order_readiness_policy(policy)
    now_dt = now or datetime.now(timezone.utc)
    row = dict(portfolio or {})
    run_execution = bool(row.get("run_investment_execution", False))
    submit_execution = bool(row.get("submit_investment_execution", False))
    account_mode = str(row.get("account_mode") or "").strip().lower() or "paper"
    market_code = _market(row.get("market"))
    hard_blocks: List[str] = []
    warnings: List[str] = []
    hard_block_details: List[Dict[str, str]] = []
    warning_details: List[Dict[str, str]] = []

    if not run_execution or not submit_execution:
        return {
            **row,
            "status": DISABLED_STATUS,
            "ready": False,
            "primary_reason": "auto_submit_disabled",
            "hard_blocks": [],
            "warnings": [],
            "policy_enabled": bool(normalized_policy.get("enabled", False)),
        }

    if market_code in set(normalized_policy.get("excluded_markets") or []):
        return {
            **row,
            "status": DISABLED_STATUS,
            "ready": False,
            "primary_reason": "auto_submit_market_excluded",
            "hard_blocks": [],
            "warnings": [],
            "policy_enabled": bool(normalized_policy.get("enabled", False)),
            "account_mode": account_mode,
            "market_readiness_status": "",
            "market_readiness_reason": "",
        }

    if account_mode != "paper" and not bool(normalized_policy.get("allow_live_submit", False)):
        hard_blocks.append("live_submit_not_allowed")
        hard_block_details.append(
            _block_detail(
                "live_submit_not_allowed",
                "block",
                f"account_mode={account_mode}",
                "Set allow_live_submit only after live governance approval.",
            )
        )

    supervisor_revision_gate = _supervisor_code_revision_gate(
        supervisor_status,
        current_code_revision,
    )
    supervisor_revision_reason = str(supervisor_revision_gate.get("reason") or "")
    if (
        bool(normalized_policy.get("block_on_supervisor_code_revision_mismatch", True))
        and supervisor_revision_reason
    ):
        hard_blocks.append(supervisor_revision_reason)
        hard_block_details.append(
            _block_detail(
                supervisor_revision_reason,
                "block",
                str(supervisor_revision_gate.get("detail") or ""),
                "Restart Supervisor with the current code before allowing automated submit.",
            )
        )

    preflight = dict(preflight_summary or {})
    preflight_age_hours = age_hours_from_timestamp(preflight.get("generated_at"), now_dt)
    preflight_fail_checks = _preflight_relevant_checks(preflight, row, ["FAIL", "FAILED", "ERROR"])
    preflight_warn_checks = _preflight_relevant_checks(preflight, row, ["WARN", "WARNING"])
    has_preflight_checks = bool(list(preflight.get("checks") or []))
    preflight_fail_count = len(preflight_fail_checks) if has_preflight_checks else _int(preflight.get("fail_count"))
    preflight_warn_count = len(preflight_warn_checks) if has_preflight_checks else _int(preflight.get("warn_count"))
    if bool(normalized_policy.get("block_on_preflight_fail", True)) and preflight_fail_count > 0:
        hard_blocks.append("preflight_failed")
        hard_block_details.append(
            _block_detail(
                "preflight_failed",
                "block",
                ",".join(str(check.get("name") or "unknown") for check in preflight_fail_checks[:5])
                or f"fail_count={preflight_fail_count}",
                "Run ibkr-quant-preflight and fix failing checks for this market/portfolio.",
            )
        )
    if preflight_age_hours is None and bool(normalized_policy.get("block_on_missing_preflight", True)):
        hard_blocks.append("preflight_missing")
        hard_block_details.append(
            _block_detail(
                "preflight_missing",
                "block",
                "preflight generated_at is missing",
                "Run ibkr-quant-preflight before automated submit.",
            )
        )
    elif (
        bool(normalized_policy.get("block_on_stale_preflight", True))
        and preflight_age_hours is not None
        and preflight_age_hours > float(normalized_policy.get("max_preflight_age_hours", 24.0))
    ):
        hard_blocks.append("preflight_stale")
        hard_block_details.append(
            _block_detail(
                "preflight_stale",
                "block",
                f"age_hours={preflight_age_hours}",
                "Refresh supervisor preflight before automated submit.",
            )
        )
    if preflight_warn_count > 0:
        warnings.append("preflight_warn")
        warning_details.append(
            _block_detail(
                "preflight_warn",
                "warning",
                ",".join(str(check.get("name") or "unknown") for check in preflight_warn_checks[:5])
                or f"warn_count={preflight_warn_count}",
                "Review warning checks; paper submit may continue if no hard blocks exist.",
            )
        )

    weekly = dict(weekly_summary or {})
    weekly_age_hours = age_hours_from_timestamp(weekly.get("generated_at"), now_dt)
    if weekly_age_hours is None and bool(normalized_policy.get("block_on_missing_weekly_review", True)):
        hard_blocks.append("weekly_review_missing")
        hard_block_details.append(
            _block_detail(
                "weekly_review_missing",
                "block",
                "weekly review generated_at is missing",
                "Run weekly review before automated submit.",
            )
        )
    elif (
        bool(normalized_policy.get("block_on_stale_weekly_review", True))
        and weekly_age_hours is not None
        and weekly_age_hours > float(normalized_policy.get("max_weekly_review_age_hours", 168.0))
    ):
        hard_blocks.append("weekly_review_stale")
        hard_block_details.append(
            _block_detail(
                "weekly_review_stale",
                "block",
                f"age_hours={weekly_age_hours}",
                "Refresh weekly review before automated submit.",
            )
        )

    strategy_effectiveness = dict(weekly.get("strategy_parameter_suggestion_effectiveness") or {})
    strategy_suggestion_rows = [
        dict(item)
        for item in list(weekly.get("strategy_parameter_suggestions") or [])
        if isinstance(item, Mapping)
    ]
    matching_suggestions = _active_strategy_suggestion_rows(_strategy_suggestion_rows_for_portfolio(weekly, row))
    matching_followups = _strategy_followup_rows_for_portfolio(weekly, row)
    stale_ids = {
        str(value)
        for value in list(strategy_effectiveness.get("stale_suggestion_ids") or [])
        if str(value).strip()
    }
    if strategy_suggestion_rows:
        auto_apply_count = sum(1 for suggestion in matching_suggestions if bool(suggestion.get("auto_apply", False)))
        if list(weekly.get("strategy_parameter_suggestion_followup") or []):
            degraded_followup_count = sum(
                1
                for followup in matching_followups
                if str(followup.get("followup_verdict") or "").strip().upper() == "DEGRADED"
            )
        else:
            degraded_followup_count = 0
        open_suggestion_count = sum(
            1 for suggestion in matching_suggestions if _is_unresolved_strategy_suggestion(suggestion)
        )
        stale_suggestion_count = sum(
            1 for suggestion in matching_suggestions if str(suggestion.get("suggestion_id") or "") in stale_ids
        )
    else:
        auto_apply_count = _int(strategy_effectiveness.get("auto_apply_count"))
        degraded_followup_count = _int(strategy_effectiveness.get("degraded_followup_count"))
        open_suggestion_count = _int(strategy_effectiveness.get("open_suggestion_count"))
        stale_suggestion_count = _int(strategy_effectiveness.get("stale_suggestion_count"))
    if bool(normalized_policy.get("block_on_strategy_auto_apply_violation", True)) and auto_apply_count > 0:
        hard_blocks.append("strategy_auto_apply_violation")
        hard_block_details.append(
            _block_detail(
                "strategy_auto_apply_violation",
                "block",
                f"auto_apply_count={auto_apply_count}",
                "Disable auto-apply and review strategy parameter governance before submit.",
            )
        )
    if bool(normalized_policy.get("block_on_degraded_strategy_followup", True)) and degraded_followup_count > 0:
        hard_blocks.append("strategy_followup_degraded")
        hard_block_details.append(
            _block_detail(
                "strategy_followup_degraded",
                "block",
                f"degraded_followup_count={degraded_followup_count}",
                "Review degraded strategy follow-up before submitting more automated orders.",
            )
        )
    if stale_suggestion_count > 0:
        hard_blocks.append("strategy_suggestion_stale")
        hard_block_details.append(
            _block_detail(
                "strategy_suggestion_stale",
                "block",
                f"stale_suggestion_count={stale_suggestion_count}",
                "Acknowledge, reject, supersede, or apply stale strategy suggestions before submit.",
            )
        )
    if bool(normalized_policy.get("warn_on_open_strategy_suggestions", True)) and open_suggestion_count > 0:
        warnings.append("strategy_suggestions_open")
        warning_details.append(
            _block_detail(
                "strategy_suggestions_open",
                "warning",
                f"open_suggestion_count={open_suggestion_count}",
                "Paper submit may continue; keep tracking open strategy suggestions.",
            )
        )

    gateway_row = _gateway_budget_row(weekly, row)
    gateway_status = _gateway_budget_status(weekly, row)
    if bool(normalized_policy.get("block_on_gateway_budget_degraded", True)) and _gateway_budget_submit_blocking(
        gateway_row,
        gateway_status,
    ):
        hard_blocks.append("gateway_budget_degraded")
        hard_block_details.append(
            _block_detail(
                "gateway_budget_degraded",
                "block",
                _gateway_budget_detail(gateway_row, row, gateway_status),
                _gateway_budget_remediation(gateway_row, blocked=True),
            )
        )
    elif gateway_status in {"fail", "failed", "error", "degraded"}:
        warnings.append("gateway_budget_research_degraded")
        warning_details.append(
            _block_detail(
                "gateway_budget_research_degraded",
                "warning",
                _gateway_budget_detail(gateway_row, row, gateway_status),
                _gateway_budget_remediation(gateway_row, blocked=False),
            )
        )
    elif gateway_status in {"warn", "warning"}:
        warnings.append("gateway_budget_warning")
        warning_details.append(
            _block_detail(
                "gateway_budget_warning",
                "warning",
                _gateway_budget_detail(gateway_row, row, gateway_status),
                _gateway_budget_remediation(gateway_row, blocked=False),
            )
        )

    market_readiness = dict(_market_readiness_row(market_readiness_summary, row))
    market_readiness_status = str(market_readiness.get("readiness_status") or "").strip().upper()
    market_readiness_reason = str(market_readiness.get("primary_reason") or "").strip()
    market_artifact_health_status = str(market_readiness.get("artifact_health_status") or "").strip().upper()
    market_feasibility_status = str(market_readiness.get("small_account_feasibility_status") or "").strip().upper()
    market_artifact_age_hours = (
        _float(market_readiness.get("execution_artifact_age_hours"), 0.0)
        if market_readiness
        else None
    )
    market_order_count = _int(market_readiness.get("order_count"), 0)
    market_planned_gross_order_value = _float(market_readiness.get("planned_gross_order_value"), 0.0)
    market_planned_order_symbols = str(market_readiness.get("planned_order_symbols") or "").strip()
    submit_quality_status = str(market_readiness.get("submit_quality_status") or "").strip().upper()
    submit_quality_tier = str(market_readiness.get("submit_quality_tier") or "").strip().upper()
    submit_quality_reason = str(market_readiness.get("submit_quality_reason") or "").strip()
    market_preparation_tier = ""
    for plan_row in list((market_readiness_summary or {}).get("preparation_plan") or []):
        if not isinstance(plan_row, Mapping):
            continue
        if str(plan_row.get("portfolio_id") or "").strip() == str(row.get("portfolio_id") or "").strip():
            market_preparation_tier = str(plan_row.get("priority_tier") or "").strip().upper()
            break
    ready_market_statuses = {"READY_FOR_PAPER_REVIEW"}
    if not market_readiness:
        detail = "market_readiness row missing"
        if bool(normalized_policy.get("block_on_missing_market_readiness", False)):
            hard_blocks.append("market_readiness_missing")
            hard_block_details.append(
                _block_detail(
                    "market_readiness_missing",
                    "block",
                    detail,
                    "Run ibkr-quant-market-readiness before automated submit.",
                )
            )
        elif bool(normalized_policy.get("warn_on_missing_market_readiness", False)):
            warnings.append("market_readiness_missing")
            warning_details.append(
                _block_detail(
                    "market_readiness_missing",
                    "warning",
                    detail,
                    "Run ibkr-quant-market-readiness before ranking ASX/HK/XETRA/US paper submit readiness.",
                )
            )
    elif (
        bool(normalized_policy.get("block_on_market_readiness_not_ready", True))
        and market_readiness_status not in ready_market_statuses
    ):
        if market_readiness_reason.upper() == "IBKR_GATEWAY_UNAVAILABLE" or market_artifact_health_status == "DEGRADED_GATEWAY":
            hard_blocks.append("ibkr_gateway_unavailable")
            hard_block_details.append(
                _block_detail(
                    "ibkr_gateway_unavailable",
                    "block",
                    (
                        f"status={market_readiness_status or '-'} reason={market_readiness_reason or '-'} "
                        f"artifact={market_artifact_health_status or '-'}"
                    ),
                    "Start or unlock IB Gateway paper API, confirm the configured port is listening, then rerun no-submit.",
                )
            )
        hard_blocks.append("market_readiness_not_ready")
        hard_block_details.append(
            _block_detail(
                "market_readiness_not_ready",
                "block",
                (
                    f"status={market_readiness_status} reason={market_readiness_reason or '-'} "
                    f"artifact={market_artifact_health_status or '-'} feasibility={market_feasibility_status or '-'}"
                ),
                str(market_readiness.get("next_action") or "Refresh market readiness before automated submit."),
            )
        )
    elif market_artifact_health_status in {"STALE", "DEGRADED_GATEWAY", "MISSING"}:
        warnings.append("market_readiness_artifact_health")
        warning_details.append(
            _block_detail(
                "market_readiness_artifact_health",
                "warning",
                f"artifact={market_artifact_health_status}",
                "Review market readiness artifact health before submit.",
            )
        )
    if (
        bool(normalized_policy.get("block_on_submit_quality_not_pass", True))
        and market_order_count > 0
        and market_readiness_status == "READY_FOR_PAPER_REVIEW"
        and submit_quality_status != "PASS"
    ):
        hard_blocks.append("submit_quality_not_pass")
        hard_block_details.append(
            _block_detail(
                "submit_quality_not_pass",
                "block",
                (
                    f"status={submit_quality_status or 'MISSING'} reason={submit_quality_reason or '-'} "
                    f"net_edge={_float(market_readiness.get('submit_quality_min_net_edge_bps'), 0.0):.2f}bps "
                    f"margin={_float(market_readiness.get('submit_quality_min_edge_margin_bps'), 0.0):.2f}bps "
                    f"cost={_float(market_readiness.get('submit_quality_max_expected_cost_bps'), 0.0):.2f}bps "
                    f"types={market_readiness.get('submit_quality_order_types') or '-'}"
                ),
                "Refresh execution dry-run or improve edge/cost/order-type quality before automated submit.",
            )
        )

    offline_recovery = _offline_recovery_state(
        preflight_age_hours=preflight_age_hours,
        weekly_age_hours=weekly_age_hours,
        market_readiness_present=bool(market_readiness),
        market_artifact_health_status=market_artifact_health_status,
        market_artifact_age_hours=market_artifact_age_hours,
        gateway_status=gateway_status,
        gateway_row=gateway_row,
        policy=normalized_policy,
    )
    ready = not hard_blocks
    status = READY_STATUS if ready and not warnings else WARNING_STATUS if ready else BLOCKED_STATUS
    primary_reason = "ready" if ready else _primary_hard_block_reason(hard_blocks)
    return {
        **row,
        "status": status,
        "ready": ready,
        "primary_reason": primary_reason,
        "hard_blocks": hard_blocks,
        "warnings": warnings,
        "hard_block_details": hard_block_details,
        "warning_details": warning_details,
        "policy_enabled": bool(normalized_policy.get("enabled", False)),
        "account_mode": account_mode,
        "preflight_age_hours": preflight_age_hours,
        "preflight_fail_count": preflight_fail_count,
        "preflight_warn_count": preflight_warn_count,
        "weekly_review_age_hours": weekly_age_hours,
        "strategy_open_suggestion_count": open_suggestion_count,
        "strategy_degraded_followup_count": degraded_followup_count,
        "strategy_auto_apply_count": auto_apply_count,
        "strategy_stale_suggestion_count": stale_suggestion_count,
        "gateway_budget_status": gateway_status or "",
        "gateway_budget_request_count": _int(gateway_row.get("gateway_request_count"), 0),
        "gateway_budget_request_limit": _int(gateway_row.get("weekly_gateway_request_budget"), 0),
        "gateway_budget_usage_pct": _float(gateway_row.get("budget_usage_pct"), 0.0),
        "gateway_budget_submit_blocking": _gateway_budget_submit_blocking(gateway_row, gateway_status),
        "gateway_execution_capacity_status": str(gateway_row.get("execution_capacity_status") or ""),
        "gateway_execution_request_count": _int(gateway_row.get("execution_gateway_request_count"), 0),
        "gateway_execution_request_limit": _int(gateway_row.get("execution_reserve_weekly_requests"), 0),
        "gateway_research_throttled": bool(gateway_row.get("research_throttled", False)),
        "gateway_research_recent_24h_request_count": _int(
            gateway_row.get("research_recent_24h_request_count"),
            0,
        ),
        "gateway_research_daily_request_budget": _int(gateway_row.get("research_daily_request_budget"), 0),
        "gateway_short_window_request_count": _int(gateway_row.get("short_window_gateway_request_count"), 0),
        "gateway_short_window_request_limit": _int(gateway_row.get("short_window_request_limit"), 0),
        "gateway_short_window_execution_request_count": _int(
            gateway_row.get("short_window_execution_request_count"),
            0,
        ),
        "gateway_short_window_execution_reserve": _int(
            gateway_row.get("short_window_execution_reserve"),
            0,
        ),
        "gateway_budget_top_request_kind": str(gateway_row.get("top_request_kind") or ""),
        "gateway_budget_top_tool": str(gateway_row.get("top_tool") or ""),
        "gateway_budget_projected_recovery_at": str(gateway_row.get("projected_recovery_at") or ""),
        "market_readiness_status": market_readiness_status,
        "market_readiness_reason": market_readiness_reason,
        "market_readiness_artifact_health_status": market_artifact_health_status,
        "market_readiness_artifact_age_hours": market_artifact_age_hours,
        "market_readiness_feasibility_status": market_feasibility_status,
        "market_readiness_preparation_tier": market_preparation_tier,
        "market_readiness_order_count": market_order_count,
        "market_readiness_planned_gross_order_value": market_planned_gross_order_value,
        "market_readiness_planned_buy_order_value": _float(market_readiness.get("planned_buy_order_value"), 0.0),
        "market_readiness_planned_sell_order_value": _float(market_readiness.get("planned_sell_order_value"), 0.0),
        "market_readiness_planned_net_cash_order_value": _float(
            market_readiness.get("planned_net_cash_order_value"),
            0.0,
        ),
        "market_readiness_planned_order_symbols": market_planned_order_symbols,
        "market_readiness_next_action": str(market_readiness.get("next_action") or ""),
        "post_cost_calibration_status": str(market_readiness.get("post_cost_calibration_status") or ""),
        "post_cost_calibration_reason": str(market_readiness.get("post_cost_calibration_reason") or ""),
        "post_cost_primary_action": str(market_readiness.get("post_cost_primary_action") or ""),
        "post_cost_candidate_count": _int(market_readiness.get("post_cost_candidate_count"), 0),
        "post_cost_high_cost_candidate_count": _int(
            market_readiness.get("post_cost_high_cost_candidate_count"),
            0,
        ),
        "post_cost_positive_edge_count": _int(market_readiness.get("post_cost_positive_edge_count"), 0),
        "post_cost_high_cost_positive_edge_count": _int(
            market_readiness.get("post_cost_high_cost_positive_edge_count"),
            0,
        ),
        "post_cost_avg_expected_cost_bps": _float(
            market_readiness.get("post_cost_avg_expected_cost_bps"),
            0.0,
        ),
        "post_cost_avg_post_cost_edge_bps": _float(
            market_readiness.get("post_cost_avg_post_cost_edge_bps"),
            0.0,
        ),
        "post_cost_top_symbols": str(market_readiness.get("post_cost_top_symbols") or ""),
        "wait_pullback_calibration_status": str(market_readiness.get("wait_pullback_calibration_status") or ""),
        "wait_pullback_calibration_reason": str(market_readiness.get("wait_pullback_calibration_reason") or ""),
        "wait_pullback_primary_action": str(market_readiness.get("wait_pullback_primary_action") or ""),
        "wait_pullback_count": _int(market_readiness.get("wait_pullback_count"), 0),
        "wait_pullback_close_count": _int(market_readiness.get("wait_pullback_close_count"), 0),
        "wait_pullback_near_candidate_count": _int(
            market_readiness.get("wait_pullback_near_candidate_count"),
            0,
        ),
        "wait_pullback_avg_gap_pct": _float(market_readiness.get("wait_pullback_avg_gap_pct"), 0.0),
        "wait_pullback_min_gap_pct": _float(market_readiness.get("wait_pullback_min_gap_pct"), 0.0),
        "wait_pullback_dominant_anchor_component": str(
            market_readiness.get("wait_pullback_dominant_anchor_component") or ""
        ),
        "wait_pullback_top_symbols": str(market_readiness.get("wait_pullback_top_symbols") or ""),
        "submit_quality_status": submit_quality_status,
        "submit_quality_tier": submit_quality_tier,
        "submit_quality_reason": submit_quality_reason,
        "submit_quality_min_net_edge_bps": _float(market_readiness.get("submit_quality_min_net_edge_bps"), 0.0),
        "submit_quality_min_edge_margin_bps": _float(market_readiness.get("submit_quality_min_edge_margin_bps"), 0.0),
        "submit_quality_max_expected_cost_bps": _float(market_readiness.get("submit_quality_max_expected_cost_bps"), 0.0),
        "submit_quality_order_types": str(market_readiness.get("submit_quality_order_types") or ""),
        "supervisor_code_revision_status": str(supervisor_revision_gate.get("revision_status") or ""),
        "supervisor_code_revision": str(supervisor_revision_gate.get("supervisor_code_revision") or ""),
        "current_code_revision": str(supervisor_revision_gate.get("current_code_revision") or ""),
        "supervisor_runtime_next_action": str(supervisor_revision_gate.get("runtime_next_action") or ""),
        "supervisor_runtime_restart_required": bool(
            supervisor_revision_gate.get("runtime_restart_required")
        ),
        "supervisor_runtime_blocks_recovery_refresh": bool(
            supervisor_revision_gate.get("runtime_blocks_recovery_refresh")
        ),
        "supervisor_runtime_request_policy": str(
            supervisor_revision_gate.get("runtime_request_policy") or ""
        ),
        "supervisor_runtime_health_status": str(
            supervisor_revision_gate.get("runtime_health_status") or ""
        ),
        **offline_recovery,
    }


def build_auto_order_submit_plan(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any] | None = None,
    weekly_summary: Mapping[str, Any] | None = None,
    account_growth_tier_plan: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_policy = normalize_auto_order_readiness_policy(policy)
    capacity_plan = build_auto_order_submit_capacity_plan(
        weekly_summary,
        policy=normalized_policy,
    )
    account_growth_context = _account_growth_submit_context(account_growth_tier_plan)
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    if not bool(normalized_policy.get("enabled", False)):
        return {
            "status": "DISABLED",
            "ready": False,
            "reason": "auto_order_readiness_policy_disabled",
            "candidate_count": 0,
            "candidate_portfolios": [],
            "frontier_candidate_count": 0,
            "frontier_candidates": [],
            "selected_portfolio_id": "",
            "submit_mode": "none",
            "submit_capacity_plan": capacity_plan,
        }

    eligible_rows = [
        row
        for row in clean_rows
        if bool(row.get("ready", False))
        and str(row.get("account_mode") or "paper").strip().lower() == "paper"
        and str(row.get("market_readiness_status") or "").strip().upper() == "READY_FOR_PAPER_REVIEW"
    ]
    max_portfolios = max(
        1,
        _int(
            capacity_plan.get("effective_max_submit_portfolios_per_run"),
            _int(normalized_policy.get("max_submit_portfolios_per_run"), 1),
        ),
    )
    max_per_market = max(1, _int(normalized_policy.get("max_submit_portfolios_per_market"), 1))
    max_orders = max(1, _int(normalized_policy.get("max_submit_orders_per_portfolio"), 1))
    max_value = max(0.0, _float(normalized_policy.get("max_submit_gross_order_value"), 100.0))
    account_max_orders = _int(account_growth_context.get("max_orders_per_run"), 0)
    if account_max_orders > 0:
        max_orders = max(1, min(max_orders, account_max_orders))
    account_max_value = _float(account_growth_context.get("max_order_value"), 0.0)
    if account_max_value > 0.0:
        max_value = max(0.0, min(max_value, account_max_value))
    max_total_value = max(
        0.0,
        _float(
            capacity_plan.get("effective_max_submit_total_gross_order_value"),
            _float(normalized_policy.get("max_submit_total_gross_order_value"), 0.0),
        ),
    )
    require_buy = bool(normalized_policy.get("require_buy_order_for_submit", False))
    policy_snapshot = _submit_plan_policy_snapshot(
        normalized_policy,
        max_portfolios=max_portfolios,
        max_per_market=max_per_market,
        max_orders=max_orders,
        max_value=max_value,
        max_total_value=max_total_value,
        require_buy=require_buy,
        account_growth_context=account_growth_context,
    )
    frontier_candidates = _build_submit_frontier_candidates(
        clean_rows,
        max_orders=max_orders,
        max_value=max_value,
        require_buy=require_buy,
        account_growth_context=account_growth_context,
    )
    candidate_rows: List[Dict[str, Any]] = []
    rejection_rows: List[Dict[str, Any]] = []
    for row in eligible_rows:
        order_count = _int(row.get("market_readiness_order_count"), 0)
        planned_gross = _float(row.get("market_readiness_planned_gross_order_value"), 0.0)
        planned_buy = _float(row.get("market_readiness_planned_buy_order_value"), 0.0)
        reject_reasons = _submit_policy_reject_reasons(
            order_count,
            planned_gross,
            max_orders=max_orders,
            max_value=max_value,
            planned_buy=planned_buy,
            require_buy=require_buy,
            account_growth_context=account_growth_context,
        )
        base = _submit_candidate_base(
            row,
            order_count=order_count,
            planned_gross=planned_gross,
            account_growth_context=account_growth_context,
        )
        if reject_reasons:
            rejection_rows.append({**base, "reject_reasons": reject_reasons})
        else:
            candidate_rows.append(base)

    candidate_rows.sort(
        key=lambda row: (
            _submit_quality_rank(row),
            -float(row.get("submit_quality_min_net_edge_bps", 0.0) or 0.0),
            -float(row.get("submit_quality_min_edge_margin_bps", 0.0) or 0.0),
            float(row.get("planned_gross_order_value", 0.0) or 0.0),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    evidence_ready_markets = {
        _market(row.get("market"))
        for row in list(capacity_plan.get("market_evidence_rows") or [])
        if isinstance(row, Mapping) and bool(row.get("evidence_ready", False))
    }
    enforce_unevidenced_market_limit = str(capacity_plan.get("scale_stage") or "") in {"trial", "full"}
    max_unevidenced_markets = (
        max(0, _int(normalized_policy.get("max_submit_unevidenced_markets_per_run"), 1))
        if enforce_unevidenced_market_limit
        else len(candidate_rows)
    )
    unevidenced_markets: set[str] = set()
    market_counts: Dict[str, int] = {}
    market_limited_rows: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        market = _market(candidate.get("market"))
        if market not in evidence_ready_markets and market not in unevidenced_markets:
            if len(unevidenced_markets) >= max_unevidenced_markets:
                rejection_rows.append(
                    {
                        **candidate,
                        "reject_reasons": ["unevidenced_market_count_exceeds_policy"],
                    }
                )
                continue
            unevidenced_markets.add(market)
        current_count = int(market_counts.get(market, 0))
        if current_count >= max_per_market:
            rejection_rows.append({**candidate, "reject_reasons": ["market_portfolio_count_exceeds_policy"]})
            continue
        market_counts[market] = current_count + 1
        market_limited_rows.append(candidate)
    if not candidate_rows:
        return {
            "status": "BLOCKED",
            "ready": False,
            "reason": "no_single_safe_submit_candidate",
            "candidate_count": 0,
            "candidate_portfolios": [],
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "submit_mode": "none",
            "policy": policy_snapshot,
            "submit_capacity_plan": capacity_plan,
        }
    if len(market_limited_rows) > max_portfolios:
        return {
            "status": "REVIEW_REQUIRED",
            "ready": False,
            "reason": "multiple_submit_candidates_require_operator_selection",
            "candidate_count": int(len(market_limited_rows)),
            "candidate_portfolios": candidate_rows,
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "submit_mode": "operator_select_one",
            "policy": policy_snapshot,
            "submit_capacity_plan": capacity_plan,
        }
    selected_rows = [dict(row) for row in market_limited_rows]
    selected_total_gross = round(
        sum(float(row.get("planned_gross_order_value", 0.0) or 0.0) for row in selected_rows),
        2,
    )
    if max_total_value > 0.0 and selected_total_gross > max_total_value + 1e-9:
        return {
            "status": "REVIEW_REQUIRED",
            "ready": False,
            "reason": "submit_total_gross_value_exceeds_policy",
            "candidate_count": int(len(selected_rows)),
            "candidate_portfolios": candidate_rows,
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "selected_portfolio_ids": [],
            "submit_mode": "operator_reduce_total_exposure",
            "selected_total_planned_gross_order_value": float(selected_total_gross),
            "policy": policy_snapshot,
            "submit_capacity_plan": capacity_plan,
        }
    selected = dict(selected_rows[0])
    selected_portfolio_ids = [str(row.get("portfolio_id") or "") for row in selected_rows if str(row.get("portfolio_id") or "")]
    selected_markets = [str(row.get("market") or "") for row in selected_rows if str(row.get("market") or "")]
    multi = len(selected_rows) > 1
    return {
        "status": "READY_MULTI_CANDIDATE" if multi else "READY_SINGLE_CANDIDATE",
        "ready": True,
        "reason": "multi_market_safe_paper_submit_candidates" if multi else "single_safe_paper_submit_candidate",
        "candidate_count": int(len(selected_rows)),
        "candidate_portfolios": candidate_rows,
        "rejected_candidates": rejection_rows,
        "frontier_candidate_count": len(frontier_candidates),
        "frontier_candidates": frontier_candidates,
        "selected_market": str(selected.get("market") or ""),
        "selected_markets": selected_markets,
        "selected_portfolio_id": str(selected.get("portfolio_id") or ""),
        "selected_portfolio_ids": selected_portfolio_ids,
        "selected_portfolios": selected_rows,
        "selected_order_count": int(selected.get("order_count", 0) or 0),
        "selected_total_order_count": int(sum(int(row.get("order_count", 0) or 0) for row in selected_rows)),
        "selected_planned_gross_order_value": float(selected.get("planned_gross_order_value", 0.0) or 0.0),
        "selected_total_planned_gross_order_value": float(selected_total_gross),
        "selected_planned_order_symbols": str(selected.get("planned_order_symbols") or ""),
        "submit_mode": "paper_multi_market_small_plan" if multi else "paper_one_portfolio_one_small_plan",
        "policy": policy_snapshot,
        "submit_capacity_plan": capacity_plan,
    }


def build_auto_order_frequency_plan(
    rows: Iterable[Mapping[str, Any]],
    *,
    submit_plan: Mapping[str, Any] | None = None,
    watchlist_expansion_summary: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Explain the next safe path to increase paper submit frequency without changing gates."""
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    plan = dict(submit_plan or {})
    expansion = dict(watchlist_expansion_summary or {})
    capacity_plan = dict(plan.get("submit_capacity_plan") or {})
    seed_proposals = [
        dict(row)
        for row in list(expansion.get("seed_proposals") or [])
        if isinstance(row, Mapping)
    ]
    seed_intake_plan = [
        dict(row)
        for row in list(expansion.get("seed_intake_plan") or [])
        if isinstance(row, Mapping)
    ]
    seed_evidence_queue = [
        dict(row)
        for row in list(expansion.get("seed_evidence_queue") or [])
        if isinstance(row, Mapping)
    ]
    seed_promotion_review = [
        dict(row)
        for row in list(expansion.get("seed_promotion_review") or [])
        if isinstance(row, Mapping)
    ]
    seed_quality_feedback = (
        dict(expansion.get("seed_quality_feedback") or {})
        or summarize_seed_promotion_quality(seed_promotion_review)
    )
    seed_quality_rejected_count = _int(seed_quality_feedback.get("quality_rejected_count"), 0)
    seed_primary_quality_reason = str(seed_quality_feedback.get("primary_quality_reason") or "")
    seed_quality_reason_counts = dict(seed_quality_feedback.get("quality_reason_counts") or {})
    ready_seed_evidence_jobs = [
        row
        for row in seed_evidence_queue
        if str(row.get("status") or "").strip().upper() == "READY"
    ]
    primary_seed_job = dict(ready_seed_evidence_jobs[0]) if ready_seed_evidence_jobs else {}
    primary_seed_market = str(
        expansion.get("seed_evidence_primary_market") or primary_seed_job.get("market") or ""
    ).strip().upper()
    primary_seed_symbols = [
        str(symbol or "").strip().upper()
        for symbol in list(
            expansion.get("seed_evidence_primary_symbols")
            or primary_seed_job.get("symbols")
            or []
        )
        if str(symbol or "").strip()
    ]
    primary_seed_mode = str(
        expansion.get("seed_evidence_mode")
        or primary_seed_job.get("evidence_mode")
        or primary_seed_job.get("mode")
        or ""
    ).strip()
    submit_ready = bool(plan.get("ready", False))
    submit_reason = str(plan.get("reason") or "").strip()
    submit_status = str(plan.get("status") or "").strip().upper()
    frontier_candidates = [
        dict(row)
        for row in list(plan.get("frontier_candidates") or [])
        if isinstance(row, Mapping)
    ]
    top_frontier = dict(frontier_candidates[0]) if frontier_candidates else {}
    top_frontier_hard_blocks = [
        str(value).strip()
        for value in list(top_frontier.get("hard_blocks") or [])
        if str(value).strip()
    ]
    safe_candidate_count = _int(plan.get("candidate_count"), 0)
    if submit_ready:
        status = "safe_submit_candidate_ready"
        reason = submit_reason or "submit_plan_ready"
        primary_action = "submit_selected_paper_plan_once"
    elif ready_seed_evidence_jobs:
        status = "seed_evidence_queue_ready"
        reason = "source_verified_candidates_need_candidate_report"
        primary_action = "run_seed_candidate_evidence_review"
    elif (
        seed_quality_rejected_count > 0
        and seed_quality_rejected_count == len(seed_promotion_review)
    ):
        status = "seed_evidence_quality_rejected"
        reason = (
            f"seed_candidate_quality_rejected:{seed_primary_quality_reason}"
            if seed_primary_quality_reason
            else "seed_candidate_quality_rejected"
        )
        primary_action = str(
            seed_quality_feedback.get("primary_action")
            or "source_higher_quality_lower_cost_seed_candidates"
        )
    elif top_frontier_hard_blocks:
        status = "frontier_blocked"
        reason = str(top_frontier.get("frontier_reason") or top_frontier_hard_blocks[0] or "frontier_not_ready")
        primary_action = str(top_frontier.get("next_action") or "resolve_submit_frontier_blocker")
    elif seed_proposals and submit_reason == "no_single_safe_submit_candidate":
        status = "candidate_supply_gap"
        reason = "no_safe_submit_candidate_with_seed_proposals"
        primary_action = str(seed_proposals[0].get("proposal_action") or "review_watchlist_seed_proposals")
    elif seed_proposals:
        status = "candidate_supply_watch"
        reason = submit_reason or "submit_plan_not_ready_with_seed_proposals"
        primary_action = str(seed_proposals[0].get("proposal_action") or "review_watchlist_seed_proposals")
    elif frontier_candidates:
        status = "frontier_blocked"
        reason = str(frontier_candidates[0].get("frontier_reason") or submit_reason or "frontier_not_ready")
        primary_action = str(frontier_candidates[0].get("next_action") or "resolve_submit_frontier_blocker")
    else:
        status = "insufficient_submit_evidence"
        reason = submit_reason or submit_status.lower() or "missing_submit_frontier"
        primary_action = "refresh_preflight_market_readiness_and_execution_dry_run"
    proposal_rows = [
        {
            "market": str(row.get("market") or ""),
            "proposal_action": str(row.get("proposal_action") or ""),
            "expansion_target": str(row.get("expansion_target") or ""),
            "near_miss_symbols": list(row.get("near_miss_symbols") or [])[:5],
            "auto_apply": bool(row.get("auto_apply", False)),
            "submit_gate_policy": str(row.get("submit_gate_policy") or ""),
        }
        for row in seed_proposals[:10]
    ]
    return {
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "submit_plan_status": submit_status,
        "submit_plan_reason": submit_reason,
        "safe_submit_candidate_count": safe_candidate_count,
        "frontier_candidate_count": len(frontier_candidates),
        "seed_proposal_count": len(seed_proposals),
        "manual_seed_proposal_count": sum(1 for row in seed_proposals if not bool(row.get("auto_apply", False))),
        "seed_proposal_markets": [
            str(row.get("market") or "")
            for row in seed_proposals
            if str(row.get("market") or "").strip()
        ],
        "seed_intake_plan_count": len(seed_intake_plan),
        "seed_source_candidate_count": sum(int(row.get("source_candidate_count", 0) or 0) for row in seed_intake_plan),
        "seed_source_markets": [
            str(row.get("market") or "")
            for row in seed_intake_plan
            if int(row.get("source_candidate_count", 0) or 0) > 0 and str(row.get("market") or "").strip()
        ],
        "seed_intake_external_source_count": sum(
            1
            for row in seed_intake_plan
            if str(row.get("intake_status") or "") == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
        ),
        "seed_promotion_review_count": _int(expansion.get("seed_promotion_review_count"), 0),
        "seed_promotion_ready_count": _int(expansion.get("seed_promotion_ready_count"), 0),
        "seed_promotion_mapping_required_count": _int(
            expansion.get("seed_promotion_mapping_required_count"),
            0,
        ),
        "seed_promotion_candidate_report_required_count": _int(
            expansion.get("seed_promotion_candidate_report_required_count"),
            0,
        ),
        "seed_quality_feedback": seed_quality_feedback,
        "seed_promotion_quality_rejected_count": seed_quality_rejected_count,
        "seed_promotion_quality_reason_counts": dict(sorted(seed_quality_reason_counts.items())),
        "seed_promotion_primary_quality_reason": seed_primary_quality_reason,
        "seed_replacement_primary_action": str(seed_quality_feedback.get("primary_action") or ""),
        "seed_evidence_queue_count": len(seed_evidence_queue),
        "seed_evidence_ready_job_count": len(ready_seed_evidence_jobs),
        "seed_evidence_primary_market": primary_seed_market,
        "seed_evidence_primary_symbols": primary_seed_symbols,
        "seed_evidence_mode": primary_seed_mode,
        "seed_evidence_jobs": [
            {
                "market": str(row.get("market") or "").strip().upper(),
                "status": str(row.get("status") or "").strip().upper(),
                "symbols": [
                    str(symbol or "").strip().upper()
                    for symbol in list(row.get("symbols") or [])
                    if str(symbol or "").strip()
                ],
                "evidence_mode": str(row.get("evidence_mode") or row.get("mode") or "").strip(),
                "submit_orders": bool(row.get("submit_orders", False)),
            }
            for row in seed_evidence_queue[:10]
        ],
        "submit_capacity_status": str(capacity_plan.get("status") or ""),
        "submit_capacity_reason": str(capacity_plan.get("reason") or ""),
        "submit_capacity_scale_allowed": bool(capacity_plan.get("scale_allowed", False)),
        "submit_capacity_scale_stage": str(capacity_plan.get("scale_stage") or ""),
        "submit_capacity_evidence_market_count": _int(
            capacity_plan.get("evidence_market_count"),
            0,
        ),
        "effective_max_submit_portfolios_per_run": _int(
            capacity_plan.get("effective_max_submit_portfolios_per_run"),
            0,
        ),
        "effective_max_submit_total_gross_order_value": _float(
            capacity_plan.get("effective_max_submit_total_gross_order_value"),
            0.0,
        ),
        "portfolio_count": len(clean_rows),
        "does_not_change_submit_decision": True,
        "submit_gate_policy": "do_not_relax_submit_gates",
        "next_actions": proposal_rows,
    }


def build_auto_order_recovery_plan(
    rows: Iterable[Mapping[str, Any]],
    *,
    submit_plan: Mapping[str, Any] | None = None,
    stale_execution_refresh_plan: Mapping[str, Any] | None = None,
    global_hard_blocks: Mapping[str, Any] | Iterable[str] | None = None,
) -> Dict[str, Any]:
    """Build the minimum-request recovery path for the highest-quality paper frontier."""
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    plan = dict(submit_plan or {})
    stale_plan = dict(stale_execution_refresh_plan or {})
    frontier_candidates = [
        dict(row)
        for row in list(plan.get("frontier_candidates") or [])
        if isinstance(row, Mapping)
    ]
    top_frontier = dict(frontier_candidates[0]) if frontier_candidates else {}
    if isinstance(global_hard_blocks, Mapping):
        global_block_reasons = {
            str(reason or "").strip()
            for reason, count in dict(global_hard_blocks).items()
            if str(reason or "").strip() and _int(count, 0) > 0
        }
    else:
        global_block_reasons = {
            str(reason or "").strip()
            for reason in list(global_hard_blocks or [])
            if str(reason or "").strip()
        }
    supervisor_revision_blocks = [
        reason
        for reason in ("supervisor_code_revision_missing", "supervisor_code_revision_mismatch")
        if reason in global_block_reasons
    ]
    if supervisor_revision_blocks:
        primary_reason = supervisor_revision_blocks[0]
        return {
            "status": "runtime_restart_required",
            "primary_action": "restart_supervisor_current_code",
            "reason": primary_reason,
            "target_market": "",
            "target_portfolio_id": "",
            "target_symbols": "",
            "target_submit_quality_status": "",
            "target_net_edge_bps": 0.0,
            "target_edge_margin_bps": 0.0,
            "target_ranking_bucket": "",
            "target_gateway_budget_blocked": False,
            "gateway_budget_projected_recovery_at": "",
            "gateway_refresh_portfolio_limit": 0,
            "estimated_gateway_refresh_count": 0,
            "request_policy": "restart_supervisor_before_any_recovery_refresh",
            "step_count": 1,
            "steps": [
                {
                    "order": 1,
                    "phase": "runtime_restart",
                    "action": "restart_supervisor_current_code",
                    "requires_ibkr_gateway": False,
                    "market": "",
                    "portfolio_id": "",
                    "condition": "running Supervisor code revision is missing or mismatched; do not run recovery refresh before restart",
                    "submit_orders": False,
                }
            ],
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        }
    if bool(plan.get("ready", False)):
        selected_portfolio_id = str(plan.get("selected_portfolio_id") or "").strip()
        selected_market = _market(plan.get("selected_market"))
        return {
            "status": "submit_review_ready",
            "primary_action": "operator_review_selected_paper_plan",
            "target_market": selected_market,
            "target_portfolio_id": selected_portfolio_id,
            "target_symbols": str(plan.get("selected_planned_order_symbols") or ""),
            "target_submit_quality_status": str(top_frontier.get("submit_quality_status") or "").strip().upper(),
            "target_net_edge_bps": _float(top_frontier.get("submit_quality_min_net_edge_bps"), 0.0),
            "target_edge_margin_bps": _float(top_frontier.get("submit_quality_min_edge_margin_bps"), 0.0),
            "gateway_budget_projected_recovery_at": "",
            "gateway_refresh_portfolio_limit": 0,
            "estimated_gateway_refresh_count": 0,
            "request_policy": "no_refresh_when_submit_plan_is_ready",
            "step_count": 1,
            "steps": [
                {
                    "order": 1,
                    "phase": "operator_review",
                    "action": "operator_review_selected_paper_plan",
                    "requires_ibkr_gateway": False,
                    "market": selected_market,
                    "portfolio_id": selected_portfolio_id,
                    "condition": "review the selected paper plan before any submit action",
                    "submit_orders": False,
                }
            ],
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        }
    target_quality = str(top_frontier.get("submit_quality_status") or "").strip().upper()
    target_portfolio_id = str(top_frontier.get("portfolio_id") or "").strip()
    target_market = _market(top_frontier.get("market"))
    actionable_target = bool(target_portfolio_id and target_quality == "PASS")
    stale_primary_market = _market(stale_plan.get("primary_market"))
    stale_primary_portfolio_id = str(stale_plan.get("primary_portfolio_id") or "").strip()
    stale_primary = next(
        (
            dict(row)
            for row in list(stale_plan.get("rows") or [])
            if isinstance(row, Mapping)
            and _market(row.get("market")) == stale_primary_market
            and str(row.get("portfolio_id") or "").strip() == stale_primary_portfolio_id
        ),
        {},
    )
    stale_target_available = bool(
        not actionable_target
        and str(stale_plan.get("status") or "").strip().upper() == "READY_FOR_TARGETED_NO_SUBMIT_REFRESH"
        and stale_primary_market
        and stale_primary_portfolio_id
        and not bool(stale_primary.get("gateway_budget_blocked", False))
    )
    target_hard_blocks = {
        str(reason or "").strip()
        for reason in list(top_frontier.get("hard_blocks") or [])
        if str(reason or "").strip()
    }
    # Recovery is target-scoped. Aggregate blockers from unrelated portfolios must
    # not create a global recovery mode when no quality-passing frontier exists.
    operational_blocks = target_hard_blocks if actionable_target else set()
    recovery_rows = [
        row
        for row in clean_rows
        if (
            not actionable_target
            or (
                str(row.get("portfolio_id") or "").strip() == target_portfolio_id
                or (
                    not str(row.get("portfolio_id") or "").strip()
                    and _market(row.get("market")) == target_market
                )
            )
        )
    ]
    recovery_times: set[str] = {
        str(row.get("gateway_budget_projected_recovery_at") or "").strip()
        for row in recovery_rows
        if str(row.get("gateway_budget_projected_recovery_at") or "").strip()
    }
    for row in recovery_rows:
        for detail in list(row.get("hard_block_details") or []) + list(row.get("warning_details") or []):
            if not isinstance(detail, Mapping):
                continue
            detail_text = str(detail.get("detail") or "")
            marker = "projected_recovery_at="
            if marker not in detail_text:
                continue
            recovery_at = detail_text.partition(marker)[2].split()[0].strip()
            if recovery_at:
                recovery_times.add(recovery_at)
    sorted_recovery_times = sorted(recovery_times)
    steps: List[Dict[str, Any]] = []

    def add_step(
        action: str,
        *,
        phase: str,
        requires_gateway: bool,
        portfolio_id: str = "",
        market: str = "",
        condition: str = "",
    ) -> None:
        steps.append(
            {
                "order": len(steps) + 1,
                "phase": phase,
                "action": action,
                "requires_ibkr_gateway": bool(requires_gateway),
                "market": market,
                "portfolio_id": portfolio_id,
                "condition": condition,
                "submit_orders": False,
            }
        )

    if operational_blocks.intersection({"preflight_missing", "preflight_failed", "preflight_stale"}):
        add_step(
            "refresh_supervisor_preflight",
            phase="local_evidence",
            requires_gateway=False,
            condition="before any Gateway-backed refresh",
        )
    if "ibkr_gateway_unavailable" in operational_blocks:
        add_step(
            "restore_ibkr_gateway_paper_api",
            phase="gateway_recovery",
            requires_gateway=False,
            condition="confirm configured paper API port is listening before continuing",
        )
    if "gateway_budget_degraded" in operational_blocks:
        add_step(
            "hold_high_request_scans_until_gateway_budget_recovers",
            phase="gateway_budget",
            requires_gateway=False,
            condition=(
                f"resume after {sorted_recovery_times[-1]}"
                if sorted_recovery_times
                else "resume after the rolling telemetry window returns below budget"
            ),
        )
    if actionable_target:
        recovering_operational_state = bool(
            operational_blocks.intersection(
                {
                    "ibkr_gateway_unavailable",
                    "gateway_budget_degraded",
                    "preflight_missing",
                    "preflight_failed",
                    "preflight_stale",
                }
            )
        )
        add_step(
            (
                "refresh_frontier_report_and_execution_no_submit"
                if recovering_operational_state
                else "refresh_frontier_evidence_no_submit"
            ),
            phase="targeted_gateway_refresh" if recovering_operational_state else "evidence_maintenance",
            requires_gateway=True,
            portfolio_id=target_portfolio_id,
            market=target_market,
            condition=(
                "only after Gateway availability and request budget gates pass"
                if recovering_operational_state
                else "run target-scoped maintenance when execution capacity is available"
            ),
        )
        add_step(
            "rebuild_market_readiness_auto_order_readiness_and_dashboard",
            phase="local_evidence",
            requires_gateway=False,
            portfolio_id=target_portfolio_id,
            market=target_market,
            condition="after the targeted no-submit execution refresh",
        )
    elif stale_target_available:
        add_step(
            "refresh_stale_execution_report",
            phase="targeted_stale_execution_refresh",
            requires_gateway=True,
            portfolio_id=stale_primary_portfolio_id,
            market=stale_primary_market,
            condition="single growth-aware stale artifact target; no submit",
        )
        add_step(
            "refresh_stale_execution_no_submit",
            phase="targeted_stale_execution_refresh",
            requires_gateway=True,
            portfolio_id=stale_primary_portfolio_id,
            market=stale_primary_market,
            condition="dry-run execution only; submit_orders=false",
        )
        add_step(
            "rebuild_market_readiness_auto_order_readiness_and_dashboard",
            phase="local_evidence",
            requires_gateway=False,
            portfolio_id=stale_primary_portfolio_id,
            market=stale_primary_market,
            condition="after stale report and no-submit execution refresh",
        )
    elif not steps:
        add_step(
            "review_submit_frontier_and_candidate_evidence",
            phase="manual_review",
            requires_gateway=False,
            condition="no quality-passing frontier is available",
        )

    if "ibkr_gateway_unavailable" in operational_blocks:
        status = "gateway_restore_required"
        primary_action = "restore_ibkr_gateway_paper_api"
    elif "gateway_budget_degraded" in operational_blocks:
        status = "wait_gateway_budget"
        primary_action = "hold_high_request_scans_until_gateway_budget_recovers"
    elif operational_blocks.intersection({"preflight_missing", "preflight_failed", "preflight_stale"}):
        status = "local_preflight_refresh_required"
        primary_action = "refresh_supervisor_preflight"
    elif actionable_target:
        status = "evidence_maintenance_required"
        primary_action = "refresh_frontier_evidence_no_submit"
    elif stale_target_available:
        status = "stale_execution_refresh_required"
        primary_action = "refresh_stale_execution_target_no_submit"
    else:
        status = "manual_review_required"
        primary_action = "review_submit_frontier_and_candidate_evidence"

    active_target_market = target_market if actionable_target else stale_primary_market if stale_target_available else ""
    active_target_portfolio_id = (
        target_portfolio_id
        if actionable_target
        else stale_primary_portfolio_id if stale_target_available else ""
    )
    active_target_symbols = (
        str(top_frontier.get("planned_order_symbols") or "")
        if actionable_target
        else str(stale_primary.get("planned_order_symbols") or "") if stale_target_available else ""
    )
    active_target_quality = (
        target_quality
        if actionable_target
        else str(stale_primary.get("submit_quality_status") or "").strip().upper()
        if stale_target_available
        else target_quality
    )

    return {
        "status": status,
        "primary_action": primary_action,
        "target_market": active_target_market,
        "target_portfolio_id": active_target_portfolio_id,
        "target_symbols": active_target_symbols,
        "target_submit_quality_status": active_target_quality,
        "target_net_edge_bps": _float(top_frontier.get("submit_quality_min_net_edge_bps"), 0.0),
        "target_edge_margin_bps": _float(top_frontier.get("submit_quality_min_edge_margin_bps"), 0.0),
        "target_ranking_bucket": str(stale_primary.get("ranking_bucket") or "") if stale_target_available else "",
        "target_gateway_budget_blocked": bool(stale_primary.get("gateway_budget_blocked", False)) if stale_target_available else False,
        "gateway_budget_projected_recovery_at": (
            sorted_recovery_times[-1]
            if "gateway_budget_degraded" in operational_blocks and sorted_recovery_times
            else ""
        ),
        "gateway_refresh_portfolio_limit": 1 if actionable_target or stale_target_available else 0,
        "estimated_gateway_refresh_count": 1 if actionable_target or stale_target_available else 0,
        "request_policy": (
            "single_highest_quality_frontier_only"
            if actionable_target
            else str(stale_plan.get("request_policy") or "one_stale_execution_portfolio_after_gateway_budget_ok")
            if stale_target_available
            else "single_highest_quality_frontier_only"
        ),
        "step_count": len(steps),
        "steps": steps,
        "paper_only": True,
        "does_not_submit_orders": True,
        "does_not_relax_submit_gates": True,
    }


def evaluate_auto_order_recovery_eligibility(
    recovery_plan: Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
) -> Dict[str, Any]:
    """Evaluate whether one target-scoped paper recovery refresh may run."""
    plan = dict(recovery_plan or {})
    status = str(plan.get("status") or "").strip().lower()
    target_market = _market(plan.get("target_market"))
    target_portfolio_id = str(plan.get("target_portfolio_id") or "").strip()
    target_quality = str(plan.get("target_submit_quality_status") or "").strip().upper()
    recovery_at_text = str(plan.get("gateway_budget_projected_recovery_at") or "").strip()
    recovery_at = parse_utc_datetime(recovery_at_text)
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    recovery_time_reached = bool(recovery_at is None or now_utc >= recovery_at)
    contract_safe = bool(
        plan.get("paper_only", False)
        and plan.get("does_not_submit_orders", False)
        and plan.get("does_not_relax_submit_gates", False)
        and _int(plan.get("gateway_refresh_portfolio_limit"), 0) <= 1
        and _int(plan.get("estimated_gateway_refresh_count"), 0) <= 1
    )
    active_statuses = {
        "gateway_restore_required",
        "wait_gateway_budget",
        "local_preflight_refresh_required",
        "targeted_frontier_refresh_required",
        "stale_execution_refresh_required",
    }
    runtime_restart_required = status == "runtime_restart_required"
    maintenance_status = status == "evidence_maintenance_required"
    maintenance_eligible = bool(
        maintenance_status
        and target_market
        and target_portfolio_id
        and contract_safe
        and target_quality == "PASS"
    )
    active = bool(runtime_restart_required or (status in active_statuses and target_market and target_portfolio_id))
    eligible = False
    reason = "recovery_plan_not_active"
    if runtime_restart_required:
        reason = str(plan.get("reason") or "supervisor_runtime_restart_required")
        if reason in {"supervisor_code_revision_missing", "supervisor_code_revision_mismatch"}:
            reason = "supervisor_runtime_restart_required"
    elif status == "submit_review_ready":
        reason = "submit_plan_ready_no_refresh"
    elif maintenance_status:
        reason = (
            "evidence_maintenance_scheduled"
            if maintenance_eligible
            else "unsafe_evidence_maintenance_contract"
        )
    elif not active:
        reason = "recovery_target_missing" if status in active_statuses else "recovery_plan_not_active"
    elif not contract_safe:
        reason = "unsafe_recovery_contract"
    elif status != "stale_execution_refresh_required" and target_quality != "PASS":
        reason = "target_quality_not_pass"
    elif status == "gateway_restore_required":
        reason = "ibkr_gateway_unavailable"
    elif status == "local_preflight_refresh_required":
        reason = "local_preflight_refresh_required"
    elif status == "wait_gateway_budget":
        reason = (
            "gateway_budget_evidence_refresh_required"
            if recovery_time_reached
            else "gateway_budget_recovery_not_reached"
        )
    elif status == "targeted_frontier_refresh_required":
        eligible = True
        reason = "eligible_targeted_no_submit_refresh"
    elif status == "stale_execution_refresh_required":
        eligible = True
        reason = "eligible_stale_execution_no_submit_refresh"

    return {
        "active": bool(active),
        "eligible": bool(eligible),
        "maintenance_active": bool(maintenance_eligible),
        "reason": reason,
        "status": status,
        "target_market": target_market,
        "target_portfolio_id": target_portfolio_id,
        "target_symbols": str(plan.get("target_symbols") or ""),
        "target_submit_quality_status": target_quality,
        "gateway_budget_projected_recovery_at": recovery_at_text,
        "gateway_budget_recovery_time_reached": bool(recovery_time_reached),
        "gateway_refresh_portfolio_limit": _int(plan.get("gateway_refresh_portfolio_limit"), 0),
        "request_policy": str(plan.get("request_policy") or ""),
        "allowed_actions": (
            ["generate_investment_report", "run_investment_execution_no_submit"]
            if eligible or maintenance_eligible
            else []
        ),
        "paper_only": True,
        "submit_orders": False,
        "does_not_relax_submit_gates": True,
    }


def build_stale_execution_refresh_plan(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Rank stale execution artifacts for the next no-submit evidence refresh."""
    candidates: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        market = _market(row.get("market"))
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not market or not portfolio_id:
            continue
        status = str(row.get("status") or "").strip().upper()
        readiness_status = str(row.get("market_readiness_status") or "").strip().upper()
        if status == DISABLED_STATUS or readiness_status == "RESEARCH_ONLY":
            continue
        artifact_status = str(row.get("market_readiness_artifact_health_status") or "").strip().upper()
        readiness_reason = str(row.get("market_readiness_reason") or "").strip().upper()
        offline_reasons = {
            str(reason or "").strip().lower()
            for reason in list(row.get("offline_recovery_reasons") or [])
            if str(reason or "").strip()
        }
        stale_execution = bool(
            artifact_status == "STALE"
            or readiness_reason == "STALE_EXECUTION_ARTIFACT"
            or "market_readiness_artifact_stale" in offline_reasons
        )
        if not stale_execution:
            continue
        age_hours = _float(row.get("market_readiness_artifact_age_hours"), 0.0)
        max_age_hours = _float(row.get("offline_recovery_max_gap_hours"), 0.0) or 24.0
        stale_gap_hours = max(0.0, age_hours - max_age_hours)
        post_cost_positive = _int(row.get("post_cost_positive_edge_count"), 0)
        high_cost_positive = _int(row.get("post_cost_high_cost_positive_edge_count"), 0)
        close_wait_pullback = _int(row.get("wait_pullback_close_count"), 0)
        order_count = _int(row.get("market_readiness_order_count"), 0)
        buy_value = _float(row.get("market_readiness_planned_buy_order_value"), 0.0)
        sell_value = _float(row.get("market_readiness_planned_sell_order_value"), 0.0)
        has_current_buy_plan = buy_value > 0.0
        sell_only_current_plan = sell_value > 0.0 and not has_current_buy_plan
        growth_candidate_supply = (post_cost_positive + high_cost_positive + close_wait_pullback) > 0
        hard_blocks = {
            str(reason or "").strip()
            for reason in list(row.get("hard_blocks") or [])
            if str(reason or "").strip()
        }
        gateway_budget_blocked = "gateway_budget_degraded" in hard_blocks
        ranking_bucket = (
            "growth_refresh_candidate"
            if growth_candidate_supply or has_current_buy_plan
            else "stale_artifact_only"
        )
        evidence_score = (
            post_cost_positive * 10.0
            + high_cost_positive * 4.0
            + close_wait_pullback * 3.0
            + min(stale_gap_hours, 72.0) * 0.5
            + order_count * 2.0
            + (20.0 if has_current_buy_plan else 0.0)
            - (6.0 if sell_only_current_plan else 0.0)
        )
        candidates.append(
            {
                "market": market,
                "portfolio_id": portfolio_id,
                "ranking_bucket": ranking_bucket,
                "refresh_rank_score": round(evidence_score, 4),
                "artifact_health_status": artifact_status,
                "artifact_age_hours": round(age_hours, 2),
                "stale_gap_hours": round(stale_gap_hours, 2),
                "primary_reason": readiness_reason,
                "post_cost_positive_edge_count": post_cost_positive,
                "post_cost_high_cost_positive_edge_count": high_cost_positive,
                "wait_pullback_close_count": close_wait_pullback,
                "order_count": order_count,
                "planned_buy_order_value": round(buy_value, 2),
                "planned_sell_order_value": round(sell_value, 2),
                "has_current_buy_plan": bool(has_current_buy_plan),
                "sell_only_current_plan": bool(sell_only_current_plan),
                "growth_candidate_supply": bool(growth_candidate_supply),
                "planned_order_symbols": str(row.get("market_readiness_planned_order_symbols") or ""),
                "submit_quality_status": str(row.get("submit_quality_status") or ""),
                "submit_quality_reason": str(row.get("submit_quality_reason") or ""),
                "gateway_budget_blocked": bool(gateway_budget_blocked),
                "action": "refresh_report_and_execution_no_submit",
                "submit_orders": False,
                "does_not_relax_submit_gates": True,
            }
        )
    candidates.sort(
        key=lambda item: (
            1 if bool(item.get("gateway_budget_blocked", False)) else 0,
            0 if str(item.get("ranking_bucket") or "") == "growth_refresh_candidate" else 1,
            -float(item.get("refresh_rank_score", 0.0) or 0.0),
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
        )
    )
    primary = dict(candidates[0]) if candidates else {}
    primary_gateway_blocked = bool(primary.get("gateway_budget_blocked", False))
    if not candidates:
        status = "NO_STALE_EXECUTION_TARGETS"
        primary_action = "none"
        reason = "no_stale_execution_artifacts"
    elif primary_gateway_blocked:
        status = "WAIT_GATEWAY_BUDGET"
        primary_action = "wait_gateway_budget_then_refresh_stale_execution"
        reason = "gateway_budget_degraded_before_no_submit_refresh"
    else:
        status = "READY_FOR_TARGETED_NO_SUBMIT_REFRESH"
        primary_action = "refresh_primary_stale_execution_no_submit"
        reason = "stale_execution_artifact_refresh_target_ranked"
    return {
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "target_count": int(len(candidates)),
        "primary_market": str(primary.get("market") or ""),
        "primary_portfolio_id": str(primary.get("portfolio_id") or ""),
        "primary_score": float(primary.get("refresh_rank_score", 0.0) or 0.0),
        "request_policy": "one_stale_execution_portfolio_after_gateway_budget_ok",
        "paper_only": True,
        "submit_orders": False,
        "does_not_relax_submit_gates": True,
        "rows": candidates[:20],
    }


def build_auto_order_readiness_summary(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any] | None = None,
    watchlist_expansion_summary: Mapping[str, Any] | None = None,
    weekly_summary: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    ready_rows = [row for row in clean_rows if bool(row.get("ready", False))]
    blocked_rows = [row for row in clean_rows if str(row.get("status") or "") == BLOCKED_STATUS]
    warning_rows = [row for row in clean_rows if str(row.get("status") or "") == WARNING_STATUS]
    disabled_rows = [row for row in clean_rows if str(row.get("status") or "") == DISABLED_STATUS]
    offline_recovery_rows = [row for row in clean_rows if bool(row.get("offline_recovery_required", False))]
    status = "blocked" if blocked_rows else "warning" if warning_rows else "ready"
    hard_block_counts: Dict[str, int] = {}
    warning_counts: Dict[str, int] = {}
    remediation_by_reason: Dict[str, str] = {}
    detail_by_reason: Dict[str, str] = {}
    markets_by_reason: Dict[str, set[str]] = {}
    portfolios_by_reason: Dict[str, set[str]] = {}
    for row in clean_rows:
        market = str(row.get("market") or "").strip().upper()
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        for reason in list(row.get("hard_blocks") or []):
            reason_text = str(reason or "").strip()
            if not reason_text:
                continue
            hard_block_counts[reason_text] = hard_block_counts.get(reason_text, 0) + 1
            markets_by_reason.setdefault(reason_text, set()).add(market)
            portfolios_by_reason.setdefault(reason_text, set()).add(portfolio_id)
        for reason in list(row.get("warnings") or []):
            reason_text = str(reason or "").strip()
            if not reason_text:
                continue
            warning_counts[reason_text] = warning_counts.get(reason_text, 0) + 1
            markets_by_reason.setdefault(reason_text, set()).add(market)
            portfolios_by_reason.setdefault(reason_text, set()).add(portfolio_id)
        for detail in list(row.get("hard_block_details") or []) + list(row.get("warning_details") or []):
            if not isinstance(detail, Mapping):
                continue
            reason_text = str(detail.get("reason") or "").strip()
            if not reason_text:
                continue
            remediation = str(detail.get("remediation") or "").strip()
            detail_text = str(detail.get("detail") or "").strip()
            remediation_by_reason.setdefault(reason_text, remediation)
            detail_by_reason.setdefault(reason_text, detail_text)

    remediation_plan: List[Dict[str, Any]] = []
    for reason, count in hard_block_counts.items():
        remediation_plan.append(
            {
                "reason": reason,
                "severity": "block",
                "priority": _reason_priority(reason),
                "affected_portfolio_count": int(count),
                "affected_markets": sorted(value for value in markets_by_reason.get(reason, set()) if value),
                "affected_portfolios": sorted(value for value in portfolios_by_reason.get(reason, set()) if value),
                "detail": detail_by_reason.get(reason, ""),
                "remediation": remediation_by_reason.get(reason, ""),
            }
        )
    for reason, count in warning_counts.items():
        remediation_plan.append(
            {
                "reason": reason,
                "severity": "warning",
                "priority": _reason_priority(reason, warning=True),
                "affected_portfolio_count": int(count),
                "affected_markets": sorted(value for value in markets_by_reason.get(reason, set()) if value),
                "affected_portfolios": sorted(value for value in portfolios_by_reason.get(reason, set()) if value),
                "detail": detail_by_reason.get(reason, ""),
                "remediation": remediation_by_reason.get(reason, ""),
            }
        )
    remediation_plan.sort(
        key=lambda item: (
            int(item.get("priority", 999)),
            -int(item.get("affected_portfolio_count", 0)),
            str(item.get("reason") or ""),
        )
    )
    primary_block_reason = (
        str(remediation_plan[0].get("reason") or "")
        if remediation_plan and str(remediation_plan[0].get("severity") or "") == "block"
        else str(blocked_rows[0].get("primary_reason") or "") if blocked_rows else ""
    )
    offline_markets = sorted(
        {
            str(row.get("market") or "").strip().upper()
            for row in offline_recovery_rows
            if str(row.get("market") or "").strip()
        }
    )
    offline_portfolios = sorted(
        {
            str(row.get("portfolio_id") or "").strip()
            for row in offline_recovery_rows
            if str(row.get("portfolio_id") or "").strip()
        }
    )
    offline_reasons: Dict[str, int] = {}
    for row in offline_recovery_rows:
        for reason in list(row.get("offline_recovery_reasons") or []):
            reason_text = str(reason or "").strip()
            if reason_text:
                offline_reasons[reason_text] = int(offline_reasons.get(reason_text, 0)) + 1
    offline_top_reason = (
        sorted(offline_reasons.items(), key=lambda item: (-item[1], item[0]))[0][0]
        if offline_reasons
        else "-"
    )
    sorted_hard_block_counts = dict(
        sorted(hard_block_counts.items(), key=lambda item: (_reason_priority(item[0]), item[0]))
    )
    sorted_warning_counts = dict(
        sorted(warning_counts.items(), key=lambda item: (_reason_priority(item[0], warning=True), item[0]))
    )
    submit_plan = build_auto_order_submit_plan(
        clean_rows,
        policy=policy,
        weekly_summary=weekly_summary,
        account_growth_tier_plan=dict(watchlist_expansion_summary or {}).get("account_growth_tier_plan"),
    )
    frequency_plan = build_auto_order_frequency_plan(
        clean_rows,
        submit_plan=submit_plan,
        watchlist_expansion_summary=watchlist_expansion_summary,
    )
    stale_execution_refresh_plan = build_stale_execution_refresh_plan(clean_rows)
    recovery_plan = build_auto_order_recovery_plan(
        clean_rows,
        submit_plan=submit_plan,
        stale_execution_refresh_plan=stale_execution_refresh_plan,
        global_hard_blocks=sorted_hard_block_counts,
    )
    return {
        "status": status,
        "summary_text": (
            f"auto_order_readiness portfolios={len(clean_rows)} ready={len(ready_rows)} "
            f"warning={len(warning_rows)} blocked={len(blocked_rows)} disabled={len(disabled_rows)}"
        ),
        "portfolio_count": len(clean_rows),
        "ready_count": len(ready_rows),
        "warning_count": len(warning_rows),
        "blocked_count": len(blocked_rows),
        "disabled_count": len(disabled_rows),
        "primary_block_reason": primary_block_reason,
        "offline_recovery_required_count": int(len(offline_recovery_rows)),
        "offline_recovery_markets": offline_markets,
        "offline_recovery_portfolios": offline_portfolios,
        "offline_recovery_reason_counts": dict(sorted(offline_reasons.items())),
        "offline_recovery_summary_text": (
            f"offline_recovery_required={len(offline_recovery_rows)} "
            f"markets={','.join(offline_markets) or '-'} "
            f"top_reason={offline_top_reason}"
        ),
        "hard_block_counts": sorted_hard_block_counts,
        "warning_counts": sorted_warning_counts,
        "remediation_plan": remediation_plan,
        "submit_plan": submit_plan,
        "frequency_plan": frequency_plan,
        "recovery_plan": recovery_plan,
        "stale_execution_refresh_plan": stale_execution_refresh_plan,
        "candidate_supply_status": str(frequency_plan.get("status") or ""),
        "candidate_supply_reason": str(frequency_plan.get("reason") or ""),
        "candidate_supply_primary_action": str(frequency_plan.get("primary_action") or ""),
    }
