from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Set
from zoneinfo import ZoneInfo

from ..common.markets import resolve_market_code
from ..common.strategy_parameter_registry import (
    StrategyParameterRegistry,
    load_strategy_parameter_registry,
    strategy_parameter_priority,
    strategy_parameter_proposed_value,
)
from .supervisor_support import (
    clamp_float,
    feedback_confidence_value,
    merge_execution_feedback_penalties,
    parse_feedback_penalty_rows,
    scale_feedback_delta,
    scale_feedback_penalty_rows,
)


BASE_DIR = Path(__file__).resolve().parents[2]
_STRATEGY_PARAMETER_REGISTRY: StrategyParameterRegistry | None = None


def _strategy_parameter_registry() -> StrategyParameterRegistry:
    global _STRATEGY_PARAMETER_REGISTRY
    if _STRATEGY_PARAMETER_REGISTRY is None:
        _STRATEGY_PARAMETER_REGISTRY = load_strategy_parameter_registry(BASE_DIR)
    return _STRATEGY_PARAMETER_REGISTRY


def market_profile_review_draft(row: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(row or {})
    if not payload:
        return {}
    market = resolve_market_code(
        str(payload.get("market") or payload.get("adaptive_strategy_active_market_profile") or "")
    )
    profile = str(payload.get("adaptive_strategy_active_market_profile") or market or "").strip().upper()
    tuning_target = str(payload.get("market_profile_tuning_target") or "").strip().upper()
    tuning_action = str(payload.get("market_profile_tuning_action") or "").strip().upper()
    tuning_note = str(payload.get("market_profile_tuning_note") or "").strip()
    plan_summary = str(payload.get("adaptive_strategy_active_market_plan_summary") or "").strip()
    regime_summary = str(payload.get("adaptive_strategy_active_market_regime_summary") or "").strip()
    execution_summary = str(payload.get("adaptive_strategy_active_market_execution_summary") or "").strip()

    draft: Dict[str, Any] = {
        "market": market,
        "profile": profile,
        "tuning_target": tuning_target,
        "tuning_action": tuning_action,
        "review_required": False,
        "scope": "WATCH",
        "scope_label": "继续观察",
        "summary": tuning_note or "当前还没有足够强的信号去调整 market profile。",
        "ready_for_manual_apply": bool(int(payload.get("market_profile_ready_for_manual_apply", 0) or 0)),
        "readiness_label": str(payload.get("market_profile_readiness_label") or ""),
        "readiness_summary": str(payload.get("market_profile_readiness_summary") or ""),
        "items": [],
    }

    if tuning_action == "REVIEW_EXECUTION_GATE" or tuning_target == "EXECUTION_GATE":
        draft.update(
            {
                "review_required": True,
                "scope": "EXECUTION",
                "scope_label": "执行门槛",
                "items": [
                    {
                        "config_path": f"market_profiles.{profile}.min_expected_edge_bps",
                        "field": "min_expected_edge_bps",
                        "field_label": "min expected edge",
                        "change_hint": "RELAX_LOWER",
                        "change_hint_label": "按放松方向温和下调",
                        "current_summary": execution_summary,
                    },
                    {
                        "config_path": f"market_profiles.{profile}.edge_cost_buffer_bps",
                        "field": "edge_cost_buffer_bps",
                        "field_label": "edge cost buffer",
                        "change_hint": "RELAX_LOWER",
                        "change_hint_label": "按放松方向温和下调",
                        "current_summary": execution_summary,
                    },
                ],
            }
        )
        draft["summary"] = (
            f"建议复核 {market}/{profile} 执行门槛："
            "min_expected_edge_bps, edge_cost_buffer_bps。"
            f"当前执行档案={execution_summary or '-'}。"
            f"{tuning_note}"
        )
    elif tuning_action == "REVIEW_REGIME_PLAN" or tuning_target == "REGIME_PLAN":
        draft.update(
            {
                "review_required": True,
                "scope": "REGIME_PLAN",
                "scope_label": "Regime / 计划参数",
                "items": [
                    {
                        "config_path": f"market_profiles.{profile}.regime_risk_on_threshold",
                        "field": "regime_risk_on_threshold",
                        "field_label": "risk_on threshold",
                        "change_hint": "RECALIBRATE_RELAX",
                        "change_hint_label": "按放松方向复核",
                        "current_summary": regime_summary,
                    },
                    {
                        "config_path": f"market_profiles.{profile}.regime_hard_risk_off_threshold",
                        "field": "regime_hard_risk_off_threshold",
                        "field_label": "hard risk-off threshold",
                        "change_hint": "RECALIBRATE_RELAX",
                        "change_hint_label": "按放松方向复核",
                        "current_summary": regime_summary,
                    },
                    {
                        "config_path": f"market_profiles.{profile}.no_trade_band_pct",
                        "field": "no_trade_band_pct",
                        "field_label": "no-trade band",
                        "change_hint": "REDUCE",
                        "change_hint_label": "优先缩小",
                        "current_summary": plan_summary,
                    },
                    {
                        "config_path": f"market_profiles.{profile}.turnover_penalty_scale",
                        "field": "turnover_penalty_scale",
                        "field_label": "turnover penalty",
                        "change_hint": "REDUCE",
                        "change_hint_label": "优先降低",
                        "current_summary": plan_summary,
                    },
                ],
            }
        )
        draft["summary"] = (
            f"建议复核 {market}/{profile} regime/计划参数："
            "regime_risk_on_threshold, regime_hard_risk_off_threshold, no_trade_band_pct, turnover_penalty_scale。"
            f"当前计划档案={plan_summary or '-'}；当前 regime 档案={regime_summary or '-'}。"
            f"{tuning_note}"
        )
    elif tuning_action == "KEEP_RISK_OVERLAY":
        draft.update(
            {
                "scope": "RISK_OVERLAY",
                "scope_label": "风险 Overlay",
                "summary": f"当前更像是风险 overlay 主导，暂不建议改 {market}/{profile} 的 market profile。{tuning_note}",
            }
        )
    elif tuning_action in {"KEEP_EXECUTION_RELAX", "KEEP_RISK_RELAX"}:
        draft.update(
            {
                "scope": tuning_target or "WATCH",
                "scope_label": "继续试运行",
                "summary": f"当前建议继续保留这轮放宽，不急着重写 {market}/{profile} 的 market profile。{tuning_note}",
            }
        )
    if draft.get("review_required", False) and not str(draft.get("readiness_summary") or "").strip():
        draft["readiness_label"] = str(draft.get("readiness_label") or "OBSERVE_COHORT")
        draft["readiness_summary"] = "当前仅连续 1 周维持同方向，先继续观察到至少 2 周再决定是否人工应用。"
    return draft


def market_profile_patch_value(field: str, current_value: Any, change_hint: str) -> Any:
    return strategy_parameter_proposed_value(
        field,
        current_value,
        change_hint,
        registry=_strategy_parameter_registry(),
    )


def market_profile_patch_priority(scope: str, field: str) -> tuple[int, str, str, str]:
    scope_code = str(scope or "").strip().upper()
    field_name = str(field or "").strip()
    rank, label = strategy_parameter_priority(scope_code, field_name, registry=_strategy_parameter_registry())
    if scope_code == "EXECUTION":
        risk = {
            "edge_cost_buffer_bps": ("LOW", "低风险"),
            "min_expected_edge_bps": ("MEDIUM", "中风险"),
        }.get(field_name, ("MEDIUM", "中风险"))
        return int(rank), str(label), risk[0], risk[1]
    if scope_code == "REGIME_PLAN":
        risk = {
            "no_trade_band_pct": ("LOW", "低风险"),
            "turnover_penalty_scale": ("LOW", "低风险"),
            "regime_risk_on_threshold": ("MEDIUM", "中风险"),
            "regime_hard_risk_off_threshold": ("HIGH", "高风险"),
        }.get(field_name, ("MEDIUM", "中风险"))
        return int(rank), str(label), risk[0], risk[1]
    return int(rank), str(label), "MEDIUM", "中风险"


def market_profile_manual_apply_patch(patch: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(patch or {})
    if not payload:
        return {}
    primary_item = dict(payload.get("primary_item") or {})
    items = [dict(row) for row in list(payload.get("items") or []) if isinstance(row, dict)]
    if not primary_item and items:
        primary_item = dict(items[0])
    if not primary_item:
        return {}
    ready = bool(payload.get("ready_for_manual_apply", False))
    deferred_items = items[1:] if len(items) > 1 else []
    candidate_text = (
        f"{str(primary_item.get('field') or '')}: "
        f"{primary_item.get('current_value')} -> {primary_item.get('suggested_value')}"
    )
    if ready:
        summary = (
            f"建议先人工应用 1 项：{candidate_text}；"
            f"其余 {len(deferred_items)} 项继续观察 {int(payload.get('observe_window_weeks', 2) or 2)} 周。"
        )
    else:
        summary = f"当前未到人工应用阶段；若 cohort 持续一致，优先先人工应用 {candidate_text}。"
    return {
        "mode": "PRIMARY_ONLY" if ready else "OBSERVE_ONLY",
        "ready": ready,
        "market": str(payload.get("market") or ""),
        "profile": str(payload.get("profile") or ""),
        "config_file": str(payload.get("config_file") or ""),
        "observe_window_weeks": int(payload.get("observe_window_weeks", 2) or 2),
        "apply_item_count": 1 if ready else 0,
        "deferred_item_count": int(len(deferred_items)),
        "candidate_item": primary_item,
        "apply_items": [primary_item] if ready else [],
        "deferred_items": deferred_items,
        "summary": summary,
    }


def file_sha1(path: Path) -> str:
    try:
        return hashlib.sha1(path.read_bytes()).hexdigest()
    except Exception:
        return ""


def iso_week_identity(ts_text: str, tz: ZoneInfo) -> tuple[str, str]:
    try:
        dt = datetime.fromisoformat(str(ts_text or "").strip())
    except Exception:
        dt = datetime.now(tz)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    iso_year, iso_week, iso_weekday = dt.isocalendar()
    week_start = (dt - timedelta(days=int(iso_weekday) - 1)).date().isoformat()
    return f"{iso_year}-W{iso_week:02d}", week_start


def patch_review_history(
    raw_events: List[Dict[str, Any]] | None,
    *,
    current_signature: str = "",
    valid_values: Set[str],
    labels: Mapping[str, str],
    history_limit: int,
) -> List[Dict[str, Any]]:
    history: List[Dict[str, Any]] = []
    for raw in list(raw_events or []):
        row = dict(raw or {})
        status = str(row.get("status") or "").strip().upper()
        if status not in valid_values:
            continue
        event_signature = str(row.get("feedback_signature") or "").strip()
        if current_signature and event_signature and event_signature != current_signature:
            continue
        ts = str(row.get("ts") or "").strip()
        status_label = str(row.get("status_label") or labels.get(status, status or "-"))
        evidence_summary = str(row.get("evidence_summary") or "").strip()
        summary = str(row.get("summary") or "").strip()
        if not summary:
            summary = status_label
            if ts:
                summary = f"{summary} @ {ts[:19]}"
            if evidence_summary:
                summary = f"{summary} | {evidence_summary}"
        history.append(
            {
                "ts": ts,
                "status": status,
                "status_label": status_label,
                "feedback_signature": event_signature,
                "evidence_summary": evidence_summary,
                "config_commit_sha": str(row.get("config_commit_sha") or "").strip(),
                "config_diff_note": str(row.get("config_diff_note") or "").strip(),
                "operator_note": str(row.get("operator_note") or "").strip(),
                "approval_status": str(row.get("approval_status") or "").strip(),
                "rollback_plan": str(row.get("rollback_plan") or "").strip(),
                "effect_tracking_window": str(row.get("effect_tracking_window") or "").strip(),
                "effect_tracking_metrics": list(row.get("effect_tracking_metrics") or []),
                "summary": summary,
            }
        )
    return history[-max(int(history_limit or 0), 1) :]


def patch_review_history_summary(
    history: List[Dict[str, Any]] | None,
    *,
    limit: int = 3,
) -> str:
    rows = list(history or [])
    if not rows:
        return ""
    recent_rows = rows[-max(int(limit or 0), 1) :]
    return " -> ".join(
        str(dict(row).get("summary") or "").strip()
        for row in recent_rows
        if str(dict(row).get("summary") or "").strip()
    )


def append_patch_review_history(
    existing_history: List[Dict[str, Any]] | None,
    *,
    feedback_signature: str,
    decision: str,
    reviewed_ts: str,
    evidence: Dict[str, Any] | None,
    valid_values: Set[str],
    labels: Mapping[str, str],
    history_limit: int,
) -> List[Dict[str, Any]]:
    status = str(decision or "").strip().upper()
    history = list(existing_history or [])
    if status not in valid_values:
        return history
    evidence_payload = dict(evidence or {})
    status_label = str(labels.get(status, status or "-"))
    evidence_summary = str(evidence_payload.get("summary") or "").strip()
    summary = status_label
    if reviewed_ts:
        summary = f"{summary} @ {reviewed_ts[:19]}"
    if evidence_summary:
        summary = f"{summary} | {evidence_summary}"
    history.append(
        {
            "ts": str(reviewed_ts or ""),
            "status": status,
            "status_label": status_label,
            "feedback_signature": str(feedback_signature or ""),
            "evidence_summary": evidence_summary,
            "config_commit_sha": str(evidence_payload.get("config_commit_sha") or "").strip(),
            "config_diff_note": str(evidence_payload.get("config_diff_note") or "").strip(),
            "operator_note": str(evidence_payload.get("operator_note") or "").strip(),
            "approval_status": str(evidence_payload.get("approval_status") or "").strip(),
            "rollback_plan": str(evidence_payload.get("rollback_plan") or "").strip(),
            "effect_tracking_window": str(evidence_payload.get("effect_tracking_window") or "").strip(),
            "effect_tracking_metrics": list(evidence_payload.get("effect_tracking_metrics") or []),
            "summary": summary,
        }
    )
    return history[-max(int(history_limit or 0), 1) :]


def live_change_governance_evidence_fields(
    *,
    reviewed_ts: str,
    config_file: str,
    config_commit_sha: str,
    operator_note: str = "",
) -> Dict[str, Any]:
    config_label = str(config_file or "config").strip() or "config"
    rollback_ref = str(config_commit_sha or "").strip()
    rollback_target = f"previous reviewed git/config state before {rollback_ref[:10]}" if rollback_ref else "previous reviewed config state"
    return {
        "approval_status": "APPLIED",
        "approved_ts": str(reviewed_ts or ""),
        "approved_by": str(operator_note or "dashboard_control").strip() or "dashboard_control",
        "rollback_plan": f"Restore {config_label} to {rollback_target}, then rerun weekly review and dashboard refresh.",
        "effect_tracking_window": "next 3 weekly reviews",
        "effect_tracking_metrics": [
            "post_cost_edge_bps",
            "realized_slippage_bps",
            "turnover",
            "drawdown",
            "blocked_edge_order_count",
        ],
    }


def patch_review_state(
    *,
    review_required: bool,
    current_signature: str,
    stored_signature: str,
    stored_status: str,
    stored_ts: str,
    stored_evidence: Dict[str, Any] | None,
    active_values: Set[str],
    labels: Mapping[str, str],
) -> Dict[str, Any]:
    if not bool(review_required):
        return {
            "status": "",
            "status_label": "-",
            "status_summary": "-",
            "reviewed_ts": "",
            "applied_ts": "",
            "pending": False,
            "signature": "",
            "evidence": {},
            "evidence_summary": "",
        }
    evidence_payload = dict(stored_evidence or {})
    if (
        current_signature
        and stored_signature == current_signature
        and stored_status in active_values
    ):
        label = str(labels.get(stored_status, stored_status or "待审批"))
        summary = f"{label} @ {stored_ts[:19]}" if stored_ts else label
        evidence_summary = str(evidence_payload.get("summary") or "")
        if stored_status == "APPLIED" and evidence_summary:
            summary = f"{summary} | {evidence_summary}"
        return {
            "status": stored_status,
            "status_label": label,
            "status_summary": summary,
            "reviewed_ts": stored_ts,
            "applied_ts": stored_ts if stored_status == "APPLIED" else "",
            "pending": False,
            "signature": current_signature,
            "evidence": evidence_payload,
            "evidence_summary": evidence_summary,
        }
    return {
        "status": "PENDING",
        "status_label": str(labels.get("PENDING", "待审批")),
        "status_summary": str(labels.get("PENDING", "待审批")),
        "reviewed_ts": "",
        "applied_ts": "",
        "pending": True,
        "signature": current_signature,
        "evidence": {},
        "evidence_summary": "",
    }


def dashboard_control_service_payload(
    *,
    enabled: bool,
    service_status: str,
    host: str,
    port: int,
    url: str,
) -> Dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "status": str(service_status or ""),
        "host": str(host or ""),
        "port": int(port or 0),
        "url": str(url or ""),
    }


def dashboard_control_actions_payload(
    *,
    run_once_in_progress: bool,
    preflight_in_progress: bool,
    weekly_review_in_progress: bool,
    last_action: str,
    last_action_ts: str,
    last_error: str,
    preflight_summary_path: str,
    action_history: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return {
        "run_once_in_progress": bool(run_once_in_progress),
        "preflight_in_progress": bool(preflight_in_progress),
        "weekly_review_in_progress": bool(weekly_review_in_progress),
        "last_action": str(last_action or ""),
        "last_action_ts": str(last_action_ts or ""),
        "last_error": str(last_error or ""),
        "preflight_summary_path": str(preflight_summary_path or ""),
        "action_history": [dict(row) for row in list(action_history or [])],
    }


def dashboard_control_artifacts_payload(
    *,
    dashboard_control_state_path: str,
    market_profile_manual_patch_json_path: str,
    market_profile_manual_patch_yaml_path: str,
    calibration_patch_json_path: str,
    calibration_patch_yaml_path: str,
    dashboard_control_action_audit_path: str = "",
) -> Dict[str, Any]:
    return {
        "dashboard_control_state_path": str(dashboard_control_state_path or ""),
        "dashboard_control_action_audit_path": str(dashboard_control_action_audit_path or ""),
        "market_profile_manual_patch_json_path": str(market_profile_manual_patch_json_path or ""),
        "market_profile_manual_patch_yaml_path": str(market_profile_manual_patch_yaml_path or ""),
        "calibration_patch_json_path": str(calibration_patch_json_path or ""),
        "calibration_patch_yaml_path": str(calibration_patch_yaml_path or ""),
    }


def dashboard_control_state_payload(
    *,
    ts: str,
    service: Dict[str, Any],
    actions: Dict[str, Any],
    artifacts: Dict[str, Any],
    portfolios: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "ts": str(ts or ""),
        "service": dict(service or {}),
        "actions": dict(actions or {}),
        "artifacts": dict(artifacts or {}),
        "portfolios": dict(portfolios or {}),
    }


def dashboard_control_artifact_payload(
    *,
    ts: str,
    service_status: str,
    dashboard_control_state_path: str,
    json_path: str,
    yaml_path: str,
    candidates: List[Dict[str, Any]] | None,
) -> Dict[str, Any]:
    rows = [dict(row) for row in list(candidates or [])]
    return {
        "ts": str(ts or ""),
        "service_status": str(service_status or ""),
        "dashboard_control_state_path": str(dashboard_control_state_path or ""),
        "artifact_paths": {
            "json": str(json_path or ""),
            "yaml": str(yaml_path or ""),
        },
        "candidate_count": int(len(rows)),
        "ready_for_manual_apply_count": int(
            sum(1 for row in rows if bool(row.get("ready_for_manual_apply", False)))
        ),
        "patch_candidates": rows,
    }


def dashboard_control_patch_governance_fields(
    patch_governance_action: Dict[str, Any] | None,
) -> Dict[str, Any]:
    action = dict(patch_governance_action or {})
    try:
        raw_priority = action.get("priority", 99)
        priority = int(float(99 if raw_priority is None else raw_priority))
    except Exception:
        priority = 99
    return {
        "weekly_feedback_patch_governance_present": bool(action),
        "weekly_feedback_patch_governance_action": str(action.get("action") or ""),
        "weekly_feedback_patch_governance_action_label": str(action.get("action_label") or ""),
        "weekly_feedback_patch_governance_priority": priority,
        "weekly_feedback_patch_governance_summary": str(action.get("summary") or ""),
        "weekly_feedback_patch_governance_note": str(action.get("note") or ""),
        "weekly_feedback_patch_governance_row": dict(action),
    }


def dashboard_control_market_profile_fields(
    market_profile_bundle: Dict[str, Any] | None,
) -> Dict[str, Any]:
    bundle = dict(market_profile_bundle or {})
    tuning_row = dict(bundle.get("tuning_row") or {})
    review_draft = dict(bundle.get("review_draft") or {})
    suggested_patch = dict(bundle.get("suggested_patch") or {})
    review_state = dict(bundle.get("review_state") or {})
    review_history = list(bundle.get("review_history") or [])
    return {
        "weekly_feedback_market_profile_tuning_action": str(
            tuning_row.get("market_profile_tuning_action") or ""
        ),
        "weekly_feedback_market_profile_tuning_target": str(
            tuning_row.get("market_profile_tuning_target") or ""
        ),
        "weekly_feedback_market_profile_tuning_note": str(
            tuning_row.get("market_profile_tuning_note") or ""
        ),
        "weekly_feedback_market_profile_review_required": bool(
            bundle.get("review_required", False)
        ),
        "weekly_feedback_market_profile_review_summary": str(
            review_draft.get("summary") or ""
        ),
        "weekly_feedback_market_profile_review_draft": dict(review_draft),
        "weekly_feedback_market_profile_suggested_patch_summary": str(
            suggested_patch.get("summary") or ""
        ),
        "weekly_feedback_market_profile_primary_summary": str(
            suggested_patch.get("primary_summary") or ""
        ),
        "weekly_feedback_market_profile_primary_item": dict(
            suggested_patch.get("primary_item") or {}
        ),
        "weekly_feedback_market_profile_manual_apply_summary": str(
            suggested_patch.get("manual_apply_summary") or ""
        ),
        "weekly_feedback_market_profile_manual_apply_patch": dict(
            suggested_patch.get("manual_apply_patch") or {}
        ),
        "weekly_feedback_market_profile_review_status": str(
            review_state.get("status") or ""
        ),
        "weekly_feedback_market_profile_review_status_label": str(
            review_state.get("status_label") or "-"
        ),
        "weekly_feedback_market_profile_review_status_summary": str(
            review_state.get("status_summary") or "-"
        ),
        "weekly_feedback_market_profile_reviewed_ts": str(
            review_state.get("reviewed_ts") or ""
        ),
        "weekly_feedback_market_profile_applied_ts": str(
            review_state.get("applied_ts") or ""
        ),
        "weekly_feedback_market_profile_review_evidence_summary": str(
            review_state.get("evidence_summary") or ""
        ),
        "weekly_feedback_market_profile_review_evidence": dict(
            review_state.get("evidence") or {}
        ),
        "weekly_feedback_market_profile_review_history_summary": str(
            bundle.get("review_history_summary") or ""
        ),
        "weekly_feedback_market_profile_review_history": list(review_history),
        "weekly_feedback_market_profile_review_pending": bool(
            review_state.get("pending", False)
        ),
        "weekly_feedback_market_profile_review_signature": str(
            review_state.get("signature") or ""
        ),
        "weekly_feedback_market_profile_suggested_patch": dict(suggested_patch),
        "weekly_feedback_market_profile_ready_for_manual_apply": bool(
            suggested_patch.get("ready_for_manual_apply", False)
        ),
        "weekly_feedback_market_profile_readiness_summary": str(
            suggested_patch.get("readiness_summary")
            or review_draft.get("readiness_summary")
            or ""
        ),
    }


def dashboard_control_calibration_patch_fields(
    calibration_bundle: Dict[str, Any] | None,
) -> Dict[str, Any]:
    bundle = dict(calibration_bundle or {})
    suggested_patch = dict(bundle.get("suggested_patch") or {})
    review_state = dict(bundle.get("review_state") or {})
    review_history = list(bundle.get("review_history") or [])
    return {
        "weekly_feedback_calibration_patch_present": bool(suggested_patch),
        "weekly_feedback_calibration_patch_review_required": bool(
            bundle.get("review_required", False)
        ),
        "weekly_feedback_calibration_patch_item_count": int(
            suggested_patch.get("item_count", 0) or 0
        ),
        "weekly_feedback_calibration_patch_summary": str(
            suggested_patch.get("summary") or ""
        ),
        "weekly_feedback_calibration_patch_primary_summary": str(
            suggested_patch.get("primary_summary") or ""
        ),
        "weekly_feedback_calibration_patch_primary_item": dict(
            suggested_patch.get("primary_item") or {}
        ),
        "weekly_feedback_calibration_patch_manual_apply_summary": str(
            suggested_patch.get("manual_apply_summary") or ""
        ),
        "weekly_feedback_calibration_patch_manual_apply_patch": dict(
            suggested_patch.get("manual_apply_patch") or {}
        ),
        "weekly_feedback_calibration_patch_review_status": str(
            review_state.get("status") or ""
        ),
        "weekly_feedback_calibration_patch_review_status_label": str(
            review_state.get("status_label") or "-"
        ),
        "weekly_feedback_calibration_patch_review_status_summary": str(
            review_state.get("status_summary") or "-"
        ),
        "weekly_feedback_calibration_patch_reviewed_ts": str(
            review_state.get("reviewed_ts") or ""
        ),
        "weekly_feedback_calibration_patch_applied_ts": str(
            review_state.get("applied_ts") or ""
        ),
        "weekly_feedback_calibration_patch_review_evidence_summary": str(
            review_state.get("evidence_summary") or ""
        ),
        "weekly_feedback_calibration_patch_review_evidence": dict(
            review_state.get("evidence") or {}
        ),
        "weekly_feedback_calibration_patch_review_history_summary": str(
            bundle.get("review_history_summary") or ""
        ),
        "weekly_feedback_calibration_patch_review_history": list(review_history),
        "weekly_feedback_calibration_patch_review_pending": bool(
            review_state.get("pending", False)
        ),
        "weekly_feedback_calibration_patch_review_signature": str(
            review_state.get("signature") or ""
        ),
        "weekly_feedback_calibration_patch_ready_for_manual_apply": bool(
            suggested_patch.get("ready_for_manual_apply", False)
        ),
        "weekly_feedback_calibration_patch_readiness_summary": str(
            suggested_patch.get("readiness_summary") or ""
        ),
        "weekly_feedback_calibration_patch": dict(suggested_patch),
    }


def dashboard_control_portfolio_identity_fields(
    *,
    report_market: str,
    watchlist: str,
    portfolio_id: str,
    account_mode: str,
    execution_control_mode: str,
) -> Dict[str, Any]:
    return {
        "market": str(report_market or ""),
        "watchlist": str(watchlist or ""),
        "portfolio_id": str(portfolio_id or ""),
        "account_mode": str(account_mode or ""),
        "execution_control_mode": str(execution_control_mode or ""),
    }


def dashboard_control_portfolio_flag_fields(
    item: Dict[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(item or {})
    return {
        "run_investment_paper": bool(payload.get("run_investment_paper", False)),
        "force_local_paper_ledger": bool(payload.get("force_local_paper_ledger", False)),
        "run_investment_execution": bool(payload.get("run_investment_execution", False)),
        "submit_investment_execution": bool(payload.get("submit_investment_execution", False)),
        "run_investment_guard": bool(payload.get("run_investment_guard", False)),
        "submit_investment_guard": bool(payload.get("submit_investment_guard", False)),
        "run_investment_opportunity": bool(payload.get("run_investment_opportunity", False)),
    }


def dashboard_control_portfolio_feedback_fields(
    *,
    feedback_signature: str,
    confirmed_signature: str,
    confirmed_ts: str,
    automation_rows: Mapping[str, Dict[str, Any]] | None = None,
    account_mode: str = "",
    live_auto_apply_enabled: bool = False,
) -> Dict[str, Any]:
    rows = {str(kind): dict(raw or {}) for kind, raw in dict(automation_rows or {}).items()}
    confirmable_feedback_present = (
        any(
            str(row.get("calibration_apply_mode") or "").strip().upper() in {"AUTO_APPLY", "SUGGEST_ONLY"}
            for row in rows.values()
        )
        if rows
        else bool(feedback_signature)
    )
    return {
        "weekly_feedback_present": bool(feedback_signature),
        "weekly_feedback_signature": str(feedback_signature or ""),
        "weekly_feedback_confirmed_signature": str(confirmed_signature or ""),
        "weekly_feedback_confirmed_ts": str(confirmed_ts or ""),
        "weekly_feedback_automation_modes": {
            kind: str(raw.get("calibration_apply_mode") or "")
            for kind, raw in rows.items()
        },
        "weekly_feedback_pending_live_confirm": bool(
            str(account_mode or "").strip().lower() == "live"
            and feedback_signature
            and not bool(live_auto_apply_enabled)
            and str(feedback_signature or "") != str(confirmed_signature or "")
            and confirmable_feedback_present
        ),
    }


def dashboard_control_override_fields(
    row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(row or {})
    return {
        "_dashboard_control_weekly_feedback_confirmed_signature": str(
            payload.get("weekly_feedback_confirmed_signature") or ""
        ),
        "_dashboard_control_weekly_feedback_confirmed_ts": str(
            payload.get("weekly_feedback_confirmed_ts") or ""
        ),
        "_dashboard_control_market_profile_patch_review_signature": str(
            payload.get("weekly_feedback_market_profile_review_signature") or ""
        ),
        "_dashboard_control_market_profile_patch_review_status": str(
            payload.get("weekly_feedback_market_profile_review_status") or ""
        ),
        "_dashboard_control_market_profile_patch_review_ts": str(
            payload.get("weekly_feedback_market_profile_reviewed_ts") or ""
        ),
        "_dashboard_control_market_profile_patch_review_evidence": dict(
            payload.get("weekly_feedback_market_profile_review_evidence") or {}
        ),
        "_dashboard_control_market_profile_patch_review_history": list(
            payload.get("weekly_feedback_market_profile_review_history") or []
        ),
        "_dashboard_control_calibration_patch_review_signature": str(
            payload.get("weekly_feedback_calibration_patch_review_signature") or ""
        ),
        "_dashboard_control_calibration_patch_review_status": str(
            payload.get("weekly_feedback_calibration_patch_review_status") or ""
        ),
        "_dashboard_control_calibration_patch_review_ts": str(
            payload.get("weekly_feedback_calibration_patch_reviewed_ts") or ""
        ),
        "_dashboard_control_calibration_patch_review_evidence": dict(
            payload.get("weekly_feedback_calibration_patch_review_evidence") or {}
        ),
        "_dashboard_control_calibration_patch_review_history": list(
            payload.get("weekly_feedback_calibration_patch_review_history") or []
        ),
    }


def market_profile_manual_patch_candidate(
    portfolio_id: str,
    row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(row or {})
    suggested_patch = dict(payload.get("weekly_feedback_market_profile_suggested_patch") or {})
    manual_apply_patch = dict(payload.get("weekly_feedback_market_profile_manual_apply_patch") or {})
    if (
        not bool(payload.get("weekly_feedback_market_profile_review_required", False))
        and not suggested_patch
        and not manual_apply_patch
    ):
        return {}
    return {
        "portfolio_id": str(portfolio_id or ""),
        "market": str(payload.get("market") or ""),
        "watchlist": str(payload.get("watchlist") or ""),
        "account_mode": str(payload.get("account_mode") or ""),
        "execution_control_mode": str(payload.get("execution_control_mode") or ""),
        "tuning_action": str(payload.get("weekly_feedback_market_profile_tuning_action") or ""),
        "tuning_target": str(payload.get("weekly_feedback_market_profile_tuning_target") or ""),
        "tuning_note": str(payload.get("weekly_feedback_market_profile_tuning_note") or ""),
        "review_required": bool(payload.get("weekly_feedback_market_profile_review_required", False)),
        "review_summary": str(payload.get("weekly_feedback_market_profile_review_summary") or ""),
        "suggested_patch_summary": str(payload.get("weekly_feedback_market_profile_suggested_patch_summary") or ""),
        "primary_summary": str(payload.get("weekly_feedback_market_profile_primary_summary") or ""),
        "manual_apply_summary": str(payload.get("weekly_feedback_market_profile_manual_apply_summary") or ""),
        "review_status": str(payload.get("weekly_feedback_market_profile_review_status") or ""),
        "review_status_label": str(payload.get("weekly_feedback_market_profile_review_status_label") or "-"),
        "review_status_summary": str(payload.get("weekly_feedback_market_profile_review_status_summary") or "-"),
        "reviewed_ts": str(payload.get("weekly_feedback_market_profile_reviewed_ts") or ""),
        "applied_ts": str(payload.get("weekly_feedback_market_profile_applied_ts") or ""),
        "review_evidence_summary": str(payload.get("weekly_feedback_market_profile_review_evidence_summary") or ""),
        "review_evidence": dict(payload.get("weekly_feedback_market_profile_review_evidence") or {}),
        "review_history_summary": str(payload.get("weekly_feedback_market_profile_review_history_summary") or ""),
        "review_history": list(payload.get("weekly_feedback_market_profile_review_history") or []),
        "ready_for_manual_apply": bool(payload.get("weekly_feedback_market_profile_ready_for_manual_apply", False)),
        "readiness_summary": str(payload.get("weekly_feedback_market_profile_readiness_summary") or ""),
        "manual_apply_patch": manual_apply_patch,
        "suggested_patch": suggested_patch,
    }


def calibration_patch_candidate(
    portfolio_id: str,
    row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(row or {})
    suggested_patch = dict(payload.get("weekly_feedback_calibration_patch") or {})
    if not suggested_patch:
        return {}
    return {
        "portfolio_id": str(portfolio_id or ""),
        "market": str(payload.get("market") or ""),
        "watchlist": str(payload.get("watchlist") or ""),
        "account_mode": str(payload.get("account_mode") or ""),
        "execution_control_mode": str(payload.get("execution_control_mode") or ""),
        "review_required": bool(payload.get("weekly_feedback_calibration_patch_review_required", False)),
        "item_count": int(payload.get("weekly_feedback_calibration_patch_item_count", 0) or 0),
        "summary": str(payload.get("weekly_feedback_calibration_patch_summary") or ""),
        "primary_summary": str(payload.get("weekly_feedback_calibration_patch_primary_summary") or ""),
        "manual_apply_summary": str(payload.get("weekly_feedback_calibration_patch_manual_apply_summary") or ""),
        "review_status": str(payload.get("weekly_feedback_calibration_patch_review_status") or ""),
        "review_status_label": str(payload.get("weekly_feedback_calibration_patch_review_status_label") or "-"),
        "review_status_summary": str(payload.get("weekly_feedback_calibration_patch_review_status_summary") or "-"),
        "reviewed_ts": str(payload.get("weekly_feedback_calibration_patch_reviewed_ts") or ""),
        "applied_ts": str(payload.get("weekly_feedback_calibration_patch_applied_ts") or ""),
        "review_evidence_summary": str(payload.get("weekly_feedback_calibration_patch_review_evidence_summary") or ""),
        "review_evidence": dict(payload.get("weekly_feedback_calibration_patch_review_evidence") or {}),
        "review_history_summary": str(payload.get("weekly_feedback_calibration_patch_review_history_summary") or ""),
        "review_history": list(payload.get("weekly_feedback_calibration_patch_review_history") or []),
        "ready_for_manual_apply": bool(payload.get("weekly_feedback_calibration_patch_ready_for_manual_apply", False)),
        "readiness_summary": str(payload.get("weekly_feedback_calibration_patch_readiness_summary") or ""),
        "manual_apply_patch": dict(payload.get("weekly_feedback_calibration_patch_manual_apply_patch") or {}),
        "suggested_patch": suggested_patch,
    }


def sorted_patch_candidates(
    candidates: List[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in list(candidates or [])]
    rows.sort(
        key=lambda row: (
            0 if bool(row.get("ready_for_manual_apply", False)) else 1,
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return rows


def overlay_patch_metadata(
    *,
    market_profile_tuning_row: Dict[str, Any] | None,
    market_profile_review_draft: Dict[str, Any] | None,
    market_profile_suggested_patch: Dict[str, Any] | None,
    calibration_patch_suggested_patch: Dict[str, Any] | None,
    existing_feedback: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    tuning_row = dict(market_profile_tuning_row or {})
    review_draft = dict(market_profile_review_draft or {})
    market_profile_patch = dict(market_profile_suggested_patch or {})
    calibration_patch = dict(calibration_patch_suggested_patch or {})
    existing = dict(existing_feedback or {})
    tuning_action = str(
        tuning_row.get("market_profile_tuning_action")
        or existing.get("market_profile_tuning_action")
        or ""
    )
    return {
        "market_profile": str(
            tuning_row.get("adaptive_strategy_active_market_profile")
            or existing.get("market_profile")
            or ""
        ),
        "market_profile_note": str(
            tuning_row.get("adaptive_strategy_market_profile_note")
            or existing.get("market_profile_note")
            or ""
        ),
        "market_profile_tuning_target": str(
            tuning_row.get("market_profile_tuning_target")
            or existing.get("market_profile_tuning_target")
            or ""
        ),
        "market_profile_tuning_action": tuning_action,
        "market_profile_tuning_bias": str(
            tuning_row.get("market_profile_tuning_bias")
            or existing.get("market_profile_tuning_bias")
            or ""
        ),
        "market_profile_tuning_note": str(
            tuning_row.get("market_profile_tuning_note")
            or existing.get("market_profile_tuning_note")
            or ""
        ),
        "market_profile_review_required": bool(
            tuning_action.upper() in {"REVIEW_EXECUTION_GATE", "REVIEW_REGIME_PLAN"}
        ),
        "market_profile_review_summary": str(
            review_draft.get("summary")
            or existing.get("market_profile_review_summary")
            or ""
        ),
        "market_profile_review_draft": dict(
            review_draft or dict(existing.get("market_profile_review_draft") or {})
        ),
        "market_profile_suggested_patch_summary": str(
            market_profile_patch.get("summary")
            or existing.get("market_profile_suggested_patch_summary")
            or ""
        ),
        "market_profile_primary_summary": str(
            market_profile_patch.get("primary_summary")
            or existing.get("market_profile_primary_summary")
            or ""
        ),
        "market_profile_primary_item": dict(
            market_profile_patch.get("primary_item")
            or dict(existing.get("market_profile_primary_item") or {})
        ),
        "market_profile_manual_apply_summary": str(
            market_profile_patch.get("manual_apply_summary")
            or existing.get("market_profile_manual_apply_summary")
            or ""
        ),
        "market_profile_manual_apply_patch": dict(
            market_profile_patch.get("manual_apply_patch")
            or dict(existing.get("market_profile_manual_apply_patch") or {})
        ),
        "market_profile_suggested_patch": dict(
            market_profile_patch
            or dict(existing.get("market_profile_suggested_patch") or {})
        ),
        "calibration_patch_suggested_patch_summary": str(
            calibration_patch.get("summary")
            or existing.get("calibration_patch_suggested_patch_summary")
            or ""
        ),
        "calibration_patch_primary_summary": str(
            calibration_patch.get("primary_summary")
            or existing.get("calibration_patch_primary_summary")
            or ""
        ),
        "calibration_patch_primary_item": dict(
            calibration_patch.get("primary_item")
            or dict(existing.get("calibration_patch_primary_item") or {})
        ),
        "calibration_patch_manual_apply_summary": str(
            calibration_patch.get("manual_apply_summary")
            or existing.get("calibration_patch_manual_apply_summary")
            or ""
        ),
        "calibration_patch_manual_apply_patch": dict(
            calibration_patch.get("manual_apply_patch")
            or dict(existing.get("calibration_patch_manual_apply_patch") or {})
        ),
        "calibration_patch_suggested_patch": dict(
            calibration_patch
            or dict(existing.get("calibration_patch_suggested_patch") or {})
        ),
    }


def overlay_feedback_identity(
    *,
    primary_row: Dict[str, Any] | None,
    secondary_row: Dict[str, Any] | None = None,
    existing_feedback: Dict[str, Any] | None = None,
    shadow_apply_mode: str = "",
    execution_apply_mode: str = "",
    default_scope: str = "paper_only",
) -> Dict[str, Any]:
    primary = dict(primary_row or {})
    secondary = dict(secondary_row or {})
    existing = dict(existing_feedback or {})
    return {
        "portfolio_id": str(
            primary.get("portfolio_id")
            or secondary.get("portfolio_id")
            or existing.get("portfolio_id")
            or ""
        ),
        "market": str(
            primary.get("market")
            or secondary.get("market")
            or existing.get("market")
            or ""
        ),
        "shadow_calibration_apply_mode": str(shadow_apply_mode or ""),
        "execution_calibration_apply_mode": str(execution_apply_mode or ""),
        "feedback_scope": str(
            primary.get("feedback_scope")
            or secondary.get("feedback_scope")
            or existing.get("feedback_scope")
            or default_scope
        ),
    }


def overlay_effective_feedback_rows(
    *,
    rows_by_kind: Mapping[str, Dict[str, Any] | None] | None = None,
    auto_apply_enabled: Mapping[str, bool] | None = None,
) -> Dict[str, Dict[str, Any]]:
    rows = dict(rows_by_kind or {})
    enabled_map = dict(auto_apply_enabled or {})
    payload: Dict[str, Dict[str, Any]] = {}
    for kind, row in rows.items():
        if enabled_map.get(str(kind), False):
            payload[str(kind)] = dict(row or {})
        else:
            payload[str(kind)] = {}
    return payload


def overlay_should_write(
    *,
    rows: List[Dict[str, Any] | None] | None = None,
    penalty_rows: List[Dict[str, Any] | None] | None = None,
    auto_apply_enabled: bool = True,
) -> bool:
    if not bool(auto_apply_enabled):
        return False
    if any(bool(dict(row or {})) for row in list(rows or [])):
        return True
    if any(bool(dict(row or {})) for row in list(penalty_rows or [])):
        return True
    return False


def overlay_weekly_feedback_payload(
    *,
    sections: List[Mapping[str, Any] | None] | None = None,
    extra_fields: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"enabled": True}
    for section in list(sections or []):
        payload.update(dict(section or {}))
    payload.update(dict(extra_fields or {}))
    return payload


def overlay_shadow_feedback_fields(
    *,
    shadow_feedback_row: Dict[str, Any] | None,
    existing_feedback: Dict[str, Any] | None = None,
    include_reason_field: bool = False,
) -> Dict[str, Any]:
    shadow = dict(shadow_feedback_row or {})
    existing = dict(existing_feedback or {})
    payload = {
        "shadow_review_action": str(shadow.get("shadow_review_action") or ""),
        "shadow_feedback_base_confidence": float(
            clamp_float(shadow.get("feedback_base_confidence", feedback_confidence_value(shadow)), 0.0, 1.0)
        )
        if shadow
        else 0.0,
        "shadow_feedback_calibration_score": float(
            clamp_float(shadow.get("feedback_calibration_score", 0.5), 0.0, 1.0)
        ),
        "shadow_feedback_confidence": float(
            feedback_confidence_value(shadow)
        )
        if shadow
        else 0.0,
    }
    if include_reason_field:
        payload["shadow_feedback_reason"] = str(
            shadow.get("feedback_reason") or existing.get("shadow_feedback_reason") or ""
        )
    return payload


def overlay_execution_feedback_fields(
    *,
    execution_feedback_row: Dict[str, Any] | None,
    existing_feedback: Dict[str, Any] | None = None,
    execution_feedback_action: str = "",
    execution_feedback_reason: str = "",
    include_hotspot_fields: bool = False,
) -> Dict[str, Any]:
    execution = dict(execution_feedback_row or {})
    existing = dict(existing_feedback or {})
    payload = {
        "execution_feedback_action": str(
            execution_feedback_action or existing.get("execution_feedback_action") or ""
        ),
        "execution_feedback_base_confidence": float(
            clamp_float(execution.get("feedback_base_confidence", feedback_confidence_value(execution)), 0.0, 1.0)
        )
        if execution
        else 0.0,
        "execution_feedback_calibration_score": float(
            clamp_float(execution.get("feedback_calibration_score", 0.5), 0.0, 1.0)
        ),
        "execution_feedback_confidence": float(
            feedback_confidence_value(execution)
        )
        if execution
        else 0.0,
    }
    if execution_feedback_reason:
        payload["execution_feedback_reason"] = str(execution_feedback_reason)
    if include_hotspot_fields:
        payload.update(
            {
                "execution_dominant_session_bucket": str(
                    execution.get("dominant_execution_session_bucket")
                    or existing.get("execution_dominant_session_bucket")
                    or ""
                ),
                "execution_dominant_session_label": str(
                    execution.get("dominant_execution_session_label")
                    or existing.get("execution_dominant_session_label")
                    or ""
                ),
                "execution_dominant_hotspot_symbol": str(
                    execution.get("dominant_execution_hotspot_symbol")
                    or existing.get("execution_dominant_hotspot_symbol")
                    or ""
                ),
                "execution_dominant_hotspot_session_label": str(
                    execution.get("dominant_execution_hotspot_session_label")
                    or existing.get("execution_dominant_hotspot_session_label")
                    or ""
                ),
                "execution_session_feedback_json": str(
                    execution.get("execution_session_feedback_json")
                    or existing.get("execution_session_feedback_json")
                    or ""
                ),
                "execution_hotspots_json": str(
                    execution.get("execution_hotspots_json")
                    or existing.get("execution_hotspots_json")
                    or ""
                ),
            }
        )
    return payload


def overlay_previous_execution_penalties(
    *,
    existing_feedback: Dict[str, Any] | None = None,
    execution_auto_apply_enabled: bool = True,
    has_execution_automation: bool = False,
    legacy_feedback: Dict[str, Any] | None = None,
    prefer_hotspot_penalties: bool = False,
) -> List[Dict[str, Any]]:
    existing = dict(existing_feedback or {})
    penalties_source = (
        existing.get("execution_hotspot_penalties") or existing.get("execution_penalties")
        if prefer_hotspot_penalties
        else existing.get("execution_penalties")
    )
    penalties = parse_feedback_penalty_rows(penalties_source)
    if not penalties and legacy_feedback:
        penalties = parse_feedback_penalty_rows(dict(legacy_feedback or {}).get("execution_penalties"))
    if not execution_auto_apply_enabled and has_execution_automation:
        return []
    return penalties


def overlay_feedback_reason(
    *,
    primary_row: Dict[str, Any] | None = None,
    secondary_row: Dict[str, Any] | None = None,
    previous_execution_penalties: List[Dict[str, Any]] | None = None,
    current_execution_penalties: List[Dict[str, Any]] | None = None,
) -> str:
    primary = dict(primary_row or {})
    secondary = dict(secondary_row or {})
    parts = [
        str(primary.get("feedback_reason") or "").strip(),
        str(secondary.get("feedback_reason") or "").strip(),
    ]
    if list(previous_execution_penalties or []) and not list(current_execution_penalties or []):
        parts.append("沿用并衰减上一轮执行热点惩罚。")
    return " ".join(part for part in parts if part)


def overlay_execution_feedback_action_value(
    *,
    execution_feedback_row: Dict[str, Any] | None,
    existing_feedback: Dict[str, Any] | None = None,
    merged_execution_penalties: List[Dict[str, Any]] | None = None,
    current_execution_penalties: List[Dict[str, Any]] | None = None,
) -> str:
    execution = dict(execution_feedback_row or {})
    existing = dict(existing_feedback or {})
    return str(
        execution.get("execution_feedback_action")
        or ("DECAY" if list(merged_execution_penalties or []) and not list(current_execution_penalties or []) else "")
        or existing.get("execution_feedback_action")
        or ""
    )


def overlay_shadow_execution_config_fields(
    *,
    execution_config: Dict[str, Any] | None,
    shadow_feedback_row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    execution = dict(execution_config or {})
    shadow_feedback = dict(shadow_feedback_row or {})
    if not shadow_feedback:
        return execution
    execution["shadow_ml_min_score_auto_submit"] = round(
        clamp_float(
            float(execution.get("shadow_ml_min_score_auto_submit", 0.0) or 0.0)
            + scale_feedback_delta(
                shadow_feedback.get("execution_shadow_score_delta", 0.0),
                shadow_feedback,
                min_abs=0.002,
            ),
            -0.25,
            1.0,
        ),
        6,
    )
    execution["shadow_ml_min_positive_prob_auto_submit"] = round(
        clamp_float(
            float(execution.get("shadow_ml_min_positive_prob_auto_submit", 0.50) or 0.50)
            + scale_feedback_delta(
                shadow_feedback.get("execution_shadow_prob_delta", 0.0),
                shadow_feedback,
                min_abs=0.002,
            ),
            0.0,
            1.0,
        ),
        6,
    )
    return execution


def overlay_execution_config_fields(
    *,
    execution_config: Dict[str, Any] | None,
    execution_feedback_row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    execution = dict(execution_config or {})
    feedback = dict(execution_feedback_row or {})
    if not feedback:
        return execution
    execution["adv_max_participation_pct"] = round(
        clamp_float(
            float(execution.get("adv_max_participation_pct", 0.05) or 0.05)
            + scale_feedback_delta(
                feedback.get("execution_adv_max_participation_pct_delta", 0.0),
                feedback,
                min_abs=0.001,
            ),
            0.01,
            0.20,
        ),
        6,
    )
    execution["adv_split_trigger_pct"] = round(
        clamp_float(
            float(execution.get("adv_split_trigger_pct", 0.02) or 0.02)
            + scale_feedback_delta(
                feedback.get("execution_adv_split_trigger_pct_delta", 0.0),
                feedback,
                min_abs=0.001,
            ),
            0.005,
            0.10,
        ),
        6,
    )
    execution["max_slices_per_symbol"] = int(
        round(
            clamp_float(
                float(execution.get("max_slices_per_symbol", 4) or 4)
                + scale_feedback_delta(
                    feedback.get("execution_max_slices_per_symbol_delta", 0.0),
                    feedback,
                    min_abs=1.0,
                ),
                1.0,
                8.0,
            )
        )
    )
    execution["open_session_participation_scale"] = round(
        clamp_float(
            float(execution.get("open_session_participation_scale", 0.70) or 0.70)
            + scale_feedback_delta(
                feedback.get("execution_open_session_participation_scale_delta", 0.0),
                feedback,
                min_abs=0.01,
            ),
            0.30,
            1.50,
        ),
        6,
    )
    execution["midday_session_participation_scale"] = round(
        clamp_float(
            float(execution.get("midday_session_participation_scale", 1.00) or 1.00)
            + scale_feedback_delta(
                feedback.get("execution_midday_session_participation_scale_delta", 0.0),
                feedback,
                min_abs=0.01,
            ),
            0.30,
            1.50,
        ),
        6,
    )
    execution["close_session_participation_scale"] = round(
        clamp_float(
            float(execution.get("close_session_participation_scale", 0.85) or 0.85)
            + scale_feedback_delta(
                feedback.get("execution_close_session_participation_scale_delta", 0.0),
                feedback,
                min_abs=0.01,
            ),
            0.30,
            1.50,
        ),
        6,
    )
    return execution


def overlay_execution_penalty_fields(
    *,
    execution_config: Dict[str, Any] | None,
    execution_feedback_row: Dict[str, Any] | None,
    previous_execution_penalties: List[Dict[str, Any]] | None,
) -> Dict[str, Any]:
    execution = dict(execution_config or {})
    feedback = dict(execution_feedback_row or {})
    previous_penalties = [dict(row) for row in list(previous_execution_penalties or [])]
    current_penalties = scale_feedback_penalty_rows(
        parse_feedback_penalty_rows(feedback.get("execution_penalties_json")),
        feedback,
    )
    merged_penalties = merge_execution_feedback_penalties(current_penalties, previous_penalties)
    if merged_penalties:
        execution["execution_hotspot_penalties"] = merged_penalties
    else:
        execution.pop("execution_hotspot_penalties", None)
    return {
        "execution": execution,
        "current_execution_penalties": current_penalties,
        "execution_hotspot_penalties": merged_penalties,
    }


def overlay_investment_config_fields(
    *,
    scoring_config: Dict[str, Any] | None,
    plan_config: Dict[str, Any] | None,
    shadow_feedback_row: Dict[str, Any] | None,
) -> Dict[str, Dict[str, Any]]:
    scoring = dict(scoring_config or {})
    plan = dict(plan_config or {})
    feedback = dict(shadow_feedback_row or {})
    if not feedback:
        return {"scoring": scoring, "plan": plan}
    scoring["accumulate_threshold"] = round(
        clamp_float(
            float(scoring.get("accumulate_threshold", 0.35) or 0.35)
            + scale_feedback_delta(
                feedback.get("scoring_accumulate_threshold_delta", 0.0),
                feedback,
                min_abs=0.002,
            ),
            -1.0,
            1.0,
        ),
        6,
    )
    scoring["execution_ready_threshold"] = round(
        clamp_float(
            float(scoring.get("execution_ready_threshold", 0.08) or 0.08)
            + scale_feedback_delta(
                feedback.get("scoring_execution_ready_threshold_delta", 0.0),
                feedback,
                min_abs=0.002,
            ),
            0.0,
            1.0,
        ),
        6,
    )
    plan["review_window_days"] = int(
        round(
            clamp_float(
                int(plan.get("review_window_days", 90) or 90)
                + scale_feedback_delta(
                    feedback.get("plan_review_window_days_delta", 0),
                    feedback,
                ),
                7.0,
                365.0,
            )
        )
    )
    return {
        "scoring": scoring,
        "plan": plan,
    }


def overlay_investment_penalty_fields(
    *,
    shadow_feedback_row: Dict[str, Any] | None,
    execution_feedback_row: Dict[str, Any] | None,
    previous_execution_penalties: List[Dict[str, Any]] | None,
) -> Dict[str, List[Dict[str, Any]]]:
    shadow_feedback = dict(shadow_feedback_row or {})
    execution_feedback = dict(execution_feedback_row or {})
    previous_penalties = [dict(row) for row in list(previous_execution_penalties or [])]
    signal_penalties = scale_feedback_penalty_rows(
        parse_feedback_penalty_rows(shadow_feedback.get("signal_penalties_json")),
        shadow_feedback,
    )
    current_execution_penalties = scale_feedback_penalty_rows(
        parse_feedback_penalty_rows(execution_feedback.get("execution_penalties_json")),
        execution_feedback,
    )
    execution_penalties = merge_execution_feedback_penalties(
        current_execution_penalties,
        previous_penalties,
    )
    return {
        "signal_penalties": signal_penalties,
        "current_execution_penalties": current_execution_penalties,
        "execution_penalties": execution_penalties,
    }


def overlay_paper_config_fields(
    *,
    paper_config: Dict[str, Any] | None,
    risk_feedback_row: Dict[str, Any] | None,
) -> Dict[str, Any]:
    paper = dict(paper_config or {})
    feedback = dict(risk_feedback_row or {})
    if not feedback:
        return paper
    next_max_single = round(
        clamp_float(
            float(paper.get("max_single_weight", 0.22) or 0.22)
            + scale_feedback_delta(
                feedback.get("paper_max_single_weight_delta", 0.0),
                feedback,
                min_abs=0.002,
            ),
            0.05,
            0.50,
        ),
        6,
    )
    next_max_sector = round(
        clamp_float(
            float(paper.get("max_sector_weight", 0.40) or 0.40)
            + scale_feedback_delta(
                feedback.get("paper_max_sector_weight_delta", 0.0),
                feedback,
                min_abs=0.002,
            ),
            0.10,
            1.00,
        ),
        6,
    )
    next_max_net = round(
        clamp_float(
            float(paper.get("max_net_exposure", 1.00) or 1.00)
            + scale_feedback_delta(
                feedback.get("paper_max_net_exposure_delta", 0.0),
                feedback,
                min_abs=0.005,
            ),
            0.20,
            1.50,
        ),
        6,
    )
    next_max_gross = round(
        clamp_float(
            float(paper.get("max_gross_exposure", 1.00) or 1.00)
            + scale_feedback_delta(
                feedback.get("paper_max_gross_exposure_delta", 0.0),
                feedback,
                min_abs=0.005,
            ),
            0.20,
            2.00,
        ),
        6,
    )
    next_max_short = round(
        clamp_float(
            float(paper.get("max_short_exposure", 0.35) or 0.35)
            + scale_feedback_delta(
                feedback.get("paper_max_short_exposure_delta", 0.0),
                feedback,
                min_abs=0.002,
            ),
            0.0,
            min(next_max_gross, 1.00),
        ),
        6,
    )
    next_corr_soft = round(
        clamp_float(
            float(paper.get("correlation_soft_limit", 0.62) or 0.62)
            + scale_feedback_delta(
                feedback.get("paper_correlation_soft_limit_delta", 0.0),
                feedback,
                min_abs=0.005,
            ),
            0.25,
            0.95,
        ),
        6,
    )
    paper["max_single_weight"] = next_max_single
    paper["max_sector_weight"] = next_max_sector
    paper["max_net_exposure"] = next_max_net
    paper["max_gross_exposure"] = next_max_gross
    paper["max_short_exposure"] = next_max_short
    paper["correlation_soft_limit"] = next_corr_soft
    return paper


def overlay_risk_feedback_fields(
    *,
    risk_feedback_row: Dict[str, Any] | None,
    risk_apply_mode: str = "",
    existing_feedback: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    feedback = dict(risk_feedback_row or {})
    existing = dict(existing_feedback or {})
    return {
        "portfolio_id": str(
            feedback.get("portfolio_id")
            or existing.get("portfolio_id")
            or ""
        ),
        "market": str(
            feedback.get("market")
            or existing.get("market")
            or ""
        ),
        "risk_calibration_apply_mode": str(
            risk_apply_mode or existing.get("risk_calibration_apply_mode") or ""
        ),
        "risk_feedback_action": str(
            feedback.get("risk_feedback_action")
            or existing.get("risk_feedback_action")
            or ""
        ),
        "risk_feedback_base_confidence": float(
            clamp_float(
                feedback.get("feedback_base_confidence", feedback_confidence_value(feedback)),
                0.0,
                1.0,
            )
        )
        if feedback
        else 0.0,
        "risk_feedback_calibration_score": float(
            clamp_float(
                feedback.get("feedback_calibration_score", 0.5),
                0.0,
                1.0,
            )
        ),
        "risk_feedback_confidence": float(feedback_confidence_value(feedback)) if feedback else 0.0,
        "feedback_scope": str(
            feedback.get("feedback_scope")
            or existing.get("feedback_scope")
            or "paper_only"
        ),
        "feedback_reason": str(
            feedback.get("feedback_reason")
            or existing.get("feedback_reason")
            or ""
        ),
    }
