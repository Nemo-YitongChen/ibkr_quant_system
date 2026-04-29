from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ..common.markets import resolve_market_code
from ..common.storage import Storage
from .review_weekly_common_support import _safe_float


def _weekly_tuning_history_trend_label(
    delta: float,
    *,
    threshold: float,
    improving_if_negative: bool = False,
) -> str:
    value = float(delta or 0.0)
    if improving_if_negative:
        if value <= -abs(float(threshold or 0.0)):
            return "IMPROVING"
        if value >= abs(float(threshold or 0.0)):
            return "WORSENING"
        return "STABLE"
    if value >= abs(float(threshold or 0.0)):
        return "IMPROVING"
    if value <= -abs(float(threshold or 0.0)):
        return "WORSENING"
    return "STABLE"

def _build_weekly_tuning_history_overview(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        if not portfolio_id or not market:
            continue
        history_rows = storage.get_recent_investment_weekly_tuning_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(2, int(limit)),
        )
        if not history_rows:
            continue
        latest = dict(history_rows[0] or {})
        baseline = dict(history_rows[-1] or latest)
        driver_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('dominant_driver') or '-')}"
            for item in reversed(history_rows)
        )
        tuning_action_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('market_profile_tuning_action') or '-')}"
            for item in reversed(history_rows)
        )
        signal_quality_delta = float(latest.get("signal_quality_score", 0.0) or 0.0) - float(
            baseline.get("signal_quality_score", 0.0) or 0.0
        )
        execution_cost_gap_delta = float(latest.get("execution_cost_gap", 0.0) or 0.0) - float(
            baseline.get("execution_cost_gap", 0.0) or 0.0
        )
        blocked_weight_delta = float(latest.get("execution_gate_blocked_weight", 0.0) or 0.0) - float(
            baseline.get("execution_gate_blocked_weight", 0.0) or 0.0
        )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "weeks_tracked": int(len(history_rows)),
                "latest_week_label": str(latest.get("week_label") or ""),
                "baseline_week_label": str(baseline.get("week_label") or ""),
                "latest_dominant_driver": str(latest.get("dominant_driver") or ""),
                "latest_market_profile_tuning_action": str(latest.get("market_profile_tuning_action") or ""),
                "latest_market_profile_ready_for_manual_apply": int(
                    latest.get("market_profile_ready_for_manual_apply", 0) or 0
                ),
                "driver_chain": driver_chain,
                "tuning_action_chain": tuning_action_chain,
                "latest_signal_quality_score": float(latest.get("signal_quality_score", 0.0) or 0.0),
                "baseline_signal_quality_score": float(baseline.get("signal_quality_score", 0.0) or 0.0),
                "signal_quality_delta": float(signal_quality_delta),
                "signal_quality_trend": _weekly_tuning_history_trend_label(signal_quality_delta, threshold=0.05),
                "latest_execution_cost_gap": float(latest.get("execution_cost_gap", 0.0) or 0.0),
                "baseline_execution_cost_gap": float(baseline.get("execution_cost_gap", 0.0) or 0.0),
                "execution_cost_gap_delta": float(execution_cost_gap_delta),
                "execution_cost_gap_trend": _weekly_tuning_history_trend_label(
                    execution_cost_gap_delta,
                    threshold=5.0,
                    improving_if_negative=True,
                ),
                "latest_execution_gate_blocked_weight": float(
                    latest.get("execution_gate_blocked_weight", 0.0) or 0.0
                ),
                "baseline_execution_gate_blocked_weight": float(
                    baseline.get("execution_gate_blocked_weight", 0.0) or 0.0
                ),
                "execution_gate_blocked_weight_delta": float(blocked_weight_delta),
                "execution_gate_pressure_trend": _weekly_tuning_history_trend_label(
                    blocked_weight_delta,
                    threshold=0.01,
                    improving_if_negative=True,
                ),
            }
        )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out

def _patch_review_kind_label(kind: str) -> str:
    raw = str(kind or "").strip().lower()
    if raw == "market_profile":
        return "市场档案"
    if raw == "calibration":
        return "校准补丁"
    return raw or "-"

def _patch_review_week_start_dt(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None

def _build_weekly_patch_governance_summary_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 24,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    cycle_rows: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(raw.get("market") or ""))
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_patch_review_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(20, int(limit) * 8),
        )
        if not history_rows:
            continue
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for item in list(history_rows or []):
            row = dict(item)
            patch_kind = str(row.get("patch_kind") or "").strip().lower()
            if not patch_kind:
                continue
            feedback_signature = str(row.get("feedback_signature") or "").strip()
            if not feedback_signature:
                details_json = dict(row.get("details_json") or {})
                primary_item = dict(details_json.get("primary_item") or {})
                feedback_signature = (
                    f"{patch_kind}|"
                    f"{str(primary_item.get('config_path') or row.get('config_path') or '').strip()}|"
                    f"{str(row.get('week_label') or '').strip()}"
                )
            grouped.setdefault((patch_kind, feedback_signature), []).append(row)
        for (patch_kind, _feedback_signature), cycle_events in grouped.items():
            cycle_events.sort(
                key=lambda item: (
                    str(item.get("week_start") or ""),
                    str(item.get("ts") or ""),
                    int(item.get("id", 0) or 0),
                )
            )
            first = dict(cycle_events[0] or {})
            latest = dict(cycle_events[-1] or {})
            first_details = dict(first.get("details_json") or {})
            latest_details = dict(latest.get("details_json") or {})
            latest_primary_item = dict(latest_details.get("primary_item") or first_details.get("primary_item") or {})
            config_path = str(latest_primary_item.get("config_path") or latest.get("config_path") or "").strip()
            field = str(latest_primary_item.get("field") or "").strip()
            if not field and config_path:
                field = config_path.split(".")[-1]
            scope_label = str(
                latest_primary_item.get("scope_label")
                or latest_primary_item.get("scope")
                or latest.get("scope")
                or "-"
            )
            applied_row = next(
                (
                    dict(item)
                    for item in cycle_events
                    if str(item.get("review_status") or "").strip().upper() == "APPLIED"
                ),
                {},
            )
            start_week = _patch_review_week_start_dt(str(first.get("week_start") or ""))
            applied_week = _patch_review_week_start_dt(str(applied_row.get("week_start") or ""))
            review_to_apply_weeks = None
            if start_week is not None and applied_week is not None:
                review_to_apply_weeks = round(max(0.0, (applied_week - start_week).days / 7.0), 2)
            latest_status = str(latest.get("review_status") or "").strip().upper()
            cycle_rows.append(
                {
                    "market": market,
                    "portfolio_id": portfolio_id,
                    "patch_kind": patch_kind,
                    "patch_kind_label": _patch_review_kind_label(patch_kind),
                    "field": field or "-",
                    "scope_label": scope_label,
                    "latest_week_label": str(latest.get("week_label") or "-"),
                    "latest_ts": str(latest.get("ts") or ""),
                    "latest_status": latest_status,
                    "latest_status_label": str(latest.get("review_status_label") or latest_status or "-"),
                    "approved": any(
                        str(item.get("review_status") or "").strip().upper() == "APPROVED"
                        for item in cycle_events
                    ),
                    "rejected": any(
                        str(item.get("review_status") or "").strip().upper() == "REJECTED"
                        for item in cycle_events
                    ),
                    "applied": bool(applied_row),
                    "approved_not_applied": latest_status == "APPROVED" and not bool(applied_row),
                    "open_cycle": latest_status not in {"APPLIED", "REJECTED", "CLEAR"},
                    "review_to_apply_weeks": review_to_apply_weeks,
                }
            )
    grouped_rows: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for cycle in cycle_rows:
        key = (
            str(cycle.get("market") or ""),
            str(cycle.get("patch_kind") or ""),
            str(cycle.get("field") or ""),
            str(cycle.get("scope_label") or ""),
        )
        agg = grouped_rows.get(key)
        if agg is None:
            agg = {
                "market": str(cycle.get("market") or ""),
                "patch_kind_label": str(cycle.get("patch_kind_label") or "-"),
                "field": str(cycle.get("field") or "-"),
                "scope_label": str(cycle.get("scope_label") or "-"),
                "review_cycle_count": 0,
                "approved_count": 0,
                "rejected_count": 0,
                "applied_count": 0,
                "approved_not_applied_count": 0,
                "open_cycle_count": 0,
                "review_to_apply_weeks_values": [],
                "latest_ts": "",
                "latest_week_label": "-",
                "latest_status_label": "-",
                "examples": [],
            }
            grouped_rows[key] = agg
        agg["review_cycle_count"] += 1
        if bool(cycle.get("approved", False)):
            agg["approved_count"] += 1
        if bool(cycle.get("rejected", False)):
            agg["rejected_count"] += 1
        if bool(cycle.get("applied", False)):
            agg["applied_count"] += 1
        if bool(cycle.get("approved_not_applied", False)):
            agg["approved_not_applied_count"] += 1
        if bool(cycle.get("open_cycle", False)):
            agg["open_cycle_count"] += 1
        if cycle.get("review_to_apply_weeks") is not None:
            agg["review_to_apply_weeks_values"].append(float(cycle["review_to_apply_weeks"]))
        latest_ts = str(cycle.get("latest_ts") or "")
        if latest_ts >= str(agg.get("latest_ts") or ""):
            agg["latest_ts"] = latest_ts
            agg["latest_week_label"] = str(cycle.get("latest_week_label") or "-")
            agg["latest_status_label"] = str(cycle.get("latest_status_label") or "-")
        example = f"{str(cycle.get('portfolio_id') or '-') or '-'}:{str(cycle.get('latest_status_label') or '-')}"
        if example not in agg["examples"]:
            agg["examples"].append(example)
    out: List[Dict[str, Any]] = []
    for agg in grouped_rows.values():
        review_cycle_count = max(1, int(agg.get("review_cycle_count", 0) or 0))
        review_to_apply_values = list(agg.get("review_to_apply_weeks_values") or [])
        out.append(
            {
                "market": str(agg.get("market") or ""),
                "patch_kind_label": str(agg.get("patch_kind_label") or "-"),
                "field": str(agg.get("field") or "-"),
                "scope_label": str(agg.get("scope_label") or "-"),
                "review_cycle_count": review_cycle_count,
                "approved_count": int(agg.get("approved_count", 0) or 0),
                "rejected_count": int(agg.get("rejected_count", 0) or 0),
                "applied_count": int(agg.get("applied_count", 0) or 0),
                "approved_not_applied_count": int(agg.get("approved_not_applied_count", 0) or 0),
                "open_cycle_count": int(agg.get("open_cycle_count", 0) or 0),
                "approval_rate": round(float(agg.get("approved_count", 0) or 0) / review_cycle_count, 4),
                "rejection_rate": round(float(agg.get("rejected_count", 0) or 0) / review_cycle_count, 4),
                "apply_rate": round(float(agg.get("applied_count", 0) or 0) / review_cycle_count, 4),
                "avg_review_to_apply_weeks": (
                    round(sum(review_to_apply_values) / len(review_to_apply_values), 2)
                    if review_to_apply_values
                    else None
                ),
                "review_latency_basis": "review_to_apply",
                "latest_week_label": str(agg.get("latest_week_label") or "-"),
                "latest_status_label": str(agg.get("latest_status_label") or "-"),
                "examples": " / ".join(list(agg.get("examples") or [])[:3]) or "-",
            }
        )
    out.sort(
        key=lambda row: (
            -int(row.get("open_cycle_count", 0) or 0),
            -int(row.get("approved_not_applied_count", 0) or 0),
            -int(row.get("review_cycle_count", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("patch_kind_label") or ""),
            str(row.get("field") or ""),
        )
    )
    return out[:24]

def _build_weekly_control_timeseries_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(raw.get("market") or ""))
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen:
            continue
        seen.add(key)
        history_rows = storage.get_recent_investment_weekly_tuning_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(2, int(limit)),
        )
        for item in reversed(list(history_rows or [])):
            strategy_delta = _safe_float(item.get("strategy_control_weight_delta"), 0.0)
            risk_delta = _safe_float(item.get("risk_overlay_weight_delta"), 0.0)
            execution_delta = _safe_float(item.get("execution_gate_blocked_weight"), 0.0)
            total_delta = float(strategy_delta + risk_delta + execution_delta)
            out.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": market,
                    "week_label": str(item.get("week_label") or ""),
                    "week_start": str(item.get("week_start") or ""),
                    "weekly_return": _safe_float(item.get("weekly_return"), 0.0),
                    "signal_quality_score": _safe_float(item.get("signal_quality_score"), 0.0),
                    "execution_cost_gap": _safe_float(item.get("execution_cost_gap"), 0.0),
                    "strategy_control_weight_delta": float(strategy_delta),
                    "risk_overlay_weight_delta": float(risk_delta),
                    "execution_gate_blocked_weight": float(execution_delta),
                    "control_total_weight": float(total_delta),
                    "strategy_control_share": float(strategy_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "risk_overlay_share": float(risk_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "execution_gate_share": float(execution_delta / total_delta) if total_delta > 0.0 else 0.0,
                    "dominant_driver": str(item.get("dominant_driver") or ""),
                }
            )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("week_start") or row.get("week_label") or ""),
        )
    )
    return out
