from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List

from ..common.markets import resolve_market_code
from ..common.storage import Storage
from .review_weekly_feedback_support import (
    _avg_defined,
    _parse_json_dict,
    _parse_json_list,
    _portfolio_row_map,
    _safe_float,
    _safe_int,
)
from .review_weekly_governance_support import _weekly_tuning_history_trend_label


def _decision_evidence_context_maps(
    *,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    return {
        "strategy_context_map": _portfolio_row_map(strategy_context_rows),
        "attribution_map": _portfolio_row_map(attribution_rows),
    }


def _build_weekly_decision_evidence_row(
    row: Dict[str, Any],
    *,
    strategy_context: Dict[str, Any],
    attribution: Dict[str, Any],
) -> Dict[str, Any]:
    realized_edge_bps = row.get("realized_edge_bps")
    if realized_edge_bps in (None, ""):
        realized_edge_bps = row.get("execution_capture_bps")
    return {
        "portfolio_id": str(row.get("portfolio_id") or ""),
        "market": str(row.get("market") or ""),
        "run_id": str(row.get("run_id") or ""),
        "parent_order_key": str(row.get("parent_order_key") or ""),
        "symbol": str(row.get("symbol") or ""),
        "action": str(row.get("action") or ""),
        "decision_status": str(row.get("status_bucket") or ""),
        "candidate_snapshot_id": str(row.get("linked_snapshot_id") or ""),
        "candidate_stage": str(row.get("linked_snapshot_stage") or ""),
        "order_value": float(row.get("order_value", 0.0) or 0.0),
        "fill_notional": float(row.get("fill_notional", 0.0) or 0.0),
        "signal_score": float(row.get("score_before_cost", 0.0) or 0.0),
        "expected_edge_bps": float(row.get("expected_edge_bps", 0.0) or 0.0),
        "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
        "edge_gate_threshold_bps": float(row.get("edge_gate_threshold_bps", 0.0) or 0.0),
        "required_edge_gap_bps": float(row.get("required_edge_gap_bps", 0.0) or 0.0),
        "blocked_market_rule_order_count": int(row.get("blocked_market_rule_order_count", 0) or 0),
        "blocked_edge_order_count": int(row.get("blocked_edge_order_count", 0) or 0),
        "blocked_gate_order_count": int(row.get("blocked_gate_order_count", 0) or 0),
        "dynamic_liquidity_bucket": str(row.get("dynamic_liquidity_bucket") or ""),
        "dynamic_order_adv_pct": float(row.get("avg_dynamic_order_adv_pct", 0.0) or 0.0),
        "slice_count": int(row.get("slice_count", 1) or 1),
        "strategy_control_weight_delta": float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0),
        "risk_overlay_weight_delta": float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0),
        "risk_market_profile_budget_weight_delta": float(
            attribution.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0
        ),
        "risk_throttle_weight_delta": float(attribution.get("risk_throttle_weight_delta", 0.0) or 0.0),
        "risk_recovery_weight_credit": float(attribution.get("risk_recovery_weight_credit", 0.0) or 0.0),
        "execution_gate_blocked_weight": float(attribution.get("execution_gate_blocked_weight", 0.0) or 0.0),
        "strategy_effective_controls_note": str(strategy_context.get("strategy_effective_controls_note") or ""),
        "execution_gate_summary": str(strategy_context.get("execution_gate_summary") or ""),
        "realized_slippage_bps": row.get("realized_slippage_bps"),
        "realized_edge_bps": realized_edge_bps,
        "execution_capture_bps": row.get("execution_capture_bps"),
        "first_fill_delay_seconds": row.get("first_fill_delay_seconds"),
        "outcome_5d_bps": row.get("outcome_5d_future_return_bps"),
        "outcome_20d_bps": row.get("outcome_20d_future_return_bps"),
        "outcome_60d_bps": row.get("outcome_60d_future_return_bps"),
        "outcome_5d_realized_edge_bps": row.get("outcome_5d_realized_edge_bps"),
        "outcome_20d_realized_edge_bps": row.get("outcome_20d_realized_edge_bps"),
        "outcome_60d_realized_edge_bps": row.get("outcome_60d_realized_edge_bps"),
    }


def _build_weekly_decision_evidence_rows(
    execution_parent_rows: List[Dict[str, Any]],
    *,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    context_maps = _decision_evidence_context_maps(
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
    )
    strategy_context_map = dict(context_maps.get("strategy_context_map") or {})
    attribution_map = dict(context_maps.get("attribution_map") or {})
    out: List[Dict[str, Any]] = []
    for raw in list(execution_parent_rows or []):
        row = dict(raw or {})
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        strategy_context = dict(strategy_context_map.get(portfolio_id) or {})
        attribution = dict(attribution_map.get(portfolio_id) or {})
        out.append(
            _build_weekly_decision_evidence_row(
                row,
                strategy_context=strategy_context,
                attribution=attribution,
            )
        )
    out.sort(
        key=lambda item: (
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
            str(item.get("parent_order_key") or ""),
        )
    )
    return out


def _weighted_avg_defined(
    rows: List[Dict[str, Any]],
    key: str,
    *,
    weight_key: str = "order_value",
) -> float | None:
    numerator = 0.0
    denominator = 0.0
    for item in list(rows or []):
        value = item.get(key)
        if value in (None, ""):
            continue
        weight = abs(_safe_float(item.get(weight_key), 0.0))
        if weight <= 0.0:
            weight = 1.0
        numerator += weight * _safe_float(value, 0.0)
        denominator += weight
    if denominator <= 0.0:
        return None
    return float(numerator / denominator)


def _primary_liquidity_bucket(rows: List[Dict[str, Any]]) -> str:
    liquidity_counts: Dict[str, float] = {}
    has_fill_weight = any(abs(_safe_float(item.get("fill_notional"), 0.0)) > 0.0 for item in list(rows or []))
    for item in list(rows or []):
        bucket = str(item.get("dynamic_liquidity_bucket") or "").strip().upper()
        if not bucket:
            continue
        weight = abs(_safe_float(item.get("fill_notional" if has_fill_weight else "order_value"), 0.0))
        liquidity_counts[bucket] = float(liquidity_counts.get(bucket, 0.0) or 0.0) + weight
    if not liquidity_counts:
        return ""
    return max(
        liquidity_counts.items(),
        key=lambda part: (float(part[1] or 0.0), str(part[0] or "")),
    )[0]


def _build_weekly_decision_evidence_summary_rows(
    decision_evidence_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(decision_evidence_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(rows[0].get("market") or ""),
                "decision_evidence_row_count": int(len(rows)),
                "decision_blocked_market_rule_order_count": int(
                    sum(int(item.get("blocked_market_rule_order_count", 0) or 0) for item in rows)
                ),
                "decision_blocked_edge_order_count": int(
                    sum(int(item.get("blocked_edge_order_count", 0) or 0) for item in rows)
                ),
                "decision_primary_liquidity_bucket": str(_primary_liquidity_bucket(rows)),
                "decision_avg_dynamic_order_adv_pct": _weighted_avg_defined(rows, "dynamic_order_adv_pct"),
                "decision_avg_slice_count": _weighted_avg_defined(rows, "slice_count"),
                "decision_avg_expected_edge_bps": _weighted_avg_defined(rows, "expected_edge_bps"),
                "decision_avg_expected_cost_bps": _weighted_avg_defined(rows, "expected_cost_bps"),
                "decision_avg_edge_gate_threshold_bps": _weighted_avg_defined(rows, "edge_gate_threshold_bps"),
                "decision_avg_realized_slippage_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("realized_slippage_bps") not in (None, "")],
                    "realized_slippage_bps",
                    weight_key="fill_notional",
                ),
                "decision_avg_realized_edge_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("realized_edge_bps") not in (None, "")],
                    "realized_edge_bps",
                    weight_key="fill_notional",
                ),
                "decision_avg_fill_delay_seconds": _weighted_avg_defined(
                    [item for item in rows if item.get("first_fill_delay_seconds") not in (None, "")],
                    "first_fill_delay_seconds",
                    weight_key="fill_notional",
                ),
                "decision_avg_outcome_5d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_5d_bps") not in (None, "")],
                    "outcome_5d_bps",
                ),
                "decision_avg_outcome_20d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_20d_bps") not in (None, "")],
                    "outcome_20d_bps",
                ),
                "decision_avg_outcome_60d_bps": _weighted_avg_defined(
                    [item for item in rows if item.get("outcome_60d_bps") not in (None, "")],
                    "outcome_60d_bps",
                ),
            }
        )
    out.sort(key=lambda item: (str(item.get("market") or ""), str(item.get("portfolio_id") or "")))
    return out


def _decision_summary_by_week(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped_decision_history: Dict[str, List[Dict[str, Any]]] = {}
    for item in list(rows or []):
        week_key = str(item.get("week_label") or "").strip()
        if week_key:
            grouped_decision_history.setdefault(week_key, []).append(dict(item))
    decision_weekly_map: Dict[str, Dict[str, Any]] = {}
    for week_key, week_items in grouped_decision_history.items():
        summary_rows = _build_weekly_decision_evidence_summary_rows(week_items)
        if summary_rows:
            decision_weekly_map[week_key] = dict(summary_rows[0])
    return decision_weekly_map


def _build_weekly_decision_evidence_history_overview(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
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
        history_rows = storage.get_recent_investment_weekly_decision_evidence_history(
            market,
            portfolio_id=portfolio_id,
            limit=max(20, int(limit) * 50),
        )
        if not history_rows:
            continue
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for item in list(history_rows or []):
            week_key = str(item.get("week_label") or "").strip()
            if week_key:
                grouped.setdefault(week_key, []).append(dict(item))
        weekly_rows: List[Dict[str, Any]] = []
        for week_key, week_items in grouped.items():
            summary_rows = _build_weekly_decision_evidence_summary_rows(week_items)
            if not summary_rows:
                continue
            summary_row = dict(summary_rows[0])
            summary_row["week_label"] = week_key
            summary_row["week_start"] = str((week_items[0] or {}).get("week_start") or "")
            weekly_rows.append(summary_row)
        weekly_rows.sort(
            key=lambda item: (
                str(item.get("week_start") or ""),
                str(item.get("week_label") or ""),
            ),
            reverse=True,
        )
        tracked_rows = weekly_rows[: max(2, int(limit))]
        if not tracked_rows:
            continue
        latest = dict(tracked_rows[0] or {})
        baseline = dict(tracked_rows[-1] or latest)
        liquidity_bucket_chain = " -> ".join(
            f"{str(item.get('week_label') or '')}:{str(item.get('decision_primary_liquidity_bucket') or '-')}"
            for item in reversed(tracked_rows)
        )
        realized_slippage_delta = float(latest.get("decision_avg_realized_slippage_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
        )
        realized_edge_delta = float(latest.get("decision_avg_realized_edge_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_realized_edge_bps", 0.0) or 0.0
        )
        outcome_20d_delta = float(latest.get("decision_avg_outcome_20d_bps", 0.0) or 0.0) - float(
            baseline.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
        )
        fill_delay_delta = float(_safe_float(latest.get("decision_avg_fill_delay_seconds"), 0.0)) - float(
            _safe_float(baseline.get("decision_avg_fill_delay_seconds"), 0.0)
        )
        blocked_edge_delta = float(latest.get("decision_blocked_edge_order_count", 0) or 0.0) - float(
            baseline.get("decision_blocked_edge_order_count", 0) or 0.0
        )
        blocked_market_rule_delta = float(
            latest.get("decision_blocked_market_rule_order_count", 0) or 0.0
        ) - float(baseline.get("decision_blocked_market_rule_order_count", 0) or 0.0)
        dynamic_adv_pct_delta = float(latest.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0) - float(
            baseline.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0
        )
        slice_count_delta = float(latest.get("decision_avg_slice_count", 0.0) or 0.0) - float(
            baseline.get("decision_avg_slice_count", 0.0) or 0.0
        )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "weeks_tracked": int(len(tracked_rows)),
                "latest_week_label": str(latest.get("week_label") or ""),
                "baseline_week_label": str(baseline.get("week_label") or ""),
                "latest_primary_liquidity_bucket": str(latest.get("decision_primary_liquidity_bucket") or ""),
                "liquidity_bucket_chain": liquidity_bucket_chain,
                "latest_decision_evidence_row_count": int(latest.get("decision_evidence_row_count", 0) or 0),
                "latest_blocked_edge_order_count": int(latest.get("decision_blocked_edge_order_count", 0) or 0),
                "latest_blocked_market_rule_order_count": int(
                    latest.get("decision_blocked_market_rule_order_count", 0) or 0
                ),
                "latest_decision_avg_expected_edge_bps": float(
                    latest.get("decision_avg_expected_edge_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_expected_edge_bps": float(
                    baseline.get("decision_avg_expected_edge_bps", 0.0) or 0.0
                ),
                "latest_decision_avg_realized_slippage_bps": float(
                    latest.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_realized_slippage_bps": float(
                    baseline.get("decision_avg_realized_slippage_bps", 0.0) or 0.0
                ),
                "decision_avg_realized_slippage_bps_delta": float(realized_slippage_delta),
                "decision_slippage_trend": _weekly_tuning_history_trend_label(
                    realized_slippage_delta,
                    threshold=3.0,
                    improving_if_negative=True,
                ),
                "latest_decision_avg_realized_edge_bps": float(
                    latest.get("decision_avg_realized_edge_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_realized_edge_bps": float(
                    baseline.get("decision_avg_realized_edge_bps", 0.0) or 0.0
                ),
                "decision_avg_realized_edge_bps_delta": float(realized_edge_delta),
                "decision_realized_edge_trend": _weekly_tuning_history_trend_label(
                    realized_edge_delta,
                    threshold=10.0,
                ),
                "latest_decision_avg_outcome_20d_bps": float(
                    latest.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
                ),
                "baseline_decision_avg_outcome_20d_bps": float(
                    baseline.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
                ),
                "decision_avg_outcome_20d_bps_delta": float(outcome_20d_delta),
                "decision_outcome_20d_trend": _weekly_tuning_history_trend_label(
                    outcome_20d_delta,
                    threshold=25.0,
                ),
                "latest_decision_avg_fill_delay_seconds": float(
                    latest.get("decision_avg_fill_delay_seconds", 0.0) or 0.0
                ),
                "baseline_decision_avg_fill_delay_seconds": float(
                    baseline.get("decision_avg_fill_delay_seconds", 0.0) or 0.0
                ),
                "decision_avg_fill_delay_seconds_delta": float(fill_delay_delta),
                "decision_fill_delay_trend": _weekly_tuning_history_trend_label(
                    fill_delay_delta,
                    threshold=30.0,
                    improving_if_negative=True,
                ),
                "decision_blocked_edge_order_count_delta": float(blocked_edge_delta),
                "decision_blocked_edge_trend": _weekly_tuning_history_trend_label(
                    blocked_edge_delta,
                    threshold=1.0,
                    improving_if_negative=True,
                ),
                "decision_blocked_market_rule_order_count_delta": float(blocked_market_rule_delta),
                "decision_market_rule_block_trend": _weekly_tuning_history_trend_label(
                    blocked_market_rule_delta,
                    threshold=1.0,
                    improving_if_negative=True,
                ),
                "decision_avg_dynamic_order_adv_pct_delta": float(dynamic_adv_pct_delta),
                "decision_avg_slice_count_delta": float(slice_count_delta),
            }
        )
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _recent_decision_history_rows(
    storage: Storage,
    market: str,
    portfolio_id: str,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    history_rows = storage.get_recent_investment_weekly_decision_evidence_history(
        market,
        portfolio_id=portfolio_id,
        limit=max(20, int(limit) * 50),
    )
    if not history_rows:
        return []
    weekly_order: List[str] = []
    for item in list(history_rows or []):
        week_key = str(item.get("week_label") or "").strip()
        if week_key and week_key not in weekly_order:
            weekly_order.append(week_key)
    allowed_weeks = set(weekly_order[: max(2, int(limit))])
    return [dict(item) for item in list(history_rows or []) if str(item.get("week_label") or "").strip() in allowed_weeks]


def _market_portfolio_keys(rows: List[Dict[str, Any]] | None) -> List[tuple[str, str]]:
    keys = {
        (resolve_market_code(str(raw.get("market") or "")), str(raw.get("portfolio_id") or "").strip())
        for raw in list(rows or [])
        if resolve_market_code(str(raw.get("market") or "")) and str(raw.get("portfolio_id") or "").strip()
    }
    return sorted(keys)


def _build_weekly_edge_calibration_row(
    market: str,
    portfolio_id: str,
    history_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    filled = [
        row for row in history_rows
        if str(row.get("decision_status") or "").strip().upper() == "FILLED"
    ]
    blocked_edge = [
        row for row in history_rows
        if int(row.get("blocked_edge_order_count", 0) or 0) > 0
        or str(row.get("decision_status") or "").strip().upper() == "BLOCKED_EDGE"
    ]
    blocked_market_rule = [
        row for row in history_rows
        if int(row.get("blocked_market_rule_order_count", 0) or 0) > 0
    ]
    filled_outcome_20d = _avg_defined(
        [row.get("outcome_20d_bps") for row in filled if row.get("outcome_20d_bps") not in (None, "")]
    )
    blocked_edge_outcome_20d = _avg_defined(
        [row.get("outcome_20d_bps") for row in blocked_edge if row.get("outcome_20d_bps") not in (None, "")]
    )
    blocked_market_rule_outcome_20d = _avg_defined(
        [row.get("outcome_20d_bps") for row in blocked_market_rule if row.get("outcome_20d_bps") not in (None, "")]
    )
    edge_gap = None
    if filled_outcome_20d is not None and blocked_edge_outcome_20d is not None:
        edge_gap = float(blocked_edge_outcome_20d - filled_outcome_20d)
    market_rule_gap = None
    if filled_outcome_20d is not None and blocked_market_rule_outcome_20d is not None:
        market_rule_gap = float(blocked_market_rule_outcome_20d - filled_outcome_20d)

    edge_quality = "OBSERVE"
    if edge_gap is not None:
        if edge_gap <= -25.0:
            edge_quality = "GATE_DISCIPLINE_GOOD"
        elif edge_gap >= 25.0:
            edge_quality = "GATE_TOO_TIGHT"
        else:
            edge_quality = "GATE_MIXED"
    market_rule_quality = "OBSERVE"
    if market_rule_gap is not None:
        if market_rule_gap <= -25.0:
            market_rule_quality = "RULE_FILTER_GOOD"
        elif market_rule_gap >= 25.0:
            market_rule_quality = "RULE_FILTER_TOO_TIGHT"
        else:
            market_rule_quality = "RULE_FILTER_MIXED"

    note = "继续观察 edge 与市场规则阻断的事后表现。"
    if edge_quality == "GATE_DISCIPLINE_GOOD":
        note = "被 edge gate 挡掉的单事后 outcome 明显弱于成交单，当前 gate 纪律有效。"
    elif edge_quality == "GATE_TOO_TIGHT":
        note = "被 edge gate 挡掉的单事后并不差，当前 edge floor/buffer 可能偏紧。"
    elif market_rule_quality == "RULE_FILTER_TOO_TIGHT":
        note = "市场规则阻断样本事后并不弱，需复核 board lot / research-only 等限制是否过保守。"

    return {
        "portfolio_id": portfolio_id,
        "market": market,
        "weeks_tracked": int(
            len({str(item.get("week_label") or "") for item in history_rows if str(item.get("week_label") or "").strip()})
        ),
        "filled_sample_count": int(len(filled)),
        "blocked_edge_sample_count": int(len(blocked_edge)),
        "blocked_market_rule_sample_count": int(len(blocked_market_rule)),
        "filled_avg_outcome_20d_bps": filled_outcome_20d,
        "blocked_edge_avg_outcome_20d_bps": blocked_edge_outcome_20d,
        "blocked_market_rule_avg_outcome_20d_bps": blocked_market_rule_outcome_20d,
        "blocked_edge_vs_filled_outcome_20d_bps": edge_gap,
        "blocked_market_rule_vs_filled_outcome_20d_bps": market_rule_gap,
        "edge_gate_quality": edge_quality,
        "market_rule_quality": market_rule_quality,
        "edge_calibration_note": note,
    }


def _build_weekly_slicing_calibration_bucket_rows(
    market: str,
    portfolio_id: str,
    history_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    bucket_groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in list(history_rows or []):
        bucket = str(item.get("dynamic_liquidity_bucket") or "").strip().upper()
        if bucket:
            bucket_groups.setdefault(bucket, []).append(dict(item))
    out: List[Dict[str, Any]] = []
    for bucket, bucket_rows in bucket_groups.items():
        filled_rows = [
            row for row in bucket_rows
            if str(row.get("decision_status") or "").strip().upper() == "FILLED"
        ]
        avg_adv_pct = _avg_defined(
            [row.get("dynamic_order_adv_pct") for row in bucket_rows if row.get("dynamic_order_adv_pct") not in (None, "")]
        )
        avg_slice_count = _avg_defined([row.get("slice_count") for row in bucket_rows if row.get("slice_count") not in (None, "")])
        avg_slippage = _avg_defined(
            [row.get("realized_slippage_bps") for row in filled_rows if row.get("realized_slippage_bps") not in (None, "")]
        )
        avg_fill_delay = _avg_defined(
            [row.get("first_fill_delay_seconds") for row in filled_rows if row.get("first_fill_delay_seconds") not in (None, "")]
        )
        avg_realized_edge = _avg_defined([row.get("realized_edge_bps") for row in filled_rows if row.get("realized_edge_bps") not in (None, "")])
        avg_outcome_20d = _avg_defined([row.get("outcome_20d_bps") for row in filled_rows if row.get("outcome_20d_bps") not in (None, "")])

        assessment = "BALANCED"
        note = "当前 bucket 的切片强度与成交质量大体匹配。"
        if (avg_slice_count or 0.0) >= 3.5 and (avg_slippage or 0.0) <= 8.0:
            assessment = "POSSIBLY_TOO_CONSERVATIVE"
            note = "切片次数偏多但滑点仍低，当前 bucket 可能过度保守。"
        elif (avg_slice_count or 0.0) <= 1.5 and (avg_slippage or 0.0) >= 18.0:
            assessment = "NEED_MORE_SLICING"
            note = "切片偏少且滑点偏高，当前 bucket 可能需要更积极拆单。"
        elif (avg_fill_delay or 0.0) >= 150.0 and (avg_slice_count or 0.0) >= 3.0:
            assessment = "DELAY_HEAVY"
            note = "成交等待偏长，当前切片节奏可能拖慢执行。"

        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "dynamic_liquidity_bucket": bucket,
                "sample_count": int(len(bucket_rows)),
                "filled_sample_count": int(len(filled_rows)),
                "avg_dynamic_order_adv_pct": avg_adv_pct,
                "avg_slice_count": avg_slice_count,
                "avg_realized_slippage_bps": avg_slippage,
                "avg_fill_delay_seconds": avg_fill_delay,
                "avg_realized_edge_bps": avg_realized_edge,
                "avg_outcome_20d_bps": avg_outcome_20d,
                "slicing_assessment": assessment,
                "slicing_calibration_note": note,
            }
        )
    return out


def _build_weekly_risk_calibration_row(
    storage: Storage,
    market: str,
    portfolio_id: str,
    *,
    limit: int = 6,
) -> Dict[str, Any] | None:
    tuning_rows = storage.get_recent_investment_weekly_tuning_history(
        market,
        portfolio_id=portfolio_id,
        limit=max(2, int(limit)),
    )
    if not tuning_rows:
        return None
    decision_history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
    decision_weekly_map = _decision_summary_by_week(decision_history_rows)

    latest = dict(tuning_rows[0] or {})
    baseline = dict(tuning_rows[-1] or latest)
    latest_details = dict(latest.get("details_json") or {})
    baseline_details = dict(baseline.get("details_json") or {})
    latest_decision = dict(decision_weekly_map.get(str(latest.get("week_label") or ""), {}) or {})
    baseline_decision = dict(decision_weekly_map.get(str(baseline.get("week_label") or ""), {}) or {})

    latest_budget = float(latest_details.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0)
    latest_throttle = float(latest_details.get("risk_throttle_weight_delta", 0.0) or 0.0)
    latest_recovery = float(latest_details.get("risk_recovery_weight_credit", 0.0) or 0.0)
    baseline_budget = float(baseline_details.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0)
    baseline_throttle = float(baseline_details.get("risk_throttle_weight_delta", 0.0) or 0.0)
    baseline_recovery = float(baseline_details.get("risk_recovery_weight_credit", 0.0) or 0.0)
    outcome_20d_delta = float(latest_decision.get("decision_avg_outcome_20d_bps", 0.0) or 0.0) - float(
        baseline_decision.get("decision_avg_outcome_20d_bps", 0.0) or 0.0
    )
    realized_edge_delta = float(latest_decision.get("decision_avg_realized_edge_bps", 0.0) or 0.0) - float(
        baseline_decision.get("decision_avg_realized_edge_bps", 0.0) or 0.0
    )
    component_scores = {
        "BUDGET": abs(latest_budget),
        "THROTTLE": abs(latest_throttle),
        "RECOVERY": abs(latest_recovery),
    }
    dominant_component = max(component_scores.items(), key=lambda item: (float(item[1] or 0.0), str(item[0] or "")))[0]
    calibration_target = "OBSERVE"
    note = "当前风险预算、throttle 与 recovery 还需要继续观察。"
    if dominant_component == "BUDGET" and latest_budget > baseline_budget and outcome_20d_delta < -25.0:
        calibration_target = "BUDGET_TOO_TIGHT"
        note = "最近收益拖累更像来自 market-profile budget 收紧，优先复核 net/gross exposure budget。"
    elif dominant_component == "THROTTLE" and latest_throttle > baseline_throttle and outcome_20d_delta < -25.0:
        calibration_target = "THROTTLE_TOO_TIGHT"
        note = "最近收益拖累更像来自 throttle 层，优先复核相关性/流动性/集中度 throttle。"
    elif latest_recovery > baseline_recovery and outcome_20d_delta > 25.0 and realized_edge_delta > 10.0:
        calibration_target = "RECOVERY_HELPING"
        note = "recovery 近期在改善收益恢复，可继续保持温和回补节奏。"

    return {
        "portfolio_id": portfolio_id,
        "market": market,
        "latest_week_label": str(latest.get("week_label") or ""),
        "baseline_week_label": str(baseline.get("week_label") or ""),
        "latest_budget_weight_delta": latest_budget,
        "baseline_budget_weight_delta": baseline_budget,
        "latest_throttle_weight_delta": latest_throttle,
        "baseline_throttle_weight_delta": baseline_throttle,
        "latest_recovery_weight_credit": latest_recovery,
        "baseline_recovery_weight_credit": baseline_recovery,
        "latest_dominant_throttle_layer": str(latest_details.get("risk_dominant_throttle_layer") or ""),
        "latest_dominant_throttle_layer_label": str(latest_details.get("risk_dominant_throttle_layer_label") or ""),
        "decision_avg_outcome_20d_bps_delta": float(outcome_20d_delta),
        "decision_avg_realized_edge_bps_delta": float(realized_edge_delta),
        "risk_calibration_target": calibration_target,
        "risk_calibration_note": note,
    }


def _build_weekly_edge_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
        if not history_rows:
            continue
        out.append(_build_weekly_edge_calibration_row(market, portfolio_id, history_rows))
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _build_weekly_slicing_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        history_rows = _recent_decision_history_rows(storage, market, portfolio_id, limit=limit)
        out.extend(_build_weekly_slicing_calibration_bucket_rows(market, portfolio_id, history_rows))
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("dynamic_liquidity_bucket") or ""),
        )
    )
    return out


def _build_weekly_risk_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for market, portfolio_id in _market_portfolio_keys(rows):
        row = _build_weekly_risk_calibration_row(
            storage,
            market,
            portfolio_id,
            limit=limit,
        )
        if row:
            out.append(row)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out


def _market_profile_patch_conflict(raw: Dict[str, Any]) -> tuple[bool, str]:
    row = dict(raw or {})
    action = str(row.get("market_profile_tuning_action") or "").strip().upper()
    risk_action = str(row.get("risk_feedback_action") or "").strip().upper()
    execution_action = str(row.get("execution_feedback_action") or "").strip().upper()
    strategy_delta = float(row.get("strategy_control_weight_delta", 0.0) or 0.0)
    risk_delta = float(row.get("risk_overlay_weight_delta", 0.0) or 0.0)
    if action == "REVIEW_EXECUTION_GATE" and execution_action == "TIGHTEN":
        return True, "执行反馈仍建议收紧，不宜现在下调 edge gate。"
    if action == "REVIEW_REGIME_PLAN" and risk_action == "TIGHTEN" and risk_delta >= max(0.04, strategy_delta - 0.01):
        return True, "风险 overlay 仍在主导压仓，先不要放松 regime/plan 参数。"
    return False, ""


def _build_market_profile_patch_readiness(
    db_path: Path,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    storage = Storage(str(db_path))
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        market = resolve_market_code(str(row.get("market") or ""))
        tuning_action = str(row.get("market_profile_tuning_action") or "").strip().upper()
        review_required = tuning_action in {"REVIEW_EXECUTION_GATE", "REVIEW_REGIME_PLAN"}
        if not portfolio_id or not market:
            continue
        history_rows = storage.get_recent_investment_market_profile_patch_history(
            market,
            portfolio_id=portfolio_id,
            limit=12,
        )
        history_rows = sorted(
            list(history_rows or []),
            key=lambda item: (str(item.get("week_start", "") or ""), str(item.get("ts", "") or "")),
            reverse=True,
        )
        same_action_weeks = 0
        for item in history_rows:
            if str(item.get("tuning_action") or "").strip().upper() != tuning_action:
                break
            same_action_weeks += 1
        action_chain = " -> ".join(
            f"{str(item.get('week_label', '') or '-')}:"
            f"{str(item.get('tuning_action', '') or '-')}"
            for item in reversed(history_rows[: max(1, same_action_weeks)])
        ) or "-"
        baseline_week = str(history_rows[same_action_weeks - 1].get("week_label") or "-") if same_action_weeks > 0 else "-"
        conflict_flag, conflict_reason = _market_profile_patch_conflict(row)
        ready_for_manual_apply = bool(review_required and same_action_weeks >= 2 and not conflict_flag)
        if not review_required:
            readiness_label = "NO_PATCH"
            readiness_summary = "当前还没有需要进入人工复核的 market profile patch。"
        elif conflict_flag:
            readiness_label = "BLOCKED_BY_CONFLICT"
            readiness_summary = (
                f"虽已连续 {max(1, same_action_weeks)} 周维持同方向，但当前与执行/风险反馈冲突；"
                f"{conflict_reason}"
            )
        elif ready_for_manual_apply:
            readiness_label = "READY_FOR_MANUAL_APPLY"
            readiness_summary = (
                f"已连续 {same_action_weeks} 周维持同方向，且当前无明显执行/风险冲突，"
                "可升级为人工应用候选。"
            )
        else:
            readiness_label = "OBSERVE_COHORT"
            readiness_summary = (
                f"当前仅连续 {max(1, same_action_weeks)} 周维持同方向，先继续观察到至少 2 周再决定是否人工应用。"
            )
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "adaptive_strategy_active_market_profile": str(row.get("adaptive_strategy_active_market_profile") or ""),
                "market_profile_tuning_action": tuning_action,
                "market_profile_tuning_target": str(row.get("market_profile_tuning_target") or ""),
                "market_profile_cohort_weeks": int(same_action_weeks),
                "market_profile_baseline_week": baseline_week,
                "market_profile_action_chain": action_chain,
                "market_profile_conflict_flag": int(conflict_flag),
                "market_profile_conflict_reason": conflict_reason,
                "market_profile_ready_for_manual_apply": int(ready_for_manual_apply),
                "market_profile_readiness_label": readiness_label,
                "market_profile_readiness_summary": readiness_summary,
            }
        )
    out.sort(
        key=lambda row: (
            0
            if int(row.get("market_profile_ready_for_manual_apply", 0) or 0) == 1
            else 1
            if str(row.get("market_profile_readiness_label") or "") == "BLOCKED_BY_CONFLICT"
            else 2,
            -int(row.get("market_profile_cohort_weeks", 0) or 0),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return out


def _portfolio_horizon_row_map(rows: List[Dict[str, Any]] | None) -> Dict[str, Dict[int, Dict[str, Any]]]:
    outcome_map: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for raw in list(rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not portfolio_id or horizon_days <= 0:
            continue
        outcome_map.setdefault(portfolio_id, {})[horizon_days] = dict(raw)
    return outcome_map


def _portfolio_feedback_kind_map(
    rows: List[Dict[str, Any]] | None,
) -> Dict[tuple[str, str], Dict[str, Any]]:
    return {
        (str(row.get("portfolio_id") or "").strip(), str(row.get("feedback_kind") or "").strip().lower()): dict(row)
        for row in list(rows or [])
        if str(row.get("portfolio_id") or "").strip() and str(row.get("feedback_kind") or "").strip()
    }


def _build_weekly_tuning_dataset_lookup_maps(
    *,
    decision_evidence_rows: List[Dict[str, Any]] | None = None,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
    outcome_spread_rows: List[Dict[str, Any]] | None = None,
    edge_realization_rows: List[Dict[str, Any]] | None = None,
    blocked_edge_rows: List[Dict[str, Any]] | None = None,
    risk_review_rows: List[Dict[str, Any]] | None = None,
    risk_feedback_rows: List[Dict[str, Any]] | None = None,
    execution_feedback_rows: List[Dict[str, Any]] | None = None,
    market_profile_tuning_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_rows: List[Dict[str, Any]] | None = None,
    feedback_automation_rows: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return {
        "strategy_context_map": _portfolio_row_map(strategy_context_rows),
        "attribution_map": _portfolio_row_map(attribution_rows),
        "decision_evidence_summary_map": _portfolio_row_map(
            _build_weekly_decision_evidence_summary_rows(list(decision_evidence_rows or []))
        ),
        "outcome_spread_map": _portfolio_horizon_row_map(outcome_spread_rows),
        "edge_realization_map": _portfolio_row_map(edge_realization_rows),
        "blocked_edge_map": _portfolio_row_map(blocked_edge_rows),
        "risk_review_map": _portfolio_row_map(risk_review_rows),
        "risk_feedback_map": _portfolio_row_map(risk_feedback_rows),
        "execution_feedback_map": _portfolio_row_map(execution_feedback_rows),
        "tuning_map": _portfolio_row_map(market_profile_tuning_rows),
        "calibration_map": _portfolio_row_map(feedback_calibration_rows),
        "automation_map": _portfolio_feedback_kind_map(feedback_automation_rows),
    }


def _build_weekly_tuning_dataset_row(
    summary: Dict[str, Any],
    *,
    lookup_maps: Dict[str, Any],
    week_label: str = "",
    window_start: str = "",
    window_end: str = "",
) -> Dict[str, Any]:
    portfolio_id = str(summary.get("portfolio_id") or "").strip()
    strategy_context = dict(dict(lookup_maps.get("strategy_context_map") or {}).get(portfolio_id) or {})
    attribution = dict(dict(lookup_maps.get("attribution_map") or {}).get(portfolio_id) or {})
    decision_evidence = dict(dict(lookup_maps.get("decision_evidence_summary_map") or {}).get(portfolio_id) or {})
    outcome_spreads = dict(dict(lookup_maps.get("outcome_spread_map") or {}).get(portfolio_id) or {})
    edge_realization = dict(dict(lookup_maps.get("edge_realization_map") or {}).get(portfolio_id) or {})
    blocked_edge = dict(dict(lookup_maps.get("blocked_edge_map") or {}).get(portfolio_id) or {})
    risk_review = dict(dict(lookup_maps.get("risk_review_map") or {}).get(portfolio_id) or {})
    risk_feedback = dict(dict(lookup_maps.get("risk_feedback_map") or {}).get(portfolio_id) or {})
    execution_feedback = dict(dict(lookup_maps.get("execution_feedback_map") or {}).get(portfolio_id) or {})
    tuning = dict(dict(lookup_maps.get("tuning_map") or {}).get(portfolio_id) or {})
    calibration = dict(dict(lookup_maps.get("calibration_map") or {}).get(portfolio_id) or {})
    automation_map = dict(lookup_maps.get("automation_map") or {})
    shadow_automation = dict(automation_map.get((portfolio_id, "shadow")) or {})
    risk_automation = dict(automation_map.get((portfolio_id, "risk")) or {})
    execution_automation = dict(automation_map.get((portfolio_id, "execution")) or {})
    return {
        "week_label": str(week_label or ""),
        "window_start": str(window_start or ""),
        "window_end": str(window_end or ""),
        "portfolio_id": portfolio_id,
        "market": str(summary.get("market") or ""),
        "weekly_return": float(summary.get("weekly_return", 0.0) or 0.0),
        "max_drawdown": float(summary.get("max_drawdown", 0.0) or 0.0),
        "turnover": float(summary.get("turnover", 0.0) or 0.0),
        "latest_equity": float(summary.get("latest_equity", 0.0) or 0.0),
        "adaptive_strategy_active_market_profile": str(
            strategy_context.get("adaptive_strategy_active_market_profile")
            or tuning.get("adaptive_strategy_active_market_profile")
            or ""
        ),
        "adaptive_strategy_market_profile_note": str(
            strategy_context.get("adaptive_strategy_market_profile_note")
            or tuning.get("adaptive_strategy_market_profile_note")
            or ""
        ),
        "strategy_effective_controls_applied": int(bool(summary.get("strategy_effective_controls_applied", False))),
        "strategy_effective_controls_note": str(
            strategy_context.get("strategy_effective_controls_note")
            or summary.get("strategy_effective_controls_note")
            or ""
        ),
        "execution_gate_summary": str(
            strategy_context.get("execution_gate_summary")
            or summary.get("execution_gate_summary")
            or ""
        ),
        "outcome_sample_count": int(calibration.get("outcome_sample_count", 0) or 0),
        "outcome_positive_rate": float(calibration.get("outcome_positive_rate", 0.0) or 0.0),
        "outcome_broken_rate": float(calibration.get("outcome_broken_rate", 0.0) or 0.0),
        "signal_quality_score": float(calibration.get("signal_quality_score", 0.0) or 0.0),
        "calibration_confidence": float(calibration.get("calibration_confidence", 0.0) or 0.0),
        "calibration_confidence_label": str(calibration.get("calibration_confidence_label") or ""),
        "latest_outcome_ts": str(calibration.get("latest_outcome_ts") or ""),
        "selection_scope_label": str(calibration.get("selection_scope_label") or ""),
        "selected_horizon_days": str(calibration.get("selected_horizon_days") or ""),
        "shadow_apply_mode": str(shadow_automation.get("calibration_apply_mode") or ""),
        "shadow_apply_mode_label": str(shadow_automation.get("calibration_apply_mode_label") or ""),
        "shadow_outcome_maturity_label": str(shadow_automation.get("outcome_maturity_label") or ""),
        "risk_feedback_action": str(risk_feedback.get("risk_feedback_action") or ""),
        "risk_feedback_confidence": float(risk_feedback.get("feedback_confidence", 0.0) or 0.0),
        "risk_feedback_confidence_label": str(risk_feedback.get("feedback_confidence_label") or ""),
        "risk_feedback_reason": str(risk_feedback.get("feedback_reason") or ""),
        "risk_apply_mode": str(risk_automation.get("calibration_apply_mode") or ""),
        "risk_apply_mode_label": str(risk_automation.get("calibration_apply_mode_label") or ""),
        "risk_outcome_maturity_label": str(risk_automation.get("outcome_maturity_label") or ""),
        "execution_feedback_action": str(execution_feedback.get("execution_feedback_action") or ""),
        "execution_feedback_confidence": float(execution_feedback.get("feedback_confidence", 0.0) or 0.0),
        "execution_feedback_confidence_label": str(execution_feedback.get("feedback_confidence_label") or ""),
        "execution_feedback_reason": str(execution_feedback.get("feedback_reason") or ""),
        "execution_apply_mode": str(execution_automation.get("calibration_apply_mode") or ""),
        "execution_apply_mode_label": str(execution_automation.get("calibration_apply_mode_label") or ""),
        "execution_outcome_maturity_label": str(execution_automation.get("outcome_maturity_label") or ""),
        "market_data_gate_status": str(execution_automation.get("market_data_gate_status") or ""),
        "market_data_gate_label": str(execution_automation.get("market_data_gate_label") or ""),
        "planned_execution_cost_total": float(attribution.get("planned_execution_cost_total", 0.0) or 0.0),
        "execution_cost_total": float(attribution.get("execution_cost_total", 0.0) or 0.0),
        "execution_cost_gap": float(attribution.get("execution_cost_gap", 0.0) or 0.0),
        "avg_expected_cost_bps": float(
            decision_evidence.get("decision_avg_expected_cost_bps", attribution.get("avg_expected_cost_bps", 0.0)) or 0.0
        ),
        "avg_actual_slippage_bps": float(
            decision_evidence.get("decision_avg_realized_slippage_bps", attribution.get("avg_actual_slippage_bps", 0.0)) or 0.0
        ),
        "avg_expected_edge_bps": float(
            decision_evidence.get("decision_avg_expected_edge_bps", edge_realization.get("avg_expected_edge_bps", 0.0)) or 0.0
        ),
        "avg_edge_gate_threshold_bps": float(
            decision_evidence.get("decision_avg_edge_gate_threshold_bps", edge_realization.get("avg_edge_gate_threshold_bps", 0.0)) or 0.0
        ),
        "avg_execution_capture_bps": float(edge_realization.get("avg_execution_capture_bps", 0.0) or 0.0),
        "avg_fill_delay_seconds": float(edge_realization.get("avg_fill_delay_seconds", 0.0) or 0.0),
        "median_fill_delay_seconds": float(edge_realization.get("median_fill_delay_seconds", 0.0) or 0.0),
        "matured_20d_avg_realized_edge_bps": float(
            decision_evidence.get("decision_avg_realized_edge_bps", edge_realization.get("matured_20d_avg_realized_edge_bps", 0.0)) or 0.0
        ),
        "decision_evidence_row_count": int(decision_evidence.get("decision_evidence_row_count", 0) or 0),
        "decision_blocked_market_rule_order_count": int(
            decision_evidence.get("decision_blocked_market_rule_order_count", 0) or 0
        ),
        "decision_blocked_edge_order_count": int(decision_evidence.get("decision_blocked_edge_order_count", 0) or 0),
        "decision_primary_liquidity_bucket": str(decision_evidence.get("decision_primary_liquidity_bucket") or ""),
        "decision_avg_dynamic_order_adv_pct": float(
            decision_evidence.get("decision_avg_dynamic_order_adv_pct", 0.0) or 0.0
        ),
        "decision_avg_slice_count": float(decision_evidence.get("decision_avg_slice_count", 0.0) or 0.0),
        "decision_avg_realized_edge_bps": float(decision_evidence.get("decision_avg_realized_edge_bps", 0.0) or 0.0),
        "decision_avg_outcome_5d_bps": float(decision_evidence.get("decision_avg_outcome_5d_bps", 0.0) or 0.0),
        "decision_avg_outcome_20d_bps": float(decision_evidence.get("decision_avg_outcome_20d_bps", 0.0) or 0.0),
        "decision_avg_outcome_60d_bps": float(decision_evidence.get("decision_avg_outcome_60d_bps", 0.0) or 0.0),
        "outcome_selected_spread_5d_bps": float(
            dict(outcome_spreads.get(5) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_selected_spread_20d_bps": float(
            dict(outcome_spreads.get(20) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_selected_spread_60d_bps": float(
            dict(outcome_spreads.get(60) or {}).get("selected_spread_vs_unselected_bps", 0.0) or 0.0
        ),
        "outcome_executed_vs_blocked_edge_spread_20d_bps": float(
            dict(outcome_spreads.get(20) or {}).get("executed_spread_vs_blocked_edge_bps", 0.0) or 0.0
        ),
        "dominant_execution_session_label": str(execution_feedback.get("dominant_execution_session_label") or ""),
        "dominant_execution_hotspot_symbol": str(execution_feedback.get("dominant_execution_hotspot_symbol") or ""),
        "execution_penalty_symbol_count": int(execution_feedback.get("execution_penalty_symbol_count", 0) or 0),
        "strategy_control_weight_delta": float(attribution.get("strategy_control_weight_delta", 0.0) or 0.0),
        "risk_overlay_weight_delta": float(attribution.get("risk_overlay_weight_delta", 0.0) or 0.0),
        "risk_market_profile_budget_weight_delta": float(
            attribution.get("risk_market_profile_budget_weight_delta", 0.0) or 0.0
        ),
        "risk_throttle_weight_delta": float(attribution.get("risk_throttle_weight_delta", 0.0) or 0.0),
        "risk_recovery_weight_credit": float(attribution.get("risk_recovery_weight_credit", 0.0) or 0.0),
        "risk_layered_split_text": str(attribution.get("risk_layered_split_text") or ""),
        "risk_dominant_throttle_layer": str(attribution.get("risk_dominant_throttle_layer") or ""),
        "risk_dominant_throttle_layer_label": str(attribution.get("risk_dominant_throttle_layer_label") or ""),
        "execution_gate_blocked_order_count": int(attribution.get("execution_gate_blocked_order_count", 0) or 0),
        "execution_gate_blocked_order_value": float(attribution.get("execution_gate_blocked_order_value", 0.0) or 0.0),
        "execution_gate_blocked_order_ratio": float(attribution.get("execution_gate_blocked_order_ratio", 0.0) or 0.0),
        "execution_gate_blocked_weight": float(attribution.get("execution_gate_blocked_weight", 0.0) or 0.0),
        "blocked_edge_parent_count": int(blocked_edge.get("blocked_edge_parent_count", 0) or 0),
        "blocked_edge_order_value": float(blocked_edge.get("blocked_edge_order_value", 0.0) or 0.0),
        "blocked_expected_edge_value": float(blocked_edge.get("blocked_expected_edge_value", 0.0) or 0.0),
        "blocked_required_gap_value": float(blocked_edge.get("blocked_required_gap_value", 0.0) or 0.0),
        "blocked_20d_avg_counterfactual_edge_bps": float(
            blocked_edge.get("matured_20d_avg_counterfactual_edge_bps", 0.0) or 0.0
        ),
        "feedback_control_driver": str(execution_feedback.get("feedback_control_driver") or ""),
        "feedback_control_driver_label": str(
            execution_feedback.get("feedback_control_driver_label")
            or risk_feedback.get("feedback_control_driver_label")
            or ""
        ),
        "control_split_text": str(attribution.get("control_split_text") or ""),
        "dominant_driver": str(attribution.get("dominant_driver") or ""),
        "dominant_risk_driver": str(risk_review.get("dominant_risk_driver") or ""),
        "risk_latest_market_profile_budget_tightening": float(
            risk_review.get("latest_market_profile_budget_tightening", 0.0) or 0.0
        ),
        "risk_latest_throttle_tightening": float(risk_review.get("latest_throttle_tightening", 0.0) or 0.0),
        "risk_latest_recovery_credit": float(risk_review.get("latest_recovery_credit", 0.0) or 0.0),
        "risk_latest_dominant_throttle_layer": str(risk_review.get("latest_dominant_throttle_layer") or ""),
        "risk_latest_dominant_throttle_layer_label": str(risk_review.get("latest_dominant_throttle_layer_label") or ""),
        "risk_diagnosis": str(risk_review.get("risk_diagnosis") or ""),
        "market_profile_tuning_target": str(tuning.get("market_profile_tuning_target") or ""),
        "market_profile_tuning_bias": str(tuning.get("market_profile_tuning_bias") or ""),
        "market_profile_tuning_action": str(tuning.get("market_profile_tuning_action") or ""),
        "market_profile_tuning_note": str(tuning.get("market_profile_tuning_note") or ""),
        "market_profile_ready_for_manual_apply": int(summary.get("market_profile_ready_for_manual_apply", 0) or 0),
        "market_profile_readiness_label": str(summary.get("market_profile_readiness_label") or ""),
        "market_profile_readiness_summary": str(summary.get("market_profile_readiness_summary") or ""),
        "market_profile_cohort_weeks": int(summary.get("market_profile_cohort_weeks", 0) or 0),
    }


def _build_weekly_tuning_dataset_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    decision_evidence_rows: List[Dict[str, Any]] | None = None,
    strategy_context_rows: List[Dict[str, Any]] | None = None,
    attribution_rows: List[Dict[str, Any]] | None = None,
    outcome_spread_rows: List[Dict[str, Any]] | None = None,
    edge_realization_rows: List[Dict[str, Any]] | None = None,
    blocked_edge_rows: List[Dict[str, Any]] | None = None,
    risk_review_rows: List[Dict[str, Any]] | None = None,
    risk_feedback_rows: List[Dict[str, Any]] | None = None,
    execution_feedback_rows: List[Dict[str, Any]] | None = None,
    market_profile_tuning_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_rows: List[Dict[str, Any]] | None = None,
    feedback_automation_rows: List[Dict[str, Any]] | None = None,
    week_label: str = "",
    window_start: str = "",
    window_end: str = "",
) -> List[Dict[str, Any]]:
    lookup_maps = _build_weekly_tuning_dataset_lookup_maps(
        decision_evidence_rows=decision_evidence_rows,
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_rows=blocked_edge_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_feedback_rows=execution_feedback_rows,
        market_profile_tuning_rows=market_profile_tuning_rows,
        feedback_calibration_rows=feedback_calibration_rows,
        feedback_automation_rows=feedback_automation_rows,
    )

    rows: List[Dict[str, Any]] = []
    for raw in list(summary_rows or []):
        summary = dict(raw or {})
        portfolio_id = str(summary.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        rows.append(
            _build_weekly_tuning_dataset_row(
                summary,
                lookup_maps=lookup_maps,
                week_label=week_label,
                window_start=window_start,
                window_end=window_end,
            )
        )
    rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    return rows


def _build_weekly_tuning_dataset_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    portfolio_count = int(len(rows))
    dominant_driver_counts = {"STRATEGY": 0, "RISK": 0, "EXECUTION": 0, "OTHER": 0}
    for row in list(rows or []):
        driver = str(row.get("dominant_driver") or "").strip().upper()
        if driver not in dominant_driver_counts:
            driver = "OTHER"
        dominant_driver_counts[driver] = int(dominant_driver_counts.get(driver, 0) or 0) + 1
    return {
        "portfolio_count": portfolio_count,
        "strategy_driver_count": int(dominant_driver_counts.get("STRATEGY", 0) or 0),
        "risk_driver_count": int(dominant_driver_counts.get("RISK", 0) or 0),
        "execution_driver_count": int(dominant_driver_counts.get("EXECUTION", 0) or 0),
        "market_profile_review_count": int(
            sum(
                1
                for row in list(rows or [])
                if str(row.get("market_profile_tuning_action") or "").startswith("REVIEW_")
            )
        ),
        "ready_for_manual_apply_count": int(
            sum(1 for row in list(rows or []) if int(row.get("market_profile_ready_for_manual_apply", 0) or 0) == 1)
        ),
        "execution_tighten_count": int(
            sum(1 for row in list(rows or []) if str(row.get("execution_feedback_action") or "") == "TIGHTEN")
        ),
        "risk_tighten_count": int(
            sum(1 for row in list(rows or []) if str(row.get("risk_feedback_action") or "") == "TIGHTEN")
        ),
        "avg_execution_cost_gap": float(_avg_defined([row.get("execution_cost_gap") for row in list(rows or [])]) or 0.0),
        "avg_execution_gate_blocked_weight": float(
            _avg_defined([row.get("execution_gate_blocked_weight") for row in list(rows or [])]) or 0.0
        ),
        "avg_outcome_sample_count": float(_avg_defined([row.get("outcome_sample_count") for row in list(rows or [])]) or 0.0),
        "avg_signal_quality_score": float(_avg_defined([row.get("signal_quality_score") for row in list(rows or [])]) or 0.0),
    }


def _build_weekly_portfolio_summary_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    *,
    trade_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    sector_rows: List[Dict[str, Any]],
    change_rows: List[Dict[str, Any]],
    run_source_fn: Callable[[Dict[str, Any]], str],
    mean_fn: Callable[[List[float]], float],
    max_drawdown_fn: Callable[[List[float]], float],
    top_holdings_fn: Callable[[List[Dict[str, Any]], int], str],
    top_sector_fn: Callable[[List[Dict[str, Any]], str, int], str],
    summarize_changes_fn: Callable[[List[Dict[str, Any]], str], str],
    holdings_limit: int = 5,
    sector_limit: int = 3,
) -> List[Dict[str, Any]]:
    summary_rows: List[Dict[str, Any]] = []
    for portfolio_id, rows in runs_by_portfolio.items():
        first_row = rows[0]
        last_row = rows[-1]
        perf_rows = [r for r in rows if run_source_fn(r) != "broker_sync"]
        perf_first_row = perf_rows[0] if perf_rows else first_row
        perf_last_row = perf_rows[-1] if perf_rows else last_row
        equity_path = [float(r.get("equity_after") or 0.0) for r in perf_rows if r.get("equity_after") is not None]
        start_equity = float(perf_first_row.get("equity_before") or perf_first_row.get("equity_after") or 0.0)
        latest_equity = float(perf_last_row.get("equity_after") or 0.0)
        weekly_return = ((latest_equity / start_equity) - 1.0) if start_equity > 0 else 0.0
        portfolio_trades = [row for row in trade_rows if str(row.get("portfolio_id") or "") == portfolio_id]
        gross_buy_value = sum(
            abs(float(row.get("trade_value") or 0.0))
            for row in portfolio_trades
            if str(row.get("action") or "").upper() == "BUY"
        )
        gross_sell_value = sum(
            abs(float(row.get("trade_value") or 0.0))
            for row in portfolio_trades
            if str(row.get("action") or "").upper() == "SELL"
        )
        holdings = latest_rows_by_portfolio.get(portfolio_id, [])
        summary_rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(last_row.get("market") or ""),
                "runs_in_window": int(len(rows)),
                "executed_rebalances": int(sum(1 for r in rows if int(r.get("executed") or 0) == 1)),
                "trade_count": int(len(portfolio_trades)),
                "buy_count": int(sum(1 for row in portfolio_trades if str(row.get("action") or "").upper() == "BUY")),
                "sell_count": int(sum(1 for row in portfolio_trades if str(row.get("action") or "").upper() == "SELL")),
                "gross_buy_value": float(gross_buy_value),
                "gross_sell_value": float(gross_sell_value),
                "net_trade_value": float(gross_buy_value - gross_sell_value),
                "start_equity": float(start_equity),
                "latest_equity": float(last_row.get("equity_after") or latest_equity),
                "weekly_return": float(weekly_return),
                "avg_equity": float(mean_fn(equity_path)),
                "max_drawdown": float(max_drawdown_fn(equity_path)),
                "turnover": float((gross_buy_value + gross_sell_value) / max(1.0, mean_fn(equity_path))),
                "cash_after": float(last_row.get("cash_after") or 0.0),
                "holdings_count": int(len(holdings)),
                "top_holdings": top_holdings_fn(holdings, holdings_limit),
                "top_sectors": top_sector_fn(sector_rows, portfolio_id, sector_limit),
                "holdings_change_summary": summarize_changes_fn(change_rows, portfolio_id),
                "broker_sync_runs": int(sum(1 for r in rows if run_source_fn(r) == "broker_sync")),
            }
        )
    summary_rows.sort(key=lambda row: float(row.get("weekly_return", 0.0) or 0.0), reverse=True)
    return summary_rows


def _apply_market_profile_tuning_context(
    summary_rows: List[Dict[str, Any]],
    strategy_context_rows: List[Dict[str, Any]],
    market_profile_tuning_rows: List[Dict[str, Any]],
    market_profile_patch_readiness_rows: List[Dict[str, Any]],
) -> None:
    tuning_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(market_profile_tuning_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }
    readiness_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(market_profile_patch_readiness_rows or [])
        if str(row.get("portfolio_id") or "").strip()
    }

    def _apply(row: Dict[str, Any]) -> None:
        portfolio_id = str(row.get("portfolio_id") or "")
        tuning = dict(tuning_map.get(portfolio_id, {}) or {})
        readiness = dict(readiness_map.get(portfolio_id, {}) or {})
        row["market_profile_tuning_target"] = str(tuning.get("market_profile_tuning_target", "") or "")
        row["market_profile_tuning_target_label"] = str(tuning.get("market_profile_tuning_target_label", "") or "")
        row["market_profile_tuning_bias"] = str(tuning.get("market_profile_tuning_bias", "") or "")
        row["market_profile_tuning_bias_label"] = str(tuning.get("market_profile_tuning_bias_label", "") or "")
        row["market_profile_tuning_action"] = str(tuning.get("market_profile_tuning_action", "") or "")
        row["market_profile_tuning_note"] = str(tuning.get("market_profile_tuning_note", "") or "")
        row["market_profile_tuning_summary"] = str(tuning.get("market_profile_tuning_summary", "") or "")
        row["market_profile_cohort_weeks"] = int(readiness.get("market_profile_cohort_weeks", 0) or 0)
        row["market_profile_baseline_week"] = str(readiness.get("market_profile_baseline_week", "") or "")
        row["market_profile_action_chain"] = str(readiness.get("market_profile_action_chain", "") or "")
        row["market_profile_conflict_flag"] = int(readiness.get("market_profile_conflict_flag", 0) or 0)
        row["market_profile_conflict_reason"] = str(readiness.get("market_profile_conflict_reason", "") or "")
        row["market_profile_ready_for_manual_apply"] = int(readiness.get("market_profile_ready_for_manual_apply", 0) or 0)
        row["market_profile_readiness_label"] = str(readiness.get("market_profile_readiness_label", "") or "")
        row["market_profile_readiness_summary"] = str(readiness.get("market_profile_readiness_summary", "") or "")

    for row in list(summary_rows or []):
        _apply(row)
    for row in list(strategy_context_rows or []):
        _apply(row)


def _risk_overlay_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if str(row.get("source_kind") or "").strip():
        stress_scenarios = _parse_json_dict(row.get("stress_scenarios_json"))
        details = _parse_json_dict(row.get("details"))
        risk_details = dict(details.get("risk_overlay") or {})
        normalized = {
            "dynamic_scale": row.get("dynamic_scale"),
            "dynamic_net_exposure": row.get("dynamic_net_exposure"),
            "dynamic_gross_exposure": row.get("dynamic_gross_exposure"),
            "dynamic_short_exposure": row.get("dynamic_short_exposure"),
            "applied_net_exposure": row.get("applied_net_exposure"),
            "applied_gross_exposure": row.get("applied_gross_exposure"),
            "avg_pair_correlation": row.get("avg_pair_correlation"),
            "final_avg_pair_correlation": row.get("avg_pair_correlation"),
            "max_pair_correlation": row.get("max_pair_correlation"),
            "final_max_pair_correlation": row.get("max_pair_correlation"),
            "top_sector_share": row.get("top_sector_share"),
            "stress_worst_loss": row.get("stress_worst_loss"),
            "final_stress_worst_loss": row.get("stress_worst_loss"),
            "stress_worst_scenario": row.get("stress_worst_scenario"),
            "final_stress_worst_scenario": row.get("stress_worst_scenario"),
            "stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "final_stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "notes": _parse_json_list(row.get("notes_json")),
            "correlation_reduced_symbols": _parse_json_list(row.get("correlation_reduced_symbols_json")),
            "stress_scenarios": stress_scenarios,
            "final_stress_scenarios": stress_scenarios,
        }
        for key, value in risk_details.items():
            if key not in normalized or normalized.get(key) in (None, "", [], {}):
                normalized[key] = value
        return normalized
    details = _parse_json_dict(row.get("details"))
    risk = dict(details.get("risk_overlay") or {})
    if not risk:
        summary = _parse_json_dict(details.get("summary"))
        if summary:
            risk = {
                "dynamic_scale": summary.get("risk_dynamic_scale"),
                "dynamic_net_exposure": summary.get("risk_dynamic_net_exposure"),
                "dynamic_gross_exposure": summary.get("risk_dynamic_gross_exposure"),
                "dynamic_short_exposure": summary.get("risk_dynamic_short_exposure"),
                "avg_pair_correlation": summary.get("risk_avg_pair_correlation"),
                "max_pair_correlation": summary.get("risk_max_pair_correlation"),
                "stress_worst_loss": summary.get("risk_stress_worst_loss"),
                "stress_worst_scenario_label": summary.get("risk_stress_worst_scenario_label"),
                "top_sector_share": summary.get("risk_top_sector_share"),
                "notes": summary.get("risk_notes"),
                "correlation_reduced_symbols": summary.get("risk_correlation_reduced_symbols"),
            }
    return risk


def _latest_risk_overlay(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    latest: Dict[str, Any] = {}
    latest_ts = ""
    for row in rows:
        risk = _risk_overlay_from_history_row(row)
        ts = str(row.get("ts") or "")
        if not risk or ts < latest_ts:
            continue
        latest = risk
        latest_ts = ts
    return latest


def _risk_driver_and_diagnosis(row: Dict[str, Any]) -> tuple[str, str]:
    avg_corr = float(row.get("latest_avg_pair_correlation", 0.0) or 0.0)
    worst_loss = float(row.get("latest_stress_worst_loss", 0.0) or 0.0)
    dynamic_net = float(row.get("latest_dynamic_net_exposure", 0.0) or 0.0)
    dynamic_gross = float(row.get("latest_dynamic_gross_exposure", 0.0) or 0.0)
    top_sector_share = float(row.get("latest_top_sector_share", 0.0) or 0.0)
    market_budget_tightening = float(row.get("latest_market_profile_budget_tightening", 0.0) or 0.0)
    throttle_tightening = float(row.get("latest_throttle_tightening", 0.0) or 0.0)
    recovery_credit = float(row.get("latest_recovery_credit", 0.0) or 0.0)
    throttle_layer = str(row.get("latest_dominant_throttle_layer", "") or "").strip().upper()
    throttle_layer_label = str(row.get("latest_dominant_throttle_layer_label", "") or "").strip()
    if market_budget_tightening >= max(throttle_tightening, 0.03):
        return "MARKET_PROFILE_BUDGET", "当前市场档案先收紧了基础风险预算，优先复核 market-profile exposure budget 是否仍匹配这类市场。"
    if throttle_layer:
        diagnosis = f"当前主导风险 throttle 为 {throttle_layer_label or throttle_layer}，优先复核这一层的风险阈值与持仓结构。"
        if recovery_credit > 1e-9:
            diagnosis += " 组合已经出现部分 recovery，但还未完全释放预算。"
        return throttle_layer, diagnosis
    if avg_corr >= 0.62 or top_sector_share >= 0.45:
        return "CORRELATION", "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。"
    if worst_loss >= 0.085:
        return "STRESS", "最差 stress 场景压力偏大，优先收缩净/总敞口并复盘高波动标的。"
    if dynamic_net <= 0.70 or dynamic_gross <= 0.75:
        return "EXPOSURE_BUDGET", "组合风险预算仍偏紧，优先提升流动性与数据质量，再争取释放仓位。"
    return "NORMAL", "当前组合风险覆盖整体平稳，可以继续观察信号质量与资金利用率。"
