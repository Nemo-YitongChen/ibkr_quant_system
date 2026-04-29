from __future__ import annotations

import json

from typing import Any, Callable, Dict, List

from ..common.markets import resolve_market_code
from .review_weekly_common_support import (
    _apply_outcome_calibration,
    _avg_defined,
    _candidate_snapshot_stage_priority,
    _clamp,
    _feedback_calibration_support,
    _feedback_confidence,
    _feedback_confidence_label,
    _feedback_control_driver_context,
    _is_selected_snapshot_stage,
    _median,
    _parse_json_dict,
    _preferred_snapshot_stages_for_order,
    _safe_float,
    _safe_int,
    _seconds_between,
)


_SESSION_LABELS = {
    "OPEN": "开盘",
    "MIDDAY": "午盘",
    "CLOSE": "尾盘",
    "UNKNOWN": "未知时段",
}


def _market_from_portfolio_or_symbol(portfolio_id: str, symbol: str = "") -> str:
    text = str(portfolio_id or "").strip().upper()
    if ":" in text:
        return resolve_market_code(text.split(":", 1)[0])
    symbol_text = str(symbol or "").strip().upper()
    if symbol_text.endswith(".HK"):
        return "HK"
    if symbol_text.endswith(".AX"):
        return "ASX"
    if symbol_text.endswith(".DE"):
        return "XETRA"
    if symbol_text.endswith(".SS") or symbol_text.endswith(".SZ"):
        return "CN"
    return "US" if symbol_text else ""

def _build_execution_hotspot_penalties(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for symbol, symbol_rows in grouped.items():
        symbol_rows.sort(
            key=lambda item: (
                -float(item.get("pressure_score", 0.0) or 0.0),
                -float(item.get("avg_actual_slippage_bps", 0.0) or 0.0),
                str(item.get("session_label") or ""),
            )
        )
        hotspot_count = int(len(symbol_rows))
        investigate_count = int(
            sum(1 for row in symbol_rows if str(row.get("hotspot_action") or "").upper() == "INVESTIGATE_EXECUTION")
        )
        avg_pressure = _avg_defined([row.get("pressure_score") for row in symbol_rows]) or 0.0
        avg_expected_bps = _avg_defined([row.get("avg_expected_cost_bps") for row in symbol_rows]) or 0.0
        avg_actual_bps = _avg_defined([row.get("avg_actual_slippage_bps") for row in symbol_rows]) or 0.0
        avg_bps_deviation = max(0.0, avg_actual_bps - avg_expected_bps)
        session_labels = sorted(
            {
                str(row.get("session_label") or "").strip()
                for row in symbol_rows
                if str(row.get("session_label") or "").strip()
            }
        )
        if investigate_count <= 0 and hotspot_count < 2 and avg_bps_deviation < 2.0:
            continue

        expected_cost_bps_add = _clamp(2.0 + avg_bps_deviation * 0.60 + max(0, hotspot_count - 1) * 1.50, 2.0, 18.0)
        slippage_proxy_bps_add = _clamp(1.5 + avg_bps_deviation * 0.80 + investigate_count * 1.50, 1.5, 20.0)
        execution_penalty = _clamp(0.02 + avg_pressure * 0.08 + investigate_count * 0.01, 0.02, 0.12)
        score_penalty = _clamp(execution_penalty * 0.35, 0.0, 0.05)
        out.append(
            {
                "symbol": symbol,
                "hotspot_count": hotspot_count,
                "investigate_count": investigate_count,
                "session_count": int(len(session_labels)),
                "session_labels": ",".join(session_labels[:6]),
                "avg_pressure": round(float(avg_pressure), 6),
                "avg_expected_cost_bps": round(float(avg_expected_bps), 6),
                "avg_actual_slippage_bps": round(float(avg_actual_bps), 6),
                "avg_bps_deviation": round(float(avg_bps_deviation), 6),
                "score_penalty": round(float(score_penalty), 6),
                "execution_penalty": round(float(execution_penalty), 6),
                "expected_cost_bps_add": round(float(expected_cost_bps_add), 6),
                "slippage_proxy_bps_add": round(float(slippage_proxy_bps_add), 6),
                "reason": "repeat_execution_hotspot",
            }
        )
    out.sort(
        key=lambda row: (
            -int(row.get("investigate_count", 0) or 0),
            -float(row.get("execution_penalty", 0.0) or 0.0),
            -float(row.get("expected_cost_bps_add", 0.0) or 0.0),
            str(row.get("symbol") or ""),
        )
    )
    return out

def _execution_session_profile_from_order(row: Dict[str, Any]) -> Dict[str, str]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}
    session_bucket = str(
        details.get("session_bucket")
        or plan_row.get("session_bucket")
        or row.get("session_bucket")
        or ""
    ).strip().upper()
    if session_bucket not in {"OPEN", "MIDDAY", "CLOSE"}:
        session_bucket = "UNKNOWN"
    session_label = str(
        details.get("session_label")
        or plan_row.get("session_label")
        or _SESSION_LABELS.get(session_bucket, "未知时段")
    ).strip() or _SESSION_LABELS.get(session_bucket, "未知时段")
    execution_style = str(
        details.get("execution_style")
        or plan_row.get("execution_style")
        or row.get("execution_style")
        or ""
    ).strip()
    return {
        "session_bucket": session_bucket,
        "session_label": session_label,
        "execution_style": execution_style,
    }

def _planned_cost_metrics_from_order(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = 0.0) -> Any:
        direct = details.get(key)
        if direct not in (None, ""):
            return direct
        nested = plan_row.get(key)
        if nested not in (None, ""):
            return nested
        raw = row.get(key)
        if raw not in (None, ""):
            return raw
        return default

    order_value = abs(float(_pick("order_value", row.get("order_value") or 0.0) or 0.0))
    spread_bps = float(_pick("spread_proxy_bps", 0.0) or 0.0)
    slippage_bps = float(_pick("slippage_proxy_bps", 0.0) or 0.0)
    commission_bps = float(_pick("commission_proxy_bps", 0.0) or 0.0)
    expected_cost_bps = float(_pick("expected_cost_bps", spread_bps + slippage_bps + commission_bps) or 0.0)
    expected_spread_cost = details.get("expected_spread_cost")
    expected_slippage_cost = details.get("expected_slippage_cost")
    expected_commission_cost = details.get("expected_commission_cost")
    expected_cost_value = details.get("expected_cost_value")
    return {
        "order_value": float(order_value),
        "expected_cost_bps": float(expected_cost_bps),
        "expected_spread_cost": float(
            expected_spread_cost if expected_spread_cost not in (None, "") else order_value * spread_bps / 10000.0
        ),
        "expected_slippage_cost": float(
            expected_slippage_cost if expected_slippage_cost not in (None, "") else order_value * slippage_bps / 10000.0
        ),
        "expected_commission_cost": float(
            expected_commission_cost if expected_commission_cost not in (None, "") else order_value * commission_bps / 10000.0
        ),
        "expected_cost_value": float(
            expected_cost_value if expected_cost_value not in (None, "") else order_value * expected_cost_bps / 10000.0
        ),
        "execution_style": str(_pick("execution_style", "") or ""),
    }

def _build_planned_execution_cost_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        submitted_like = int(row.get("broker_order_id") or 0) > 0 or status in {
            "CREATED",
            "SUBMITTED",
            "PRESUBMITTED",
            "FILLED",
            "PARTIAL",
            "PARTIALLY_FILLED",
        } or status.startswith("ERROR_")
        metrics = _planned_cost_metrics_from_order(row)
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_spread_cost": 0.0,
                "all_expected_slippage_cost": 0.0,
                "all_expected_commission_cost": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_spread_cost": 0.0,
                "submitted_expected_slippage_cost": 0.0,
                "submitted_expected_commission_cost": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "_all_style_counts": {},
                "_submitted_style_counts": {},
            },
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_spread_cost"] = float(bucket["all_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
        bucket["all_expected_slippage_cost"] = float(bucket["all_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
        bucket["all_expected_commission_cost"] = float(bucket["all_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(metrics.get("execution_style") or "")
        if style:
            style_counts = dict(bucket.get("_all_style_counts") or {})
            style_counts[style] = int(style_counts.get(style, 0)) + 1
            bucket["_all_style_counts"] = style_counts
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_spread_cost"] = float(bucket["submitted_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
            bucket["submitted_expected_slippage_cost"] = float(bucket["submitted_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
            bucket["submitted_expected_commission_cost"] = float(bucket["submitted_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                submitted_style_counts = dict(bucket.get("_submitted_style_counts") or {})
                submitted_style_counts[style] = int(submitted_style_counts.get(style, 0)) + 1
                bucket["_submitted_style_counts"] = submitted_style_counts

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        basis_label = "submitted_orders" if use_submitted else "planned_orders"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        style_counts = dict(bucket.get("_submitted_style_counts") or {}) if use_submitted else dict(bucket.get("_all_style_counts") or {})
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "planned_cost_basis": basis_label,
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "planned_order_value": float(value),
                "planned_spread_cost_total": float(bucket.get(f"{basis_prefix}_expected_spread_cost", 0.0) or 0.0),
                "planned_slippage_cost_total": float(bucket.get(f"{basis_prefix}_expected_slippage_cost", 0.0) or 0.0),
                "planned_commission_cost_total": float(bucket.get(f"{basis_prefix}_expected_commission_cost", 0.0) or 0.0),
                "planned_execution_cost_total": float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0),
                "avg_expected_cost_bps": float(numerator / value) if value > 0.0 else None,
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out

def _is_execution_gate_status(status: str) -> bool:
    normalized = str(status or "").strip().upper()
    return normalized.startswith("BLOCKED") or normalized in {"DEFERRED_RISK_ALERT", "REVIEW_REQUIRED"}

def _build_execution_gate_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        order_value = abs(float(row.get("order_value") or 0.0))
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
                "execution_order_count": 0,
                "execution_order_value": 0.0,
                "blocked_order_count": 0,
                "blocked_order_value": 0.0,
            },
        )
        bucket["execution_order_count"] = int(bucket["execution_order_count"]) + 1
        bucket["execution_order_value"] = float(bucket["execution_order_value"]) + float(order_value)
        if not _is_execution_gate_status(str(row.get("status") or "")):
            continue
        bucket["blocked_order_count"] = int(bucket["blocked_order_count"]) + 1
        bucket["blocked_order_value"] = float(bucket["blocked_order_value"]) + float(order_value)

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        total_count = int(bucket.get("execution_order_count", 0) or 0)
        total_value = float(bucket.get("execution_order_value", 0.0) or 0.0)
        blocked_count = int(bucket.get("blocked_order_count", 0) or 0)
        blocked_value = float(bucket.get("blocked_order_value", 0.0) or 0.0)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "execution_order_count": total_count,
                "execution_order_value": total_value,
                "blocked_order_count": blocked_count,
                "blocked_order_value": blocked_value,
                "blocked_order_ratio": float(blocked_count / total_count) if total_count > 0 else 0.0,
                "blocked_order_value_ratio": float(blocked_value / total_value) if total_value > 0.0 else 0.0,
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out

def _build_execution_effect_rows(
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in fill_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        market = _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))
        bucket = grouped.setdefault(
            portfolio_id,
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
            },
        )
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + fill_notional
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["commission_total"] = float(bucket["commission_total"]) + commission
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + fill_notional * slip / 10000.0

    out: List[Dict[str, Any]] = []
    for portfolio_id, bucket in grouped.items():
        slippage_samples = [float(v) for v in list(bucket.pop("_slippage_samples", []) or [])]
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(bucket.get("market") or ""),
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "commission_total": float(bucket.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "execution_cost_total": float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "avg_actual_slippage_bps": _avg_defined(slippage_samples),
            }
        )
    out.sort(key=lambda row: str(row.get("portfolio_id") or ""))
    return out

def _build_execution_session_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    order_meta_by_broker: Dict[int, Dict[str, Any]] = {}
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}

    def _bucket(portfolio_id: str, market: str, session_bucket: str, session_label: str) -> Dict[str, Any]:
        return grouped.setdefault(
            (portfolio_id, session_bucket),
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "session_bucket": session_bucket,
                "session_label": session_label,
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_spread_cost": 0.0,
                "all_expected_slippage_cost": 0.0,
                "all_expected_commission_cost": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_spread_cost": 0.0,
                "submitted_expected_slippage_cost": 0.0,
                "submitted_expected_commission_cost": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
                "_slippage_dev_samples": [],
                "_style_counts_all": {},
                "_style_counts_submitted": {},
            },
        )

    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        broker_order_id = int(row.get("broker_order_id") or 0)
        if broker_order_id > 0:
            order_meta_by_broker[broker_order_id] = dict(row)
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        session = _execution_session_profile_from_order(row)
        metrics = _planned_cost_metrics_from_order(row)
        bucket = _bucket(
            portfolio_id,
            str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
            session["session_bucket"],
            session["session_label"],
        )
        submitted_like = (
            broker_order_id > 0
            or status in {"CREATED", "SUBMITTED", "PRESUBMITTED", "FILLED", "PARTIAL", "PARTIALLY_FILLED"}
            or status.startswith("ERROR_")
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_spread_cost"] = float(bucket["all_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
        bucket["all_expected_slippage_cost"] = float(bucket["all_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
        bucket["all_expected_commission_cost"] = float(bucket["all_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(session.get("execution_style") or metrics.get("execution_style") or "").strip()
        if style:
            style_counts_all = dict(bucket.get("_style_counts_all") or {})
            style_counts_all[style] = int(style_counts_all.get(style, 0)) + 1
            bucket["_style_counts_all"] = style_counts_all
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_spread_cost"] = float(bucket["submitted_expected_spread_cost"]) + float(metrics["expected_spread_cost"])
            bucket["submitted_expected_slippage_cost"] = float(bucket["submitted_expected_slippage_cost"]) + float(metrics["expected_slippage_cost"])
            bucket["submitted_expected_commission_cost"] = float(bucket["submitted_expected_commission_cost"]) + float(metrics["expected_commission_cost"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                style_counts_submitted = dict(bucket.get("_style_counts_submitted") or {})
                style_counts_submitted[style] = int(style_counts_submitted.get(style, 0)) + 1
                bucket["_style_counts_submitted"] = style_counts_submitted

    for row in fill_rows:
        broker_order_id = int(row.get("order_id") or 0)
        order_meta = dict(order_meta_by_broker.get(broker_order_id) or {})
        portfolio_id = str(order_meta.get("portfolio_id") or row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        session = _execution_session_profile_from_order(order_meta)
        market = str(order_meta.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or "")))
        bucket = _bucket(portfolio_id, market, session["session_bucket"], session["session_label"])
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        slippage_dev_bps = row.get("slippage_bps_deviation")
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + float(fill_notional)
        bucket["commission_total"] = float(bucket["commission_total"]) + float(commission)
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + float(fill_notional) * slip / 10000.0
        if slippage_dev_bps not in (None, ""):
            bucket["_slippage_dev_samples"].append(float(slippage_dev_bps or 0.0))

    session_sort_order = {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}
    out: List[Dict[str, Any]] = []
    for (_, session_bucket), bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        basis_label = "submitted_orders" if use_submitted else "planned_orders"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        style_counts = dict(bucket.get("_style_counts_submitted") or {}) if use_submitted else dict(bucket.get("_style_counts_all") or {})
        slippage_samples = [float(v) for v in list(bucket.pop("_slippage_samples", []) or [])]
        slippage_dev_samples = [float(v) for v in list(bucket.pop("_slippage_dev_samples", []) or [])]
        planned_execution_cost_total = float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0)
        execution_cost_total = float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0)
        out.append(
            {
                "portfolio_id": str(bucket.get("portfolio_id") or ""),
                "market": str(bucket.get("market") or ""),
                "session_bucket": str(session_bucket),
                "session_label": str(bucket.get("session_label") or _SESSION_LABELS.get(session_bucket, "未知时段")),
                "planned_cost_basis": basis_label,
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "submitted_order_rows": int(submitted_rows),
                "planned_order_value": float(value),
                "planned_spread_cost_total": float(bucket.get(f"{basis_prefix}_expected_spread_cost", 0.0) or 0.0),
                "planned_slippage_cost_total": float(bucket.get(f"{basis_prefix}_expected_slippage_cost", 0.0) or 0.0),
                "planned_commission_cost_total": float(bucket.get(f"{basis_prefix}_expected_commission_cost", 0.0) or 0.0),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "avg_expected_cost_bps": float(numerator / value) if value > 0.0 else None,
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "commission_total": float(bucket.get("commission_total", 0.0) or 0.0),
                "slippage_cost_total": float(bucket.get("slippage_cost_total", 0.0) or 0.0),
                "execution_cost_total": float(execution_cost_total),
                "execution_cost_gap": float(execution_cost_total - planned_execution_cost_total),
                "avg_actual_slippage_bps": _avg_defined(slippage_samples),
                "avg_slippage_bps_deviation": _avg_defined(slippage_dev_samples),
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
            }
        )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), session_sort_order.get(str(row.get("session_bucket") or ""), 9)))
    return out

def _build_execution_hotspot_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    order_meta_by_broker: Dict[int, Dict[str, Any]] = {}
    grouped: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    def _bucket(portfolio_id: str, market: str, symbol: str, session_bucket: str, session_label: str) -> Dict[str, Any]:
        return grouped.setdefault(
            (portfolio_id, session_bucket, symbol),
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "symbol": symbol,
                "session_bucket": session_bucket,
                "session_label": session_label,
                "all_order_rows": 0,
                "submitted_order_rows": 0,
                "all_order_value": 0.0,
                "submitted_order_value": 0.0,
                "all_expected_cost_total": 0.0,
                "all_expected_cost_bps_numerator": 0.0,
                "submitted_expected_cost_total": 0.0,
                "submitted_expected_cost_bps_numerator": 0.0,
                "fill_count": 0,
                "fill_notional": 0.0,
                "commission_total": 0.0,
                "slippage_cost_total": 0.0,
                "_slippage_samples": [],
                "_slippage_dev_samples": [],
                "_style_counts_all": {},
                "_style_counts_submitted": {},
            },
        )

    for row in execution_orders:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        if not portfolio_id or not symbol:
            continue
        broker_order_id = int(row.get("broker_order_id") or 0)
        if broker_order_id > 0:
            order_meta_by_broker[broker_order_id] = dict(row)
        status = str(row.get("status") or "").strip().upper()
        if status.startswith("BLOCKED"):
            continue
        session = _execution_session_profile_from_order(row)
        metrics = _planned_cost_metrics_from_order(row)
        bucket = _bucket(
            portfolio_id,
            str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, symbol)),
            symbol,
            session["session_bucket"],
            session["session_label"],
        )
        submitted_like = (
            broker_order_id > 0
            or status in {"CREATED", "SUBMITTED", "PRESUBMITTED", "FILLED", "PARTIAL", "PARTIALLY_FILLED"}
            or status.startswith("ERROR_")
        )
        bucket["all_order_rows"] = int(bucket["all_order_rows"]) + 1
        bucket["all_order_value"] = float(bucket["all_order_value"]) + float(metrics["order_value"])
        bucket["all_expected_cost_total"] = float(bucket["all_expected_cost_total"]) + float(metrics["expected_cost_value"])
        bucket["all_expected_cost_bps_numerator"] = float(bucket["all_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
        style = str(session.get("execution_style") or metrics.get("execution_style") or "").strip()
        if style:
            style_counts_all = dict(bucket.get("_style_counts_all") or {})
            style_counts_all[style] = int(style_counts_all.get(style, 0)) + 1
            bucket["_style_counts_all"] = style_counts_all
        if submitted_like:
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + 1
            bucket["submitted_order_value"] = float(bucket["submitted_order_value"]) + float(metrics["order_value"])
            bucket["submitted_expected_cost_total"] = float(bucket["submitted_expected_cost_total"]) + float(metrics["expected_cost_value"])
            bucket["submitted_expected_cost_bps_numerator"] = float(bucket["submitted_expected_cost_bps_numerator"]) + float(metrics["expected_cost_bps"]) * float(metrics["order_value"])
            if style:
                style_counts_submitted = dict(bucket.get("_style_counts_submitted") or {})
                style_counts_submitted[style] = int(style_counts_submitted.get(style, 0)) + 1
                bucket["_style_counts_submitted"] = style_counts_submitted

    for row in fill_rows:
        broker_order_id = int(row.get("order_id") or 0)
        order_meta = dict(order_meta_by_broker.get(broker_order_id) or {})
        portfolio_id = str(order_meta.get("portfolio_id") or row.get("portfolio_id") or "").strip()
        symbol = str(order_meta.get("symbol") or row.get("symbol") or "").upper().strip()
        if not portfolio_id or not symbol:
            continue
        session = _execution_session_profile_from_order(order_meta)
        bucket = _bucket(
            portfolio_id,
            str(order_meta.get("market") or _market_from_portfolio_or_symbol(portfolio_id, symbol)),
            symbol,
            session["session_bucket"],
            session["session_label"],
        )
        fill_notional = abs(float(row.get("qty") or 0.0)) * abs(float(row.get("price") or 0.0))
        actual_slippage_bps = row.get("actual_slippage_bps")
        slippage_dev_bps = row.get("slippage_bps_deviation")
        commission = float(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0))
        bucket["fill_count"] = int(bucket["fill_count"]) + 1
        bucket["fill_notional"] = float(bucket["fill_notional"]) + float(fill_notional)
        bucket["commission_total"] = float(bucket["commission_total"]) + float(commission)
        if actual_slippage_bps not in (None, ""):
            slip = float(actual_slippage_bps or 0.0)
            bucket["_slippage_samples"].append(slip)
            bucket["slippage_cost_total"] = float(bucket["slippage_cost_total"]) + float(fill_notional) * slip / 10000.0
        if slippage_dev_bps not in (None, ""):
            bucket["_slippage_dev_samples"].append(float(slippage_dev_bps or 0.0))

    session_sort_order = {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}
    out: List[Dict[str, Any]] = []
    for (_, session_bucket, symbol), bucket in grouped.items():
        submitted_rows = int(bucket.get("submitted_order_rows", 0) or 0)
        submitted_value = float(bucket.get("submitted_order_value", 0.0) or 0.0)
        use_submitted = submitted_rows > 0 and submitted_value > 0.0
        basis_prefix = "submitted" if use_submitted else "all"
        value = float(bucket.get(f"{basis_prefix}_order_value", 0.0) or 0.0)
        numerator = float(bucket.get(f"{basis_prefix}_expected_cost_bps_numerator", 0.0) or 0.0)
        planned_execution_cost_total = float(bucket.get(f"{basis_prefix}_expected_cost_total", 0.0) or 0.0)
        execution_cost_total = float(bucket.get("commission_total", 0.0) or 0.0) + float(bucket.get("slippage_cost_total", 0.0) or 0.0)
        execution_cost_gap = float(execution_cost_total - planned_execution_cost_total)
        avg_expected_cost_bps = float(numerator / value) if value > 0.0 else 0.0
        avg_actual_slippage_bps = _avg_defined([float(v) for v in list(bucket.pop("_slippage_samples", []) or [])])
        avg_slippage_bps_deviation = _avg_defined([float(v) for v in list(bucket.pop("_slippage_dev_samples", []) or [])])
        style_counts = dict(bucket.get("_style_counts_submitted") or {}) if use_submitted else dict(bucket.get("_style_counts_all") or {})
        bps_gap = max(0.0, float(avg_actual_slippage_bps or 0.0) - float(avg_expected_cost_bps or 0.0))
        pressure_score = float(max(0.0, execution_cost_gap) + max(0.0, float(bucket.get("fill_notional", 0.0) or value)) * bps_gap / 10000.0)
        hotspot_action = "INVESTIGATE_EXECUTION"
        hotspot_reason = "该标的在当前时段的实际执行成本高于计划，优先复盘成交时机、参与率和拆单风格。"
        if execution_cost_gap <= max(2.0, planned_execution_cost_total * 0.12) and bps_gap <= 4.0:
            hotspot_action = "OBSERVE"
            hotspot_reason = "该标的在当前时段没有明显超成本，先继续观察样本。"
        out.append(
            {
                "portfolio_id": str(bucket.get("portfolio_id") or ""),
                "market": str(bucket.get("market") or ""),
                "symbol": symbol,
                "session_bucket": str(session_bucket),
                "session_label": str(bucket.get("session_label") or _SESSION_LABELS.get(session_bucket, "未知时段")),
                "planned_order_rows": int(bucket.get(f"{basis_prefix}_order_rows", 0) or 0),
                "submitted_order_rows": int(submitted_rows),
                "planned_order_value": float(value),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "execution_cost_total": float(execution_cost_total),
                "execution_cost_gap": float(execution_cost_gap),
                "avg_expected_cost_bps": float(avg_expected_cost_bps),
                "avg_actual_slippage_bps": avg_actual_slippage_bps,
                "avg_slippage_bps_deviation": avg_slippage_bps_deviation,
                "fill_count": int(bucket.get("fill_count", 0) or 0),
                "fill_notional": float(bucket.get("fill_notional", 0.0) or 0.0),
                "execution_style_breakdown": ",".join(f"{name}:{style_counts[name]}" for name in sorted(style_counts)),
                "pressure_score": float(pressure_score),
                "hotspot_action": hotspot_action,
                "hotspot_reason": hotspot_reason,
            }
        )
    out.sort(
        key=lambda row: (
            -float(row.get("pressure_score", 0.0) or 0.0),
            -float(row.get("execution_cost_gap", 0.0) or 0.0),
            str(row.get("portfolio_id") or ""),
            session_sort_order.get(str(row.get("session_bucket") or ""), 9),
            str(row.get("symbol") or ""),
        )
    )
    return out

def _build_execution_feedback_rows(
    attribution_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]] | None = None,
    execution_hotspot_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    broker_summary_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in broker_summary_rows
        if str(row.get("portfolio_id") or "").strip()
    }
    session_map: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(execution_session_rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        session_map.setdefault(portfolio_id, []).append(dict(raw))
    hotspot_map: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(execution_hotspot_rows or []):
        portfolio_id = str(raw.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        hotspot_map.setdefault(portfolio_id, []).append(dict(raw))
    out: List[Dict[str, Any]] = []
    for row in attribution_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        broker_summary = dict(broker_summary_map.get(portfolio_id) or {})
        plan_cost = float(row.get("planned_execution_cost_total", 0.0) or 0.0)
        actual_cost = float(row.get("execution_cost_total", 0.0) or 0.0)
        cost_gap = float(row.get("execution_cost_gap", actual_cost - plan_cost) or 0.0)
        expected_bps = float(row.get("avg_expected_cost_bps", 0.0) or 0.0)
        actual_bps = float(row.get("avg_actual_slippage_bps", 0.0) or 0.0)
        submitted_order_rows = int(broker_summary.get("submitted_order_rows", 0) or 0)
        error_order_rows = int(broker_summary.get("error_order_rows", 0) or 0)
        latest_gap_symbols = int(broker_summary.get("latest_gap_symbols", 0) or 0)
        if submitted_order_rows <= 0 and plan_cost <= 0.0 and actual_cost <= 0.0:
            continue
        control_context = _feedback_control_driver_context(
            strategy_delta=float(row.get("strategy_control_weight_delta", 0.0) or 0.0),
            risk_delta=float(row.get("risk_overlay_weight_delta", 0.0) or 0.0),
            execution_gate_weight=float(row.get("execution_gate_blocked_weight", 0.0) or 0.0),
            execution_gate_ratio=float(row.get("execution_gate_blocked_order_ratio", 0.0) or 0.0),
            execution_gate_value=float(row.get("execution_gate_blocked_order_value", 0.0) or 0.0),
        )
        control_driver = str(control_context.get("feedback_control_driver", "") or "")
        gate_weight = float(control_context.get("execution_gate_blocked_weight", 0.0) or 0.0)
        gate_ratio = float(control_context.get("execution_gate_blocked_order_ratio", 0.0) or 0.0)
        control_driver_reason = ""

        action = "HOLD"
        feedback_reason = "计划与实际执行成本目前大致一致，继续观察当前拆单与参与率。"
        adv_max_participation_delta = 0.0
        adv_split_trigger_delta = 0.0
        max_slices_delta = 0
        open_session_scale_delta = 0.0
        midday_session_scale_delta = 0.0
        close_session_scale_delta = 0.0

        gap_ratio = max(0.0, cost_gap / max(plan_cost, 1.0))
        bps_gap = max(0.0, actual_bps - expected_bps)
        severity = max(
            gap_ratio,
            bps_gap / 20.0,
            0.50 if error_order_rows > 0 else 0.0,
            0.35 if latest_gap_symbols > 0 else 0.0,
        )

        if cost_gap > max(10.0, plan_cost * 0.35) or actual_bps > expected_bps + 8.0 or error_order_rows > 0:
            action = "TIGHTEN"
            adv_max_participation_delta = -_clamp(0.005 + severity * 0.007, 0.005, 0.020)
            adv_split_trigger_delta = -_clamp(0.002 + severity * 0.003, 0.002, 0.010)
            max_slices_delta = int(min(2, max(1, round(1.0 + severity))))
            open_session_scale_delta = -_clamp(0.03 + severity * 0.04, 0.03, 0.10)
            midday_session_scale_delta = -_clamp(0.02 + severity * 0.03, 0.02, 0.08)
            close_session_scale_delta = -_clamp(0.03 + severity * 0.04, 0.03, 0.10)
            feedback_reason = "实际执行成本高于计划，下一轮收紧 ADV 参与率、提前触发拆单，并降低开盘/尾盘的参与强度。"
        elif (
            plan_cost > 0.0
            and cost_gap < -max(5.0, plan_cost * 0.20)
            and actual_bps + 5.0 < expected_bps
            and error_order_rows == 0
            and latest_gap_symbols == 0
        ):
            action = "RELAX"
            adv_max_participation_delta = _clamp(0.005 + min(0.010, abs(cost_gap) / max(plan_cost, 1.0) * 0.004), 0.005, 0.015)
            adv_split_trigger_delta = _clamp(0.002 + min(0.004, abs(cost_gap) / max(plan_cost, 1.0) * 0.002), 0.002, 0.006)
            max_slices_delta = -1 if submitted_order_rows > 0 else 0
            open_session_scale_delta = _clamp(0.02 + min(0.03, abs(cost_gap) / max(plan_cost, 1.0) * 0.02), 0.02, 0.05)
            midday_session_scale_delta = _clamp(0.01 + min(0.02, abs(cost_gap) / max(plan_cost, 1.0) * 0.01), 0.01, 0.03)
            close_session_scale_delta = _clamp(0.02 + min(0.03, abs(cost_gap) / max(plan_cost, 1.0) * 0.02), 0.02, 0.05)
            feedback_reason = "实际执行成本持续低于计划，可适度放宽参与率并减少过度拆单。"

        session_feedback_rows: List[Dict[str, Any]] = []
        session_scale_delta_map = {"OPEN": 0.0, "MIDDAY": 0.0, "CLOSE": 0.0}
        dominant_session_bucket = ""
        dominant_session_label = ""
        dominant_session_magnitude = -1.0
        dominant_hotspot_symbol = ""
        dominant_hotspot_session_label = ""
        hotspot_rows = list(hotspot_map.get(portfolio_id, []) or [])
        execution_penalties = _build_execution_hotspot_penalties(hotspot_rows)
        used_session_specific_scales = False
        for session_row in sorted(
            list(session_map.get(portfolio_id, []) or []),
            key=lambda item: {"OPEN": 0, "MIDDAY": 1, "CLOSE": 2, "UNKNOWN": 3}.get(str(item.get("session_bucket") or ""), 9),
        ):
            session_bucket = str(session_row.get("session_bucket") or "").upper().strip()
            if session_bucket not in {"OPEN", "MIDDAY", "CLOSE"}:
                continue
            session_label = str(session_row.get("session_label") or _SESSION_LABELS.get(session_bucket, session_bucket))
            session_plan_cost = float(session_row.get("planned_execution_cost_total", 0.0) or 0.0)
            session_actual_cost = float(session_row.get("execution_cost_total", 0.0) or 0.0)
            session_cost_gap = float(session_row.get("execution_cost_gap", session_actual_cost - session_plan_cost) or 0.0)
            session_expected_bps = float(session_row.get("avg_expected_cost_bps", 0.0) or 0.0)
            session_actual_bps = float(session_row.get("avg_actual_slippage_bps", 0.0) or 0.0)
            session_fill_count = int(session_row.get("fill_count", 0) or 0)
            session_submitted_rows = int(session_row.get("submitted_order_rows", 0) or 0)
            session_action = "HOLD"
            session_reason = f"{session_label}成本与滑点大致稳定，暂不单独调整该时段参与率。"
            session_scale_delta = 0.0
            if session_submitted_rows > 0 or session_fill_count > 0:
                session_gap_ratio = max(0.0, session_cost_gap / max(session_plan_cost, 1.0))
                session_bps_gap = max(0.0, session_actual_bps - session_expected_bps)
                session_severity = max(session_gap_ratio, session_bps_gap / 16.0)
                if session_cost_gap > max(3.0, session_plan_cost * 0.20) or session_actual_bps > session_expected_bps + 4.0:
                    session_action = "TIGHTEN"
                    session_scale_delta = -_clamp(0.015 + session_severity * 0.040, 0.015, 0.100)
                    session_reason = f"{session_label}的实际执行成本高于计划，下一轮应降低该时段参与率。"
                elif (
                    session_plan_cost > 0.0
                    and session_cost_gap < -max(2.0, session_plan_cost * 0.12)
                    and session_actual_bps + 3.0 < session_expected_bps
                ):
                    session_action = "RELAX"
                    session_scale_delta = _clamp(0.010 + min(0.030, abs(session_cost_gap) / max(session_plan_cost, 1.0) * 0.015), 0.010, 0.050)
                    session_reason = f"{session_label}的实际执行成本持续低于计划，可适度放宽该时段参与率。"
            if abs(session_scale_delta) > 1e-9:
                session_scale_delta_map[session_bucket] = float(session_scale_delta)
                used_session_specific_scales = True
            magnitude = abs(session_cost_gap) + abs(session_actual_bps - session_expected_bps) / 10.0
            if magnitude > dominant_session_magnitude:
                dominant_session_magnitude = magnitude
                dominant_session_bucket = session_bucket
                dominant_session_label = session_label
            session_feedback_rows.append(
                {
                    "session_bucket": session_bucket,
                    "session_label": session_label,
                    "session_action": session_action,
                    "planned_execution_cost_total": round(float(session_plan_cost), 6),
                    "execution_cost_total": round(float(session_actual_cost), 6),
                    "execution_cost_gap": round(float(session_cost_gap), 6),
                    "avg_expected_cost_bps": round(float(session_expected_bps), 6),
                    "avg_actual_slippage_bps": round(float(session_actual_bps), 6),
                    "submitted_order_rows": int(session_submitted_rows),
                    "fill_count": int(session_fill_count),
                    "scale_delta": round(float(session_scale_delta), 6),
                    "execution_style_breakdown": str(session_row.get("execution_style_breakdown", "") or ""),
                    "reason": session_reason,
                }
            )

        if used_session_specific_scales:
            open_session_scale_delta = float(session_scale_delta_map.get("OPEN", 0.0))
            midday_session_scale_delta = float(session_scale_delta_map.get("MIDDAY", 0.0))
            close_session_scale_delta = float(session_scale_delta_map.get("CLOSE", 0.0))
            if action == "HOLD":
                if any(str(item.get("session_action") or "") == "TIGHTEN" for item in session_feedback_rows):
                    action = "TIGHTEN"
                elif any(str(item.get("session_action") or "") == "RELAX" for item in session_feedback_rows):
                    action = "RELAX"
            if dominant_session_label:
                feedback_reason = f"总执行成本之外，{dominant_session_label}是本周最需要关注的执行时段；已优先按时段反馈调整下一轮参与率。"
        hotspot_rows.sort(
            key=lambda item: (
                -float(item.get("pressure_score", 0.0) or 0.0),
                -float(item.get("execution_cost_gap", 0.0) or 0.0),
                str(item.get("symbol") or ""),
            )
        )
        top_hotspots: List[Dict[str, Any]] = []
        for hotspot in hotspot_rows:
            if float(hotspot.get("pressure_score", 0.0) or 0.0) <= 0.0:
                continue
            top_hotspots.append(
                {
                    "symbol": str(hotspot.get("symbol") or ""),
                    "session_bucket": str(hotspot.get("session_bucket") or ""),
                    "session_label": str(hotspot.get("session_label") or ""),
                    "hotspot_action": str(hotspot.get("hotspot_action") or ""),
                    "planned_execution_cost_total": round(float(hotspot.get("planned_execution_cost_total", 0.0) or 0.0), 6),
                    "execution_cost_total": round(float(hotspot.get("execution_cost_total", 0.0) or 0.0), 6),
                    "execution_cost_gap": round(float(hotspot.get("execution_cost_gap", 0.0) or 0.0), 6),
                    "avg_expected_cost_bps": round(float(hotspot.get("avg_expected_cost_bps", 0.0) or 0.0), 6),
                    "avg_actual_slippage_bps": round(float(hotspot.get("avg_actual_slippage_bps", 0.0) or 0.0), 6),
                    "pressure_score": round(float(hotspot.get("pressure_score", 0.0) or 0.0), 6),
                    "execution_style_breakdown": str(hotspot.get("execution_style_breakdown", "") or ""),
                    "reason": str(hotspot.get("hotspot_reason", "") or ""),
                }
            )
            if len(top_hotspots) >= 6:
                break
        if top_hotspots:
            dominant_hotspot_symbol = str(top_hotspots[0].get("symbol") or "")
            dominant_hotspot_session_label = str(top_hotspots[0].get("session_label") or "")
            feedback_reason = f"{feedback_reason.rstrip('。')}。当前最需要排查的执行热点是 {dominant_hotspot_session_label}/{dominant_hotspot_symbol}。"
        if execution_penalties:
            penalty_symbols = ",".join(str(item.get("symbol") or "") for item in execution_penalties[:6])
            feedback_reason = f"{feedback_reason.rstrip('。')}。下一轮候选会对这些执行热点标的增加成本/执行惩罚: {penalty_symbols}。"

        gate_pressure_high = gate_ratio >= 0.35 or gate_weight >= 0.03
        if gate_pressure_high and control_driver == "EXECUTION" and action in {"HOLD", "RELAX"}:
            action = "HOLD"
            adv_max_participation_delta = 0.0
            adv_split_trigger_delta = 0.0
            max_slices_delta = 0
            open_session_scale_delta = 0.0
            midday_session_scale_delta = 0.0
            close_session_scale_delta = 0.0
            execution_penalties = []
            control_driver_reason = "本周更明显的问题是执行 gate 阻断，而不是成交成本；优先复核 opportunity/quality/risk/review gate，暂不直接调整 ADV/拆单参数。"
            feedback_reason = f"{control_driver_reason}（{str(control_context.get('feedback_control_split_text') or '')}）"
        elif gate_pressure_high and control_driver == "EXECUTION" and action == "TIGHTEN":
            control_driver_reason = (
                f"同时存在明显的执行 gate 阻断（{str(control_context.get('feedback_control_split_text') or '')}），"
                "执行参数收紧之外还应复核 gate 阈值。"
            )
            feedback_reason = f"{feedback_reason.rstrip('。')}。{control_driver_reason}"

        bps_gap_ratio = max(0.0, actual_bps - expected_bps) / 12.0
        gap_value_ratio = max(0.0, cost_gap) / max(plan_cost, 1.0)
        if action != "HOLD" and control_driver == "EXECUTION":
            gap_value_ratio = max(gap_value_ratio, min(1.0, gate_weight / 0.05))
        base_confidence = _feedback_confidence(
            sample_ratio=float((submitted_order_rows + latest_gap_symbols) / 5.0),
            magnitude_ratio=max(bps_gap_ratio, gap_value_ratio / 0.50),
            persistence_ratio=float(len(top_hotspots) / 3.0),
            structure_ratio=float(len(session_feedback_rows) / 3.0),
        ) if action != "HOLD" else 0.0
        calibration_info = _feedback_calibration_support(
            dict((feedback_calibration_map or {}).get(portfolio_id, {}) or {}),
            feedback_kind="execution",
            action=action,
        )
        confidence = _apply_outcome_calibration(base_confidence, float(calibration_info.get("score", 0.5) or 0.5))

        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or ""),
                "feedback_scope": "paper_only",
                "execution_feedback_action": action,
                "execution_adv_max_participation_pct_delta": round(float(adv_max_participation_delta), 6),
                "execution_adv_split_trigger_pct_delta": round(float(adv_split_trigger_delta), 6),
                "execution_max_slices_per_symbol_delta": int(max_slices_delta),
                "execution_open_session_participation_scale_delta": round(float(open_session_scale_delta), 6),
                "execution_midday_session_participation_scale_delta": round(float(midday_session_scale_delta), 6),
                "execution_close_session_participation_scale_delta": round(float(close_session_scale_delta), 6),
                "planned_execution_cost_total": round(float(plan_cost), 6),
                "execution_cost_total": round(float(actual_cost), 6),
                "execution_cost_gap": round(float(cost_gap), 6),
                "avg_expected_cost_bps": round(float(expected_bps), 6),
                "avg_actual_slippage_bps": round(float(actual_bps), 6),
                "submitted_order_rows": int(submitted_order_rows),
                "error_order_rows": int(error_order_rows),
                "latest_gap_symbols": int(latest_gap_symbols),
                "execution_style_breakdown": str(row.get("execution_style_breakdown", "") or ""),
                "dominant_execution_session_bucket": dominant_session_bucket,
                "dominant_execution_session_label": dominant_session_label,
                "execution_session_feedback_json": json.dumps(session_feedback_rows, ensure_ascii=False),
                "dominant_execution_hotspot_symbol": dominant_hotspot_symbol,
                "dominant_execution_hotspot_session_label": dominant_hotspot_session_label,
                "execution_hotspots_json": json.dumps(top_hotspots, ensure_ascii=False),
                "execution_penalty_symbol_count": int(len(execution_penalties)),
                "execution_penalty_symbols": ",".join(str(item.get("symbol") or "") for item in execution_penalties[:12]),
                "execution_penalties_json": json.dumps(execution_penalties, ensure_ascii=False),
                "feedback_sample_count": int(submitted_order_rows + latest_gap_symbols),
                "feedback_base_confidence": float(base_confidence),
                "feedback_base_confidence_label": _feedback_confidence_label(base_confidence),
                "feedback_calibration_score": float(calibration_info.get("score", 0.5) or 0.5),
                "feedback_calibration_label": str(calibration_info.get("label", "MEDIUM") or "MEDIUM"),
                "feedback_calibration_sample_count": int(calibration_info.get("sample_count", 0) or 0),
                "feedback_calibration_horizon_days": str(calibration_info.get("selected_horizon_days", "") or ""),
                "feedback_calibration_scope": str(calibration_info.get("selection_scope_label", "") or "-"),
                "feedback_calibration_reason": str(calibration_info.get("reason", "") or ""),
                "feedback_confidence": float(confidence),
                "feedback_confidence_label": _feedback_confidence_label(confidence),
                "feedback_reason": feedback_reason,
                "feedback_control_driver": str(control_context.get("feedback_control_driver", "") or ""),
                "feedback_control_driver_label": str(control_context.get("feedback_control_driver_label", "") or ""),
                "feedback_control_driver_weight": float(control_context.get("feedback_control_driver_weight", 0.0) or 0.0),
                "feedback_control_split_text": str(control_context.get("feedback_control_split_text", "") or ""),
                "feedback_control_driver_reason": control_driver_reason,
                "strategy_control_weight_delta": float(control_context.get("strategy_control_weight_delta", 0.0) or 0.0),
                "risk_overlay_weight_delta": float(control_context.get("risk_overlay_weight_delta", 0.0) or 0.0),
                "execution_gate_blocked_weight": float(gate_weight),
                "execution_gate_blocked_order_ratio": float(gate_ratio),
                "execution_gate_blocked_order_value": float(control_context.get("execution_gate_blocked_order_value", 0.0) or 0.0),
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("execution_feedback_action", "") or "") == "TIGHTEN" else 1 if str(row.get("execution_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("portfolio_id", "") or ""),
        )
    )
    return out

def _filter_execution_metric_rows(
    rows: List[Dict[str, Any]],
    *,
    since_ts: str,
    portfolio_filter: str,
    market_filter: str,
    market_from_portfolio_or_symbol_fn: Callable[[str, str], str],
) -> List[Dict[str, Any]]:
    filtered_rows: List[Dict[str, Any]] = []
    for row in list(rows or []):
        if str(row.get("system_kind") or "").strip() not in {"investment", ""}:
            continue
        ts = str(row.get("ts") or "")
        if ts and ts < since_ts:
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_filter and portfolio_id != portfolio_filter:
            continue
        market_code = market_from_portfolio_or_symbol_fn(portfolio_id, str(row.get("symbol") or ""))
        if market_filter and market_code and market_code != market_filter:
            continue
        filtered_rows.append(dict(row))
    return filtered_rows

def _apply_execution_broker_summary_context(
    broker_summary_rows: List[Dict[str, Any]],
    *,
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
) -> None:
    execution_effect_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(execution_effect_rows or [])}
    planned_execution_map = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in list(planned_execution_cost_rows or [])
    }
    edge_realization_map = {str(row.get("portfolio_id") or ""): dict(row) for row in list(edge_realization_rows or [])}
    for row in list(broker_summary_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "")
        effect = dict(execution_effect_map.get(portfolio_id) or {})
        planned = dict(planned_execution_map.get(portfolio_id) or {})
        realized = dict(edge_realization_map.get(portfolio_id) or {})
        row["fill_count"] = int(effect.get("fill_count", 0) or 0)
        row["fill_notional"] = float(effect.get("fill_notional", 0.0) or 0.0)
        row["commission_total"] = float(effect.get("commission_total", 0.0) or 0.0)
        row["slippage_cost_total"] = float(effect.get("slippage_cost_total", 0.0) or 0.0)
        row["execution_cost_total"] = float(effect.get("execution_cost_total", 0.0) or 0.0)
        row["avg_actual_slippage_bps"] = effect.get("avg_actual_slippage_bps")
        row["avg_fill_delay_seconds"] = realized.get("avg_fill_delay_seconds")
        row["avg_realized_total_cost_bps"] = realized.get("avg_realized_total_cost_bps")
        row["avg_execution_capture_bps"] = realized.get("avg_execution_capture_bps")
        row["planned_cost_basis"] = str(planned.get("planned_cost_basis", "") or "")
        row["planned_order_rows"] = int(planned.get("planned_order_rows", 0) or 0)
        row["planned_order_value"] = float(planned.get("planned_order_value", 0.0) or 0.0)
        row["planned_spread_cost_total"] = float(planned.get("planned_spread_cost_total", 0.0) or 0.0)
        row["planned_slippage_cost_total"] = float(planned.get("planned_slippage_cost_total", 0.0) or 0.0)
        row["planned_commission_cost_total"] = float(planned.get("planned_commission_cost_total", 0.0) or 0.0)
        row["planned_execution_cost_total"] = float(planned.get("planned_execution_cost_total", 0.0) or 0.0)
        row["avg_expected_cost_bps"] = planned.get("avg_expected_cost_bps")
        row["execution_style_breakdown"] = str(planned.get("execution_style_breakdown", "") or "")
        row["execution_cost_gap"] = float(row["execution_cost_total"] - row["planned_execution_cost_total"])

def _build_execution_analysis_bundle(
    *,
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
    execution_order_rows: List[Dict[str, Any]],
    execution_run_rows: List[Dict[str, Any]],
    snapshot_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    since_ts: str,
    portfolio_filter: str,
    market_filter: str,
    market_from_portfolio_or_symbol_fn: Callable[[str, str], str],
) -> Dict[str, Any]:
    filtered_fill_rows = _filter_execution_metric_rows(
        fill_rows,
        since_ts=since_ts,
        portfolio_filter=portfolio_filter,
        market_filter=market_filter,
        market_from_portfolio_or_symbol_fn=market_from_portfolio_or_symbol_fn,
    )
    filtered_commission_rows = _filter_execution_metric_rows(
        commission_rows,
        since_ts=since_ts,
        portfolio_filter=portfolio_filter,
        market_filter=market_filter,
        market_from_portfolio_or_symbol_fn=market_from_portfolio_or_symbol_fn,
    )
    execution_effect_rows = _build_execution_effect_rows(filtered_fill_rows, filtered_commission_rows)
    planned_execution_cost_rows = _build_planned_execution_cost_rows(execution_order_rows)
    execution_gate_rows = _build_execution_gate_rows(execution_order_rows)
    linked_execution_order_rows = _link_execution_orders_to_candidate_snapshots(
        execution_order_rows,
        execution_run_rows,
        snapshot_rows,
    )
    execution_parent_rows = _build_execution_parent_rows(
        linked_execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
        outcome_rows,
    )
    outcome_spread_rows = _build_weekly_outcome_spread_rows(
        snapshot_rows,
        outcome_rows,
        execution_parent_rows,
    )
    edge_realization_rows = _build_weekly_edge_realization_rows(execution_parent_rows)
    blocked_edge_attribution_rows = _build_weekly_blocked_edge_attribution_rows(execution_parent_rows)
    execution_session_rows = _build_execution_session_rows(
        execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
    )
    execution_hotspot_rows = _build_execution_hotspot_rows(
        execution_order_rows,
        filtered_fill_rows,
        filtered_commission_rows,
    )
    _apply_execution_broker_summary_context(
        broker_summary_rows,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        edge_realization_rows=edge_realization_rows,
    )
    return {
        "filtered_fill_rows": filtered_fill_rows,
        "filtered_commission_rows": filtered_commission_rows,
        "execution_effect_rows": execution_effect_rows,
        "planned_execution_cost_rows": planned_execution_cost_rows,
        "execution_gate_rows": execution_gate_rows,
        "linked_execution_order_rows": linked_execution_order_rows,
        "execution_parent_rows": execution_parent_rows,
        "outcome_spread_rows": outcome_spread_rows,
        "edge_realization_rows": edge_realization_rows,
        "blocked_edge_attribution_rows": blocked_edge_attribution_rows,
        "execution_session_rows": execution_session_rows,
        "execution_hotspot_rows": execution_hotspot_rows,
    }

def _execution_order_status_bucket(row: Dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip().upper()
    broker_order_id = _safe_int(row.get("broker_order_id"), 0)
    if status == "BLOCKED_EDGE":
        return "BLOCKED_EDGE"
    if _is_execution_gate_status(status):
        return "BLOCKED_GATE"
    if broker_order_id > 0 or status in {
        "CREATED",
        "SUBMITTED",
        "PRESUBMITTED",
        "FILLED",
        "PARTIAL",
        "PARTIALLY_FILLED",
    } or status.startswith("ERROR_"):
        return "SUBMITTED"
    return "PLANNED"

def _order_edge_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = 0.0) -> Any:
        direct = row.get(key)
        if direct not in (None, ""):
            return direct
        nested = details.get(key)
        if nested not in (None, ""):
            return nested
        plan_val = plan_row.get(key)
        if plan_val not in (None, ""):
            return plan_val
        return default

    return {
        "parent_order_key": str(_pick("parent_order_key", "") or ""),
        "score_before_cost": _safe_float(_pick("score_before_cost", _pick("score", 0.0)), 0.0),
        "expected_cost_bps": _safe_float(_pick("expected_cost_bps", 0.0), 0.0),
        "expected_edge_threshold": _safe_float(_pick("expected_edge_threshold", 0.0), 0.0),
        "expected_edge_score": _safe_float(_pick("expected_edge_score", 0.0), 0.0),
        "expected_edge_bps": _safe_float(_pick("expected_edge_bps", 0.0), 0.0),
        "edge_gate_threshold_bps": _safe_float(_pick("edge_gate_threshold_bps", 0.0), 0.0),
        "session_bucket": str(_pick("session_bucket", "") or ""),
        "session_label": str(_pick("session_label", "") or ""),
        "execution_style": str(_pick("execution_style", "") or ""),
    }

def _order_execution_microstructure(row: Dict[str, Any]) -> Dict[str, Any]:
    details = _parse_json_dict(row.get("details"))
    plan_row = dict(details.get("plan_row") or {}) if isinstance(details.get("plan_row"), dict) else {}

    def _pick(key: str, default: Any = "") -> Any:
        direct = row.get(key)
        if direct not in (None, ""):
            return direct
        nested = details.get(key)
        if nested not in (None, ""):
            return nested
        plan_val = plan_row.get(key)
        if plan_val not in (None, ""):
            return plan_val
        return default

    return {
        "dynamic_liquidity_bucket": str(_pick("dynamic_liquidity_bucket", "") or "").strip().upper(),
        "dynamic_order_adv_pct": _safe_float(_pick("dynamic_order_adv_pct", 0.0), 0.0),
        "slice_count": max(1, _safe_int(_pick("slice_count", 1), 1)),
        "market_rule_status": str(_pick("market_rule_status", "") or "").strip().upper(),
    }

def _enrich_snapshot_rows(snapshot_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for raw in list(snapshot_rows or []):
        row = dict(raw)
        details = _parse_json_dict(row.get("details"))
        stage = str(row.get("stage") or details.get("stage") or "").strip().lower()
        row["details_json"] = details
        row["stage"] = stage
        row["analysis_run_id"] = str(row.get("analysis_run_id") or "").strip()
        row["report_dir"] = str(row.get("report_dir") or "").strip()
        row["stage_rank"] = _safe_int(details.get("stage_rank"), 0)
        row["stage1_rank"] = _safe_int(details.get("stage1_rank"), 0)
        row["expected_edge_threshold"] = _safe_float(
            row.get("expected_edge_threshold", details.get("expected_edge_threshold", 0.0)),
            0.0,
        )
        row["expected_edge_score"] = _safe_float(
            row.get("expected_edge_score", details.get("expected_edge_score", 0.0)),
            0.0,
        )
        row["expected_edge_bps"] = _safe_float(
            row.get("expected_edge_bps", details.get("expected_edge_bps", 0.0)),
            0.0,
        )
        enriched.append(row)
    return enriched

def _link_execution_orders_to_candidate_snapshots(
    execution_orders: List[Dict[str, Any]],
    execution_runs: List[Dict[str, Any]],
    snapshot_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    run_meta = {
        str(row.get("run_id") or ""): {
            "report_dir": str(row.get("report_dir") or "").strip(),
            "portfolio_id": str(row.get("portfolio_id") or "").strip(),
            "market": str(row.get("market") or "").strip(),
        }
        for row in list(execution_runs or [])
        if str(row.get("run_id") or "").strip()
    }
    enriched_snapshots = _enrich_snapshot_rows(snapshot_rows)
    snapshots_by_key: Dict[tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    snapshots_by_symbol: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for row in enriched_snapshots:
        report_dir = str(row.get("report_dir") or "").strip()
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        stage = str(row.get("stage") or "").strip().lower()
        if not report_dir or not portfolio_id or not symbol:
            continue
        snapshots_by_key.setdefault((report_dir, portfolio_id, symbol, stage), []).append(row)
        snapshots_by_symbol.setdefault((report_dir, portfolio_id, symbol), []).append(row)
    for rows in snapshots_by_key.values():
        rows.sort(
            key=lambda item: (
                -_candidate_snapshot_stage_priority(str(item.get("stage") or "")),
                _safe_int(item.get("stage_rank"), 10**6),
                str(item.get("ts") or ""),
            )
        )
    for rows in snapshots_by_symbol.values():
        rows.sort(
            key=lambda item: (
                -_candidate_snapshot_stage_priority(str(item.get("stage") or "")),
                _safe_int(item.get("stage_rank"), 10**6),
                str(item.get("ts") or ""),
            )
        )

    linked: List[Dict[str, Any]] = []
    for raw in list(execution_orders or []):
        row = dict(raw)
        metrics = _order_edge_metrics(row)
        row.update(metrics)
        run_id = str(row.get("run_id") or "").strip()
        meta = dict(run_meta.get(run_id) or {})
        report_dir = str(meta.get("report_dir") or "").strip()
        portfolio_id = str(row.get("portfolio_id") or meta.get("portfolio_id") or "").strip()
        symbol = str(row.get("symbol") or "").upper().strip()
        linked_snapshot: Dict[str, Any] = {}
        if report_dir and portfolio_id and symbol:
            for stage in _preferred_snapshot_stages_for_order(row):
                candidates = snapshots_by_key.get((report_dir, portfolio_id, symbol, stage), [])
                if candidates:
                    linked_snapshot = dict(candidates[0])
                    break
            if not linked_snapshot:
                fallback_rows = snapshots_by_symbol.get((report_dir, portfolio_id, symbol), [])
                if fallback_rows:
                    linked_snapshot = dict(fallback_rows[0])
        row["linked_report_dir"] = report_dir
        row["linked_snapshot_id"] = str(linked_snapshot.get("snapshot_id") or "")
        row["linked_snapshot_stage"] = str(linked_snapshot.get("stage") or "")
        row["linked_snapshot_stage_rank"] = _safe_int(linked_snapshot.get("stage_rank"), 0)
        row["linked_snapshot_ts"] = str(linked_snapshot.get("ts") or linked_snapshot.get("snapshot_ts") or "")
        row["linked_analysis_run_id"] = str(linked_snapshot.get("analysis_run_id") or "")
        linked.append(row)
    return linked

def _build_execution_parent_rows(
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]],
    commission_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    commission_by_exec: Dict[str, float] = {}
    for row in list(commission_rows or []):
        exec_id = str(row.get("exec_id") or "").strip()
        if exec_id:
            commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0) or 0.0) + _safe_float(row.get("value"), 0.0)

    fills_by_order: Dict[int, List[Dict[str, Any]]] = {}
    for raw in list(fill_rows or []):
        order_id = _safe_int(raw.get("order_id"), 0)
        if order_id <= 0:
            continue
        fills_by_order.setdefault(order_id, []).append(dict(raw))
    for rows in fills_by_order.values():
        rows.sort(key=lambda item: str(item.get("ts") or ""))

    outcomes_by_snapshot: Dict[str, Dict[int, Dict[str, Any]]] = {}
    for raw in list(outcome_rows or []):
        snapshot_id = str(raw.get("snapshot_id") or "").strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not snapshot_id or horizon_days <= 0:
            continue
        outcomes_by_snapshot.setdefault(snapshot_id, {})[horizon_days] = dict(raw)

    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for idx, raw in enumerate(list(execution_orders or []), start=1):
        row = dict(raw)
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        parent_order_key = str(row.get("parent_order_key") or "").strip()
        if not parent_order_key:
            parent_order_key = f"{str(row.get('run_id') or '')}:{str(row.get('linked_snapshot_id') or '')}:{str(row.get('symbol') or '')}:{idx}"
        key = (portfolio_id, parent_order_key)
        bucket = grouped.setdefault(
            key,
            {
                "portfolio_id": portfolio_id,
                "market": str(row.get("market") or _market_from_portfolio_or_symbol(portfolio_id, str(row.get("symbol") or ""))),
                "run_id": str(row.get("run_id") or ""),
                "parent_order_key": parent_order_key,
                "symbol": str(row.get("symbol") or "").upper(),
                "action": str(row.get("action") or ""),
                "linked_snapshot_id": str(row.get("linked_snapshot_id") or ""),
                "linked_snapshot_stage": str(row.get("linked_snapshot_stage") or ""),
                "linked_analysis_run_id": str(row.get("linked_analysis_run_id") or ""),
                "linked_report_dir": str(row.get("linked_report_dir") or ""),
                "order_row_count": 0,
                "order_value": 0.0,
                "expected_edge_bps_numerator": 0.0,
                "expected_cost_bps_numerator": 0.0,
                "edge_gate_threshold_bps_numerator": 0.0,
                "score_before_cost_numerator": 0.0,
                "expected_edge_score_numerator": 0.0,
                "dynamic_order_adv_pct_numerator": 0.0,
                "submitted_ts": "",
                "slice_count_max": 1,
                "blocked_market_rule_order_count": 0,
                "blocked_edge_order_count": 0,
                "blocked_gate_order_count": 0,
                "statuses": set(),
                "broker_order_ids": set(),
                "dynamic_liquidity_bucket_value_map": {},
                "market_rule_statuses": set(),
            },
        )
        order_value = abs(_safe_float(row.get("order_value"), 0.0))
        status_bucket = _execution_order_status_bucket(row)
        micro = _order_execution_microstructure(row)
        bucket["order_row_count"] = int(bucket["order_row_count"]) + 1
        bucket["order_value"] = float(bucket["order_value"]) + order_value
        bucket["expected_edge_bps_numerator"] = float(bucket["expected_edge_bps_numerator"]) + order_value * _safe_float(row.get("expected_edge_bps"), 0.0)
        bucket["expected_cost_bps_numerator"] = float(bucket["expected_cost_bps_numerator"]) + order_value * _safe_float(row.get("expected_cost_bps"), 0.0)
        bucket["edge_gate_threshold_bps_numerator"] = float(bucket["edge_gate_threshold_bps_numerator"]) + order_value * _safe_float(row.get("edge_gate_threshold_bps"), 0.0)
        bucket["score_before_cost_numerator"] = float(bucket["score_before_cost_numerator"]) + order_value * _safe_float(row.get("score_before_cost"), 0.0)
        bucket["expected_edge_score_numerator"] = float(bucket["expected_edge_score_numerator"]) + order_value * _safe_float(row.get("expected_edge_score"), 0.0)
        bucket["dynamic_order_adv_pct_numerator"] = float(bucket["dynamic_order_adv_pct_numerator"]) + order_value * _safe_float(micro.get("dynamic_order_adv_pct"), 0.0)
        bucket["slice_count_max"] = max(int(bucket.get("slice_count_max", 1) or 1), int(micro.get("slice_count", 1) or 1))
        bucket["statuses"].add(status_bucket)
        if status_bucket == "BLOCKED_GATE":
            bucket["blocked_gate_order_count"] = int(bucket.get("blocked_gate_order_count", 0) or 0) + 1
        if str(row.get("status") or "").strip().upper() == "BLOCKED_MARKET_RULE":
            bucket["blocked_market_rule_order_count"] = int(bucket.get("blocked_market_rule_order_count", 0) or 0) + 1
        if status_bucket == "BLOCKED_EDGE":
            bucket["blocked_edge_order_count"] = int(bucket.get("blocked_edge_order_count", 0) or 0) + 1
        bucket_name = str(micro.get("dynamic_liquidity_bucket") or "").strip().upper()
        if bucket_name:
            bucket_value_map = dict(bucket.get("dynamic_liquidity_bucket_value_map") or {})
            bucket_value_map[bucket_name] = float(bucket_value_map.get(bucket_name, 0.0) or 0.0) + float(order_value)
            bucket["dynamic_liquidity_bucket_value_map"] = bucket_value_map
        market_rule_status = str(micro.get("market_rule_status") or "").strip().upper()
        if market_rule_status:
            cast_rule_statuses = set(bucket.get("market_rule_statuses") or set())
            cast_rule_statuses.add(market_rule_status)
            bucket["market_rule_statuses"] = cast_rule_statuses
        broker_order_id = _safe_int(row.get("broker_order_id"), 0)
        if broker_order_id > 0:
            cast_ids = set(bucket.get("broker_order_ids") or set())
            cast_ids.add(broker_order_id)
            bucket["broker_order_ids"] = cast_ids
        if status_bucket == "SUBMITTED":
            row_ts = str(row.get("ts") or "")
            current_ts = str(bucket.get("submitted_ts") or "")
            if row_ts and (not current_ts or row_ts < current_ts):
                bucket["submitted_ts"] = row_ts
        if not bucket.get("linked_snapshot_id") and str(row.get("linked_snapshot_id") or "").strip():
            bucket["linked_snapshot_id"] = str(row.get("linked_snapshot_id") or "")
            bucket["linked_snapshot_stage"] = str(row.get("linked_snapshot_stage") or "")
            bucket["linked_analysis_run_id"] = str(row.get("linked_analysis_run_id") or "")
            bucket["linked_report_dir"] = str(row.get("linked_report_dir") or "")

    parent_rows: List[Dict[str, Any]] = []
    for bucket in grouped.values():
        broker_order_ids = sorted(int(v) for v in list(bucket.get("broker_order_ids") or set()))
        order_fills: List[Dict[str, Any]] = []
        for broker_order_id in broker_order_ids:
            order_fills.extend(list(fills_by_order.get(broker_order_id, []) or []))
        order_fills.sort(key=lambda item: str(item.get("ts") or ""))
        fill_notional = 0.0
        slippage_cost_total = 0.0
        commission_total = 0.0
        slippage_samples: List[float] = []
        fill_delay_samples: List[float] = []
        for fill in order_fills:
            notional = abs(_safe_float(fill.get("qty"), 0.0)) * abs(_safe_float(fill.get("price"), 0.0))
            fill_notional += notional
            actual_slippage_bps = fill.get("actual_slippage_bps")
            if actual_slippage_bps not in (None, ""):
                slip = _safe_float(actual_slippage_bps, 0.0)
                slippage_samples.append(slip)
                slippage_cost_total += notional * slip / 10000.0
            commission_total += _safe_float(commission_by_exec.get(str(fill.get("exec_id") or "").strip()), 0.0)
            fill_delay = fill.get("fill_delay_seconds")
            if fill_delay not in (None, ""):
                fill_delay_samples.append(_safe_float(fill_delay, 0.0))
        execution_cost_total = float(slippage_cost_total + commission_total)
        realized_total_cost_bps = float(execution_cost_total / fill_notional * 10000.0) if fill_notional > 0.0 else None
        avg_actual_slippage_bps = float(slippage_cost_total / fill_notional * 10000.0) if fill_notional > 0.0 else None
        first_fill_ts = str(order_fills[0].get("ts") or "") if order_fills else ""
        last_fill_ts = str(order_fills[-1].get("ts") or "") if order_fills else ""
        first_fill_delay_seconds = (
            min(fill_delay_samples)
            if fill_delay_samples
            else _seconds_between(bucket.get("submitted_ts"), first_fill_ts)
        )
        statuses = set(bucket.get("statuses") or set())
        status_bucket = "PLANNED"
        if order_fills:
            status_bucket = "FILLED"
        elif "SUBMITTED" in statuses:
            status_bucket = "SUBMITTED"
        elif "BLOCKED_EDGE" in statuses:
            status_bucket = "BLOCKED_EDGE"
        elif "BLOCKED_GATE" in statuses:
            status_bucket = "BLOCKED_GATE"
        expected_edge_bps = (
            float(bucket.get("expected_edge_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        expected_cost_bps = (
            float(bucket.get("expected_cost_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        edge_gate_threshold_bps = (
            float(bucket.get("edge_gate_threshold_bps_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        score_before_cost = (
            float(bucket.get("score_before_cost_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        expected_edge_score = (
            float(bucket.get("expected_edge_score_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        avg_dynamic_order_adv_pct = (
            float(bucket.get("dynamic_order_adv_pct_numerator", 0.0) or 0.0) / float(bucket.get("order_value", 0.0) or 1.0)
            if float(bucket.get("order_value", 0.0) or 0.0) > 0.0 else 0.0
        )
        liquidity_bucket_value_map = dict(bucket.get("dynamic_liquidity_bucket_value_map") or {})
        dominant_liquidity_bucket = ""
        if liquidity_bucket_value_map:
            dominant_liquidity_bucket = max(
                liquidity_bucket_value_map.items(),
                key=lambda item: (float(item[1] or 0.0), str(item[0] or "")),
            )[0]
        row = {
            "portfolio_id": str(bucket.get("portfolio_id") or ""),
            "market": str(bucket.get("market") or ""),
            "run_id": str(bucket.get("run_id") or ""),
            "parent_order_key": str(bucket.get("parent_order_key") or ""),
            "symbol": str(bucket.get("symbol") or ""),
            "action": str(bucket.get("action") or ""),
            "linked_snapshot_id": str(bucket.get("linked_snapshot_id") or ""),
            "linked_snapshot_stage": str(bucket.get("linked_snapshot_stage") or ""),
            "linked_analysis_run_id": str(bucket.get("linked_analysis_run_id") or ""),
            "linked_report_dir": str(bucket.get("linked_report_dir") or ""),
            "status_bucket": status_bucket,
            "order_row_count": int(bucket.get("order_row_count", 0) or 0),
            "order_value": float(bucket.get("order_value", 0.0) or 0.0),
            "score_before_cost": float(score_before_cost),
            "expected_edge_score": float(expected_edge_score),
            "expected_edge_bps": float(expected_edge_bps),
            "expected_cost_bps": float(expected_cost_bps),
            "edge_gate_threshold_bps": float(edge_gate_threshold_bps),
            "required_edge_gap_bps": max(0.0, float(edge_gate_threshold_bps) - float(expected_edge_bps)),
            "expected_edge_value": float(float(bucket.get("order_value", 0.0) or 0.0) * float(expected_edge_bps) / 10000.0),
            "blocked_market_rule_order_count": int(bucket.get("blocked_market_rule_order_count", 0) or 0),
            "blocked_edge_order_count": int(bucket.get("blocked_edge_order_count", 0) or 0),
            "blocked_gate_order_count": int(bucket.get("blocked_gate_order_count", 0) or 0),
            "dynamic_liquidity_bucket": str(dominant_liquidity_bucket),
            "avg_dynamic_order_adv_pct": float(avg_dynamic_order_adv_pct),
            "slice_count": int(bucket.get("slice_count_max", 1) or 1),
            "market_rule_statuses": ",".join(sorted(str(item) for item in list(bucket.get("market_rule_statuses") or set()) if str(item).strip())),
            "submitted_ts": str(bucket.get("submitted_ts") or ""),
            "fill_count": int(len(order_fills)),
            "fill_notional": float(fill_notional),
            "commission_total": float(commission_total),
            "slippage_cost_total": float(slippage_cost_total),
            "execution_cost_total": float(execution_cost_total),
            "avg_actual_slippage_bps": avg_actual_slippage_bps,
            "avg_realized_total_cost_bps": realized_total_cost_bps,
            "execution_capture_bps": (
                float(expected_edge_bps) - float(realized_total_cost_bps)
                if realized_total_cost_bps is not None
                else None
            ),
            "first_fill_ts": first_fill_ts,
            "last_fill_ts": last_fill_ts,
            "first_fill_delay_seconds": first_fill_delay_seconds,
            "median_fill_delay_seconds": _median(fill_delay_samples),
        }
        for horizon_days in (5, 20, 60):
            outcome = dict(outcomes_by_snapshot.get(str(bucket.get("linked_snapshot_id") or ""), {}).get(horizon_days) or {})
            future_return_bps = (
                float(_safe_float(outcome.get("future_return"), 0.0) * 10000.0)
                if outcome else None
            )
            row[f"outcome_{horizon_days}d_future_return_bps"] = future_return_bps
            row[f"outcome_{horizon_days}d_counterfactual_edge_bps"] = (
                future_return_bps - float(expected_cost_bps)
                if future_return_bps is not None
                else None
            )
            row[f"outcome_{horizon_days}d_realized_edge_bps"] = (
                future_return_bps - float(realized_total_cost_bps)
                if future_return_bps is not None and realized_total_cost_bps is not None
                else None
            )
        row["realized_slippage_bps"] = row.get("avg_actual_slippage_bps")
        row["realized_edge_bps"] = (
            row.get("outcome_20d_realized_edge_bps")
            if row.get("outcome_20d_realized_edge_bps") not in (None, "")
            else row.get("execution_capture_bps")
        )
        parent_rows.append(row)
    parent_rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("parent_order_key") or ""),
        )
    )
    return parent_rows

def _avg_bps(rows: List[Dict[str, Any]], key: str) -> float | None:
    values = [row.get(key) for row in list(rows or []) if row.get(key) not in (None, "")]
    return _avg_defined(values)

def _build_weekly_outcome_spread_rows(
    snapshot_rows: List[Dict[str, Any]],
    outcome_rows: List[Dict[str, Any]],
    execution_parent_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    snapshots = {str(row.get("snapshot_id") or ""): row for row in _enrich_snapshot_rows(snapshot_rows) if str(row.get("snapshot_id") or "").strip()}
    status_by_snapshot: Dict[str, str] = {}
    precedence = {"FILLED": 4, "SUBMITTED": 3, "BLOCKED_EDGE": 2, "BLOCKED_GATE": 1, "PLANNED": 0}
    for row in list(execution_parent_rows or []):
        snapshot_id = str(row.get("linked_snapshot_id") or "").strip()
        status = str(row.get("status_bucket") or "").strip().upper() or "PLANNED"
        if not snapshot_id:
            continue
        current = str(status_by_snapshot.get(snapshot_id) or "").strip().upper()
        if precedence.get(status, -1) >= precedence.get(current, -1):
            status_by_snapshot[snapshot_id] = status

    deduped: Dict[tuple[str, str, str, str, int], Dict[str, Any]] = {}
    for raw in list(outcome_rows or []):
        snapshot_id = str(raw.get("snapshot_id") or "").strip()
        snapshot = dict(snapshots.get(snapshot_id) or {})
        if not snapshot:
            continue
        analysis_run_id = str(snapshot.get("analysis_run_id") or "").strip()
        symbol = str(raw.get("symbol") or snapshot.get("symbol") or "").upper().strip()
        direction = str(raw.get("direction") or snapshot.get("direction") or "LONG").upper().strip()
        horizon_days = _safe_int(raw.get("horizon_days"), 0)
        if not analysis_run_id or not symbol or horizon_days <= 0:
            continue
        enriched = dict(raw)
        enriched["analysis_run_id"] = analysis_run_id
        enriched["report_dir"] = str(snapshot.get("report_dir") or "")
        enriched["stage"] = str(snapshot.get("stage") or "")
        enriched["stage_rank"] = _safe_int(snapshot.get("stage_rank"), 0)
        enriched["stage1_rank"] = _safe_int(snapshot.get("stage1_rank"), 0)
        enriched["score"] = _safe_float(snapshot.get("score"), 0.0)
        enriched["score_before_cost"] = _safe_float(snapshot.get("score_before_cost"), _safe_float(snapshot.get("score"), 0.0))
        enriched["expected_cost_bps"] = _safe_float(snapshot.get("expected_cost_bps"), 0.0)
        enriched["expected_edge_bps"] = _safe_float(snapshot.get("expected_edge_bps"), 0.0)
        enriched["selected"] = int(_is_selected_snapshot_stage(str(snapshot.get("stage") or "")))
        enriched["execution_status"] = str(status_by_snapshot.get(snapshot_id) or "PLANNED")
        key = (
            str(raw.get("portfolio_id") or ""),
            analysis_run_id,
            symbol,
            direction,
            horizon_days,
        )
        current = dict(deduped.get(key) or {})
        if not current:
            deduped[key] = enriched
            continue
        current_priority = _candidate_snapshot_stage_priority(str(current.get("stage") or ""))
        next_priority = _candidate_snapshot_stage_priority(str(enriched.get("stage") or ""))
        if next_priority > current_priority:
            deduped[key] = enriched
            continue
        if next_priority == current_priority and _safe_int(enriched.get("stage_rank"), 10**6) < _safe_int(current.get("stage_rank"), 10**6):
            deduped[key] = enriched

    top_rank_cutoff: Dict[tuple[str, str], int] = {}
    grouped_selected: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in deduped.values():
        if int(row.get("selected", 0) or 0) != 1:
            continue
        key = (str(row.get("portfolio_id") or ""), str(row.get("analysis_run_id") or ""))
        grouped_selected.setdefault(key, []).append(row)
    for key, rows in grouped_selected.items():
        count = max(1, len(rows))
        top_rank_cutoff[key] = max(1, min(5, (count + 3) // 4))

    grouped: Dict[tuple[str, str, int], List[Dict[str, Any]]] = {}
    for row in deduped.values():
        key = (
            str(row.get("portfolio_id") or ""),
            str(row.get("market") or ""),
            _safe_int(row.get("horizon_days"), 0),
        )
        grouped.setdefault(key, []).append(row)

    def _avg_future(rows: List[Dict[str, Any]]) -> float | None:
        return _avg_defined([_safe_float(item.get("future_return"), 0.0) * 10000.0 for item in list(rows or [])])

    def _positive_rate(rows: List[Dict[str, Any]]) -> float | None:
        if not rows:
            return None
        positives = sum(1 for item in rows if _safe_float(item.get("future_return"), 0.0) > 0.0)
        return float(positives / len(rows))

    out: List[Dict[str, Any]] = []
    for (portfolio_id, market, horizon_days), rows in grouped.items():
        selected_rows = [row for row in rows if int(row.get("selected", 0) or 0) == 1]
        unselected_rows = [row for row in rows if int(row.get("selected", 0) or 0) != 1]
        top_ranked_rows = [
            row
            for row in selected_rows
            if _safe_int(row.get("stage_rank"), 0) > 0
            and _safe_int(row.get("stage_rank"), 0)
            <= int(top_rank_cutoff.get((portfolio_id, str(row.get("analysis_run_id") or "")), 1))
        ]
        executed_rows = [row for row in selected_rows if str(row.get("execution_status") or "") == "FILLED"]
        blocked_edge_rows = [row for row in selected_rows if str(row.get("execution_status") or "") == "BLOCKED_EDGE"]
        selected_avg = _avg_future(selected_rows)
        unselected_avg = _avg_future(unselected_rows)
        top_rank_avg = _avg_future(top_ranked_rows)
        executed_avg = _avg_future(executed_rows)
        blocked_edge_avg = _avg_future(blocked_edge_rows)
        out.append(
            {
                "portfolio_id": portfolio_id,
                "market": market,
                "horizon_days": horizon_days,
                "universe_sample_count": int(len(rows)),
                "selected_sample_count": int(len(selected_rows)),
                "unselected_sample_count": int(len(unselected_rows)),
                "top_ranked_sample_count": int(len(top_ranked_rows)),
                "executed_sample_count": int(len(executed_rows)),
                "blocked_edge_sample_count": int(len(blocked_edge_rows)),
                "universe_avg_future_return_bps": _avg_future(rows),
                "selected_avg_future_return_bps": selected_avg,
                "unselected_avg_future_return_bps": unselected_avg,
                "selected_spread_vs_unselected_bps": (
                    float(selected_avg - unselected_avg)
                    if selected_avg is not None and unselected_avg is not None
                    else None
                ),
                "top_ranked_avg_future_return_bps": top_rank_avg,
                "top_ranked_spread_vs_unselected_bps": (
                    float(top_rank_avg - unselected_avg)
                    if top_rank_avg is not None and unselected_avg is not None
                    else None
                ),
                "executed_avg_future_return_bps": executed_avg,
                "blocked_edge_avg_future_return_bps": blocked_edge_avg,
                "executed_spread_vs_blocked_edge_bps": (
                    float(executed_avg - blocked_edge_avg)
                    if executed_avg is not None and blocked_edge_avg is not None
                    else None
                ),
                "selected_positive_rate": _positive_rate(selected_rows),
                "unselected_positive_rate": _positive_rate(unselected_rows),
                "executed_positive_rate": _positive_rate(executed_rows),
                "blocked_edge_positive_rate": _positive_rate(blocked_edge_rows),
            }
        )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            _safe_int(row.get("horizon_days"), 0),
        )
    )
    return out

def _build_weekly_edge_realization_rows(execution_parent_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(execution_parent_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))
    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        relevant = [
            row
            for row in rows
            if str(row.get("linked_snapshot_id") or "").strip()
            or _safe_float(row.get("expected_edge_bps"), 0.0) > 0.0
            or _safe_float(row.get("score_before_cost"), 0.0) != 0.0
        ]
        if not relevant:
            continue
        weighted_order_value = sum(abs(_safe_float(row.get("order_value"), 0.0)) for row in relevant)
        filled = [row for row in relevant if str(row.get("status_bucket") or "") == "FILLED"]
        fill_notional = sum(abs(_safe_float(row.get("fill_notional"), 0.0)) for row in filled)
        edge_blocked = [row for row in relevant if str(row.get("status_bucket") or "") == "BLOCKED_EDGE"]
        output = {
            "portfolio_id": portfolio_id,
            "market": str(relevant[0].get("market") or ""),
            "candidate_parent_count": int(len(relevant)),
            "filled_parent_count": int(len(filled)),
            "blocked_edge_parent_count": int(len(edge_blocked)),
            "linked_snapshot_count": int(sum(1 for row in relevant if str(row.get("linked_snapshot_id") or "").strip())),
            "avg_score_before_cost": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("score_before_cost"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_edge_score": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_score"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_edge_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_edge_gate_threshold_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("edge_gate_threshold_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_expected_cost_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_cost_bps"), 0.0) for row in relevant) / weighted_order_value)
                if weighted_order_value > 0.0 else None
            ),
            "avg_actual_slippage_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("avg_actual_slippage_bps"), 0.0) for row in filled) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_realized_total_cost_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("avg_realized_total_cost_bps"), 0.0) for row in filled) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_execution_capture_bps": (
                float(sum(abs(_safe_float(row.get("fill_notional"), 0.0)) * _safe_float(row.get("execution_capture_bps"), 0.0) for row in filled if row.get("execution_capture_bps") not in (None, "")) / fill_notional)
                if fill_notional > 0.0 else None
            ),
            "avg_fill_delay_seconds": _avg_defined([row.get("first_fill_delay_seconds") for row in filled if row.get("first_fill_delay_seconds") not in (None, "")]),
            "median_fill_delay_seconds": _median([row.get("first_fill_delay_seconds") for row in filled if row.get("first_fill_delay_seconds") not in (None, "")]),
        }
        for horizon_days in (5, 20, 60):
            future_key = f"outcome_{horizon_days}d_future_return_bps"
            edge_key = f"outcome_{horizon_days}d_realized_edge_bps"
            samples = [row for row in filled if row.get(future_key) not in (None, "")]
            output[f"matured_{horizon_days}d_sample_count"] = int(len(samples))
            output[f"matured_{horizon_days}d_avg_future_return_bps"] = _avg_bps(samples, future_key)
            output[f"matured_{horizon_days}d_avg_realized_edge_bps"] = _avg_bps(samples, edge_key)
        out.append(output)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out

def _build_weekly_blocked_edge_attribution_rows(execution_parent_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total_value_by_portfolio: Dict[str, float] = {}
    for row in list(execution_parent_rows or []):
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        total_value_by_portfolio[portfolio_id] = float(total_value_by_portfolio.get(portfolio_id, 0.0) or 0.0) + abs(_safe_float(row.get("order_value"), 0.0))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in list(execution_parent_rows or []):
        if str(row.get("status_bucket") or "") != "BLOCKED_EDGE":
            continue
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            grouped.setdefault(portfolio_id, []).append(dict(row))

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in grouped.items():
        blocked_value = sum(abs(_safe_float(row.get("order_value"), 0.0)) for row in rows)
        total_value = float(total_value_by_portfolio.get(portfolio_id, 0.0) or 0.0)
        output = {
            "portfolio_id": portfolio_id,
            "market": str(rows[0].get("market") or ""),
            "blocked_edge_parent_count": int(len(rows)),
            "blocked_edge_order_value": float(blocked_value),
            "blocked_edge_weight": float(blocked_value / total_value) if total_value > 0.0 else 0.0,
            "avg_expected_edge_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("expected_edge_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "avg_edge_gate_threshold_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("edge_gate_threshold_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "avg_required_gap_bps": (
                float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("required_edge_gap_bps"), 0.0) for row in rows) / blocked_value)
                if blocked_value > 0.0 else None
            ),
            "blocked_expected_edge_value": float(sum(_safe_float(row.get("expected_edge_value"), 0.0) for row in rows)),
            "blocked_required_gap_value": float(sum(abs(_safe_float(row.get("order_value"), 0.0)) * _safe_float(row.get("required_edge_gap_bps"), 0.0) / 10000.0 for row in rows)),
        }
        for horizon_days in (5, 20, 60):
            future_key = f"outcome_{horizon_days}d_future_return_bps"
            edge_key = f"outcome_{horizon_days}d_counterfactual_edge_bps"
            samples = [row for row in rows if row.get(future_key) not in (None, "")]
            output[f"matured_{horizon_days}d_sample_count"] = int(len(samples))
            output[f"matured_{horizon_days}d_avg_future_return_bps"] = _avg_bps(samples, future_key)
            output[f"matured_{horizon_days}d_avg_counterfactual_edge_bps"] = _avg_bps(samples, edge_key)
        out.append(output)
    out.sort(key=lambda row: (str(row.get("market") or ""), str(row.get("portfolio_id") or "")))
    return out
