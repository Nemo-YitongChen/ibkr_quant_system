from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _cfg_value(cfg: Any, key: str, default: Any = None) -> Any:
    if isinstance(cfg, dict):
        return cfg.get(key, default)
    return getattr(cfg, key, default)


def _row_value(row: Dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return default


def _status_token(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _status_to_reason_key(row: Dict[str, Any]) -> str:
    status = str(row.get("status") or "").strip().upper()
    reason = str(row.get("reason") or "").strip().lower()
    market_rule = str(row.get("market_rule_status") or "").strip().upper()
    quality = str(row.get("quality_status") or "").strip().upper()
    opportunity = str(row.get("opportunity_status") or "").strip().upper()
    manual = str(row.get("manual_review_status") or "").strip().upper()
    shadow = str(row.get("shadow_review_status") or "").strip().upper()

    if status == "BLOCKED_PENDING_BROKER_ORDER":
        return "BLOCKED_PENDING_BROKER_ORDER"
    if status == "BLOCKED_MARKET_RULE" or market_rule.startswith("BLOCKED"):
        return market_rule or "BLOCKED_MARKET_RULE"
    if str(row.get("edge_gate_status") or "").upper() == "BLOCKED" or "edge_gate" in reason:
        return "BLOCKED_EDGE"
    if status == "BLOCKED_LIQUIDITY" or "liquidity" in reason or "adv_" in reason:
        return "BLOCKED_LIQUIDITY"
    if status == "BLOCKED_FRACTIONAL_API" or "fractional_api" in reason:
        return "BLOCKED_FRACTIONAL_API"
    if quality.startswith("BLOCK") or "quality" in reason:
        return "BLOCKED_DATA_QUALITY"
    if opportunity.startswith("BLOCK") or "opportunity" in reason:
        return "BLOCKED_OPPORTUNITY"
    if shadow == "REVIEW_REQUIRED" or "shadow_ml" in reason:
        return "REVIEW_SHADOW_ML"
    if manual == "REVIEW_REQUIRED" or "manual_review" in reason:
        return "REVIEW_MANUAL"
    if status.startswith("DEFERRED_RISK") or "risk_alert" in reason:
        return "DEFERRED_RISK_ALERT"
    if "market_structure" in reason:
        return "REVIEW_MARKET_STRUCTURE"
    if status:
        return status
    return "UNKNOWN_BLOCK"


def _sample_symbols(rows: Iterable[Dict[str, Any]], *, limit: int = 8) -> str:
    symbols: List[str] = []
    seen = set()
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= int(limit):
            break
    return ",".join(symbols)


def _order_value_sum(rows: Iterable[Dict[str, Any]]) -> float:
    return float(sum(abs(_to_float(row.get("order_value"), 0.0)) for row in rows or []))


def _whole_share_sample_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sample_rows: List[Dict[str, Any]] = []
    for row in rows or []:
        reason = str(row.get("reason") or "").lower()
        is_shadow_sample = (
            str(row.get("shadow_review_status") or "").strip().upper() == "SAMPLE_COLLECTION"
            or "shadow_ml_sample_collection" in reason
        )
        is_missing_opportunity_sample = (
            bool(row.get("whole_share_missing_opportunity_paper_sample", False))
            or "whole_share_opportunity_sample" in reason
        )
        if not is_shadow_sample and not is_missing_opportunity_sample:
            continue
        if not bool(row.get("whole_share_preferred_buy_override", False)):
            continue
        sample_rows.append(row)
    return sample_rows


NON_CRITICAL_SUBMIT_BLOCK_REASONS = {"BLOCKED_PENDING_BROKER_ORDER"}


def _diagnostic_status(condition: bool, *, warning: bool = False) -> str:
    if condition:
        return "WARNING" if warning else "PASS"
    return "BLOCKED"


def build_no_order_diagnostics(
    *,
    market: str,
    portfolio_id: str,
    report_dir: str,
    submitted: bool,
    broker_equity: float,
    broker_cash: float,
    target_equity: float,
    target_weights: Dict[str, float],
    candidate_rows: List[Dict[str, Any]],
    plan_rows: List[Dict[str, Any]],
    raw_order_rows: List[Dict[str, Any]],
    blocked_rows: List[Dict[str, Any]],
    order_rows: List[Dict[str, Any]],
    execution_cfg: Any,
    account_profile: Dict[str, Any] | None = None,
    execution_session_bucket: str = "",
    execution_session_label: str = "",
    market_open_for_submit: bool = True,
) -> Dict[str, Any]:
    """Explain why a portfolio did or did not produce executable orders."""

    generated_at = datetime.now(timezone.utc).isoformat()
    market_code = str(market or "").upper()
    session_bucket = str(execution_session_bucket or "").upper()
    session_label = str(execution_session_label or "")
    submit_session_open = bool(market_open_for_submit)
    target_weight_count = int(sum(1 for weight in target_weights.values() if abs(_to_float(weight)) > 1e-9))
    raw_order_count = int(len(raw_order_rows or []))
    blocked_count = int(len(blocked_rows or []))
    order_count = int(len(order_rows or []))
    submitted_statuses = {"SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT", "APIPENDING", "FILLED", "PARTIAL"}
    submitted_order_count = int(
        sum(1 for row in order_rows if _status_token(row.get("status") or row.get("broker_order_status")) in submitted_statuses)
    )
    cancelled_order_count = int(
        sum(1 for row in order_rows if "CANCEL" in _status_token(row.get("status") or row.get("broker_order_status")))
    )
    error_order_count = int(
        sum(1 for row in order_rows if _status_token(row.get("status") or row.get("broker_order_status")).startswith("ERROR"))
    )

    min_trade_value = max(0.0, _to_float(_cfg_value(execution_cfg, "min_trade_value"), 0.0))
    max_order_value_pct = max(0.0, _to_float(_cfg_value(execution_cfg, "max_order_value_pct"), 0.0))
    account_equity_cap = max(0.0, _to_float(_cfg_value(execution_cfg, "account_equity_cap"), 0.0))
    account_allocation_pct = max(0.0, min(1.0, _to_float(_cfg_value(execution_cfg, "account_allocation_pct"), 0.0)))
    cash_buffer_floor = max(0.0, _to_float(_cfg_value(execution_cfg, "cash_buffer_floor"), 0.0))
    min_cash_buffer_pct = max(0.0, _to_float(_cfg_value(execution_cfg, "min_cash_buffer_pct"), 0.0))
    allow_fractional_qty = bool(_cfg_value(execution_cfg, "allow_fractional_qty", False))
    fractional_order_api_enabled = bool(_cfg_value(execution_cfg, "fractional_order_api_enabled", False))
    max_orders_per_run = max(0, _to_int(_cfg_value(execution_cfg, "max_orders_per_run"), 0))
    reserve_cash = max(cash_buffer_floor, float(broker_equity or 0.0) * min_cash_buffer_pct)
    cash_after_buffer = max(0.0, float(broker_cash or 0.0) - reserve_cash)
    max_order_value = float(broker_equity or 0.0) * max_order_value_pct
    account_target_capital = float(broker_equity or 0.0) * account_allocation_pct

    funnel_rows = [
        {"stage": "candidate_rows", "count": int(len(candidate_rows or [])), "status": "PASS" if candidate_rows else "BLOCKED"},
        {"stage": "plan_rows", "count": int(len(plan_rows or [])), "status": "PASS" if plan_rows else "BLOCKED"},
        {"stage": "target_weight_symbols", "count": target_weight_count, "status": "PASS" if target_weight_count else "BLOCKED"},
        {"stage": "raw_order_rows", "count": raw_order_count, "status": "PASS" if raw_order_count else "BLOCKED"},
        {"stage": "blocked_order_rows", "count": blocked_count, "status": "INFO" if blocked_count else "PASS"},
        {"stage": "executable_order_rows", "count": order_count, "status": "PASS" if order_count else "BLOCKED"},
        {"stage": "submitted_order_rows", "count": submitted_order_count, "status": "PASS" if submitted_order_count else ("INFO" if not submitted else "BLOCKED")},
    ]

    capital_rows = [
        {
            "check": "cash_after_buffer_vs_min_trade",
            "value": round(cash_after_buffer, 6),
            "threshold": round(min_trade_value, 6),
            "status": _diagnostic_status(cash_after_buffer + 1e-9 >= min_trade_value),
            "note": "cash available after required reserve must cover at least one minimum trade",
        },
        {
            "check": "target_equity_vs_min_trade",
            "value": round(float(target_equity or 0.0), 6),
            "threshold": round(min_trade_value, 6),
            "status": _diagnostic_status(float(target_equity or 0.0) + 1e-9 >= min_trade_value),
            "note": "allocated execution capital must cover at least one minimum trade",
        },
        {
            "check": "max_order_value_vs_min_trade",
            "value": round(max_order_value, 6),
            "threshold": round(min_trade_value, 6),
            "status": _diagnostic_status(max_order_value + 1e-9 >= min_trade_value),
            "note": "max_order_value_pct must not make every order smaller than min_trade_value",
        },
        {
            "check": "fractional_qty_enabled",
            "value": int(allow_fractional_qty),
            "threshold": 0,
            "status": "WARNING" if allow_fractional_qty and not fractional_order_api_enabled else "INFO",
            "note": "small US paper accounts now prefer whole-share tradable ETFs because IBKR API can reject fractional-sized orders",
        },
        {
            "check": "fractional_order_api_enabled",
            "value": int(fractional_order_api_enabled),
            "threshold": 1,
            "status": "PASS" if fractional_order_api_enabled else ("WARNING" if allow_fractional_qty else "INFO"),
            "note": "IBKR API may reject fractional-sized paper/live orders; whole-share tradable ETFs are preferred until verified",
        },
    ]

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in blocked_rows or []:
        grouped[_status_to_reason_key(row)].append(row)
    blocking_rows: List[Dict[str, Any]] = []
    for reason_key, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        blocking_rows.append(
            {
                "reason_key": reason_key,
                "count": int(len(rows)),
                "order_value": round(sum(abs(_to_float(row.get("order_value"), 0.0)) for row in rows), 6),
                "sample_symbols": _sample_symbols(rows),
                "sample_reason": str(_row_value(rows[0], "user_reason", "market_rule_reason", "edge_gate_reason", "quality_reason", "opportunity_reason", "manual_review_reason", "reason")),
            }
        )

    submit_blocking_rows = [
        row for row in blocking_rows if str(row.get("reason_key") or "").upper() not in NON_CRITICAL_SUBMIT_BLOCK_REASONS
    ]
    submit_blocking_count = int(sum(_to_int(row.get("count"), 0) for row in submit_blocking_rows))
    allowed_edge_margin_avg_bps = _avg_edge_margin(list(order_rows or []))
    planned_order_value = _order_value_sum(order_rows or [])
    planned_order_symbols = _sample_symbols(order_rows or [])
    whole_share_sample_rows = _whole_share_sample_rows(order_rows or [])
    whole_share_sample_count = int(len(whole_share_sample_rows))
    whole_share_sample_order_value = _order_value_sum(whole_share_sample_rows)
    whole_share_sample_avg_edge_margin_bps = _avg_edge_margin(whole_share_sample_rows)
    paper_submit_ready = bool(order_count > 0 and submit_blocking_count <= 0 and not submitted and submit_session_open)
    if submitted and submitted_order_count > 0:
        paper_submit_readiness_status = "SUBMITTED"
    elif order_count > 0 and submit_blocking_count <= 0 and not submit_session_open:
        paper_submit_readiness_status = "MARKET_CLOSED"
    elif paper_submit_ready:
        paper_submit_readiness_status = "READY"
    elif order_count > 0:
        paper_submit_readiness_status = "NEEDS_REVIEW"
    else:
        paper_submit_readiness_status = "BLOCKED"

    if not candidate_rows:
        primary_reason = "NO_CANDIDATES"
        primary_action = "refresh_report_inputs"
    elif target_weight_count <= 0:
        primary_reason = "NO_TARGET_WEIGHTS"
        primary_action = "review_strategy_scoring_and_risk_overlay"
    elif raw_order_count <= 0 and max_order_value + 1e-9 < min_trade_value:
        primary_reason = "MAX_ORDER_VALUE_BELOW_MIN_TRADE"
        primary_action = "lower_small_account_min_trade_or_raise_max_order_pct"
    elif raw_order_count <= 0 and float(target_equity or 0.0) + 1e-9 < min_trade_value:
        primary_reason = "TARGET_EQUITY_BELOW_MIN_TRADE"
        primary_action = "lower_cash_buffer_or_min_trade_for_small_account"
    elif raw_order_count <= 0:
        primary_reason = "ORDER_GENERATION_ZERO_QTY"
        primary_action = "enable_fractional_or_adjust_lot_size_and_target_weights"
    elif order_count <= 0 and blocking_rows:
        primary_reason = str(blocking_rows[0]["reason_key"])
        primary_action = "review_top_blocking_gate"
    elif order_count > 0 and submit_blocking_count > 0:
        primary_reason = "PARTIAL_ORDERS_PLANNED_WITH_BLOCKS"
        primary_action = "review_remaining_blocked_orders_before_submit"
    elif order_count > 0 and submit_blocking_count <= 0 and not submitted and not submit_session_open:
        primary_reason = "MARKET_CLOSED_FOR_SUBMIT"
        primary_action = "wait_for_regular_session_or_enable_overnight_config"
    elif order_count > 0 and not submitted:
        primary_reason = "ORDERS_PLANNED_NOT_SUBMITTED"
        primary_action = "run_paper_submit_after_readiness_passes"
    elif order_count > 0 and submitted and submitted_order_count > 0 and (cancelled_order_count > 0 or error_order_count > 0):
        primary_reason = "PAPER_ORDERS_PARTIAL_BROKER_ACK"
        primary_action = "fix_rejected_orders_and_monitor_accepted_fills"
    elif order_count > 0 and submitted and submitted_order_count > 0:
        primary_reason = "PAPER_ORDERS_SUBMITTED"
        primary_action = "monitor_fills_slippage_and_post_cost_edge"
    elif order_count > 0 and submitted:
        primary_reason = "SUBMIT_REQUESTED_NO_BROKER_ACK"
        primary_action = "inspect_broker_submit_errors_and_order_audit"
    else:
        primary_reason = "UNKNOWN"
        primary_action = "inspect_execution_plan"

    diagnostic_rows: List[Dict[str, Any]] = []
    for row in funnel_rows:
        diagnostic_rows.append({"section": "funnel", **row})
    for row in capital_rows:
        diagnostic_rows.append({"section": "capital", **row})
    for row in blocking_rows:
        diagnostic_rows.append({"section": "blocking", **row})
    paper_submit_readiness_rows = [
        {
            "check": "executable_orders_available",
            "value": int(order_count),
            "threshold": 1,
            "status": "PASS" if order_count > 0 else "BLOCKED",
            "note": "paper submit needs at least one executable order row",
        },
        {
            "check": "blocked_orders_clear",
            "value": int(submit_blocking_count),
            "threshold": 0,
            "status": "PASS" if submit_blocking_count <= 0 else "BLOCKED",
            "note": "hard blocked gate rows must be clear before paper submit; existing pending broker orders are duplicate-prevention rows",
        },
        {
            "check": "whole_share_sample_collection",
            "value": int(whole_share_sample_count),
            "threshold": 1,
            "status": "PASS" if whole_share_sample_count > 0 else "INFO",
            "note": "whole-share ETF paper samples are allowed only when risk and edge gates remain active",
        },
        {
            "check": "market_open_for_submit",
            "value": int(submit_session_open),
            "threshold": 1,
            "status": "PASS" if submit_session_open else "BLOCKED",
            "note": (
                f"session={session_bucket or '-'} label={session_label or '-'}; "
                "paper submit is blocked outside the configured regular session unless overnight execution is explicitly enabled"
            ),
        },
        {
            "check": "paper_submit_state",
            "value": paper_submit_readiness_status,
            "threshold": "READY",
            "status": (
                "PASS"
                if paper_submit_readiness_status in {"READY", "SUBMITTED"}
                else ("BLOCKED" if paper_submit_readiness_status == "MARKET_CLOSED" else ("INFO" if order_count > 0 else "BLOCKED"))
            ),
            "note": "READY means the dry-run has planned orders; operator must still choose --submit",
        },
    ]
    for row in paper_submit_readiness_rows:
        diagnostic_rows.append({"section": "paper_submit_readiness", **row})
    paper_sample_collection_rows = [
        {
            "sample_type": "whole_share_etf_shadow_ml",
            "count": int(whole_share_sample_count),
            "order_value": round(whole_share_sample_order_value, 6),
            "sample_symbols": _sample_symbols(whole_share_sample_rows),
            "avg_edge_margin_bps": round(whole_share_sample_avg_edge_margin_bps, 6),
            "status": "PASS" if whole_share_sample_count > 0 else "INFO",
        }
    ]
    for row in paper_sample_collection_rows:
        diagnostic_rows.append({"section": "paper_sample_collection", **row})

    payload = {
        "generated_at": generated_at,
        "market": market_code,
        "portfolio_id": str(portfolio_id or ""),
        "report_dir": str(report_dir or ""),
        "submitted": bool(submitted),
        "broker_equity": float(broker_equity or 0.0),
        "broker_cash": float(broker_cash or 0.0),
        "account_equity_cap": float(account_equity_cap),
        "account_profile": dict(account_profile or {}),
        "candidate_count": int(len(candidate_rows or [])),
        "plan_count": int(len(plan_rows or [])),
        "target_weight_count": target_weight_count,
        "raw_order_count": raw_order_count,
        "blocked_order_count": blocked_count,
        "submit_blocking_order_count": int(submit_blocking_count),
        "order_count": order_count,
        "submitted_order_count": submitted_order_count,
        "cancelled_order_count": cancelled_order_count,
        "error_order_count": error_order_count,
        "min_trade_value": float(min_trade_value),
        "max_order_value": float(max_order_value),
        "target_equity": float(target_equity or 0.0),
        "reserve_cash": float(reserve_cash),
        "cash_after_buffer": float(cash_after_buffer),
        "account_target_capital": float(account_target_capital),
        "allow_fractional_qty": bool(allow_fractional_qty),
        "fractional_order_api_enabled": bool(fractional_order_api_enabled),
        "max_orders_per_run": int(max_orders_per_run),
        "execution_session_bucket": str(session_bucket),
        "execution_session_label": str(session_label),
        "market_open_for_submit": bool(submit_session_open),
        "allowed_edge_margin_avg_bps": float(allowed_edge_margin_avg_bps),
        "planned_order_value": float(planned_order_value),
        "planned_order_symbols": str(planned_order_symbols),
        "paper_submit_ready": bool(paper_submit_ready),
        "paper_submit_readiness_status": str(paper_submit_readiness_status),
        "whole_share_sample_collection_count": int(whole_share_sample_count),
        "whole_share_sample_collection_symbols": _sample_symbols(whole_share_sample_rows),
        "whole_share_sample_collection_order_value": float(whole_share_sample_order_value),
        "whole_share_sample_collection_avg_edge_margin_bps": float(whole_share_sample_avg_edge_margin_bps),
        "primary_no_order_reason": primary_reason,
        "primary_action": primary_action,
        "funnel_rows": funnel_rows,
        "capital_constraint_rows": capital_rows,
        "blocking_reason_rows": blocking_rows,
        "submit_blocking_reason_rows": submit_blocking_rows,
        "paper_submit_readiness_rows": paper_submit_readiness_rows,
        "paper_sample_collection_rows": paper_sample_collection_rows,
        "diagnostic_rows": diagnostic_rows,
    }
    payload["progression_assessment"] = build_owner_progression_assessment(payload)
    return payload


def _avg_edge_margin(rows: List[Dict[str, Any]]) -> float:
    values: List[float] = []
    for row in rows:
        expected_edge = _to_float(row.get("expected_edge_bps"), 0.0)
        threshold = _to_float(row.get("edge_gate_threshold_bps"), _to_float(row.get("expected_cost_bps"), 0.0))
        values.append(float(expected_edge - threshold))
    return float(sum(values) / len(values)) if values else 0.0


def build_owner_progression_assessment(no_order_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Map P0-P6 into explicit owner-facing readiness rows."""

    order_count = _to_int(no_order_payload.get("order_count"), 0)
    submitted_order_count = _to_int(no_order_payload.get("submitted_order_count"), 0)
    blocked_count = _to_int(no_order_payload.get("blocked_order_count"), 0)
    broker_equity = _to_float(no_order_payload.get("broker_equity"), 0.0)
    min_trade_value = _to_float(no_order_payload.get("min_trade_value"), 0.0)
    max_order_value = _to_float(no_order_payload.get("max_order_value"), 0.0)
    target_equity = _to_float(no_order_payload.get("target_equity"), 0.0)
    allow_fractional_qty = bool(no_order_payload.get("allow_fractional_qty", False))
    primary_reason = str(no_order_payload.get("primary_no_order_reason") or "")
    blocking_rows = list(no_order_payload.get("blocking_reason_rows") or [])
    paper_submit_ready = bool(no_order_payload.get("paper_submit_ready", False))

    small_account_ready = (
        broker_equity > 0.0
        and min_trade_value > 0.0
        and max_order_value + 1e-9 >= min_trade_value
        and target_equity + 1e-9 >= min_trade_value
    )
    edge_margin = _to_float(no_order_payload.get("allowed_edge_margin_avg_bps"), 0.0)
    rows = [
        {
            "step": "P0",
            "name": "no_order_diagnostics",
            "status": "PASS",
            "evidence": primary_reason,
            "next_action": str(no_order_payload.get("primary_action") or ""),
        },
        {
            "step": "P1",
            "name": "small_account_capital_profile",
            "status": "PASS" if small_account_ready else "BLOCKED",
            "evidence": f"equity={broker_equity:.2f} min_trade={min_trade_value:.2f} max_order={max_order_value:.2f} target_equity={target_equity:.2f} fractional={int(allow_fractional_qty)}",
            "next_action": "use small-account whole-share ETF/cash profile" if not small_account_ready else "keep small-account whole-share limits",
        },
        {
            "step": "P2",
            "name": "paper_order_activation",
            "status": "PASS" if submitted_order_count > 0 else ("WARNING" if paper_submit_ready else ("INFO" if order_count > 0 else "BLOCKED")),
            "evidence": f"orders={order_count} submitted_orders={submitted_order_count}",
            "next_action": (
                str(no_order_payload.get("primary_action") or "")
                if order_count > 0 and submitted_order_count <= 0 and not paper_submit_ready
                else ("submit paper after readiness" if order_count > 0 and submitted_order_count <= 0 else "fix no-order blocking reason")
            ),
        },
        {
            "step": "P3",
            "name": "post_cost_edge_gate",
            "status": "PASS" if order_count > 0 else "INSUFFICIENT_SAMPLE",
            "evidence": f"avg_edge_margin_bps={edge_margin:.2f}",
            "next_action": "collect fill/outcome evidence before loosening gates",
        },
        {
            "step": "P4",
            "name": "micro_live_acceptance",
            "status": "BLOCKED",
            "evidence": "requires multiple weeks of paper submitted/fill/post-cost evidence",
            "next_action": "do not enable live submit yet",
        },
        {
            "step": "P5",
            "name": "live_safety_controls",
            "status": "PASS",
            "evidence": "live submit remains disabled by default",
            "next_action": "keep live blocked until P4 acceptance passes",
        },
        {
            "step": "P6",
            "name": "investment_state_assessment",
            "status": "PAPER_BLOCKED" if order_count <= 0 else ("PAPER_SUBMITTED" if submitted_order_count > 0 else "PAPER_PLANNED"),
            "evidence": f"primary_reason={primary_reason} blocked_gates={len(blocking_rows)}",
            "next_action": str(no_order_payload.get("primary_action") or ""),
        },
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": rows[-1]["status"],
        "primary_no_order_reason": primary_reason,
        "rows": rows,
        "open_blocker_count": int(sum(1 for row in rows if str(row.get("status")) == "BLOCKED")),
    }
