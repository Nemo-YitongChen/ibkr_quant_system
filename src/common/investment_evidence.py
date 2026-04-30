from __future__ import annotations

from typing import Any, Dict, Iterable, List

from .markets import resolve_market_code


EVIDENCE_COLUMNS = [
    "week",
    "market",
    "portfolio_id",
    "symbol",
    "decision_ts",
    "decision_source",
    "run_id",
    "parent_order_key",
    "candidate_snapshot_id",
    "candidate_stage",
    "candidate_only_flag",
    "signal_score",
    "expected_edge_bps",
    "required_edge_bps",
    "edge_gate_threshold_bps",
    "expected_cost_bps",
    "expected_post_cost_edge_bps",
    "gate_status",
    "blocked_flag",
    "allowed_flag",
    "blocked_reason",
    "block_reason",
    "blocked_market_rule_order_count",
    "blocked_edge_order_count",
    "blocked_gate_order_count",
    "dynamic_liquidity_bucket",
    "dynamic_order_adv_pct",
    "slice_count",
    "adv_participation_pct",
    "risk_market_profile_budget_weight_delta",
    "risk_throttle_weight_delta",
    "risk_recovery_weight_credit",
    "strategy_control_weight_delta",
    "risk_overlay_weight_delta",
    "execution_gate_blocked_weight",
    "planned_order_value",
    "filled_order_value",
    "realized_slippage_bps",
    "fill_delay_sec",
    "first_fill_delay_seconds",
    "realized_edge_bps",
    "realized_edge_delta_bps",
    "outcome_5d",
    "outcome_20d",
    "outcome_60d",
    "outcome_5d_bps",
    "outcome_20d_bps",
    "outcome_60d_bps",
    "join_quality",
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value) != 0.0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _avg_defined(values: Iterable[Any]) -> float | None:
    floats: List[float] = []
    for value in values:
        if value in (None, ""):
            continue
        try:
            floats.append(float(value))
        except (TypeError, ValueError):
            continue
    if not floats:
        return None
    return float(sum(floats) / len(floats))


def _avg_from_rows(rows: List[Dict[str, Any]], key: str) -> float | None:
    return _avg_defined(row.get(key) for row in rows)


def normalize_evidence_row(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    row = {column: "" for column in EVIDENCE_COLUMNS}
    row.update(dict(raw or {}))
    return row


def evidence_block_reason(row: Dict[str, Any]) -> str:
    status = str(row.get("decision_status") or row.get("gate_status") or "").strip().upper()
    if _safe_int(row.get("blocked_market_rule_order_count"), 0) > 0 or status == "BLOCKED_MARKET_RULE":
        return "MARKET_RULE_GATE"
    if _safe_int(row.get("blocked_edge_order_count"), 0) > 0 or status == "BLOCKED_EDGE":
        return "EDGE_GATE"
    if _safe_int(row.get("blocked_gate_order_count"), 0) > 0 or status.startswith("BLOCKED"):
        return "EXECUTION_GATE"
    if status in {"FILLED", "PARTIAL_FILLED"} or _safe_float(row.get("fill_notional"), 0.0) > 0.0:
        return "ALLOWED_FILLED"
    if status in {"SUBMITTED", "ALLOWED"}:
        return "ALLOWED_UNFILLED"
    existing = str(row.get("block_reason") or row.get("blocked_reason") or "").strip().upper()
    return existing or status or "UNKNOWN"


def is_blocked_evidence_row(row: Dict[str, Any]) -> bool:
    if _truthy_flag(row.get("blocked_flag")):
        return True
    return evidence_block_reason(row) in {"MARKET_RULE_GATE", "EDGE_GATE", "EXECUTION_GATE"}


def is_allowed_evidence_row(row: Dict[str, Any]) -> bool:
    if _truthy_flag(row.get("allowed_flag")):
        return True
    return not is_blocked_evidence_row(row) and evidence_block_reason(row).startswith("ALLOWED")


def _looks_like_unified_evidence_row(row: Dict[str, Any]) -> bool:
    return any(key in row for key in ("block_reason", "blocked_reason", "allowed_flag", "blocked_flag")) and any(
        key in row for key in ("planned_order_value", "filled_order_value", "expected_post_cost_edge_bps")
    )


def build_unified_evidence_rows(decision_evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw in list(decision_evidence_rows or []):
        row = dict(raw or {})
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if not portfolio_id:
            continue
        if _looks_like_unified_evidence_row(row):
            normalized = normalize_evidence_row(row)
            normalized["market"] = resolve_market_code(str(normalized.get("market") or ""))
            normalized["blocked_flag"] = int(is_blocked_evidence_row(normalized))
            normalized["allowed_flag"] = int(is_allowed_evidence_row(normalized))
            normalized["block_reason"] = evidence_block_reason(normalized)
            normalized["blocked_reason"] = normalized["block_reason"]
            out.append(normalized)
            continue

        block_reason = evidence_block_reason(row)
        blocked_flag = is_blocked_evidence_row(row)
        allowed_flag = is_allowed_evidence_row(row)
        expected_edge = _safe_float(row.get("expected_edge_bps"), 0.0)
        expected_cost = _safe_float(row.get("expected_cost_bps"), 0.0)
        realized_edge = row.get("realized_edge_bps")
        realized_edge_delta = None
        if realized_edge not in (None, ""):
            realized_edge_delta = _safe_float(realized_edge, 0.0) - (expected_edge - expected_cost)
        out.append(
            normalize_evidence_row(
                {
                    "market": resolve_market_code(str(row.get("market") or "")),
                    "portfolio_id": portfolio_id,
                    "decision_source": str(row.get("decision_source") or "execution_parent"),
                    "run_id": str(row.get("run_id") or ""),
                    "parent_order_key": str(row.get("parent_order_key") or ""),
                    "candidate_snapshot_id": str(row.get("candidate_snapshot_id") or ""),
                    "candidate_stage": str(row.get("candidate_stage") or ""),
                    "candidate_only_flag": int(row.get("candidate_only_flag", 0) or 0),
                    "join_quality": str(row.get("join_quality") or ""),
                    "symbol": str(row.get("symbol") or ""),
                    "action": str(row.get("action") or ""),
                    "decision_status": str(row.get("decision_status") or ""),
                    "allowed_flag": int(allowed_flag),
                    "blocked_flag": int(blocked_flag),
                    "block_reason": block_reason,
                    "blocked_reason": block_reason,
                    "signal_score": _safe_float(row.get("signal_score"), 0.0),
                    "expected_edge_bps": expected_edge,
                    "expected_cost_bps": expected_cost,
                    "expected_post_cost_edge_bps": expected_edge - expected_cost,
                    "edge_gate_threshold_bps": _safe_float(row.get("edge_gate_threshold_bps"), 0.0),
                    "required_edge_gap_bps": _safe_float(row.get("required_edge_gap_bps"), 0.0),
                    "blocked_market_rule_order_count": _safe_int(row.get("blocked_market_rule_order_count"), 0),
                    "blocked_edge_order_count": _safe_int(row.get("blocked_edge_order_count"), 0),
                    "blocked_gate_order_count": _safe_int(row.get("blocked_gate_order_count"), 0),
                    "dynamic_liquidity_bucket": str(row.get("dynamic_liquidity_bucket") or ""),
                    "dynamic_order_adv_pct": _safe_float(row.get("dynamic_order_adv_pct"), 0.0),
                    "slice_count": _safe_int(row.get("slice_count"), 0),
                    "risk_market_profile_budget_weight_delta": _safe_float(
                        row.get("risk_market_profile_budget_weight_delta"), 0.0
                    ),
                    "risk_throttle_weight_delta": _safe_float(row.get("risk_throttle_weight_delta"), 0.0),
                    "risk_recovery_weight_credit": _safe_float(row.get("risk_recovery_weight_credit"), 0.0),
                    "strategy_control_weight_delta": _safe_float(row.get("strategy_control_weight_delta"), 0.0),
                    "risk_overlay_weight_delta": _safe_float(row.get("risk_overlay_weight_delta"), 0.0),
                    "execution_gate_blocked_weight": _safe_float(row.get("execution_gate_blocked_weight"), 0.0),
                    "planned_order_value": _safe_float(row.get("order_value"), 0.0),
                    "filled_order_value": _safe_float(row.get("fill_notional"), 0.0),
                    "realized_slippage_bps": row.get("realized_slippage_bps"),
                    "realized_edge_bps": realized_edge,
                    "realized_edge_delta_bps": realized_edge_delta,
                    "first_fill_delay_seconds": row.get("first_fill_delay_seconds"),
                    "fill_delay_sec": row.get("first_fill_delay_seconds"),
                    "outcome_5d": row.get("outcome_5d_bps"),
                    "outcome_20d": row.get("outcome_20d_bps"),
                    "outcome_60d": row.get("outcome_60d_bps"),
                    "outcome_5d_bps": row.get("outcome_5d_bps"),
                    "outcome_20d_bps": row.get("outcome_20d_bps"),
                    "outcome_60d_bps": row.get("outcome_60d_bps"),
                }
            )
        )
    out.sort(
        key=lambda item: (
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
            str(item.get("blocked_flag") or ""),
            str(item.get("symbol") or ""),
        )
    )
    return out


def build_blocked_vs_allowed_expost_review(evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unified_rows = build_unified_evidence_rows(evidence_rows)
    grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in unified_rows:
        market = resolve_market_code(str(row.get("market") or ""))
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if market and portfolio_id:
            grouped.setdefault((market, portfolio_id), []).append(row)

    out: List[Dict[str, Any]] = []
    for (market, portfolio_id), rows in grouped.items():
        allowed_rows = [row for row in rows if _safe_int(row.get("allowed_flag"), 0) > 0]
        blocked_by_reason: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            if _safe_int(row.get("blocked_flag"), 0) <= 0:
                continue
            reason = str(row.get("block_reason") or row.get("blocked_reason") or "UNKNOWN").strip().upper() or "UNKNOWN"
            blocked_by_reason.setdefault(reason, []).append(row)
        for block_reason, blocked_rows in blocked_by_reason.items():
            allowed_avg_5d = _avg_from_rows(allowed_rows, "outcome_5d_bps")
            blocked_avg_5d = _avg_from_rows(blocked_rows, "outcome_5d_bps")
            allowed_avg_20d = _avg_from_rows(allowed_rows, "outcome_20d_bps")
            blocked_avg_20d = _avg_from_rows(blocked_rows, "outcome_20d_bps")
            allowed_avg_60d = _avg_from_rows(allowed_rows, "outcome_60d_bps")
            blocked_avg_60d = _avg_from_rows(blocked_rows, "outcome_60d_bps")
            delta_5d = float(allowed_avg_5d - blocked_avg_5d) if allowed_avg_5d is not None and blocked_avg_5d is not None else None
            delta_20d = (
                float(allowed_avg_20d - blocked_avg_20d) if allowed_avg_20d is not None and blocked_avg_20d is not None else None
            )
            delta_60d = (
                float(allowed_avg_60d - blocked_avg_60d) if allowed_avg_60d is not None and blocked_avg_60d is not None else None
            )
            horizon_deltas = [value for value in (delta_5d, delta_20d, delta_60d) if value is not None]
            positive_horizons = sum(1 for value in horizon_deltas if float(value) >= 15.0)
            negative_horizons = sum(1 for value in horizon_deltas if float(value) <= -15.0)
            review_label = "INSUFFICIENT_OUTCOME_SAMPLE"
            review_note = "等待更多 5/20/60d outcome 样本。"
            review_basis = "no_outcome"
            if horizon_deltas:
                review_basis = "5/20/60d_multi_horizon"
                if positive_horizons >= 2 or (delta_20d is not None and delta_20d >= 25.0):
                    review_label = "BLOCKING_HELPED"
                    review_note = "被允许订单在 outcome 维度强于被挡订单，当前 gate 方向有效。"
                elif negative_horizons >= 2 or (delta_20d is not None and delta_20d <= -25.0):
                    review_label = "BLOCKED_OUTPERFORMED_ALLOWED"
                    review_note = "被挡订单事后强于被允许订单，优先复核该 gate 是否过紧。"
                else:
                    review_label = "MIXED"
                    review_note = "被允许与被挡样本在 5/20/60d 上差异不稳定，继续累计样本。"
            out.append(
                {
                    "market": market,
                    "portfolio_id": portfolio_id,
                    "block_reason": block_reason,
                    "allowed_count": len(allowed_rows),
                    "blocked_count": len(blocked_rows),
                    "allowed_avg_expected_edge_bps": _avg_from_rows(allowed_rows, "expected_edge_bps"),
                    "blocked_avg_expected_edge_bps": _avg_from_rows(blocked_rows, "expected_edge_bps"),
                    "allowed_avg_realized_edge_bps": _avg_from_rows(allowed_rows, "realized_edge_bps"),
                    "blocked_avg_realized_edge_bps": _avg_from_rows(blocked_rows, "realized_edge_bps"),
                    "allowed_avg_outcome_5d_bps": allowed_avg_5d,
                    "blocked_avg_outcome_5d_bps": blocked_avg_5d,
                    "allowed_avg_outcome_20d_bps": allowed_avg_20d,
                    "blocked_avg_outcome_20d_bps": blocked_avg_20d,
                    "allowed_avg_outcome_60d_bps": allowed_avg_60d,
                    "blocked_avg_outcome_60d_bps": blocked_avg_60d,
                    "allowed_minus_blocked_outcome_5d_bps": delta_5d,
                    "allowed_minus_blocked_outcome_20d_bps": delta_20d,
                    "allowed_minus_blocked_outcome_60d_bps": delta_60d,
                    "outcome_horizon_count": int(len(horizon_deltas)),
                    "positive_outcome_horizon_count": int(positive_horizons),
                    "negative_outcome_horizon_count": int(negative_horizons),
                    "review_basis": review_basis,
                    "review_label": review_label,
                    "review_note": review_note,
                }
            )
    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("block_reason") or ""),
        )
    )
    return out
