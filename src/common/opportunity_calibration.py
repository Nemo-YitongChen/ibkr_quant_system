from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping

from .markets import resolve_market_code


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values]
    return round(sum(clean) / len(clean), 6) if clean else 0.0


def _number_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _status(value: Any) -> str:
    return str(value or "").strip().upper()


def _component_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get("entry_anchor_selected_component") or "UNKNOWN").strip().lower() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _expected_edge_bps(row: Mapping[str, Any]) -> float:
    direct = _float(row.get("expected_edge_bps"), 0.0)
    if direct > 0.0:
        return direct
    whole_share = _float(row.get("whole_share_expected_edge_bps"), 0.0)
    if whole_share > 0.0:
        return whole_share
    score_before_cost = _float(row.get("score_before_cost"), 0.0)
    threshold = _float(row.get("accumulate_threshold"), _float(row.get("hold_threshold"), 0.0))
    if score_before_cost > threshold:
        return (score_before_cost - threshold) * 140.0
    return 0.0


def _symbol(row: Mapping[str, Any]) -> str:
    return str(row.get("symbol") or "").strip()


def _unique_symbols(rows: Iterable[Mapping[str, Any]]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for row in rows:
        symbol = _symbol(row)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _post_cost_candidate(row: Mapping[str, Any]) -> Dict[str, Any]:
    expected_edge_bps = _expected_edge_bps(row)
    expected_cost_bps = _float(row.get("expected_cost_bps"), 0.0)
    return {
        "symbol": str(row.get("symbol") or ""),
        "action": str(row.get("action") or ""),
        "score": _float(row.get("score"), 0.0),
        "score_before_cost": _float(row.get("score_before_cost"), 0.0),
        "score_cost_drag": round(
            max(0.0, _float(row.get("score_before_cost"), 0.0) - _float(row.get("score"), 0.0)),
            6,
        ),
        "expected_edge_bps": round(expected_edge_bps, 6),
        "expected_cost_bps": round(expected_cost_bps, 6),
        "post_cost_edge_bps": round(expected_edge_bps - expected_cost_bps, 6),
        "spread_proxy_bps": round(_float(row.get("spread_proxy_bps"), 0.0), 6),
        "slippage_proxy_bps": round(_float(row.get("slippage_proxy_bps"), 0.0), 6),
        "commission_proxy_bps": round(_float(row.get("commission_proxy_bps"), 0.0), 6),
        "liquidity_score": round(_float(row.get("liquidity_score"), 0.0), 6),
        "last_close": round(_float(row.get("last_close"), 0.0), 6),
        "asset_class": str(row.get("asset_class") or ""),
    }


def build_post_cost_calibration(
    rows: Iterable[Mapping[str, Any]],
    *,
    market: str = "",
    portfolio_id: str = "",
    max_expected_cost_bps: float = 45.0,
    min_post_cost_edge_bps: float = 0.0,
    top_limit: int = 5,
) -> Dict[str, Any]:
    clean_rows = [
        dict(row or {})
        for row in list(rows or [])
        if isinstance(row, Mapping) and str(row.get("symbol") or "").strip()
    ]
    candidate_rows = [_post_cost_candidate(row) for row in clean_rows]
    high_cost_rows = [
        row for row in candidate_rows if _float(row.get("expected_cost_bps"), 0.0) > float(max_expected_cost_bps)
    ]
    positive_rows = [
        row
        for row in candidate_rows
        if _float(row.get("post_cost_edge_bps"), 0.0) >= float(min_post_cost_edge_bps)
    ]
    high_cost_positive_rows = [
        row
        for row in high_cost_rows
        if _float(row.get("post_cost_edge_bps"), 0.0) >= float(min_post_cost_edge_bps)
    ]
    avg_expected_cost = _mean(_float(row.get("expected_cost_bps"), 0.0) for row in candidate_rows)
    avg_post_cost_edge = _mean(_float(row.get("post_cost_edge_bps"), 0.0) for row in candidate_rows)
    high_cost_ratio = float(len(high_cost_rows) / len(candidate_rows)) if candidate_rows else 0.0

    if not candidate_rows:
        status = "NO_CANDIDATES"
        reason = "missing_candidate_rows"
        primary_action = "refresh_candidate_report_evidence"
    elif not positive_rows:
        status = "EDGE_AFTER_COST_WEAK"
        reason = "no_positive_post_cost_edge_candidates"
        primary_action = "improve_signal_edge_before_expansion"
    elif high_cost_ratio >= 0.5 and high_cost_positive_rows:
        status = "COST_THRESHOLD_REVIEW"
        reason = "global_cost_threshold_blocks_positive_post_cost_candidates"
        primary_action = "review_market_specific_cost_threshold_with_post_cost_margin"
    elif high_cost_ratio >= 0.5 or avg_expected_cost > float(max_expected_cost_bps):
        status = "COST_DRAG_DOMINANT"
        reason = "expected_cost_above_market_threshold"
        primary_action = "expand_lower_cost_candidates_before_submit"
    else:
        status = "POST_COST_HEALTHY"
        reason = "candidate_post_cost_edge_positive"
        primary_action = "keep_post_cost_gate_monitor_outcomes"

    top_candidates = sorted(
        candidate_rows,
        key=lambda row: (
            -_float(row.get("post_cost_edge_bps"), 0.0),
            _float(row.get("expected_cost_bps"), 0.0),
            -_float(row.get("score"), 0.0),
            str(row.get("symbol") or ""),
        ),
    )[: max(0, int(top_limit))]
    positive_candidates = sorted(
        positive_rows,
        key=lambda row: (
            -_float(row.get("post_cost_edge_bps"), 0.0),
            _float(row.get("expected_cost_bps"), 0.0),
            -_float(row.get("score"), 0.0),
            str(row.get("symbol") or ""),
        ),
    )[: max(0, int(max(top_limit, 20)))]
    return {
        "market": resolve_market_code(market),
        "portfolio_id": str(portfolio_id or ""),
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "candidate_count": int(len(candidate_rows)),
        "high_cost_candidate_count": int(len(high_cost_rows)),
        "positive_post_cost_edge_count": int(len(positive_rows)),
        "high_cost_positive_edge_count": int(len(high_cost_positive_rows)),
        "max_expected_cost_bps": float(max_expected_cost_bps),
        "min_post_cost_edge_bps": float(min_post_cost_edge_bps),
        "avg_expected_cost_bps": avg_expected_cost,
        "avg_post_cost_edge_bps": avg_post_cost_edge,
        "high_cost_ratio": round(high_cost_ratio, 6),
        "top_post_cost_symbols": ",".join(
            str(row.get("symbol") or "") for row in top_candidates if str(row.get("symbol") or "")
        ),
        "top_post_cost_rows": top_candidates,
        "positive_post_cost_symbols": ",".join(
            str(row.get("symbol") or "") for row in positive_candidates if str(row.get("symbol") or "")
        ),
        "positive_post_cost_rows": positive_candidates,
    }


def build_wait_pullback_calibration(
    rows: Iterable[Mapping[str, Any]],
    *,
    market: str = "",
    portfolio_id: str = "",
    near_entry_gap_pct: float = 1.0,
    review_gap_pct: float = 3.0,
) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or []) if isinstance(row, Mapping)]
    wait_rows = [row for row in clean_rows if _status(row.get("entry_status")) == "WAIT_PULLBACK"]
    near_rows = [row for row in clean_rows if _status(row.get("entry_status")) == "NEAR_ENTRY"]
    entry_rows = [
        row
        for row in clean_rows
        if _status(row.get("entry_status")) in {"ENTRY_NOW", "ADD_ON_PULLBACK"}
    ]
    gap_values = [_float(row.get("entry_anchor_gap_pct"), 0.0) for row in wait_rows]
    close_wait_rows = [
        row
        for row in wait_rows
        if _float(row.get("entry_anchor_gap_pct"), 0.0) <= float(review_gap_pct)
    ]
    near_candidate_rows = [
        row
        for row in wait_rows
        if _float(row.get("entry_anchor_gap_pct"), 0.0) <= float(near_entry_gap_pct)
    ]
    component_counts = _component_counts(wait_rows)
    dominant_component = next(iter(component_counts), "")
    missing_asset_class_count = sum(
        1
        for row in wait_rows
        if not str(row.get("asset_class") or "").strip()
    )
    max_profile_count = sum(
        1
        for row in wait_rows
        if str(row.get("entry_anchor_selection_rule") or "").strip().lower().startswith("max_")
    )

    if not wait_rows:
        status = "NO_WAIT_PULLBACK"
        primary_action = "keep_existing_opportunity_policy"
        reason = "no_wait_pullback_rows"
    elif near_candidate_rows:
        status = "REVIEW_NEAR_ENTRY"
        primary_action = "review_near_entry_paper_limit_trial_without_relaxing_gates"
        reason = "wait_pullback_gap_within_near_entry_band"
    elif close_wait_rows:
        status = "REVIEW_ANCHOR"
        primary_action = "review_pullback_anchor_before_changing_thresholds"
        reason = "wait_pullback_gap_within_review_band"
    elif dominant_component == "ma" and component_counts.get("ma", 0) >= max(2, len(wait_rows) // 2):
        status = "MA_ANCHOR_CONSERVATIVE"
        primary_action = "review_ma_anchor_conservatism_and_asset_class_tagging"
        reason = "ma_anchor_dominates_wait_pullback"
    elif missing_asset_class_count and max_profile_count <= 0:
        status = "MISSING_ASSET_CLASS"
        primary_action = "populate_asset_class_before_etf_anchor_calibration"
        reason = "missing_asset_class_prevents_profile_specific_anchor_review"
    else:
        status = "KEEP_WAIT"
        primary_action = "do_not_chase_price_wait_for_pullback"
        reason = "wait_pullback_gap_too_wide"

    top_wait = sorted(
        wait_rows,
        key=lambda row: (
            _float(row.get("entry_anchor_gap_pct"), 999.0),
            -_float(row.get("score"), 0.0),
            str(row.get("symbol") or ""),
        ),
    )[:5]
    close_wait = sorted(
        close_wait_rows,
        key=lambda row: (
            _float(row.get("entry_anchor_gap_pct"), 999.0),
            -_float(row.get("score"), 0.0),
            str(row.get("symbol") or ""),
        ),
    )[:20]
    return {
        "market": resolve_market_code(market),
        "portfolio_id": str(portfolio_id or ""),
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "wait_pullback_count": int(len(wait_rows)),
        "near_entry_count": int(len(near_rows)),
        "entry_now_count": int(len(entry_rows)),
        "close_wait_pullback_count": int(len(close_wait_rows)),
        "near_candidate_count": int(len(near_candidate_rows)),
        "avg_entry_anchor_gap_pct": _mean(gap_values),
        "min_entry_anchor_gap_pct": round(min(gap_values or [0.0]), 6),
        "max_entry_anchor_gap_pct": round(max(gap_values or [0.0]), 6),
        "dominant_anchor_component": dominant_component,
        "anchor_component_counts": component_counts,
        "missing_asset_class_count": int(missing_asset_class_count),
        "profile_specific_anchor_count": int(max_profile_count),
        "top_wait_symbols": ",".join(str(row.get("symbol") or "") for row in top_wait if str(row.get("symbol") or "")),
        "top_wait_rows": [
            {
                "symbol": str(row.get("symbol") or ""),
                "action": str(row.get("action") or ""),
                "score": _float(row.get("score"), 0.0),
                "entry_anchor_gap_pct": _float(row.get("entry_anchor_gap_pct"), 0.0),
                "entry_anchor_selected_component": str(row.get("entry_anchor_selected_component") or ""),
                "entry_anchor_profile": str(row.get("entry_anchor_profile") or ""),
                "asset_class": str(row.get("asset_class") or ""),
            }
            for row in top_wait
        ],
        "close_wait_pullback_symbols": ",".join(
            str(row.get("symbol") or "") for row in close_wait if str(row.get("symbol") or "")
        ),
        "close_wait_pullback_rows": [
            {
                "symbol": str(row.get("symbol") or ""),
                "action": str(row.get("action") or ""),
                "score": _float(row.get("score"), 0.0),
                "entry_anchor_gap_pct": _float(row.get("entry_anchor_gap_pct"), 0.0),
                "entry_anchor_selected_component": str(row.get("entry_anchor_selected_component") or ""),
                "entry_anchor_profile": str(row.get("entry_anchor_profile") or ""),
                "asset_class": str(row.get("asset_class") or ""),
            }
            for row in close_wait
        ],
    }


def build_candidate_outcome_validation(
    candidate_rows: Iterable[Mapping[str, Any]],
    outcome_rows: Iterable[Mapping[str, Any]],
    *,
    market: str = "",
    portfolio_id: str = "",
    group_name: str = "",
    min_5d_samples: int = 5,
    min_20d_samples: int = 5,
) -> Dict[str, Any]:
    """Validate a current candidate group against mature historical symbol outcomes.

    The function intentionally validates by symbol history rather than implying that
    the latest candidate snapshot already has a mature 5/20d outcome.
    """
    normalized_market = resolve_market_code(market)
    normalized_portfolio = str(portfolio_id or "")
    candidates = [dict(row or {}) for row in list(candidate_rows or []) if isinstance(row, Mapping)]
    symbols = _unique_symbols(candidates)
    symbol_set = set(symbols)
    by_symbol: Dict[str, Dict[str, List[float]]] = {
        symbol: {"5d": [], "20d": []}
        for symbol in symbols
    }
    matched_outcome_rows = 0
    latest_decision_ts = ""
    for raw_row in outcome_rows or []:
        if not isinstance(raw_row, Mapping):
            continue
        symbol = _symbol(raw_row)
        if symbol not in symbol_set:
            continue
        row_market = resolve_market_code(str(raw_row.get("market") or ""))
        row_portfolio = str(raw_row.get("portfolio_id") or "")
        if normalized_market and row_market and row_market != normalized_market:
            continue
        if normalized_portfolio and row_portfolio and row_portfolio != normalized_portfolio:
            continue
        value_5d = _number_or_none(raw_row.get("outcome_5d_bps"))
        value_20d = _number_or_none(raw_row.get("outcome_20d_bps"))
        if value_5d is None and value_20d is None:
            continue
        matched_outcome_rows += 1
        if value_5d is not None:
            by_symbol[symbol]["5d"].append(value_5d)
        if value_20d is not None:
            by_symbol[symbol]["20d"].append(value_20d)
        decision_ts = str(raw_row.get("decision_ts") or "").strip()
        if decision_ts and decision_ts > latest_decision_ts:
            latest_decision_ts = decision_ts

    values_5d = [value for symbol in symbols for value in by_symbol[symbol]["5d"]]
    values_20d = [value for symbol in symbols for value in by_symbol[symbol]["20d"]]
    matched_symbols = [
        symbol
        for symbol in symbols
        if by_symbol[symbol]["5d"] or by_symbol[symbol]["20d"]
    ]
    unmatched_symbols = [symbol for symbol in symbols if symbol not in set(matched_symbols)]
    mature_5d = len(values_5d) >= int(min_5d_samples)
    mature_20d = len(values_20d) >= int(min_20d_samples)
    avg_5d = _mean(values_5d)
    avg_20d = _mean(values_20d)
    positive_rate_5d = round(sum(1 for value in values_5d if value > 0.0) / len(values_5d), 6) if values_5d else 0.0
    positive_rate_20d = round(sum(1 for value in values_20d if value > 0.0) / len(values_20d), 6) if values_20d else 0.0

    if not symbols:
        status = "NO_CANDIDATE_SYMBOLS"
        reason = "candidate_group_empty"
        primary_action = "refresh_candidate_group_evidence"
    elif not matched_symbols:
        status = "OUTCOME_PENDING"
        reason = "no_mature_symbol_outcome_for_candidate_group"
        primary_action = "wait_for_5d_20d_outcome_maturity"
    elif not (mature_5d or mature_20d):
        status = "OUTCOME_SAMPLE_THIN"
        reason = "mature_outcome_sample_below_threshold"
        primary_action = "continue_paper_review_until_outcome_sample_matures"
    elif (
        (not mature_5d or avg_5d >= 0.0)
        and (not mature_20d or avg_20d >= 0.0)
    ):
        status = "OUTCOME_SUPPORTS_GROUP"
        reason = "candidate_group_historical_symbol_outcome_positive"
        primary_action = "keep_gate_monitor_realized_outcomes"
    else:
        status = "OUTCOME_WEAK_OR_MIXED"
        reason = "candidate_group_historical_symbol_outcome_not_consistently_positive"
        primary_action = "review_gate_or_anchor_before_submit_expansion"

    symbol_rows = []
    for symbol in symbols[:20]:
        symbol_5d = by_symbol[symbol]["5d"]
        symbol_20d = by_symbol[symbol]["20d"]
        symbol_rows.append(
            {
                "symbol": symbol,
                "outcome_5d_sample_count": int(len(symbol_5d)),
                "outcome_20d_sample_count": int(len(symbol_20d)),
                "avg_outcome_5d_bps": _mean(symbol_5d),
                "avg_outcome_20d_bps": _mean(symbol_20d),
            }
        )

    return {
        "market": normalized_market,
        "portfolio_id": normalized_portfolio,
        "group_name": str(group_name or ""),
        "status": status,
        "reason": reason,
        "primary_action": primary_action,
        "candidate_symbol_count": int(len(symbols)),
        "matched_symbol_count": int(len(matched_symbols)),
        "matched_outcome_row_count": int(matched_outcome_rows),
        "matured_5d_sample_count": int(len(values_5d)),
        "matured_20d_sample_count": int(len(values_20d)),
        "avg_outcome_5d_bps": avg_5d,
        "avg_outcome_20d_bps": avg_20d,
        "positive_rate_5d": positive_rate_5d,
        "positive_rate_20d": positive_rate_20d,
        "latest_outcome_decision_ts": latest_decision_ts,
        "candidate_symbols": ",".join(symbols),
        "matched_symbols": ",".join(matched_symbols),
        "unmatched_symbols": ",".join(unmatched_symbols),
        "symbol_rows": symbol_rows,
    }


def build_candidate_outcome_validation_summary(
    rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or []) if isinstance(row, Mapping)]
    status_counts: Dict[str, int] = {}
    market_counts: Dict[str, int] = {}
    for row in clean_rows:
        status = str(row.get("status") or "UNKNOWN")
        market = resolve_market_code(str(row.get("market") or "")) or "UNKNOWN"
        status_counts[status] = status_counts.get(status, 0) + 1
        market_counts[market] = market_counts.get(market, 0) + 1
    return {
        "validation_count": int(len(clean_rows)),
        "market_count": int(len(market_counts)),
        "status_counts": status_counts,
        "market_counts": market_counts,
        "candidate_symbol_count": int(sum(int(row.get("candidate_symbol_count", 0) or 0) for row in clean_rows)),
        "matched_symbol_count": int(sum(int(row.get("matched_symbol_count", 0) or 0) for row in clean_rows)),
        "matured_5d_sample_count": int(sum(int(row.get("matured_5d_sample_count", 0) or 0) for row in clean_rows)),
        "matured_20d_sample_count": int(sum(int(row.get("matured_20d_sample_count", 0) or 0) for row in clean_rows)),
        "primary_status": next(iter(status_counts), "UNKNOWN") if len(status_counts) == 1 else "MIXED",
    }


def build_post_cost_calibration_summary(
    rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or []) if isinstance(row, Mapping)]
    status_counts: Dict[str, int] = {}
    market_counts: Dict[str, int] = {}
    for row in clean_rows:
        status = str(row.get("status") or "UNKNOWN")
        market = resolve_market_code(str(row.get("market") or "")) or "UNKNOWN"
        status_counts[status] = status_counts.get(status, 0) + 1
        market_counts[market] = market_counts.get(market, 0) + 1
    review_statuses = {"COST_THRESHOLD_REVIEW", "COST_DRAG_DOMINANT", "EDGE_AFTER_COST_WEAK"}
    review_count = sum(1 for row in clean_rows if str(row.get("status") or "") in review_statuses)
    high_cost_count = sum(int(row.get("high_cost_candidate_count", 0) or 0) for row in clean_rows)
    positive_count = sum(int(row.get("positive_post_cost_edge_count", 0) or 0) for row in clean_rows)
    return {
        "portfolio_count": int(len(clean_rows)),
        "market_count": int(len(market_counts)),
        "status_counts": status_counts,
        "market_counts": market_counts,
        "review_portfolio_count": int(review_count),
        "high_cost_candidate_count": int(high_cost_count),
        "positive_post_cost_edge_count": int(positive_count),
        "primary_status": next(iter(status_counts), "UNKNOWN") if len(status_counts) == 1 else "MIXED",
        "summary_text": (
            f"portfolios={len(clean_rows)} review_portfolios={review_count} "
            f"high_cost_candidates={high_cost_count}"
        ),
    }


def build_wait_pullback_calibration_summary(
    rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or []) if isinstance(row, Mapping)]
    status_counts: Dict[str, int] = {}
    market_counts: Dict[str, int] = {}
    for row in clean_rows:
        status = str(row.get("status") or "UNKNOWN")
        market = resolve_market_code(str(row.get("market") or "")) or "UNKNOWN"
        status_counts[status] = status_counts.get(status, 0) + 1
        market_counts[market] = market_counts.get(market, 0) + 1
    review_count = sum(
        int(row.get("close_wait_pullback_count", 0) or 0)
        for row in clean_rows
    )
    near_candidate_count = sum(
        int(row.get("near_candidate_count", 0) or 0)
        for row in clean_rows
    )
    return {
        "portfolio_count": int(len(clean_rows)),
        "market_count": int(len(market_counts)),
        "status_counts": status_counts,
        "market_counts": market_counts,
        "review_wait_pullback_count": int(review_count),
        "near_candidate_count": int(near_candidate_count),
        "primary_status": next(iter(status_counts), "UNKNOWN") if len(status_counts) == 1 else "MIXED",
        "summary_text": (
            f"portfolios={len(clean_rows)} review_wait={review_count} "
            f"near_candidates={near_candidate_count}"
        ),
    }
