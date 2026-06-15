from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping

from .markets import resolve_market_code


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values]
    return round(sum(clean) / len(clean), 6) if clean else 0.0


def _status(value: Any) -> str:
    return str(value or "").strip().upper()


def _component_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get("entry_anchor_selected_component") or "UNKNOWN").strip().lower() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


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
