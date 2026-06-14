from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping


def _float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return parsed if math.isfinite(parsed) else float(default)


def build_seed_evidence_queue(
    promotion_rows: Iterable[Mapping[str, Any]],
    *,
    account_growth_tier_plan: Mapping[str, Any] | None = None,
    max_symbols_per_market: int = 4,
) -> List[Dict[str, Any]]:
    """Build bounded, review-only seed report jobs from missing candidate evidence."""
    account_plan = dict(account_growth_tier_plan or {})
    max_order_value = max(0.0, _float(account_plan.get("max_order_value"), 0.0))
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for raw in list(promotion_rows or []):
        row = dict(raw or {})
        if str(row.get("promotion_status") or "").strip().upper() != "CANDIDATE_REPORT_REQUIRED":
            continue
        market = str(row.get("market") or "").strip().upper()
        symbol = str(row.get("symbol") or "").strip().upper()
        if not market or not symbol:
            continue
        reference_price = max(0.0, _float(row.get("reference_price"), 0.0))
        reasons: List[str] = []
        if not bool(row.get("source_fresh", False)):
            reasons.append("source_not_fresh")
        if reference_price <= 0.0:
            reasons.append("reference_price_missing")
        elif max_order_value > 0.0 and reference_price > max_order_value:
            reasons.append("reference_price_above_order_cap")
        grouped.setdefault(market, []).append(
            {
                "symbol": symbol,
                "asset_class": str(row.get("asset_class") or "").strip().lower(),
                "reference_price": round(reference_price, 6),
                "reference_price_currency": str(row.get("reference_price_currency") or "").strip().upper(),
                "reference_price_at": str(row.get("reference_price_at") or "").strip(),
                "order_value_utilization": round(
                    reference_price / max_order_value,
                    6,
                )
                if reference_price > 0.0 and max_order_value > 0.0
                else None,
                "eligible": not reasons,
                "block_reasons": reasons,
            }
        )

    jobs: List[Dict[str, Any]] = []
    limit = max(1, int(max_symbols_per_market or 1))
    for market, candidates in grouped.items():
        candidates.sort(
            key=lambda row: (
                0 if bool(row.get("eligible", False)) else 1,
                _float(row.get("order_value_utilization"), 999.0),
                str(row.get("symbol") or ""),
            )
        )
        selected = [row for row in candidates if bool(row.get("eligible", False))][:limit]
        blocked = [row for row in candidates if not bool(row.get("eligible", False))]
        jobs.append(
            {
                "market": market,
                "status": "READY" if selected else "BLOCKED",
                "symbols": [str(row.get("symbol") or "") for row in selected],
                "symbol_count": len(selected),
                "candidate_count": len(candidates),
                "blocked_candidate_count": len(blocked),
                "candidates": candidates,
                "max_order_value": round(max_order_value, 6),
                "evidence_mode": "YFINANCE_ONLY",
                "review_only": True,
                "auto_promote": False,
                "submit_orders": False,
                "next_action": (
                    "run_bounded_seed_candidate_report"
                    if selected
                    else "refresh_seed_reference_price_or_account_fit"
                ),
            }
        )
    jobs.sort(
        key=lambda row: (
            0 if str(row.get("status") or "") == "READY" else 1,
            min(
                (
                    _float(candidate.get("order_value_utilization"), 999.0)
                    for candidate in list(row.get("candidates") or [])
                    if bool(candidate.get("eligible", False))
                ),
                default=999.0,
            ),
            str(row.get("market") or ""),
        )
    )
    return jobs
