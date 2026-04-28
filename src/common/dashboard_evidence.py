from __future__ import annotations

from typing import Any, Dict, List

from .dashboard_market_context import MARKET_CONTEXT, market_context

WATERFALL_COMPONENTS: tuple[tuple[str, str, str], ...] = (
    ("selection_contribution", "selection", "return_component"),
    ("sizing_contribution", "sizing", "return_component"),
    ("sector_contribution", "sector", "return_component"),
    ("market_contribution", "market", "return_component"),
    ("execution_contribution", "execution", "return_component"),
    ("strategy_control_weight_delta", "strategy_control", "control_delta"),
    ("risk_overlay_weight_delta", "risk_overlay", "control_delta"),
    ("execution_gate_blocked_weight", "execution_gate", "control_delta"),
)
DEFAULT_MARKET_VIEW_MARKETS: tuple[str, ...] = ("US", "HK", "CN")
DATA_ATTENTION_LABELS = {"待排查", "有缺失", "无数据", "混合", "研究Fallback", "warning", "fail"}
MARKET_VIEW_CONTEXTS: Dict[str, Dict[str, Any]] = MARKET_CONTEXT


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _flag(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def build_weekly_attribution_waterfall(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in list(cards or []):
        attribution = dict(card.get("weekly_attribution", {}) or {})
        if not attribution:
            continue
        market = str(card.get("market", "") or "")
        portfolio_id = str(card.get("portfolio_id", "") or "")
        watchlist = str(card.get("watchlist", "") or "")
        running = 0.0
        for idx, (source_key, layer, component_role) in enumerate(WATERFALL_COMPONENTS, start=1):
            value = _safe_float(attribution.get(source_key), 0.0)
            start_value = running
            running += value
            rows.append(
                {
                    "market": market,
                    "watchlist": watchlist,
                    "portfolio_id": portfolio_id,
                    "component_order": idx,
                    "component": layer,
                    "source_key": source_key,
                    "component_role": component_role,
                    "contribution": value,
                    "running_start": start_value,
                    "running_end": running,
                }
            )
        reported_return = _safe_float(attribution.get("weekly_return"), 0.0)
        rows.append(
            {
                "market": market,
                "watchlist": watchlist,
                "portfolio_id": portfolio_id,
                "component_order": 98,
                "component": "residual_to_reported_return",
                "source_key": "weekly_return",
                "component_role": "residual",
                "contribution": reported_return - running,
                "running_start": running,
                "running_end": reported_return,
            }
        )
        rows.append(
            {
                "market": market,
                "watchlist": watchlist,
                "portfolio_id": portfolio_id,
                "component_order": 99,
                "component": "reported_weekly_return",
                "source_key": "weekly_return",
                "component_role": "total",
                "contribution": reported_return,
                "running_start": 0.0,
                "running_end": reported_return,
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            int(row.get("component_order", 0) or 0),
        )
    )
    return rows


def build_market_views(
    cards: List[Dict[str, Any]],
    *,
    markets: tuple[str, ...] = DEFAULT_MARKET_VIEW_MARKETS,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for market in markets:
        market_code = str(market or "").strip().upper()
        ctx = market_context(market_code)
        market_cards = [
            dict(card)
            for card in list(cards or [])
            if str(card.get("market", "") or "").strip().upper() == market_code
        ]
        portfolios: List[Dict[str, Any]] = []
        open_count = 0
        fresh_count = 0
        stale_count = 0
        degraded_count = 0
        data_attention_count = 0
        auto_submit_count = 0
        review_only_count = 0
        paused_count = 0
        for card in market_cards:
            report_status = dict(card.get("report_status", {}) or {})
            health_rows = list(card.get("ops_health_rows", []) or [])
            market_data_rows = list(card.get("market_data_health_rows", []) or [])
            control = dict(card.get("dashboard_control", {}).get("portfolio", {}) or {})
            execution_mode = str(control.get("execution_control_mode") or "AUTO").strip().upper()
            if bool(card.get("exchange_open_raw", False)):
                open_count += 1
            if bool(report_status.get("fresh")):
                fresh_count += 1
            else:
                stale_count += 1
            if any(
                str(row.get("status") or "").strip().lower() not in {"ok", "pass"}
                for row in health_rows
                if isinstance(row, dict)
            ):
                degraded_count += 1
            if any(
                str(row.get("status_label") or row.get("status") or "").strip() in DATA_ATTENTION_LABELS
                for row in market_data_rows
                if isinstance(row, dict)
            ):
                data_attention_count += 1
            if execution_mode == "REVIEW_ONLY":
                review_only_count += 1
            elif execution_mode == "PAUSED":
                paused_count += 1
            if bool(control.get("submit_investment_execution")) or bool(
                dict(card.get("execution_summary", {}) or {}).get("submit_orders")
            ):
                auto_submit_count += 1
            portfolios.append(
                {
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "mode": str(card.get("mode", "") or ""),
                    "execution_control_mode": execution_mode or "AUTO",
                    "market_state": str(card.get("market_state_label", "") or ""),
                    "report_status": str(card.get("report_status_label", "") or ""),
                    "action": str(card.get("action_label", "") or card.get("priority_reason", "") or ""),
                    "detail": str(card.get("action_detail", "") or ""),
                }
            )
        out[market_code] = {
            "market": market_code,
            "context": ctx,
            "context_summary": str(ctx.get("context_summary") or ctx.get("summary") or ""),
            "primary_risks": list(ctx.get("primary_risks") or []),
            "settlement_cycle": str(ctx.get("settlement_cycle") or ""),
            "day_turnaround_allowed": bool(ctx.get("day_turnaround_allowed", False)),
            "research_only": bool(ctx.get("research_only", False)),
            "primary_review_axis": str(ctx.get("primary_review_axis") or ""),
            "portfolio_count": len(market_cards),
            "open_count": open_count,
            "fresh_report_count": fresh_count,
            "stale_report_count": stale_count,
            "degraded_health_count": degraded_count,
            "data_attention_count": data_attention_count,
            "auto_submit_count": auto_submit_count,
            "review_only_count": review_only_count,
            "paused_count": paused_count,
            "portfolios": portfolios[:12],
        }
    return out


def build_unified_evidence_overview(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_market: Dict[str, Dict[str, Any]] = {}
    blocked = 0
    allowed = 0
    for raw in list(rows or []):
        row = dict(raw or {})
        market = str(row.get("market") or "").strip().upper() or "UNKNOWN"
        market_row = by_market.setdefault(
            market,
            {
                "market": market,
                "row_count": 0,
                "blocked_row_count": 0,
                "allowed_row_count": 0,
            },
        )
        market_row["row_count"] = int(market_row.get("row_count", 0) or 0) + 1
        if _flag(row.get("blocked_flag")):
            blocked += 1
            market_row["blocked_row_count"] = int(market_row.get("blocked_row_count", 0) or 0) + 1
        if _flag(row.get("allowed_flag")):
            allowed += 1
            market_row["allowed_row_count"] = int(market_row.get("allowed_row_count", 0) or 0) + 1
    return {
        "row_count": len(rows),
        "blocked_row_count": blocked,
        "allowed_row_count": allowed,
        "market_rows": sorted(by_market.values(), key=lambda row: str(row.get("market") or "")),
    }
