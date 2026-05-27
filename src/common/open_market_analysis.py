from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _bool(value: Any) -> bool:
    return bool(value)


def _report_fresh(card: Mapping[str, Any]) -> bool:
    return bool(_dict(card.get("report_status")).get("fresh", False))


def _exchange_open(card: Mapping[str, Any]) -> Any:
    if "exchange_open_raw" in card:
        return card.get("exchange_open_raw")
    return card.get("exchange_open")


def _market_data_attention(card: Mapping[str, Any]) -> bool:
    rows = _list(card.get("market_data_health_overview")) or _list(card.get("market_data_health_rows"))
    row = _dict(rows[0]) if rows else {}
    label = str(row.get("status_label") or "").strip()
    status = str(row.get("status") or "").strip().lower()
    return label in {"待排查", "混合", "有缺失", "无数据"} or status in {"warning", "degraded", "fail", "failed"}


def _auto_rows_by_portfolio(auto_order_readiness: Mapping[str, Any] | None) -> Dict[str, Dict[str, Any]]:
    rows = _list(_dict(auto_order_readiness).get("rows"))
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        row_dict = _dict(row)
        portfolio_id = str(row_dict.get("portfolio_id") or "").strip()
        if portfolio_id:
            result[portfolio_id] = row_dict
    return result


def _primary_reason_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        if not bool(row.get("submit_enabled", False)):
            continue
        reason = str(row.get("auto_order_primary_reason") or row.get("primary_no_order_reason") or "").strip()
        if not reason:
            continue
        counts[reason] = int(counts.get(reason, 0)) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def build_open_market_analysis_summary(
    cards: Iterable[Mapping[str, Any]],
    *,
    auto_order_readiness: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Summarize whether currently-open markets have fresh analysis and submit-gate evidence."""

    card_rows = [_dict(card) for card in list(cards or []) if isinstance(card, Mapping)]
    auto_rows = _auto_rows_by_portfolio(auto_order_readiness)
    auto_artifact_present = bool(_dict(auto_order_readiness))
    missing_market_state_count = sum(1 for card in card_rows if _exchange_open(card) is None)
    open_rows: list[Dict[str, Any]] = []
    market_counts: Dict[str, Dict[str, int]] = {}

    for card in card_rows:
        exchange_open = _exchange_open(card)
        if exchange_open is not True:
            continue
        market = str(card.get("market") or "").strip().upper()
        portfolio_id = str(card.get("portfolio_id") or "").strip()
        auto_row = auto_rows.get(portfolio_id, {})
        execution_summary = _dict(card.get("execution_summary"))
        submit_enabled = _bool(card.get("submit_investment_execution"))
        auto_status = str(auto_row.get("status") or ("MISSING" if submit_enabled else "NOT_REQUIRED")).strip().upper()
        auto_primary_reason = str(auto_row.get("primary_reason") or "").strip()
        if not auto_primary_reason:
            auto_primary_reason = str(execution_summary.get("primary_no_order_reason") or "").strip()
        row = {
            "market": market,
            "watchlist": str(card.get("watchlist") or "").strip(),
            "portfolio_id": portfolio_id,
            "mode": str(card.get("mode") or card.get("runtime_mode_summary") or "").strip(),
            "exchange_open": True,
            "report_fresh": _report_fresh(card),
            "report_freshness_label": str(card.get("report_freshness_label") or "").strip(),
            "recommended_action": str(card.get("recommended_action") or "").strip(),
            "actionable": bool(card.get("actionable", False)),
            "submit_enabled": submit_enabled,
            "auto_order_status": auto_status,
            "auto_order_ready": bool(auto_row.get("ready", False)),
            "auto_order_primary_reason": auto_primary_reason,
            "auto_order_hard_blocks": [str(value) for value in _list(auto_row.get("hard_blocks")) if str(value).strip()],
            "auto_order_warnings": [str(value) for value in _list(auto_row.get("warnings")) if str(value).strip()],
            "data_attention": _market_data_attention(card),
            "primary_no_order_reason": str(execution_summary.get("primary_no_order_reason") or "").strip(),
            "submit_guard_status": str(execution_summary.get("submit_guard_status") or "").strip(),
        }
        open_rows.append(row)

        market_row = market_counts.setdefault(
            market,
            {
                "open_portfolio_count": 0,
                "fresh_report_count": 0,
                "stale_report_count": 0,
                "submit_enabled_count": 0,
                "auto_ready_count": 0,
                "auto_blocked_count": 0,
                "auto_missing_count": 0,
                "data_attention_count": 0,
            },
        )
        market_row["open_portfolio_count"] += 1
        if row["report_fresh"]:
            market_row["fresh_report_count"] += 1
        else:
            market_row["stale_report_count"] += 1
        if submit_enabled:
            market_row["submit_enabled_count"] += 1
            if not auto_artifact_present or not auto_row:
                market_row["auto_missing_count"] += 1
            elif row["auto_order_ready"]:
                market_row["auto_ready_count"] += 1
            else:
                market_row["auto_blocked_count"] += 1
        if row["data_attention"]:
            market_row["data_attention_count"] += 1

    open_market_count = len({row["market"] for row in open_rows if row.get("market")})
    open_portfolio_count = len(open_rows)
    fresh_open_report_count = sum(1 for row in open_rows if bool(row.get("report_fresh")))
    stale_open_report_count = open_portfolio_count - fresh_open_report_count
    actionable_open_count = sum(1 for row in open_rows if bool(row.get("actionable")) or str(row.get("recommended_action") or "") in {"可执行调仓", "可关注进场", "接近进场"})
    submit_enabled_open_count = sum(1 for row in open_rows if bool(row.get("submit_enabled")))
    auto_ready_open_count = sum(1 for row in open_rows if bool(row.get("submit_enabled")) and bool(row.get("auto_order_ready")))
    auto_missing_open_count = sum(
        1
        for row in open_rows
        if bool(row.get("submit_enabled"))
        and (not auto_artifact_present or str(row.get("auto_order_status") or "") == "MISSING")
    )
    auto_blocked_open_count = sum(
        1
        for row in open_rows
        if bool(row.get("submit_enabled"))
        and not bool(row.get("auto_order_ready"))
        and str(row.get("auto_order_status") or "") != "MISSING"
    )
    data_attention_open_count = sum(1 for row in open_rows if bool(row.get("data_attention")))
    reason_counts = _primary_reason_counts(open_rows)
    primary_reason = next(iter(reason_counts), "")

    if missing_market_state_count or stale_open_report_count:
        status = "degraded"
        status_label = "开市分析降级"
    elif submit_enabled_open_count and auto_missing_open_count:
        status = "warning"
        status_label = "开市门控证据缺失"
    elif auto_blocked_open_count or data_attention_open_count:
        status = "warning"
        status_label = "开市分析待处理"
    else:
        status = "ready"
        status_label = "开市分析可用"

    market_rows = [
        {"market": market, **counts}
        for market, counts in sorted(market_counts.items(), key=lambda item: item[0])
    ]
    return {
        "status": status,
        "status_label": status_label,
        "summary_text": (
            f"open_markets={open_market_count} open_portfolios={open_portfolio_count} "
            f"fresh={fresh_open_report_count}/{open_portfolio_count} actionable={actionable_open_count} "
            f"submit_enabled={submit_enabled_open_count} auto_ready={auto_ready_open_count} "
            f"auto_blocked={auto_blocked_open_count} auto_missing={auto_missing_open_count} "
            f"data_attention={data_attention_open_count} primary_reason={primary_reason or '-'}"
        ),
        "open_market_count": open_market_count,
        "open_portfolio_count": open_portfolio_count,
        "fresh_open_report_count": fresh_open_report_count,
        "stale_open_report_count": stale_open_report_count,
        "actionable_open_count": actionable_open_count,
        "submit_enabled_open_count": submit_enabled_open_count,
        "auto_order_artifact_present": auto_artifact_present,
        "auto_ready_open_count": auto_ready_open_count,
        "auto_blocked_open_count": auto_blocked_open_count,
        "auto_missing_open_count": auto_missing_open_count,
        "data_attention_open_count": data_attention_open_count,
        "missing_market_state_count": missing_market_state_count,
        "primary_reason": primary_reason,
        "primary_reason_counts": reason_counts,
        "market_rows": market_rows,
        "rows": open_rows,
    }
