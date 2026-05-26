from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping

READY_STATUS = "READY"
BLOCKED_STATUS = "BLOCKED"
WARNING_STATUS = "WARNING"
DISABLED_STATUS = "DISABLED"

_BLOCK_REASON_PRIORITY = {
    "live_submit_not_allowed": 10,
    "ibkr_gateway_unavailable": 15,
    "preflight_missing": 20,
    "preflight_failed": 25,
    "preflight_stale": 30,
    "weekly_review_missing": 40,
    "weekly_review_stale": 45,
    "gateway_budget_degraded": 50,
    "market_readiness_missing": 55,
    "market_readiness_not_ready": 60,
    "submit_quality_not_pass": 65,
    "strategy_auto_apply_violation": 70,
    "strategy_followup_degraded": 75,
    "strategy_suggestion_stale": 80,
}

_WARNING_REASON_PRIORITY = {
    "preflight_warn": 100,
    "gateway_budget_warning": 110,
    "market_readiness_missing": 120,
    "market_readiness_artifact_health": 130,
    "strategy_suggestions_open": 140,
}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_hours(value: Any, now: datetime) -> float | None:
    dt = _parse_datetime(value)
    if dt is None:
        return None
    return round(max(0.0, (now.astimezone(timezone.utc) - dt).total_seconds() / 3600.0), 2)


def _status(value: Any) -> str:
    return str(value or "").strip().lower()


def _market(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_auto_order_readiness_policy(raw: Mapping[str, Any] | None) -> Dict[str, Any]:
    source = dict(raw or {})
    raw_excluded_markets = source.get("excluded_markets") or source.get("exclude_markets") or []
    if isinstance(raw_excluded_markets, str):
        raw_excluded_markets = raw_excluded_markets.split(",")
    excluded_markets = [
        _market(value)
        for value in list(raw_excluded_markets or [])
        if _market(value)
    ]
    return {
        "enabled": bool(source.get("enabled", False)),
        "allow_live_submit": bool(source.get("allow_live_submit", False)),
        "excluded_markets": excluded_markets,
        "max_preflight_age_hours": _float(source.get("max_preflight_age_hours", 24.0), 24.0),
        "max_weekly_review_age_hours": _float(source.get("max_weekly_review_age_hours", 168.0), 168.0),
        "block_on_preflight_fail": bool(source.get("block_on_preflight_fail", True)),
        "block_on_missing_preflight": bool(source.get("block_on_missing_preflight", True)),
        "block_on_stale_preflight": bool(source.get("block_on_stale_preflight", True)),
        "block_on_missing_weekly_review": bool(source.get("block_on_missing_weekly_review", True)),
        "block_on_stale_weekly_review": bool(source.get("block_on_stale_weekly_review", True)),
        "block_on_gateway_budget_degraded": bool(source.get("block_on_gateway_budget_degraded", True)),
        "block_on_missing_market_readiness": bool(source.get("block_on_missing_market_readiness", False)),
        "block_on_market_readiness_not_ready": bool(source.get("block_on_market_readiness_not_ready", True)),
        "warn_on_missing_market_readiness": bool(source.get("warn_on_missing_market_readiness", False)),
        "max_submit_portfolios_per_run": max(1, _int(source.get("max_submit_portfolios_per_run"), 1)),
        "max_submit_portfolios_per_market": max(1, _int(source.get("max_submit_portfolios_per_market"), 1)),
        "max_submit_orders_per_portfolio": max(1, _int(source.get("max_submit_orders_per_portfolio"), 1)),
        "max_submit_gross_order_value": _float(source.get("max_submit_gross_order_value"), 100.0),
        "max_submit_total_gross_order_value": _float(
            source.get("max_submit_total_gross_order_value"),
            0.0,
        ),
        "require_buy_order_for_submit": bool(source.get("require_buy_order_for_submit", False)),
        "block_on_submit_quality_not_pass": bool(source.get("block_on_submit_quality_not_pass", True)),
        "min_submit_net_edge_bps": _float(source.get("min_submit_net_edge_bps"), 8.0),
        "min_submit_edge_margin_bps": _float(source.get("min_submit_edge_margin_bps"), 3.0),
        "max_submit_expected_cost_bps": _float(source.get("max_submit_expected_cost_bps"), 35.0),
        "require_limit_order_for_submit": bool(source.get("require_limit_order_for_submit", True)),
        "max_submit_order_adv_pct": _float(source.get("max_submit_order_adv_pct"), 0.001),
        "high_quality_min_net_edge_bps": _float(source.get("high_quality_min_net_edge_bps"), 16.0),
        "high_quality_min_edge_margin_bps": _float(source.get("high_quality_min_edge_margin_bps"), 8.0),
        "high_quality_max_expected_cost_bps": _float(source.get("high_quality_max_expected_cost_bps"), 25.0),
        "block_on_strategy_auto_apply_violation": bool(
            source.get("block_on_strategy_auto_apply_violation", True)
        ),
        "block_on_degraded_strategy_followup": bool(source.get("block_on_degraded_strategy_followup", True)),
        "warn_on_open_strategy_suggestions": bool(source.get("warn_on_open_strategy_suggestions", True)),
    }


def _block_detail(reason: str, severity: str, detail: str = "", remediation: str = "") -> Dict[str, str]:
    return {
        "reason": reason,
        "severity": severity,
        "detail": detail,
        "remediation": remediation,
    }


def _reason_priority(reason: str, *, warning: bool = False) -> int:
    reason_text = str(reason or "").strip()
    priorities = _WARNING_REASON_PRIORITY if warning else _BLOCK_REASON_PRIORITY
    return int(priorities.get(reason_text, 999))


def _primary_hard_block_reason(hard_blocks: Iterable[Any]) -> str:
    reasons = [str(reason or "").strip() for reason in list(hard_blocks or []) if str(reason or "").strip()]
    if not reasons:
        return ""
    return min(reasons, key=lambda reason: (_reason_priority(reason), reason))


def _preflight_check_relevant(check: Mapping[str, Any], portfolio: Mapping[str, Any]) -> bool:
    name = str(check.get("name") or "").strip()
    if not name:
        return True
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    watchlist = str(portfolio.get("watchlist") or "").strip()
    ibkr_config = str(portfolio.get("ibkr_config") or "").strip()
    global_names = {
        "config",
        "summary_out_dir",
        "dashboard_weekly_review_dir",
        "dashboard_execution_kpi_dir",
        "dashboard_db",
        "dashboard_control_state",
        "runtime_root",
    }
    if name in global_names:
        return True
    if name.startswith("ibkr_port:"):
        markets = {_market(value) for value in list(check.get("markets") or []) if _market(value)}
        config_paths = {str(value).strip() for value in list(check.get("ibkr_config_paths") or []) if str(value).strip()}
        return (market and market in markets) or (ibkr_config and ibkr_config in config_paths)
    prefix = name.split(":", 1)[0].strip().upper()
    if market and prefix != market:
        return False
    if portfolio_id and name.startswith(f"{portfolio_id}:"):
        return True
    if market and watchlist and name.startswith(f"{market}:{watchlist}:"):
        return True
    if market and name.startswith(f"{market}:"):
        return portfolio_id.count(":") <= 1 and watchlist == ""
    return True


def _preflight_relevant_checks(
    preflight_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
    statuses: Iterable[str],
) -> List[Dict[str, Any]]:
    wanted = {_status(value) for value in statuses}
    checks = [
        dict(check)
        for check in list((preflight_summary or {}).get("checks") or [])
        if isinstance(check, Mapping)
    ]
    return [
        check
        for check in checks
        if _status(check.get("status")) in wanted and _preflight_check_relevant(check, portfolio)
    ]


def _gateway_budget_row(weekly_summary: Mapping[str, Any] | None, portfolio: Mapping[str, Any]) -> Dict[str, Any]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("ibkr_gateway_budget_rows") or [])
        if isinstance(row, Mapping)
    ]
    market = _market(portfolio.get("market"))
    for row in rows:
        if _market(row.get("market")) == market:
            return row
    return {}


def _gateway_budget_status(weekly_summary: Mapping[str, Any] | None, portfolio: Mapping[str, Any]) -> str:
    row = _gateway_budget_row(weekly_summary, portfolio)
    if row:
        return _status(row.get("status"))
    summary = dict((weekly_summary or {}).get("ibkr_gateway_budget") or {})
    status = _status(summary.get("status"))
    return status


def _gateway_budget_detail(row: Mapping[str, Any], portfolio: Mapping[str, Any], status: str) -> str:
    reason = str(row.get("reason") or status).strip()
    budget = _int(row.get("weekly_gateway_request_budget"), 0)
    gateway_count = _int(row.get("gateway_request_count"), 0)
    usage_pct = _float(row.get("budget_usage_pct"), 0.0)
    top_kind = str(row.get("top_request_kind") or "").strip()
    top_tool = str(row.get("top_tool") or "").strip()
    recovery_days = _int(row.get("projected_recovery_days"), 0)
    recovery_at = str(row.get("projected_recovery_at") or "").strip()
    parts = [f"market={portfolio.get('market', '')}", f"reason={reason}"]
    if budget > 0 or gateway_count > 0:
        parts.append(f"requests={gateway_count}/{budget}")
        parts.append(f"usage={usage_pct:.2f}%")
    if top_kind:
        parts.append(f"top_request_kind={top_kind}")
    if top_tool:
        parts.append(f"top_tool={top_tool}")
    if recovery_days > 0:
        parts.append(f"projected_recovery_days={recovery_days}")
    if recovery_at:
        parts.append(f"projected_recovery_at={recovery_at}")
    return " ".join(parts)


def _gateway_budget_remediation(row: Mapping[str, Any], *, blocked: bool) -> str:
    recovery_at = str(row.get("projected_recovery_at") or "").strip()
    top_tool = str(row.get("top_tool") or "").strip()
    top_kind = str(row.get("top_request_kind") or "").strip()
    action = "Keep high-request scans disabled and avoid submit until the Gateway budget recovers."
    if not blocked:
        action = "Keep monitoring IBKR Gateway load before submit."
    if top_tool or top_kind:
        action += f" Highest load: {top_tool or 'unknown_tool'} / {top_kind or 'unknown_kind'}."
    if recovery_at:
        action += f" Re-run weekly review and auto-order readiness after {recovery_at}."
    else:
        action += " Re-run weekly review after the 7-day telemetry window rolls."
    return action


def _market_readiness_row(
    market_readiness_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> Dict[str, Any]:
    rows = [
        dict(row)
        for row in list((market_readiness_summary or {}).get("rows") or [])
        if isinstance(row, Mapping)
    ]
    if not rows:
        return {}
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    watchlist = str(portfolio.get("watchlist") or "").strip()
    for row in rows:
        if portfolio_id and str(row.get("portfolio_id") or "").strip() == portfolio_id:
            return row
    for row in rows:
        if market and _market(row.get("market")) == market and watchlist and str(row.get("watchlist") or "").strip() == watchlist:
            return row
    for row in rows:
        if market and _market(row.get("market")) == market:
            return row
    return {}


def _strategy_suggestion_rows_for_portfolio(
    weekly_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("strategy_parameter_suggestions") or [])
        if isinstance(row, Mapping)
    ]
    if not rows:
        return []
    market = str(portfolio.get("market") or "").strip().upper()
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        row_market = str(row.get("market") or "").strip().upper()
        row_portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id and row_portfolio_id:
            if row_portfolio_id == portfolio_id:
                matched.append(row)
            continue
        if market and row_market == market:
            matched.append(row)
    return matched


def _is_unresolved_strategy_suggestion(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or "SUGGESTED").strip().upper()
    return status not in {"APPLIED", "REJECTED", "SUPERSEDED", "RESOLVED"}


def _active_strategy_suggestion_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = "|".join(
            [
                str(row.get("portfolio_id") or "").strip(),
                str(row.get("primary_field") or row.get("field") or "").strip(),
                str(row.get("config_path") or "").strip(),
            ]
        )
        if not key.strip("|"):
            key = str(row.get("suggestion_id") or len(latest))
        current = latest.get(key)
        current_dt = _parse_datetime(current.get("created_at")) if current else None
        row_dt = _parse_datetime(row.get("created_at"))
        if current is None or (row_dt or datetime.min.replace(tzinfo=timezone.utc)) >= (
            current_dt or datetime.min.replace(tzinfo=timezone.utc)
        ):
            latest[key] = row
    return list(latest.values())


def _strategy_followup_rows_for_portfolio(
    weekly_summary: Mapping[str, Any] | None,
    portfolio: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows = [
        dict(row)
        for row in list((weekly_summary or {}).get("strategy_parameter_suggestion_followup") or [])
        if isinstance(row, Mapping)
    ]
    market = _market(portfolio.get("market"))
    portfolio_id = str(portfolio.get("portfolio_id") or "").strip()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        if portfolio_id and str(row.get("portfolio_id") or "").strip() == portfolio_id:
            matched.append(row)
            continue
        if market and _market(row.get("market")) == market:
            matched.append(row)
    return matched


def _submit_policy_reject_reasons(
    order_count: int,
    planned_gross: float,
    *,
    max_orders: int,
    max_value: float,
    planned_buy: float = 0.0,
    require_buy: bool = False,
) -> List[str]:
    reject_reasons: List[str] = []
    if order_count <= 0:
        reject_reasons.append("no_planned_orders")
    if bool(require_buy) and float(planned_buy) <= 0.0:
        reject_reasons.append("no_buy_order_for_growth_submit")
    if order_count > int(max_orders):
        reject_reasons.append("order_count_exceeds_policy")
    if float(max_value) > 0.0 and float(planned_gross) > float(max_value) + 1e-9:
        reject_reasons.append("planned_gross_value_exceeds_policy")
    return reject_reasons


def _submit_candidate_base(row: Mapping[str, Any], *, order_count: int, planned_gross: float) -> Dict[str, Any]:
    return {
        "market": str(row.get("market") or ""),
        "portfolio_id": str(row.get("portfolio_id") or ""),
        "order_count": int(order_count),
        "planned_gross_order_value": float(planned_gross),
        "planned_buy_order_value": _float(row.get("market_readiness_planned_buy_order_value"), 0.0),
        "planned_sell_order_value": _float(row.get("market_readiness_planned_sell_order_value"), 0.0),
        "planned_net_cash_order_value": _float(row.get("market_readiness_planned_net_cash_order_value"), 0.0),
        "planned_order_symbols": str(row.get("market_readiness_planned_order_symbols") or ""),
        "market_readiness_reason": str(row.get("market_readiness_reason") or ""),
        "submit_quality_status": str(row.get("submit_quality_status") or ""),
        "submit_quality_tier": str(row.get("submit_quality_tier") or ""),
        "submit_quality_reason": str(row.get("submit_quality_reason") or ""),
        "submit_quality_min_net_edge_bps": _float(row.get("submit_quality_min_net_edge_bps"), 0.0),
        "submit_quality_min_edge_margin_bps": _float(row.get("submit_quality_min_edge_margin_bps"), 0.0),
        "submit_quality_max_expected_cost_bps": _float(row.get("submit_quality_max_expected_cost_bps"), 0.0),
        "submit_quality_order_types": str(row.get("submit_quality_order_types") or ""),
    }


def _submit_quality_rank(row: Mapping[str, Any]) -> int:
    tier = str(row.get("submit_quality_tier") or "").strip().upper()
    if tier == "HIGH":
        return 0
    if tier == "PASS":
        return 1
    if str(row.get("submit_quality_status") or "").strip().upper() == "PASS":
        return 1
    return 2


def _market_readiness_rank(status: str) -> int:
    normalized = str(status or "").strip().upper()
    if normalized == "READY_FOR_PAPER_REVIEW":
        return 0
    if normalized in {"PLANNED_MARKET_CLOSED", "WAIT_MARKET_SESSION", "WAITING_FOR_MARKET_SESSION"}:
        return 1
    if normalized:
        return 2
    return 3


def _first_remediation(row: Mapping[str, Any], *, preferred_reason: str = "") -> str:
    wanted = str(preferred_reason or "").strip()
    if wanted:
        for key in ("hard_block_details", "warning_details"):
            for detail in list(row.get(key) or []):
                if not isinstance(detail, Mapping):
                    continue
                if str(detail.get("reason") or "").strip() != wanted:
                    continue
                remediation = str(detail.get("remediation") or "").strip()
                if remediation:
                    return remediation
    for key in ("hard_block_details", "warning_details"):
        for detail in list(row.get(key) or []):
            if not isinstance(detail, Mapping):
                continue
            remediation = str(detail.get("remediation") or "").strip()
            if remediation:
                return remediation
    return str(row.get("market_readiness_next_action") or "").strip()


def _build_submit_frontier_candidates(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_orders: int,
    max_value: float,
    require_buy: bool = False,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    frontier: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw or {})
        if str(row.get("status") or "").strip().upper() == DISABLED_STATUS:
            continue
        if str(row.get("account_mode") or "paper").strip().lower() != "paper":
            continue
        order_count = _int(row.get("market_readiness_order_count"), 0)
        planned_gross = _float(row.get("market_readiness_planned_gross_order_value"), 0.0)
        planned_buy = _float(row.get("market_readiness_planned_buy_order_value"), 0.0)
        policy_reject_reasons = _submit_policy_reject_reasons(
            order_count,
            planned_gross,
            max_orders=max_orders,
            max_value=max_value,
            planned_buy=planned_buy,
            require_buy=require_buy,
        )
        hard_blocks = [str(value) for value in list(row.get("hard_blocks") or []) if str(value).strip()]
        warnings = [str(value) for value in list(row.get("warnings") or []) if str(value).strip()]
        readiness_status = str(row.get("market_readiness_status") or "").strip().upper()
        if bool(row.get("ready", False)) and readiness_status == "READY_FOR_PAPER_REVIEW" and not policy_reject_reasons:
            frontier_reason = "eligible_candidate"
        elif policy_reject_reasons and not hard_blocks:
            frontier_reason = policy_reject_reasons[0]
        elif hard_blocks:
            frontier_reason = _primary_hard_block_reason(hard_blocks)
        else:
            frontier_reason = str(row.get("primary_reason") or "not_ready").strip() or "not_ready"
        frontier.append(
            {
                **_submit_candidate_base(row, order_count=order_count, planned_gross=planned_gross),
                "status": str(row.get("status") or ""),
                "ready": bool(row.get("ready", False)),
                "frontier_reason": frontier_reason,
                "hard_blocks": hard_blocks,
                "warnings": warnings,
                "policy_reject_reasons": policy_reject_reasons,
                "market_readiness_status": readiness_status,
                "market_readiness_artifact_health_status": str(
                    row.get("market_readiness_artifact_health_status") or ""
                ).strip().upper(),
                "market_readiness_feasibility_status": str(
                    row.get("market_readiness_feasibility_status") or ""
                ).strip().upper(),
                "next_action": _first_remediation(row, preferred_reason=frontier_reason),
            }
        )
    frontier.sort(
        key=lambda item: (
            0
            if int(item.get("order_count", 0) or 0) > 0
            or float(item.get("planned_gross_order_value", 0.0) or 0.0) > 0.0
            else 1,
            _submit_quality_rank(item),
            -float(item.get("submit_quality_min_net_edge_bps", 0.0) or 0.0),
            -float(item.get("submit_quality_min_edge_margin_bps", 0.0) or 0.0),
            _market_readiness_rank(str(item.get("market_readiness_status") or "")),
            len(list(item.get("policy_reject_reasons") or [])),
            len(list(item.get("hard_blocks") or [])),
            float(item.get("planned_gross_order_value", 0.0) or 0.0),
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
        )
    )
    return frontier[: max(1, int(limit))]


def evaluate_auto_order_readiness(
    portfolio: Mapping[str, Any],
    *,
    preflight_summary: Mapping[str, Any] | None = None,
    weekly_summary: Mapping[str, Any] | None = None,
    market_readiness_summary: Mapping[str, Any] | None = None,
    policy: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> Dict[str, Any]:
    """Evaluate whether one portfolio is allowed to submit automated orders."""
    normalized_policy = normalize_auto_order_readiness_policy(policy)
    now_dt = now or datetime.now(timezone.utc)
    row = dict(portfolio or {})
    run_execution = bool(row.get("run_investment_execution", False))
    submit_execution = bool(row.get("submit_investment_execution", False))
    account_mode = str(row.get("account_mode") or "").strip().lower() or "paper"
    market_code = _market(row.get("market"))
    hard_blocks: List[str] = []
    warnings: List[str] = []
    hard_block_details: List[Dict[str, str]] = []
    warning_details: List[Dict[str, str]] = []

    if not run_execution or not submit_execution:
        return {
            **row,
            "status": DISABLED_STATUS,
            "ready": False,
            "primary_reason": "auto_submit_disabled",
            "hard_blocks": [],
            "warnings": [],
            "policy_enabled": bool(normalized_policy.get("enabled", False)),
        }

    if market_code in set(normalized_policy.get("excluded_markets") or []):
        return {
            **row,
            "status": DISABLED_STATUS,
            "ready": False,
            "primary_reason": "auto_submit_market_excluded",
            "hard_blocks": [],
            "warnings": [],
            "policy_enabled": bool(normalized_policy.get("enabled", False)),
            "account_mode": account_mode,
            "market_readiness_status": "",
            "market_readiness_reason": "",
        }

    if account_mode != "paper" and not bool(normalized_policy.get("allow_live_submit", False)):
        hard_blocks.append("live_submit_not_allowed")
        hard_block_details.append(
            _block_detail(
                "live_submit_not_allowed",
                "block",
                f"account_mode={account_mode}",
                "Set allow_live_submit only after live governance approval.",
            )
        )

    preflight = dict(preflight_summary or {})
    preflight_age_hours = _age_hours(preflight.get("generated_at"), now_dt)
    preflight_fail_checks = _preflight_relevant_checks(preflight, row, ["FAIL", "FAILED", "ERROR"])
    preflight_warn_checks = _preflight_relevant_checks(preflight, row, ["WARN", "WARNING"])
    has_preflight_checks = bool(list(preflight.get("checks") or []))
    preflight_fail_count = len(preflight_fail_checks) if has_preflight_checks else _int(preflight.get("fail_count"))
    preflight_warn_count = len(preflight_warn_checks) if has_preflight_checks else _int(preflight.get("warn_count"))
    if bool(normalized_policy.get("block_on_preflight_fail", True)) and preflight_fail_count > 0:
        hard_blocks.append("preflight_failed")
        hard_block_details.append(
            _block_detail(
                "preflight_failed",
                "block",
                ",".join(str(check.get("name") or "unknown") for check in preflight_fail_checks[:5])
                or f"fail_count={preflight_fail_count}",
                "Run ibkr-quant-preflight and fix failing checks for this market/portfolio.",
            )
        )
    if preflight_age_hours is None and bool(normalized_policy.get("block_on_missing_preflight", True)):
        hard_blocks.append("preflight_missing")
        hard_block_details.append(
            _block_detail(
                "preflight_missing",
                "block",
                "preflight generated_at is missing",
                "Run ibkr-quant-preflight before automated submit.",
            )
        )
    elif (
        bool(normalized_policy.get("block_on_stale_preflight", True))
        and preflight_age_hours is not None
        and preflight_age_hours > float(normalized_policy.get("max_preflight_age_hours", 24.0))
    ):
        hard_blocks.append("preflight_stale")
        hard_block_details.append(
            _block_detail(
                "preflight_stale",
                "block",
                f"age_hours={preflight_age_hours}",
                "Refresh supervisor preflight before automated submit.",
            )
        )
    if preflight_warn_count > 0:
        warnings.append("preflight_warn")
        warning_details.append(
            _block_detail(
                "preflight_warn",
                "warning",
                ",".join(str(check.get("name") or "unknown") for check in preflight_warn_checks[:5])
                or f"warn_count={preflight_warn_count}",
                "Review warning checks; paper submit may continue if no hard blocks exist.",
            )
        )

    weekly = dict(weekly_summary or {})
    weekly_age_hours = _age_hours(weekly.get("generated_at"), now_dt)
    if weekly_age_hours is None and bool(normalized_policy.get("block_on_missing_weekly_review", True)):
        hard_blocks.append("weekly_review_missing")
        hard_block_details.append(
            _block_detail(
                "weekly_review_missing",
                "block",
                "weekly review generated_at is missing",
                "Run weekly review before automated submit.",
            )
        )
    elif (
        bool(normalized_policy.get("block_on_stale_weekly_review", True))
        and weekly_age_hours is not None
        and weekly_age_hours > float(normalized_policy.get("max_weekly_review_age_hours", 168.0))
    ):
        hard_blocks.append("weekly_review_stale")
        hard_block_details.append(
            _block_detail(
                "weekly_review_stale",
                "block",
                f"age_hours={weekly_age_hours}",
                "Refresh weekly review before automated submit.",
            )
        )

    strategy_effectiveness = dict(weekly.get("strategy_parameter_suggestion_effectiveness") or {})
    strategy_suggestion_rows = [
        dict(item)
        for item in list(weekly.get("strategy_parameter_suggestions") or [])
        if isinstance(item, Mapping)
    ]
    matching_suggestions = _active_strategy_suggestion_rows(_strategy_suggestion_rows_for_portfolio(weekly, row))
    matching_followups = _strategy_followup_rows_for_portfolio(weekly, row)
    stale_ids = {
        str(value)
        for value in list(strategy_effectiveness.get("stale_suggestion_ids") or [])
        if str(value).strip()
    }
    if strategy_suggestion_rows:
        auto_apply_count = sum(1 for suggestion in matching_suggestions if bool(suggestion.get("auto_apply", False)))
        if list(weekly.get("strategy_parameter_suggestion_followup") or []):
            degraded_followup_count = sum(
                1
                for followup in matching_followups
                if str(followup.get("followup_verdict") or "").strip().upper() == "DEGRADED"
            )
        else:
            degraded_followup_count = 0
        open_suggestion_count = sum(
            1 for suggestion in matching_suggestions if _is_unresolved_strategy_suggestion(suggestion)
        )
        stale_suggestion_count = sum(
            1 for suggestion in matching_suggestions if str(suggestion.get("suggestion_id") or "") in stale_ids
        )
    else:
        auto_apply_count = _int(strategy_effectiveness.get("auto_apply_count"))
        degraded_followup_count = _int(strategy_effectiveness.get("degraded_followup_count"))
        open_suggestion_count = _int(strategy_effectiveness.get("open_suggestion_count"))
        stale_suggestion_count = _int(strategy_effectiveness.get("stale_suggestion_count"))
    if bool(normalized_policy.get("block_on_strategy_auto_apply_violation", True)) and auto_apply_count > 0:
        hard_blocks.append("strategy_auto_apply_violation")
        hard_block_details.append(
            _block_detail(
                "strategy_auto_apply_violation",
                "block",
                f"auto_apply_count={auto_apply_count}",
                "Disable auto-apply and review strategy parameter governance before submit.",
            )
        )
    if bool(normalized_policy.get("block_on_degraded_strategy_followup", True)) and degraded_followup_count > 0:
        hard_blocks.append("strategy_followup_degraded")
        hard_block_details.append(
            _block_detail(
                "strategy_followup_degraded",
                "block",
                f"degraded_followup_count={degraded_followup_count}",
                "Review degraded strategy follow-up before submitting more automated orders.",
            )
        )
    if stale_suggestion_count > 0:
        hard_blocks.append("strategy_suggestion_stale")
        hard_block_details.append(
            _block_detail(
                "strategy_suggestion_stale",
                "block",
                f"stale_suggestion_count={stale_suggestion_count}",
                "Acknowledge, reject, supersede, or apply stale strategy suggestions before submit.",
            )
        )
    if bool(normalized_policy.get("warn_on_open_strategy_suggestions", True)) and open_suggestion_count > 0:
        warnings.append("strategy_suggestions_open")
        warning_details.append(
            _block_detail(
                "strategy_suggestions_open",
                "warning",
                f"open_suggestion_count={open_suggestion_count}",
                "Paper submit may continue; keep tracking open strategy suggestions.",
            )
        )

    gateway_row = _gateway_budget_row(weekly, row)
    gateway_status = _gateway_budget_status(weekly, row)
    if (
        bool(normalized_policy.get("block_on_gateway_budget_degraded", True))
        and gateway_status in {"fail", "failed", "error", "degraded"}
    ):
        hard_blocks.append("gateway_budget_degraded")
        hard_block_details.append(
            _block_detail(
                "gateway_budget_degraded",
                "block",
                _gateway_budget_detail(gateway_row, row, gateway_status),
                _gateway_budget_remediation(gateway_row, blocked=True),
            )
        )
    elif gateway_status in {"warn", "warning"}:
        warnings.append("gateway_budget_warning")
        warning_details.append(
            _block_detail(
                "gateway_budget_warning",
                "warning",
                _gateway_budget_detail(gateway_row, row, gateway_status),
                _gateway_budget_remediation(gateway_row, blocked=False),
            )
        )

    market_readiness = dict(_market_readiness_row(market_readiness_summary, row))
    market_readiness_status = str(market_readiness.get("readiness_status") or "").strip().upper()
    market_readiness_reason = str(market_readiness.get("primary_reason") or "").strip()
    market_artifact_health_status = str(market_readiness.get("artifact_health_status") or "").strip().upper()
    market_feasibility_status = str(market_readiness.get("small_account_feasibility_status") or "").strip().upper()
    market_order_count = _int(market_readiness.get("order_count"), 0)
    market_planned_gross_order_value = _float(market_readiness.get("planned_gross_order_value"), 0.0)
    market_planned_order_symbols = str(market_readiness.get("planned_order_symbols") or "").strip()
    submit_quality_status = str(market_readiness.get("submit_quality_status") or "").strip().upper()
    submit_quality_tier = str(market_readiness.get("submit_quality_tier") or "").strip().upper()
    submit_quality_reason = str(market_readiness.get("submit_quality_reason") or "").strip()
    market_preparation_tier = ""
    for plan_row in list((market_readiness_summary or {}).get("preparation_plan") or []):
        if not isinstance(plan_row, Mapping):
            continue
        if str(plan_row.get("portfolio_id") or "").strip() == str(row.get("portfolio_id") or "").strip():
            market_preparation_tier = str(plan_row.get("priority_tier") or "").strip().upper()
            break
    ready_market_statuses = {"READY_FOR_PAPER_REVIEW"}
    if not market_readiness:
        detail = "market_readiness row missing"
        if bool(normalized_policy.get("block_on_missing_market_readiness", False)):
            hard_blocks.append("market_readiness_missing")
            hard_block_details.append(
                _block_detail(
                    "market_readiness_missing",
                    "block",
                    detail,
                    "Run ibkr-quant-market-readiness before automated submit.",
                )
            )
        elif bool(normalized_policy.get("warn_on_missing_market_readiness", False)):
            warnings.append("market_readiness_missing")
            warning_details.append(
                _block_detail(
                    "market_readiness_missing",
                    "warning",
                    detail,
                    "Run ibkr-quant-market-readiness before ranking ASX/HK/XETRA/US paper submit readiness.",
                )
            )
    elif (
        bool(normalized_policy.get("block_on_market_readiness_not_ready", True))
        and market_readiness_status not in ready_market_statuses
    ):
        if market_readiness_reason.upper() == "IBKR_GATEWAY_UNAVAILABLE" or market_artifact_health_status == "DEGRADED_GATEWAY":
            hard_blocks.append("ibkr_gateway_unavailable")
            hard_block_details.append(
                _block_detail(
                    "ibkr_gateway_unavailable",
                    "block",
                    (
                        f"status={market_readiness_status or '-'} reason={market_readiness_reason or '-'} "
                        f"artifact={market_artifact_health_status or '-'}"
                    ),
                    "Start or unlock IB Gateway paper API, confirm the configured port is listening, then rerun no-submit.",
                )
            )
        hard_blocks.append("market_readiness_not_ready")
        hard_block_details.append(
            _block_detail(
                "market_readiness_not_ready",
                "block",
                (
                    f"status={market_readiness_status} reason={market_readiness_reason or '-'} "
                    f"artifact={market_artifact_health_status or '-'} feasibility={market_feasibility_status or '-'}"
                ),
                str(market_readiness.get("next_action") or "Refresh market readiness before automated submit."),
            )
        )
    elif market_artifact_health_status in {"STALE", "DEGRADED_GATEWAY", "MISSING"}:
        warnings.append("market_readiness_artifact_health")
        warning_details.append(
            _block_detail(
                "market_readiness_artifact_health",
                "warning",
                f"artifact={market_artifact_health_status}",
                "Review market readiness artifact health before submit.",
            )
        )
    if (
        bool(normalized_policy.get("block_on_submit_quality_not_pass", True))
        and market_order_count > 0
        and market_readiness_status == "READY_FOR_PAPER_REVIEW"
        and submit_quality_status != "PASS"
    ):
        hard_blocks.append("submit_quality_not_pass")
        hard_block_details.append(
            _block_detail(
                "submit_quality_not_pass",
                "block",
                (
                    f"status={submit_quality_status or 'MISSING'} reason={submit_quality_reason or '-'} "
                    f"net_edge={_float(market_readiness.get('submit_quality_min_net_edge_bps'), 0.0):.2f}bps "
                    f"margin={_float(market_readiness.get('submit_quality_min_edge_margin_bps'), 0.0):.2f}bps "
                    f"cost={_float(market_readiness.get('submit_quality_max_expected_cost_bps'), 0.0):.2f}bps "
                    f"types={market_readiness.get('submit_quality_order_types') or '-'}"
                ),
                "Refresh execution dry-run or improve edge/cost/order-type quality before automated submit.",
            )
        )

    ready = not hard_blocks
    status = READY_STATUS if ready and not warnings else WARNING_STATUS if ready else BLOCKED_STATUS
    primary_reason = "ready" if ready else _primary_hard_block_reason(hard_blocks)
    return {
        **row,
        "status": status,
        "ready": ready,
        "primary_reason": primary_reason,
        "hard_blocks": hard_blocks,
        "warnings": warnings,
        "hard_block_details": hard_block_details,
        "warning_details": warning_details,
        "policy_enabled": bool(normalized_policy.get("enabled", False)),
        "account_mode": account_mode,
        "preflight_age_hours": preflight_age_hours,
        "preflight_fail_count": preflight_fail_count,
        "preflight_warn_count": preflight_warn_count,
        "weekly_review_age_hours": weekly_age_hours,
        "strategy_open_suggestion_count": open_suggestion_count,
        "strategy_degraded_followup_count": degraded_followup_count,
        "strategy_auto_apply_count": auto_apply_count,
        "strategy_stale_suggestion_count": stale_suggestion_count,
        "gateway_budget_status": gateway_status or "",
        "market_readiness_status": market_readiness_status,
        "market_readiness_reason": market_readiness_reason,
        "market_readiness_artifact_health_status": market_artifact_health_status,
        "market_readiness_feasibility_status": market_feasibility_status,
        "market_readiness_preparation_tier": market_preparation_tier,
        "market_readiness_order_count": market_order_count,
        "market_readiness_planned_gross_order_value": market_planned_gross_order_value,
        "market_readiness_planned_buy_order_value": _float(market_readiness.get("planned_buy_order_value"), 0.0),
        "market_readiness_planned_sell_order_value": _float(market_readiness.get("planned_sell_order_value"), 0.0),
        "market_readiness_planned_net_cash_order_value": _float(
            market_readiness.get("planned_net_cash_order_value"),
            0.0,
        ),
        "market_readiness_planned_order_symbols": market_planned_order_symbols,
        "market_readiness_next_action": str(market_readiness.get("next_action") or ""),
        "submit_quality_status": submit_quality_status,
        "submit_quality_tier": submit_quality_tier,
        "submit_quality_reason": submit_quality_reason,
        "submit_quality_min_net_edge_bps": _float(market_readiness.get("submit_quality_min_net_edge_bps"), 0.0),
        "submit_quality_min_edge_margin_bps": _float(market_readiness.get("submit_quality_min_edge_margin_bps"), 0.0),
        "submit_quality_max_expected_cost_bps": _float(market_readiness.get("submit_quality_max_expected_cost_bps"), 0.0),
        "submit_quality_order_types": str(market_readiness.get("submit_quality_order_types") or ""),
    }


def build_auto_order_submit_plan(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_policy = normalize_auto_order_readiness_policy(policy)
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    if not bool(normalized_policy.get("enabled", False)):
        return {
            "status": "DISABLED",
            "ready": False,
            "reason": "auto_order_readiness_policy_disabled",
            "candidate_count": 0,
            "candidate_portfolios": [],
            "frontier_candidate_count": 0,
            "frontier_candidates": [],
            "selected_portfolio_id": "",
            "submit_mode": "none",
        }

    eligible_rows = [
        row
        for row in clean_rows
        if bool(row.get("ready", False))
        and str(row.get("account_mode") or "paper").strip().lower() == "paper"
        and str(row.get("market_readiness_status") or "").strip().upper() == "READY_FOR_PAPER_REVIEW"
    ]
    max_portfolios = max(1, _int(normalized_policy.get("max_submit_portfolios_per_run"), 1))
    max_per_market = max(1, _int(normalized_policy.get("max_submit_portfolios_per_market"), 1))
    max_orders = max(1, _int(normalized_policy.get("max_submit_orders_per_portfolio"), 1))
    max_value = max(0.0, _float(normalized_policy.get("max_submit_gross_order_value"), 100.0))
    max_total_value = max(0.0, _float(normalized_policy.get("max_submit_total_gross_order_value"), 0.0))
    require_buy = bool(normalized_policy.get("require_buy_order_for_submit", False))
    frontier_candidates = _build_submit_frontier_candidates(
        clean_rows,
        max_orders=max_orders,
        max_value=max_value,
        require_buy=require_buy,
    )
    candidate_rows: List[Dict[str, Any]] = []
    rejection_rows: List[Dict[str, Any]] = []
    for row in eligible_rows:
        order_count = _int(row.get("market_readiness_order_count"), 0)
        planned_gross = _float(row.get("market_readiness_planned_gross_order_value"), 0.0)
        planned_buy = _float(row.get("market_readiness_planned_buy_order_value"), 0.0)
        reject_reasons = _submit_policy_reject_reasons(
            order_count,
            planned_gross,
            max_orders=max_orders,
            max_value=max_value,
            planned_buy=planned_buy,
            require_buy=require_buy,
        )
        base = _submit_candidate_base(row, order_count=order_count, planned_gross=planned_gross)
        if reject_reasons:
            rejection_rows.append({**base, "reject_reasons": reject_reasons})
        else:
            candidate_rows.append(base)

    candidate_rows.sort(
        key=lambda row: (
            _submit_quality_rank(row),
            -float(row.get("submit_quality_min_net_edge_bps", 0.0) or 0.0),
            -float(row.get("submit_quality_min_edge_margin_bps", 0.0) or 0.0),
            float(row.get("planned_gross_order_value", 0.0) or 0.0),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
        )
    )
    market_counts: Dict[str, int] = {}
    market_limited_rows: List[Dict[str, Any]] = []
    for candidate in candidate_rows:
        market = _market(candidate.get("market"))
        current_count = int(market_counts.get(market, 0))
        if current_count >= max_per_market:
            rejection_rows.append({**candidate, "reject_reasons": ["market_portfolio_count_exceeds_policy"]})
            continue
        market_counts[market] = current_count + 1
        market_limited_rows.append(candidate)
    if not candidate_rows:
        return {
            "status": "BLOCKED",
            "ready": False,
            "reason": "no_single_safe_submit_candidate",
            "candidate_count": 0,
            "candidate_portfolios": [],
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "submit_mode": "none",
            "policy": {
                "max_submit_portfolios_per_run": int(max_portfolios),
                "max_submit_portfolios_per_market": int(max_per_market),
                "max_submit_orders_per_portfolio": int(max_orders),
                "max_submit_gross_order_value": float(max_value),
                "max_submit_total_gross_order_value": float(max_total_value),
                "require_buy_order_for_submit": bool(require_buy),
                "excluded_markets": list(normalized_policy.get("excluded_markets") or []),
                "block_on_submit_quality_not_pass": bool(
                    normalized_policy.get("block_on_submit_quality_not_pass", True)
                ),
            },
        }
    if len(market_limited_rows) > max_portfolios:
        return {
            "status": "REVIEW_REQUIRED",
            "ready": False,
            "reason": "multiple_submit_candidates_require_operator_selection",
            "candidate_count": int(len(market_limited_rows)),
            "candidate_portfolios": candidate_rows,
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "submit_mode": "operator_select_one",
            "policy": {
                "max_submit_portfolios_per_run": int(max_portfolios),
                "max_submit_portfolios_per_market": int(max_per_market),
                "max_submit_orders_per_portfolio": int(max_orders),
                "max_submit_gross_order_value": float(max_value),
                "max_submit_total_gross_order_value": float(max_total_value),
                "require_buy_order_for_submit": bool(require_buy),
                "excluded_markets": list(normalized_policy.get("excluded_markets") or []),
                "block_on_submit_quality_not_pass": bool(
                    normalized_policy.get("block_on_submit_quality_not_pass", True)
                ),
            },
        }
    selected_rows = [dict(row) for row in market_limited_rows]
    selected_total_gross = round(
        sum(float(row.get("planned_gross_order_value", 0.0) or 0.0) for row in selected_rows),
        2,
    )
    if max_total_value > 0.0 and selected_total_gross > max_total_value + 1e-9:
        return {
            "status": "REVIEW_REQUIRED",
            "ready": False,
            "reason": "submit_total_gross_value_exceeds_policy",
            "candidate_count": int(len(selected_rows)),
            "candidate_portfolios": candidate_rows,
            "rejected_candidates": rejection_rows,
            "frontier_candidate_count": len(frontier_candidates),
            "frontier_candidates": frontier_candidates,
            "selected_portfolio_id": "",
            "selected_portfolio_ids": [],
            "submit_mode": "operator_reduce_total_exposure",
            "selected_total_planned_gross_order_value": float(selected_total_gross),
            "policy": {
                "max_submit_portfolios_per_run": int(max_portfolios),
                "max_submit_portfolios_per_market": int(max_per_market),
                "max_submit_orders_per_portfolio": int(max_orders),
                "max_submit_gross_order_value": float(max_value),
                "max_submit_total_gross_order_value": float(max_total_value),
                "require_buy_order_for_submit": bool(require_buy),
                "excluded_markets": list(normalized_policy.get("excluded_markets") or []),
                "block_on_submit_quality_not_pass": bool(
                    normalized_policy.get("block_on_submit_quality_not_pass", True)
                ),
            },
        }
    selected = dict(selected_rows[0])
    selected_portfolio_ids = [str(row.get("portfolio_id") or "") for row in selected_rows if str(row.get("portfolio_id") or "")]
    selected_markets = [str(row.get("market") or "") for row in selected_rows if str(row.get("market") or "")]
    multi = len(selected_rows) > 1
    return {
        "status": "READY_MULTI_CANDIDATE" if multi else "READY_SINGLE_CANDIDATE",
        "ready": True,
        "reason": "multi_market_safe_paper_submit_candidates" if multi else "single_safe_paper_submit_candidate",
        "candidate_count": int(len(selected_rows)),
        "candidate_portfolios": candidate_rows,
        "rejected_candidates": rejection_rows,
        "frontier_candidate_count": len(frontier_candidates),
        "frontier_candidates": frontier_candidates,
        "selected_market": str(selected.get("market") or ""),
        "selected_markets": selected_markets,
        "selected_portfolio_id": str(selected.get("portfolio_id") or ""),
        "selected_portfolio_ids": selected_portfolio_ids,
        "selected_portfolios": selected_rows,
        "selected_order_count": int(selected.get("order_count", 0) or 0),
        "selected_total_order_count": int(sum(int(row.get("order_count", 0) or 0) for row in selected_rows)),
        "selected_planned_gross_order_value": float(selected.get("planned_gross_order_value", 0.0) or 0.0),
        "selected_total_planned_gross_order_value": float(selected_total_gross),
        "selected_planned_order_symbols": str(selected.get("planned_order_symbols") or ""),
        "submit_mode": "paper_multi_market_small_plan" if multi else "paper_one_portfolio_one_small_plan",
        "policy": {
            "max_submit_portfolios_per_run": int(max_portfolios),
            "max_submit_portfolios_per_market": int(max_per_market),
            "max_submit_orders_per_portfolio": int(max_orders),
            "max_submit_gross_order_value": float(max_value),
            "max_submit_total_gross_order_value": float(max_total_value),
            "require_buy_order_for_submit": bool(require_buy),
            "excluded_markets": list(normalized_policy.get("excluded_markets") or []),
            "block_on_submit_quality_not_pass": bool(normalized_policy.get("block_on_submit_quality_not_pass", True)),
        },
    }


def build_auto_order_readiness_summary(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    clean_rows = [dict(row) for row in list(rows or []) if isinstance(row, Mapping)]
    ready_rows = [row for row in clean_rows if bool(row.get("ready", False))]
    blocked_rows = [row for row in clean_rows if str(row.get("status") or "") == BLOCKED_STATUS]
    warning_rows = [row for row in clean_rows if str(row.get("status") or "") == WARNING_STATUS]
    disabled_rows = [row for row in clean_rows if str(row.get("status") or "") == DISABLED_STATUS]
    status = "blocked" if blocked_rows else "warning" if warning_rows else "ready"
    hard_block_counts: Dict[str, int] = {}
    warning_counts: Dict[str, int] = {}
    remediation_by_reason: Dict[str, str] = {}
    detail_by_reason: Dict[str, str] = {}
    markets_by_reason: Dict[str, set[str]] = {}
    portfolios_by_reason: Dict[str, set[str]] = {}
    for row in clean_rows:
        market = str(row.get("market") or "").strip().upper()
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        for reason in list(row.get("hard_blocks") or []):
            reason_text = str(reason or "").strip()
            if not reason_text:
                continue
            hard_block_counts[reason_text] = hard_block_counts.get(reason_text, 0) + 1
            markets_by_reason.setdefault(reason_text, set()).add(market)
            portfolios_by_reason.setdefault(reason_text, set()).add(portfolio_id)
        for reason in list(row.get("warnings") or []):
            reason_text = str(reason or "").strip()
            if not reason_text:
                continue
            warning_counts[reason_text] = warning_counts.get(reason_text, 0) + 1
            markets_by_reason.setdefault(reason_text, set()).add(market)
            portfolios_by_reason.setdefault(reason_text, set()).add(portfolio_id)
        for detail in list(row.get("hard_block_details") or []) + list(row.get("warning_details") or []):
            if not isinstance(detail, Mapping):
                continue
            reason_text = str(detail.get("reason") or "").strip()
            if not reason_text:
                continue
            remediation = str(detail.get("remediation") or "").strip()
            detail_text = str(detail.get("detail") or "").strip()
            remediation_by_reason.setdefault(reason_text, remediation)
            detail_by_reason.setdefault(reason_text, detail_text)

    remediation_plan: List[Dict[str, Any]] = []
    for reason, count in hard_block_counts.items():
        remediation_plan.append(
            {
                "reason": reason,
                "severity": "block",
                "priority": _reason_priority(reason),
                "affected_portfolio_count": int(count),
                "affected_markets": sorted(value for value in markets_by_reason.get(reason, set()) if value),
                "affected_portfolios": sorted(value for value in portfolios_by_reason.get(reason, set()) if value),
                "detail": detail_by_reason.get(reason, ""),
                "remediation": remediation_by_reason.get(reason, ""),
            }
        )
    for reason, count in warning_counts.items():
        remediation_plan.append(
            {
                "reason": reason,
                "severity": "warning",
                "priority": _reason_priority(reason, warning=True),
                "affected_portfolio_count": int(count),
                "affected_markets": sorted(value for value in markets_by_reason.get(reason, set()) if value),
                "affected_portfolios": sorted(value for value in portfolios_by_reason.get(reason, set()) if value),
                "detail": detail_by_reason.get(reason, ""),
                "remediation": remediation_by_reason.get(reason, ""),
            }
        )
    remediation_plan.sort(
        key=lambda item: (
            int(item.get("priority", 999)),
            -int(item.get("affected_portfolio_count", 0)),
            str(item.get("reason") or ""),
        )
    )
    primary_block_reason = (
        str(remediation_plan[0].get("reason") or "")
        if remediation_plan and str(remediation_plan[0].get("severity") or "") == "block"
        else str(blocked_rows[0].get("primary_reason") or "") if blocked_rows else ""
    )
    return {
        "status": status,
        "summary_text": (
            f"auto_order_readiness portfolios={len(clean_rows)} ready={len(ready_rows)} "
            f"warning={len(warning_rows)} blocked={len(blocked_rows)} disabled={len(disabled_rows)}"
        ),
        "portfolio_count": len(clean_rows),
        "ready_count": len(ready_rows),
        "warning_count": len(warning_rows),
        "blocked_count": len(blocked_rows),
        "disabled_count": len(disabled_rows),
        "primary_block_reason": primary_block_reason,
        "hard_block_counts": dict(sorted(hard_block_counts.items(), key=lambda item: (_reason_priority(item[0]), item[0]))),
        "warning_counts": dict(sorted(warning_counts.items(), key=lambda item: (_reason_priority(item[0], warning=True), item[0]))),
        "remediation_plan": remediation_plan,
        "submit_plan": build_auto_order_submit_plan(clean_rows, policy=policy),
    }
