from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from math import ceil
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml

from .freshness import age_hours_from_timestamp, parse_utc_datetime
from .ibkr_telemetry import infer_ibkr_request_lane


DEFAULT_IBKR_GATEWAY_BUDGET_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "default_weekly_gateway_request_budget": 1500,
    "stale_telemetry_warning_hours": 72,
    "over_budget_degraded_ratio": 1.5,
    "missing_telemetry_status": "warning",
    "execution_reserve_ratio": 0.15,
    "protective_reserve_ratio": 0.40,
    "minimum_execution_reserve_requests": 50,
    "research_daily_burst_ratio": 1.0,
    "short_window_minutes": 10,
    "short_window_request_limit": 50,
    "short_window_execution_reserve": 10,
    "markets": {},
}


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or str(value).strip() == "":
        return int(default)
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_ts(value: Any) -> datetime | None:
    return parse_utc_datetime(value)


def _parse_date(value: Any) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        parsed = _parse_ts(raw)
        return parsed.date() if parsed is not None else None


def _status_rank(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized == "degraded":
        return 3
    if normalized == "warning":
        return 2
    if normalized == "disabled":
        return 0
    return 1


def _worst_status(rows: Iterable[Dict[str, Any]]) -> str:
    worst = "ok"
    for row in rows:
        status = str(row.get("status") or "ok").strip().lower()
        if _status_rank(status) > _status_rank(worst):
            worst = status
    return worst


def normalize_ibkr_gateway_budget_config(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = dict(raw or {})
    cfg = dict(DEFAULT_IBKR_GATEWAY_BUDGET_CONFIG)
    cfg.update({k: v for k, v in raw.items() if k != "markets"})
    raw_markets = raw.get("markets") if isinstance(raw.get("markets"), dict) else {}
    markets: Dict[str, Dict[str, Any]] = {}
    for market, market_cfg in dict(raw_markets or {}).items():
        market_code = str(market or "").upper().strip()
        if not market_code:
            continue
        markets[market_code] = dict(market_cfg or {}) if isinstance(market_cfg, dict) else {}
    cfg["markets"] = markets
    cfg["enabled"] = bool(cfg.get("enabled", True))
    cfg["default_weekly_gateway_request_budget"] = max(
        0,
        _safe_int(cfg.get("default_weekly_gateway_request_budget"), 1500),
    )
    cfg["stale_telemetry_warning_hours"] = max(
        0.0,
        _safe_float(cfg.get("stale_telemetry_warning_hours"), 72.0),
    )
    cfg["over_budget_degraded_ratio"] = max(
        1.0,
        _safe_float(cfg.get("over_budget_degraded_ratio"), 1.5),
    )
    cfg["execution_reserve_ratio"] = min(
        0.5,
        max(0.0, _safe_float(cfg.get("execution_reserve_ratio"), 0.15)),
    )
    cfg["protective_reserve_ratio"] = min(
        0.8,
        max(0.0, _safe_float(cfg.get("protective_reserve_ratio"), 0.40)),
    )
    cfg["minimum_execution_reserve_requests"] = max(
        1,
        _safe_int(cfg.get("minimum_execution_reserve_requests"), 50),
    )
    cfg["research_daily_burst_ratio"] = max(
        0.1,
        _safe_float(cfg.get("research_daily_burst_ratio"), 1.0),
    )
    cfg["short_window_minutes"] = max(1, _safe_int(cfg.get("short_window_minutes"), 10))
    cfg["short_window_request_limit"] = max(1, _safe_int(cfg.get("short_window_request_limit"), 50))
    cfg["short_window_execution_reserve"] = max(
        1,
        _safe_int(cfg.get("short_window_execution_reserve"), 10),
    )
    missing_status = str(cfg.get("missing_telemetry_status") or "warning").strip().lower()
    cfg["missing_telemetry_status"] = missing_status if missing_status in {"ok", "warning", "degraded"} else "warning"
    return cfg


def load_ibkr_gateway_budget_config(
    base_dir: Path,
    *,
    supervisor_config_path: str = "config/supervisor.yaml",
) -> Dict[str, Any]:
    path = base_dir / supervisor_config_path
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        payload = {}
    raw = payload.get("ibkr_gateway_budgets") if isinstance(payload, dict) else {}
    return normalize_ibkr_gateway_budget_config(raw if isinstance(raw, dict) else {})


def _market_budget(config: Dict[str, Any], market: str) -> int:
    market_cfg = dict(dict(config.get("markets") or {}).get(str(market or "").upper(), {}) or {})
    return max(
        0,
        _safe_int(
            market_cfg.get("weekly_gateway_request_budget"),
            _safe_int(config.get("default_weekly_gateway_request_budget"), 1500),
        ),
    )


def _market_lane_budgets(config: Dict[str, Any], market: str, total_budget: int) -> Dict[str, int]:
    market_cfg = dict(dict(config.get("markets") or {}).get(str(market or "").upper(), {}) or {})
    execution_budget = min(
        max(1, total_budget),
        max(
            1,
            _safe_int(
                market_cfg.get("execution_reserve_weekly_requests"),
                max(
                    _safe_int(config.get("minimum_execution_reserve_requests"), 50),
                    int(round(total_budget * _safe_float(config.get("execution_reserve_ratio"), 0.15))),
                ),
            ),
        ),
    )
    protective_budget = max(
        0,
        _safe_int(
            market_cfg.get("protective_reserve_weekly_requests"),
            int(round(total_budget * _safe_float(config.get("protective_reserve_ratio"), 0.40))),
        ),
    )
    if execution_budget + protective_budget > total_budget:
        protective_budget = max(0, total_budget - execution_budget)
    research_budget = max(0, total_budget - execution_budget - protective_budget)
    research_daily_budget = max(
        1,
        int(
            ceil(
                (research_budget / 7.0)
                * _safe_float(config.get("research_daily_burst_ratio"), 1.0)
            )
        ),
    )
    return {
        "execution": execution_budget,
        "protective": protective_budget,
        "research": research_budget,
        "research_daily": research_daily_budget,
    }


def _group_request_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw or {})
        market = str(row.get("market") or "UNKNOWN").upper().strip() or "UNKNOWN"
        bucket = grouped.setdefault(
            market,
            {
                "market": market,
                "event_count": 0,
                "gateway_request_count": 0,
                "cache_hit_count": 0,
                "by_request_kind": {},
                "by_tool": {},
                "by_request_lane": {},
                "by_date_gateway": {},
                "latest_event_ts": "",
            },
        )
        event_count = _safe_int(row.get("event_count"))
        gateway_count = _safe_int(row.get("gateway_request_count"))
        cache_count = _safe_int(row.get("cache_hit_count"))
        bucket["event_count"] += event_count
        bucket["gateway_request_count"] += gateway_count
        bucket["cache_hit_count"] += cache_count
        kind = str(row.get("request_kind") or "unknown").lower().strip() or "unknown"
        tool = str(row.get("tool") or "unknown").strip() or "unknown"
        lane = str(
            row.get("request_lane")
            or infer_ibkr_request_lane(tool, kind)
            or "unknown"
        ).lower().strip() or "unknown"
        bucket["by_request_kind"][kind] = int(bucket["by_request_kind"].get(kind, 0)) + gateway_count
        bucket["by_tool"][tool] = int(bucket["by_tool"].get(tool, 0)) + gateway_count
        bucket["by_request_lane"][lane] = int(bucket["by_request_lane"].get(lane, 0)) + gateway_count
        request_date = _parse_date(row.get("date") or row.get("latest_event_ts"))
        if request_date is not None:
            date_key = request_date.isoformat()
            bucket["by_date_gateway"][date_key] = int(bucket["by_date_gateway"].get(date_key, 0)) + gateway_count
        row_ts = _parse_ts(row.get("latest_event_ts") or row.get("date"))
        current_ts = _parse_ts(bucket.get("latest_event_ts"))
        if row_ts is not None and (current_ts is None or row_ts > current_ts):
            bucket["latest_event_ts"] = row_ts.isoformat()
    return grouped


def _top_key(counts: Dict[str, int]) -> str:
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-int(item[1]), item[0]))[0][0]


def _gateway_budget_recovery_projection(
    *,
    gateway_count: int,
    budget: int,
    by_date_gateway: Dict[str, int],
    generated_dt: datetime,
) -> Dict[str, Any]:
    excess = max(0, int(gateway_count) - int(budget))
    daily_budget = round((float(budget) / 7.0), 2) if budget > 0 else 0.0
    if excess <= 0:
        return {
            "excess_gateway_requests": 0,
            "daily_gateway_request_budget": daily_budget,
            "projected_recovery_days": 0,
            "projected_recovery_at": "",
        }

    generated_dt = generated_dt.astimezone(timezone.utc)
    remaining = int(gateway_count)
    for day_text, count in sorted(dict(by_date_gateway or {}).items()):
        request_day = _parse_date(day_text)
        if request_day is None:
            continue
        remaining -= max(0, _safe_int(count))
        if remaining <= budget:
            recovery_dt = datetime.combine(request_day + timedelta(days=7), time.max, tzinfo=timezone.utc)
            if recovery_dt < generated_dt:
                recovery_dt = generated_dt
            days = int(ceil(max(0.0, (recovery_dt - generated_dt).total_seconds()) / 86400.0))
            return {
                "excess_gateway_requests": int(excess),
                "daily_gateway_request_budget": daily_budget,
                "projected_recovery_days": int(days),
                "projected_recovery_at": recovery_dt.isoformat(),
            }

    fallback_days = int(ceil(float(excess) / max(daily_budget, 1.0)))
    recovery_dt = generated_dt + timedelta(days=fallback_days)
    return {
        "excess_gateway_requests": int(excess),
        "daily_gateway_request_budget": daily_budget,
        "projected_recovery_days": int(fallback_days),
        "projected_recovery_at": recovery_dt.isoformat(),
    }


def build_ibkr_gateway_budget_rows(
    request_summary_rows: Iterable[Dict[str, Any]],
    *,
    config: Dict[str, Any] | None = None,
    generated_at: str | datetime,
    window_start: str | datetime = "",
    window_end: str | datetime = "",
    recent_24h_rows: Iterable[Dict[str, Any]] | None = None,
    recent_short_rows: Iterable[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    cfg = normalize_ibkr_gateway_budget_config(config)
    if not bool(cfg.get("enabled", True)):
        return [
            {
                "market": "ALL",
                "status": "disabled",
                "reason": "ibkr_gateway_budgets_disabled",
                "weekly_gateway_request_budget": 0,
                "gateway_request_count": 0,
                "cache_hit_count": 0,
                "event_count": 0,
                "cache_hit_ratio": 0.0,
                "budget_usage_pct": 0.0,
                "excess_gateway_requests": 0,
                "daily_gateway_request_budget": 0.0,
                "projected_recovery_days": 0,
                "projected_recovery_at": "",
                "telemetry_age_hours": 0.0,
                "top_request_kind": "",
                "top_tool": "",
                "submit_blocking": False,
                "execution_capacity_status": "disabled",
                "research_throttled": False,
                "generated_at": str(generated_at or ""),
                "window_start": str(window_start or ""),
                "window_end": str(window_end or ""),
            }
        ]

    generated_dt = _parse_ts(generated_at) or datetime.now(timezone.utc)
    grouped = _group_request_rows(request_summary_rows)
    recent_24h_grouped = _group_request_rows(recent_24h_rows or [])
    recent_short_grouped = _group_request_rows(recent_short_rows or [])
    markets = sorted(grouped.keys())
    if not markets:
        return [
            {
                "market": "ALL",
                "status": str(cfg.get("missing_telemetry_status") or "warning"),
                "reason": "missing_ibkr_request_telemetry",
                "weekly_gateway_request_budget": _safe_int(cfg.get("default_weekly_gateway_request_budget"), 1500),
                "gateway_request_count": 0,
                "cache_hit_count": 0,
                "event_count": 0,
                "cache_hit_ratio": 0.0,
                "budget_usage_pct": 0.0,
                "excess_gateway_requests": 0,
                "daily_gateway_request_budget": round(
                    _safe_int(cfg.get("default_weekly_gateway_request_budget"), 1500) / 7.0,
                    2,
                ),
                "projected_recovery_days": 0,
                "projected_recovery_at": "",
                "telemetry_age_hours": 0.0,
                "top_request_kind": "",
                "top_tool": "",
                "submit_blocking": False,
                "execution_capacity_status": "warning",
                "research_throttled": False,
                "latest_event_ts": "",
                "generated_at": generated_dt.isoformat(),
                "window_start": str(window_start or ""),
                "window_end": str(window_end or ""),
            }
        ]

    rows: List[Dict[str, Any]] = []
    stale_hours = _safe_float(cfg.get("stale_telemetry_warning_hours"), 72.0)
    degraded_ratio = _safe_float(cfg.get("over_budget_degraded_ratio"), 1.5)
    for market in markets:
        bucket = dict(grouped.get(market) or {"market": market})
        gateway_count = _safe_int(bucket.get("gateway_request_count"))
        cache_count = _safe_int(bucket.get("cache_hit_count"))
        event_count = _safe_int(bucket.get("event_count"))
        budget = _market_budget(cfg, market)
        lane_budgets = _market_lane_budgets(cfg, market, budget)
        lane_counts = dict(bucket.get("by_request_lane") or {})
        recent_24h_counts = dict(
            dict(recent_24h_grouped.get(market) or {}).get("by_request_lane") or {}
        )
        recent_short_bucket = dict(recent_short_grouped.get(market) or {})
        recent_short_counts = dict(recent_short_bucket.get("by_request_lane") or {})
        execution_count = _safe_int(lane_counts.get("execution"))
        protective_count = _safe_int(lane_counts.get("protective"))
        research_count = _safe_int(lane_counts.get("research"))
        unknown_count = _safe_int(lane_counts.get("unknown"))
        execution_capacity_used = execution_count + unknown_count
        recent_research_count = _safe_int(recent_24h_counts.get("research"))
        recent_short_total = _safe_int(recent_short_bucket.get("gateway_request_count"))
        recent_short_research = _safe_int(recent_short_counts.get("research"))
        recent_short_execution = _safe_int(recent_short_counts.get("execution")) + _safe_int(
            recent_short_counts.get("unknown")
        )
        short_limit = _safe_int(cfg.get("short_window_request_limit"), 50)
        short_execution_reserve = min(
            short_limit,
            _safe_int(cfg.get("short_window_execution_reserve"), 10),
        )
        short_research_limit = max(1, short_limit - short_execution_reserve)
        execution_capacity_status = "ok"
        execution_capacity_reason = "execution_reserve_available"
        if execution_capacity_used >= lane_budgets["execution"]:
            execution_capacity_status = "degraded"
            execution_capacity_reason = "execution_weekly_reserve_exhausted"
        elif recent_short_execution >= short_execution_reserve:
            execution_capacity_status = "degraded"
            execution_capacity_reason = "short_window_execution_reserve_exhausted"
        research_throttled = bool(
            recent_research_count >= lane_budgets["research_daily"]
            or recent_short_research >= short_research_limit
        )
        recovery = _gateway_budget_recovery_projection(
            gateway_count=gateway_count,
            budget=budget,
            by_date_gateway=dict(bucket.get("by_date_gateway") or {}),
            generated_dt=generated_dt,
        )
        usage_pct = round((gateway_count / budget) * 100.0, 2) if budget > 0 else 0.0
        cache_hit_ratio = round(cache_count / event_count, 4) if event_count > 0 else 0.0
        latest_ts = _parse_ts(bucket.get("latest_event_ts"))
        age_hours = age_hours_from_timestamp(latest_ts.isoformat(), generated_dt) if latest_ts else 0.0

        status = "ok"
        reason = "under_budget"
        if event_count <= 0:
            status = str(cfg.get("missing_telemetry_status") or "warning")
            reason = "missing_market_telemetry"
        elif budget > 0 and gateway_count > budget:
            status = "degraded" if gateway_count >= budget * degraded_ratio else "warning"
            reason = "gateway_request_budget_exceeded"
        elif latest_ts is not None and stale_hours > 0 and age_hours > stale_hours:
            status = "warning"
            reason = "stale_ibkr_request_telemetry"

        rows.append(
            {
                "market": market,
                "status": status,
                "reason": reason,
                "weekly_gateway_request_budget": int(budget),
                "gateway_request_count": int(gateway_count),
                "cache_hit_count": int(cache_count),
                "event_count": int(event_count),
                "cache_hit_ratio": cache_hit_ratio,
                "budget_usage_pct": usage_pct,
                "excess_gateway_requests": int(recovery.get("excess_gateway_requests", 0)),
                "daily_gateway_request_budget": float(recovery.get("daily_gateway_request_budget", 0.0)),
                "projected_recovery_days": int(recovery.get("projected_recovery_days", 0)),
                "projected_recovery_at": str(recovery.get("projected_recovery_at") or ""),
                "telemetry_age_hours": age_hours,
                "top_request_kind": _top_key(dict(bucket.get("by_request_kind") or {})),
                "top_tool": _top_key(dict(bucket.get("by_tool") or {})),
                "research_gateway_request_count": int(research_count),
                "protective_gateway_request_count": int(protective_count),
                "execution_gateway_request_count": int(execution_count),
                "unknown_gateway_request_count": int(unknown_count),
                "execution_reserve_weekly_requests": int(lane_budgets["execution"]),
                "protective_reserve_weekly_requests": int(lane_budgets["protective"]),
                "research_weekly_request_budget": int(lane_budgets["research"]),
                "research_recent_24h_request_count": int(recent_research_count),
                "research_daily_request_budget": int(lane_budgets["research_daily"]),
                "short_window_minutes": int(_safe_int(cfg.get("short_window_minutes"), 10)),
                "short_window_gateway_request_count": int(recent_short_total),
                "short_window_execution_request_count": int(recent_short_execution),
                "short_window_request_limit": int(short_limit),
                "short_window_execution_reserve": int(short_execution_reserve),
                "execution_capacity_status": execution_capacity_status,
                "execution_capacity_reason": execution_capacity_reason,
                "submit_blocking": execution_capacity_status == "degraded",
                "research_throttled": research_throttled,
                "latest_event_ts": str(bucket.get("latest_event_ts") or ""),
                "generated_at": generated_dt.isoformat(),
                "window_start": str(window_start or ""),
                "window_end": str(window_end or ""),
            }
        )
    rows.sort(key=lambda row: (_status_rank(str(row.get("status") or "ok")) * -1, str(row.get("market") or "")))
    return rows


def build_ibkr_gateway_budget_payload(
    *,
    generated_at: str,
    week_label: str,
    window_start: str,
    window_end: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    status = _worst_status(rows)
    gateway_count = sum(_safe_int(row.get("gateway_request_count")) for row in rows)
    cache_count = sum(_safe_int(row.get("cache_hit_count")) for row in rows)
    event_count = sum(_safe_int(row.get("event_count")) for row in rows)
    over_budget_count = sum(1 for row in rows if str(row.get("reason") or "") == "gateway_request_budget_exceeded")
    stale_count = sum(1 for row in rows if str(row.get("reason") or "") == "stale_ibkr_request_telemetry")
    missing_count = sum(1 for row in rows if str(row.get("reason") or "").startswith("missing"))
    submit_blocking_count = sum(1 for row in rows if bool(row.get("submit_blocking", False)))
    research_throttled_count = sum(1 for row in rows if bool(row.get("research_throttled", False)))
    cache_hit_ratio = round(cache_count / event_count, 4) if event_count > 0 else 0.0
    max_usage = max((_safe_float(row.get("budget_usage_pct")) for row in rows), default=0.0)
    summary_text = (
        f"gateway_requests={gateway_count} cache_hits={cache_count} "
        f"cache_hit_ratio={cache_hit_ratio:.2f} over_budget={over_budget_count} "
        f"submit_blocking={submit_blocking_count} research_throttled={research_throttled_count} "
        f"stale={stale_count} missing={missing_count}"
    )
    return {
        "generated_at": str(generated_at or ""),
        "week_label": str(week_label or ""),
        "window_start": str(window_start or ""),
        "window_end": str(window_end or ""),
        "summary": {
            "status": status,
            "summary_text": summary_text,
            "market_count": int(len(rows)),
            "gateway_request_count": int(gateway_count),
            "cache_hit_count": int(cache_count),
            "event_count": int(event_count),
            "cache_hit_ratio": cache_hit_ratio,
            "max_budget_usage_pct": round(max_usage, 2),
            "over_budget_market_count": int(over_budget_count),
            "submit_blocking_market_count": int(submit_blocking_count),
            "research_throttled_market_count": int(research_throttled_count),
            "stale_telemetry_market_count": int(stale_count),
            "missing_telemetry_market_count": int(missing_count),
        },
        "rows": list(rows or []),
    }
