from __future__ import annotations

from datetime import datetime, timezone

from src.common.ibkr_gateway_budget import (
    build_ibkr_gateway_budget_payload,
    build_ibkr_gateway_budget_rows,
    normalize_ibkr_gateway_budget_config,
)


def _cfg():
    return normalize_ibkr_gateway_budget_config(
        {
            "default_weekly_gateway_request_budget": 100,
            "stale_telemetry_warning_hours": 24,
            "over_budget_degraded_ratio": 1.5,
            "markets": {
                "US": {"weekly_gateway_request_budget": 10},
                "HK": {"weekly_gateway_request_budget": 20},
            },
        }
    )


def test_ibkr_gateway_budget_under_budget_is_ok():
    rows = build_ibkr_gateway_budget_rows(
        [
            {
                "market": "US",
                "tool": "generate_investment_report",
                "request_kind": "historical_daily",
                "event_count": 8,
                "gateway_request_count": 6,
                "cache_hit_count": 2,
                "latest_event_ts": "2026-05-09T10:00:00+00:00",
            }
        ],
        config=_cfg(),
        generated_at="2026-05-09T11:00:00+00:00",
        window_start="2026-05-02T00:00:00+00:00",
        window_end="2026-05-09T11:00:00+00:00",
    )

    us = next(row for row in rows if row["market"] == "US")
    assert us["status"] == "ok"
    assert us["budget_usage_pct"] == 60.0
    assert us["cache_hit_ratio"] == 0.25
    assert us["top_request_kind"] == "historical_daily"


def test_ibkr_gateway_budget_over_budget_warns_before_degraded_ratio():
    rows = build_ibkr_gateway_budget_rows(
        [
            {
                "market": "US",
                "tool": "run_investment_guard",
                "request_kind": "positions",
                "event_count": 12,
                "gateway_request_count": 12,
                "cache_hit_count": 0,
                "latest_event_ts": "2026-05-09T10:00:00+00:00",
            }
        ],
        config=_cfg(),
        generated_at="2026-05-09T11:00:00+00:00",
    )

    us = next(row for row in rows if row["market"] == "US")
    assert us["status"] == "warning"
    assert us["reason"] == "gateway_request_budget_exceeded"
    assert us["budget_usage_pct"] == 120.0


def test_ibkr_gateway_budget_degrades_when_far_over_budget():
    rows = build_ibkr_gateway_budget_rows(
        [
            {
                "market": "US",
                "tool": "run_investment_opportunity",
                "request_kind": "historical_daily",
                "event_count": 18,
                "gateway_request_count": 16,
                "cache_hit_count": 2,
                "latest_event_ts": "2026-05-09T10:00:00+00:00",
            }
        ],
        config=_cfg(),
        generated_at="2026-05-09T11:00:00+00:00",
    )

    payload = build_ibkr_gateway_budget_payload(
        generated_at="2026-05-09T11:00:00+00:00",
        week_label="2026-W19",
        window_start="2026-05-02T00:00:00+00:00",
        window_end="2026-05-09T11:00:00+00:00",
        rows=rows,
    )

    us = next(row for row in rows if row["market"] == "US")
    assert us["status"] == "degraded"
    assert payload["summary"]["status"] == "degraded"
    assert payload["summary"]["over_budget_market_count"] == 1


def test_ibkr_gateway_budget_missing_telemetry_warns():
    rows = build_ibkr_gateway_budget_rows(
        [],
        config=_cfg(),
        generated_at=datetime(2026, 5, 9, 11, 0, tzinfo=timezone.utc),
    )

    assert rows == [
        {
            "market": "ALL",
            "status": "warning",
            "reason": "missing_ibkr_request_telemetry",
            "weekly_gateway_request_budget": 100,
            "gateway_request_count": 0,
            "cache_hit_count": 0,
            "event_count": 0,
            "cache_hit_ratio": 0.0,
            "budget_usage_pct": 0.0,
            "excess_gateway_requests": 0,
            "daily_gateway_request_budget": 14.29,
            "projected_recovery_days": 0,
            "projected_recovery_at": "",
            "telemetry_age_hours": 0.0,
            "top_request_kind": "",
            "top_tool": "",
            "latest_event_ts": "",
            "generated_at": "2026-05-09T11:00:00+00:00",
            "window_start": "",
            "window_end": "",
        }
    ]


def test_ibkr_gateway_budget_stale_telemetry_warns():
    rows = build_ibkr_gateway_budget_rows(
        [
            {
                "market": "HK",
                "tool": "generate_investment_report",
                "request_kind": "scanner",
                "event_count": 1,
                "gateway_request_count": 1,
                "cache_hit_count": 0,
                "latest_event_ts": "2026-05-07T10:00:00+00:00",
            }
        ],
        config=_cfg(),
        generated_at="2026-05-09T11:00:00+00:00",
    )

    hk = next(row for row in rows if row["market"] == "HK")
    assert hk["status"] == "warning"
    assert hk["reason"] == "stale_ibkr_request_telemetry"
    assert hk["telemetry_age_hours"] == 49.0


def test_ibkr_gateway_budget_projects_recovery_from_daily_distribution():
    rows = build_ibkr_gateway_budget_rows(
        [
            {
                "date": "2026-05-03",
                "market": "US",
                "tool": "run_investment_opportunity:us:watchlist",
                "request_kind": "historical_daily",
                "event_count": 95,
                "gateway_request_count": 90,
                "cache_hit_count": 5,
                "latest_event_ts": "2026-05-03T20:00:00+00:00",
            },
            {
                "date": "2026-05-09",
                "market": "US",
                "tool": "run_investment_guard",
                "request_kind": "positions",
                "event_count": 5,
                "gateway_request_count": 5,
                "cache_hit_count": 0,
                "latest_event_ts": "2026-05-09T10:00:00+00:00",
            },
        ],
        config=_cfg(),
        generated_at="2026-05-09T11:00:00+00:00",
        window_start="2026-05-02T00:00:00+00:00",
        window_end="2026-05-09T11:00:00+00:00",
    )

    us = next(row for row in rows if row["market"] == "US")
    assert us["status"] == "degraded"
    assert us["excess_gateway_requests"] == 85
    assert us["daily_gateway_request_budget"] == 1.43
    assert us["projected_recovery_days"] == 2
    assert us["projected_recovery_at"] == "2026-05-10T23:59:59.999999+00:00"
    assert us["top_tool"] == "run_investment_opportunity:us:watchlist"
