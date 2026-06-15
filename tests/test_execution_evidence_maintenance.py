from __future__ import annotations

from datetime import datetime, timezone

from src.common.execution_evidence_maintenance import (
    build_execution_evidence_maintenance_plan,
)


NOW = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)


def _row(market: str, portfolio_id: str, **overrides):
    row = {
        "market": market,
        "portfolio_id": portfolio_id,
        "account_mode": "paper",
        "run_investment_execution": True,
        "market_readiness_artifact_health_status": "STALE",
        "market_readiness_artifact_age_hours": 48.0,
        "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
        "market_readiness_order_count": 0,
        "market_readiness_planned_gross_order_value": 0.0,
        "strategy_stale_suggestion_count": 0,
        "maintenance_report_fresh": True,
        "maintenance_report_reason": "fresh",
    }
    row.update(overrides)
    return row


def _budget(market: str, **overrides):
    row = {
        "market": market,
        "submit_blocking": False,
        "execution_capacity_status": "ok",
        "execution_capacity_reason": "execution_reserve_available",
        "execution_gateway_request_count": 1,
        "execution_reserve_weekly_requests": 100,
    }
    row.update(overrides)
    return row


def test_plan_uses_single_safe_target_and_skips_exhausted_market() -> None:
    plan = build_execution_evidence_maintenance_plan(
        [
            _row(
                "US",
                "US:watchlist",
                market_readiness_order_count=1,
                market_readiness_planned_gross_order_value=71.0,
            ),
            _row(
                "ASX",
                "ASX:asx_top_quality",
                market_readiness_artifact_health_status="DEGRADED_GATEWAY",
            ),
        ],
        [
            _budget(
                "US",
                submit_blocking=True,
                execution_capacity_status="degraded",
                execution_capacity_reason="short_window_execution_reserve_exhausted",
            ),
            _budget("ASX"),
        ],
        generated_at=NOW,
    )

    assert plan["status"] == "READY"
    assert plan["target_market"] == "ASX"
    assert plan["target_portfolio_id"] == "ASX:asx_top_quality"
    assert plan["max_targets"] == 1
    assert plan["submit_orders"] is False
    assert plan["recovery_evidence_only"] is True
    assert plan["candidate_count"] == 1
    assert plan["rejection_count"] == 1


def test_plan_excludes_cn_and_requires_fresh_report() -> None:
    plan = build_execution_evidence_maintenance_plan(
        [
            _row("CN", "CN:cn_top_quality"),
            _row(
                "HK",
                "HK:bluechip",
                maintenance_report_fresh=False,
                maintenance_report_reason="stale_report_trading_days_old:3",
            ),
        ],
        [_budget("CN"), _budget("HK")],
        excluded_markets=["CN"],
        generated_at=NOW,
    )

    assert plan["status"] == "BLOCKED"
    assert plan["target_portfolio_id"] == ""
    reasons = {
        reason
        for row in plan["rejections"]
        for reason in row["reject_reasons"]
    }
    assert "excluded_market" in reasons
    assert "stale_report_trading_days_old:3" in reasons


def test_plan_is_empty_when_all_execution_artifacts_are_fresh() -> None:
    plan = build_execution_evidence_maintenance_plan(
        [
            _row(
                "ASX",
                "ASX:asx_top_quality",
                market_readiness_artifact_health_status="FRESH",
            )
        ],
        [_budget("ASX")],
        generated_at=NOW,
    )

    assert plan["status"] == "EMPTY"
    assert plan["reason"] == "no_stale_execution_evidence"


def test_plan_prioritizes_degraded_gateway_before_plain_stale() -> None:
    plan = build_execution_evidence_maintenance_plan(
        [
            _row(
                "US",
                "US:watchlist",
                market_readiness_order_count=1,
                market_readiness_artifact_age_hours=100.0,
            ),
            _row(
                "HK",
                "HK:bluechip",
                market_readiness_artifact_health_status="DEGRADED_GATEWAY",
                strategy_stale_suggestion_count=1,
            ),
        ],
        [_budget("US"), _budget("HK")],
        generated_at=NOW,
    )

    assert plan["target_market"] == "HK"
    assert plan["target_portfolio_id"] == "HK:bluechip"
