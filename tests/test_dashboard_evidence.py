from __future__ import annotations

import pytest

from src.common.dashboard_evidence import (
    build_market_views,
    build_unified_evidence_overview,
    build_weekly_attribution_waterfall,
)


def test_build_market_views_always_returns_us_hk_cn():
    views = build_market_views([])

    assert list(views) == ["US", "HK", "CN"]
    assert views["US"]["portfolio_count"] == 0
    assert views["HK"]["portfolios"] == []
    assert views["CN"]["stale_report_count"] == 0


def test_build_market_views_counts_health_and_execution_modes():
    views = build_market_views(
        [
            {
                "market": "US",
                "portfolio_id": "US:core",
                "watchlist": "core",
                "mode": "paper-auto-submit",
                "exchange_open_raw": True,
                "report_status": {"fresh": True},
                "ops_health_rows": [{"status": "ok"}],
                "market_data_health_rows": [{"status_label": "IBKR正常"}],
                "dashboard_control": {
                    "portfolio": {
                        "execution_control_mode": "AUTO",
                        "submit_investment_execution": True,
                    }
                },
            },
            {
                "market": "HK",
                "portfolio_id": "HK:quality",
                "watchlist": "quality",
                "report_status": {"fresh": False},
                "ops_health_rows": [{"status": "warning"}],
                "market_data_health_rows": [{"status_label": "研究Fallback"}],
                "dashboard_control": {
                    "portfolio": {
                        "execution_control_mode": "REVIEW_ONLY",
                    }
                },
            },
            {
                "market": "CN",
                "portfolio_id": "CN:quality",
                "watchlist": "quality",
                "report_status": {"fresh": False},
                "dashboard_control": {
                    "portfolio": {
                        "execution_control_mode": "PAUSED",
                    }
                },
            },
        ]
    )

    assert views["US"]["open_count"] == 1
    assert views["US"]["fresh_report_count"] == 1
    assert views["US"]["auto_submit_count"] == 1
    assert views["HK"]["stale_report_count"] == 1
    assert views["HK"]["degraded_health_count"] == 1
    assert views["HK"]["data_attention_count"] == 1
    assert views["HK"]["review_only_count"] == 1
    assert views["CN"]["paused_count"] == 1


def test_build_weekly_attribution_waterfall_includes_residual_and_total():
    rows = build_weekly_attribution_waterfall(
        [
            {
                "market": "US",
                "watchlist": "core",
                "portfolio_id": "US:core",
                "weekly_attribution": {
                    "weekly_return": 0.10,
                    "selection_contribution": 0.03,
                    "sizing_contribution": 0.02,
                    "sector_contribution": 0.01,
                    "market_contribution": 0.01,
                    "execution_contribution": -0.005,
                    "strategy_control_weight_delta": 0.002,
                    "risk_overlay_weight_delta": -0.003,
                    "execution_gate_blocked_weight": -0.004,
                },
            }
        ]
    )

    assert [row["component"] for row in rows] == [
        "selection",
        "sizing",
        "sector",
        "market",
        "execution",
        "strategy_control",
        "risk_overlay",
        "execution_gate",
        "residual_to_reported_return",
        "reported_weekly_return",
    ]
    residual = next(row for row in rows if row["component"] == "residual_to_reported_return")
    assert residual["running_end"] == pytest.approx(0.10)
    assert residual["contribution"] == pytest.approx(0.04)


def test_build_weekly_attribution_waterfall_handles_missing_fields_as_zero():
    rows = build_weekly_attribution_waterfall(
        [
            {
                "market": "US",
                "portfolio_id": "US:partial",
                "weekly_attribution": {"weekly_return": 0.04},
            }
        ]
    )

    known_components = [row for row in rows if row["component_role"] in {"return_component", "control_delta"}]
    assert all(row["contribution"] == 0.0 for row in known_components)
    residual = next(row for row in rows if row["component"] == "residual_to_reported_return")
    assert residual["contribution"] == pytest.approx(0.04)


def test_build_unified_evidence_overview_groups_by_market():
    overview = build_unified_evidence_overview(
        [
            {"market": "US", "allowed_flag": 1, "blocked_flag": 0},
            {"market": "US", "allowed_flag": 0, "blocked_flag": 1},
            {"market": "HK", "allowed_flag": "true", "blocked_flag": ""},
            {"market": "", "allowed_flag": 0, "blocked_flag": True},
        ]
    )

    assert overview["row_count"] == 4
    assert overview["allowed_row_count"] == 2
    assert overview["blocked_row_count"] == 2
    by_market = {row["market"]: row for row in overview["market_rows"]}
    assert by_market["US"]["row_count"] == 2
    assert by_market["HK"]["allowed_row_count"] == 1
    assert by_market["UNKNOWN"]["blocked_row_count"] == 1
