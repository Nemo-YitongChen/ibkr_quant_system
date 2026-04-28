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
    assert views["US"]["settlement_cycle"] == "T+1"
    assert views["US"]["context"]["timezone"] == "America/New_York"
    assert "趋势优先" in views["US"]["context_summary"]
    assert views["HK"]["portfolios"] == []
    assert views["HK"]["primary_review_axis"] == "board_lot_fee_drag_and_edge_gate"
    assert "board_lot_mismatch" in views["HK"]["primary_risks"]
    assert views["CN"]["stale_report_count"] == 0
    assert views["CN"]["research_only"] is True
    assert views["CN"]["day_turnaround_allowed"] is False
    assert "research_only" in views["CN"]["primary_risks"]


def test_market_views_empty_input_returns_all_markets():
    views = build_market_views([])

    assert set(views) == {"US", "HK", "CN"}
    assert views["US"]["market"] == "US"
    assert views["HK"]["portfolio_count"] == 0
    assert views["CN"]["portfolios"] == []


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


def test_market_views_counts_modes_and_health():
    cards = [
        {
            "market": "US",
            "exchange_open_raw": True,
            "report_status": {"fresh": True},
            "ops_health_rows": [{"status": "warn"}],
            "dashboard_control": {
                "portfolio": {"execution_control_mode": "REVIEW_ONLY"},
            },
            "execution_summary": {"submit_orders": False},
        },
        {
            "market": "US",
            "exchange_open_raw": False,
            "report_status": {"fresh": False},
            "ops_health_rows": [],
            "dashboard_control": {
                "portfolio": {"execution_control_mode": "PAUSED"},
            },
            "execution_summary": {"submit_orders": True},
        },
    ]

    us = build_market_views(cards)["US"]

    assert us["portfolio_count"] == 2
    assert us["open_count"] == 1
    assert us["fresh_report_count"] == 1
    assert us["stale_report_count"] == 1
    assert us["degraded_health_count"] == 1
    assert us["review_only_count"] == 1
    assert us["paused_count"] == 1
    assert us["auto_submit_count"] == 1


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


def test_waterfall_has_stable_components_and_residual():
    cards = [
        {
            "market": "US",
            "portfolio_id": "paper-us",
            "watchlist": "core",
            "weekly_attribution": {
                "selection_contribution": 0.01,
                "execution_contribution": -0.002,
                "weekly_return": 0.02,
            },
        }
    ]

    rows = build_weekly_attribution_waterfall(cards)
    components = [row["component"] for row in rows]

    assert components[:8] == [
        "selection",
        "sizing",
        "sector",
        "market",
        "execution",
        "strategy_control",
        "risk_overlay",
        "execution_gate",
    ]
    assert components[-2:] == [
        "residual_to_reported_return",
        "reported_weekly_return",
    ]
    assert rows[-1]["running_end"] == 0.02


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


def test_unified_evidence_overview_counts_bool_and_string_flags():
    rows = [
        {"market": "US", "blocked_flag": "1", "allowed_flag": "0"},
        {"market": "US", "blocked_flag": False, "allowed_flag": True},
        {"market": "HK", "blocked_flag": "true", "allowed_flag": "false"},
        {"market": "", "blocked_flag": "", "allowed_flag": ""},
    ]

    overview = build_unified_evidence_overview(rows)

    assert overview["row_count"] == 4
    assert overview["blocked_row_count"] == 2
    assert overview["allowed_row_count"] == 1
    markets = {row["market"] for row in overview["market_rows"]}
    assert {"US", "HK", "UNKNOWN"} <= markets
