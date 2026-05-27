from __future__ import annotations

from src.common.open_market_analysis import build_open_market_analysis_summary


def test_open_market_analysis_warns_when_submit_gate_artifact_missing() -> None:
    summary = build_open_market_analysis_summary(
        [
            {
                "market": "US",
                "watchlist": "whole_share_etf",
                "portfolio_id": "US:whole_share_etf",
                "exchange_open_raw": True,
                "report_status": {"fresh": True},
                "recommended_action": "可执行调仓",
                "submit_investment_execution": True,
            }
        ],
        auto_order_readiness={},
    )

    assert summary["status"] == "warning"
    assert summary["open_market_count"] == 1
    assert summary["fresh_open_report_count"] == 1
    assert summary["auto_missing_open_count"] == 1


def test_open_market_analysis_degrades_on_stale_open_report() -> None:
    summary = build_open_market_analysis_summary(
        [
            {
                "market": "ASX",
                "portfolio_id": "ASX:whole_share_etf",
                "exchange_open_raw": True,
                "report_status": {"fresh": False},
                "submit_investment_execution": False,
            }
        ],
        auto_order_readiness={},
    )

    assert summary["status"] == "degraded"
    assert summary["stale_open_report_count"] == 1


def test_open_market_analysis_tracks_ready_and_blocked_submit_rows() -> None:
    summary = build_open_market_analysis_summary(
        [
            {
                "market": "US",
                "portfolio_id": "US:ready",
                "exchange_open_raw": True,
                "report_status": {"fresh": True},
                "submit_investment_execution": True,
            },
            {
                "market": "HK",
                "portfolio_id": "HK:blocked",
                "exchange_open_raw": True,
                "report_status": {"fresh": True},
                "submit_investment_execution": True,
            },
        ],
        auto_order_readiness={
            "rows": [
                {
                    "portfolio_id": "US:ready",
                    "market": "US",
                    "status": "READY",
                    "ready": True,
                },
                {
                    "portfolio_id": "HK:blocked",
                    "market": "HK",
                    "status": "BLOCKED",
                    "ready": False,
                    "primary_reason": "ibkr_gateway_unavailable",
                },
            ]
        },
    )

    assert summary["status"] == "warning"
    assert summary["auto_ready_open_count"] == 1
    assert summary["auto_blocked_open_count"] == 1
    assert summary["primary_reason_counts"] == {"ibkr_gateway_unavailable": 1}
