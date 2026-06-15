from __future__ import annotations

from src.common.opportunity_calibration import (
    build_post_cost_calibration,
    build_post_cost_calibration_summary,
    build_wait_pullback_calibration,
    build_wait_pullback_calibration_summary,
)


def test_wait_pullback_calibration_flags_close_wait_rows() -> None:
    row = build_wait_pullback_calibration(
        [
            {
                "symbol": "AZJ.AX",
                "action": "ACCUMULATE",
                "score": "0.77",
                "entry_status": "WAIT_PULLBACK",
                "entry_anchor_gap_pct": "2.2",
                "entry_anchor_selected_component": "pullback",
                "entry_anchor_profile": "STANDARD_CONSERVATIVE",
            }
        ],
        market="ASX",
        portfolio_id="ASX:asx_top_quality",
    )

    assert row["status"] == "REVIEW_ANCHOR"
    assert row["primary_action"] == "review_pullback_anchor_before_changing_thresholds"
    assert row["close_wait_pullback_count"] == 1
    assert row["top_wait_symbols"] == "AZJ.AX"


def test_wait_pullback_calibration_detects_ma_anchor_conservatism() -> None:
    row = build_wait_pullback_calibration(
        [
            {
                "symbol": "AAA",
                "entry_status": "WAIT_PULLBACK",
                "entry_anchor_gap_pct": "8.0",
                "entry_anchor_selected_component": "ma",
            },
            {
                "symbol": "BBB",
                "entry_status": "WAIT_PULLBACK",
                "entry_anchor_gap_pct": "7.5",
                "entry_anchor_selected_component": "ma",
            },
        ],
        market="US",
        portfolio_id="US:watchlist",
    )

    assert row["status"] == "MA_ANCHOR_CONSERVATIVE"
    assert row["dominant_anchor_component"] == "ma"
    assert row["missing_asset_class_count"] == 2


def test_wait_pullback_summary_counts_review_candidates() -> None:
    summary = build_wait_pullback_calibration_summary(
        [
            {"market": "ASX", "status": "REVIEW_ANCHOR", "close_wait_pullback_count": 1, "near_candidate_count": 0},
            {"market": "US", "status": "MA_ANCHOR_CONSERVATIVE", "close_wait_pullback_count": 0, "near_candidate_count": 0},
        ]
    )

    assert summary["portfolio_count"] == 2
    assert summary["review_wait_pullback_count"] == 1
    assert summary["status_counts"]["REVIEW_ANCHOR"] == 1


def test_post_cost_calibration_flags_market_specific_cost_threshold_review() -> None:
    row = build_post_cost_calibration(
        [
            {
                "symbol": "3988.HK",
                "action": "HOLD",
                "score": "0.97",
                "score_before_cost": "1.02",
                "accumulate_threshold": "0.34",
                "expected_cost_bps": "50.0",
                "spread_proxy_bps": "14.0",
                "slippage_proxy_bps": "25.0",
                "commission_proxy_bps": "11.0",
            },
            {
                "symbol": "0939.HK",
                "action": "HOLD",
                "score": "0.83",
                "score_before_cost": "0.88",
                "accumulate_threshold": "0.34",
                "expected_cost_bps": "48.0",
            },
        ],
        market="HK",
        portfolio_id="HK:resolved_hk_top100_bluechip",
        max_expected_cost_bps=45.0,
    )

    assert row["status"] == "COST_THRESHOLD_REVIEW"
    assert row["primary_action"] == "review_market_specific_cost_threshold_with_post_cost_margin"
    assert row["high_cost_candidate_count"] == 2
    assert row["positive_post_cost_edge_count"] == 2
    assert row["top_post_cost_symbols"] == "3988.HK,0939.HK"


def test_post_cost_calibration_flags_weak_edge_after_cost() -> None:
    row = build_post_cost_calibration(
        [
            {
                "symbol": "AAA",
                "score": "0.35",
                "score_before_cost": "0.38",
                "accumulate_threshold": "0.34",
                "expected_cost_bps": "20.0",
            }
        ],
        market="US",
        portfolio_id="US:watchlist",
    )

    assert row["status"] == "EDGE_AFTER_COST_WEAK"
    assert row["primary_action"] == "improve_signal_edge_before_expansion"


def test_post_cost_summary_counts_review_and_candidates() -> None:
    summary = build_post_cost_calibration_summary(
        [
            {
                "market": "HK",
                "status": "COST_THRESHOLD_REVIEW",
                "high_cost_candidate_count": 2,
                "positive_post_cost_edge_count": 2,
            },
            {
                "market": "US",
                "status": "POST_COST_HEALTHY",
                "high_cost_candidate_count": 0,
                "positive_post_cost_edge_count": 1,
            },
        ]
    )

    assert summary["portfolio_count"] == 2
    assert summary["review_portfolio_count"] == 1
    assert summary["high_cost_candidate_count"] == 2
    assert summary["positive_post_cost_edge_count"] == 3
