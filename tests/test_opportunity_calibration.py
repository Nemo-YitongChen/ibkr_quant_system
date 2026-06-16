from __future__ import annotations

from src.common.opportunity_calibration import (
    build_candidate_outcome_validation,
    build_candidate_outcome_validation_summary,
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
    assert row["close_wait_pullback_symbols"] == "AZJ.AX"
    assert row["close_wait_pullback_rows"][0]["symbol"] == "AZJ.AX"


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
    assert row["positive_post_cost_symbols"] == "3988.HK,0939.HK"
    assert row["positive_post_cost_rows"][0]["symbol"] == "3988.HK"


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


def test_candidate_outcome_validation_supports_positive_hk_candidate_group() -> None:
    row = build_candidate_outcome_validation(
        [
            {"symbol": "3988.HK"},
            {"symbol": "0939.HK"},
        ],
        [
            {
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "symbol": "3988.HK",
                "outcome_5d_bps": "120.0",
                "outcome_20d_bps": "220.0",
                "decision_ts": "2026-05-01T00:00:00+00:00",
            },
            {
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "symbol": "0939.HK",
                "outcome_5d_bps": "80.0",
                "outcome_20d_bps": "180.0",
                "decision_ts": "2026-05-02T00:00:00+00:00",
            },
        ],
        market="HK",
        portfolio_id="HK:resolved_hk_top100_bluechip",
        group_name="positive_post_cost_candidates",
        min_5d_samples=2,
        min_20d_samples=2,
    )

    assert row["status"] == "OUTCOME_SUPPORTS_GROUP"
    assert row["matched_symbol_count"] == 2
    assert row["matured_5d_sample_count"] == 2
    assert row["matured_20d_sample_count"] == 2
    assert row["avg_outcome_5d_bps"] == 100.0
    assert row["avg_outcome_20d_bps"] == 200.0
    assert row["latest_outcome_decision_ts"] == "2026-05-02T00:00:00+00:00"


def test_candidate_outcome_validation_marks_missing_maturity_pending() -> None:
    row = build_candidate_outcome_validation(
        [{"symbol": "3988.HK"}],
        [
            {
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "symbol": "0005.HK",
                "outcome_5d_bps": "50.0",
            }
        ],
        market="HK",
        portfolio_id="HK:resolved_hk_top100_bluechip",
        group_name="close_wait_pullback",
    )

    assert row["status"] == "OUTCOME_PENDING"
    assert row["primary_action"] == "wait_for_5d_20d_outcome_maturity"
    assert row["matched_symbol_count"] == 0
    assert row["unmatched_symbols"] == "3988.HK"


def test_candidate_outcome_validation_summary_counts_mature_samples() -> None:
    summary = build_candidate_outcome_validation_summary(
        [
            {
                "market": "HK",
                "status": "OUTCOME_SUPPORTS_GROUP",
                "candidate_symbol_count": 2,
                "matched_symbol_count": 2,
                "matured_5d_sample_count": 10,
                "matured_20d_sample_count": 8,
            },
            {
                "market": "ASX",
                "status": "OUTCOME_PENDING",
                "candidate_symbol_count": 1,
                "matched_symbol_count": 0,
                "matured_5d_sample_count": 0,
                "matured_20d_sample_count": 0,
            },
        ]
    )

    assert summary["validation_count"] == 2
    assert summary["candidate_symbol_count"] == 3
    assert summary["matched_symbol_count"] == 2
    assert summary["primary_status"] == "MIXED"
