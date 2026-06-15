from __future__ import annotations

from src.common.opportunity_calibration import (
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
