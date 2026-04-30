from __future__ import annotations

from src.common.investment_evidence import (
    EVIDENCE_COLUMNS,
    build_blocked_vs_allowed_expost_review,
    build_unified_evidence_rows,
    normalize_evidence_row,
)


def test_normalize_evidence_row_preserves_schema_and_values():
    row = normalize_evidence_row({"market": "US", "symbol": "AAPL", "expected_edge_bps": 42.5})

    assert set(EVIDENCE_COLUMNS) <= set(row)
    assert row["market"] == "US"
    assert row["symbol"] == "AAPL"
    assert row["expected_edge_bps"] == 42.5
    assert row["realized_edge_bps"] == ""


def test_build_unified_evidence_rows_keeps_candidate_only_partial_join():
    rows = build_unified_evidence_rows(
        [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "decision_source": "candidate_snapshot",
                "candidate_snapshot_id": "snap-1",
                "candidate_only_flag": 1,
                "join_quality": "candidate_outcome_only",
                "symbol": "AAPL",
                "signal_score": 0.88,
                "expected_edge_bps": 95.0,
                "expected_cost_bps": 15.0,
                "outcome_20d_bps": 120.0,
            }
        ]
    )

    assert len(rows) == 1
    assert rows[0]["candidate_only_flag"] == 1
    assert rows[0]["join_quality"] == "candidate_outcome_only"
    assert rows[0]["expected_post_cost_edge_bps"] == 80.0
    assert rows[0]["outcome_20d"] == 120.0


def test_build_unified_evidence_rows_classifies_allowed_and_blocked_rows():
    rows = build_unified_evidence_rows(
        [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "symbol": "AAA",
                "decision_status": "FILLED",
                "fill_notional": 1000.0,
                "expected_edge_bps": 80.0,
                "expected_cost_bps": 12.0,
                "realized_edge_bps": 75.0,
            },
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "symbol": "BBB",
                "decision_status": "BLOCKED_EDGE",
                "blocked_edge_order_count": 1,
                "expected_edge_bps": 20.0,
                "expected_cost_bps": 15.0,
            },
        ]
    )

    allowed = next(row for row in rows if row["symbol"] == "AAA")
    blocked = next(row for row in rows if row["symbol"] == "BBB")
    assert allowed["allowed_flag"] == 1
    assert allowed["blocked_flag"] == 0
    assert allowed["block_reason"] == "ALLOWED_FILLED"
    assert allowed["realized_edge_delta_bps"] == 7.0
    assert blocked["allowed_flag"] == 0
    assert blocked["blocked_flag"] == 1
    assert blocked["block_reason"] == "EDGE_GATE"


def test_blocked_vs_allowed_expost_review_accepts_unified_rows():
    unified_rows = build_unified_evidence_rows(
        [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "symbol": "AAA",
                "decision_status": "FILLED",
                "fill_notional": 1000.0,
                "expected_edge_bps": 70.0,
                "expected_cost_bps": 10.0,
                "realized_edge_bps": 76.0,
                "outcome_5d_bps": 35.0,
                "outcome_20d_bps": 120.0,
                "outcome_60d_bps": 180.0,
            },
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "symbol": "BBB",
                "decision_status": "BLOCKED_EDGE",
                "blocked_edge_order_count": 1,
                "expected_edge_bps": 25.0,
                "expected_cost_bps": 20.0,
                "outcome_5d_bps": -12.0,
                "outcome_20d_bps": -20.0,
                "outcome_60d_bps": -55.0,
            },
        ]
    )
    review = build_blocked_vs_allowed_expost_review(unified_rows)

    assert len(review) == 1
    row = review[0]
    assert row["block_reason"] == "EDGE_GATE"
    assert row["allowed_count"] == 1
    assert row["blocked_count"] == 1
    assert row["allowed_minus_blocked_outcome_20d_bps"] == 140.0
    assert row["positive_outcome_horizon_count"] == 3
    assert row["review_label"] == "BLOCKING_HELPED"
