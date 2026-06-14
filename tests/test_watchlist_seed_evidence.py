from __future__ import annotations

from src.common.watchlist_seed_evidence import build_seed_evidence_queue


def test_seed_evidence_queue_prioritizes_lower_order_value_utilization() -> None:
    queue = build_seed_evidence_queue(
        [
            {
                "market": "ASX",
                "symbol": "BGBL.AX",
                "asset_class": "etf",
                "promotion_status": "CANDIDATE_REPORT_REQUIRED",
                "source_fresh": True,
                "reference_price": 83.42,
                "reference_price_currency": "AUD",
            },
            {
                "market": "ASX",
                "symbol": "DHHF.AX",
                "asset_class": "etf",
                "promotion_status": "CANDIDATE_REPORT_REQUIRED",
                "source_fresh": True,
                "reference_price": 40.90,
                "reference_price_currency": "AUD",
            },
        ],
        account_growth_tier_plan={"max_order_value": 100.0},
    )

    assert queue[0]["status"] == "READY"
    assert queue[0]["symbols"] == ["DHHF.AX", "BGBL.AX"]
    assert queue[0]["evidence_mode"] == "YFINANCE_ONLY"
    assert queue[0]["submit_orders"] is False
    assert queue[0]["auto_promote"] is False


def test_seed_evidence_queue_blocks_price_above_small_account_cap() -> None:
    queue = build_seed_evidence_queue(
        [
            {
                "market": "ASX",
                "symbol": "EXPENSIVE.AX",
                "promotion_status": "CANDIDATE_REPORT_REQUIRED",
                "source_fresh": True,
                "reference_price": 125.0,
            }
        ],
        account_growth_tier_plan={"max_order_value": 100.0},
    )

    assert queue[0]["status"] == "BLOCKED"
    assert queue[0]["symbols"] == []
    assert queue[0]["candidates"][0]["block_reasons"] == [
        "reference_price_above_order_cap"
    ]
