from __future__ import annotations

from src.common.watchlist_expansion import (
    WatchlistExpansionPolicy,
    build_watchlist_expansion_rows,
    selected_watchlist_symbols,
    selection_reason_summary,
    summarize_watchlist_expansion,
)


def test_watchlist_expansion_selects_whole_share_quality_candidates() -> None:
    rows = build_watchlist_expansion_rows(
        [
            {
                "symbol": "SPLG",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.61,
                "data_quality_score": 0.9,
                "liquidity_score": 0.82,
                "expected_cost_bps": 21.0,
                "whole_share_expected_edge_bps": 39.0,
                "whole_share_edge_margin_bps": 12.0,
                "whole_share_tradability_reason": "PASS",
                "last_close": 88.0,
            },
            {
                "symbol": "QQQ",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.75,
                "data_quality_score": 0.9,
                "liquidity_score": 0.9,
                "expected_cost_bps": 18.0,
                "whole_share_expected_edge_bps": 60.0,
                "whole_share_edge_margin_bps": 30.0,
                "whole_share_tradability_reason": "PRICE_ABOVE_MAX_ORDER_VALUE",
                "last_close": 700.0,
            },
        ],
        market="US",
        base_symbols=["SPLG"],
        policy=WatchlistExpansionPolicy(min_whole_share_edge_margin_bps=8.0),
    )

    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["SPLG"]["selection_status"] == "SELECTED"
    assert by_symbol["SPLG"]["already_in_base_watchlist"] is True
    assert by_symbol["QQQ"]["selection_status"] == "REJECTED"
    assert "whole_share_not_tradable" in by_symbol["QQQ"]["selection_reason"]
    assert selected_watchlist_symbols(rows) == ["SPLG"]


def test_watchlist_expansion_rejects_high_cost_or_low_quality_rows() -> None:
    rows = build_watchlist_expansion_rows(
        [
            {
                "symbol": "CHEAP",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "equity",
                "score": 0.58,
                "data_quality_score": 0.4,
                "liquidity_score": 0.75,
                "expected_cost_bps": 18.0,
                "expected_edge_bps": 40.0,
                "whole_share_edge_margin_bps": 10.0,
                "whole_share_tradability_reason": "PASS",
            },
            {
                "symbol": "COSTLY",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "equity",
                "score": 0.62,
                "data_quality_score": 0.9,
                "liquidity_score": 0.8,
                "expected_cost_bps": 80.0,
                "expected_edge_bps": 120.0,
                "whole_share_edge_margin_bps": 20.0,
                "whole_share_tradability_reason": "PASS",
            },
        ],
        market="US",
        policy=WatchlistExpansionPolicy(max_expected_cost_bps=45.0),
    )

    reasons = {row["symbol"]: row["selection_reason"] for row in rows}
    assert "data_quality_below_min" in reasons["CHEAP"]
    assert "expected_cost_above_max" in reasons["COSTLY"]
    assert selected_watchlist_symbols(rows) == []


def test_watchlist_expansion_applies_market_limit_after_quality_sort() -> None:
    rows = build_watchlist_expansion_rows(
        [
            {
                "symbol": "LOW",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.5,
                "data_quality_score": 0.9,
                "liquidity_score": 0.9,
                "expected_cost_bps": 20.0,
                "expected_edge_bps": 35.0,
                "whole_share_edge_margin_bps": 5.0,
                "whole_share_tradability_reason": "PASS",
            },
            {
                "symbol": "HIGH",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.7,
                "data_quality_score": 0.9,
                "liquidity_score": 0.9,
                "expected_cost_bps": 20.0,
                "expected_edge_bps": 45.0,
                "whole_share_edge_margin_bps": 15.0,
                "whole_share_tradability_reason": "PASS",
            },
        ],
        market="US",
        policy=WatchlistExpansionPolicy(max_symbols_per_market=1),
    )

    assert selected_watchlist_symbols(rows) == ["HIGH"]
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["LOW"]["selection_reason"] == "market_symbol_limit_reached"


def test_watchlist_expansion_can_enforce_small_account_etf_price_cap() -> None:
    policy = WatchlistExpansionPolicy(
        max_symbols_per_market=5,
        min_score=0.5,
        min_data_quality_score=0.7,
        min_liquidity_score=0.7,
        max_expected_cost_bps=35.0,
        min_expected_edge_bps=8.0,
        min_whole_share_edge_margin_bps=6.0,
        max_last_close=100.0,
        preferred_asset_classes=("etf",),
    )
    rows = build_watchlist_expansion_rows(
        [
            {
                "symbol": "SPLG",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.65,
                "data_quality_score": 0.9,
                "liquidity_score": 0.9,
                "expected_cost_bps": 20.0,
                "expected_edge_bps": 38.0,
                "whole_share_edge_margin_bps": 12.0,
                "whole_share_tradability_reason": "PASS",
                "last_close": 88.0,
            },
            {
                "symbol": "SPY",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "etf",
                "score": 0.8,
                "data_quality_score": 0.95,
                "liquidity_score": 0.95,
                "expected_cost_bps": 15.0,
                "expected_edge_bps": 60.0,
                "whole_share_edge_margin_bps": 30.0,
                "whole_share_tradability_reason": "PASS",
                "last_close": 600.0,
            },
            {
                "symbol": "AAPL",
                "action": "ACCUMULATE",
                "execution_ready": 1,
                "asset_class": "equity",
                "score": 0.72,
                "data_quality_score": 0.9,
                "liquidity_score": 0.9,
                "expected_cost_bps": 18.0,
                "expected_edge_bps": 40.0,
                "whole_share_edge_margin_bps": 10.0,
                "whole_share_tradability_reason": "PASS",
                "last_close": 95.0,
            },
        ],
        market="US",
        policy=policy,
    )

    by_symbol = {row["symbol"]: row for row in rows}
    assert selected_watchlist_symbols(rows) == ["SPLG"]
    assert "last_close_above_account_cap" in by_symbol["SPY"]["selection_reason"]
    assert "asset_class_not_preferred" in by_symbol["AAPL"]["selection_reason"]


def test_watchlist_expansion_policy_merges_account_overrides() -> None:
    policy = WatchlistExpansionPolicy().with_overrides(
        {
            "max_symbols_per_market": 3,
            "max_last_close": 100.0,
            "preferred_asset_classes": ["etf"],
        }
    )

    assert policy.max_symbols_per_market == 3
    assert policy.max_last_close == 100.0
    assert policy.preferred_asset_classes == ("etf",)


def test_watchlist_expansion_summary_recommends_market_followups() -> None:
    rows = [
        {
            "market": "ASX",
            "symbol": "A200.AX",
            "selection_status": "REJECTED",
            "selection_reason": "expected_cost_above_max,whole_share_not_tradable",
        },
        {
            "market": "ASX",
            "symbol": "VAS.AX",
            "selection_status": "REJECTED",
            "selection_reason": "expected_cost_above_max",
        },
        {
            "market": "HK",
            "symbol": "2800.HK",
            "selection_status": "REJECTED",
            "selection_reason": "whole_share_not_tradable",
        },
        {
            "market": "US",
            "symbol": "SPLG",
            "selection_status": "SELECTED",
            "selection_reason": "PASS",
        },
    ]

    summary = summarize_watchlist_expansion(
        rows,
        market_rows=[
            {"market": "ASX", "candidate_row_count": 2, "selected_count": 0},
            {"market": "HK", "candidate_row_count": 1, "selected_count": 0},
            {"market": "US", "candidate_row_count": 1, "selected_count": 1},
        ],
        policy=WatchlistExpansionPolicy(preferred_asset_classes=("etf",)),
    )

    assert selection_reason_summary(rows)[0] == {"reason": "expected_cost_above_max", "count": 2}
    assert summary["selected_count"] == 1
    assert summary["rejected_count"] == 3
    assert summary["zero_selected_market_count"] == 2
    assert summary["primary_recommendation_market"] == "ASX"
    assert summary["primary_recommendation_reason"] == "expected_cost_above_max"
    assert summary["primary_recommendation_action"] == "calibrate_cost_or_expand_lower_cost_etfs"
    assert summary["market_recommendations"][0]["preferred_asset_class_gap"] is True
    assert summary["market_recommendations"][0]["expansion_target"] == "seed_preferred_asset_class_candidates"
    assert summary["market_recommendations"][0]["near_miss_candidates"][0]["symbol"] == "VAS.AX"
    assert summary["market_recommendations"][1]["market"] == "HK"
    assert summary["market_recommendations"][1]["recommendation_action"] == "expand_whole_share_tradable_etfs"
