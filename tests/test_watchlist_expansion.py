from __future__ import annotations

from datetime import datetime, timezone

from src.common.watchlist_expansion import (
    WatchlistExpansionPolicy,
    build_account_growth_tier_plan,
    build_watchlist_seed_intake_plan,
    build_watchlist_seed_promotion_review,
    build_watchlist_seed_proposals,
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
    assert summary["seed_proposal_count"] == 2
    assert summary["manual_seed_proposal_count"] == 2
    assert summary["seed_proposals"][0]["market"] == "ASX"
    assert summary["seed_proposals"][0]["proposal_action"] == "create_or_refresh_preferred_asset_seed_watchlist"
    assert summary["seed_proposals"][0]["near_miss_symbols"] == ["VAS.AX", "A200.AX"]
    assert summary["seed_proposals"][0]["auto_apply"] is False
    assert summary["seed_proposals"][0]["submit_gate_policy"] == "do_not_relax_submit_gates"
    assert summary["seed_intake_plan_count"] == 2
    assert summary["seed_intake_external_source_count"] == 2
    assert summary["seed_intake_plan"][0]["market"] == "ASX"
    assert summary["seed_intake_plan"][0]["intake_status"] == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
    assert summary["seed_intake_plan"][0]["candidate_symbols"] == []
    assert summary["seed_intake_plan"][0]["evidence_symbols"] == ["VAS.AX", "A200.AX"]
    assert summary["seed_intake_plan"][0]["does_not_change_symbol_master"] is True
    assert summary["account_growth_tier_plan"]["quality_gate_policy"] == "do_not_relax_submit_gates"


def test_watchlist_seed_proposals_keep_manual_acceptance_rules() -> None:
    proposals = build_watchlist_seed_proposals(
        [
            {
                "market": "XETRA",
                "expansion_target": "lower_cost_whole_share_etf_candidates",
                "recommendation_action": "calibrate_cost_or_expand_lower_cost_etfs",
                "top_reject_reason": "expected_cost_above_max",
                "preferred_asset_class_gap": False,
                "preferred_asset_classes": ["etf"],
                "near_miss_candidates": [{"symbol": "EXS1.DE"}, {"symbol": "IFX.DE"}],
            }
        ]
    )

    assert proposals == [
        {
            "market": "XETRA",
            "proposal_status": "MANUAL_REVIEW_REQUIRED",
            "proposal_action": "add_lower_cost_whole_share_etf_candidates",
            "expansion_target": "lower_cost_whole_share_etf_candidates",
            "linked_recommendation_action": "calibrate_cost_or_expand_lower_cost_etfs",
            "top_reject_reason": "expected_cost_above_max",
            "preferred_asset_class_gap": False,
            "preferred_asset_classes": ["etf"],
            "near_miss_symbols": ["EXS1.DE", "IFX.DE"],
            "acceptance_rule": (
                "Add or tag seed symbols only after they are verified as IBKR-tradable, match the account profile, "
                "and pass whole-share, cost, liquidity, data-quality, and expected-edge gates in the next candidate report."
            ),
            "submit_gate_policy": "do_not_relax_submit_gates",
            "auto_apply": False,
        }
    ]


def test_watchlist_seed_intake_plan_keeps_etf_first_review_only() -> None:
    plan = build_watchlist_seed_intake_plan(
        [
            {
                "market": "ASX",
                "expansion_target": "seed_preferred_asset_class_candidates",
                "top_reject_reason": "expected_cost_above_max",
                "preferred_asset_class_gap": True,
                "preferred_asset_classes": ["etf"],
                "near_miss_candidates": [
                    {"symbol": "BHP.AX", "asset_class": "equity"},
                    {"symbol": "VAS.AX", "asset_class": "etf"},
                ],
            },
            {
                "market": "HK",
                "expansion_target": "seed_preferred_asset_class_candidates",
                "top_reject_reason": "expected_cost_above_max",
                "preferred_asset_class_gap": True,
                "preferred_asset_classes": ["etf"],
                "near_miss_candidates": [{"symbol": "3988.HK", "asset_class": "equity"}],
            },
        ]
    )

    assert plan[0]["market"] == "ASX"
    assert plan[0]["intake_status"] == "MANUAL_REVIEW_REQUIRED"
    assert plan[0]["candidate_symbols"] == ["VAS.AX"]
    assert plan[0]["evidence_symbols"] == ["BHP.AX", "VAS.AX"]
    assert plan[0]["auto_apply"] is False
    assert plan[0]["does_not_change_symbol_master"] is True
    assert plan[1]["market"] == "HK"
    assert plan[1]["intake_status"] == "NEEDS_EXTERNAL_PREFERRED_ASSET_SOURCE"
    assert plan[1]["candidate_symbols"] == []
    assert plan[1]["evidence_symbols"] == ["3988.HK"]


def test_account_growth_tier_plan_keeps_small_account_etf_first() -> None:
    plan = build_account_growth_tier_plan(
        {
            "name": "small",
            "label": "小资金",
            "broker_equity": 1000.0,
            "max_equity": 25000.0,
            "equity_band": "< 25,000",
            "preferred_instruments": ["ETF", "Large Cap"],
            "execution_overrides": {
                "max_orders_per_run": 1,
                "max_order_value_pct": 0.10,
                "min_trade_value": 25,
            },
        },
        market_recommendations=[{"market": "ASX"}, {"market": "HK"}],
        seed_intake_plan=[
            {
                "market": "ASX",
                "intake_status": "MANUAL_REVIEW_REQUIRED",
                "source_candidate_count": 2,
            },
            {
                "market": "HK",
                "intake_status": "MANUAL_REVIEW_REQUIRED",
                "source_candidate_count": 2,
            },
        ],
    )

    assert plan["profile"] == "small"
    assert plan["primary_action"] == "verify_seed_etfs_in_candidate_report_before_submit"
    assert plan["expansion_mode"] == "whole_share_tradable_etf_first"
    assert plan["submit_frequency_mode"] == "single_small_limit_order_until_fill_quality_passes"
    assert plan["max_orders_per_run"] == 1
    assert plan["max_order_value"] == 100.0
    assert plan["seed_source_candidate_count"] == 4
    assert plan["quality_gate_policy"] == "do_not_relax_submit_gates"
    assert plan["read_only"] is True


def test_watchlist_seed_intake_plan_uses_review_only_source_registry() -> None:
    plan = build_watchlist_seed_intake_plan(
        [
            {
                "market": "XETRA",
                "expansion_target": "seed_preferred_asset_class_candidates",
                "top_reject_reason": "expected_cost_above_max",
                "preferred_asset_class_gap": True,
                "preferred_asset_classes": ["etf"],
                "near_miss_candidates": [{"symbol": "IFX.DE", "asset_class": "equity"}],
            }
        ],
        seed_source_registry={
            "review_only": True,
            "markets": {
                "XETRA": {
                    "candidates": [
                        {
                            "symbol": "EUN1.DE",
                            "exchange_ticker": "EUN1",
                            "asset_class": "etf",
                            "product_name": "iShares STOXX Europe 50 UCITS ETF",
                            "source_name": "Official product page",
                            "source_url": "https://example.test/eun1",
                            "source_verified_at": "2026-06-08",
                            "broker_mapping_status": "TO_VERIFY",
                            "rationale": "Broad European large-cap exposure.",
                        },
                        {
                            "symbol": "IFX.DE",
                            "asset_class": "equity",
                        },
                    ]
                }
            },
        },
    )

    assert plan[0]["intake_status"] == "MANUAL_REVIEW_REQUIRED"
    assert plan[0]["candidate_symbols"] == ["EUN1.DE"]
    assert plan[0]["source_candidate_count"] == 1
    assert plan[0]["source_candidates"][0]["exchange_ticker"] == "EUN1"
    assert plan[0]["source_candidates"][0]["broker_mapping_status"] == "TO_VERIFY"
    assert plan[0]["next_action"] == "verify_seed_source_candidates_in_candidate_report"
    assert plan[0]["auto_apply"] is False
    assert plan[0]["does_not_change_symbol_master"] is True


def test_watchlist_seed_promotion_review_requires_mapping_before_manual_promotion() -> None:
    intake = [
        {
            "market": "ASX",
            "source_candidates": [
                {
                    "symbol": "BGBL.AX",
                    "asset_class": "etf",
                    "source_verified_at": "2026-06-11",
                    "broker_mapping_status": "TO_VERIFY",
                    "reference_price": 83.42,
                    "reference_price_currency": "AUD",
                    "reference_price_at": "2026-06-11",
                }
            ],
        }
    ]
    rows = [
        {
            "market": "ASX",
            "symbol": "BGBL.AX",
            "review_seed_original_action": "ACCUMULATE",
            "review_seed_original_execution_ready": 1,
            "asset_class": "etf",
            "score": 0.7,
            "data_quality_score": 0.9,
            "liquidity_score": 0.85,
            "expected_cost_bps": 20.0,
            "expected_edge_bps": 40.0,
            "whole_share_edge_margin_bps": 12.0,
            "whole_share_tradability_reason": "PASS",
            "last_close": 83.0,
        }
    ]

    review = build_watchlist_seed_promotion_review(
        intake,
        rows,
        policy=WatchlistExpansionPolicy(
            min_score=0.55,
            min_data_quality_score=0.75,
            min_liquidity_score=0.65,
            max_expected_cost_bps=35.0,
            min_expected_edge_bps=20.0,
            min_whole_share_edge_margin_bps=2.0,
            max_last_close=100.0,
            preferred_asset_classes=("etf",),
        ),
        now=datetime(2026, 6, 14, tzinfo=timezone.utc),
    )

    assert review[0]["promotion_status"] == "BROKER_MAPPING_REQUIRED"
    assert review[0]["candidate_evidence_present"] is True
    assert review[0]["quality_reasons"] == []
    assert review[0]["reference_price"] == 83.42
    assert review[0]["auto_apply"] is False


def test_watchlist_seed_promotion_review_marks_quality_pass_as_review_ready() -> None:
    review = build_watchlist_seed_promotion_review(
        [
            {
                "market": "ASX",
                "source_candidates": [
                    {
                        "symbol": "DHHF.AX",
                        "asset_class": "etf",
                        "source_verified_at": "2026-06-11",
                        "broker_mapping_status": "VERIFIED",
                    }
                ],
            }
        ],
        [
            {
                "market": "ASX",
                "symbol": "DHHF.AX",
                "review_seed_original_action": "HOLD",
                "review_seed_original_execution_ready": 1,
                "asset_class": "etf",
                "score": 0.62,
                "data_quality_score": 0.88,
                "liquidity_score": 0.8,
                "expected_cost_bps": 18.0,
                "expected_edge_bps": 35.0,
                "whole_share_edge_margin_bps": 9.0,
                "whole_share_tradability_reason": "PASS",
                "last_close": 41.0,
            }
        ],
        policy=WatchlistExpansionPolicy(max_last_close=100.0, preferred_asset_classes=("etf",)),
        now=datetime(2026, 6, 14, tzinfo=timezone.utc),
    )

    assert review[0]["promotion_status"] == "PROMOTION_REVIEW_READY"
    assert review[0]["next_action"] == "manual_review_before_symbol_master_promotion"
    assert review[0]["does_not_change_symbol_master"] is True


def test_watchlist_seed_promotion_review_normalizes_non_finite_score() -> None:
    review = build_watchlist_seed_promotion_review(
        [
            {
                "market": "HK",
                "source_candidates": [
                    {
                        "symbol": "2800.HK",
                        "asset_class": "etf",
                        "source_verified_at": "2026-06-11",
                        "broker_mapping_status": "TO_VERIFY",
                    }
                ],
            }
        ],
        [
            {
                "market": "HK",
                "symbol": "2800.HK",
                "review_seed_original_action": "WATCH",
                "review_seed_original_execution_ready": 0,
                "asset_class": "etf",
                "score": float("nan"),
            }
        ],
        now=datetime(2026, 6, 14, tzinfo=timezone.utc),
    )

    assert review[0]["score"] == 0.0
    assert review[0]["promotion_status"] == "QUALITY_REJECTED"
