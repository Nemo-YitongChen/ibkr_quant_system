from __future__ import annotations

import json

from src.analysis.investment import (
    InvestmentPlanConfig,
    InvestmentScoringConfig,
    make_investment_plan,
    score_investment_candidate,
)
from src.analysis.investment_portfolio import InvestmentPaperConfig, build_target_allocations
from src.tools.review_investment_weekly import (
    _build_candidate_model_review_rows,
    _build_unified_evidence_rows,
    _build_weekly_decision_evidence_rows,
)


def _score_candidate(
    *,
    symbol: str,
    long_score: float,
    trend: float,
    mid_scale: float,
    regime_composite: float,
    expected_cost_bps: float,
    sector: str,
    recommendation_score: float,
) -> dict:
    return score_investment_candidate(
        {
            "symbol": symbol,
            "market": "US",
            "long_score": long_score,
            "trend_vs_ma200": trend,
            "mdd_1y": -0.02,
            "rebalance_flag": 0,
            "last_close": 100.0,
        },
        {
            "symbol": symbol,
            "market": "US",
            "mid_scale": mid_scale,
            "trend_slope_60d": trend * 0.8,
            "regime_composite": regime_composite,
            "regime_state": "RISK_ON",
            "regime_reason": "unit-test",
            "risk_on": True,
            "last_close": 100.0,
            "regime_state_v2": {"state": "RISK_ON"},
        },
        vix=14.0,
        earnings_in_14d=False,
        macro_high_risk=False,
        market_sentiment_score=0.24,
        data_quality_score=0.95,
        source_coverage=0.90,
        missing_ratio=0.02,
        expected_cost_bps=expected_cost_bps,
        spread_proxy_bps=expected_cost_bps * 0.30,
        slippage_proxy_bps=expected_cost_bps * 0.50,
        commission_proxy_bps=expected_cost_bps * 0.20,
        liquidity_score=0.90,
        avg_daily_dollar_volume=100_000_000.0,
        avg_daily_volume=1_000_000.0,
        atr_pct=0.02,
        micro_breakout_5m=0.30,
        micro_volume_burst_5m=0.20,
        microstructure_score=0.25,
        fundamentals={
            "profit_margin": 0.25,
            "operating_margin": 0.28,
            "revenue_growth": 0.12,
            "roe": 0.25,
            "sector": sector,
            "country": "US",
        },
        recommendation={"recommendation_score": recommendation_score},
        cfg=InvestmentScoringConfig(),
    )


def _snapshot_row(*, run_id: str, stage_rank: int, scored: dict, plan: dict) -> dict:
    stage = "final" if stage_rank <= 2 else "deep"
    snapshot_id = f"{run_id}|{stage}|{scored['symbol']}"
    expected_edge_bps = float(plan["expected_edge_score"]) * 140.0
    return {
        "snapshot_id": snapshot_id,
        "analysis_run_id": run_id,
        "portfolio_id": "US:pure_strategy",
        "market": "US",
        "stage": stage,
        "stage_rank": stage_rank,
        "symbol": scored["symbol"],
        "direction": "LONG",
        "action": plan["action"],
        "score": scored["score"],
        "score_before_cost": scored["score_before_cost"],
        "expected_edge_bps": expected_edge_bps,
        "expected_cost_bps": scored["expected_cost_bps"],
        "details": json.dumps({"stage_rank": stage_rank}, sort_keys=True),
    }


def _outcome_rows(snapshot_rows: list[dict]) -> list[dict]:
    outcome_by_symbol = {
        "AAA": {5: 0.020, 20: 0.038, 60: 0.070},
        "BBB": {5: 0.010, 20: 0.018, 60: 0.036},
        "CCC": {5: -0.004, 20: -0.003, 60: 0.004},
    }
    rows: list[dict] = []
    for snapshot in snapshot_rows:
        for horizon_days, future_return in outcome_by_symbol[str(snapshot["symbol"])].items():
            rows.append(
                {
                    "snapshot_id": snapshot["snapshot_id"],
                    "symbol": snapshot["symbol"],
                    "horizon_days": horizon_days,
                    "future_return": future_return,
                }
            )
    return rows


def test_pure_strategy_no_trade_loop_can_train_from_candidate_outcomes():
    scored_rows = [
        _score_candidate(
            symbol="AAA",
            long_score=0.55,
            trend=0.18,
            mid_scale=0.82,
            regime_composite=0.30,
            expected_cost_bps=8.0,
            sector="Technology",
            recommendation_score=0.60,
        ),
        _score_candidate(
            symbol="BBB",
            long_score=0.42,
            trend=0.12,
            mid_scale=0.70,
            regime_composite=0.18,
            expected_cost_bps=10.0,
            sector="Healthcare",
            recommendation_score=0.35,
        ),
        _score_candidate(
            symbol="CCC",
            long_score=0.22,
            trend=0.03,
            mid_scale=0.54,
            regime_composite=0.04,
            expected_cost_bps=14.0,
            sector="Industrials",
            recommendation_score=0.00,
        ),
    ]
    plan_cfg = InvestmentPlanConfig(no_trade_band_pct=0.04, turnover_penalty_scale=0.18)
    plan_rows = [make_investment_plan(row, vix=14.0, cfg=plan_cfg) for row in scored_rows]

    assert scored_rows[0]["score_before_cost"] > scored_rows[1]["score_before_cost"] > scored_rows[2]["score_before_cost"]
    assert [plan["action"] for plan in plan_rows] == ["ACCUMULATE", "ACCUMULATE", "HOLD"]

    target_weights = build_target_allocations(
        scored_rows,
        plan_rows,
        cfg=InvestmentPaperConfig(max_holdings=3, max_single_weight=0.50, max_sector_weight=0.50, min_position_weight=0.02),
    )
    assert set(target_weights) == {"AAA", "BBB", "CCC"}
    assert target_weights["AAA"] > target_weights["CCC"]

    snapshot_rows = [
        _snapshot_row(run_id="PURE1", stage_rank=index + 1, scored=scored, plan=plan)
        for index, (scored, plan) in enumerate(zip(scored_rows, plan_rows))
    ]
    decision_rows = _build_weekly_decision_evidence_rows(
        [],
        strategy_context_rows=[
            {
                "portfolio_id": "US:pure_strategy",
                "strategy_effective_controls_note": "candidate-only week; no broker orders submitted",
            }
        ],
        attribution_rows=[
            {
                "portfolio_id": "US:pure_strategy",
                "strategy_control_weight_delta": 0.0,
                "execution_gate_blocked_weight": 0.0,
            }
        ],
        snapshot_rows=snapshot_rows,
        outcome_rows=_outcome_rows(snapshot_rows),
    )
    unified_rows = _build_unified_evidence_rows(decision_rows)
    review_rows = _build_candidate_model_review_rows(unified_rows)

    assert len(decision_rows) == 3
    assert {row["decision_source"] for row in decision_rows} == {"candidate_snapshot"}
    assert {int(row["candidate_only_flag"]) for row in unified_rows} == {1}
    assert {int(row["allowed_flag"]) for row in unified_rows} == {0}
    assert {int(row["blocked_flag"]) for row in unified_rows} == {0}
    assert {row["join_quality"] for row in unified_rows} == {"candidate_outcome_only"}

    assert len(review_rows) == 1
    review = review_rows[0]
    assert review["review_label"] == "SIGNAL_RANKING_WORKING"
    assert int(review["candidate_only_count"]) == 3
    assert int(review["labeled_candidate_count"]) == 3
    assert float(review["top_minus_bottom_outcome_20d_bps"]) > 25.0
    assert "没有成交" in str(review["recommendation"])
