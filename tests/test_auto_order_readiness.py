from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from src.common.auto_order_readiness import (
    BLOCKED_STATUS,
    DISABLED_STATUS,
    READY_STATUS,
    WARNING_STATUS,
    build_auto_order_frequency_plan,
    build_auto_order_recovery_plan,
    build_auto_order_submit_capacity_plan,
    build_auto_order_submit_plan,
    build_auto_order_readiness_summary,
    build_stale_execution_refresh_plan,
    evaluate_auto_order_readiness,
    evaluate_auto_order_recovery_eligibility,
)
from src.tools.review_auto_order_readiness import (
    _overlay_gateway_budget_evidence,
    build_auto_order_readiness_payload,
)


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def test_gateway_budget_artifact_overrides_stale_weekly_embedded_budget() -> None:
    result = _overlay_gateway_budget_evidence(
        {
            "generated_at": "2026-06-10T00:00:00+00:00",
            "ibkr_gateway_budget": {"status": "degraded"},
            "ibkr_gateway_budget_rows": [{"market": "US", "status": "degraded"}],
        },
        {
            "generated_at": "2026-06-12T00:00:00+00:00",
            "summary": {"status": "ok"},
            "rows": [{"market": "US", "status": "ok"}],
        },
    )

    assert result["ibkr_gateway_budget"]["status"] == "ok"
    assert result["ibkr_gateway_budget_rows"][0]["status"] == "ok"
    assert result["ibkr_gateway_budget_generated_at"] == "2026-06-12T00:00:00+00:00"


def _portfolio(**overrides):
    row = {
        "market": "US",
        "portfolio_id": "US:watchlist",
        "account_mode": "paper",
        "run_investment_execution": True,
        "submit_investment_execution": True,
    }
    row.update(overrides)
    return row


def _preflight(**overrides):
    row = {
        "generated_at": "2026-05-10T11:30:00+00:00",
        "fail_count": 0,
        "warn_count": 0,
    }
    row.update(overrides)
    return row


def _weekly(**overrides):
    row = {
        "generated_at": "2026-05-10T11:45:00+00:00",
        "strategy_parameter_suggestion_effectiveness": {
            "open_suggestion_count": 0,
            "stale_suggestion_count": 0,
            "auto_apply_count": 0,
            "degraded_followup_count": 0,
        },
        "ibkr_gateway_budget": {"status": "ok"},
    }
    row.update(overrides)
    return row


def _market_readiness_row(**overrides):
    row = {
        "market": "US",
        "portfolio_id": "US:watchlist",
        "readiness_status": "READY_FOR_PAPER_REVIEW",
        "primary_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
        "artifact_health_status": "FRESH",
        "small_account_feasibility_status": "CONFIG_TRADABLE",
    }
    row.update(overrides)
    return {"rows": [row], "preparation_plan": [{"portfolio_id": row["portfolio_id"], "priority_tier": "REVIEW_FOR_PAPER"}]}


def test_auto_order_readiness_allows_fresh_paper_submit() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["status"] == READY_STATUS
    assert result["primary_reason"] == "ready"


def test_auto_order_readiness_blocks_market_readiness_not_ready() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        market_readiness_summary=_market_readiness_row(
            readiness_status="PLANNED_MARKET_CLOSED",
            primary_reason="MARKET_CLOSED_FOR_SUBMIT",
            artifact_health_status="FRESH",
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "market_readiness_not_ready"
    assert result["market_readiness_status"] == "PLANNED_MARKET_CLOSED"


def test_auto_order_readiness_blocks_submit_quality_not_pass() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        market_readiness_summary=_market_readiness_row(
            order_count=1,
            submit_quality_status="BLOCKED",
            submit_quality_reason="net_edge_below_min",
            submit_quality_min_net_edge_bps=5.0,
            submit_quality_min_edge_margin_bps=2.0,
            submit_quality_max_expected_cost_bps=24.0,
            submit_quality_order_types="LMT",
        ),
        policy={"enabled": True, "block_on_submit_quality_not_pass": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "submit_quality_not_pass"
    assert "submit_quality_not_pass" in result["hard_blocks"]
    assert result["submit_quality_status"] == "BLOCKED"


def test_auto_order_readiness_surfaces_submit_quality_tier() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        market_readiness_summary=_market_readiness_row(
            order_count=1,
            submit_quality_status="PASS",
            submit_quality_tier="HIGH",
            submit_quality_min_net_edge_bps=22.0,
            submit_quality_min_edge_margin_bps=9.0,
            submit_quality_max_expected_cost_bps=19.0,
            submit_quality_order_types="LMT",
        ),
        policy={"enabled": True, "block_on_submit_quality_not_pass": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["submit_quality_status"] == "PASS"
    assert result["submit_quality_tier"] == "HIGH"
    assert result["submit_quality_min_net_edge_bps"] == 22.0


def test_auto_order_readiness_prioritizes_gateway_unavailable() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(generated_at="2026-04-01T00:00:00+00:00"),
        weekly_summary=_weekly(),
        market_readiness_summary=_market_readiness_row(
            readiness_status="BLOCKED",
            primary_reason="IBKR_GATEWAY_UNAVAILABLE",
            artifact_health_status="DEGRADED_GATEWAY",
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "ibkr_gateway_unavailable"
    assert "ibkr_gateway_unavailable" in result["hard_blocks"]
    assert "market_readiness_not_ready" in result["hard_blocks"]
    assert "preflight_stale" in result["hard_blocks"]


def test_auto_order_readiness_scopes_market_readiness_to_portfolio() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        market_readiness_summary={
            "rows": [
                _market_readiness_row(
                    market="HK",
                    portfolio_id="HK:resolved_hk_top100_bluechip",
                    readiness_status="BLOCKED",
                    primary_reason="IBKR_GATEWAY_UNAVAILABLE",
                )["rows"][0],
                _market_readiness_row(portfolio_id="US:watchlist")["rows"][0],
            ]
        },
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["market_readiness_status"] == "READY_FOR_PAPER_REVIEW"


def test_auto_order_readiness_excludes_cn_market_even_if_submit_enabled() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="CN", portfolio_id="CN:cn_top_quality"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        policy={"enabled": True, "excluded_markets": ["CN"]},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["status"] == DISABLED_STATUS
    assert result["primary_reason"] == "auto_submit_market_excluded"


def test_auto_order_submit_plan_selects_single_small_ready_candidate() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 87.0,
                "market_readiness_planned_order_symbols": "SPLG",
            }
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
        },
    )

    assert plan["ready"] is True
    assert plan["status"] == "READY_SINGLE_CANDIDATE"
    assert plan["selected_portfolio_id"] == "US:watchlist"
    assert plan["selected_portfolio_ids"] == ["US:watchlist"]
    assert plan["selected_order_count"] == 1


def test_auto_order_submit_plan_blocks_multi_order_candidate() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 2,
                "market_readiness_planned_gross_order_value": 87.0,
            }
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
        },
    )

    assert plan["ready"] is False
    assert plan["reason"] == "no_single_safe_submit_candidate"
    assert plan["rejected_candidates"][0]["reject_reasons"] == ["order_count_exceeds_policy"]
    assert plan["frontier_candidates"][0]["policy_reject_reasons"] == ["order_count_exceeds_policy"]


def test_auto_order_submit_plan_requires_operator_selection_for_multiple_candidates() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 87.0,
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "ASX",
                "portfolio_id": "ASX:asx_top_quality",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 80.0,
            },
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
        },
    )

    assert plan["ready"] is False
    assert plan["status"] == "REVIEW_REQUIRED"
    assert plan["reason"] == "multiple_submit_candidates_require_operator_selection"
    assert plan["frontier_candidate_count"] == 2


def test_submit_capacity_stays_at_small_account_baseline_without_fill_evidence() -> None:
    capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "submitted_order_rows": 0,
                    "fill_count": 0,
                    "avg_actual_slippage_bps": None,
                }
            ],
            "edge_realization_summary": [
                {
                    "matured_5d_sample_count": 0,
                    "matured_5d_avg_realized_edge_bps": None,
                }
            ],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_total_gross_order_value": 400.0,
            "baseline_submit_portfolios_per_run": 1,
            "baseline_submit_total_gross_order_value": 100.0,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
        },
    )

    assert capacity["status"] == "BASELINE_INSUFFICIENT_EVIDENCE"
    assert capacity["scale_allowed"] is False
    assert capacity["effective_max_submit_portfolios_per_run"] == 1
    assert capacity["effective_max_submit_total_gross_order_value"] == 100.0


def test_submit_capacity_reaches_full_scale_when_all_configured_evidence_passes() -> None:
    weekly = {
        "execution_session_summary": [
            {
                "market": "US",
                "submitted_order_rows": 6,
                "fill_count": 6,
                "avg_actual_slippage_bps": 4.0,
            }
        ],
        "execution_feedback_summary": [{"market": "US", "error_order_rows": 0}],
        "edge_realization_summary": [
            {
                "market": "US",
                "matured_5d_sample_count": 6,
                "matured_5d_avg_realized_edge_bps": 12.0,
            }
        ],
    }
    policy = {
        "enabled": True,
        "evidence_scaled_submit_enabled": True,
        "max_submit_portfolios_per_run": 4,
        "max_submit_total_gross_order_value": 400.0,
        "baseline_submit_portfolios_per_run": 1,
        "baseline_submit_total_gross_order_value": 100.0,
        "scale_min_filled_orders": 5,
        "scale_min_matured_edge_samples": 5,
        "scale_min_realized_edge_bps": 0.0,
        "scale_max_abs_slippage_bps": 15.0,
        "scale_max_error_rate": 0.05,
    }

    capacity = build_auto_order_submit_capacity_plan(weekly, policy=policy)

    assert capacity["status"] == "FULL_SCALE_ALLOWED"
    assert capacity["scale_allowed"] is True
    assert capacity["scale_stage"] == "full"
    assert capacity["effective_max_submit_portfolios_per_run"] == 4
    assert capacity["effective_max_submit_total_gross_order_value"] == 400.0


def test_submit_capacity_uses_trial_stage_before_stricter_full_scale_thresholds() -> None:
    capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "market": "US",
                    "submitted_order_rows": 6,
                    "fill_count": 6,
                    "fill_notional": 480.0,
                    "avg_actual_slippage_bps": 4.0,
                }
            ],
            "execution_feedback_summary": [{"market": "US", "error_order_rows": 0}],
            "edge_realization_summary": [
                {
                    "market": "US",
                    "matured_5d_sample_count": 6,
                    "matured_5d_avg_realized_edge_bps": 12.0,
                }
            ],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_total_gross_order_value": 400.0,
            "baseline_submit_portfolios_per_run": 1,
            "baseline_submit_total_gross_order_value": 100.0,
            "trial_submit_portfolios_per_run": 2,
            "trial_submit_total_gross_order_value": 200.0,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
            "full_scale_min_filled_orders": 20,
            "full_scale_min_matured_edge_samples": 15,
            "full_scale_min_evidence_markets": 2,
        },
    )

    assert capacity["status"] == "TRIAL_SCALE_ALLOWED"
    assert capacity["scale_stage"] == "trial"
    assert capacity["effective_max_submit_portfolios_per_run"] == 2
    assert capacity["effective_max_submit_total_gross_order_value"] == 200.0
    assert capacity["evidence_market_count"] == 1


def test_submit_capacity_weights_slippage_by_fill_notional() -> None:
    capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "market": "US",
                    "submitted_order_rows": 1,
                    "fill_count": 1,
                    "fill_notional": 900.0,
                    "avg_actual_slippage_bps": 2.0,
                },
                {
                    "market": "US",
                    "submitted_order_rows": 1,
                    "fill_count": 1,
                    "fill_notional": 100.0,
                    "avg_actual_slippage_bps": 30.0,
                },
            ],
            "edge_realization_summary": [],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
        },
    )

    assert capacity["avg_realized_slippage_bps"] == 4.8


def test_submit_capacity_rejects_missing_or_non_finite_quality_evidence() -> None:
    capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "market": "US",
                    "submitted_order_rows": 6,
                    "fill_count": 6,
                    "avg_actual_slippage_bps": float("nan"),
                }
            ],
            "execution_feedback_summary": [{"market": "US", "error_order_rows": 0}],
            "edge_realization_summary": [
                {
                    "market": "US",
                    "matured_5d_sample_count": 6,
                    "matured_5d_avg_realized_edge_bps": 12.0,
                }
            ],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
        },
    )

    assert capacity["status"] == "HOLD_QUALITY_DEGRADED"
    assert capacity["scale_stage"] == "baseline"
    assert capacity["reason"] == "realized_slippage_missing"
    assert capacity["matured_5d_sample_count"] == 6
    assert capacity["avg_realized_slippage_bps"] is None

    missing_edge_capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "market": "US",
                    "submitted_order_rows": 6,
                    "fill_count": 6,
                    "avg_actual_slippage_bps": 4.0,
                }
            ],
            "edge_realization_summary": [
                {
                    "market": "US",
                    "matured_5d_sample_count": 6,
                    "matured_5d_avg_realized_edge_bps": float("nan"),
                }
            ],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
        },
    )
    assert missing_edge_capacity["status"] == "BASELINE_INSUFFICIENT_EVIDENCE"
    assert missing_edge_capacity["matured_5d_sample_count"] == 0


def test_submit_capacity_holds_baseline_when_realized_quality_degrades() -> None:
    capacity = build_auto_order_submit_capacity_plan(
        {
            "execution_session_summary": [
                {
                    "submitted_order_rows": 8,
                    "fill_count": 8,
                    "avg_actual_slippage_bps": 19.0,
                }
            ],
            "execution_feedback_summary": [{"error_order_rows": 1}],
            "edge_realization_summary": [
                {
                    "matured_5d_sample_count": 8,
                    "matured_5d_avg_realized_edge_bps": -3.0,
                }
            ],
        },
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_total_gross_order_value": 400.0,
            "baseline_submit_portfolios_per_run": 1,
            "baseline_submit_total_gross_order_value": 100.0,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
            "scale_min_realized_edge_bps": 0.0,
            "scale_max_abs_slippage_bps": 15.0,
            "scale_max_error_rate": 0.05,
        },
    )

    assert capacity["status"] == "HOLD_QUALITY_DEGRADED"
    assert capacity["scale_allowed"] is False
    assert capacity["reason"] == (
        "realized_edge_below_min,realized_slippage_above_max,"
        "execution_error_rate_above_max"
    )
    assert capacity["effective_max_submit_portfolios_per_run"] == 1
    assert capacity["effective_max_submit_total_gross_order_value"] == 100.0


def test_submit_plan_applies_evidence_capacity_to_multiple_ready_markets() -> None:
    rows = [
        {
            "ready": True,
            "account_mode": "paper",
            "market": market,
            "portfolio_id": f"{market}:portfolio",
            "market_readiness_status": "READY_FOR_PAPER_REVIEW",
            "market_readiness_order_count": 1,
            "market_readiness_planned_gross_order_value": 80.0,
            "market_readiness_planned_buy_order_value": 80.0,
        }
        for market in ("US", "ASX")
    ]
    plan = build_auto_order_submit_plan(
        rows,
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_portfolios_per_market": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
            "max_submit_total_gross_order_value": 400.0,
            "baseline_submit_portfolios_per_run": 1,
            "baseline_submit_total_gross_order_value": 100.0,
            "require_buy_order_for_submit": True,
        },
        weekly_summary={},
    )

    assert plan["status"] == "REVIEW_REQUIRED"
    assert plan["reason"] == "multiple_submit_candidates_require_operator_selection"
    assert plan["policy"]["max_submit_portfolios_per_run"] == 1
    assert plan["policy"]["configured_max_submit_portfolios_per_run"] == 4
    assert plan["submit_capacity_plan"]["status"] == "BASELINE_INSUFFICIENT_EVIDENCE"


def test_trial_submit_plan_allows_only_one_market_without_realized_evidence() -> None:
    rows = [
        {
            "ready": True,
            "account_mode": "paper",
            "market": market,
            "portfolio_id": f"{market}:portfolio",
            "market_readiness_status": "READY_FOR_PAPER_REVIEW",
            "market_readiness_order_count": 1,
            "market_readiness_planned_gross_order_value": gross,
            "market_readiness_planned_buy_order_value": gross,
        }
        for market, gross in (("US", 60.0), ("ASX", 70.0), ("HK", 80.0))
    ]
    plan = build_auto_order_submit_plan(
        rows,
        policy={
            "enabled": True,
            "evidence_scaled_submit_enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_portfolios_per_market": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
            "max_submit_total_gross_order_value": 400.0,
            "baseline_submit_portfolios_per_run": 1,
            "baseline_submit_total_gross_order_value": 100.0,
            "trial_submit_portfolios_per_run": 2,
            "trial_submit_total_gross_order_value": 200.0,
            "scale_min_filled_orders": 5,
            "scale_min_matured_edge_samples": 5,
            "full_scale_min_filled_orders": 20,
            "full_scale_min_matured_edge_samples": 15,
            "full_scale_min_evidence_markets": 2,
            "max_submit_unevidenced_markets_per_run": 1,
            "require_buy_order_for_submit": True,
        },
        weekly_summary={
            "execution_session_summary": [
                {
                    "market": "US",
                    "submitted_order_rows": 6,
                    "fill_count": 6,
                    "fill_notional": 480.0,
                    "avg_actual_slippage_bps": 4.0,
                }
            ],
            "execution_feedback_summary": [{"market": "US", "error_order_rows": 0}],
            "edge_realization_summary": [
                {
                    "market": "US",
                    "matured_5d_sample_count": 6,
                    "matured_5d_avg_realized_edge_bps": 12.0,
                }
            ],
        },
    )

    assert plan["status"] == "READY_MULTI_CANDIDATE"
    assert plan["selected_markets"] == ["US", "ASX"]
    assert plan["submit_capacity_plan"]["scale_stage"] == "trial"
    assert plan["rejected_candidates"][0]["market"] == "HK"
    assert plan["rejected_candidates"][0]["reject_reasons"] == [
        "unevidenced_market_count_exceeds_policy"
    ]


def test_auto_order_submit_plan_allows_multi_market_candidates_with_market_cap() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 87.0,
                "market_readiness_planned_buy_order_value": 87.0,
                "market_readiness_planned_order_symbols": "SPLG",
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:us_overnight_core",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 95.0,
                "market_readiness_planned_buy_order_value": 95.0,
                "market_readiness_planned_order_symbols": "SPYI",
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "HK",
                "portfolio_id": "HK:resolved_hk_top100_bluechip",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 72.0,
                "market_readiness_planned_buy_order_value": 72.0,
                "market_readiness_planned_order_symbols": "2800.HK",
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "ASX",
                "portfolio_id": "ASX:asx_top_quality",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 63.0,
                "market_readiness_planned_buy_order_value": 63.0,
                "market_readiness_planned_order_symbols": "VAS.AX",
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "XETRA",
                "portfolio_id": "XETRA:xetra_top_quality",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 82.0,
                "market_readiness_planned_buy_order_value": 82.0,
                "market_readiness_planned_order_symbols": "EXS1.DE",
            },
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 4,
            "max_submit_portfolios_per_market": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
            "max_submit_total_gross_order_value": 400.0,
            "require_buy_order_for_submit": True,
            "excluded_markets": ["CN"],
        },
    )

    assert plan["ready"] is True
    assert plan["status"] == "READY_MULTI_CANDIDATE"
    assert plan["reason"] == "multi_market_safe_paper_submit_candidates"
    assert plan["selected_portfolio_ids"] == [
        "ASX:asx_top_quality",
        "HK:resolved_hk_top100_bluechip",
        "XETRA:xetra_top_quality",
        "US:watchlist",
    ]
    assert plan["selected_total_order_count"] == 4
    assert plan["selected_total_planned_gross_order_value"] == 304.0
    assert plan["rejected_candidates"][0]["portfolio_id"] == "US:us_overnight_core"
    assert plan["rejected_candidates"][0]["reject_reasons"] == ["market_portfolio_count_exceeds_policy"]


def test_auto_order_submit_plan_prioritizes_high_quality_candidates() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:standard",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 40.0,
                "market_readiness_planned_buy_order_value": 40.0,
                "market_readiness_planned_order_symbols": "SCHB",
                "submit_quality_status": "PASS",
                "submit_quality_tier": "PASS",
                "submit_quality_min_net_edge_bps": 11.0,
                "submit_quality_min_edge_margin_bps": 4.0,
            },
            {
                "ready": True,
                "account_mode": "paper",
                "market": "ASX",
                "portfolio_id": "ASX:high",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 90.0,
                "market_readiness_planned_buy_order_value": 90.0,
                "market_readiness_planned_order_symbols": "VAS.AX",
                "submit_quality_status": "PASS",
                "submit_quality_tier": "HIGH",
                "submit_quality_min_net_edge_bps": 24.0,
                "submit_quality_min_edge_margin_bps": 11.0,
            },
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
            "require_buy_order_for_submit": True,
        },
    )

    assert plan["ready"] is False
    assert plan["candidate_portfolios"][0]["portfolio_id"] == "ASX:high"
    assert plan["frontier_candidates"][0]["portfolio_id"] == "ASX:high"


def test_auto_order_submit_plan_surfaces_blocked_frontier_candidates() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": False,
                "status": BLOCKED_STATUS,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_reason": "preflight_stale",
                "hard_blocks": ["preflight_stale", "market_readiness_not_ready"],
                "hard_block_details": [
                    {
                        "reason": "preflight_stale",
                        "remediation": "Refresh supervisor preflight before automated submit.",
                    }
                ],
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 87.0,
                "market_readiness_planned_order_symbols": "SPLG",
            },
            {
                "ready": False,
                "status": BLOCKED_STATUS,
                "account_mode": "paper",
                "market": "HK",
                "portfolio_id": "HK:watchlist",
                "primary_reason": "market_readiness_not_ready",
                "hard_blocks": ["market_readiness_not_ready"],
                "market_readiness_status": "BLOCKED",
                "market_readiness_order_count": 0,
                "market_readiness_planned_gross_order_value": 0.0,
            },
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
        },
    )

    assert plan["ready"] is False
    assert plan["frontier_candidate_count"] == 2
    assert plan["frontier_candidates"][0]["portfolio_id"] == "US:watchlist"
    assert plan["frontier_candidates"][0]["frontier_reason"] == "preflight_stale"
    assert plan["frontier_candidates"][0]["order_count"] == 1
    assert plan["frontier_candidates"][0]["planned_order_symbols"] == "SPLG"
    assert "Refresh supervisor preflight" in plan["frontier_candidates"][0]["next_action"]


def test_auto_order_submit_plan_rejects_sell_only_growth_submit_candidate() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": True,
                "status": READY_STATUS,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "READY_FOR_PAPER_REVIEW",
                "market_readiness_order_count": 1,
                "market_readiness_planned_gross_order_value": 29.3,
                "market_readiness_planned_buy_order_value": 0.0,
                "market_readiness_planned_sell_order_value": 29.3,
                "market_readiness_planned_order_symbols": "SCHX",
            }
        ],
        policy={
            "enabled": True,
            "max_submit_portfolios_per_run": 1,
            "max_submit_orders_per_portfolio": 1,
            "max_submit_gross_order_value": 100.0,
            "require_buy_order_for_submit": True,
        },
    )

    assert plan["ready"] is False
    assert plan["status"] == "BLOCKED"
    assert plan["candidate_count"] == 0
    assert plan["policy"]["require_buy_order_for_submit"] is True
    assert plan["rejected_candidates"][0]["planned_sell_order_value"] == 29.3
    assert "no_buy_order_for_growth_submit" in plan["rejected_candidates"][0]["reject_reasons"]
    assert "no_buy_order_for_growth_submit" in plan["frontier_candidates"][0]["policy_reject_reasons"]


def test_auto_order_submit_plan_uses_frontier_reason_remediation() -> None:
    plan = build_auto_order_submit_plan(
        [
            {
                "ready": False,
                "status": BLOCKED_STATUS,
                "account_mode": "paper",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "hard_blocks": ["preflight_stale", "ibkr_gateway_unavailable", "market_readiness_not_ready"],
                "hard_block_details": [
                    {
                        "reason": "preflight_stale",
                        "remediation": "Refresh supervisor preflight before automated submit.",
                    },
                    {
                        "reason": "ibkr_gateway_unavailable",
                        "remediation": "Start or unlock IB Gateway paper API, then rerun no-submit.",
                    },
                ],
                "market_readiness_status": "BLOCKED",
                "market_readiness_order_count": 0,
                "market_readiness_planned_gross_order_value": 0.0,
            }
        ],
        policy={"enabled": True},
    )

    assert plan["frontier_candidates"][0]["frontier_reason"] == "ibkr_gateway_unavailable"
    assert "Start or unlock IB Gateway" in plan["frontier_candidates"][0]["next_action"]


def test_auto_order_readiness_payload_builds_market_readiness_when_missing(tmp_path: Path) -> None:
    reports_root = tmp_path / "reports"
    report_dir = reports_root / "watchlist"
    report_dir.mkdir(parents=True)
    (report_dir / "investment_execution_summary.json").write_text(
        json.dumps(
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "paper_submit_ready": True,
                "paper_submit_readiness_status": "READY",
                "primary_no_order_reason": "ORDERS_PLANNED_NOT_SUBMITTED",
                "order_count": 1,
                "broker_equity": 1000.0,
            }
        ),
        encoding="utf-8",
    )
    (report_dir / "investment_execution_plan.csv").write_text(
        "\n".join(
            [
                (
                    "symbol,action,status,expected_edge_bps,expected_cost_bps,edge_gate_threshold_bps,"
                    "whole_share_edge_margin_bps,dynamic_order_adv_pct,execution_order_type,"
                    "edge_gate_status,quality_status,market_rule_status,shadow_review_status,manual_review_status"
                ),
                "SPLG,BUY,PLANNED,34,22,28,6,0.0001,LMT,PASS,QUALITY_OK,RULES_OK,AUTO_OK,AUTO_OK",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    preflight = tmp_path / "preflight.json"
    weekly = tmp_path / "weekly.json"
    preflight.write_text(json.dumps({"generated_at": "2999-01-01T00:00:00+00:00", "fail_count": 0}), encoding="utf-8")
    weekly.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "ibkr_gateway_budget": {"status": "ok"},
                "strategy_parameter_suggestion_effectiveness": {},
            }
        ),
        encoding="utf-8",
    )
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                "auto_order_readiness:",
                "  enabled: true",
                "markets:",
                "  - name: us",
                "    market: US",
                "    enabled: true",
                "    reports:",
                "      - kind: investment",
                f"        out_dir: {reports_root}",
                "        watchlist_yaml: config/watchlist.yaml",
                "        run_investment_execution: true",
                "        submit_investment_execution: true",
            ]
        ),
        encoding="utf-8",
    )

    payload = build_auto_order_readiness_payload(
        config_path=str(cfg_path),
        preflight_summary_path=str(preflight),
        weekly_summary_path=str(weekly),
        market_readiness_path=str(tmp_path / "missing_market_readiness.json"),
        runtime_root=str(tmp_path / "runtime"),
    )

    row = payload["rows"][0]
    assert row["ready"] is True
    assert row["market_readiness_status"] == "READY_FOR_PAPER_REVIEW"
    assert payload["summary"]["recovery_eligibility"]["reason"] == "submit_plan_ready_no_refresh"
    assert payload["summary"]["recovery_eligibility"]["eligible"] is False


def test_auto_order_readiness_uses_scoped_watchlist_expansion_artifact(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    expansion_path = (
        runtime_root
        / "reports_supervisor"
        / "watchlist_expansion"
        / "watchlist_expansion_summary.json"
    )
    expansion_path.parent.mkdir(parents=True)
    expansion_path.write_text(
        json.dumps(
            {
                "seed_promotion_review_count": 8,
                "seed_promotion_ready_count": 0,
                "seed_promotion_candidate_report_required_count": 2,
                "seed_evidence_queue": [
                    {
                        "market": "ASX",
                        "status": "READY",
                        "symbols": ["DHHF.AX", "BGBL.AX"],
                    }
                ],
                "seed_evidence_primary_market": "ASX",
                "seed_evidence_primary_symbols": ["DHHF.AX", "BGBL.AX"],
                "seed_evidence_mode": "YFINANCE_ONLY",
            }
        ),
        encoding="utf-8",
    )
    cfg_path = tmp_path / "supervisor.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                'summary_out_dir: "reports_supervisor"',
                "scope_summary_out_dir: true",
                "auto_order_readiness:",
                "  enabled: true",
                "markets: []",
            ]
        ),
        encoding="utf-8",
    )

    payload = build_auto_order_readiness_payload(
        config_path=str(cfg_path),
        runtime_root=str(runtime_root),
    )

    assert payload["watchlist_expansion_path"] == str(expansion_path)
    frequency = payload["summary"]["frequency_plan"]
    assert frequency["seed_promotion_review_count"] == 8
    assert frequency["seed_promotion_candidate_report_required_count"] == 2
    assert frequency["seed_evidence_queue_count"] == 1
    assert frequency["seed_evidence_ready_job_count"] == 1
    assert frequency["seed_evidence_primary_market"] == "ASX"
    assert frequency["seed_evidence_primary_symbols"] == ["DHHF.AX", "BGBL.AX"]


def test_auto_order_readiness_blocks_live_without_explicit_policy() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(account_mode="live"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(),
        policy={"enabled": True, "allow_live_submit": False},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["status"] == BLOCKED_STATUS
    assert "live_submit_not_allowed" in result["hard_blocks"]


def test_auto_order_readiness_blocks_stale_preflight() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(generated_at="2026-05-08T11:00:00+00:00"),
        weekly_summary=_weekly(),
        policy={"enabled": True, "max_preflight_age_hours": 24},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "preflight_stale"


def test_auto_order_readiness_surfaces_offline_recovery_after_stale_artifacts() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(generated_at="2026-05-08T11:00:00+00:00"),
        weekly_summary=_weekly(),
        market_readiness_summary=_market_readiness_row(
            artifact_health_status="STALE",
            execution_artifact_age_hours=49.0,
        ),
        policy={"enabled": True, "max_preflight_age_hours": 24, "max_offline_recovery_gap_hours": 24},
        now=NOW,
    )

    assert result["offline_recovery_required"] is True
    assert "preflight_stale_after_offline_gap" in result["offline_recovery_reasons"]
    assert "market_readiness_artifact_stale" in result["offline_recovery_reasons"]
    assert "Refresh investment report" in result["offline_recovery_next_action"]


def test_auto_order_readiness_scopes_preflight_failures_to_portfolio() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist", watchlist="watchlist"),
        preflight_summary=_preflight(
            fail_count=1,
            checks=[
                {
                    "name": "HK:resolved_hk_top100_bluechip:watchlist",
                    "status": "FAIL",
                }
            ],
        ),
        weekly_summary=_weekly(),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["preflight_fail_count"] == 0


def test_auto_order_readiness_global_preflight_failure_blocks_all() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist", watchlist="watchlist"),
        preflight_summary=_preflight(
            fail_count=1,
            checks=[
                {
                    "name": "runtime_root",
                    "status": "FAIL",
                }
            ],
        ),
        weekly_summary=_weekly(),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "preflight_failed"
    assert result["hard_block_details"][0]["detail"] == "runtime_root"


def test_auto_order_readiness_warns_on_open_strategy_suggestions() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            strategy_parameter_suggestion_effectiveness={
                "open_suggestion_count": 2,
                "stale_suggestion_count": 0,
                "auto_apply_count": 0,
                "degraded_followup_count": 0,
            }
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["status"] == WARNING_STATUS
    assert "strategy_suggestions_open" in result["warnings"]


def test_auto_order_readiness_scopes_strategy_suggestions_to_portfolio() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            strategy_parameter_suggestions=[
                {
                    "suggestion_id": "hk-1",
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "status": "SUGGESTED",
                }
            ],
            strategy_parameter_suggestion_effectiveness={
                "open_suggestion_count": 1,
                "stale_suggestion_count": 0,
                "auto_apply_count": 0,
                "degraded_followup_count": 0,
            },
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["status"] == READY_STATUS
    assert result["strategy_open_suggestion_count"] == 0


def test_auto_order_readiness_dedupes_carried_strategy_suggestions() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="HK", portfolio_id="HK:resolved_hk_top100_bluechip"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            strategy_parameter_suggestions=[
                {
                    "suggestion_id": "w19",
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "primary_field": "mr_weight",
                    "config_path": "engine.mr_weight",
                    "status": "SUGGESTED",
                    "created_at": "2026-05-05T00:00:00+00:00",
                    "carried_forward": 1,
                },
                {
                    "suggestion_id": "w20",
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "primary_field": "mr_weight",
                    "config_path": "engine.mr_weight",
                    "status": "SUGGESTED",
                    "created_at": "2026-05-10T00:00:00+00:00",
                },
            ],
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["status"] == WARNING_STATUS
    assert result["strategy_open_suggestion_count"] == 1


def test_auto_order_readiness_blocks_degraded_followup() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            strategy_parameter_suggestion_effectiveness={
                "open_suggestion_count": 0,
                "stale_suggestion_count": 0,
                "auto_apply_count": 0,
                "degraded_followup_count": 1,
            }
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert "strategy_followup_degraded" in result["hard_blocks"]


def test_auto_order_readiness_scopes_strategy_followup_to_portfolio() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            strategy_parameter_suggestions=[
                {
                    "suggestion_id": "us-1",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "status": "SUGGESTED",
                }
            ],
            strategy_parameter_suggestion_followup=[
                {
                    "suggestion_id": "hk-1",
                    "market": "HK",
                    "portfolio_id": "HK:resolved_hk_top100_bluechip",
                    "followup_verdict": "DEGRADED",
                }
            ],
            strategy_parameter_suggestion_effectiveness={
                "open_suggestion_count": 1,
                "stale_suggestion_count": 0,
                "auto_apply_count": 0,
                "degraded_followup_count": 1,
            },
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert "strategy_followup_degraded" not in result["hard_blocks"]


def test_auto_order_readiness_scopes_gateway_budget_to_market() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            ibkr_gateway_budget={"status": "degraded"},
            ibkr_gateway_budget_rows=[
                {"market": "HK", "status": "degraded", "reason": "over_budget"},
                {"market": "US", "status": "ok", "reason": "under_budget"},
            ],
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["gateway_budget_status"] == "ok"


def test_auto_order_readiness_blocks_matching_gateway_budget_degraded() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            ibkr_gateway_budget_rows=[
                {
                    "market": "US",
                    "status": "degraded",
                    "reason": "gateway_request_budget_exceeded",
                    "weekly_gateway_request_budget": 2000,
                    "gateway_request_count": 8034,
                    "budget_usage_pct": 401.7,
                    "top_request_kind": "positions",
                    "top_tool": "run_investment_opportunity:us:watchlist",
                    "projected_recovery_days": 3,
                    "projected_recovery_at": "2026-05-13T23:59:59.999999+00:00",
                },
            ],
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is False
    assert result["primary_reason"] == "gateway_budget_degraded"
    detail = result["hard_block_details"][0]
    assert detail["reason"] == "gateway_budget_degraded"
    assert "requests=8034/2000" in detail["detail"]
    assert "top_request_kind=positions" in detail["detail"]
    assert "top_tool=run_investment_opportunity:us:watchlist" in detail["detail"]
    assert "projected_recovery_days=3" in detail["detail"]
    assert "after 2026-05-13T23:59:59.999999+00:00" in detail["remediation"]
    assert result["gateway_budget_request_count"] == 8034
    assert result["gateway_budget_request_limit"] == 2000
    assert result["gateway_budget_top_tool"] == "run_investment_opportunity:us:watchlist"
    assert result["gateway_budget_projected_recovery_at"] == "2026-05-13T23:59:59.999999+00:00"


def test_auto_order_readiness_allows_submit_when_only_research_budget_is_degraded() -> None:
    result = evaluate_auto_order_readiness(
        _portfolio(market="US", portfolio_id="US:watchlist"),
        preflight_summary=_preflight(),
        weekly_summary=_weekly(
            ibkr_gateway_budget_rows=[
                {
                    "market": "US",
                    "status": "degraded",
                    "reason": "gateway_request_budget_exceeded",
                    "weekly_gateway_request_budget": 2000,
                    "gateway_request_count": 3062,
                    "budget_usage_pct": 153.1,
                    "submit_blocking": False,
                    "execution_capacity_status": "ok",
                    "execution_gateway_request_count": 4,
                    "execution_reserve_weekly_requests": 300,
                    "research_throttled": True,
                    "research_recent_24h_request_count": 320,
                    "research_daily_request_budget": 129,
                    "short_window_gateway_request_count": 2,
                    "short_window_request_limit": 50,
                },
            ],
        ),
        policy={"enabled": True},
        now=NOW,
    )

    assert result["ready"] is True
    assert result["status"] == WARNING_STATUS
    assert "gateway_budget_degraded" not in result["hard_blocks"]
    assert "gateway_budget_research_degraded" in result["warnings"]
    assert result["gateway_budget_submit_blocking"] is False
    assert result["gateway_execution_capacity_status"] == "ok"
    assert result["gateway_research_throttled"] is True
    assert result["offline_recovery_required"] is False


def test_auto_order_readiness_summary_counts_rows() -> None:
    summary = build_auto_order_readiness_summary(
        [
            {"status": READY_STATUS, "ready": True},
            {"status": WARNING_STATUS, "ready": True, "warnings": ["strategy_suggestions_open"]},
            {
                "status": BLOCKED_STATUS,
                "ready": False,
                "primary_reason": "market_readiness_not_ready",
                "hard_blocks": ["market_readiness_not_ready", "preflight_stale"],
                "hard_block_details": [
                    {
                        "reason": "market_readiness_not_ready",
                        "remediation": "Refresh market readiness before automated submit.",
                    },
                    {
                        "reason": "preflight_stale",
                        "remediation": "Refresh supervisor preflight before automated submit.",
                    },
                ],
                "market": "US",
                "portfolio_id": "US:watchlist",
                "offline_recovery_required": True,
                "offline_recovery_reasons": ["preflight_stale_after_offline_gap"],
            },
        ]
    )

    assert summary["status"] == "blocked"
    assert summary["ready_count"] == 2
    assert summary["blocked_count"] == 1
    assert summary["primary_block_reason"] == "preflight_stale"
    assert summary["hard_block_counts"]["market_readiness_not_ready"] == 1
    assert summary["warning_counts"]["strategy_suggestions_open"] == 1
    assert summary["offline_recovery_required_count"] == 1
    assert summary["offline_recovery_markets"] == ["US"]
    assert summary["offline_recovery_reason_counts"]["preflight_stale_after_offline_gap"] == 1
    assert summary["remediation_plan"][0]["reason"] == "preflight_stale"
    assert summary["remediation_plan"][1]["reason"] == "market_readiness_not_ready"


def test_stale_execution_refresh_plan_ranks_no_submit_target() -> None:
    plan = build_stale_execution_refresh_plan(
        [
            {
                "status": BLOCKED_STATUS,
                "market": "HK",
                "portfolio_id": "HK:bluechip",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "market_readiness_artifact_age_hours": 42,
                "offline_recovery_max_gap_hours": 24,
                "post_cost_positive_edge_count": 5,
                "post_cost_high_cost_positive_edge_count": 2,
                "wait_pullback_close_count": 8,
                "market_readiness_order_count": 1,
                "market_readiness_planned_buy_order_value": 0,
                "market_readiness_planned_order_symbols": "3988.HK",
                "hard_blocks": ["market_readiness_not_ready"],
            },
            {
                "status": BLOCKED_STATUS,
                "market": "ASX",
                "portfolio_id": "ASX:quality",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "market_readiness_artifact_age_hours": 48,
                "offline_recovery_max_gap_hours": 24,
                "post_cost_positive_edge_count": 1,
                "wait_pullback_close_count": 1,
            },
            {
                "status": DISABLED_STATUS,
                "market": "CN",
                "portfolio_id": "CN:research",
                "market_readiness_status": "RESEARCH_ONLY",
                "market_readiness_artifact_health_status": "STALE",
            },
        ]
    )

    assert plan["status"] == "READY_FOR_TARGETED_NO_SUBMIT_REFRESH"
    assert plan["primary_market"] == "HK"
    assert plan["primary_portfolio_id"] == "HK:bluechip"
    assert plan["target_count"] == 2
    assert plan["submit_orders"] is False
    assert plan["does_not_relax_submit_gates"] is True
    assert plan["rows"][0]["action"] == "refresh_report_and_execution_no_submit"


def test_stale_execution_refresh_plan_waits_for_gateway_budget() -> None:
    plan = build_auto_order_readiness_summary(
        [
            {
                "status": BLOCKED_STATUS,
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "market_readiness_artifact_age_hours": 40,
                "post_cost_positive_edge_count": 3,
                "wait_pullback_close_count": 2,
                "hard_blocks": ["gateway_budget_degraded", "market_readiness_not_ready"],
            }
        ]
    )["stale_execution_refresh_plan"]

    assert plan["status"] == "WAIT_GATEWAY_BUDGET"
    assert plan["primary_action"] == "wait_gateway_budget_then_refresh_stale_execution"
    assert plan["rows"][0]["gateway_budget_blocked"] is True


def test_stale_execution_refresh_plan_does_not_wait_for_lower_rank_gateway_block() -> None:
    plan = build_stale_execution_refresh_plan(
        [
            {
                "status": BLOCKED_STATUS,
                "market": "HK",
                "portfolio_id": "HK:bluechip",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "post_cost_positive_edge_count": 8,
                "wait_pullback_close_count": 8,
                "hard_blocks": ["market_readiness_not_ready"],
            },
            {
                "status": BLOCKED_STATUS,
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "post_cost_positive_edge_count": 1,
                "hard_blocks": ["gateway_budget_degraded", "market_readiness_not_ready"],
            },
        ]
    )

    assert plan["status"] == "READY_FOR_TARGETED_NO_SUBMIT_REFRESH"
    assert plan["primary_market"] == "HK"
    assert plan["rows"][1]["gateway_budget_blocked"] is True


def test_stale_execution_refresh_plan_prefers_unblocked_growth_target_over_gateway_blocked_score() -> None:
    plan = build_stale_execution_refresh_plan(
        [
            {
                "status": BLOCKED_STATUS,
                "market": "US",
                "portfolio_id": "US:watchlist",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "market_readiness_artifact_age_hours": 72,
                "offline_recovery_max_gap_hours": 24,
                "post_cost_positive_edge_count": 20,
                "post_cost_high_cost_positive_edge_count": 10,
                "wait_pullback_close_count": 5,
                "market_readiness_order_count": 1,
                "market_readiness_planned_buy_order_value": 0,
                "market_readiness_planned_sell_order_value": 30,
                "hard_blocks": ["gateway_budget_degraded", "market_readiness_not_ready"],
            },
            {
                "status": BLOCKED_STATUS,
                "market": "HK",
                "portfolio_id": "HK:bluechip",
                "market_readiness_status": "NEEDS_REFRESH",
                "market_readiness_reason": "STALE_EXECUTION_ARTIFACT",
                "market_readiness_artifact_health_status": "STALE",
                "market_readiness_artifact_age_hours": 36,
                "offline_recovery_max_gap_hours": 24,
                "post_cost_positive_edge_count": 4,
                "post_cost_high_cost_positive_edge_count": 2,
                "wait_pullback_close_count": 6,
                "market_readiness_planned_buy_order_value": 0,
                "market_readiness_planned_sell_order_value": 0,
                "hard_blocks": ["market_readiness_not_ready"],
            },
        ]
    )

    assert plan["status"] == "READY_FOR_TARGETED_NO_SUBMIT_REFRESH"
    assert plan["primary_market"] == "HK"
    assert plan["primary_portfolio_id"] == "HK:bluechip"
    assert plan["rows"][0]["growth_candidate_supply"] is True
    assert plan["rows"][0]["gateway_budget_blocked"] is False
    assert plan["rows"][1]["gateway_budget_blocked"] is True
    assert plan["rows"][1]["sell_only_current_plan"] is True


def test_auto_order_frequency_plan_surfaces_seed_proposals_without_changing_submit_decision() -> None:
    submit_plan = {
        "status": "BLOCKED",
        "ready": False,
        "reason": "no_single_safe_submit_candidate",
        "candidate_count": 0,
        "frontier_candidates": [],
    }
    expansion_summary = {
        "seed_proposals": [
            {
                "market": "ASX",
                "proposal_action": "create_or_refresh_preferred_asset_seed_watchlist",
                "expansion_target": "seed_preferred_asset_class_candidates",
                "near_miss_symbols": ["BHP.AX", "RIO.AX"],
                "auto_apply": False,
                "submit_gate_policy": "do_not_relax_submit_gates",
            }
        ],
        "seed_intake_plan": [
            {
                "market": "ASX",
                "intake_status": "MANUAL_REVIEW_REQUIRED",
                "source_candidate_count": 2,
            }
        ],
    }

    plan = build_auto_order_frequency_plan(
        [{"market": "ASX", "portfolio_id": "ASX:asx_top_quality"}],
        submit_plan=submit_plan,
        watchlist_expansion_summary=expansion_summary,
    )

    assert plan["status"] == "candidate_supply_gap"
    assert plan["reason"] == "no_safe_submit_candidate_with_seed_proposals"
    assert plan["primary_action"] == "create_or_refresh_preferred_asset_seed_watchlist"
    assert plan["seed_proposal_count"] == 1
    assert plan["manual_seed_proposal_count"] == 1
    assert plan["seed_proposal_markets"] == ["ASX"]
    assert plan["seed_intake_plan_count"] == 1
    assert plan["seed_source_candidate_count"] == 2
    assert plan["seed_source_markets"] == ["ASX"]
    assert plan["seed_intake_external_source_count"] == 0
    assert plan["does_not_change_submit_decision"] is True
    assert plan["submit_gate_policy"] == "do_not_relax_submit_gates"
    assert plan["next_actions"][0]["near_miss_symbols"] == ["BHP.AX", "RIO.AX"]


def test_auto_order_recovery_plan_targets_one_quality_frontier_after_budget_recovery() -> None:
    rows = [
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "hard_blocks": ["preflight_stale", "gateway_budget_degraded"],
            "gateway_budget_projected_recovery_at": "2026-06-12T23:59:59+00:00",
        },
        {
            "market": "HK",
            "portfolio_id": "HK:bluechip",
            "hard_blocks": ["ibkr_gateway_unavailable", "gateway_budget_degraded"],
            "gateway_budget_projected_recovery_at": "2026-06-13T23:59:59+00:00",
        },
    ]
    submit_plan = {
        "status": "BLOCKED",
        "ready": False,
        "frontier_candidates": [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "planned_order_symbols": "SPLG",
                "submit_quality_status": "PASS",
                "submit_quality_min_net_edge_bps": 10.8,
                "submit_quality_min_edge_margin_bps": 4.8,
                "hard_blocks": ["preflight_stale", "gateway_budget_degraded"],
            },
            {
                "market": "HK",
                "portfolio_id": "HK:bluechip",
                "planned_order_symbols": "2800.HK",
                "submit_quality_status": "NO_ORDERS",
            },
        ],
    }

    plan = build_auto_order_recovery_plan(rows, submit_plan=submit_plan)

    assert plan["status"] == "wait_gateway_budget"
    assert plan["target_market"] == "US"
    assert plan["target_portfolio_id"] == "US:watchlist"
    assert plan["target_symbols"] == "SPLG"
    assert plan["gateway_budget_projected_recovery_at"] == "2026-06-12T23:59:59+00:00"
    assert plan["gateway_refresh_portfolio_limit"] == 1
    assert plan["estimated_gateway_refresh_count"] == 1
    assert plan["request_policy"] == "single_highest_quality_frontier_only"
    assert [step["action"] for step in plan["steps"]] == [
        "refresh_supervisor_preflight",
        "hold_high_request_scans_until_gateway_budget_recovers",
        "refresh_frontier_report_and_execution_no_submit",
        "rebuild_market_readiness_auto_order_readiness_and_dashboard",
    ]
    assert plan["steps"][2]["portfolio_id"] == "US:watchlist"
    assert plan["steps"][2]["requires_ibkr_gateway"] is True
    assert all(step["submit_orders"] is False for step in plan["steps"])
    assert plan["does_not_submit_orders"] is True
    assert plan["does_not_relax_submit_gates"] is True


def test_auto_order_recovery_plan_does_not_refresh_when_submit_plan_is_ready() -> None:
    plan = build_auto_order_recovery_plan(
        [{"market": "US", "portfolio_id": "US:watchlist", "ready": True}],
        submit_plan={
            "status": "READY_SINGLE_CANDIDATE",
            "ready": True,
            "selected_market": "US",
            "selected_portfolio_id": "US:watchlist",
            "selected_planned_order_symbols": "SPLG",
            "frontier_candidates": [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "submit_quality_status": "PASS",
                }
            ],
        },
    )

    assert plan["status"] == "submit_review_ready"
    assert plan["request_policy"] == "no_refresh_when_submit_plan_is_ready"
    assert plan["estimated_gateway_refresh_count"] == 0
    assert [step["action"] for step in plan["steps"]] == ["operator_review_selected_paper_plan"]
    assert plan["steps"][0]["submit_orders"] is False


def test_auto_order_recovery_plan_targets_stale_execution_refresh_when_no_quality_frontier() -> None:
    stale_plan = {
        "status": "READY_FOR_TARGETED_NO_SUBMIT_REFRESH",
        "primary_market": "HK",
        "primary_portfolio_id": "HK:bluechip",
        "request_policy": "one_stale_execution_portfolio_after_gateway_budget_ok",
        "paper_only": True,
        "submit_orders": False,
        "does_not_relax_submit_gates": True,
        "rows": [
            {
                "market": "HK",
                "portfolio_id": "HK:bluechip",
                "ranking_bucket": "growth_refresh_candidate",
                "gateway_budget_blocked": False,
                "planned_order_symbols": "3988.HK,2388.HK",
                "submit_quality_status": "NO_ORDERS",
            }
        ],
    }

    plan = build_auto_order_recovery_plan(
        [],
        submit_plan={
            "ready": False,
            "status": "BLOCKED",
            "frontier_candidates": [],
        },
        stale_execution_refresh_plan=stale_plan,
    )

    assert plan["status"] == "stale_execution_refresh_required"
    assert plan["primary_action"] == "refresh_stale_execution_target_no_submit"
    assert plan["target_market"] == "HK"
    assert plan["target_portfolio_id"] == "HK:bluechip"
    assert plan["target_submit_quality_status"] == "NO_ORDERS"
    assert plan["target_ranking_bucket"] == "growth_refresh_candidate"
    assert plan["gateway_refresh_portfolio_limit"] == 1
    assert plan["estimated_gateway_refresh_count"] == 1
    assert plan["request_policy"] == "one_stale_execution_portfolio_after_gateway_budget_ok"
    assert plan["does_not_submit_orders"] is True
    assert plan["does_not_relax_submit_gates"] is True
    assert [step["action"] for step in plan["steps"]] == [
        "refresh_stale_execution_report",
        "refresh_stale_execution_no_submit",
        "rebuild_market_readiness_auto_order_readiness_and_dashboard",
    ]
    assert all(step["submit_orders"] is False for step in plan["steps"])


def test_auto_order_recovery_eligibility_waits_for_fresh_budget_evidence() -> None:
    plan = {
        "status": "wait_gateway_budget",
        "target_market": "US",
        "target_portfolio_id": "US:watchlist",
        "target_submit_quality_status": "PASS",
        "gateway_budget_projected_recovery_at": "2026-06-12T23:59:59+00:00",
        "gateway_refresh_portfolio_limit": 1,
        "estimated_gateway_refresh_count": 1,
        "paper_only": True,
        "does_not_submit_orders": True,
        "does_not_relax_submit_gates": True,
    }

    before = evaluate_auto_order_recovery_eligibility(
        plan,
        now=datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc),
    )
    after = evaluate_auto_order_recovery_eligibility(
        plan,
        now=datetime(2026, 6, 13, 1, 0, tzinfo=timezone.utc),
    )

    assert before["active"] is True
    assert before["eligible"] is False
    assert before["reason"] == "gateway_budget_recovery_not_reached"
    assert after["active"] is True
    assert after["eligible"] is False
    assert after["reason"] == "gateway_budget_evidence_refresh_required"


def test_auto_order_recovery_eligibility_allows_only_targeted_no_submit_refresh() -> None:
    result = evaluate_auto_order_recovery_eligibility(
        {
            "status": "targeted_frontier_refresh_required",
            "target_market": "US",
            "target_portfolio_id": "US:watchlist",
            "target_symbols": "SPLG",
            "target_submit_quality_status": "PASS",
            "gateway_refresh_portfolio_limit": 1,
            "estimated_gateway_refresh_count": 1,
            "request_policy": "single_highest_quality_frontier_only",
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        },
        now=datetime(2026, 6, 13, 1, 0, tzinfo=timezone.utc),
    )

    assert result["active"] is True
    assert result["eligible"] is True
    assert result["reason"] == "eligible_targeted_no_submit_refresh"
    assert result["allowed_actions"] == [
        "generate_investment_report",
        "run_investment_execution_no_submit",
    ]
    assert result["submit_orders"] is False


def test_auto_order_recovery_eligibility_allows_stale_no_submit_refresh_without_pass_quality() -> None:
    result = evaluate_auto_order_recovery_eligibility(
        {
            "status": "stale_execution_refresh_required",
            "target_market": "HK",
            "target_portfolio_id": "HK:bluechip",
            "target_symbols": "3988.HK",
            "target_submit_quality_status": "NO_ORDERS",
            "gateway_refresh_portfolio_limit": 1,
            "estimated_gateway_refresh_count": 1,
            "request_policy": "one_stale_execution_portfolio_after_gateway_budget_ok",
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        },
        now=datetime(2026, 6, 13, 1, 0, tzinfo=timezone.utc),
    )

    assert result["active"] is True
    assert result["eligible"] is True
    assert result["reason"] == "eligible_stale_execution_no_submit_refresh"
    assert result["allowed_actions"] == [
        "generate_investment_report",
        "run_investment_execution_no_submit",
    ]
    assert result["submit_orders"] is False


def test_stale_frontier_uses_non_blocking_evidence_maintenance() -> None:
    rows = [
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "hard_blocks": ["market_readiness_not_ready"],
        }
    ]
    submit_plan = {
        "status": "BLOCKED",
        "ready": False,
        "frontier_candidates": [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "planned_order_symbols": "SPLG",
                "submit_quality_status": "PASS",
                "hard_blocks": ["market_readiness_not_ready"],
            }
        ],
    }

    plan = build_auto_order_recovery_plan(rows, submit_plan=submit_plan)
    eligibility = evaluate_auto_order_recovery_eligibility(plan)

    assert plan["status"] == "evidence_maintenance_required"
    assert plan["primary_action"] == "refresh_frontier_evidence_no_submit"
    assert plan["steps"][0]["phase"] == "evidence_maintenance"
    assert eligibility["active"] is False
    assert eligibility["eligible"] is False
    assert eligibility["maintenance_active"] is True
    assert eligibility["reason"] == "evidence_maintenance_scheduled"


def test_unrelated_gateway_block_does_not_create_recovery_without_quality_target() -> None:
    plan = build_auto_order_recovery_plan(
        [
            {
                "market": "ASX",
                "portfolio_id": "ASX:watchlist",
                "hard_blocks": ["ibkr_gateway_unavailable"],
            }
        ],
        submit_plan={
            "status": "BLOCKED",
            "ready": False,
            "frontier_candidates": [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "submit_quality_status": "BLOCKED",
                    "hard_blocks": ["market_readiness_not_ready"],
                }
            ],
        },
    )

    assert plan["status"] == "manual_review_required"
    assert plan["target_portfolio_id"] == ""
    assert plan["primary_action"] == "review_submit_frontier_and_candidate_evidence"


def test_auto_order_recovery_eligibility_rejects_unsafe_contract() -> None:
    result = evaluate_auto_order_recovery_eligibility(
        {
            "status": "targeted_frontier_refresh_required",
            "target_market": "US",
            "target_portfolio_id": "US:watchlist",
            "target_submit_quality_status": "PASS",
            "gateway_refresh_portfolio_limit": 2,
            "estimated_gateway_refresh_count": 2,
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        }
    )

    assert result["active"] is True
    assert result["eligible"] is False
    assert result["reason"] == "unsafe_recovery_contract"


def test_auto_order_readiness_summary_includes_frequency_plan_from_watchlist_expansion() -> None:
    summary = build_auto_order_readiness_summary(
        [
            {
                "status": BLOCKED_STATUS,
                "ready": False,
                "primary_reason": "market_readiness_not_ready",
                "hard_blocks": ["market_readiness_not_ready"],
                "market": "ASX",
                "portfolio_id": "ASX:asx_top_quality",
            }
        ],
        policy={"enabled": True},
        watchlist_expansion_summary={
            "seed_proposals": [
                {
                    "market": "ASX",
                    "proposal_action": "create_or_refresh_preferred_asset_seed_watchlist",
                    "expansion_target": "seed_preferred_asset_class_candidates",
                    "auto_apply": False,
                }
            ]
        },
    )

    assert summary["frequency_plan"]["status"] == "frontier_blocked"
    assert summary["frequency_plan"]["reason"] == "market_readiness_not_ready"
    assert summary["frequency_plan"]["seed_proposal_count"] == 1
    assert summary["candidate_supply_status"] == "frontier_blocked"
    assert summary["candidate_supply_primary_action"] == "resolve_submit_frontier_blocker"
    assert summary["recovery_plan"]["status"] == "manual_review_required"
    assert summary["recovery_plan"]["does_not_submit_orders"] is True
