from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from src.common.auto_order_readiness import (
    BLOCKED_STATUS,
    DISABLED_STATUS,
    READY_STATUS,
    WARNING_STATUS,
    build_auto_order_submit_plan,
    build_auto_order_readiness_summary,
    evaluate_auto_order_readiness,
)
from src.tools.review_auto_order_readiness import build_auto_order_readiness_payload


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


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
                    "symbol,status,expected_edge_bps,expected_cost_bps,edge_gate_threshold_bps,"
                    "whole_share_edge_margin_bps,dynamic_order_adv_pct,execution_order_type,"
                    "edge_gate_status,quality_status,market_rule_status,shadow_review_status,manual_review_status"
                ),
                "SPLG,PLANNED,34,22,28,6,0.0001,LMT,PASS,QUALITY_OK,RULES_OK,AUTO_OK,AUTO_OK",
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
