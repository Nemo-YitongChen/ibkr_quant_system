from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.tools import reconcile_investment_broker
from src.tools import review_investment_weekly
from src.tools import run_investment_execution
from src.tools import run_investment_guard
from src.tools import run_investment_opportunity
from src.tools import run_investment_paper
from src.tools import sync_investment_paper_from_broker

pytestmark = pytest.mark.guardrail


def test_remaining_cli_help_includes_commands_and_examples() -> None:
    cases = [
        (run_investment_paper.build_parser, ["ibkr-quant-paper", "investment_paper_report.md", "Examples:"]),
        (run_investment_execution.build_parser, ["ibkr-quant-execution", "investment_execution_report.md", "Examples:"]),
        (run_investment_guard.build_parser, ["ibkr-quant-guard", "investment_guard_report.md", "Examples:"]),
        (run_investment_opportunity.build_parser, ["ibkr-quant-opportunity", "investment_opportunity_report.md", "Examples:"]),
        (review_investment_weekly.build_parser, ["ibkr-quant-weekly-review", "weekly_review.md", "Examples:"]),
        (reconcile_investment_broker.build_parser, ["ibkr-quant-reconcile", "broker_reconciliation.md", "Examples:"]),
        (sync_investment_paper_from_broker.build_parser, ["ibkr-quant-sync-paper", "broker_sync_report.md", "Examples:"]),
    ]
    for build_parser, snippets in cases:
        help_text = build_parser().format_help()
        assert "Relative paths resolve from the repository root by default." in help_text
        for snippet in snippets:
            assert snippet in help_text


def test_remaining_cli_parse_args_accepts_explicit_argv() -> None:
    assert run_investment_paper.parse_args(["--market", "HK", "--force"]).force is True
    execution_args = run_investment_execution.parse_args(["--market", "HK", "--submit", "--account_profile_config", "config/account_profiles.yaml"])
    assert execution_args.submit is True
    assert execution_args.account_profile_config == "config/account_profiles.yaml"
    guard_args = run_investment_guard.parse_args(
        [
            "--market",
            "US",
            "--submit",
            "--market_structure_config",
            "config/market_structure_us.yaml",
            "--adaptive_strategy_config",
            "config/adaptive_strategy_framework.yaml",
        ]
    )
    assert guard_args.submit is True
    assert guard_args.market_structure_config == "config/market_structure_us.yaml"
    assert guard_args.adaptive_strategy_config == "config/adaptive_strategy_framework.yaml"
    opportunity_args = run_investment_opportunity.parse_args(
        [
            "--market",
            "US",
            "--request_timeout_sec",
            "5",
            "--market_structure_config",
            "config/market_structure_us.yaml",
            "--adaptive_strategy_config",
            "config/adaptive_strategy_framework.yaml",
        ]
    )
    assert opportunity_args.request_timeout_sec == 5.0
    assert opportunity_args.market_structure_config == "config/market_structure_us.yaml"
    assert opportunity_args.adaptive_strategy_config == "config/adaptive_strategy_framework.yaml"
    assert review_investment_weekly.parse_args(["--market", "HK", "--days", "14"]).days == 14
    assert reconcile_investment_broker.parse_args(["--market", "HK", "--portfolio_id", "HK:watchlist"]).portfolio_id == "HK:watchlist"
    assert sync_investment_paper_from_broker.parse_args(["--market", "US", "--portfolio_id", "US:market_us"]).portfolio_id == "US:market_us"


def test_remaining_cli_summary_payloads() -> None:
    paper_summary, paper_artifacts = run_investment_paper._cli_summary_payload(
        {"market": "HK", "portfolio_id": "HK:watchlist", "rebalance_due": True, "executed": False},
        Path("reports/hk"),
        trade_count=3,
        position_count=12,
    )
    assert paper_summary["trade_count"] == 3
    assert paper_artifacts["report_md"] == Path("reports/hk/investment_paper_report.md")

    execution_summary, execution_artifacts = run_investment_execution._cli_summary_payload(
        SimpleNamespace(
            market="HK",
            portfolio_id="HK:watchlist",
            submitted=True,
            account_profile_label="小资金",
            order_count=5,
            gap_symbols=2,
            gap_notional=123.45,
        ),
        Path("reports/hk"),
    )
    assert execution_summary["submitted"] is True
    assert execution_summary["account_profile"] == "小资金"
    assert execution_summary["gap_notional"] == "123.45"
    assert execution_artifacts["plan_csv"] == Path("reports/hk/investment_execution_plan.csv")

    guard_summary, guard_artifacts = run_investment_guard._cli_summary_payload(
        SimpleNamespace(
            market="US",
            portfolio_id="US:market_us",
            submitted=False,
            order_count=4,
            stop_count=3,
            take_profit_count=1,
            market_rules="settlement=T+1 | buy_lot=1",
        ),
        Path("reports/us"),
    )
    assert guard_summary["stop_count"] == 3
    assert guard_summary["market_rules"] == "settlement=T+1 | buy_lot=1"
    assert guard_artifacts["summary_json"] == Path("reports/us/investment_guard_summary.json")

    opp_summary, opp_artifacts = run_investment_opportunity._cli_summary_payload(
        SimpleNamespace(
            market="US",
            portfolio_id="US:market_us",
            entry_now_count=2,
            near_entry_count=5,
            wait_count=8,
            market_structure_wait_count=2,
            adaptive_strategy_wait_count=1,
            market_rules="settlement=T+1 | buy_lot=1",
        ),
        Path("reports/us"),
    )
    assert opp_summary["near_entry_count"] == 5
    assert opp_summary["market_structure_wait_count"] == 2
    assert opp_summary["adaptive_strategy_wait_count"] == 1
    assert opp_summary["market_rules"] == "settlement=T+1 | buy_lot=1"
    assert opp_artifacts["report_md"] == Path("reports/us/investment_opportunity_report.md")

    weekly_summary, weekly_artifacts = review_investment_weekly._cli_summary_payload(
        {
            "market_filter": "HK",
            "portfolio_filter": "HK:watchlist",
            "portfolio_count": 2,
            "trade_count": 10,
            "execution_run_count": 3,
            "best_portfolio": "HK:watchlist",
            "worst_portfolio": "HK:legacy",
        },
        Path("reports/weekly"),
    )
    assert weekly_summary["portfolio_count"] == 2
    assert weekly_artifacts["report_md"] == Path("reports/weekly/weekly_review.md")

    reconcile_summary, reconcile_artifacts = reconcile_investment_broker._cli_summary_payload(
        {
            "market": "HK",
            "portfolio_id": "HK:watchlist",
            "match_rows": 8,
            "only_local_rows": 1,
            "only_broker_rows": 0,
            "qty_mismatch_rows": 2,
        },
        Path("reports/reconcile"),
    )
    assert reconcile_summary["qty_mismatch_rows"] == 2
    assert reconcile_artifacts["summary_json"] == Path("reports/reconcile/broker_reconciliation_summary.json")

    sync_summary, sync_artifacts = sync_investment_paper_from_broker._cli_summary_payload(
        {
            "market": "US",
            "portfolio_id": "US:market_us",
            "account_id": "DU123",
            "position_count": 9,
            "equity_after": 100000.0,
        },
        Path("reports/sync"),
    )
    assert sync_summary["equity_after"] == "100000.00"
    assert sync_artifacts["positions_csv"] == Path("reports/sync/broker_sync_positions.csv")
