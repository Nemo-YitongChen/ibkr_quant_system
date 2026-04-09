from __future__ import annotations

from pathlib import Path

from src.tools import generate_dashboard
from src.tools import generate_investment_report
from src.tools import preflight_supervisor


def test_primary_cli_help_includes_examples_and_command_names() -> None:
    cases = [
        (preflight_supervisor.build_parser, ["ibkr-quant-preflight", "reports_preflight", "Examples:"]),
        (generate_dashboard.build_parser, ["ibkr-quant-dashboard", "reports_supervisor", "Examples:"]),
        (generate_investment_report.build_parser, ["ibkr-quant-report", "investment_report.md", "Examples:"]),
    ]

    for build_parser, snippets in cases:
        help_text = build_parser().format_help()
        assert "Relative paths resolve from the repository root by default." in help_text
        for snippet in snippets:
            assert snippet in help_text


def test_primary_cli_parse_args_accepts_explicit_argv() -> None:
    preflight_args = preflight_supervisor.parse_args(["--config", "config/supervisor_live.yaml", "--out_dir", "reports_preflight_live"])
    assert preflight_args.config == "config/supervisor_live.yaml"
    assert preflight_args.out_dir == "reports_preflight_live"

    dashboard_args = generate_dashboard.parse_args(["--config", "config/supervisor.yaml", "--out_dir", "reports_supervisor_test"])
    assert dashboard_args.config == "config/supervisor.yaml"
    assert dashboard_args.out_dir == "reports_supervisor_test"

    report_args = generate_investment_report.parse_args(["--market", "HK", "--top_n", "12", "--use_audit_recent"])
    assert report_args.market == "HK"
    assert report_args.top_n == 12
    assert report_args.use_audit_recent is True


def test_preflight_cli_summary_payload() -> None:
    summary, artifacts = preflight_supervisor._cli_summary_payload(
        {"pass_count": 6, "warn_count": 1, "fail_count": 0, "runtime_root": "runtime_data/paper_scope"},
        Path("reports_preflight"),
    )

    assert summary["pass_count"] == 6
    assert summary["runtime_root"] == "runtime_data/paper_scope"
    assert artifacts["summary_json"] == Path("reports_preflight/supervisor_preflight_summary.json")


def test_dashboard_cli_summary_payload() -> None:
    summary, artifacts = generate_dashboard._cli_summary_payload(
        {
            "cards": [{}, {}],
            "trade_cards": [{}],
            "dry_run_cards": [{}, {}],
            "ops_overview": {"preflight_warn_count": 2, "preflight_fail_count": 1},
        },
        Path("reports_supervisor"),
    )

    assert summary["market_cards"] == 2
    assert summary["trade_cards"] == 1
    assert summary["dry_run_cards"] == 2
    assert summary["preflight_fail_count"] == 1
    assert artifacts["dashboard_html"] == Path("reports_supervisor/dashboard.html")


def test_investment_report_cli_summary_payload() -> None:
    summary, artifacts = generate_investment_report._cli_summary_payload(
        market="HK",
        portfolio_id="HK:watchlist",
        out_dir=Path("reports_investment_hk/watchlist"),
        candidate_count=120,
        ranked_count=15,
        short_candidate_count=4,
        plan_count=12,
        backtest_count=10,
    )

    assert summary["market"] == "HK"
    assert summary["portfolio_id"] == "HK:watchlist"
    assert summary["short_candidate_count"] == 4
    assert artifacts["report_md"] == Path("reports_investment_hk/watchlist/investment_report.md")
