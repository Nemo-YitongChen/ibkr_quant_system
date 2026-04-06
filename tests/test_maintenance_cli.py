from __future__ import annotations

from src.tools import generate_trade_report
from src.tools import label_investment_snapshots
from src.tools import review_investment_execution
from src.tools import sync_short_safety_from_ibkr


def test_maintenance_cli_help_includes_examples_and_repo_path_note() -> None:
    cases = [
        (review_investment_execution.build_parser, ["ibkr-quant-execution-review", "reports_investment_execution", "Examples:"]),
        (label_investment_snapshots.build_parser, ["ibkr-quant-label-snapshots", "reports_investment_labels", "Examples:"]),
        (generate_trade_report.build_parser, ["ibkr-quant-trade-report", "SPY,TSLA,AAPL,MSFT,NVDA", "Examples:"]),
        (sync_short_safety_from_ibkr.build_parser, ["ibkr-quant-short-safety-sync", "--market_data_type", "Examples:"]),
    ]

    for build_parser, expected_snippets in cases:
        help_text = build_parser().format_help()
        assert "Relative paths resolve from the repository root by default." in help_text
        for snippet in expected_snippets:
            assert snippet in help_text


def test_maintenance_cli_parse_args_accepts_explicit_argv() -> None:
    execution_args = review_investment_execution.parse_args(["--market", "HK", "--days", "7"])
    assert execution_args.market == "HK"
    assert execution_args.days == 7

    labeling_args = label_investment_snapshots.parse_args(["--market", "US", "--stage", "deep", "--limit", "25"])
    assert labeling_args.market == "US"
    assert labeling_args.stage == "deep"
    assert labeling_args.limit == 25

    report_args = generate_trade_report.parse_args(["--market", "US", "--top_n", "12", "--use_scanner"])
    assert report_args.market == "US"
    assert report_args.top_n == 12
    assert report_args.use_scanner is True

    short_args = sync_short_safety_from_ibkr.parse_args(["--market", "HK", "--max_symbols", "20", "--no_delayed_fallback"])
    assert short_args.market == "HK"
    assert short_args.max_symbols == 20
    assert short_args.no_delayed_fallback is True
