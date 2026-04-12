from __future__ import annotations

from pathlib import Path

import pytest

from src.common.cli import emit_cli_summary
from src.tools import generate_trade_report
from src.tools import label_investment_snapshots
from src.tools import review_investment_execution
from src.tools import sync_short_safety_from_ibkr

pytestmark = pytest.mark.guardrail


def test_emit_cli_summary_formats_sections(capsys) -> None:
    emit_cli_summary(
        command="ibkr-quant-demo",
        headline="demo complete",
        summary={"market": "HK", "scanner_enabled": True},
        artifacts={"report_md": Path("reports/demo/report.md")},
    )

    out = capsys.readouterr().out
    assert "ibkr-quant-demo: demo complete" in out
    assert "summary:" in out
    assert "  market: HK" in out
    assert "  scanner_enabled: true" in out
    assert "artifacts:" in out
    assert "  report_md: reports/demo/report.md" in out


def test_execution_review_cli_summary_payload() -> None:
    summary, artifacts = review_investment_execution._cli_summary_payload(
        {
            "summary": {
                "market": "HK",
                "portfolio_id": "HK:watchlist",
                "execution_run_rows": 4,
                "planned_order_rows": 9,
                "fill_rows": 7,
                "realized_net_pnl": 12.34,
            }
        },
        Path("reports/execution"),
    )

    assert summary["market"] == "HK"
    assert summary["portfolio_id"] == "HK:watchlist"
    assert summary["execution_runs"] == 4
    assert summary["realized_net_pnl"] == "12.34"
    assert artifacts["markdown"] == Path("reports/execution/investment_execution_kpi.md")


def test_label_snapshot_cli_summary_payload() -> None:
    summary, artifacts = label_investment_snapshots._cli_summary_payload(
        {
            "market": "US",
            "portfolio_id": "US:watchlist",
            "stage": "final",
            "labeled_rows": 11,
            "skipped_rows": 3,
            "horizons": [5, 20, 60],
        },
        Path("reports/labels/us"),
    )

    assert summary["market"] == "US"
    assert summary["horizons"] == "5,20,60"
    assert artifacts["summary_json"] == Path("reports/labels/us/investment_candidate_outcomes_summary.json")


def test_trade_report_cli_summary_payload() -> None:
    summary, artifacts = generate_trade_report._cli_summary_payload(
        market="US",
        out_dir=Path("reports/us"),
        candidate_count=120,
        ranked_count=10,
        plan_count=8,
        scanner_enabled=False,
        watchlist_name="resolved_us.yaml",
    )

    assert summary["market"] == "US"
    assert summary["candidate_count"] == 120
    assert summary["scanner_enabled"] is False
    assert summary["watchlist"] == "resolved_us.yaml"
    assert artifacts["report_md"] == Path("reports/us/report.md")


def test_short_safety_cli_summary_payload() -> None:
    summary, artifacts = sync_short_safety_from_ibkr._cli_summary_payload(
        market="HK",
        symbol_count=20,
        borrow_rows=20,
        safety_rows=20,
        delayed_fallback_enabled=True,
        borrow_out=Path("config/reference/short_borrow_fee.csv"),
        safety_out=Path("config/reference/short_safety_rules.csv"),
    )

    assert summary["market"] == "HK"
    assert summary["symbol_count"] == 20
    assert summary["delayed_fallback_enabled"] is True
    assert artifacts["borrow_csv"] == Path("config/reference/short_borrow_fee.csv")
    assert artifacts["short_safety_csv"] == Path("config/reference/short_safety_rules.csv")
