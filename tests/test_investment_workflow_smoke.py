from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from src.tools import generate_dashboard
from src.tools import reconcile_investment_broker
from src.tools import review_investment_weekly
from src.tools import run_investment_execution
from src.tools import run_investment_paper

pytestmark = [pytest.mark.guardrail, pytest.mark.integration]


class _DummyEvent:
    def __iadd__(self, _handler):
        return self


class _SummaryRow:
    def __init__(self, account: str, tag: str, value: str):
        self.account = account
        self.tag = tag
        self.value = value


class _FakeIB:
    orderStatusEvent = _DummyEvent()
    errorEvent = _DummyEvent()
    execDetailsEvent = _DummyEvent()
    commissionReportEvent = _DummyEvent()

    def accountSummary(self, *_args, **_kwargs):
        return [
            _SummaryRow("DUQ152001", "NetLiquidation", "100000"),
            _SummaryRow("DUQ152001", "TotalCashValue", "100000"),
            _SummaryRow("DUQ152001", "BuyingPower", "200000"),
        ]

    def portfolio(self, *_args, **_kwargs):
        return []

    def positions(self, *_args, **_kwargs):
        return []

    def sleep(self, *_args, **_kwargs):
        return None

    def disconnect(self):
        return None


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_minimal_report_fixture(report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "investment_candidates.csv").write_text(
        "\n".join(
            [
                "symbol,last_close,score,model_recommendation_score,execution_score,execution_ready,direction,market,asset_class,asset_theme,sector,industry",
                "AAPL,100,0.82,0.82,0.35,1,LONG,US,equity,core,Technology,Software",
            ]
        ),
        encoding="utf-8",
    )
    (report_dir / "investment_plan.csv").write_text(
        "\n".join(
            [
                "symbol,action,allocation_mult,direction,execution_ready",
                "AAPL,ACCUMULATE,1.0,LONG,1",
            ]
        ),
        encoding="utf-8",
    )


def test_investment_workflow_cli_smoke_generates_contract_artifacts(tmp_path, monkeypatch, capsys) -> None:
    runtime_dir = tmp_path / "runtime"
    report_root = runtime_dir / "reports_investment"
    report_dir = report_root / "market_us"
    weekly_dir = runtime_dir / "reports_investment_weekly"
    reconcile_dir = runtime_dir / "reports_investment_reconcile"
    dashboard_dir = runtime_dir / "reports_supervisor"
    preflight_dir = runtime_dir / "reports_preflight"
    config_dir = runtime_dir / "config"
    db_path = runtime_dir / "audit.db"
    portfolio_id = "US:market_us"

    _write_minimal_report_fixture(report_dir)
    preflight_dir.mkdir(parents=True, exist_ok=True)

    paper_cfg_path = config_dir / "investment_paper_us.yaml"
    execution_cfg_path = config_dir / "investment_execution_us.yaml"
    market_structure_cfg_path = config_dir / "market_structure_us.yaml"
    account_profiles_cfg_path = config_dir / "account_profiles.yaml"
    adaptive_strategy_cfg_path = config_dir / "adaptive_strategy.yaml"
    ibkr_cfg_path = config_dir / "ibkr_us.yaml"
    supervisor_cfg_path = config_dir / "supervisor.yaml"

    config_dir.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        paper_cfg_path,
        {
            "paper": {
                "initial_cash": 100000.0,
                "max_holdings": 1,
                "max_single_weight": 0.60,
                "min_position_weight": 0.05,
            }
        },
    )
    _write_yaml(
        execution_cfg_path,
        {
            "execution": {
                "min_cash_buffer_pct": 0.0,
                "cash_buffer_floor": 0.0,
                "min_trade_value": 100.0,
                "max_order_value_pct": 0.50,
                "max_orders_per_run": 2,
                "account_allocation_pct": 0.50,
                "manual_review_enabled": False,
                "shadow_ml_review_enabled": False,
                "risk_alert_guard_enabled": False,
                "wait_fill_sec": 0.0,
                "poll_interval_sec": 0.0,
            }
        },
    )
    _write_yaml(
        market_structure_cfg_path,
        {
            "market_structure": {
                "market": "US",
                "benchmark_symbol": "SPY",
                "account_rules": {"standard_settlement_cycle": "T+1"},
                "order_rules": {"buy_lot_multiple": 1, "day_turnaround_allowed": True},
            }
        },
    )
    _write_yaml(account_profiles_cfg_path, {"account_profiles": {"profiles": []}})
    _write_yaml(adaptive_strategy_cfg_path, {"adaptive_strategy": {}})
    _write_yaml(
        ibkr_cfg_path,
        {
            "host": "127.0.0.1",
            "port": 4001,
            "client_id": 1,
            "account_id": "DUQ152001",
            "mode": "paper",
            "execution_mode": "investment",
            "investment_paper_config": str(paper_cfg_path),
            "investment_execution_config": str(execution_cfg_path),
            "market_structure_config": str(market_structure_cfg_path),
            "account_profile_config": str(account_profiles_cfg_path),
            "adaptive_strategy_config": str(adaptive_strategy_cfg_path),
        },
    )
    _write_yaml(
        supervisor_cfg_path,
        {
            "timezone": "Australia/Sydney",
            "summary_out_dir": str(dashboard_dir),
            "dashboard_db": str(db_path),
            "dashboard_weekly_review_dir": str(weekly_dir),
            "dashboard_preflight_dir": str(preflight_dir),
            "markets": [
                {
                    "name": "us",
                    "market": "US",
                    "enabled": True,
                    "reports": [
                         {
                             "kind": "investment",
                             "out_dir": str(report_root),
                             "ibkr_config": str(ibkr_cfg_path),
                             "portfolio_id": portfolio_id,
                             "run_investment_paper": True,
                             "run_investment_execution": True,
                             "submit_investment_execution": False,
                         }
                    ],
                }
            ],
        },
    )

    monkeypatch.setattr(run_investment_execution, "connect_ib", lambda *args, **kwargs: _FakeIB())

    run_investment_paper.main(
        [
            "--market",
            "US",
            "--db",
            str(db_path),
            "--report_dir",
            str(report_dir),
            "--paper_config",
            str(paper_cfg_path),
            "--portfolio_id",
            portfolio_id,
            "--force",
        ]
    )
    paper_stdout = capsys.readouterr().out
    assert "ibkr-quant-paper: investment paper run complete" in paper_stdout

    run_investment_execution.main(
        [
            "--market",
            "US",
            "--db",
            str(db_path),
            "--report_dir",
            str(report_dir),
            "--ibkr_config",
            str(ibkr_cfg_path),
            "--portfolio_id",
            portfolio_id,
        ]
    )
    execution_stdout = capsys.readouterr().out
    assert "ibkr-quant-execution: investment execution run complete" in execution_stdout

    review_investment_weekly.main(
        [
            "--market",
            "US",
            "--db",
            str(db_path),
            "--out_dir",
            str(weekly_dir),
            "--portfolio_id",
            portfolio_id,
            "--days",
            "30",
            "--preflight_dir",
            str(preflight_dir),
        ]
    )
    weekly_stdout = capsys.readouterr().out
    assert "ibkr-quant-weekly-review: weekly investment review complete" in weekly_stdout

    reconcile_investment_broker.main(
        [
            "--market",
            "US",
            "--db",
            str(db_path),
            "--portfolio_id",
            portfolio_id,
            "--out_dir",
            str(reconcile_dir),
        ]
    )
    reconcile_stdout = capsys.readouterr().out
    assert "ibkr-quant-reconcile: broker reconciliation complete" in reconcile_stdout

    generate_dashboard.main(
        [
            "--config",
            str(supervisor_cfg_path),
            "--out_dir",
            str(dashboard_dir),
        ]
    )
    dashboard_stdout = capsys.readouterr().out
    assert "ibkr-quant-dashboard: dashboard build complete" in dashboard_stdout

    paper_summary_path = report_dir / "investment_paper_summary.json"
    execution_summary_path = report_dir / "investment_execution_summary.json"
    weekly_summary_path = weekly_dir / "weekly_review_summary.json"
    reconcile_summary_path = reconcile_dir / "broker_reconciliation_summary.json"
    dashboard_json_path = dashboard_dir / "dashboard.json"

    for path in (
        paper_summary_path,
        report_dir / "investment_portfolio.csv",
        report_dir / "investment_rebalance_trades.csv",
        report_dir / "investment_paper_report.md",
        execution_summary_path,
        report_dir / "investment_execution_plan.csv",
        report_dir / "investment_execution_report.md",
        weekly_summary_path,
        weekly_dir / "weekly_execution_summary.csv",
        weekly_dir / "weekly_review.md",
        reconcile_summary_path,
        reconcile_dir / "broker_reconciliation.csv",
        reconcile_dir / "broker_reconciliation.md",
        dashboard_json_path,
        dashboard_dir / "dashboard.html",
    ):
        assert path.exists(), f"expected artifact missing: {path}"

    paper_summary = json.loads(paper_summary_path.read_text(encoding="utf-8"))
    execution_summary = json.loads(execution_summary_path.read_text(encoding="utf-8"))
    weekly_summary = json.loads(weekly_summary_path.read_text(encoding="utf-8"))
    reconcile_summary = json.loads(reconcile_summary_path.read_text(encoding="utf-8"))
    dashboard_payload = json.loads(dashboard_json_path.read_text(encoding="utf-8"))

    assert paper_summary["market"] == "US"
    assert paper_summary["portfolio_id"] == portfolio_id
    assert "equity_after" in paper_summary
    assert "target_invested_weight" in paper_summary

    assert execution_summary["market"] == "US"
    assert execution_summary["portfolio_id"] == portfolio_id
    assert "order_count" in execution_summary
    assert "gap_symbols" in execution_summary
    assert "gap_notional" in execution_summary

    assert weekly_summary["market_filter"] == "US"
    assert weekly_summary["portfolio_filter"] == portfolio_id
    assert weekly_summary["portfolio_count"] == 1
    assert weekly_summary["execution_run_count"] == 1

    assert reconcile_summary["market"] == "US"
    assert reconcile_summary["portfolio_id"] == portfolio_id
    assert "only_local_rows" in reconcile_summary
    assert "qty_mismatch_rows" in reconcile_summary

    assert len(dashboard_payload["cards"]) == 1
    assert dashboard_payload["cards"][0]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["paper_summary"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["execution_summary"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["execution_weekly_row"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["execution_weekly"]["portfolio_id"] == portfolio_id
