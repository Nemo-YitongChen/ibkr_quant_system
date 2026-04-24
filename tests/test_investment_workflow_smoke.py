from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from src.common.storage import Storage
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
    (report_dir / "investment_adaptive_strategy_summary.json").write_text(
        json.dumps(
            {
                "adaptive_strategy": {
                    "name": "ACM-RS",
                    "display_name": "Adaptive Cross-Market Relative Strength",
                    "summary_text": "ACM-RS | RS=126/63/20 | rebalance=weekly | entry_delay=15-30m",
                },
                "summary": {
                    "enabled": True,
                    "defensive_cap_count": 1,
                    "defensive_regime_detected": True,
                    "active_regime_states": ["RISK_OFF"],
                    "top_defensive_symbols": ["AAPL"],
                },
                "active_market_plan": {
                    "profile_key": "US",
                    "profile_label": "US trend-first",
                    "summary_text": "staged=3x | no_trade_band=3.0%",
                },
                "active_market_regime": {
                    "profile_key": "US",
                    "profile_label": "US trend-first",
                    "summary_text": "vol=1.00%/1.80% | risk_on=0.50",
                },
                "active_market_execution": {
                    "profile_key": "US",
                    "profile_label": "US trend-first",
                    "summary_text": "min_edge=16.0bps | edge_buffer=5.0bps",
                    "overrides": {
                        "min_expected_edge_bps": 16.0,
                        "edge_cost_buffer_bps": 5.0,
                    },
                },
            },
            ensure_ascii=False,
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
                    "max_sector_weight": 0.60,
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
                "account_allocation_pct": 0.60,
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

    storage = Storage(str(db_path))
    storage.insert_investment_candidate_snapshot(
        {
            "snapshot_id": "SMOKE|final|AAPL",
            "ts": "2026-02-01T00:00:00+00:00",
            "market": "US",
            "portfolio_id": portfolio_id,
            "report_dir": str(report_dir),
            "analysis_run_id": "SMOKE",
            "stage": "final",
            "symbol": "AAPL",
            "action": "ACCUMULATE",
            "direction": "LONG",
            "score": 0.82,
            "execution_score": 0.35,
            "expected_cost_bps": 11.0,
            "score_before_cost": 0.87,
            "expected_edge_threshold": 0.20,
            "expected_edge_score": 0.24,
            "expected_edge_bps": 34.0,
            "details": {
                "stage_rank": 1,
            },
        }
    )
    storage.insert_investment_candidate_snapshot(
        {
            "snapshot_id": "SMOKE|deep|MSFT",
            "ts": "2026-02-01T00:00:00+00:00",
            "market": "US",
            "portfolio_id": portfolio_id,
            "report_dir": str(report_dir),
            "analysis_run_id": "SMOKE",
            "stage": "deep",
            "symbol": "MSFT",
            "action": "WATCH",
            "direction": "LONG",
            "score": 0.46,
            "execution_score": 0.22,
            "expected_cost_bps": 9.0,
            "score_before_cost": 0.41,
            "expected_edge_threshold": 0.18,
            "expected_edge_score": 0.08,
            "expected_edge_bps": 12.0,
            "details": {
                "stage_rank": 1,
            },
        }
    )
    for horizon_days, aapl_ret, msft_ret in (
        (5, 0.03, 0.01),
        (20, 0.07, 0.02),
        (60, 0.12, 0.04),
    ):
        storage.upsert_investment_candidate_outcome(
            {
                "snapshot_id": "SMOKE|final|AAPL",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "AAPL",
                "horizon_days": horizon_days,
                "snapshot_ts": "2026-02-01T00:00:00+00:00",
                "outcome_ts": "2026-03-01T00:00:00+00:00",
                "direction": "LONG",
                "start_close": 100.0,
                "end_close": 107.0,
                "future_return": aapl_ret,
                "max_drawdown": -0.02,
                "max_runup": aapl_ret,
                "outcome_label": "POSITIVE",
                "details": {"stage": "final", "action": "ACCUMULATE"},
            }
        )
        storage.upsert_investment_candidate_outcome(
            {
                "snapshot_id": "SMOKE|deep|MSFT",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "MSFT",
                "horizon_days": horizon_days,
                "snapshot_ts": "2026-02-01T00:00:00+00:00",
                "outcome_ts": "2026-03-01T00:00:00+00:00",
                "direction": "LONG",
                "start_close": 100.0,
                "end_close": 102.0,
                "future_return": msft_ret,
                "max_drawdown": -0.01,
                "max_runup": msft_ret,
                "outcome_label": "POSITIVE",
                "details": {"stage": "deep", "action": "WATCH"},
            }
        )

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
        weekly_dir / "weekly_outcome_spread_summary.csv",
        weekly_dir / "weekly_edge_realization_summary.csv",
        weekly_dir / "weekly_blocked_edge_attribution.csv",
        weekly_dir / "weekly_trading_quality_evidence.csv",
        weekly_dir / "weekly_tuning_dataset.csv",
        weekly_dir / "weekly_tuning_dataset.json",
        weekly_dir / "weekly_tuning_history_overview.csv",
        weekly_dir / "weekly_decision_evidence_history_overview.csv",
        weekly_dir / "weekly_edge_calibration_summary.csv",
        weekly_dir / "weekly_slicing_calibration_summary.csv",
        weekly_dir / "weekly_risk_calibration_summary.csv",
        weekly_dir / "weekly_calibration_patch_suggestions.csv",
        weekly_dir / "weekly_patch_governance_summary.csv",
        weekly_dir / "weekly_control_timeseries.csv",
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
    assert paper_summary["adaptive_strategy_name"] == "ACM-RS"
    assert paper_summary["adaptive_strategy_defensive_caps"] == 1
    assert paper_summary["adaptive_strategy_defensive_regime"] is True
    assert paper_summary["strategy_effective_controls_applied"] is True
    assert "当前使用 US trend-first 市场档案" in paper_summary["adaptive_strategy_active_market_note"]
    assert paper_summary["target_invested_weight"] == pytest.approx(0.30)
    assert paper_summary["strategy_effective_controls"]["base_target_invested_weight"] == pytest.approx(0.60)
    assert paper_summary["strategy_effective_controls"]["effective_target_invested_weight"] == pytest.approx(0.30)
    assert "risk_base_gross_exposure" in paper_summary
    assert paper_summary["risk_gross_exposure_tightening"] >= 0.0

    assert execution_summary["market"] == "US"
    assert execution_summary["portfolio_id"] == portfolio_id
    assert "order_count" in execution_summary
    assert "gap_symbols" in execution_summary
    assert "gap_notional" in execution_summary
    assert execution_summary["adaptive_strategy_name"] == "ACM-RS"
    assert execution_summary["adaptive_strategy_runtime_note"].startswith("enabled=true defensive_caps=1")
    assert execution_summary["strategy_effective_controls_applied"] is True
    assert "当前使用 US trend-first 市场档案" in execution_summary["adaptive_strategy_active_market_note"]
    assert execution_summary["strategy_effective_controls"]["base_effective_target_invested_weight"] == pytest.approx(0.36)
    assert execution_summary["strategy_effective_controls"]["effective_target_invested_weight"] == pytest.approx(0.30)
    assert execution_summary["strategy_effective_controls"]["effective_account_allocation_pct"] == pytest.approx(0.50)
    assert execution_summary["strategy_effective_controls"]["effective_max_order_value_pct"] == pytest.approx(0.4166666667)
    assert "risk_base_gross_exposure" in execution_summary
    assert execution_summary["risk_gross_exposure_tightening"] >= 0.0

    assert weekly_summary["market_filter"] == "US"
    assert weekly_summary["portfolio_filter"] == portfolio_id
    assert weekly_summary["portfolio_count"] == 1
    assert weekly_summary["execution_run_count"] == 1
    assert weekly_summary["weekly_tuning_dataset_summary"]["portfolio_count"] == 1
    assert weekly_summary["weekly_tuning_dataset"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["weekly_tuning_history_overview"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["decision_evidence_history_overview"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["trading_quality_evidence"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["edge_calibration_summary"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["slicing_calibration_summary"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["risk_calibration_summary"][0]["portfolio_id"] == portfolio_id
    assert "calibration_patch_suggestions" in weekly_summary
    assert isinstance(weekly_summary["calibration_patch_suggestions"], list)
    assert "patch_governance_summary" in weekly_summary
    assert isinstance(weekly_summary["patch_governance_summary"], list)
    assert weekly_summary["weekly_control_timeseries"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["outcome_spread_summary"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["edge_realization_summary"][0]["portfolio_id"] == portfolio_id
    assert weekly_summary["blocked_edge_attribution_summary"][0]["portfolio_id"] == portfolio_id
    assert "当前使用 US trend-first 市场档案" in weekly_summary["portfolio_strategy_context"][0]["adaptive_strategy_market_profile_note"]
    assert str(weekly_summary["portfolio_strategy_context"][0]["market_profile_tuning_note"])
    assert weekly_summary["portfolio_strategy_context"][0]["strategy_effective_controls_applied"] is True
    assert "策略主动转入防守" in weekly_summary["portfolio_strategy_context"][0]["strategy_effective_controls_note"]
    assert weekly_summary["attribution_summary"][0]["strategy_control_weight_delta"] == pytest.approx(0.06)
    assert execution_summary["blocked_edge_order_count"] == 1
    assert weekly_summary["attribution_summary"][0]["execution_gate_blocked_order_count"] == 1
    assert "策略" in weekly_summary["attribution_summary"][0]["control_split_text"]
    assert weekly_summary["weekly_tuning_dataset"][0]["blocked_edge_parent_count"] == 1

    assert reconcile_summary["market"] == "US"
    assert reconcile_summary["portfolio_id"] == portfolio_id
    assert "only_local_rows" in reconcile_summary
    assert "qty_mismatch_rows" in reconcile_summary
    assert reconcile_summary["adaptive_strategy_name"] == "ACM-RS"
    assert reconcile_summary["adaptive_strategy_top_defensive_symbols"] == ["AAPL"]
    assert "当前使用 US trend-first 市场档案" in reconcile_summary["adaptive_strategy_active_market_note"]
    assert reconcile_summary["strategy_effective_controls_applied"] is True
    assert "策略主动转入防守" in reconcile_summary["strategy_effective_controls_note"]
    assert reconcile_summary["execution_blocked_order_count"] == 1

    assert len(dashboard_payload["cards"]) == 1
    assert dashboard_payload["cards"][0]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["paper_summary"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["paper_summary"]["adaptive_strategy_name"] == "ACM-RS"
    assert dashboard_payload["cards"][0]["paper_summary"]["strategy_effective_controls_applied"] is True
    assert dashboard_payload["cards"][0]["execution_summary"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["cards"][0]["execution_summary"]["adaptive_strategy_defensive_caps"] == 1
    assert dashboard_payload["cards"][0]["execution_summary"]["strategy_effective_controls_applied"] is True
    assert dashboard_payload["cards"][0]["execution_summary"]["blocked_edge_order_count"] == 1
    assert "当前使用 US trend-first 市场档案" in dashboard_payload["cards"][0]["weekly_strategy_context"]["adaptive_strategy_market_profile_note"]
    assert str(dashboard_payload["cards"][0]["weekly_strategy_context"]["market_profile_tuning_note"])
    assert dashboard_payload["cards"][0]["execution_weekly_row"]["portfolio_id"] == portfolio_id
    assert dashboard_payload["execution_weekly"]["portfolio_id"] == portfolio_id
    assert "control_split_text" in dashboard_payload["cards"][0]["weekly_attribution"]
    assert "策略主动转入防守" in dashboard_payload["cards"][0]["weekly_strategy_context"]["strategy_effective_controls_note"]
