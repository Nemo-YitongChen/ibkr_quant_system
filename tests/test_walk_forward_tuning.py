from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from src.common.storage import Storage
from src.tools import review_market_walk_forward


def _seed_weekly_history(
    db_path: Path,
    *,
    market: str,
    profile: str,
    portfolio_id: str,
    weeks: int,
    weekly_return: float,
    max_drawdown: float,
    turnover: float,
    signal_quality_score: float,
    execution_gate_blocked_weight: float,
    avg_execution_capture_bps: float,
    avg_actual_slippage_bps: float,
    avg_expected_cost_bps: float,
    outcome_selected_spread_5d_bps: float,
    outcome_selected_spread_20d_bps: float,
    outcome_selected_spread_60d_bps: float,
    blocked_20d_avg_counterfactual_edge_bps: float,
    matured_20d_avg_realized_edge_bps: float,
    strategy_control_weight_delta: float,
    risk_overlay_weight_delta: float,
) -> None:
    storage = Storage(str(db_path))
    start = date(2026, 1, 5)
    for idx in range(weeks):
        week_start = (start + timedelta(days=7 * idx)).isoformat()
        week_label = f"2026-W{idx + 1:02d}"
        details = {
            "portfolio_id": portfolio_id,
            "market": market,
            "adaptive_strategy_active_market_profile": profile,
            "weekly_return": weekly_return + (idx * 0.0001),
            "max_drawdown": max_drawdown - (idx * 0.0002),
            "turnover": turnover + (0.005 if idx % 2 else 0.0),
            "outcome_sample_count": 12 + idx,
            "signal_quality_score": signal_quality_score + (0.01 if idx % 3 == 0 else 0.0),
            "execution_gate_blocked_weight": execution_gate_blocked_weight + (0.01 if idx % 4 == 0 else 0.0),
            "strategy_control_weight_delta": strategy_control_weight_delta,
            "risk_overlay_weight_delta": risk_overlay_weight_delta,
            "avg_execution_capture_bps": avg_execution_capture_bps + (1.0 if idx % 3 == 0 else 0.0),
            "avg_actual_slippage_bps": avg_actual_slippage_bps + (0.5 if idx % 2 else 0.0),
            "avg_expected_cost_bps": avg_expected_cost_bps,
            "outcome_selected_spread_5d_bps": outcome_selected_spread_5d_bps + (1.0 if idx % 2 else 0.0),
            "outcome_selected_spread_20d_bps": outcome_selected_spread_20d_bps + (2.0 if idx % 2 else 0.0),
            "outcome_selected_spread_60d_bps": outcome_selected_spread_60d_bps + (2.0 if idx % 3 == 0 else 0.0),
            "blocked_20d_avg_counterfactual_edge_bps": blocked_20d_avg_counterfactual_edge_bps + (2.0 if idx % 2 else 0.0),
            "matured_20d_avg_realized_edge_bps": matured_20d_avg_realized_edge_bps + (2.0 if idx % 2 else 0.0),
            "market_profile_tuning_action": "",
            "dominant_driver": "EXECUTION" if market == "US" else "STRATEGY",
        }
        storage.upsert_investment_weekly_tuning_history(
            {
                "week_label": week_label,
                "week_start": week_start,
                "window_start": week_start,
                "window_end": week_start,
                "market": market,
                "portfolio_id": portfolio_id,
                "active_market_profile": profile,
                "dominant_driver": details["dominant_driver"],
                "market_profile_tuning_action": "",
                "weekly_return": details["weekly_return"],
                "max_drawdown": details["max_drawdown"],
                "turnover": details["turnover"],
                "outcome_sample_count": details["outcome_sample_count"],
                "signal_quality_score": details["signal_quality_score"],
                "execution_cost_gap": 0.0,
                "execution_gate_blocked_weight": details["execution_gate_blocked_weight"],
                "strategy_control_weight_delta": details["strategy_control_weight_delta"],
                "risk_overlay_weight_delta": details["risk_overlay_weight_delta"],
                "risk_feedback_action": "",
                "execution_feedback_action": "",
                "shadow_apply_mode": "SUGGEST_ONLY",
                "risk_apply_mode": "SUGGEST_ONLY",
                "execution_apply_mode": "SUGGEST_ONLY",
                "market_profile_ready_for_manual_apply": 0,
                "details": details,
            }
        )


def test_build_market_walk_forward_report_recommends_market_specific_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.db"
    _seed_weekly_history(
        db_path,
        market="US",
        profile="US",
        portfolio_id="US:main",
        weeks=15,
        weekly_return=0.006,
        max_drawdown=-0.020,
        turnover=0.18,
        signal_quality_score=0.62,
        execution_gate_blocked_weight=0.18,
        avg_execution_capture_bps=12.0,
        avg_actual_slippage_bps=4.0,
        avg_expected_cost_bps=5.0,
        outcome_selected_spread_5d_bps=18.0,
        outcome_selected_spread_20d_bps=42.0,
        outcome_selected_spread_60d_bps=60.0,
        blocked_20d_avg_counterfactual_edge_bps=24.0,
        matured_20d_avg_realized_edge_bps=38.0,
        strategy_control_weight_delta=0.03,
        risk_overlay_weight_delta=0.02,
    )
    _seed_weekly_history(
        db_path,
        market="HK",
        profile="HK",
        portfolio_id="HK:main",
        weeks=15,
        weekly_return=0.004,
        max_drawdown=-0.030,
        turnover=0.42,
        signal_quality_score=0.48,
        execution_gate_blocked_weight=0.04,
        avg_execution_capture_bps=6.0,
        avg_actual_slippage_bps=11.0,
        avg_expected_cost_bps=9.0,
        outcome_selected_spread_5d_bps=10.0,
        outcome_selected_spread_20d_bps=12.0,
        outcome_selected_spread_60d_bps=18.0,
        blocked_20d_avg_counterfactual_edge_bps=4.0,
        matured_20d_avg_realized_edge_bps=11.0,
        strategy_control_weight_delta=0.05,
        risk_overlay_weight_delta=0.04,
    )

    report = review_market_walk_forward.build_market_walk_forward_report(
        db_path,
        adaptive_strategy_config="config/adaptive_strategy_framework.yaml",
        markets=["US", "HK"],
        min_weeks=10,
        train_weeks=6,
        validate_weeks=3,
        step_weeks=3,
    )

    summary_rows = {str(row["market"]): row for row in report["summary_rows"]}
    assert summary_rows["US"]["selected_candidate_family"] == "EXECUTION_RELAX"
    assert summary_rows["US"]["status"] == "RECOMMEND_PATCH"
    assert summary_rows["US"]["consecutive_stable_windows"] >= 3
    assert summary_rows["US"]["acceptance_failed_rules"] == ""
    assert summary_rows["HK"]["selected_candidate_family"] == "TURNOVER_TIGHTEN"
    assert summary_rows["HK"]["status"] == "RECOMMEND_PATCH"

    patch_rows = list(report["patch_rows"])
    assert any(row["config_path"] == "market_profiles.US.min_expected_edge_bps" for row in patch_rows)
    assert any(row["config_path"] == "market_profiles.HK.no_trade_band_pct" for row in patch_rows)


def test_build_market_walk_forward_report_marks_insufficient_history(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.db"
    _seed_weekly_history(
        db_path,
        market="CN",
        profile="CN",
        portfolio_id="CN:main",
        weeks=4,
        weekly_return=0.002,
        max_drawdown=-0.015,
        turnover=0.12,
        signal_quality_score=0.44,
        execution_gate_blocked_weight=0.03,
        avg_execution_capture_bps=4.0,
        avg_actual_slippage_bps=7.0,
        avg_expected_cost_bps=8.0,
        outcome_selected_spread_5d_bps=5.0,
        outcome_selected_spread_20d_bps=8.0,
        outcome_selected_spread_60d_bps=12.0,
        blocked_20d_avg_counterfactual_edge_bps=3.0,
        matured_20d_avg_realized_edge_bps=6.0,
        strategy_control_weight_delta=0.06,
        risk_overlay_weight_delta=0.05,
    )

    report = review_market_walk_forward.build_market_walk_forward_report(
        db_path,
        markets=["CN"],
        min_weeks=10,
        train_weeks=6,
        validate_weeks=3,
        step_weeks=3,
    )
    assert report["summary_rows"][0]["status"] == "INSUFFICIENT_HISTORY"


def test_walk_forward_main_writes_artifacts(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "audit.db"
    out_dir = tmp_path / "out"
    _seed_weekly_history(
        db_path,
        market="US",
        profile="US",
        portfolio_id="US:main",
        weeks=15,
        weekly_return=0.006,
        max_drawdown=-0.020,
        turnover=0.18,
        signal_quality_score=0.62,
        execution_gate_blocked_weight=0.18,
        avg_execution_capture_bps=12.0,
        avg_actual_slippage_bps=4.0,
        avg_expected_cost_bps=5.0,
        outcome_selected_spread_5d_bps=18.0,
        outcome_selected_spread_20d_bps=42.0,
        outcome_selected_spread_60d_bps=60.0,
        blocked_20d_avg_counterfactual_edge_bps=24.0,
        matured_20d_avg_realized_edge_bps=38.0,
        strategy_control_weight_delta=0.03,
        risk_overlay_weight_delta=0.02,
    )

    review_market_walk_forward.main(
        [
            "--db",
            str(db_path),
            "--market",
            "US",
            "--out_dir",
            str(out_dir),
            "--min_weeks",
            "10",
            "--train_weeks",
            "6",
            "--validate_weeks",
            "3",
            "--step_weeks",
            "3",
        ]
    )
    stdout = capsys.readouterr().out
    assert "ibkr-quant-walk-forward: market walk-forward tuning complete" in stdout
    summary_json = out_dir / "market_walk_forward_summary.json"
    assert summary_json.exists()
    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    assert payload["summary"]["market_count"] == 1
    assert (out_dir / "market_walk_forward_summary.csv").exists()
    assert (out_dir / "market_walk_forward_candidate_summary.csv").exists()
    assert (out_dir / "market_walk_forward_windows.csv").exists()
    assert (out_dir / "market_walk_forward_patch_recommendations.csv").exists()
    assert (out_dir / "market_walk_forward.md").exists()


def test_build_market_walk_forward_report_rejects_when_outcome_support_is_not_consistent(tmp_path: Path) -> None:
    db_path = tmp_path / "audit.db"
    _seed_weekly_history(
        db_path,
        market="US",
        profile="US",
        portfolio_id="US:weak",
        weeks=15,
        weekly_return=0.004,
        max_drawdown=-0.018,
        turnover=0.16,
        signal_quality_score=0.58,
        execution_gate_blocked_weight=0.16,
        avg_execution_capture_bps=10.0,
        avg_actual_slippage_bps=4.0,
        avg_expected_cost_bps=5.0,
        outcome_selected_spread_5d_bps=-2.0,
        outcome_selected_spread_20d_bps=18.0,
        outcome_selected_spread_60d_bps=24.0,
        blocked_20d_avg_counterfactual_edge_bps=20.0,
        matured_20d_avg_realized_edge_bps=22.0,
        strategy_control_weight_delta=0.02,
        risk_overlay_weight_delta=0.02,
    )
    report = review_market_walk_forward.build_market_walk_forward_report(
        db_path,
        markets=["US"],
        min_weeks=10,
        train_weeks=6,
        validate_weeks=3,
        step_weeks=3,
    )
    row = report["summary_rows"][0]
    assert row["selected_candidate_family"] == "EXECUTION_RELAX"
    assert row["status"] == "WATCH"
    assert "outcome_support_5_20_60" in str(row["acceptance_failed_rules"])
