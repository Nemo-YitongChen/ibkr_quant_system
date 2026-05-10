from __future__ import annotations

from src.tools.review_weekly_output_support import build_weekly_output_bundle, build_weekly_rows_artifact_payload


def test_build_weekly_rows_artifact_payload_adds_contract_metadata() -> None:
    payload = build_weekly_rows_artifact_payload(
        generated_at="2026-04-30T00:00:00+00:00",
        week_label="2026-W18",
        window_start="2026-04-24",
        window_end="2026-04-30",
        artifact_type="weekly_unified_evidence",
        rows=[
            {"portfolio_id": "US:paper", "symbol": "AAPL"},
            {"portfolio_id": "HK:paper", "symbol": "0700.HK"},
        ],
    )

    assert payload["artifact_type"] == "weekly_unified_evidence"
    assert payload["week_label"] == "2026-W18"
    assert payload["row_count"] == 2
    assert payload["rows"][0]["portfolio_id"] == "US:paper"


def test_build_weekly_rows_artifact_payload_filters_non_dict_rows() -> None:
    payload = build_weekly_rows_artifact_payload(
        generated_at="",
        week_label="",
        window_start="",
        window_end="",
        artifact_type="weekly_blocked_vs_allowed_expost",
        rows=[{"market": "US"}, "bad-row"],  # type: ignore[list-item]
    )

    assert payload["row_count"] == 1
    assert payload["rows"] == [{"market": "US"}]


def test_build_weekly_output_bundle_adds_evidence_focus_effectiveness(tmp_path) -> None:
    bundle = build_weekly_output_bundle(
        out_dir=tmp_path,
        week_label="2026-W18",
        window_start="2026-04-24T00:00:00+00:00",
        window_end="2026-04-30T00:00:00+00:00",
        window_label="2026-W18",
        market_filter="ALL",
        portfolio_filter="ALL",
        thresholds_config_path=tmp_path / "thresholds.yaml",
        summary_rows=[],
        trade_rows=[],
        change_rows=[],
        sector_rows=[],
        reason_rows=[],
        equity_curve_rows=[],
        broker_summary_rows=[],
        execution_run_rows=[],
        execution_order_rows=[],
        shadow_review_order_rows=[],
        shadow_review_summary_rows=[],
        shadow_feedback_rows=[],
        feedback_calibration_rows=[],
        feedback_automation_rows=[],
        feedback_automation_effect_overview_rows=[],
        feedback_effect_market_summary_rows=[],
        feedback_threshold_suggestion_rows=[],
        feedback_threshold_history_overview_rows=[],
        feedback_threshold_effect_overview_rows=[],
        feedback_threshold_cohort_overview_rows=[],
        feedback_threshold_trial_alert_rows=[],
        feedback_threshold_tuning_rows=[],
        labeling_summary={},
        labeling_skip_rows=[],
        outcome_spread_rows=[],
        edge_realization_rows=[],
        blocked_edge_attribution_rows=[],
        decision_evidence_rows=[],
        decision_evidence_summary_rows=[],
        unified_evidence_rows=[],
        blocked_vs_allowed_expost_rows=[
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "BLOCKED_OUTPERFORMED_ALLOWED",
                "horizon": "20d",
                "blocked_count": 3,
                "allowed_count": 4,
            }
        ],
        candidate_model_review_rows=[
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 4,
                "top_minus_bottom_outcome_20d_bps": -85.0,
                "expected_to_realized_gap_bps": -35.0,
            }
        ],
        weekly_decision_evidence_history_overview_rows=[],
        trading_quality_evidence_rows=[],
        execution_effect_rows=[],
        planned_execution_cost_rows=[],
        execution_session_rows=[],
        execution_hotspot_rows=[],
        attribution_rows=[],
        risk_review_rows=[],
        risk_feedback_rows=[],
        execution_feedback_rows=[],
        market_profile_tuning_rows=[],
        market_profile_patch_readiness_rows=[],
        weekly_tuning_dataset_rows=[],
        weekly_tuning_dataset_summary={},
        weekly_tuning_history_overview_rows=[],
        weekly_edge_calibration_rows=[],
        weekly_slicing_calibration_rows=[],
        weekly_risk_calibration_rows=[],
        weekly_calibration_patch_suggestion_rows=[],
        weekly_patch_governance_summary_rows=[],
        weekly_control_timeseries_rows=[],
        broker_latest_rows_by_portfolio={},
        broker_diff_rows=[],
        strategy_context_rows=[],
        dashboard_control_audit_rows=[
            {
                "ts": "2026-05-02T00:00:00+00:00",
                "linked_strategy_parameter_suggestion_id": "2026-w18-us-us-watchlist-mr-weight",
                "resolution_status": "APPLIED",
                "resolution_note": "paper applied",
            }
        ],
    )

    summary = bundle["summary_payload"]["evidence_focus_effectiveness"]
    assert summary["new_action_count"] == 1
    assert summary["urgent_action_count"] == 1
    assert bundle["summary_payload"]["evidence_focus_actions"][0]["primary_action"] == "review_gate_thresholds"
    assert bundle["weekly_tuning_dataset_payload"]["evidence_focus_effectiveness"]["new_action_count"] == 1
    suggestions = bundle["summary_payload"]["strategy_parameter_suggestions"]
    assert suggestions[0]["primary_field"] == "mr_weight"
    assert suggestions[0]["linked_evidence_artifact"] == "weekly_candidate_model_review"
    assert suggestions[0]["auto_apply"] == 0
    assert suggestions[0]["status"] == "APPLIED"
    assert suggestions[0]["resolution_note"] == "paper applied"
    assert suggestions[0]["created_at"]
    suggestion_effectiveness = bundle["summary_payload"]["strategy_parameter_suggestion_effectiveness"]
    assert suggestion_effectiveness["suggestion_count"] == 1
    assert suggestion_effectiveness["open_suggestion_count"] == 0
    assert suggestion_effectiveness["auto_apply_count"] == 0
    assert suggestion_effectiveness["followup_count"] == 1
    assert suggestion_effectiveness["degraded_followup_count"] == 1
    assert bundle["weekly_tuning_dataset_payload"]["strategy_parameter_suggestions"][0]["primary_field"] == "mr_weight"
    assert (
        bundle["weekly_tuning_dataset_payload"]["strategy_parameter_suggestion_followup"][0]["followup_verdict"]
        == "DEGRADED"
    )
    assert (
        bundle["weekly_tuning_dataset_payload"]["strategy_parameter_suggestion_effectiveness"]["suggestion_count"]
        == 1
    )
    assert bundle["json_artifacts"]["weekly_strategy_parameter_suggestions.json"]["row_count"] == 1
    assert bundle["json_artifacts"]["weekly_strategy_parameter_suggestion_followup.json"]["row_count"] == 1
    assert "weekly_strategy_parameter_suggestions.csv" in bundle["csv_artifacts"]
    assert "weekly_strategy_parameter_suggestion_followup.csv" in bundle["csv_artifacts"]
    assert "evidence_focus_effectiveness_summary" in bundle["markdown_kwargs"]
    assert "strategy_parameter_suggestion_rows" in bundle["markdown_kwargs"]
    assert "strategy_parameter_suggestion_followup_rows" in bundle["markdown_kwargs"]
    assert "strategy_parameter_suggestion_effectiveness_summary" in bundle["markdown_kwargs"]
