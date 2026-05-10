from __future__ import annotations

from pathlib import Path

from src.common.strategy_parameter_suggestions import (
    apply_strategy_parameter_suggestion_resolutions,
    build_strategy_parameter_suggestion_effectiveness_summary,
    build_strategy_parameter_suggestion_followup_rows,
    build_weekly_strategy_parameter_suggestion_rows,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def _write_strategy_suggestion_fixture(base_dir: Path) -> None:
    _write(
        base_dir / "config" / "ibkr_us.yaml",
        """
strategy_config: config/strategy_defaults_us.yaml
""",
    )
    _write(
        base_dir / "config" / "strategy_defaults_us.yaml",
        """
engine:
  mr_weight: 0.60
  bo_weight: 0.40
""",
    )
    _write(
        base_dir / "config" / "strategy_parameter_registry.yaml",
        """
fields:
  mr_weight:
    field_label: mean reversion signal weight
    step: 0.05
    bounds: [0.0, 1.0]
    precision: 2
  bo_weight:
    field_label: breakout signal weight
    step: 0.05
    bounds: [0.0, 1.0]
    precision: 2
priorities:
  SIGNAL_FUSION:
    mr_weight:
      rank: 1
      label: first review mean reversion weight
    bo_weight:
      rank: 2
      label: then review breakout weight
""",
    )


def test_signal_ranking_inverted_builds_read_only_primary_parameter_suggestion(tmp_path: Path) -> None:
    _write_strategy_suggestion_fixture(tmp_path)

    rows = build_weekly_strategy_parameter_suggestion_rows(
        [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 4,
                "top_minus_bottom_outcome_20d_bps": -85.0,
                "expected_to_realized_gap_bps": -35.0,
            }
        ],
        week_label="2026-W19",
        base_dir=tmp_path,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["primary_field"] == "mr_weight"
    assert row["config_scope"] == "STRATEGY_DEFAULTS"
    assert row["config_path"] == "engine.mr_weight"
    assert float(row["current_value"]) == 0.60
    assert float(row["suggested_value"]) == 0.55
    assert row["linked_evidence_artifact"] == "weekly_candidate_model_review"
    assert row["linked_evidence_key"] == "US:US:watchlist:SIGNAL_RANKING_INVERTED"
    assert int(row["auto_apply"]) == 0
    assert int(row["read_only"]) == 1
    assert "3 validation windows" in row["acceptance_rule"]
    assert "Revert" in row["rollback_note"]


def test_non_ranking_or_insufficient_samples_do_not_build_parameter_suggestions(tmp_path: Path) -> None:
    _write_strategy_suggestion_fixture(tmp_path)

    rows = build_weekly_strategy_parameter_suggestion_rows(
        [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "EXPECTED_EDGE_OVERSTATED",
                "labeled_candidate_count": 4,
            },
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 2,
            },
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE",
                "labeled_candidate_count": 1,
            },
        ],
        week_label="2026-W19",
        base_dir=tmp_path,
    )

    assert rows == []


def test_only_one_primary_suggestion_per_market_portfolio_week(tmp_path: Path) -> None:
    _write_strategy_suggestion_fixture(tmp_path)

    rows = build_weekly_strategy_parameter_suggestion_rows(
        [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 4,
                "top_minus_bottom_outcome_20d_bps": -85.0,
            },
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 5,
                "top_minus_bottom_outcome_20d_bps": -120.0,
            },
        ],
        week_label="2026-W19",
        base_dir=tmp_path,
    )

    assert len(rows) == 1


def test_apply_strategy_parameter_suggestion_resolutions_uses_latest_audit_row() -> None:
    suggestions = [
        {
            "suggestion_id": "s1",
            "market": "US",
            "portfolio_id": "US:watchlist",
            "primary_field": "mr_weight",
            "created_at": "2026-05-01T00:00:00+00:00",
        }
    ]

    resolved = apply_strategy_parameter_suggestion_resolutions(
        suggestions,
        [
            {
                "ts": "2026-05-02T00:00:00+00:00",
                "linked_strategy_parameter_suggestion_id": "s1",
                "resolution_status": "ACKNOWLEDGED",
                "resolution_note": "reviewed",
            },
            {
                "ts": "2026-05-03T00:00:00+00:00",
                "linked_strategy_parameter_suggestion_id": "s1",
                "resolution_status": "APPLIED",
                "resolution_note": "paper applied",
            },
        ],
    )

    assert resolved[0]["status"] == "APPLIED"
    assert resolved[0]["resolved_at"] == "2026-05-03T00:00:00+00:00"
    assert resolved[0]["resolution_source"] == "dashboard_control"
    assert resolved[0]["resolution_note"] == "paper applied"


def test_apply_strategy_parameter_suggestion_resolutions_ignores_unknown_status() -> None:
    suggestions = [{"suggestion_id": "s1", "primary_field": "mr_weight"}]

    resolved = apply_strategy_parameter_suggestion_resolutions(
        suggestions,
        [
            {
                "linked_strategy_parameter_suggestion_id": "s1",
                "resolution_status": "UNKNOWN",
            }
        ],
    )

    assert resolved[0]["status"] == "SUGGESTED"
    assert resolved[0]["resolved_at"] == ""


def test_strategy_parameter_suggestion_effectiveness_counts_resolutions_and_stale() -> None:
    summary = build_strategy_parameter_suggestion_effectiveness_summary(
        [
            {
                "suggestion_id": "s1",
                "primary_field": "mr_weight",
                "status": "APPLIED",
                "created_at": "2026-05-01T00:00:00+00:00",
                "resolved_at": "2026-05-02T00:00:00+00:00",
                "auto_apply": 0,
                "read_only": 1,
            },
            {
                "suggestion_id": "s2",
                "primary_field": "bo_weight",
                "status": "SUGGESTED",
                "created_at": "2026-04-15T00:00:00+00:00",
                "auto_apply": 0,
                "read_only": 1,
            },
        ],
        now_iso="2026-05-09T00:00:00+00:00",
        stale_after_days=14,
    )

    assert summary["suggestion_count"] == 2
    assert summary["applied_suggestion_count"] == 1
    assert summary["resolved_suggestion_count"] == 1
    assert summary["open_suggestion_count"] == 1
    assert summary["stale_suggestion_count"] == 1
    assert summary["avg_resolution_hours"] == 24.0
    assert summary["status"] == "warn"


def test_strategy_parameter_suggestion_followup_marks_applied_signal_fix_improved() -> None:
    followups = build_strategy_parameter_suggestion_followup_rows(
        [
            {
                "suggestion_id": "s1",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_field": "mr_weight",
                "status": "APPLIED",
                "source_signal": "SIGNAL_RANKING_INVERTED",
                "resolved_at": "2026-05-02T00:00:00+00:00",
            }
        ],
        [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_WORKING",
                "labeled_candidate_count": 4,
                "top_minus_bottom_outcome_5d_bps": 18.0,
                "top_minus_bottom_outcome_20d_bps": 42.0,
                "top_minus_bottom_outcome_60d_bps": 65.0,
                "expected_to_realized_gap_bps": 8.0,
                "avg_realized_edge_bps": 31.0,
                "avg_expected_post_cost_edge_bps": 23.0,
            }
        ],
        week_label="2026-W20",
        now_iso="2026-05-09T00:00:00+00:00",
    )

    assert len(followups) == 1
    assert followups[0]["followup_verdict"] == "IMPROVED"
    assert followups[0]["followup_review_label"] == "SIGNAL_RANKING_WORKING"
    assert followups[0]["followup_top_minus_bottom_outcome_5d_bps"] == 18.0
    assert followups[0]["followup_top_minus_bottom_outcome_20d_bps"] == 42.0
    assert followups[0]["followup_top_minus_bottom_outcome_60d_bps"] == 65.0
    assert followups[0]["followup_avg_realized_edge_bps"] == 31.0

    summary = build_strategy_parameter_suggestion_effectiveness_summary(
        [
            {
                "suggestion_id": "s1",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_field": "mr_weight",
                "status": "APPLIED",
            }
        ],
        now_iso="2026-05-09T00:00:00+00:00",
        followup_rows=followups,
    )
    assert summary["followup_count"] == 1
    assert summary["improved_followup_count"] == 1
    assert summary["status"] == "ok"


def test_strategy_parameter_suggestion_followup_marks_persistent_inversion_degraded() -> None:
    followups = build_strategy_parameter_suggestion_followup_rows(
        [
            {
                "suggestion_id": "s1",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_field": "mr_weight",
                "status": "APPLIED",
                "source_signal": "SIGNAL_RANKING_INVERTED",
            }
        ],
        [
            {
                "market": "US",
                "portfolio_id": "US:watchlist",
                "review_label": "SIGNAL_RANKING_INVERTED",
                "labeled_candidate_count": 5,
                "top_minus_bottom_outcome_20d_bps": -54.0,
            }
        ],
    )

    assert followups[0]["followup_verdict"] == "DEGRADED"

    summary = build_strategy_parameter_suggestion_effectiveness_summary(
        [{"suggestion_id": "s1", "status": "APPLIED"}],
        now_iso="2026-05-09T00:00:00+00:00",
        followup_rows=followups,
    )
    assert summary["degraded_followup_count"] == 1
    assert summary["status"] == "warn"


def test_strategy_parameter_suggestion_followup_handles_missing_or_insufficient_sample() -> None:
    followups = build_strategy_parameter_suggestion_followup_rows(
        [
            {
                "suggestion_id": "s1",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "primary_field": "mr_weight",
                "status": "APPLIED",
                "source_signal": "SIGNAL_RANKING_INVERTED",
            }
        ],
        [],
    )

    assert followups[0]["followup_verdict"] == "INSUFFICIENT_FOLLOWUP_SAMPLE"
    assert followups[0]["followup_review_label"] == "MISSING_CANDIDATE_MODEL_REVIEW"
