from pathlib import Path

from src.tools.review_weekly_io import read_csv_rows
from src.tools.review_weekly_markdown import write_weekly_review_markdown
from src.tools.review_weekly_thresholds import (
    build_feedback_threshold_suggestion_rows,
    load_feedback_threshold_overrides,
)


def test_load_feedback_threshold_overrides_normalizes_market_keys(tmp_path: Path):
    path = tmp_path / "thresholds.yaml"
    path.write_text(
        "markets:\n  us:\n    execution:\n      auto_confidence: 0.7\n",
        encoding="utf-8",
    )
    overrides = load_feedback_threshold_overrides(path)
    assert overrides == {"US": {"execution": {"auto_confidence": 0.7}}}


def test_build_feedback_threshold_suggestion_rows_relaxes_on_consistent_improvement():
    rows = build_feedback_threshold_suggestion_rows(
        [
            {
                "market": "US",
                "feedback_kind": "execution",
                "feedback_kind_label": "执行参数",
                "summary_signal": "持续改善",
                "tracked_count": 2,
                "avg_active_weeks": 3.0,
                "latest_improved_count": 1,
                "latest_deteriorated_count": 0,
                "w1_improved_count": 1,
                "w2_improved_count": 1,
                "w4_improved_count": 0,
                "w1_deteriorated_count": 0,
                "w2_deteriorated_count": 0,
                "w4_deteriorated_count": 0,
                "top_portfolios_text": "US:watchlist: 改善",
            }
        ]
    )
    assert rows[0]["suggestion_action"] == "RELAX_AUTO_APPLY"


def test_read_csv_rows_and_markdown_writer(tmp_path: Path):
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("symbol,score\nAAPL,1\n", encoding="utf-8")
    rows = read_csv_rows(csv_path)
    assert rows == [{"symbol": "AAPL", "score": "1"}]

    out_path = tmp_path / "weekly_review.md"
    write_weekly_review_markdown(
        out_path,
        summary_rows=[],
        trade_rows=[],
        broker_summary_rows=[],
        broker_diff_rows=[],
        reason_rows=[],
        shadow_summary_rows=[],
        shadow_feedback_rows=[],
        feedback_calibration_rows=[],
        feedback_automation_rows=[],
        feedback_effect_market_summary_rows=[],
        feedback_threshold_suggestion_rows=[],
        feedback_threshold_history_overview_rows=[],
        feedback_threshold_effect_overview_rows=[],
        feedback_threshold_cohort_overview_rows=[],
        feedback_threshold_trial_alert_rows=[],
        feedback_threshold_tuning_rows=[],
        labeling_summary={},
        labeling_skip_rows=[],
        attribution_rows=[],
        risk_review_rows=[],
        risk_feedback_rows=[],
        execution_session_rows=[],
        execution_hotspot_rows=[],
        execution_feedback_rows=[],
        window_label="2026-W13",
    )
    text = out_path.read_text(encoding="utf-8")
    assert "# Weekly Investment Review" in text
    assert "## Broker Execution Summary" in text
