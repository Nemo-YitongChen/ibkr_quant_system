from datetime import datetime

from src.app.supervisor_support import (
    feedback_confidence_value,
    in_window,
    merge_execution_feedback_penalties,
    parse_feedback_penalty_rows,
    past_time,
    scale_feedback_delta,
    scale_feedback_penalty_rows,
)


def test_parse_feedback_penalty_rows_accepts_json_string():
    rows = parse_feedback_penalty_rows('[{"symbol": "AAPL", "score_penalty": 0.1}]')
    assert rows == [{"symbol": "AAPL", "score_penalty": 0.1}]


def test_merge_execution_feedback_penalties_applies_decay_for_missing_symbols():
    merged = merge_execution_feedback_penalties(
        current_rows=[{"symbol": "AAPL", "execution_penalty": 0.12, "expected_cost_bps_add": 5}],
        previous_rows=[{"symbol": "TSLA", "execution_penalty": 0.20, "expected_cost_bps_add": 10, "score_penalty": 0.1}],
    )
    assert [row["symbol"] for row in merged] == ["TSLA", "AAPL"]
    assert merged[0]["reason"] == "execution_hotspot_decay"
    assert merged[0]["decay_steps"] == 1
    assert merged[0]["execution_penalty"] == 0.13


def test_scale_feedback_penalty_rows_respects_confidence():
    scaled = scale_feedback_penalty_rows(
        rows=[{"symbol": "AAPL", "score_penalty": 0.2, "execution_penalty": 0.1, "cooldown_days": 5}],
        row={"feedback_confidence": 0.5},
    )
    assert scaled == [
        {
            "symbol": "AAPL",
            "score_penalty": 0.1,
            "execution_penalty": 0.05,
            "cooldown_days": 2,
            "feedback_confidence": 0.5,
        }
    ]


def test_feedback_confidence_and_delta_floor():
    row = {"feedback_confidence": 0.25}
    assert feedback_confidence_value(row) == 0.25
    assert scale_feedback_delta(0.001, row, min_abs=0.01) == 0.01


def test_in_window_handles_same_day_and_overnight_windows():
    same_day = datetime(2026, 3, 30, 10, 0)
    overnight = datetime(2026, 3, 31, 1, 0)
    assert in_window(same_day, "09:30", "16:00", [0]) is True
    assert in_window(overnight, "23:20", "06:10", [0]) is True
    assert past_time(same_day, "09:45") is True
    assert past_time(same_day, "10:30") is False
