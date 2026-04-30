from __future__ import annotations

from src.tools.review_weekly_output_support import build_weekly_rows_artifact_payload


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
