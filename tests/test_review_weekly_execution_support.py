from __future__ import annotations

from unittest.mock import patch

from src.tools.review_weekly_execution_support import _enrich_snapshot_rows


def test_enrich_snapshot_rows_filters_before_parsing_details():
    rows = [
        {
            "snapshot_id": "keep",
            "report_dir": "/tmp/report",
            "portfolio_id": "P1",
            "symbol": "AAA",
            "details": "{}",
        },
        {
            "snapshot_id": "skip",
            "report_dir": "/tmp/report",
            "portfolio_id": "P1",
            "symbol": "BBB",
            "details": "{}",
        },
    ]
    with patch("src.tools.review_weekly_execution_support._parse_json_dict", return_value={}) as parser:
        enriched = _enrich_snapshot_rows(rows, snapshot_ids={"keep"})

    assert [row["snapshot_id"] for row in enriched] == ["keep"]
    assert parser.call_count == 1


def test_enrich_snapshot_rows_filters_by_portfolio_symbol_key_before_parsing_details():
    rows = [
        {
            "snapshot_id": "keep",
            "report_dir": "/tmp/report",
            "portfolio_id": "P1",
            "symbol": "AAA",
            "details": "{}",
        },
        {
            "snapshot_id": "skip",
            "report_dir": "/tmp/report",
            "portfolio_id": "P1",
            "symbol": "BBB",
            "details": "{}",
        },
    ]
    with patch("src.tools.review_weekly_execution_support._parse_json_dict", return_value={}) as parser:
        enriched = _enrich_snapshot_rows(rows, portfolio_symbol_keys={("/tmp/report", "P1", "AAA")})

    assert [row["snapshot_id"] for row in enriched] == ["keep"]
    assert parser.call_count == 1
