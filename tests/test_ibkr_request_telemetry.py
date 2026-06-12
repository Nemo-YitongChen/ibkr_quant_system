from __future__ import annotations

from datetime import datetime, timezone
from tempfile import TemporaryDirectory

from src.common.ibkr_telemetry import (
    build_ibkr_request_summary_payload,
    record_ibkr_request,
    summarize_ibkr_request_events,
)


def test_ibkr_request_telemetry_records_and_summarizes_gateway_requests(monkeypatch):
    with TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("IBKR_TELEMETRY_TOOL", "run_investment_opportunity:us:watchlist")
        monkeypatch.setenv("IBKR_TELEMETRY_MARKET", "US")
        record_ibkr_request(
            "historical_daily",
            status="success",
            symbol="AAPL",
            quantity=1,
            ts=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            directory=tmpdir,
        )
        rows = summarize_ibkr_request_events(
            window_start="2026-05-01T00:00:00+00:00",
            window_end="2026-05-02T00:00:00+00:00",
            directory=tmpdir,
        )

        assert len(rows) == 1
        assert rows[0]["market"] == "US"
        assert rows[0]["tool"] == "run_investment_opportunity:us:watchlist"
        assert rows[0]["request_kind"] == "historical_daily"
        assert rows[0]["request_lane"] == "research"
        assert rows[0]["gateway_request_count"] == 1
        assert rows[0]["cache_hit_count"] == 0
        assert rows[0]["sample_symbols"] == "AAPL"


def test_ibkr_request_telemetry_counts_cache_hits_separately(monkeypatch):
    with TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("IBKR_TELEMETRY_TOOL", "generate_investment_report:us:watchlist")
        monkeypatch.setenv("IBKR_TELEMETRY_MARKET", "US")
        record_ibkr_request(
            "historical_daily",
            status="cache_hit",
            symbol="MSFT",
            actual_gateway_request=False,
            ts=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            directory=tmpdir,
        )
        rows = summarize_ibkr_request_events(
            window_start="2026-05-01T00:00:00+00:00",
            window_end="2026-05-02T00:00:00+00:00",
            directory=tmpdir,
        )
        payload = build_ibkr_request_summary_payload(
            generated_at="2026-05-02T00:00:00+00:00",
            week_label="2026-W18",
            window_start="2026-05-01T00:00:00+00:00",
            window_end="2026-05-02T00:00:00+00:00",
            rows=rows,
        )

        assert rows[0]["gateway_request_count"] == 0
        assert rows[0]["cache_hit_count"] == 1
        assert payload["summary"]["gateway_request_count"] == 0
        assert payload["summary"]["cache_hit_count"] == 1
        assert payload["summary"]["by_request_kind"]["historical_daily"]["cache_hit_count"] == 1
        assert payload["summary"]["by_request_lane"]["research"]["cache_hit_count"] == 1


def test_ibkr_request_telemetry_uses_explicit_execution_lane(monkeypatch):
    with TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("IBKR_TELEMETRY_TOOL", "run_investment_execution:us:watchlist")
        monkeypatch.setenv("IBKR_TELEMETRY_MARKET", "US")
        record_ibkr_request(
            "positions",
            ts=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            directory=tmpdir,
        )

        rows = summarize_ibkr_request_events(
            window_start="2026-05-01T00:00:00+00:00",
            window_end="2026-05-02T00:00:00+00:00",
            directory=tmpdir,
        )

        assert rows[0]["request_lane"] == "execution"
