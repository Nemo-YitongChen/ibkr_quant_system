from __future__ import annotations

import os
from datetime import datetime, timezone

from src.common.freshness import age_hours_from_timestamp, file_age_hours, freshness_status, parse_utc_datetime


NOW = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)


def test_parse_utc_datetime_normalizes_z_suffix() -> None:
    parsed = parse_utc_datetime("2026-05-10T11:00:00Z")

    assert parsed is not None
    assert parsed.isoformat() == "2026-05-10T11:00:00+00:00"


def test_age_hours_from_timestamp_returns_none_for_missing() -> None:
    assert age_hours_from_timestamp("", NOW) is None
    assert age_hours_from_timestamp("bad timestamp", NOW) is None


def test_age_hours_from_timestamp_clamps_future_age() -> None:
    assert age_hours_from_timestamp("2026-05-10T13:00:00+00:00", NOW) == 0.0


def test_file_age_hours_uses_mtime(tmp_path) -> None:
    path = tmp_path / "artifact.json"
    path.write_text("{}", encoding="utf-8")
    ts = datetime(2026, 5, 10, 10, 0, tzinfo=timezone.utc).timestamp()
    os.utime(path, (ts, ts))

    assert round(file_age_hours(path, NOW) or 0.0, 2) == 2.0


def test_freshness_status_labels_missing_stale_and_fresh() -> None:
    assert freshness_status(None, max_age_hours=24) == "MISSING"
    assert freshness_status(25.0, max_age_hours=24) == "STALE"
    assert freshness_status(1.0, max_age_hours=24) == "FRESH"
