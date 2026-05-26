from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_utc_datetime(value: Any) -> datetime | None:
    """Parse common artifact timestamps and normalize them to UTC."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def age_hours_from_timestamp(
    value: Any,
    now: datetime | None = None,
    *,
    ndigits: int = 2,
) -> float | None:
    dt = parse_utc_datetime(value)
    if dt is None:
        return None
    now_dt = (now or utc_now()).astimezone(timezone.utc)
    age = max(0.0, (now_dt - dt).total_seconds() / 3600.0)
    return round(age, int(ndigits))


def file_age_hours(path: Path | str | None, now: datetime | None = None) -> float | None:
    if path is None:
        return None
    artifact_path = Path(path)
    if not artifact_path.exists():
        return None
    try:
        modified_at = datetime.fromtimestamp(artifact_path.stat().st_mtime, timezone.utc)
    except OSError:
        return None
    now_dt = (now or utc_now()).astimezone(timezone.utc)
    return max(0.0, (now_dt - modified_at).total_seconds() / 3600.0)


def freshness_status(
    age_hours: float | None,
    *,
    max_age_hours: float,
    missing_status: str = "MISSING",
    stale_status: str = "STALE",
    fresh_status: str = "FRESH",
) -> str:
    if age_hours is None:
        return str(missing_status)
    if float(max_age_hours) > 0.0 and float(age_hours) > float(max_age_hours):
        return str(stale_status)
    return str(fresh_status)
