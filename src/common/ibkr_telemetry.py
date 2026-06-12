from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_TELEMETRY_DIR = BASE_DIR / ".cache" / "ibkr_request_telemetry"
REQUEST_LANE_EXECUTION = "execution"
REQUEST_LANE_PROTECTIVE = "protective"
REQUEST_LANE_RESEARCH = "research"
REQUEST_LANE_UNKNOWN = "unknown"


def infer_ibkr_request_lane(tool: str, request_kind: str = "") -> str:
    tool_name = str(tool or "").strip().lower()
    kind = str(request_kind or "").strip().lower()
    if tool_name.startswith("run_investment_execution:"):
        return REQUEST_LANE_EXECUTION
    if tool_name.startswith(
        (
            "sync_investment_broker_snapshot:",
            "run_investment_guard:",
            "short_safety_sync:",
        )
    ):
        return REQUEST_LANE_PROTECTIVE
    if tool_name.startswith(
        (
            "generate_investment_report:",
            "generate_trade_report:",
            "run_investment_opportunity:",
            "label_investment_snapshots:",
            "probe_ibkr_history_access:",
        )
    ):
        return REQUEST_LANE_RESEARCH
    if kind in {"scanner", "historical_daily", "historical_5m", "market_data_snapshot"}:
        return REQUEST_LANE_RESEARCH
    return REQUEST_LANE_UNKNOWN


def _truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def telemetry_dir(path: str | Path | None = None) -> Path:
    raw = str(path or os.environ.get("IBKR_TELEMETRY_DIR", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_TELEMETRY_DIR


def _parse_ts(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _event_path(ts: datetime, *, directory: Path) -> Path:
    return directory / f"ibkr_requests_{ts.date().isoformat()}.jsonl"


def record_ibkr_request(
    request_kind: str,
    *,
    status: str = "success",
    market: str = "",
    tool: str = "",
    request_lane: str = "",
    symbol: str = "",
    actual_gateway_request: bool = True,
    quantity: int = 1,
    details: Dict[str, Any] | None = None,
    ts: datetime | None = None,
    directory: str | Path | None = None,
) -> None:
    if _truthy(os.environ.get("IBKR_TELEMETRY_DISABLED", "")):
        return
    event_ts = (ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
    out_dir = telemetry_dir(directory)
    effective_tool = str(tool or os.environ.get("IBKR_TELEMETRY_TOOL", "") or "")
    effective_lane = str(
        request_lane
        or os.environ.get("IBKR_REQUEST_LANE", "")
        or infer_ibkr_request_lane(effective_tool, request_kind)
    ).strip().lower()
    event = {
        "ts": event_ts.isoformat(),
        "request_kind": str(request_kind or "").strip().lower(),
        "status": str(status or "").strip().lower(),
        "market": str(market or os.environ.get("IBKR_TELEMETRY_MARKET", "") or "").upper(),
        "tool": effective_tool,
        "request_lane": effective_lane,
        "symbol": str(symbol or "").upper(),
        "actual_gateway_request": bool(actual_gateway_request),
        "quantity": max(1, int(quantity or 1)),
        "details": dict(details or {}),
    }
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with _event_path(event_ts, directory=out_dir).open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        return


def _date_range(start: date, end: date) -> Iterable[date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def load_ibkr_request_events(
    *,
    window_start: str | datetime,
    window_end: str | datetime,
    directory: str | Path | None = None,
) -> List[Dict[str, Any]]:
    start = _parse_ts(window_start) if not isinstance(window_start, datetime) else window_start.astimezone(timezone.utc)
    end = _parse_ts(window_end) if not isinstance(window_end, datetime) else window_end.astimezone(timezone.utc)
    if start is None or end is None or end < start:
        return []
    out_dir = telemetry_dir(directory)
    events: List[Dict[str, Any]] = []
    for day in _date_range(start.date(), end.date()):
        path = out_dir / f"ibkr_requests_{day.isoformat()}.jsonl"
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for line in lines:
            try:
                event = json.loads(line)
            except Exception:
                continue
            event_ts = _parse_ts(event.get("ts"))
            if event_ts is None or event_ts < start or event_ts > end:
                continue
            events.append(dict(event))
    return events


def summarize_ibkr_request_events(
    *,
    window_start: str | datetime,
    window_end: str | datetime,
    market_filter: str = "",
    directory: str | Path | None = None,
) -> List[Dict[str, Any]]:
    market_filter = str(market_filter or "").upper().strip()
    buckets: Dict[tuple[str, str, str, str, str, str], Dict[str, Any]] = {}
    symbols_by_key: Dict[tuple[str, str, str, str, str, str], set[str]] = defaultdict(set)
    for event in load_ibkr_request_events(window_start=window_start, window_end=window_end, directory=directory):
        market = str(event.get("market") or "").upper().strip()
        if market_filter and market_filter != "ALL" and market != market_filter:
            continue
        event_ts = _parse_ts(event.get("ts"))
        day = event_ts.date().isoformat() if event_ts is not None else ""
        tool = str(event.get("tool") or "")
        request_kind = str(event.get("request_kind") or "").lower().strip()
        request_lane = str(
            event.get("request_lane")
            or infer_ibkr_request_lane(tool, request_kind)
        ).lower().strip()
        status = str(event.get("status") or "").lower().strip()
        key = (day, market, tool, request_kind, request_lane, status)
        row = buckets.setdefault(
            key,
            {
                "date": day,
                "market": market,
                "tool": tool,
                "request_kind": request_kind,
                "request_lane": request_lane,
                "status": status,
                "event_count": 0,
                "gateway_request_count": 0,
                "cache_hit_count": 0,
                "symbol_count": 0,
                "sample_symbols": "",
                "latest_event_ts": "",
            },
        )
        quantity = max(1, int(event.get("quantity") or 1))
        row["event_count"] = int(row["event_count"]) + quantity
        if bool(event.get("actual_gateway_request", True)):
            row["gateway_request_count"] = int(row["gateway_request_count"]) + quantity
        if status == "cache_hit" or not bool(event.get("actual_gateway_request", True)):
            row["cache_hit_count"] = int(row["cache_hit_count"]) + quantity
        symbol = str(event.get("symbol") or "").upper().strip()
        if symbol:
            symbols_by_key[key].add(symbol)
        if event_ts is not None:
            latest_raw = str(row.get("latest_event_ts") or "")
            latest_ts = _parse_ts(latest_raw)
            if latest_ts is None or event_ts > latest_ts:
                row["latest_event_ts"] = event_ts.isoformat()
    rows = list(buckets.values())
    for row in rows:
        key = (
            str(row.get("date") or ""),
            str(row.get("market") or ""),
            str(row.get("tool") or ""),
            str(row.get("request_kind") or ""),
            str(row.get("request_lane") or ""),
            str(row.get("status") or ""),
        )
        symbols = sorted(symbols_by_key.get(key, set()))
        row["symbol_count"] = int(len(symbols))
        row["sample_symbols"] = ",".join(symbols[:10])
    rows.sort(key=lambda row: (
        str(row.get("date") or ""),
        str(row.get("market") or ""),
        str(row.get("tool") or ""),
        str(row.get("request_kind") or ""),
        str(row.get("request_lane") or ""),
        str(row.get("status") or ""),
    ))
    return rows


def build_ibkr_request_summary_payload(
    *,
    generated_at: str,
    week_label: str,
    window_start: str,
    window_end: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    gateway_total = sum(int(row.get("gateway_request_count") or 0) for row in rows)
    cache_total = sum(int(row.get("cache_hit_count") or 0) for row in rows)
    event_total = sum(int(row.get("event_count") or 0) for row in rows)
    by_kind: Dict[str, Dict[str, int]] = {}
    by_lane: Dict[str, Dict[str, int]] = {}
    for row in rows:
        kind = str(row.get("request_kind") or "unknown")
        bucket = by_kind.setdefault(kind, {"event_count": 0, "gateway_request_count": 0, "cache_hit_count": 0})
        bucket["event_count"] += int(row.get("event_count") or 0)
        bucket["gateway_request_count"] += int(row.get("gateway_request_count") or 0)
        bucket["cache_hit_count"] += int(row.get("cache_hit_count") or 0)
        lane = str(row.get("request_lane") or REQUEST_LANE_UNKNOWN)
        lane_bucket = by_lane.setdefault(
            lane,
            {"event_count": 0, "gateway_request_count": 0, "cache_hit_count": 0},
        )
        lane_bucket["event_count"] += int(row.get("event_count") or 0)
        lane_bucket["gateway_request_count"] += int(row.get("gateway_request_count") or 0)
        lane_bucket["cache_hit_count"] += int(row.get("cache_hit_count") or 0)
    return {
        "generated_at": str(generated_at or ""),
        "week_label": str(week_label or ""),
        "window_start": str(window_start or ""),
        "window_end": str(window_end or ""),
        "summary": {
            "row_count": int(len(rows)),
            "event_count": int(event_total),
            "gateway_request_count": int(gateway_total),
            "cache_hit_count": int(cache_total),
            "by_request_kind": by_kind,
            "by_request_lane": by_lane,
        },
        "rows": list(rows or []),
    }
