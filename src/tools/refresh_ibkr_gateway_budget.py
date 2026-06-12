from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..common.ibkr_gateway_budget import (
    build_ibkr_gateway_budget_payload,
    build_ibkr_gateway_budget_rows,
    load_ibkr_gateway_budget_config,
)
from ..common.ibkr_telemetry import (
    build_ibkr_request_summary_payload,
    summarize_ibkr_request_events,
)


BASE_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh IBKR Gateway budget evidence from local telemetry only.")
    parser.add_argument("--out_dir", default="reports_investment_weekly")
    parser.add_argument("--supervisor_config", default="config/supervisor.yaml")
    parser.add_argument("--telemetry_dir", default="")
    parser.add_argument("--days", type=int, default=7)
    return parser.parse_args()


def _atomic_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


def _atomic_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    clean_rows = [dict(row or {}) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fieldnames: List[str] = []
    for row in clean_rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with temp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(clean_rows)
    os.replace(temp_path, path)


def refresh_gateway_budget_artifacts(
    *,
    out_dir: Path,
    supervisor_config: str,
    telemetry_directory: str | Path | None = None,
    days: int = 7,
    now: datetime | None = None,
) -> Dict[str, Any]:
    generated_dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window_end = generated_dt
    window_start = window_end - timedelta(days=max(1, int(days)))
    iso_year, iso_week, _ = generated_dt.isocalendar()
    week_label = f"{iso_year}-W{iso_week:02d}"
    generated_at = generated_dt.isoformat()
    window_start_text = window_start.isoformat()
    window_end_text = window_end.isoformat()

    request_rows = summarize_ibkr_request_events(
        window_start=window_start,
        window_end=window_end,
        market_filter="ALL",
        directory=telemetry_directory,
    )
    budget_config = load_ibkr_gateway_budget_config(
        BASE_DIR,
        supervisor_config_path=supervisor_config,
    )
    recent_24h_rows = summarize_ibkr_request_events(
        window_start=window_end - timedelta(hours=24),
        window_end=window_end,
        market_filter="ALL",
        directory=telemetry_directory,
    )
    recent_short_rows = summarize_ibkr_request_events(
        window_start=window_end
        - timedelta(minutes=max(1, int(budget_config.get("short_window_minutes", 10) or 10))),
        window_end=window_end,
        market_filter="ALL",
        directory=telemetry_directory,
    )
    request_payload = build_ibkr_request_summary_payload(
        generated_at=generated_at,
        week_label=week_label,
        window_start=window_start_text,
        window_end=window_end_text,
        rows=request_rows,
    )
    budget_rows = build_ibkr_gateway_budget_rows(
        request_rows,
        config=budget_config,
        generated_at=generated_at,
        window_start=window_start_text,
        window_end=window_end_text,
        recent_24h_rows=recent_24h_rows,
        recent_short_rows=recent_short_rows,
    )
    budget_payload = build_ibkr_gateway_budget_payload(
        generated_at=generated_at,
        week_label=week_label,
        window_start=window_start_text,
        window_end=window_end_text,
        rows=budget_rows,
    )

    _atomic_json(out_dir / "weekly_ibkr_request_summary.json", request_payload)
    _atomic_csv(out_dir / "weekly_ibkr_request_summary.csv", request_rows)
    _atomic_json(out_dir / "weekly_ibkr_gateway_budget_status.json", budget_payload)
    _atomic_csv(out_dir / "weekly_ibkr_gateway_budget_status.csv", budget_rows)
    return budget_payload


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser()
    if not out_dir.is_absolute():
        out_dir = (BASE_DIR / out_dir).resolve()
    payload = refresh_gateway_budget_artifacts(
        out_dir=out_dir,
        supervisor_config=str(args.supervisor_config),
        telemetry_directory=str(args.telemetry_dir or "") or None,
        days=int(args.days),
    )
    summary = dict(payload.get("summary") or {})
    print(
        "ibkr-gateway-budget-refresh: "
        f"status={summary.get('status', 'unknown')} "
        f"requests={summary.get('gateway_request_count', 0)} "
        f"over_budget={summary.get('over_budget_market_count', 0)}"
    )


if __name__ == "__main__":
    main()
