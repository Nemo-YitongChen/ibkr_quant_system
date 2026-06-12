from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping

from .freshness import parse_utc_datetime


SCHEMA_VERSION = "2026Q2.auto_order_recovery_checkpoint.v1"


def load_recovery_checkpoint(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def write_recovery_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


def build_pending_recovery_checkpoint(
    recovery_plan: Mapping[str, Any],
    *,
    now: datetime,
    retry_interval_min: int,
) -> Dict[str, Any]:
    now_utc = now.astimezone(timezone.utc)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "PENDING",
        "target_market": str(recovery_plan.get("target_market") or "").strip().upper(),
        "target_portfolio_id": str(recovery_plan.get("target_portfolio_id") or "").strip(),
        "target_symbols": str(recovery_plan.get("target_symbols") or "").strip(),
        "target_submit_quality_status": str(
            recovery_plan.get("target_submit_quality_status") or ""
        ).strip().upper(),
        "started_at": now_utc.isoformat(),
        "updated_at": now_utc.isoformat(),
        "last_attempt_at": "",
        "next_attempt_at": now_utc.isoformat(),
        "retry_interval_min": max(1, int(retry_interval_min)),
        "attempt_count": 0,
        "report_refreshed": False,
        "execution_dry_run_refreshed": False,
        "completed_at": "",
        "last_error": "",
        "paper_only": True,
        "submit_orders": False,
    }


def recovery_checkpoint_context(
    checkpoint: Mapping[str, Any],
    *,
    now: datetime,
    report_refreshed: bool,
    execution_refreshed: bool,
) -> Dict[str, Any]:
    state = dict(checkpoint or {})
    if str(state.get("status") or "").strip().upper() != "PENDING":
        return {}
    now_utc = now.astimezone(timezone.utc)
    next_attempt = parse_utc_datetime(state.get("next_attempt_at"))
    eligible = bool(next_attempt is None or now_utc >= next_attempt)
    reason = "eligible_targeted_no_submit_refresh" if eligible else "recovery_retry_cooldown"
    target_market = str(state.get("target_market") or "").strip().upper()
    target_portfolio_id = str(state.get("target_portfolio_id") or "").strip()
    return {
        "checkpoint": {
            **state,
            "report_refreshed": bool(report_refreshed),
            "execution_dry_run_refreshed": bool(execution_refreshed),
        },
        "plan": {
            "status": "targeted_frontier_refresh_required",
            "target_market": target_market,
            "target_portfolio_id": target_portfolio_id,
            "target_symbols": str(state.get("target_symbols") or ""),
            "target_submit_quality_status": str(state.get("target_submit_quality_status") or ""),
            "paper_only": True,
            "does_not_submit_orders": True,
            "does_not_relax_submit_gates": True,
        },
        "eligibility": {
            "active": True,
            "eligible": eligible,
            "reason": reason,
            "status": "targeted_frontier_refresh_required",
            "target_market": target_market,
            "target_portfolio_id": target_portfolio_id,
            "target_symbols": str(state.get("target_symbols") or ""),
            "target_submit_quality_status": str(state.get("target_submit_quality_status") or ""),
            "allowed_actions": (
                ["generate_investment_report", "run_investment_execution_no_submit"]
                if eligible
                else []
            ),
            "paper_only": True,
            "submit_orders": False,
            "does_not_relax_submit_gates": True,
            "force_target_refresh": True,
        },
    }


def mark_recovery_checkpoint_attempt(
    checkpoint: Mapping[str, Any],
    *,
    now: datetime,
    error: str = "",
) -> Dict[str, Any]:
    state = dict(checkpoint or {})
    now_utc = now.astimezone(timezone.utc)
    retry_interval = max(1, int(state.get("retry_interval_min", 60) or 60))
    state.update(
        {
            "updated_at": now_utc.isoformat(),
            "last_attempt_at": now_utc.isoformat(),
            "next_attempt_at": (now_utc + timedelta(minutes=retry_interval)).isoformat(),
            "attempt_count": int(state.get("attempt_count", 0) or 0) + 1,
            "last_error": str(error or ""),
        }
    )
    return state


def mark_recovery_checkpoint_complete(
    checkpoint: Mapping[str, Any],
    *,
    now: datetime,
) -> Dict[str, Any]:
    state = dict(checkpoint or {})
    now_utc = now.astimezone(timezone.utc)
    state.update(
        {
            "status": "COMPLETE",
            "updated_at": now_utc.isoformat(),
            "completed_at": now_utc.isoformat(),
            "report_refreshed": True,
            "execution_dry_run_refreshed": True,
            "last_error": "",
        }
    )
    return state
