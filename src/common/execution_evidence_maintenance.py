from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from .markets import resolve_market_code


SCHEMA_VERSION = "2026Q2.execution_evidence_maintenance.v1"
MAINTAINABLE_ARTIFACT_STATUSES = {"DEGRADED_GATEWAY", "STALE"}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _gateway_rows_by_market(
    rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for raw in list(rows or []):
        row = dict(raw or {})
        market = resolve_market_code(str(row.get("market") or ""))
        if market:
            result[market] = row
    return result


def _candidate_rejection_reasons(
    row: Mapping[str, Any],
    *,
    gateway_row: Mapping[str, Any],
    excluded_markets: set[str],
) -> list[str]:
    market = resolve_market_code(str(row.get("market") or ""))
    reasons: list[str] = []
    if not market:
        reasons.append("missing_market")
    elif market in excluded_markets:
        reasons.append("excluded_market")
    if str(row.get("account_mode") or "paper").strip().lower() != "paper":
        reasons.append("non_paper_account")
    if not bool(row.get("run_investment_execution", False)):
        reasons.append("execution_disabled")
    artifact_status = str(
        row.get("market_readiness_artifact_health_status") or ""
    ).strip().upper()
    if artifact_status not in MAINTAINABLE_ARTIFACT_STATUSES:
        reasons.append("artifact_not_maintainable")
    if not bool(row.get("maintenance_report_fresh", False)):
        reasons.append(
            str(row.get("maintenance_report_reason") or "report_not_fresh")
        )
    if not gateway_row:
        reasons.append("missing_gateway_budget")
    else:
        if bool(gateway_row.get("submit_blocking", False)):
            reasons.append(
                str(
                    gateway_row.get("execution_capacity_reason")
                    or "gateway_execution_capacity_blocked"
                )
            )
        elif (
            str(gateway_row.get("execution_capacity_status") or "")
            .strip()
            .lower()
            == "degraded"
        ):
            reasons.append(
                str(
                    gateway_row.get("execution_capacity_reason")
                    or "gateway_execution_capacity_degraded"
                )
            )
    return reasons


def _candidate_priority(row: Mapping[str, Any]) -> tuple[Any, ...]:
    artifact_status = str(
        row.get("artifact_health_status") or ""
    ).strip().upper()
    return (
        0 if artifact_status == "DEGRADED_GATEWAY" else 1,
        0 if _int(row.get("planned_order_count"), 0) > 0 else 1,
        0 if _int(row.get("strategy_stale_suggestion_count"), 0) <= 0 else 1,
        -_float(row.get("artifact_age_hours"), 0.0),
        resolve_market_code(str(row.get("market") or "")),
        str(row.get("portfolio_id") or ""),
    )


def build_execution_evidence_maintenance_plan(
    readiness_rows: Iterable[Mapping[str, Any]],
    gateway_budget_rows: Iterable[Mapping[str, Any]],
    *,
    excluded_markets: Iterable[str] = (),
    max_targets: int = 1,
    generated_at: datetime | None = None,
) -> Dict[str, Any]:
    now = (generated_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    clean_readiness_rows = [
        dict(raw or {})
        for raw in list(readiness_rows or [])
        if isinstance(raw, Mapping)
    ]
    excluded = {
        resolve_market_code(str(value))
        for value in list(excluded_markets or [])
        if str(value or "").strip()
    }
    gateway_by_market = _gateway_rows_by_market(gateway_budget_rows)
    candidates: list[Dict[str, Any]] = []
    rejections: list[Dict[str, Any]] = []

    for row in clean_readiness_rows:
        market = resolve_market_code(str(row.get("market") or ""))
        gateway_row = dict(gateway_by_market.get(market) or {})
        reasons = _candidate_rejection_reasons(
            row,
            gateway_row=gateway_row,
            excluded_markets=excluded,
        )
        base = {
            "market": market,
            "portfolio_id": str(row.get("portfolio_id") or ""),
            "artifact_health_status": str(
                row.get("market_readiness_artifact_health_status") or ""
            ).strip().upper(),
            "artifact_age_hours": _float(
                row.get("market_readiness_artifact_age_hours"),
                0.0,
            ),
            "market_readiness_reason": str(
                row.get("market_readiness_reason") or ""
            ),
            "planned_order_count": _int(
                row.get("market_readiness_order_count"),
                0,
            ),
            "planned_gross_order_value": _float(
                row.get("market_readiness_planned_gross_order_value"),
                0.0,
            ),
            "planned_order_symbols": str(
                row.get("market_readiness_planned_order_symbols") or ""
            ),
            "strategy_stale_suggestion_count": _int(
                row.get("strategy_stale_suggestion_count"),
                0,
            ),
            "report_reason": str(row.get("maintenance_report_reason") or ""),
            "gateway_execution_capacity_status": str(
                gateway_row.get("execution_capacity_status") or ""
            ),
            "gateway_execution_capacity_reason": str(
                gateway_row.get("execution_capacity_reason") or ""
            ),
            "gateway_execution_request_count": _int(
                gateway_row.get("execution_gateway_request_count"),
                0,
            ),
            "gateway_execution_request_limit": _int(
                gateway_row.get("execution_reserve_weekly_requests"),
                0,
            ),
        }
        if reasons:
            rejections.append({**base, "reject_reasons": reasons})
        else:
            candidates.append(base)

    candidates.sort(key=_candidate_priority)
    selected = candidates[: max(1, min(_int(max_targets, 1), 1))]
    stale_or_degraded_count = sum(
        1
        for row in clean_readiness_rows
        if str(
            row.get("market_readiness_artifact_health_status") or ""
        ).strip().upper()
        in MAINTAINABLE_ARTIFACT_STATUSES
    )
    if selected:
        status = "READY"
        reason = "single_paper_no_submit_execution_refresh_ready"
    elif stale_or_degraded_count > 0:
        status = "BLOCKED"
        reason = "no_safe_execution_evidence_target"
    else:
        status = "EMPTY"
        reason = "no_stale_execution_evidence"

    target = dict(selected[0]) if selected else {}
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "status": status,
        "reason": reason,
        "paper_only": True,
        "submit_orders": False,
        "recovery_evidence_only": True,
        "does_not_relax_submit_gates": True,
        "max_targets": 1,
        "candidate_count": len(candidates),
        "rejection_count": len(rejections),
        "target_market": str(target.get("market") or ""),
        "target_portfolio_id": str(target.get("portfolio_id") or ""),
        "target": target,
        "candidates": candidates,
        "rejections": rejections,
    }


def write_execution_evidence_maintenance_state(
    path: Path,
    payload: Mapping[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temp_path, path)
