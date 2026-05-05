from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping

from .artifact_contracts import ArtifactContract
from .artifact_loader import LoadedArtifact


def artifact_health_status_label(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw == "degraded":
        return "有降级"
    if raw == "warning":
        return "有告警"
    return "已就绪"


def _parse_ts(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _age_hours(text: str, *, now: datetime) -> float | None:
    dt = _parse_ts(text)
    if dt is None:
        return None
    return round(max(0.0, (now - dt).total_seconds() / 3600.0), 2)


def evaluate_artifact_health(
    contract: ArtifactContract,
    loaded: LoadedArtifact,
    *,
    now: datetime | None = None,
    scope_label: str = "",
    market: str = "",
    watchlist: str = "",
    portfolio_id: str = "",
) -> Dict[str, Any]:
    now_dt = now or datetime.now(timezone.utc)
    missing_fields: List[str] = []
    missing_columns: List[str] = []
    warnings: List[str] = []

    if contract.format == "json":
        payload = dict(loaded.payload or {}) if isinstance(loaded.payload, dict) else {}
        for field in contract.required_fields:
            if field not in payload:
                missing_fields.append(field)
    else:
        columns = {str(column or "") for column in list(loaded.columns or [])}
        for column in contract.required_columns:
            if column not in columns:
                missing_columns.append(column)

    if loaded.exists and loaded.row_count <= 0 and not bool(contract.allow_empty):
        empty_warning = "artifact empty"
        if empty_warning not in warnings:
            warnings.append(empty_warning)

    age_hours = _age_hours(loaded.generated_at, now=now_dt)
    if contract.freshness_hours is not None and age_hours is not None and age_hours > float(contract.freshness_hours):
        warnings.append(f"stale artifact: age_hours={age_hours}")

    if loaded.generated_at_source == "file_mtime":
        warnings.append("legacy artifact: generated_at missing, using file mtime")
    elif not str(loaded.generated_at or "").strip():
        warnings.append("generated_at missing")

    if str(loaded.source or "").startswith("fallback:"):
        warnings.append(f"partial compatibility: {loaded.source}")

    if not str(loaded.schema_version or "").strip():
        warnings.append("schema_version missing")

    if not bool(loaded.exists):
        status = str(contract.missing_status or "degraded").strip().lower() or "degraded"
    elif missing_fields or missing_columns:
        status = "degraded"
    elif warnings:
        status = "warning"
    else:
        status = "ready"

    summary_bits: List[str] = []
    if not bool(loaded.exists):
        summary_bits.append(f"缺失 {contract.filename}")
    if missing_fields:
        summary_bits.append("缺字段 " + ",".join(missing_fields[:4]))
    if missing_columns:
        summary_bits.append("缺列 " + ",".join(missing_columns[:4]))
    if not summary_bits and warnings:
        warning = warnings[0]
        if warning.startswith("stale artifact"):
            summary_bits.append(f"产物过旧 {age_hours}h")
        elif warning.startswith("legacy artifact"):
            summary_bits.append("旧版产物，已退回 file mtime")
        elif warning.startswith("partial compatibility"):
            summary_bits.append("兼容模式读取")
        elif warning == "schema_version missing":
            summary_bits.append("旧版产物缺 schema_version")
        elif warning == "artifact empty":
            summary_bits.append("产物为空")
        else:
            summary_bits.append(warning)
    if not summary_bits:
        summary_bits.append("产物已就绪")

    freshness_ts = loaded.file_mtime_ts
    if loaded.generated_at_source != "file_mtime":
        generated_at_dt = _parse_ts(loaded.generated_at)
        if generated_at_dt is not None:
            freshness_ts = generated_at_dt.timestamp()

    return {
        "artifact_key": contract.artifact_key,
        "artifact_label": contract.label,
        "status": status,
        "status_label": artifact_health_status_label(status),
        "summary": "；".join(summary_bits),
        "path": str(loaded.path or ""),
        "source": str(loaded.source or ""),
        "scope_label": str(scope_label or "GLOBAL"),
        "market": str(market or ""),
        "watchlist": str(watchlist or ""),
        "portfolio_id": str(portfolio_id or ""),
        "generated_at": str(loaded.generated_at or ""),
        "generated_at_source": str(loaded.generated_at_source or ""),
        "schema_version": str(loaded.schema_version or ""),
        "schema_version_source": str(loaded.schema_version_source or ""),
        "age_hours": age_hours,
        "row_count": int(loaded.row_count or 0),
        "missing_fields": missing_fields,
        "missing_columns": missing_columns,
        "warnings": warnings,
        "freshness_ts": freshness_ts,
        "file_mtime_ts": loaded.file_mtime_ts,
    }


def build_artifact_consistency_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    row_map = {
        str(row.get("artifact_key") or ""): dict(row)
        for row in rows
        if str(row.get("scope_label") or "GLOBAL") == "GLOBAL"
    }
    review_row = row_map.get("weekly_review_summary")
    if not review_row:
        return []
    review_ts = review_row.get("file_mtime_ts") or review_row.get("freshness_ts")
    if review_ts in (None, ""):
        return []
    checks: List[Dict[str, Any]] = []
    for peer_key in (
        "weekly_execution_summary",
        "weekly_unified_evidence",
        "weekly_blocked_vs_allowed_expost",
        "weekly_trading_quality_evidence",
        "weekly_candidate_model_review",
        "weekly_broker_positions",
        "weekly_broker_comparison",
        "weekly_attribution_summary",
        "weekly_risk_review_summary",
        "weekly_patch_governance_summary",
    ):
        peer_row = row_map.get(peer_key)
        if not peer_row:
            continue
        peer_ts = peer_row.get("file_mtime_ts") or peer_row.get("freshness_ts")
        if peer_ts in (None, ""):
            continue
        delta_hours = round(abs(float(peer_ts) - float(review_ts)) / 3600.0, 2)
        if delta_hours <= 6.0:
            continue
        checks.append(
            {
                "artifact_key": f"consistency:{peer_key}",
                "artifact_label": f"{peer_row.get('artifact_label', peer_key)} Consistency",
                "status": "warning",
                "status_label": artifact_health_status_label("warning"),
                "summary": f"{peer_key} 与 weekly_review_summary 不同步 ({delta_hours}h)",
                "path": str(peer_row.get("path") or ""),
                "scope_label": "GLOBAL",
                "market": "",
                "watchlist": "",
                "portfolio_id": "",
                "generated_at": "",
                "generated_at_source": "",
                "schema_version": "",
                "schema_version_source": "",
                "age_hours": None,
                "row_count": 0,
                "missing_fields": [],
                "missing_columns": [],
                "warnings": [f"consistency drift: {delta_hours}h"],
                "freshness_ts": None,
                "consistency_with": "weekly_review_summary",
                "delta_hours": delta_hours,
            }
        )
    return checks


def build_artifact_health_overview(
    rows: Iterable[Mapping[str, Any]],
    *,
    consistency_rows: Iterable[Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    artifact_rows = [dict(row) for row in list(rows or [])]
    consistency = [dict(row) for row in list(consistency_rows or [])]
    all_rows = artifact_rows + consistency
    ready_count = sum(1 for row in all_rows if str(row.get("status") or "") == "ready")
    warning_count = sum(1 for row in all_rows if str(row.get("status") or "") == "warning")
    degraded_count = sum(1 for row in all_rows if str(row.get("status") or "") == "degraded")
    missing_count = sum(1 for row in artifact_rows if str(row.get("status") or "") == "degraded" and "缺失" in str(row.get("summary") or ""))
    stale_count = sum(1 for row in artifact_rows if any(str(w).startswith("stale artifact") for w in list(row.get("warnings", []) or [])))
    compatibility_count = sum(
        1
        for row in artifact_rows
        if any(
            str(w) in {"schema_version missing", "generated_at missing"} or str(w).startswith("legacy artifact") or str(w).startswith("partial compatibility")
            for w in list(row.get("warnings", []) or [])
        )
    )
    consistency_warning_count = len(consistency)
    status = "degraded" if degraded_count > 0 else "warning" if (warning_count > 0 or consistency_warning_count > 0) else "ready"
    summary = (
        f"artifact {len(artifact_rows)} | ready {ready_count} | warning {warning_count} | "
        f"degraded {degraded_count} | missing {missing_count} | stale {stale_count}"
    )
    if compatibility_count > 0:
        summary += f" | legacy {compatibility_count}"
    if consistency_warning_count > 0:
        summary += f" | consistency {consistency_warning_count}"
    sorted_rows = sorted(
        all_rows,
        key=lambda row: (
            0 if str(row.get("status") or "") == "degraded" else 1 if str(row.get("status") or "") == "warning" else 2,
            str(row.get("scope_label") or ""),
            str(row.get("artifact_key") or ""),
            str(row.get("market") or ""),
            str(row.get("watchlist") or ""),
        ),
    )
    return {
        "status": status,
        "status_label": artifact_health_status_label(status),
        "summary_text": summary,
        "artifact_count": len(artifact_rows),
        "ready_count": ready_count,
        "warning_count": warning_count,
        "degraded_count": degraded_count,
        "missing_count": missing_count,
        "stale_count": stale_count,
        "compatibility_warning_count": compatibility_count,
        "consistency_warning_count": consistency_warning_count,
        "rows": sorted_rows,
        "consistency_rows": consistency,
    }
