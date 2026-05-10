from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

from .markets import market_config_path, resolve_market_code
from .runtime_paths import resolve_repo_path
from .strategy_parameter_registry import (
    load_strategy_parameter_registry,
    strategy_parameter_field_meta,
    strategy_parameter_priority,
    strategy_parameter_proposed_value,
)

STRATEGY_PARAMETER_SUGGESTION_ARTIFACT = "weekly_strategy_parameter_suggestions"
LINKED_CANDIDATE_MODEL_REVIEW_ARTIFACT = "weekly_candidate_model_review"
SUGGESTION_STATUS_SUGGESTED = "SUGGESTED"
SUGGESTION_STATUS_ACKNOWLEDGED = "ACKNOWLEDGED"
SUGGESTION_STATUS_APPLIED = "APPLIED"
SUGGESTION_STATUS_REJECTED = "REJECTED"
SUGGESTION_STATUS_SUPERSEDED = "SUPERSEDED"
FOLLOWUP_VERDICT_IMPROVED = "IMPROVED"
FOLLOWUP_VERDICT_NO_CLEAR_CHANGE = "NO_CLEAR_CHANGE"
FOLLOWUP_VERDICT_DEGRADED = "DEGRADED"
FOLLOWUP_VERDICT_INSUFFICIENT_SAMPLE = "INSUFFICIENT_FOLLOWUP_SAMPLE"

KNOWN_FOLLOWUP_VERDICTS = {
    FOLLOWUP_VERDICT_IMPROVED,
    FOLLOWUP_VERDICT_NO_CLEAR_CHANGE,
    FOLLOWUP_VERDICT_DEGRADED,
    FOLLOWUP_VERDICT_INSUFFICIENT_SAMPLE,
}

KNOWN_SUGGESTION_STATUSES = {
    SUGGESTION_STATUS_SUGGESTED,
    SUGGESTION_STATUS_ACKNOWLEDGED,
    SUGGESTION_STATUS_APPLIED,
    SUGGESTION_STATUS_REJECTED,
    SUGGESTION_STATUS_SUPERSEDED,
}
RESOLVED_SUGGESTION_STATUSES = {
    SUGGESTION_STATUS_APPLIED,
    SUGGESTION_STATUS_REJECTED,
    SUGGESTION_STATUS_SUPERSEDED,
}
HANDLED_SUGGESTION_STATUSES = RESOLVED_SUGGESTION_STATUSES | {SUGGESTION_STATUS_ACKNOWLEDGED}
DEFAULT_FOLLOWUP_MIN_LABELED_CANDIDATES = 3
DEFAULT_FOLLOWUP_OUTCOME_SPREAD_THRESHOLD_BPS = 25.0


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _slug(value: Any) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    while "--" in text:
        text = text.replace("--", "-")
    return text.strip("-") or "unknown"


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _hours_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds() / 3600.0), 2)


def normalize_suggestion_status(status: Any) -> str:
    normalized = str(status or "").strip().upper()
    return normalized if normalized in KNOWN_SUGGESTION_STATUSES else SUGGESTION_STATUS_SUGGESTED


def _strategy_config_path(base_dir: Path, market: str) -> Path:
    market_cfg = _read_yaml(market_config_path(base_dir, market))
    raw_path = str(market_cfg.get("strategy_config") or "config/strategy_defaults.yaml")
    return resolve_repo_path(base_dir, raw_path)


def _current_engine_value(base_dir: Path, market: str, field: str) -> Tuple[str, str, Any]:
    config_file = _strategy_config_path(base_dir, market)
    payload = _read_yaml(config_file)
    engine_payload = dict(payload.get("engine") or {})
    return str(config_file), f"engine.{field}", engine_payload.get(field)


def _select_signal_weight_field(base_dir: Path, market: str) -> Tuple[str, str, str, Any]:
    mr_file, mr_path, mr_value = _current_engine_value(base_dir, market, "mr_weight")
    bo_file, bo_path, bo_value = _current_engine_value(base_dir, market, "bo_weight")
    mr = _safe_float(mr_value, 0.0)
    bo = _safe_float(bo_value, 0.0)
    if bo > mr:
        return "bo_weight", bo_file, bo_path, bo_value
    return "mr_weight", mr_file, mr_path, mr_value


def _source_signal_label(review_label: str) -> str:
    labels = {
        "SIGNAL_RANKING_INVERTED": "signal ranking inverted",
        "EXPECTED_EDGE_OVERSTATED": "expected edge overstated",
        "INSUFFICIENT_CANDIDATE_OUTCOME_SAMPLE": "insufficient candidate outcome sample",
        "SIGNAL_RANKING_WORKING": "signal ranking working",
        "MIXED_SIGNAL": "mixed signal",
    }
    return labels.get(str(review_label or "").strip().upper(), str(review_label or "") or "-")


def _build_signal_ranking_inverted_suggestion(
    row: Dict[str, Any],
    *,
    week_label: str,
    base_dir: Path,
    registry: Any,
) -> Dict[str, Any]:
    market = resolve_market_code(str(row.get("market") or ""))
    portfolio_id = str(row.get("portfolio_id") or "").strip()
    if not market or not portfolio_id:
        return {}

    field, config_file, config_path, current_value = _select_signal_weight_field(base_dir, market)
    if current_value in (None, ""):
        return {}
    suggested_value = strategy_parameter_proposed_value(
        field,
        current_value,
        "REDUCE",
        registry=registry,
    )
    try:
        delta_value = round(float(suggested_value) - float(current_value), 6)
    except Exception:
        delta_value = 0.0
    if delta_value == 0:
        return {}

    review_label = str(row.get("review_label") or "").strip().upper()
    field_meta = strategy_parameter_field_meta(field, registry=registry)
    priority_rank, priority_label = strategy_parameter_priority("SIGNAL_FUSION", field, registry=registry)
    spread = _safe_float(row.get("top_minus_bottom_outcome_20d_bps"), 0.0)
    expected_gap = _safe_float(row.get("expected_to_realized_gap_bps"), 0.0)
    evidence_key = f"{market}:{portfolio_id}:{review_label}"
    suggestion_id = f"{_slug(week_label)}-{_slug(market)}-{_slug(portfolio_id)}-{_slug(field)}"
    rationale = (
        "Top-ranked candidate outcome underperformed lower-ranked candidates post-cost; "
        "reduce the currently dominant signal fusion weight by one registry step before considering broader model changes."
    )
    return {
        "week_label": str(week_label or ""),
        "suggestion_id": suggestion_id,
        "market": market,
        "portfolio_id": portfolio_id,
        "primary_field": field,
        "field": field,
        "field_label": str(field_meta.get("field_label") or field),
        "scope": "SIGNAL_FUSION",
        "scope_label": "signal fusion",
        "config_scope": "STRATEGY_DEFAULTS",
        "config_file": config_file,
        "config_path": config_path,
        "current_value": current_value,
        "suggested_value": suggested_value,
        "delta_value": delta_value,
        "change_hint": "REDUCE",
        "change_hint_label": "reduce dominant signal fusion weight by one step",
        "priority_rank": int(priority_rank),
        "priority_label": str(priority_label or "review first"),
        "source_kind": "CANDIDATE_MODEL_REVIEW",
        "source_signal": review_label,
        "source_signal_label": _source_signal_label(review_label),
        "linked_evidence_artifact": LINKED_CANDIDATE_MODEL_REVIEW_ARTIFACT,
        "linked_evidence_key": evidence_key,
        "rationale": rationale,
        "acceptance_rule": (
            "Paper first; require at least 3 validation windows without post-cost degradation "
            "and top-ranked candidates must keep improving versus the median across 5/20/60d horizons."
        ),
        "rollback_note": (
            "Revert this primary field to the previous strategy default if 20d post-cost edge "
            "or 60d outcome spread deteriorates after application."
        ),
        "effect_tracking_window_days": 60,
        "auto_apply": 0,
        "read_only": 1,
        "status": SUGGESTION_STATUS_SUGGESTED,
        "created_at": "",
        "resolved_at": "",
        "resolution_source": "",
        "resolution_note": "",
        "labeled_candidate_count": int(row.get("labeled_candidate_count", 0) or 0),
        "top_minus_bottom_outcome_20d_bps": spread,
        "expected_to_realized_gap_bps": expected_gap,
        "suggestion_summary": (
            f"{market}/{portfolio_id} suggest {field}: {current_value} -> {suggested_value} "
            f"from {review_label}"
        ),
    }


def build_weekly_strategy_parameter_suggestion_rows(
    candidate_model_review_rows: Iterable[Dict[str, Any]],
    *,
    week_label: str,
    base_dir: Path,
) -> List[Dict[str, Any]]:
    registry = load_strategy_parameter_registry(base_dir)
    out: List[Dict[str, Any]] = []
    seen_portfolios: set[Tuple[str, str]] = set()
    for raw in list(candidate_model_review_rows or []):
        row = dict(raw or {})
        market = resolve_market_code(str(row.get("market") or ""))
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        key = (market, portfolio_id)
        if not market or not portfolio_id or key in seen_portfolios:
            continue
        review_label = str(row.get("review_label") or "").strip().upper()
        if review_label != "SIGNAL_RANKING_INVERTED":
            continue
        if int(row.get("labeled_candidate_count", 0) or 0) < 3:
            continue
        suggestion = _build_signal_ranking_inverted_suggestion(
            row,
            week_label=week_label,
            base_dir=base_dir,
            registry=registry,
        )
        if suggestion:
            seen_portfolios.add(key)
            out.append(suggestion)
    out.sort(
        key=lambda row: (
            int(row.get("priority_rank", 99) or 99),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("primary_field") or ""),
        )
    )
    return out


def normalize_strategy_parameter_suggestion(raw: Dict[str, Any] | None) -> Dict[str, Any]:
    source = dict(raw or {})
    market = resolve_market_code(str(source.get("market") or ""))
    suggestion_id = str(
        source.get("suggestion_id")
        or source.get("strategy_parameter_suggestion_id")
        or source.get("linked_strategy_parameter_suggestion_id")
        or ""
    ).strip()
    primary_field = str(
        source.get("primary_field") or source.get("field") or source.get("linked_strategy_parameter_field") or ""
    ).strip()
    portfolio_id = str(source.get("portfolio_id") or "").strip()
    week_label = str(source.get("week_label") or source.get("week") or "").strip()
    if not suggestion_id and primary_field:
        suggestion_id = f"{_slug(week_label)}-{_slug(market)}-{_slug(portfolio_id)}-{_slug(primary_field)}"
    return {
        **source,
        "week_label": week_label,
        "suggestion_id": suggestion_id,
        "market": market or str(source.get("market") or "").strip().upper(),
        "portfolio_id": portfolio_id,
        "primary_field": primary_field,
        "field": str(source.get("field") or primary_field),
        "config_path": str(
            source.get("config_path") or source.get("linked_strategy_parameter_config_path") or ""
        ).strip(),
        "status": normalize_suggestion_status(source.get("status")),
        "created_at": str(source.get("created_at") or "").strip(),
        "resolved_at": str(source.get("resolved_at") or "").strip(),
        "resolution_source": str(source.get("resolution_source") or "").strip(),
        "resolution_note": str(source.get("resolution_note") or "").strip(),
        "auto_apply": int(source.get("auto_apply", 0) or 0),
        "read_only": int(source.get("read_only", 1) or 0),
    }


def apply_strategy_parameter_suggestion_resolutions(
    suggestions: Iterable[Dict[str, Any]],
    audit_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    normalized_suggestions = [
        normalize_strategy_parameter_suggestion(row) for row in list(suggestions or []) if isinstance(row, dict)
    ]
    latest_by_suggestion_id: Dict[str, tuple[int, str, Dict[str, Any]]] = {}
    for idx, raw in enumerate(list(audit_rows or [])):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        suggestion_id = str(row.get("linked_strategy_parameter_suggestion_id") or "").strip()
        if not suggestion_id:
            continue
        status = normalize_suggestion_status(row.get("resolution_status"))
        if status == SUGGESTION_STATUS_SUGGESTED:
            continue
        ts = str(row.get("ts") or row.get("timestamp") or row.get("updated_at") or "").strip()
        existing = latest_by_suggestion_id.get(suggestion_id)
        if existing is None or (ts, idx) >= (existing[1], existing[0]):
            latest_by_suggestion_id[suggestion_id] = (idx, ts, row)

    resolved: List[Dict[str, Any]] = []
    for suggestion in normalized_suggestions:
        suggestion_id = str(suggestion.get("suggestion_id") or "").strip()
        match = latest_by_suggestion_id.get(suggestion_id)
        if not match:
            resolved.append(suggestion)
            continue
        _, ts, audit = match
        updated = dict(suggestion)
        updated["status"] = normalize_suggestion_status(audit.get("resolution_status"))
        updated["resolved_at"] = ts
        updated["resolution_source"] = "dashboard_control"
        updated["resolution_note"] = str(audit.get("resolution_note") or "").strip()
        resolved.append(updated)
    return resolved


def _review_key(row: Dict[str, Any]) -> Tuple[str, str]:
    return (
        resolve_market_code(str(row.get("market") or "")),
        str(row.get("portfolio_id") or "").strip(),
    )


def _followup_verdict(
    *,
    source_signal: str,
    review_label: str,
    labeled_candidate_count: int,
    top_minus_bottom_outcome_20d_bps: float,
    min_labeled_candidates: int,
    spread_threshold_bps: float,
) -> str:
    if labeled_candidate_count < min_labeled_candidates:
        return FOLLOWUP_VERDICT_INSUFFICIENT_SAMPLE

    signal = str(source_signal or "").strip().upper()
    label = str(review_label or "").strip().upper()
    spread = float(top_minus_bottom_outcome_20d_bps)
    threshold = abs(float(spread_threshold_bps))

    if signal == "SIGNAL_RANKING_INVERTED":
        if label == "SIGNAL_RANKING_WORKING" or spread >= threshold:
            return FOLLOWUP_VERDICT_IMPROVED
        if label == "SIGNAL_RANKING_INVERTED" or spread <= -threshold:
            return FOLLOWUP_VERDICT_DEGRADED
        return FOLLOWUP_VERDICT_NO_CLEAR_CHANGE

    if spread >= threshold:
        return FOLLOWUP_VERDICT_IMPROVED
    if spread <= -threshold:
        return FOLLOWUP_VERDICT_DEGRADED
    return FOLLOWUP_VERDICT_NO_CLEAR_CHANGE


def build_strategy_parameter_suggestion_followup_rows(
    suggestions: Iterable[Dict[str, Any]],
    candidate_model_review_rows: Iterable[Dict[str, Any]],
    *,
    week_label: str = "",
    now_iso: str = "",
    min_labeled_candidates: int = DEFAULT_FOLLOWUP_MIN_LABELED_CANDIDATES,
    spread_threshold_bps: float = DEFAULT_FOLLOWUP_OUTCOME_SPREAD_THRESHOLD_BPS,
) -> List[Dict[str, Any]]:
    """Compare applied parameter suggestions with the latest candidate model review rows."""
    review_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for raw in list(candidate_model_review_rows or []):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        key = _review_key(row)
        if not key[0] or not key[1]:
            continue
        review_by_key[key] = row

    out: List[Dict[str, Any]] = []
    for raw in list(suggestions or []):
        if not isinstance(raw, dict):
            continue
        suggestion = normalize_strategy_parameter_suggestion(raw)
        if normalize_suggestion_status(suggestion.get("status")) != SUGGESTION_STATUS_APPLIED:
            continue
        key = _review_key(suggestion)
        review = review_by_key.get(key, {})
        labeled_count = int(review.get("labeled_candidate_count", 0) or 0)
        spread_5d = _safe_float(review.get("top_minus_bottom_outcome_5d_bps"), 0.0)
        spread_20d = _safe_float(review.get("top_minus_bottom_outcome_20d_bps"), 0.0)
        spread_60d = _safe_float(review.get("top_minus_bottom_outcome_60d_bps"), 0.0)
        review_label = str(review.get("review_label") or "").strip().upper()
        verdict = _followup_verdict(
            source_signal=str(suggestion.get("source_signal") or ""),
            review_label=review_label,
            labeled_candidate_count=labeled_count,
            top_minus_bottom_outcome_20d_bps=spread_20d,
            min_labeled_candidates=max(1, int(min_labeled_candidates)),
            spread_threshold_bps=spread_threshold_bps,
        )
        summary = (
            f"{suggestion.get('market', '')}/{suggestion.get('portfolio_id', '')} "
            f"{suggestion.get('primary_field', '')} follow-up={verdict} "
            f"review={review_label or 'MISSING'} spread20={spread_20d:.1f}bps"
        )
        out.append(
            {
                "week_label": str(week_label or suggestion.get("week_label") or ""),
                "generated_at": str(now_iso or ""),
                "suggestion_id": str(suggestion.get("suggestion_id") or ""),
                "market": str(suggestion.get("market") or ""),
                "portfolio_id": str(suggestion.get("portfolio_id") or ""),
                "primary_field": str(suggestion.get("primary_field") or ""),
                "status": SUGGESTION_STATUS_APPLIED,
                "followup_verdict": verdict,
                "followup_review_label": review_label or "MISSING_CANDIDATE_MODEL_REVIEW",
                "followup_labeled_candidate_count": labeled_count,
                "followup_top_minus_bottom_outcome_5d_bps": spread_5d,
                "followup_top_minus_bottom_outcome_20d_bps": spread_20d,
                "followup_top_minus_bottom_outcome_60d_bps": spread_60d,
                "followup_expected_to_realized_gap_bps": _safe_float(
                    review.get("expected_to_realized_gap_bps"), 0.0
                ),
                "followup_avg_realized_edge_bps": _safe_float(
                    review.get("avg_realized_edge_bps"), 0.0
                ),
                "followup_avg_expected_post_cost_edge_bps": _safe_float(
                    review.get("avg_expected_post_cost_edge_bps"), 0.0
                ),
                "effect_tracking_window_days": int(
                    suggestion.get("effect_tracking_window_days", 60) or 60
                ),
                "resolution_source": str(suggestion.get("resolution_source") or ""),
                "resolved_at": str(suggestion.get("resolved_at") or ""),
                "source_signal": str(suggestion.get("source_signal") or ""),
                "linked_evidence_artifact": str(
                    suggestion.get("linked_evidence_artifact") or LINKED_CANDIDATE_MODEL_REVIEW_ARTIFACT
                ),
                "linked_evidence_key": str(suggestion.get("linked_evidence_key") or ""),
                "followup_summary": summary,
                "read_only": 1,
            }
        )

    out.sort(
        key=lambda row: (
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("primary_field") or ""),
            str(row.get("suggestion_id") or ""),
        )
    )
    return out


def build_strategy_parameter_suggestion_effectiveness_summary(
    suggestions: Iterable[Dict[str, Any]],
    *,
    now_iso: str,
    stale_after_days: int = 14,
    followup_rows: Iterable[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    rows = [
        normalize_strategy_parameter_suggestion(row)
        for row in list(suggestions or [])
        if isinstance(row, dict)
    ]
    now_dt = _parse_iso_datetime(now_iso) or datetime.now(timezone.utc)
    stale_hours = max(0.0, float(stale_after_days) * 24.0)
    status_counts: Dict[str, int] = {}
    stale_suggestion_ids: List[str] = []
    resolution_hours: List[float] = []

    for row in rows:
        status = normalize_suggestion_status(row.get("status"))
        status_counts[status] = int(status_counts.get(status, 0)) + 1
        created_dt = _parse_iso_datetime(row.get("created_at"))
        resolved_dt = _parse_iso_datetime(row.get("resolved_at"))
        hours = _hours_between(created_dt, resolved_dt)
        if hours is not None and status in HANDLED_SUGGESTION_STATUSES:
            resolution_hours.append(hours)
        age_hours = _hours_between(created_dt, now_dt)
        if status == SUGGESTION_STATUS_SUGGESTED and created_dt is not None and age_hours is not None and age_hours >= stale_hours:
            stale_suggestion_ids.append(str(row.get("suggestion_id") or ""))

    resolved_count = sum(1 for row in rows if normalize_suggestion_status(row.get("status")) in RESOLVED_SUGGESTION_STATUSES)
    handled_count = sum(1 for row in rows if normalize_suggestion_status(row.get("status")) in HANDLED_SUGGESTION_STATUSES)
    open_count = sum(1 for row in rows if normalize_suggestion_status(row.get("status")) == SUGGESTION_STATUS_SUGGESTED)
    auto_apply_count = sum(1 for row in rows if int(row.get("auto_apply", 0) or 0) != 0)
    read_only_count = sum(1 for row in rows if int(row.get("read_only", 1) or 0) != 0)
    avg_resolution_hours = round(sum(resolution_hours) / len(resolution_hours), 2) if resolution_hours else 0.0
    followups = [
        dict(row)
        for row in list(followup_rows or [])
        if isinstance(row, dict)
    ]
    followup_verdict_counts: Dict[str, int] = {}
    for row in followups:
        verdict = str(row.get("followup_verdict") or "").strip().upper()
        if verdict not in KNOWN_FOLLOWUP_VERDICTS:
            verdict = FOLLOWUP_VERDICT_NO_CLEAR_CHANGE
        followup_verdict_counts[verdict] = int(followup_verdict_counts.get(verdict, 0)) + 1
    degraded_followup_count = int(followup_verdict_counts.get(FOLLOWUP_VERDICT_DEGRADED, 0))
    improved_followup_count = int(followup_verdict_counts.get(FOLLOWUP_VERDICT_IMPROVED, 0))
    no_clear_change_followup_count = int(followup_verdict_counts.get(FOLLOWUP_VERDICT_NO_CLEAR_CHANGE, 0))
    insufficient_followup_sample_count = int(
        followup_verdict_counts.get(FOLLOWUP_VERDICT_INSUFFICIENT_SAMPLE, 0)
    )
    primary = rows[0] if rows else {}
    summary_text = (
        f"suggestions={len(rows)} open={open_count} handled={handled_count} resolved={resolved_count} "
        f"stale={len(stale_suggestion_ids)} avg_resolution_hours={avg_resolution_hours:.2f}"
    )
    if followups:
        summary_text = (
            f"{summary_text} followups={len(followups)} improved={improved_followup_count} "
            f"degraded={degraded_followup_count} insufficient={insufficient_followup_sample_count}"
        )
    return {
        "status": "warn" if stale_suggestion_ids or auto_apply_count or degraded_followup_count else "ok",
        "summary_text": summary_text,
        "suggestion_count": len(rows),
        "new_suggestion_count": len(rows),
        "open_suggestion_count": open_count,
        "handled_suggestion_count": handled_count,
        "resolved_suggestion_count": resolved_count,
        "acknowledged_suggestion_count": int(status_counts.get(SUGGESTION_STATUS_ACKNOWLEDGED, 0)),
        "applied_suggestion_count": int(status_counts.get(SUGGESTION_STATUS_APPLIED, 0)),
        "rejected_suggestion_count": int(status_counts.get(SUGGESTION_STATUS_REJECTED, 0)),
        "superseded_suggestion_count": int(status_counts.get(SUGGESTION_STATUS_SUPERSEDED, 0)),
        "stale_suggestion_count": len(stale_suggestion_ids),
        "avg_resolution_hours": avg_resolution_hours,
        "stale_after_days": int(stale_after_days),
        "status_counts": status_counts,
        "followup_count": len(followups),
        "improved_followup_count": improved_followup_count,
        "degraded_followup_count": degraded_followup_count,
        "no_clear_change_followup_count": no_clear_change_followup_count,
        "insufficient_followup_sample_count": insufficient_followup_sample_count,
        "followup_verdict_counts": followup_verdict_counts,
        "stale_suggestion_ids": [suggestion_id for suggestion_id in stale_suggestion_ids if suggestion_id][:10],
        "auto_apply_count": auto_apply_count,
        "read_only_count": read_only_count,
        "primary_market": str(primary.get("market") or ""),
        "primary_portfolio_id": str(primary.get("portfolio_id") or ""),
        "primary_field": str(primary.get("primary_field") or ""),
        "read_only": True,
    }
