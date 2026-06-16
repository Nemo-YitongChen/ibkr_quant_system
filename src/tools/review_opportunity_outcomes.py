from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from ..common.markets import resolve_market_code
from ..common.opportunity_calibration import (
    build_candidate_outcome_validation,
    build_candidate_outcome_validation_summary,
)


def _load_json_dict(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _rows(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _candidate_group_rows(calibration_row: Mapping[str, Any], group_name: str) -> List[Dict[str, Any]]:
    if group_name == "positive_post_cost_candidates":
        rows = _rows(calibration_row.get("positive_post_cost_rows"))
        return rows or _rows(calibration_row.get("top_post_cost_rows"))
    if group_name == "close_wait_pullback":
        rows = _rows(calibration_row.get("close_wait_pullback_rows"))
        return rows or _rows(calibration_row.get("top_wait_rows"))
    return []


def _iter_weekly_evidence_rows(
    path: Path,
    *,
    markets: set[str],
    portfolio_symbols: Mapping[str, set[str]],
) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            market = resolve_market_code(str(row.get("market") or ""))
            if markets and market not in markets:
                continue
            portfolio_id = str(row.get("portfolio_id") or "")
            symbols = portfolio_symbols.get(portfolio_id)
            if symbols is None:
                continue
            if str(row.get("symbol") or "").strip() not in symbols:
                continue
            yield dict(row)


def _flatten_validation_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in dict(row).items()
        if key != "symbol_rows"
    }


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _suggestion_id(row: Mapping[str, Any], suffix: str) -> str:
    raw = ":".join(
        [
            str(row.get("market") or "GLOBAL"),
            str(row.get("portfolio_id") or "portfolio"),
            str(row.get("group_name") or "group"),
            suffix,
        ]
    )
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in raw).strip("_")


def _build_calibration_suggestion(
    validation_row: Mapping[str, Any],
    calibration_row: Mapping[str, Any],
) -> Dict[str, Any]:
    market = resolve_market_code(str(validation_row.get("market") or ""))
    portfolio_id = str(validation_row.get("portfolio_id") or "")
    group_name = str(validation_row.get("group_name") or "")
    validation_status = str(validation_row.get("status") or "")
    avg_5d = _float(validation_row.get("avg_outcome_5d_bps"), 0.0)
    avg_20d = _float(validation_row.get("avg_outcome_20d_bps"), 0.0)
    base = {
        "market": market,
        "portfolio_id": portfolio_id,
        "group_name": group_name,
        "validation_status": validation_status,
        "avg_outcome_5d_bps": avg_5d,
        "avg_outcome_20d_bps": avg_20d,
        "candidate_symbol_count": _int(validation_row.get("candidate_symbol_count"), 0),
        "matched_symbol_count": _int(validation_row.get("matched_symbol_count"), 0),
        "matured_5d_sample_count": _int(validation_row.get("matured_5d_sample_count"), 0),
        "matured_20d_sample_count": _int(validation_row.get("matured_20d_sample_count"), 0),
        "candidate_symbols": str(validation_row.get("candidate_symbols") or ""),
        "auto_apply": False,
        "read_only": True,
        "paper_only": True,
    }

    if group_name == "positive_post_cost_candidates":
        high_cost_positive_count = _int(calibration_row.get("high_cost_positive_edge_count"), 0)
        avg_post_cost_edge = _float(calibration_row.get("avg_post_cost_edge_bps"), 0.0)
        if (
            validation_status == "OUTCOME_SUPPORTS_GROUP"
            and market == "HK"
            and high_cost_positive_count > 0
        ):
            return {
                **base,
                "suggestion_id": _suggestion_id(validation_row, "hk_post_cost_threshold_review"),
                "suggestion_type": "HK_POST_COST_THRESHOLD_REVIEW",
                "priority": "P1",
                "primary_field": "submit_quality.max_expected_cost_bps",
                "direction": "review_market_specific_threshold_with_min_post_cost_margin",
                "primary_action": "prepare_hk_post_cost_threshold_paper_trial",
                "rationale": (
                    "HK positive post-cost symbols have mature positive 5/20d outcomes despite global cost-threshold pressure."
                ),
                "acceptance_rule": (
                    "Do not auto-apply. Change only one market-specific field in paper; require fresh HK BUY plan, "
                    "expected_post_cost_edge_bps >= 0, submit quality PASS, and no negative fill/slippage or 5/20d outcome degradation."
                ),
                "rollback_note": (
                    "Revert the HK-specific cost threshold if fresh paper fills show negative realized edge or worse 5/20d outcomes."
                ),
                "avg_post_cost_edge_bps": avg_post_cost_edge,
                "high_cost_positive_edge_count": high_cost_positive_count,
            }
        if validation_status == "OUTCOME_SUPPORTS_GROUP":
            return {
                **base,
                "suggestion_id": _suggestion_id(validation_row, "post_cost_monitor"),
                "suggestion_type": "POST_COST_MONITOR",
                "priority": "P2",
                "primary_field": "submit_quality.post_cost_edge_monitor",
                "direction": "keep_gate_monitor_realized_outcomes",
                "primary_action": "keep_post_cost_gate_and_collect_fresh_fills",
                "rationale": "Current positive post-cost symbol group has supportive historical 5/20d outcomes.",
                "acceptance_rule": "No automatic threshold change; use as evidence for future paper-only single-field calibration.",
                "rollback_note": "No config change to roll back.",
                "avg_post_cost_edge_bps": avg_post_cost_edge,
                "high_cost_positive_edge_count": high_cost_positive_count,
            }

    if group_name == "close_wait_pullback":
        close_wait_count = _int(calibration_row.get("close_wait_pullback_count"), 0)
        if validation_status == "OUTCOME_SUPPORTS_GROUP" and close_wait_count > 0:
            priority = "P1" if avg_20d >= 250.0 else "P2"
            return {
                **base,
                "suggestion_id": _suggestion_id(validation_row, "wait_pullback_anchor_review"),
                "suggestion_type": "WAIT_PULLBACK_ANCHOR_REVIEW",
                "priority": priority,
                "primary_field": "opportunity_entry.near_entry_gap_pct",
                "direction": "review_near_entry_limit_trial_without_relaxing_risk_or_edge",
                "primary_action": "prepare_wait_pullback_near_entry_paper_limit_trial",
                "rationale": (
                    "Close WAIT_PULLBACK symbols have mature positive 5/20d outcomes; anchor may be conservative for this market."
                ),
                "acceptance_rule": (
                    "Paper only. Do not lower risk, edge, cost, liquidity, market-rule, Gateway budget, or submit-quality gates. "
                    "Allow only small limit-trial candidates that already pass post-cost and whole-share checks."
                ),
                "rollback_note": (
                    "Revert near-entry trial settings if fill slippage or 5/20d outcomes deteriorate versus the close WAIT_PULLBACK group."
                ),
                "close_wait_pullback_count": close_wait_count,
                "avg_entry_anchor_gap_pct": _float(calibration_row.get("avg_entry_anchor_gap_pct"), 0.0),
                "min_entry_anchor_gap_pct": _float(calibration_row.get("min_entry_anchor_gap_pct"), 0.0),
                "dominant_anchor_component": str(calibration_row.get("dominant_anchor_component") or ""),
            }
        if validation_status == "NO_CANDIDATE_SYMBOLS":
            return {
                **base,
                "suggestion_id": _suggestion_id(validation_row, "wait_pullback_no_action"),
                "suggestion_type": "WAIT_PULLBACK_NO_ACTION",
                "priority": "P3",
                "primary_field": "opportunity_entry.anchor_review",
                "direction": "no_wait_pullback_candidate_group",
                "primary_action": "keep_existing_wait_pullback_policy",
                "rationale": "No close WAIT_PULLBACK candidate symbols were available for this group.",
                "acceptance_rule": "Do not change anchor settings without candidate symbols and mature outcomes.",
                "rollback_note": "No config change to roll back.",
            }

    return {
        **base,
        "suggestion_id": _suggestion_id(validation_row, "collect_more_outcomes"),
        "suggestion_type": "COLLECT_MORE_OUTCOMES",
        "priority": "P3",
        "primary_field": "opportunity_outcome_validation",
        "direction": "continue_evidence_collection",
        "primary_action": "wait_for_5d_20d_outcome_maturity",
        "rationale": "Candidate group does not yet have enough supportive mature outcome evidence for calibration.",
        "acceptance_rule": "Re-run outcome validation after more mature 5/20d samples are available.",
        "rollback_note": "No config change to roll back.",
    }


def build_calibration_suggestions(
    validation_rows: Iterable[Mapping[str, Any]],
    calibration_by_key: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    suggestions = []
    for validation_row in validation_rows:
        key = (
            resolve_market_code(str(validation_row.get("market") or "")),
            str(validation_row.get("portfolio_id") or ""),
            str(validation_row.get("group_name") or ""),
        )
        suggestions.append(_build_calibration_suggestion(validation_row, calibration_by_key.get(key, {})))
    suggestions.sort(
        key=lambda row: (
            {"P1": 0, "P2": 1, "P3": 2}.get(str(row.get("priority") or ""), 9),
            str(row.get("market") or ""),
            str(row.get("portfolio_id") or ""),
            str(row.get("group_name") or ""),
        )
    )
    return suggestions


def build_calibration_suggestion_summary(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    clean_rows = [dict(row or {}) for row in list(rows or []) if isinstance(row, Mapping)]
    type_counts: Dict[str, int] = {}
    priority_counts: Dict[str, int] = {}
    for row in clean_rows:
        suggestion_type = str(row.get("suggestion_type") or "UNKNOWN")
        priority = str(row.get("priority") or "UNKNOWN")
        type_counts[suggestion_type] = type_counts.get(suggestion_type, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
    return {
        "suggestion_count": int(len(clean_rows)),
        "type_counts": type_counts,
        "priority_counts": priority_counts,
        "read_only": True,
        "auto_apply": False,
        "paper_only": True,
    }


def build_opportunity_outcome_validation_payload(
    *,
    market_readiness_path: Path,
    weekly_unified_evidence_path: Path,
    market: str = "",
) -> Dict[str, Any]:
    market_filter = resolve_market_code(market)
    readiness = _load_json_dict(market_readiness_path)
    calibration = dict(readiness.get("opportunity_calibration") or {})
    groups: List[Dict[str, Any]] = []
    for group_name, collection_key in (
        ("positive_post_cost_candidates", "post_cost_rows"),
        ("close_wait_pullback", "wait_pullback_rows"),
    ):
        for calibration_row in _rows(calibration.get(collection_key)):
            row_market = resolve_market_code(str(calibration_row.get("market") or ""))
            if market_filter and row_market != market_filter:
                continue
            candidate_rows = _candidate_group_rows(calibration_row, group_name)
            calibration_context = {
                key: value
                for key, value in dict(calibration_row).items()
                if key not in {"positive_post_cost_rows", "top_post_cost_rows", "close_wait_pullback_rows", "top_wait_rows"}
            }
            groups.append(
                {
                    "market": row_market,
                    "portfolio_id": str(calibration_row.get("portfolio_id") or ""),
                    "group_name": group_name,
                    "candidate_rows": candidate_rows,
                    "calibration_context": calibration_context,
                }
            )

    portfolio_symbols: Dict[str, set[str]] = {}
    markets: set[str] = set()
    for group in groups:
        portfolio_id = str(group.get("portfolio_id") or "")
        markets.add(resolve_market_code(str(group.get("market") or "")))
        symbols = portfolio_symbols.setdefault(portfolio_id, set())
        for row in _rows(group.get("candidate_rows")):
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                symbols.add(symbol)

    matched_outcome_rows = list(
        _iter_weekly_evidence_rows(
            weekly_unified_evidence_path,
            markets={value for value in markets if value},
            portfolio_symbols=portfolio_symbols,
        )
    )
    validation_rows: List[Dict[str, Any]] = []
    calibration_by_key: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for group in groups:
        validation_row = build_candidate_outcome_validation(
            _rows(group.get("candidate_rows")),
            matched_outcome_rows,
            market=str(group.get("market") or ""),
            portfolio_id=str(group.get("portfolio_id") or ""),
            group_name=str(group.get("group_name") or ""),
        )
        validation_rows.append(validation_row)
        calibration_by_key[
            (
                resolve_market_code(str(group.get("market") or "")),
                str(group.get("portfolio_id") or ""),
                str(group.get("group_name") or ""),
            )
        ] = dict(group.get("calibration_context") or {})
    calibration_suggestions = build_calibration_suggestions(validation_rows, calibration_by_key)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "2026Q2.opportunity_outcome_validation.v2",
        "market": market_filter,
        "market_readiness_path": str(market_readiness_path),
        "weekly_unified_evidence_path": str(weekly_unified_evidence_path),
        "summary": build_candidate_outcome_validation_summary(validation_rows),
        "calibration_suggestion_summary": build_calibration_suggestion_summary(calibration_suggestions),
        "calibration_suggestions": calibration_suggestions,
        "rows": validation_rows,
    }


def _write_csv(path: Path, rows: List[Mapping[str, Any]]) -> None:
    fieldnames = [
        "market",
        "portfolio_id",
        "group_name",
        "status",
        "reason",
        "primary_action",
        "candidate_symbol_count",
        "matched_symbol_count",
        "matched_outcome_row_count",
        "matured_5d_sample_count",
        "matured_20d_sample_count",
        "avg_outcome_5d_bps",
        "avg_outcome_20d_bps",
        "positive_rate_5d",
        "positive_rate_20d",
        "latest_outcome_decision_ts",
        "candidate_symbols",
        "matched_symbols",
        "unmatched_symbols",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _flatten_validation_row(row).get(key, "") for key in fieldnames})


def _write_suggestions_csv(path: Path, rows: List[Mapping[str, Any]]) -> None:
    fieldnames = [
        "suggestion_id",
        "market",
        "portfolio_id",
        "group_name",
        "suggestion_type",
        "priority",
        "primary_field",
        "direction",
        "primary_action",
        "validation_status",
        "avg_outcome_5d_bps",
        "avg_outcome_20d_bps",
        "matured_5d_sample_count",
        "matured_20d_sample_count",
        "auto_apply",
        "read_only",
        "paper_only",
        "candidate_symbols",
        "rationale",
        "acceptance_rule",
        "rollback_note",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: dict(row).get(key, "") for key in fieldnames})


def _write_markdown(path: Path, payload: Mapping[str, Any]) -> None:
    lines = ["# Opportunity Outcome Validation", ""]
    summary = dict(payload.get("summary") or {})
    lines.append(
        "- Summary: "
        f"validations={summary.get('validation_count', 0)} "
        f"matched_symbols={summary.get('matched_symbol_count', 0)} "
        f"matured_5d={summary.get('matured_5d_sample_count', 0)} "
        f"matured_20d={summary.get('matured_20d_sample_count', 0)}"
    )
    lines.append(f"- Market: {payload.get('market') or 'ALL'}")
    lines.append("")
    lines.append("| Market | Portfolio | Group | Status | 5d avg bps | 20d avg bps | Symbols |")
    lines.append("|---|---|---|---:|---:|---:|---|")
    for row in _rows(payload.get("rows")):
        lines.append(
            "| {market} | {portfolio_id} | {group_name} | {status} | {avg5} | {avg20} | {symbols} |".format(
                market=row.get("market") or "",
                portfolio_id=row.get("portfolio_id") or "",
                group_name=row.get("group_name") or "",
                status=row.get("status") or "",
                avg5=row.get("avg_outcome_5d_bps", ""),
                avg20=row.get("avg_outcome_20d_bps", ""),
                symbols=row.get("candidate_symbols") or "",
            )
        )
    suggestions = _rows(payload.get("calibration_suggestions"))
    if suggestions:
        lines.append("")
        lines.append("## Calibration Suggestions")
        lines.append("")
        lines.append("| Priority | Market | Portfolio | Type | Field | Action | Auto apply |")
        lines.append("|---|---|---|---|---|---|---:|")
        for row in suggestions:
            lines.append(
                "| {priority} | {market} | {portfolio_id} | {suggestion_type} | {field} | {action} | {auto_apply} |".format(
                    priority=row.get("priority") or "",
                    market=row.get("market") or "",
                    portfolio_id=row.get("portfolio_id") or "",
                    suggestion_type=row.get("suggestion_type") or "",
                    field=row.get("primary_field") or "",
                    action=row.get("primary_action") or "",
                    auto_apply=row.get("auto_apply"),
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate opportunity candidate groups against mature weekly outcomes.")
    parser.add_argument("--market", default="", help="Optional market filter, for example HK.")
    parser.add_argument(
        "--market_readiness",
        default="runtime_data/paper_investment_only_duq152001/reports_supervisor/market_readiness.json",
        help="Path to market_readiness.json.",
    )
    parser.add_argument(
        "--weekly_unified_evidence",
        default="reports_investment_weekly/weekly_unified_evidence.csv",
        help="Path to weekly_unified_evidence.csv.",
    )
    parser.add_argument(
        "--out_dir",
        default="runtime_data/paper_investment_only_duq152001/reports_supervisor",
        help="Directory for validation artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_opportunity_outcome_validation_payload(
        market_readiness_path=Path(args.market_readiness),
        weekly_unified_evidence_path=Path(args.weekly_unified_evidence),
        market=str(args.market or ""),
    )
    json_path = out_dir / "opportunity_outcome_validation.json"
    csv_path = out_dir / "opportunity_outcome_validation.csv"
    suggestions_csv_path = out_dir / "opportunity_outcome_calibration_suggestions.csv"
    md_path = out_dir / "opportunity_outcome_validation.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(csv_path, _rows(payload.get("rows")))
    _write_suggestions_csv(suggestions_csv_path, _rows(payload.get("calibration_suggestions")))
    _write_markdown(md_path, payload)
    print(f"Wrote opportunity outcome validation -> {json_path}")


if __name__ == "__main__":
    main()
