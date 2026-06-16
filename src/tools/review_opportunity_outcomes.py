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
            groups.append(
                {
                    "market": row_market,
                    "portfolio_id": str(calibration_row.get("portfolio_id") or ""),
                    "group_name": group_name,
                    "candidate_rows": candidate_rows,
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
    validation_rows = [
        build_candidate_outcome_validation(
            _rows(group.get("candidate_rows")),
            matched_outcome_rows,
            market=str(group.get("market") or ""),
            portfolio_id=str(group.get("portfolio_id") or ""),
            group_name=str(group.get("group_name") or ""),
        )
        for group in groups
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "2026Q2.opportunity_outcome_validation.v1",
        "market": market_filter,
        "market_readiness_path": str(market_readiness_path),
        "weekly_unified_evidence_path": str(weekly_unified_evidence_path),
        "summary": build_candidate_outcome_validation_summary(validation_rows),
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
    md_path = out_dir / "opportunity_outcome_validation.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(csv_path, _rows(payload.get("rows")))
    _write_markdown(md_path, payload)
    print(f"Wrote opportunity outcome validation -> {json_path}")


if __name__ == "__main__":
    main()
