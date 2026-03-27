from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from shutil import copy2
from statistics import mean
from typing import Any, Dict, List

from ..analysis.report import write_csv, write_json


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_mean(values: List[float]) -> float:
    clean = [float(v) for v in values if v is not None]
    return float(mean(clean)) if clean else 0.0


def _hash_file(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Capture and compare baseline/regression snapshots from investment report outputs.")
    ap.add_argument("--market", default="", help="Market code for labeling only.")
    ap.add_argument("--portfolio_id", default="", help="Portfolio id for labeling only.")
    ap.add_argument("--report_dir", required=True, help="Directory containing investment report outputs.")
    ap.add_argument("--out_dir", default="reports_baseline", help="Directory to write baseline snapshots.")
    ap.add_argument("--baseline_name", default="current", help="Subdirectory name for the saved baseline.")
    ap.add_argument("--compare_to", default="", help="Path to an existing baseline_snapshot.json to compare against.")
    return ap.parse_args()


def _build_snapshot(report_dir: Path, market: str, portfolio_id: str) -> Dict[str, Any]:
    candidates = _read_csv(report_dir / "investment_candidates.csv")
    plans = _read_csv(report_dir / "investment_plan.csv")
    backtests = _read_csv(report_dir / "investment_backtest.csv")
    paper_summary = _read_json(report_dir / "investment_paper_summary.json")
    execution_summary = _read_json(report_dir / "investment_execution_summary.json")
    opportunity_summary = _read_json(report_dir / "investment_opportunity_summary.json")

    top_symbols = [str(row.get("symbol") or "").upper() for row in candidates[:10] if str(row.get("symbol") or "").strip()]
    action_counts: Dict[str, int] = {}
    for row in candidates:
        action = str(row.get("action") or "WATCH").upper()
        action_counts[action] = int(action_counts.get(action, 0) + 1)

    snapshot = {
        "market": str(market or ""),
        "portfolio_id": str(portfolio_id or ""),
        "report_dir": str(report_dir),
        "candidate_count": int(len(candidates)),
        "plan_count": int(len(plans)),
        "top_symbols": top_symbols,
        "action_counts": action_counts,
        "avg_score_top10": float(_safe_mean([_to_float(row.get("score")) for row in candidates[:10]])),
        "avg_bt_ret_30d": float(_safe_mean([_to_float(row.get("bt_avg_ret_30d")) for row in backtests])),
        "avg_bt_ret_60d": float(_safe_mean([_to_float(row.get("bt_avg_ret_60d")) for row in backtests])),
        "avg_bt_ret_90d": float(_safe_mean([_to_float(row.get("bt_avg_ret_90d")) for row in backtests])),
        "avg_bt_hit_rate_30d": float(_safe_mean([_to_float(row.get("bt_hit_rate_30d")) for row in backtests])),
        "avg_bt_hit_rate_60d": float(_safe_mean([_to_float(row.get("bt_hit_rate_60d")) for row in backtests])),
        "avg_bt_hit_rate_90d": float(_safe_mean([_to_float(row.get("bt_hit_rate_90d")) for row in backtests])),
        "paper_target_invested_weight": float(paper_summary.get("target_invested_weight") or 0.0),
        "paper_executed": bool(paper_summary.get("executed", False)),
        "paper_equity_after": float(paper_summary.get("equity_after") or 0.0),
        "execution_target_invested_weight": float(execution_summary.get("target_invested_weight") or 0.0),
        "execution_order_count": int(execution_summary.get("order_count") or 0),
        "execution_blocked_order_count": int(execution_summary.get("blocked_order_count") or 0),
        "execution_gap_symbols": int(execution_summary.get("gap_symbols") or 0),
        "execution_gap_notional": float(execution_summary.get("gap_notional") or 0.0),
        "opportunity_entry_now_count": int(opportunity_summary.get("entry_now_count") or 0),
        "opportunity_near_entry_count": int(opportunity_summary.get("near_entry_count") or 0),
        "opportunity_wait_count": int(opportunity_summary.get("wait_count") or 0),
        "report_hash": _hash_file(report_dir / "investment_report.md"),
        "paper_hash": _hash_file(report_dir / "investment_paper_report.md"),
        "execution_hash": _hash_file(report_dir / "investment_execution_report.md"),
        "opportunity_hash": _hash_file(report_dir / "investment_opportunity_report.md"),
    }
    return snapshot


def _compare_snapshots(current: Dict[str, Any], baseline: Dict[str, Any]) -> Dict[str, Any]:
    numeric_keys = [
        "candidate_count",
        "plan_count",
        "avg_score_top10",
        "avg_bt_ret_30d",
        "avg_bt_ret_60d",
        "avg_bt_ret_90d",
        "avg_bt_hit_rate_30d",
        "avg_bt_hit_rate_60d",
        "avg_bt_hit_rate_90d",
        "paper_target_invested_weight",
        "paper_equity_after",
        "execution_order_count",
        "execution_blocked_order_count",
        "execution_gap_symbols",
        "execution_gap_notional",
        "opportunity_entry_now_count",
        "opportunity_near_entry_count",
        "opportunity_wait_count",
    ]
    deltas = []
    for key in numeric_keys:
        cur = _to_float(current.get(key))
        base = _to_float(baseline.get(key))
        deltas.append({"metric": key, "baseline": base, "current": cur, "delta": cur - base})
    return {
        "deltas": deltas,
        "top_symbols_before": list(baseline.get("top_symbols", []) or []),
        "top_symbols_after": list(current.get("top_symbols", []) or []),
        "report_changed": str(current.get("report_hash") or "") != str(baseline.get("report_hash") or ""),
        "paper_changed": str(current.get("paper_hash") or "") != str(baseline.get("paper_hash") or ""),
        "execution_changed": str(current.get("execution_hash") or "") != str(baseline.get("execution_hash") or ""),
        "opportunity_changed": str(current.get("opportunity_hash") or "") != str(baseline.get("opportunity_hash") or ""),
    }


def _write_md(path: Path, snapshot: Dict[str, Any], comparison: Dict[str, Any]) -> None:
    lines = [
        "# Baseline Regression Review",
        "",
        f"- Market: {snapshot.get('market', '')}",
        f"- Portfolio: {snapshot.get('portfolio_id', '')}",
        f"- Candidate count: {int(snapshot.get('candidate_count') or 0)}",
        f"- Plan count: {int(snapshot.get('plan_count') or 0)}",
        f"- Top symbols: {', '.join(snapshot.get('top_symbols', []) or []) or 'N/A'}",
        f"- Paper target invested weight: {float(snapshot.get('paper_target_invested_weight') or 0.0):.3f}",
        f"- Execution order count: {int(snapshot.get('execution_order_count') or 0)}",
        f"- Opportunity entry-now count: {int(snapshot.get('opportunity_entry_now_count') or 0)}",
        "",
        "## Comparison",
    ]
    if not comparison:
        lines.append("- No comparison baseline provided.")
    else:
        for row in comparison.get("deltas", []) or []:
            lines.append(
                f"- {row['metric']}: baseline={float(row['baseline']):.4f} current={float(row['current']):.4f} delta={float(row['delta']):+.4f}"
            )
        lines.append(f"- Top symbols before: {', '.join(comparison.get('top_symbols_before', []) or []) or 'N/A'}")
        lines.append(f"- Top symbols after: {', '.join(comparison.get('top_symbols_after', []) or []) or 'N/A'}")
        lines.append(
            f"- Artifact hashes changed: report={comparison.get('report_changed', False)} paper={comparison.get('paper_changed', False)} "
            f"execution={comparison.get('execution_changed', False)} opportunity={comparison.get('opportunity_changed', False)}"
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    report_dir = Path(args.report_dir).resolve()
    out_dir = Path(args.out_dir).resolve() / str(args.baseline_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    snapshot = _build_snapshot(report_dir, args.market, args.portfolio_id)
    comparison: Dict[str, Any] = {}

    compare_path = Path(args.compare_to).resolve() if str(args.compare_to or "").strip() else None
    if compare_path and compare_path.exists():
        baseline = _read_json(compare_path)
        comparison = _compare_snapshots(snapshot, baseline)
        write_csv(str(out_dir / "baseline_comparison.csv"), list(comparison.get("deltas", []) or []))
        write_json(str(out_dir / "baseline_comparison.json"), comparison)

    write_json(str(out_dir / "baseline_snapshot.json"), snapshot)

    samples_dir = out_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "investment_report.md",
        "investment_paper_report.md",
        "investment_execution_report.md",
        "investment_opportunity_report.md",
    ):
        src = report_dir / name
        if src.exists():
            copy2(src, samples_dir / name)

    _write_md(out_dir / "baseline_review.md", snapshot, comparison)
    print(f"snapshot_json={out_dir / 'baseline_snapshot.json'}")
    print(f"markdown={out_dir / 'baseline_review.md'}")
    if comparison:
        print(f"comparison_json={out_dir / 'baseline_comparison.json'}")


if __name__ == "__main__":
    main()
