from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..analysis.investment_portfolio import InvestmentPaperConfig
from ..common.cli import build_cli_parser, emit_cli_summary
from .review_weekly_io import (
    read_csv_rows as _read_csv,
    read_json as _read_json,
)
from .review_weekly_thresholds import (
    build_feedback_threshold_suggestion_rows as _build_feedback_threshold_suggestion_rows,
    build_feedback_threshold_tuning_summary as _build_feedback_threshold_tuning_summary,
    load_feedback_threshold_overrides as _load_feedback_threshold_overrides,
)
from .review_weekly_output_support import (
    build_weekly_broker_local_diff_rows as _build_weekly_broker_local_diff_rows,
    build_weekly_broker_summary_rows as _build_weekly_broker_summary_rows,
    build_weekly_csv_artifacts as _build_weekly_csv_artifacts,
    build_weekly_cli_summary_payload as _build_weekly_cli_summary_payload,
    build_weekly_equity_curve_rows as _build_equity_curve_rows_support,
    build_weekly_execution_summary_rows as _build_execution_summary_rows_support,
    build_weekly_holdings_change_rows as _build_holdings_change_rows_support,
    build_weekly_latest_run_positions as _build_latest_run_positions_support,
    build_weekly_market_from_portfolio_or_symbol as _market_from_portfolio_or_symbol_support,
    build_weekly_output_bundle as _build_weekly_output_bundle,
    build_weekly_position_snapshots as _build_position_snapshots_support,
    build_weekly_reason_summary_rows as _build_reason_summary_support,
    build_weekly_review_markdown_kwargs as _build_weekly_review_markdown_kwargs,
    build_weekly_review_summary_payload as _build_weekly_review_summary_payload,
    build_weekly_sector_rows as _build_sector_rows_support,
    build_weekly_summarize_changes as _summarize_changes_support,
    build_weekly_top_holdings_text as _top_holdings_text_support,
    build_weekly_top_sector_text as _top_sector_text_support,
    build_weekly_tuning_dataset_payload as _build_weekly_tuning_dataset_payload,
    write_weekly_csv_artifacts as _write_weekly_csv_artifacts,
    write_weekly_json_artifacts as _write_weekly_json_artifacts,
    write_weekly_markdown_artifact as _write_weekly_markdown_artifact,
)
from .review_weekly_feedback_support import (
    _SESSION_LABELS,
    _apply_outcome_calibration,
    _build_execution_analysis_bundle,
    _augment_summary_rows_with_strategy_context as _augment_summary_rows_with_strategy_context_support,
    _build_attribution_rows as _build_attribution_rows_support,
    _build_market_profile_patch_readiness as _build_market_profile_patch_readiness_support,
    _build_execution_parent_rows,
    _build_feedback_automation_rows,
    _build_feedback_automation_effect_overview,
    _build_market_profile_tuning_summary as _build_market_profile_tuning_summary_support,
    _build_risk_review_rows as _build_risk_review_rows_support,
    _build_weekly_blocked_edge_attribution_rows,
    _build_feedback_calibration_rows as _build_feedback_calibration_rows_support,
    _build_weekly_decision_evidence_history_overview as _build_weekly_decision_evidence_history_overview_support,
    _build_weekly_decision_evidence_rows,
    _build_weekly_decision_evidence_summary_rows,
    _build_weekly_edge_calibration_rows as _build_weekly_edge_calibration_rows_support,
    _build_weekly_edge_realization_rows,
    _build_weekly_outcome_spread_rows,
    _build_weekly_risk_calibration_rows as _build_weekly_risk_calibration_rows_support,
    _build_weekly_slicing_calibration_rows as _build_weekly_slicing_calibration_rows_support,
    _build_weekly_control_timeseries_rows,
    _build_weekly_patch_governance_summary_rows,
    _build_weekly_portfolio_summary_rows,
    _build_weekly_tuning_dataset_rows,
    _build_weekly_tuning_history_overview,
    _build_weekly_tuning_dataset_summary,
    _filter_execution_metric_rows,
    _build_execution_effect_rows,
    _build_execution_feedback_rows,
    _build_execution_gate_rows,
    _build_execution_hotspot_penalties,
    _build_execution_hotspot_rows,
    _build_execution_session_rows,
    _build_feedback_effect_market_summary,
    _build_risk_feedback_rows as _build_risk_feedback_rows_support,
    _build_shadow_feedback_rows as _build_shadow_feedback_rows_support,
    _build_planned_execution_cost_rows,
    _decision_summary_by_week,
    _feedback_calibration_support,
    _feedback_confidence,
    _feedback_confidence_label,
    _feedback_control_driver_context,
    _feedback_effect_snapshot as _feedback_effect_snapshot_support,
    _feedback_maturity_alert_bucket as _feedback_maturity_alert_bucket_support,
    _is_execution_gate_status,
    _latest_report_dir as _latest_report_dir_support,
    _load_ibkr_history_probe_market_map as _load_ibkr_history_probe_market_map_support,
    _load_market_sentiment as _load_market_sentiment_support,
    _load_report_data_warning as _load_report_data_warning_support,
    _market_profile_patch_conflict as _market_profile_patch_conflict_support,
    _market_research_only_yfinance as _market_research_only_yfinance_support,
    _latest_risk_overlay as _latest_risk_overlay_support,
    _risk_driver_and_diagnosis as _risk_driver_and_diagnosis_support,
    _risk_overlay_from_history_row as _risk_overlay_from_history_row_support,
    _report_json as _report_json_support,
    _runtime_config_paths_for_market,
    _build_shadow_review_summary_rows,
    _build_shadow_signal_penalties,
    _build_feedback_threshold_cohort_overview,
    _build_feedback_threshold_effect_overview,
    _build_feedback_threshold_history_overview,
    _build_feedback_threshold_trial_alert_overview,
    _build_market_data_gate_map as _build_market_data_gate_map_support,
    _build_shadow_review_order_rows as _build_shadow_review_order_rows_support,
    _link_execution_orders_to_candidate_snapshots,
    _persist_feedback_automation_history as _persist_feedback_automation_history_support,
    _persist_feedback_threshold_history as _persist_feedback_threshold_history_support,
    _persist_market_profile_patch_history as _persist_market_profile_patch_history_support,
    _persist_weekly_decision_evidence_history as _persist_weekly_decision_evidence_history_support,
    _persist_weekly_tuning_history as _persist_weekly_tuning_history_support,
    _resolve_labeling_summary_dir as _resolve_labeling_summary_dir_support,
    _score_alignment_score as _score_alignment_score_support,
    _select_feedback_calibration_rows as _select_feedback_calibration_rows_support,
    _table_exists as _table_exists_support,
    _column_exists as _column_exists_support,
    _weekly_strategy_note as _weekly_strategy_note_support,
    _weekly_tuning_history_trend_label,
    _apply_market_profile_tuning_context,
    _apply_execution_broker_summary_context,
    _build_weekly_calibration_patch_suggestion_rows,
)
from ..common.logger import get_logger
from ..common.markets import add_market_args, resolve_market_code
from ..common.runtime_paths import resolve_repo_path
from ..common.storage import Storage
from ..portfolio.investment_allocator import InvestmentExecutionConfig

log = get_logger("tools.review_investment_weekly")
BASE_DIR = Path(__file__).resolve().parents[2]
FEEDBACK_CALIBRATION_LOOKBACK_DAYS = 180
DEFAULT_PAPER_CFG = InvestmentPaperConfig()
DEFAULT_EXECUTION_CFG = InvestmentExecutionConfig()


def build_parser() -> argparse.ArgumentParser:
    ap = build_cli_parser(
        description="Review weekly performance for investment paper portfolios.",
        command="ibkr-quant-weekly-review",
        examples=[
            "ibkr-quant-weekly-review --market HK --days 7",
            "ibkr-quant-weekly-review --market US --portfolio_id US:market_us --out_dir reports_investment_weekly_us",
        ],
        notes=[
            "Writes weekly_review.md, weekly_review_summary.json, and the weekly CSV breakdowns under --out_dir.",
        ],
    )
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db", help="SQLite audit database used for weekly review inputs.")
    ap.add_argument("--out_dir", default="reports_investment_weekly", help="Directory for weekly review artifacts.")
    ap.add_argument("--labeling_dir", default="", help="Optional snapshot labeling output dir. Defaults to auto-detect.")
    ap.add_argument("--preflight_dir", default="reports_preflight", help="Optional preflight output dir for IBKR history probe summary.")
    ap.add_argument(
        "--feedback_thresholds_config",
        default="",
        help="Optional YAML of market-level AUTO_APPLY threshold overrides. Defaults to weekly_review out_dir override file.",
    )
    ap.add_argument("--days", type=int, default=7, help="Lookback window in days for weekly review inputs.")
    ap.add_argument("--portfolio_id", default="", help="Optional portfolio filter.")
    ap.add_argument("--include_legacy", action="store_true", default=False, help="Include legacy non-portfolio rows when present.")
    return ap


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _resolve_project_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _max_drawdown(values: List[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    for value in values:
        equity = float(value or 0.0)
        peak = max(peak, equity)
        if peak <= 0:
            continue
        dd = (equity / peak) - 1.0
        max_dd = min(max_dd, dd)
    return float(max_dd)


def _portfolio_key(row: Dict[str, Any]) -> str:
    portfolio_id = str(row.get("portfolio_id") or "").strip()
    return portfolio_id or f"LEGACY:{row.get('market', '')}"


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        data = json.loads(value)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if not isinstance(value, str) or not value:
        return []
    try:
        data = json.loads(value)
        return list(data) if isinstance(data, list) else []
    except Exception:
        return []


def _run_source(row: Dict[str, Any]) -> str:
    details = _parse_json_dict(row.get("details"))
    return str(details.get("source") or "").strip().lower()



def _load_report_data_warning(report_dir: str) -> str:
    return _load_report_data_warning_support(report_dir)


def _market_research_only_yfinance(market_code: str) -> bool:
    return _market_research_only_yfinance_support(market_code)


def _load_ibkr_history_probe_market_map(preflight_dir: Path) -> Dict[str, Dict[str, Any]]:
    return _load_ibkr_history_probe_market_map_support(preflight_dir)


def _build_market_data_gate_map(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    *,
    preflight_dir: Path,
) -> Dict[str, Dict[str, Any]]:
    return _build_market_data_gate_map_support(runs_by_portfolio, preflight_dir=preflight_dir)



def _resolve_labeling_summary_dir(path_str: str, market_filter: str) -> Path | None:
    return _resolve_labeling_summary_dir_support(BASE_DIR, path_str, market_filter)


def _build_shadow_review_order_rows(execution_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _build_shadow_review_order_rows_support(execution_orders)


def _avg_defined(values: List[Any]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None
    return float(sum(nums) / len(nums))


def _median(values: List[Any]) -> float | None:
    nums = sorted(float(v) for v in values if v is not None)
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2 == 1:
        return float(nums[mid])
    return float((nums[mid - 1] + nums[mid]) / 2.0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _seconds_between(start_ts: Any, end_ts: Any) -> float | None:
    start_dt = _parse_ts(start_ts)
    end_dt = _parse_ts(end_ts)
    if start_dt is None or end_dt is None:
        return None
    return max(0.0, float((end_dt - start_dt).total_seconds()))


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _select_feedback_calibration_rows(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], str, str]:
    return _select_feedback_calibration_rows_support(rows)


def _score_alignment_score(rows: List[Dict[str, Any]]) -> tuple[float, float]:
    return _score_alignment_score_support(rows)


def _build_feedback_calibration_rows(outcome_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _build_feedback_calibration_rows_support(outcome_rows)




def _feedback_maturity_alert_bucket(row: Dict[str, Any], *, now_dt: datetime | None = None) -> str:
    return _feedback_maturity_alert_bucket_support(row, now_dt=now_dt)


def _feedback_effect_snapshot(
    row: Dict[str, Any],
    *,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
    risk_review_map: Dict[str, Dict[str, Any]] | None = None,
    execution_feedback_map: Dict[str, Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return _feedback_effect_snapshot_support(
        row,
        feedback_calibration_map=feedback_calibration_map,
        risk_review_map=risk_review_map,
        execution_feedback_map=execution_feedback_map,
    )


def _persist_feedback_automation_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
    risk_review_map: Dict[str, Dict[str, Any]] | None = None,
    execution_feedback_map: Dict[str, Dict[str, Any]] | None = None,
) -> None:
    return _persist_feedback_automation_history_support(
        db_path,
        rows,
        week_label=week_label,
        week_start=week_start,
        window_start=window_start,
        window_end=window_end,
        feedback_calibration_map=feedback_calibration_map,
        risk_review_map=risk_review_map,
        execution_feedback_map=execution_feedback_map,
    )


def _persist_feedback_threshold_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    return _persist_feedback_threshold_history_support(
        db_path,
        rows,
        week_label=week_label,
        week_start=week_start,
        window_start=window_start,
        window_end=window_end,
    )


def _persist_market_profile_patch_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    return _persist_market_profile_patch_history_support(
        db_path,
        rows,
        week_label=week_label,
        week_start=week_start,
        window_start=window_start,
        window_end=window_end,
    )


def _persist_weekly_tuning_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    return _persist_weekly_tuning_history_support(
        db_path,
        rows,
        week_label=week_label,
        week_start=week_start,
        window_start=window_start,
        window_end=window_end,
    )


def _persist_weekly_decision_evidence_history(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    week_label: str,
    week_start: str,
    window_start: str,
    window_end: str,
) -> None:
    return _persist_weekly_decision_evidence_history_support(
        db_path,
        rows,
        week_label=week_label,
        week_start=week_start,
        window_start=window_start,
        window_end=window_end,
    )


def _build_weekly_decision_evidence_history_overview(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    return _build_weekly_decision_evidence_history_overview_support(
        db_path,
        rows,
        limit=limit,
    )



def _build_weekly_edge_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    return _build_weekly_edge_calibration_rows_support(
        db_path,
        rows,
        limit=limit,
    )


def _build_weekly_slicing_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    return _build_weekly_slicing_calibration_rows_support(
        db_path,
        rows,
        limit=limit,
    )


def _build_weekly_risk_calibration_rows(
    db_path: Path,
    rows: List[Dict[str, Any]],
    *,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    return _build_weekly_risk_calibration_rows_support(
        db_path,
        rows,
        limit=limit,
    )


def _market_profile_patch_conflict(raw: Dict[str, Any]) -> tuple[bool, str]:
    return _market_profile_patch_conflict_support(raw)


def _build_market_profile_patch_readiness(
    db_path: Path,
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return _build_market_profile_patch_readiness_support(db_path, rows)


def _build_shadow_feedback_rows(
    shadow_rows: List[Dict[str, Any]],
    shadow_summary_rows: List[Dict[str, Any]],
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    return _build_shadow_feedback_rows_support(
        shadow_rows,
        shadow_summary_rows,
        feedback_calibration_map=feedback_calibration_map,
    )


def _weekly_strategy_note(
    *,
    market_rules: Dict[str, Any],
    account_profile: Dict[str, Any],
    adaptive_strategy: Dict[str, Any],
    opportunity_summary: Dict[str, Any],
    market_sentiment: Dict[str, Any],
    strategy_effective_controls_note: str = "",
    execution_gate_summary: str = "",
) -> str:
    return _weekly_strategy_note_support(
        market_rules=market_rules,
        account_profile=account_profile,
        adaptive_strategy=adaptive_strategy,
        opportunity_summary=opportunity_summary,
        market_sentiment=market_sentiment,
        strategy_effective_controls_note=strategy_effective_controls_note,
        execution_gate_summary=execution_gate_summary,
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return _table_exists_support(conn, table)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return _column_exists_support(conn, table, column)


def _build_position_snapshots(
    position_rows: List[Dict[str, Any]],
    *,
    asof_ts: str = "",
    strict_before: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    return _build_position_snapshots_support(
        position_rows,
        asof_ts=asof_ts,
        strict_before=strict_before,
    )


def _build_latest_run_positions(
    run_rows: List[Dict[str, Any]],
    position_rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    return _build_latest_run_positions_support(run_rows, position_rows)


def _build_sector_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    return _build_sector_rows_support(latest_rows_by_portfolio, runs_by_portfolio)


def _build_holdings_change_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    baseline_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    return _build_holdings_change_rows_support(
        latest_rows_by_portfolio,
        baseline_rows_by_portfolio,
    )


def _build_reason_summary(trade_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _build_reason_summary_support(trade_rows)


def _build_equity_curve_rows(runs_by_portfolio: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    return _build_equity_curve_rows_support(runs_by_portfolio)


def _status_count_from_breakdown(text: str, status: str) -> int:
    wanted = str(status or "").strip().upper()
    if not wanted:
        return 0
    for part in str(text or "").split(","):
        name, _, value = part.partition(":")
        if str(name or "").strip().upper() != wanted:
            continue
        try:
            return int(float(value or 0))
        except Exception:
            return 0
    return 0


def _current_iso_week_label(now_dt: datetime) -> tuple[str, str]:
    iso_year, iso_week, iso_weekday = now_dt.isocalendar()
    week_start = (now_dt - timedelta(days=int(iso_weekday) - 1)).date().isoformat()
    return f"{iso_year}-W{iso_week:02d}", week_start


def _build_execution_summary_rows(
    execution_runs: List[Dict[str, Any]],
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]] | None = None,
    commission_rows: List[Dict[str, Any]] | None = None,
    *,
    week_label: str = "",
    week_start: str = "",
) -> List[Dict[str, Any]]:
    return _build_execution_summary_rows_support(
        execution_runs,
        execution_orders,
        fill_rows,
        commission_rows,
        week_label=week_label,
        week_start=week_start,
    )


def _summarize_changes(change_rows: List[Dict[str, Any]], portfolio_id: str) -> str:
    return _summarize_changes_support(change_rows, portfolio_id)


def _top_holdings_text(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    return _top_holdings_text_support(rows, limit=limit)


def _top_sector_text(rows: List[Dict[str, Any]], portfolio_id: str, limit: int = 3) -> str:
    return _top_sector_text_support(rows, portfolio_id, limit=limit)


def _market_from_portfolio_or_symbol(portfolio_id: str, symbol: str = "") -> str:
    return _market_from_portfolio_or_symbol_support(portfolio_id, symbol)


def _latest_report_dir(runs_by_portfolio: Dict[str, List[Dict[str, Any]]], portfolio_id: str) -> str:
    return _latest_report_dir_support(runs_by_portfolio, portfolio_id)


def _load_market_sentiment(report_dir: str) -> Dict[str, Any]:
    return _load_market_sentiment_support(report_dir)


def _report_json(report_dir: str, name: str) -> Dict[str, Any]:
    return _report_json_support(report_dir, name)


def _risk_overlay_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return _risk_overlay_from_history_row_support(row)


def _latest_risk_overlay(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return _latest_risk_overlay_support(rows)


def _risk_driver_and_diagnosis(row: Dict[str, Any]) -> tuple[str, str]:
    return _risk_driver_and_diagnosis_support(row)


def _build_risk_feedback_rows(
    risk_review_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]] | None = None,
    feedback_calibration_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    return _build_risk_feedback_rows_support(
        risk_review_rows,
        attribution_rows=attribution_rows,
        feedback_calibration_map=feedback_calibration_map,
    )


def _build_broker_summary_rows(
    execution_runs: List[Dict[str, Any]],
    execution_orders: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    fill_rows: List[Dict[str, Any]] | None = None,
    commission_rows: List[Dict[str, Any]] | None = None,
    *,
    week_label: str = "",
    week_start: str = "",
) -> List[Dict[str, Any]]:
    execution_summary_rows = _build_execution_summary_rows(
        execution_runs,
        execution_orders,
        fill_rows,
        commission_rows,
        week_label=week_label,
        week_start=week_start,
    )
    return _build_weekly_broker_summary_rows(
        execution_summary_rows,
        broker_latest_rows_by_portfolio,
    )


def _build_broker_local_diff_rows(
    local_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    return _build_weekly_broker_local_diff_rows(
        local_latest_rows_by_portfolio,
        broker_latest_rows_by_portfolio,
    )


def _cli_summary_payload(summary: Dict[str, Any], out_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    return _build_weekly_cli_summary_payload(summary, out_dir)


def _augment_summary_rows_with_strategy_context(
    summary_rows: List[Dict[str, Any]],
    *,
    broker_summary_rows: List[Dict[str, Any]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    return _augment_summary_rows_with_strategy_context_support(
        summary_rows,
        broker_summary_rows=broker_summary_rows,
        runs_by_portfolio=runs_by_portfolio,
        latest_report_dir_fn=_latest_report_dir,
        load_market_sentiment_fn=_load_market_sentiment,
        report_json_fn=_report_json,
    )


def _build_attribution_rows(
    summary_rows: List[Dict[str, Any]],
    *,
    sector_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]] | None = None,
    execution_gate_rows: List[Dict[str, Any]] | None = None,
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    return _build_attribution_rows_support(
        summary_rows,
        sector_rows=sector_rows,
        latest_rows_by_portfolio=latest_rows_by_portfolio,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        execution_gate_rows=execution_gate_rows,
        runs_by_portfolio=runs_by_portfolio,
        latest_report_dir_fn=_latest_report_dir,
        load_market_sentiment_fn=_load_market_sentiment,
        report_json_fn=_report_json,
    )


def _build_risk_review_rows(
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] | None = None,
) -> List[Dict[str, Any]]:
    return _build_risk_review_rows_support(
        runs_by_portfolio,
        risk_history_by_portfolio,
        risk_overlay_from_history_row_fn=_risk_overlay_from_history_row,
        latest_risk_overlay_fn=_latest_risk_overlay,
        risk_driver_and_diagnosis_fn=_risk_driver_and_diagnosis,
        mean_fn=_mean,
    )


def _build_market_profile_tuning_summary(
    strategy_context_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return _build_market_profile_tuning_summary_support(
        strategy_context_rows,
        attribution_rows,
        risk_feedback_rows,
        execution_feedback_rows,
    )


def _build_weekly_strategy_feedback_bundle(
    *,
    summary_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    sector_rows: List[Dict[str, Any]],
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    execution_gate_rows: List[Dict[str, Any]],
    execution_parent_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    feedback_calibration_map: Dict[str, Dict[str, Any]],
    db_path: Path,
    review_week_label: str,
    review_week_start: str,
    window_start: str,
    window_end: str,
) -> Dict[str, List[Dict[str, Any]]]:
    strategy_context_rows = _augment_summary_rows_with_strategy_context(
        summary_rows,
        broker_summary_rows=broker_summary_rows,
        runs_by_portfolio=runs_by_portfolio,
    )
    attribution_rows = _build_attribution_rows(
        summary_rows,
        sector_rows=sector_rows,
        latest_rows_by_portfolio=latest_rows_by_portfolio,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        execution_gate_rows=execution_gate_rows,
        runs_by_portfolio=runs_by_portfolio,
    )
    decision_evidence_rows = _build_weekly_decision_evidence_rows(
        execution_parent_rows,
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
    )
    decision_evidence_summary_rows = _build_weekly_decision_evidence_summary_rows(decision_evidence_rows)
    risk_feedback_rows = _build_risk_feedback_rows(
        risk_review_rows,
        attribution_rows=attribution_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    execution_feedback_rows = _build_execution_feedback_rows(
        attribution_rows,
        broker_summary_rows,
        execution_session_rows=execution_session_rows,
        execution_hotspot_rows=execution_hotspot_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    market_profile_tuning_rows = _build_market_profile_tuning_summary(
        strategy_context_rows,
        attribution_rows,
        risk_feedback_rows,
        execution_feedback_rows,
    )
    _persist_market_profile_patch_history(
        db_path,
        market_profile_tuning_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=window_start,
        window_end=window_end,
    )
    market_profile_patch_readiness_rows = _build_market_profile_patch_readiness(
        db_path,
        market_profile_tuning_rows,
    )
    _apply_market_profile_tuning_context(
        summary_rows,
        strategy_context_rows,
        market_profile_tuning_rows,
        market_profile_patch_readiness_rows,
    )
    return {
        "strategy_context_rows": strategy_context_rows,
        "attribution_rows": attribution_rows,
        "decision_evidence_rows": decision_evidence_rows,
        "decision_evidence_summary_rows": decision_evidence_summary_rows,
        "risk_feedback_rows": risk_feedback_rows,
        "execution_feedback_rows": execution_feedback_rows,
        "market_profile_tuning_rows": market_profile_tuning_rows,
        "market_profile_patch_readiness_rows": market_profile_patch_readiness_rows,
    }


def _build_weekly_feedback_automation_bundle(
    *,
    db_path: Path,
    shadow_feedback_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    labeling_skip_rows: List[Dict[str, Any]],
    threshold_overrides: Dict[str, Any],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
    preflight_dir: Path,
    review_week_label: str,
    review_week_start: str,
    window_start: str,
    window_end: str,
    feedback_calibration_map: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    market_data_gate_map = _build_market_data_gate_map(
        runs_by_portfolio,
        preflight_dir=preflight_dir,
    )
    feedback_automation_rows = _build_feedback_automation_rows(
        shadow_feedback_rows,
        risk_feedback_rows,
        execution_feedback_rows,
        labeling_skip_rows=labeling_skip_rows,
        threshold_overrides=threshold_overrides,
        market_data_gate_map=market_data_gate_map,
    )
    _persist_feedback_automation_history(
        db_path,
        feedback_automation_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=window_start,
        window_end=window_end,
        feedback_calibration_map=feedback_calibration_map,
        risk_review_map={
            str(row.get("portfolio_id") or ""): dict(row)
            for row in risk_review_rows
            if str(row.get("portfolio_id") or "").strip()
        },
        execution_feedback_map={
            str(row.get("portfolio_id") or ""): dict(row)
            for row in execution_feedback_rows
            if str(row.get("portfolio_id") or "").strip()
        },
    )
    feedback_automation_effect_overview_rows = _build_feedback_automation_effect_overview(db_path, feedback_automation_rows)
    feedback_effect_market_summary_rows = _build_feedback_effect_market_summary(feedback_automation_effect_overview_rows)
    feedback_threshold_suggestion_rows = _build_feedback_threshold_suggestion_rows(
        feedback_effect_market_summary_rows,
        threshold_overrides=threshold_overrides,
    )
    _persist_feedback_threshold_history(
        db_path,
        feedback_threshold_suggestion_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=window_start,
        window_end=window_end,
    )
    feedback_threshold_history_overview_rows = _build_feedback_threshold_history_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_effect_overview_rows = _build_feedback_threshold_effect_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_cohort_overview_rows = _build_feedback_threshold_cohort_overview(
        db_path,
        feedback_threshold_suggestion_rows,
    )
    feedback_threshold_trial_alert_rows = _build_feedback_threshold_trial_alert_overview(
        feedback_threshold_cohort_overview_rows,
    )
    feedback_threshold_tuning_rows = _build_feedback_threshold_tuning_summary(
        feedback_threshold_cohort_overview_rows,
    )
    return {
        "feedback_automation_rows": feedback_automation_rows,
        "feedback_automation_effect_overview_rows": feedback_automation_effect_overview_rows,
        "feedback_effect_market_summary_rows": feedback_effect_market_summary_rows,
        "feedback_threshold_suggestion_rows": feedback_threshold_suggestion_rows,
        "feedback_threshold_history_overview_rows": feedback_threshold_history_overview_rows,
        "feedback_threshold_effect_overview_rows": feedback_threshold_effect_overview_rows,
        "feedback_threshold_cohort_overview_rows": feedback_threshold_cohort_overview_rows,
        "feedback_threshold_trial_alert_rows": feedback_threshold_trial_alert_rows,
        "feedback_threshold_tuning_rows": feedback_threshold_tuning_rows,
    }


def _build_weekly_history_calibration_bundle(
    *,
    db_path: Path,
    review_week_label: str,
    review_week_start: str,
    window_start: str,
    window_end: str,
    summary_rows: List[Dict[str, Any]],
    strategy_context_rows: List[Dict[str, Any]],
    decision_evidence_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    _persist_weekly_tuning_history(
        db_path,
        weekly_tuning_dataset_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=window_start,
        window_end=window_end,
    )
    _persist_weekly_decision_evidence_history(
        db_path,
        decision_evidence_rows,
        week_label=review_week_label,
        week_start=review_week_start,
        window_start=window_start,
        window_end=window_end,
    )
    weekly_tuning_history_overview_rows = _build_weekly_tuning_history_overview(
        db_path,
        weekly_tuning_dataset_rows,
    )
    weekly_decision_evidence_history_overview_rows = _build_weekly_decision_evidence_history_overview(
        db_path,
        decision_evidence_rows,
    )
    weekly_edge_calibration_rows = _build_weekly_edge_calibration_rows(
        db_path,
        decision_evidence_rows,
    )
    weekly_slicing_calibration_rows = _build_weekly_slicing_calibration_rows(
        db_path,
        decision_evidence_rows,
    )
    weekly_risk_calibration_rows = _build_weekly_risk_calibration_rows(
        db_path,
        weekly_tuning_dataset_rows,
    )
    weekly_calibration_patch_suggestion_rows = _build_weekly_calibration_patch_suggestion_rows(
        strategy_context_rows,
        edge_calibration_rows=weekly_edge_calibration_rows,
        slicing_calibration_rows=weekly_slicing_calibration_rows,
        risk_calibration_rows=weekly_risk_calibration_rows,
    )
    weekly_patch_governance_summary_rows = _build_weekly_patch_governance_summary_rows(
        db_path,
        summary_rows,
    )
    weekly_control_timeseries_rows = _build_weekly_control_timeseries_rows(
        db_path,
        weekly_tuning_dataset_rows,
    )
    return {
        "weekly_tuning_history_overview_rows": weekly_tuning_history_overview_rows,
        "weekly_decision_evidence_history_overview_rows": weekly_decision_evidence_history_overview_rows,
        "weekly_edge_calibration_rows": weekly_edge_calibration_rows,
        "weekly_slicing_calibration_rows": weekly_slicing_calibration_rows,
        "weekly_risk_calibration_rows": weekly_risk_calibration_rows,
        "weekly_calibration_patch_suggestion_rows": weekly_calibration_patch_suggestion_rows,
        "weekly_patch_governance_summary_rows": weekly_patch_governance_summary_rows,
        "weekly_control_timeseries_rows": weekly_control_timeseries_rows,
    }


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    db_path = _resolve_project_path(args.db)
    out_dir = _resolve_project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    thresholds_config_path = (
        _resolve_project_path(str(args.feedback_thresholds_config))
        if str(args.feedback_thresholds_config or "").strip()
        else (out_dir / "weekly_feedback_threshold_overrides.yaml")
    )
    feedback_threshold_overrides = _load_feedback_threshold_overrides(thresholds_config_path)

    market_filter = resolve_market_code(getattr(args, "market", ""))
    portfolio_filter = str(args.portfolio_id or "").strip()
    since_dt = datetime.now(timezone.utc) - timedelta(days=max(1, int(args.days)))
    since_ts = since_dt.isoformat()
    feedback_calibration_since_ts = (datetime.now(timezone.utc) - timedelta(days=FEEDBACK_CALIBRATION_LOOKBACK_DAYS)).isoformat()
    include_legacy = bool(args.include_legacy)
    labeling_dir = _resolve_labeling_summary_dir(str(args.labeling_dir or ""), market_filter)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        where = ["ts >= ?"]
        params: List[Any] = [since_ts]
        if market_filter:
            where.append("market = ?")
            params.append(market_filter)
        if portfolio_filter:
            where.append("portfolio_id = ?")
            params.append(portfolio_filter)
        elif not include_legacy:
            where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
        where_sql = " AND ".join(where)

        run_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_runs WHERE {where_sql} ORDER BY ts ASC, id ASC",
            params,
        ).fetchall()]
        execution_run_rows = []
        if _table_exists(conn, "investment_execution_runs"):
            execution_run_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_execution_runs WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        risk_history_rows = []
        if _table_exists(conn, "investment_risk_history"):
            risk_history_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_risk_history WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        broker_position_rows = []
        if _table_exists(conn, "investment_broker_positions"):
            broker_position_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_broker_positions WHERE {where_sql} ORDER BY ts ASC, id ASC",
                params,
            ).fetchall()]
        trade_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_trades WHERE {where_sql} ORDER BY ts DESC, id DESC",
            params,
        ).fetchall()]
        execution_order_rows = []
        if _table_exists(conn, "investment_execution_orders"):
            execution_order_rows = [dict(row) for row in conn.execute(
                f"SELECT * FROM investment_execution_orders WHERE {where_sql} ORDER BY ts DESC, id DESC",
                params,
            ).fetchall()]
        fill_rows = []
        if _table_exists(conn, "fills"):
            fill_columns = [
                "ts",
                "order_id",
                "exec_id",
                "symbol",
                "qty",
                "price",
                "pnl",
                "actual_slippage_bps",
                "slippage_bps_deviation",
                "portfolio_id",
                "system_kind",
                "execution_run_id",
            ]
            if _column_exists(conn, "fills", "order_submit_ts"):
                fill_columns.append("order_submit_ts")
            if _column_exists(conn, "fills", "fill_delay_seconds"):
                fill_columns.append("fill_delay_seconds")
            fill_rows = [dict(row) for row in conn.execute(
                f"SELECT {', '.join(fill_columns)} FROM fills ORDER BY ts DESC, id DESC"
            ).fetchall()]
        commission_rows = []
        if _table_exists(conn, "risk_events"):
            commission_rows = [dict(row) for row in conn.execute(
                """
                SELECT ts, kind, value, exec_id, symbol, portfolio_id, system_kind, execution_run_id
                FROM risk_events
                WHERE kind='COMMISSION'
                ORDER BY ts DESC, id DESC
                """
            ).fetchall()]

        pos_where = []
        pos_params: List[Any] = []
        if market_filter:
            pos_where.append("market = ?")
            pos_params.append(market_filter)
        if portfolio_filter:
            pos_where.append("portfolio_id = ?")
            pos_params.append(portfolio_filter)
        elif not include_legacy:
            pos_where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
        pos_sql = ("WHERE " + " AND ".join(pos_where)) if pos_where else ""
        position_rows = [dict(row) for row in conn.execute(
            f"SELECT * FROM investment_positions {pos_sql} ORDER BY ts ASC, id ASC",
            pos_params,
        ).fetchall()]
        snapshot_rows = []
        if _table_exists(conn, "investment_candidate_snapshots"):
            snapshot_where = ["ts >= ?"]
            snapshot_params: List[Any] = [feedback_calibration_since_ts]
            if market_filter:
                snapshot_where.append("market = ?")
                snapshot_params.append(market_filter)
            if portfolio_filter:
                snapshot_where.append("portfolio_id = ?")
                snapshot_params.append(portfolio_filter)
            elif not include_legacy:
                snapshot_where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
            snapshot_sql = " AND ".join(snapshot_where)
            snapshot_rows = [dict(row) for row in conn.execute(
                f"""
                SELECT *
                FROM investment_candidate_snapshots
                WHERE {snapshot_sql}
                ORDER BY ts DESC, id DESC
                """,
                snapshot_params,
            ).fetchall()]
        outcome_rows = []
        if _table_exists(conn, "investment_candidate_outcomes"):
            outcome_where = ["outcome_ts >= ?"]
            outcome_params: List[Any] = [feedback_calibration_since_ts]
            if market_filter:
                outcome_where.append("market = ?")
                outcome_params.append(market_filter)
            if portfolio_filter:
                outcome_where.append("portfolio_id = ?")
                outcome_params.append(portfolio_filter)
            elif not include_legacy:
                outcome_where.append("portfolio_id IS NOT NULL AND portfolio_id != ''")
            outcome_sql = " AND ".join(outcome_where)
            outcome_rows = [dict(row) for row in conn.execute(
                f"""
                SELECT snapshot_id, market, portfolio_id, symbol, horizon_days, snapshot_ts, outcome_ts,
                       direction, future_return, max_drawdown, max_runup, outcome_label, details
                FROM investment_candidate_outcomes
                WHERE {outcome_sql}
                ORDER BY outcome_ts DESC, id DESC
                """,
                outcome_params,
            ).fetchall()]
    finally:
        conn.close()

    labeling_summary = _read_json((labeling_dir / "investment_candidate_outcomes_summary.json")) if labeling_dir else {}
    labeling_skip_rows = _read_csv((labeling_dir / "investment_candidate_outcome_skip_summary.csv")) if labeling_dir else []
    if market_filter:
        labeling_skip_rows = [row for row in labeling_skip_rows if str(row.get("market") or "").upper() == market_filter]
    if portfolio_filter:
        labeling_skip_rows = [row for row in labeling_skip_rows if str(row.get("portfolio_id") or "") == portfolio_filter]

    runs_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in run_rows:
        runs_by_portfolio.setdefault(_portfolio_key(row), []).append(row)
    risk_history_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in risk_history_rows:
        risk_history_by_portfolio.setdefault(_portfolio_key(row), []).append(row)

    latest_rows_by_portfolio = _build_latest_run_positions(run_rows, position_rows)
    baseline_rows_by_portfolio = _build_position_snapshots(position_rows, asof_ts=since_ts, strict_before=True)
    broker_after_rows = [row for row in broker_position_rows if str(row.get("source") or "").strip().lower() == "after"]
    broker_latest_rows_by_portfolio = _build_latest_run_positions(execution_run_rows, broker_after_rows)
    sector_rows = _build_sector_rows(latest_rows_by_portfolio, runs_by_portfolio)
    change_rows = _build_holdings_change_rows(latest_rows_by_portfolio, baseline_rows_by_portfolio)
    reason_rows = _build_reason_summary(trade_rows)
    equity_curve_rows = _build_equity_curve_rows(runs_by_portfolio)
    review_now = datetime.now(timezone.utc)
    review_week_label, review_week_start = _current_iso_week_label(review_now)
    execution_summary_rows = _build_execution_summary_rows(
        execution_run_rows,
        execution_order_rows,
        fill_rows,
        commission_rows,
        week_label=review_week_label,
        week_start=review_week_start,
    )
    broker_summary_rows = _build_weekly_broker_summary_rows(
        execution_summary_rows,
        broker_latest_rows_by_portfolio,
    )
    broker_diff_rows = _build_broker_local_diff_rows(latest_rows_by_portfolio, broker_latest_rows_by_portfolio)
    execution_analysis = _build_execution_analysis_bundle(
        fill_rows=fill_rows,
        commission_rows=commission_rows,
        execution_order_rows=execution_order_rows,
        execution_run_rows=execution_run_rows,
        snapshot_rows=snapshot_rows,
        outcome_rows=outcome_rows,
        broker_summary_rows=broker_summary_rows,
        since_ts=since_ts,
        portfolio_filter=portfolio_filter,
        market_filter=market_filter,
        market_from_portfolio_or_symbol_fn=_market_from_portfolio_or_symbol,
    )
    filtered_fill_rows = list(execution_analysis.get("filtered_fill_rows") or [])
    filtered_commission_rows = list(execution_analysis.get("filtered_commission_rows") or [])
    execution_effect_rows = list(execution_analysis.get("execution_effect_rows") or [])
    planned_execution_cost_rows = list(execution_analysis.get("planned_execution_cost_rows") or [])
    execution_gate_rows = list(execution_analysis.get("execution_gate_rows") or [])
    execution_parent_rows = list(execution_analysis.get("execution_parent_rows") or [])
    outcome_spread_rows = list(execution_analysis.get("outcome_spread_rows") or [])
    edge_realization_rows = list(execution_analysis.get("edge_realization_rows") or [])
    blocked_edge_attribution_rows = list(execution_analysis.get("blocked_edge_attribution_rows") or [])
    execution_session_rows = list(execution_analysis.get("execution_session_rows") or [])
    execution_hotspot_rows = list(execution_analysis.get("execution_hotspot_rows") or [])
    shadow_review_order_rows = _build_shadow_review_order_rows(execution_order_rows)
    shadow_review_summary_rows = _build_shadow_review_summary_rows(shadow_review_order_rows)
    feedback_calibration_rows = _build_feedback_calibration_rows(outcome_rows)
    feedback_calibration_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id") or ""): dict(row)
        for row in feedback_calibration_rows
        if str(row.get("portfolio_id") or "").strip()
    }
    shadow_feedback_rows = _build_shadow_feedback_rows(
        shadow_review_order_rows,
        shadow_review_summary_rows,
        feedback_calibration_map=feedback_calibration_map,
    )
    risk_review_rows = _build_risk_review_rows(
        runs_by_portfolio,
        risk_history_by_portfolio,
    )
    window_end_ts = datetime.now(timezone.utc).isoformat()

    summary_rows = _build_weekly_portfolio_summary_rows(
        runs_by_portfolio,
        trade_rows=trade_rows,
        latest_rows_by_portfolio=latest_rows_by_portfolio,
        sector_rows=sector_rows,
        change_rows=change_rows,
        run_source_fn=_run_source,
        mean_fn=_mean,
        max_drawdown_fn=_max_drawdown,
        top_holdings_fn=_top_holdings_text,
        top_sector_fn=_top_sector_text,
        summarize_changes_fn=_summarize_changes,
    )
    strategy_feedback_bundle = _build_weekly_strategy_feedback_bundle(
        summary_rows=summary_rows,
        broker_summary_rows=broker_summary_rows,
        runs_by_portfolio=runs_by_portfolio,
        sector_rows=sector_rows,
        latest_rows_by_portfolio=latest_rows_by_portfolio,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        execution_gate_rows=execution_gate_rows,
        execution_parent_rows=execution_parent_rows,
        risk_review_rows=risk_review_rows,
        execution_session_rows=execution_session_rows,
        execution_hotspot_rows=execution_hotspot_rows,
        feedback_calibration_map=feedback_calibration_map,
        db_path=db_path,
        review_week_label=review_week_label,
        review_week_start=review_week_start,
        window_start=since_ts,
        window_end=window_end_ts,
    )
    strategy_context_rows = list(strategy_feedback_bundle.get("strategy_context_rows") or [])
    attribution_rows = list(strategy_feedback_bundle.get("attribution_rows") or [])
    decision_evidence_rows = list(strategy_feedback_bundle.get("decision_evidence_rows") or [])
    decision_evidence_summary_rows = list(strategy_feedback_bundle.get("decision_evidence_summary_rows") or [])
    risk_feedback_rows = list(strategy_feedback_bundle.get("risk_feedback_rows") or [])
    execution_feedback_rows = list(strategy_feedback_bundle.get("execution_feedback_rows") or [])
    market_profile_tuning_rows = list(strategy_feedback_bundle.get("market_profile_tuning_rows") or [])
    market_profile_patch_readiness_rows = list(
        strategy_feedback_bundle.get("market_profile_patch_readiness_rows") or []
    )
    feedback_automation_bundle = _build_weekly_feedback_automation_bundle(
        db_path=db_path,
        shadow_feedback_rows=shadow_feedback_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_feedback_rows=execution_feedback_rows,
        labeling_skip_rows=labeling_skip_rows,
        threshold_overrides=feedback_threshold_overrides,
        runs_by_portfolio=runs_by_portfolio,
        preflight_dir=_resolve_project_path(str(args.preflight_dir or "reports_preflight")),
        review_week_label=review_week_label,
        review_week_start=review_week_start,
        window_start=since_ts,
        window_end=window_end_ts,
        feedback_calibration_map=feedback_calibration_map,
    )
    feedback_automation_rows = list(feedback_automation_bundle.get("feedback_automation_rows") or [])
    feedback_automation_effect_overview_rows = list(
        feedback_automation_bundle.get("feedback_automation_effect_overview_rows") or []
    )
    feedback_effect_market_summary_rows = list(
        feedback_automation_bundle.get("feedback_effect_market_summary_rows") or []
    )
    feedback_threshold_suggestion_rows = list(
        feedback_automation_bundle.get("feedback_threshold_suggestion_rows") or []
    )
    feedback_threshold_history_overview_rows = list(
        feedback_automation_bundle.get("feedback_threshold_history_overview_rows") or []
    )
    feedback_threshold_effect_overview_rows = list(
        feedback_automation_bundle.get("feedback_threshold_effect_overview_rows") or []
    )
    feedback_threshold_cohort_overview_rows = list(
        feedback_automation_bundle.get("feedback_threshold_cohort_overview_rows") or []
    )
    feedback_threshold_trial_alert_rows = list(
        feedback_automation_bundle.get("feedback_threshold_trial_alert_rows") or []
    )
    feedback_threshold_tuning_rows = list(
        feedback_automation_bundle.get("feedback_threshold_tuning_rows") or []
    )
    window_label = f"{since_dt.date().isoformat()} -> {datetime.now(timezone.utc).date().isoformat()}"
    weekly_tuning_dataset_rows = _build_weekly_tuning_dataset_rows(
        summary_rows,
        decision_evidence_rows=decision_evidence_rows,
        strategy_context_rows=strategy_context_rows,
        attribution_rows=attribution_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_rows=blocked_edge_attribution_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_feedback_rows=execution_feedback_rows,
        market_profile_tuning_rows=market_profile_tuning_rows,
        feedback_calibration_rows=feedback_calibration_rows,
        feedback_automation_rows=feedback_automation_rows,
        week_label=review_week_label,
        window_start=since_ts,
        window_end=window_end_ts,
    )
    weekly_tuning_dataset_summary = _build_weekly_tuning_dataset_summary(
        weekly_tuning_dataset_rows,
    )
    history_calibration_bundle = _build_weekly_history_calibration_bundle(
        db_path=db_path,
        review_week_label=review_week_label,
        review_week_start=review_week_start,
        window_start=since_ts,
        window_end=window_end_ts,
        summary_rows=summary_rows,
        strategy_context_rows=strategy_context_rows,
        decision_evidence_rows=decision_evidence_rows,
        weekly_tuning_dataset_rows=weekly_tuning_dataset_rows,
    )
    weekly_tuning_history_overview_rows = list(
        history_calibration_bundle.get("weekly_tuning_history_overview_rows") or []
    )
    weekly_decision_evidence_history_overview_rows = list(
        history_calibration_bundle.get("weekly_decision_evidence_history_overview_rows") or []
    )
    weekly_edge_calibration_rows = list(
        history_calibration_bundle.get("weekly_edge_calibration_rows") or []
    )
    weekly_slicing_calibration_rows = list(
        history_calibration_bundle.get("weekly_slicing_calibration_rows") or []
    )
    weekly_risk_calibration_rows = list(
        history_calibration_bundle.get("weekly_risk_calibration_rows") or []
    )
    weekly_calibration_patch_suggestion_rows = list(
        history_calibration_bundle.get("weekly_calibration_patch_suggestion_rows") or []
    )
    weekly_patch_governance_summary_rows = list(
        history_calibration_bundle.get("weekly_patch_governance_summary_rows") or []
    )
    weekly_control_timeseries_rows = list(
        history_calibration_bundle.get("weekly_control_timeseries_rows") or []
    )
    output_bundle = _build_weekly_output_bundle(
        out_dir=out_dir,
        week_label=review_week_label,
        window_start=since_ts,
        window_end=window_end_ts,
        window_label=window_label,
        market_filter=market_filter or "ALL",
        portfolio_filter=portfolio_filter or "ALL",
        thresholds_config_path=thresholds_config_path,
        summary_rows=summary_rows,
        trade_rows=trade_rows,
        change_rows=change_rows,
        sector_rows=sector_rows,
        reason_rows=reason_rows,
        equity_curve_rows=equity_curve_rows,
        broker_summary_rows=broker_summary_rows,
        execution_run_rows=execution_run_rows,
        execution_order_rows=execution_order_rows,
        shadow_review_order_rows=shadow_review_order_rows,
        shadow_review_summary_rows=shadow_review_summary_rows,
        shadow_feedback_rows=shadow_feedback_rows,
        feedback_calibration_rows=feedback_calibration_rows,
        feedback_automation_rows=feedback_automation_rows,
        feedback_automation_effect_overview_rows=feedback_automation_effect_overview_rows,
        feedback_effect_market_summary_rows=feedback_effect_market_summary_rows,
        feedback_threshold_suggestion_rows=feedback_threshold_suggestion_rows,
        feedback_threshold_history_overview_rows=feedback_threshold_history_overview_rows,
        feedback_threshold_effect_overview_rows=feedback_threshold_effect_overview_rows,
        feedback_threshold_cohort_overview_rows=feedback_threshold_cohort_overview_rows,
        feedback_threshold_trial_alert_rows=feedback_threshold_trial_alert_rows,
        feedback_threshold_tuning_rows=feedback_threshold_tuning_rows,
        labeling_summary=labeling_summary,
        labeling_skip_rows=labeling_skip_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_attribution_rows=blocked_edge_attribution_rows,
        decision_evidence_rows=decision_evidence_rows,
        decision_evidence_summary_rows=decision_evidence_summary_rows,
        weekly_decision_evidence_history_overview_rows=weekly_decision_evidence_history_overview_rows,
        execution_effect_rows=execution_effect_rows,
        planned_execution_cost_rows=planned_execution_cost_rows,
        execution_session_rows=execution_session_rows,
        execution_hotspot_rows=execution_hotspot_rows,
        attribution_rows=attribution_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_feedback_rows=execution_feedback_rows,
        market_profile_tuning_rows=market_profile_tuning_rows,
        market_profile_patch_readiness_rows=market_profile_patch_readiness_rows,
        weekly_tuning_dataset_rows=weekly_tuning_dataset_rows,
        weekly_tuning_dataset_summary=weekly_tuning_dataset_summary,
        weekly_tuning_history_overview_rows=weekly_tuning_history_overview_rows,
        weekly_edge_calibration_rows=weekly_edge_calibration_rows,
        weekly_slicing_calibration_rows=weekly_slicing_calibration_rows,
        weekly_risk_calibration_rows=weekly_risk_calibration_rows,
        weekly_calibration_patch_suggestion_rows=weekly_calibration_patch_suggestion_rows,
        weekly_patch_governance_summary_rows=weekly_patch_governance_summary_rows,
        weekly_control_timeseries_rows=weekly_control_timeseries_rows,
        broker_latest_rows_by_portfolio=broker_latest_rows_by_portfolio,
        broker_diff_rows=broker_diff_rows,
        strategy_context_rows=strategy_context_rows,
    )
    csv_artifacts = dict(output_bundle.get("csv_artifacts") or {})
    summary_payload = dict(output_bundle.get("summary_payload") or {})
    markdown_kwargs = dict(output_bundle.get("markdown_kwargs") or {})
    summary_fields = dict(output_bundle.get("summary_fields") or {})
    artifact_fields = dict(output_bundle.get("artifact_fields") or {})
    _write_weekly_csv_artifacts(out_dir, csv_artifacts)
    _write_weekly_json_artifacts(
        out_dir,
        dict(output_bundle.get("json_artifacts") or {}),
    )
    _write_weekly_markdown_artifact(out_dir, markdown_kwargs)
    emit_cli_summary(
        command="ibkr-quant-weekly-review",
        headline="weekly investment review complete",
        summary=summary_fields,
        artifacts=artifact_fields,
    )
    log.info(
        "Wrote weekly investment review -> %s portfolios=%s trades=%s changes=%s sectors=%s",
        out_dir / "weekly_review.md",
        len(summary_rows),
        len(trade_rows),
        len(change_rows),
        len(sector_rows),
    )


if __name__ == "__main__":
    main()
