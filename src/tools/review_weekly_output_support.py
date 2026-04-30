from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..analysis.report import write_csv, write_json
from ..common.artifact_contracts import ARTIFACT_SCHEMA_VERSION
from ..common.cli_contracts import ArtifactBundle, WeeklyReviewSummary
from ..common.markets import resolve_market_code
from .review_weekly_io import read_csv_rows as _read_csv
from .review_weekly_markdown import write_weekly_review_markdown


def _flatten_broker_positions(
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for portfolio_rows in broker_latest_rows_by_portfolio.values():
        rows.extend(list(portfolio_rows or []))
    return rows


def _rows_to_symbol_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("symbol") or "").strip(): dict(row)
        for row in list(rows or [])
        if str(row.get("symbol") or "").strip()
    }


def _top_holdings_text(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    if not rows:
        return "-"
    sorted_rows = sorted(rows, key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    parts: List[str] = []
    for row in sorted_rows[: max(1, int(limit))]:
        parts.append(
            f"{str(row.get('symbol') or '-')}:"
            f"{float(row.get('weight') or 0.0):.2f}"
        )
    return ",".join(parts) if parts else "-"


def _mean_defined(values: List[Any]) -> float:
    nums: List[float] = []
    for value in values:
        try:
            nums.append(float(value))
        except Exception:
            continue
    if not nums:
        return 0.0
    return float(sum(nums) / len(nums))


def _weekly_summary_rollup(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "avg_weekly_return": _mean_defined([row.get("weekly_return") for row in summary_rows]),
        "avg_max_drawdown": _mean_defined([row.get("max_drawdown") for row in summary_rows]),
        "buy_value_total": float(sum(float(row.get("gross_buy_value") or 0.0) for row in summary_rows)),
        "sell_value_total": float(sum(float(row.get("gross_sell_value") or 0.0) for row in summary_rows)),
        "best_portfolio": str((summary_rows[0] if summary_rows else {}).get("portfolio_id") or ""),
        "worst_portfolio": str((summary_rows[-1] if summary_rows else {}).get("portfolio_id") or ""),
    }


def build_weekly_broker_summary_rows(
    execution_summary_rows: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in list(execution_summary_rows or []):
        row = dict(raw or {})
        portfolio_id = str(row.get("portfolio_id") or "")
        holdings = broker_latest_rows_by_portfolio.get(portfolio_id, [])
        row["broker_holdings_count"] = int(len(holdings))
        row["broker_holdings_value"] = float(sum(float(h.get("market_value") or 0.0) for h in holdings))
        row["broker_top_holdings"] = _top_holdings_text(holdings)
        rows.append(row)
    return rows


def build_weekly_broker_local_diff_rows(
    local_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for portfolio_id in sorted(set(local_latest_rows_by_portfolio) | set(broker_latest_rows_by_portfolio)):
        local_rows = local_latest_rows_by_portfolio.get(portfolio_id, [])
        broker_rows = broker_latest_rows_by_portfolio.get(portfolio_id, [])
        local_map = _rows_to_symbol_map(local_rows)
        broker_map = _rows_to_symbol_map(broker_rows)
        local_symbols = set(local_map)
        broker_symbols = set(broker_map)
        local_only = sorted(local_symbols - broker_symbols)
        broker_only = sorted(broker_symbols - local_symbols)
        common = sorted(local_symbols & broker_symbols)
        rows.append(
            {
                "portfolio_id": portfolio_id,
                "market": str((local_rows or broker_rows or [{}])[0].get("market") or ""),
                "local_holdings_count": int(len(local_symbols)),
                "broker_holdings_count": int(len(broker_symbols)),
                "common_symbol_count": int(len(common)),
                "local_only_count": int(len(local_only)),
                "broker_only_count": int(len(broker_only)),
                "local_only_symbols": ",".join(local_only),
                "broker_only_symbols": ",".join(broker_only),
            }
        )
    return rows


def build_weekly_cli_summary_payload(summary: Dict[str, Any], out_dir: Path) -> tuple[Dict[str, Any], Dict[str, Path]]:
    summary_contract = WeeklyReviewSummary(
        market_filter=str(summary.get("market_filter") or "ALL"),
        portfolio_filter=str(summary.get("portfolio_filter") or "ALL"),
        portfolio_count=int(summary.get("portfolio_count") or 0),
        trade_count=int(summary.get("trade_count") or 0),
        execution_run_count=int(summary.get("execution_run_count") or 0),
        best_portfolio=str(summary.get("best_portfolio") or "-"),
        worst_portfolio=str(summary.get("worst_portfolio") or "-"),
    )
    artifacts = ArtifactBundle(
        summary_json=out_dir / "weekly_review_summary.json",
        summary_csv=out_dir / "weekly_portfolio_summary.csv",
        trade_log_csv=out_dir / "weekly_trade_log.csv",
        weekly_csv=out_dir / "weekly_tuning_dataset.csv",
        report_md=out_dir / "weekly_review.md",
    )
    return summary_contract.to_dict(), artifacts.to_dict()


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


def build_weekly_position_snapshots(
    position_rows: List[Dict[str, Any]],
    *,
    asof_ts: str = "",
    strict_before: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    latest_ts_by_portfolio: Dict[str, str] = {}
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in position_rows:
        row_ts = str(row.get("ts") or "")
        if asof_ts:
            if strict_before and row_ts >= asof_ts:
                continue
            if (not strict_before) and row_ts > asof_ts:
                continue
        portfolio_id = _portfolio_key(row)
        latest_ts = latest_ts_by_portfolio.get(portfolio_id, "")
        if not latest_ts or row_ts > latest_ts:
            latest_ts_by_portfolio[portfolio_id] = row_ts
            latest_rows_by_portfolio[portfolio_id] = [dict(row)]
        elif row_ts == latest_ts:
            latest_rows_by_portfolio.setdefault(portfolio_id, []).append(dict(row))
    return latest_rows_by_portfolio


def build_weekly_latest_run_positions(
    run_rows: List[Dict[str, Any]],
    position_rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    latest_run_id_by_portfolio: Dict[str, str] = {}
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in run_rows:
        portfolio_id = _portfolio_key(row)
        latest_run_id_by_portfolio[portfolio_id] = str(row.get("run_id") or "")
        latest_rows_by_portfolio.setdefault(portfolio_id, [])
    rows_by_run: Dict[str, List[Dict[str, Any]]] = {}
    for row in position_rows:
        run_id = str(row.get("run_id") or "")
        rows_by_run.setdefault(run_id, []).append(dict(row))
    for portfolio_id, run_id in latest_run_id_by_portfolio.items():
        latest_rows_by_portfolio[portfolio_id] = list(rows_by_run.get(run_id, []))
    return latest_rows_by_portfolio


def _load_symbol_meta(report_dir: str) -> Dict[str, Dict[str, Any]]:
    meta: Dict[str, Dict[str, Any]] = {}
    if not report_dir:
        return meta
    report_path = Path(report_dir)
    fundamentals_path = report_path / "fundamentals.json"
    if fundamentals_path.exists():
        try:
            data = json.loads(fundamentals_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for symbol, row in data.items():
                    meta[str(symbol).upper()] = dict(row or {})
        except Exception:
            pass
    for row in _read_csv(report_path / "investment_candidates.csv"):
        symbol = str(row.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        current = meta.setdefault(symbol, {})
        current.update(
            {
                "score": row.get("score"),
                "action": row.get("action"),
                "sector": row.get("sector") or current.get("sector") or "",
                "industry": row.get("industry") or current.get("industry") or "",
                "source": row.get("source") or current.get("source") or "",
            }
        )
    return meta


def build_weekly_sector_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in latest_rows_by_portfolio.items():
        latest_runs = runs_by_portfolio.get(portfolio_id, [])
        report_dir = ""
        if latest_runs:
            report_dir = str(_parse_json_dict(latest_runs[-1].get("details")).get("report_dir") or latest_runs[-1].get("report_dir") or "")
        meta_by_symbol = _load_symbol_meta(report_dir)
        sector_agg: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            weight = float(row.get("weight") or 0.0)
            market_value = float(row.get("market_value") or 0.0)
            meta = meta_by_symbol.get(symbol, {})
            sector = str(meta.get("sector") or "UNKNOWN").strip() or "UNKNOWN"
            bucket = sector_agg.setdefault(
                sector,
                {
                    "portfolio_id": portfolio_id,
                    "market": str(row.get("market") or ""),
                    "sector": sector,
                    "weight": 0.0,
                    "market_value": 0.0,
                    "symbol_count": 0,
                    "symbols": [],
                },
            )
            bucket["weight"] = float(bucket["weight"]) + weight
            bucket["market_value"] = float(bucket["market_value"]) + market_value
            bucket["symbol_count"] = int(bucket["symbol_count"]) + 1
            bucket["symbols"].append(symbol)
        for bucket in sector_agg.values():
            bucket["symbols"] = ",".join(sorted(bucket["symbols"]))
            out.append(bucket)
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), -float(row.get("weight") or 0.0), str(row.get("sector") or "")))
    return out


def build_weekly_holdings_change_rows(
    latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    baseline_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for portfolio_id in sorted(set(latest_rows_by_portfolio) | set(baseline_rows_by_portfolio)):
        latest_map = _rows_to_symbol_map(latest_rows_by_portfolio.get(portfolio_id, []))
        baseline_map = _rows_to_symbol_map(baseline_rows_by_portfolio.get(portfolio_id, []))
        symbols = sorted(set(latest_map) | set(baseline_map))
        for symbol in symbols:
            latest = latest_map.get(symbol, {})
            baseline = baseline_map.get(symbol, {})
            prev_qty = float(baseline.get("qty") or 0.0)
            latest_qty = float(latest.get("qty") or 0.0)
            prev_weight = float(baseline.get("weight") or 0.0)
            latest_weight = float(latest.get("weight") or 0.0)
            if prev_qty <= 0 and latest_qty > 0:
                change_type = "ADDED"
            elif prev_qty > 0 and latest_qty <= 0:
                change_type = "REMOVED"
            elif latest_qty > prev_qty:
                change_type = "INCREASED"
            elif latest_qty < prev_qty:
                change_type = "DECREASED"
            elif abs(latest_weight - prev_weight) > 1e-9:
                change_type = "WEIGHT_CHANGED"
            else:
                continue
            row = latest or baseline
            out.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": str(row.get("market") or ""),
                    "symbol": symbol,
                    "change_type": change_type,
                    "prev_qty": prev_qty,
                    "latest_qty": latest_qty,
                    "delta_qty": latest_qty - prev_qty,
                    "prev_weight": prev_weight,
                    "latest_weight": latest_weight,
                    "delta_weight": latest_weight - prev_weight,
                }
            )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""), str(row.get("change_type") or ""), str(row.get("symbol") or "")))
    return out


def build_weekly_reason_summary_rows(trade_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agg: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for row in trade_rows:
        key = (
            str(row.get("portfolio_id") or ""),
            str(row.get("action") or ""),
            str(row.get("reason") or ""),
        )
        bucket = agg.setdefault(
            key,
            {
                "portfolio_id": key[0],
                "market": str(row.get("market") or ""),
                "action": key[1],
                "reason": key[2],
                "trade_count": 0,
                "trade_value": 0.0,
            },
        )
        bucket["trade_count"] = int(bucket["trade_count"]) + 1
        bucket["trade_value"] = float(bucket["trade_value"]) + abs(float(row.get("trade_value") or 0.0))
    rows = list(agg.values())
    rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), -float(row.get("trade_value") or 0.0), str(row.get("reason") or "")))
    return rows


def build_weekly_equity_curve_rows(runs_by_portfolio: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for portfolio_id, runs in runs_by_portfolio.items():
        for run in runs:
            rows.append(
                {
                    "portfolio_id": portfolio_id,
                    "market": str(run.get("market") or ""),
                    "ts": str(run.get("ts") or ""),
                    "rebalance_due": int(run.get("rebalance_due") or 0),
                    "executed": int(run.get("executed") or 0),
                    "cash_before": float(run.get("cash_before") or 0.0),
                    "cash_after": float(run.get("cash_after") or 0.0),
                    "equity_before": float(run.get("equity_before") or 0.0),
                    "equity_after": float(run.get("equity_after") or 0.0),
                }
            )
    rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), str(row.get("ts") or "")))
    return rows


def build_weekly_execution_summary_rows(
    execution_runs: List[Dict[str, Any]],
    execution_orders: List[Dict[str, Any]],
    fill_rows: List[Dict[str, Any]] | None = None,
    commission_rows: List[Dict[str, Any]] | None = None,
    *,
    week_label: str = "",
    week_start: str = "",
) -> List[Dict[str, Any]]:
    runs_by_portfolio: Dict[str, List[Dict[str, Any]]] = {}
    for row in execution_runs:
        runs_by_portfolio.setdefault(_portfolio_key(row), []).append(row)

    fill_rows = list(fill_rows or [])
    commission_rows = list(commission_rows or [])
    commission_by_exec: Dict[str, float] = {}
    for row in commission_rows:
        exec_id = str(row.get("exec_id") or "").strip()
        if not exec_id:
            continue
        commission_by_exec[exec_id] = float(commission_by_exec.get(exec_id, 0.0)) + float(row.get("value") or 0.0)

    fills_by_run_order: Dict[tuple[str, int], List[Dict[str, Any]]] = {}
    fallback_fills_by_order: Dict[int, List[Dict[str, Any]]] = {}
    portfolio_fills: Dict[str, List[Dict[str, Any]]] = {}
    for row in fill_rows:
        portfolio_id = str(row.get("portfolio_id") or "").strip()
        if portfolio_id:
            portfolio_fills.setdefault(portfolio_id, []).append(row)
        order_id = int(float(row.get("order_id") or 0) or 0)
        if order_id <= 0:
            continue
        run_id = str(row.get("execution_run_id") or "").strip()
        fills_by_run_order.setdefault((run_id, order_id), []).append(row)
        fallback_fills_by_order.setdefault(order_id, []).append(row)

    out: List[Dict[str, Any]] = []
    for portfolio_id, rows in runs_by_portfolio.items():
        rows.sort(key=lambda row: str(row.get("ts") or ""))
        latest = rows[-1]
        details = _parse_json_dict(latest.get("details"))
        nested_summary = dict(details.get("summary") or {}) if isinstance(details.get("summary"), dict) else {}
        order_rows = [row for row in execution_orders if _portfolio_key(row) == portfolio_id]
        order_summary_rows: List[Dict[str, Any]] = []
        status_counts: Dict[str, int] = {}
        error_statuses: List[str] = []
        for row in order_rows:
            status = str(row.get("status") or "").strip().upper()
            if not status:
                continue
            status_counts[status] = int(status_counts.get(status, 0)) + 1
            if status.startswith("ERROR_") and status not in error_statuses:
                error_statuses.append(status)
            broker_order_id = int(float(row.get("broker_order_id") or 0) or 0)
            run_id = str(row.get("run_id") or row.get("execution_run_id") or "").strip()
            order_fills = list(fills_by_run_order.get((run_id, broker_order_id), []))
            if not order_fills and broker_order_id > 0:
                order_fills = [
                    fill for fill in fallback_fills_by_order.get(broker_order_id, [])
                    if not str(fill.get("execution_run_id") or "").strip()
                ]
            order_summary_rows.append(
                {
                    "status": status,
                    "broker_order_id": broker_order_id,
                    "has_fill_audit": int(bool(order_fills)),
                }
            )
        submitted_order_rows = int(sum(1 for row in order_summary_rows if int(row.get("broker_order_id") or 0) > 0))
        filled_order_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "") == "FILLED"))
        filled_with_audit_rows = int(sum(1 for row in order_summary_rows if int(row.get("has_fill_audit") or 0) == 1))
        blocked_opportunity_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "") == "BLOCKED_OPPORTUNITY"))
        portfolio_fill_rows = list(portfolio_fills.get(portfolio_id, []))
        slippage_values = [
            float(row.get("actual_slippage_bps") or 0.0)
            for row in portfolio_fill_rows
            if row.get("actual_slippage_bps") not in (None, "")
        ]
        realized_gross_pnl = float(sum(float(row.get("pnl") or 0.0) for row in portfolio_fill_rows))
        commission_total = float(sum(commission_by_exec.get(str(row.get("exec_id") or "").strip(), 0.0) for row in portfolio_fill_rows))
        fill_rate_status = (float(filled_order_rows) / float(submitted_order_rows)) if submitted_order_rows > 0 else None
        fill_rate_audit = (float(filled_with_audit_rows) / float(submitted_order_rows)) if submitted_order_rows > 0 else None
        out.append(
            {
                "week": str(week_label or ""),
                "week_start": str(week_start or ""),
                "portfolio_id": portfolio_id,
                "market": str(latest.get("market") or ""),
                "execution_run_rows": int(len(rows)),
                "execution_runs": int(len(rows)),
                "submitted_runs": int(sum(1 for row in rows if int(row.get("submitted") or 0) == 1)),
                "planned_order_rows": int(len(order_rows)),
                "execution_order_rows": int(len(order_rows)),
                "submitted_order_rows": submitted_order_rows,
                "filled_order_rows": filled_order_rows,
                "filled_with_audit_rows": filled_with_audit_rows,
                "blocked_opportunity_rows": blocked_opportunity_rows,
                "error_order_rows": int(sum(1 for row in order_summary_rows if str(row.get("status") or "").startswith("ERROR_"))),
                "fill_rows": int(len(portfolio_fill_rows)),
                "status_breakdown": ",".join(f"{status}:{status_counts[status]}" for status in sorted(status_counts)),
                "error_statuses": ",".join(sorted(error_statuses)),
                "planned_order_value": float(sum(abs(float(row.get("order_value") or 0.0)) for row in order_rows)),
                "commission_total": commission_total,
                "realized_gross_pnl": realized_gross_pnl,
                "realized_net_pnl": float(realized_gross_pnl - commission_total),
                "fill_rate_status": fill_rate_status,
                "fill_rate_audit": fill_rate_audit,
                "fill_rate": fill_rate_audit,
                "avg_actual_slippage_bps": _mean_defined(slippage_values) if slippage_values else None,
                "latest_gap_symbols": int(nested_summary.get("gap_symbols", 0) or 0),
                "latest_gap_notional": float(nested_summary.get("gap_notional", 0.0) or 0.0),
                "latest_broker_equity": float(latest.get("broker_equity") or 0.0),
                "latest_broker_cash": float(latest.get("broker_cash") or 0.0),
            }
        )
    out.sort(key=lambda row: (str(row.get("portfolio_id") or ""),))
    return out


def build_weekly_summarize_changes(change_rows: List[Dict[str, Any]], portfolio_id: str) -> str:
    marks = {"ADDED": "+", "REMOVED": "-", "INCREASED": "↑", "DECREASED": "↓", "WEIGHT_CHANGED": "~"}
    items = [
        f"{marks.get(str(row.get('change_type') or ''), '')}{row['symbol']}"
        for row in change_rows
        if str(row.get("portfolio_id") or "") == portfolio_id
    ]
    return ", ".join(items[:8])


def build_weekly_top_holdings_text(rows: List[Dict[str, Any]], limit: int = 5) -> str:
    if not rows:
        return "-"
    return _top_holdings_text(rows, limit=limit)


def build_weekly_top_sector_text(rows: List[Dict[str, Any]], portfolio_id: str, limit: int = 3) -> str:
    ordered = [row for row in rows if str(row.get("portfolio_id") or "") == portfolio_id]
    ordered.sort(key=lambda row: float(row.get("weight") or 0.0), reverse=True)
    return ",".join(
        f"{row['sector']}:{float(row.get('weight', 0.0) or 0.0):.2f}"
        for row in ordered[:limit]
    )


def build_weekly_market_from_portfolio_or_symbol(portfolio_id: str, symbol: str = "") -> str:
    text = str(portfolio_id or "").strip().upper()
    if ":" in text:
        return resolve_market_code(text.split(":", 1)[0])
    symbol_text = str(symbol or "").strip().upper()
    if symbol_text.endswith(".HK"):
        return "HK"
    if symbol_text.endswith(".AX"):
        return "ASX"
    if symbol_text.endswith(".DE"):
        return "XETRA"
    if symbol_text.endswith(".SS") or symbol_text.endswith(".SZ"):
        return "CN"
    return "US" if symbol_text else ""


def build_weekly_csv_artifacts(
    *,
    summary_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    change_rows: List[Dict[str, Any]],
    sector_rows: List[Dict[str, Any]],
    reason_rows: List[Dict[str, Any]],
    equity_curve_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    execution_order_rows: List[Dict[str, Any]],
    shadow_review_order_rows: List[Dict[str, Any]],
    shadow_review_summary_rows: List[Dict[str, Any]],
    shadow_feedback_rows: List[Dict[str, Any]],
    feedback_calibration_rows: List[Dict[str, Any]],
    feedback_automation_rows: List[Dict[str, Any]],
    feedback_automation_effect_overview_rows: List[Dict[str, Any]],
    feedback_effect_market_summary_rows: List[Dict[str, Any]],
    feedback_threshold_suggestion_rows: List[Dict[str, Any]],
    feedback_threshold_history_overview_rows: List[Dict[str, Any]],
    feedback_threshold_effect_overview_rows: List[Dict[str, Any]],
    feedback_threshold_cohort_overview_rows: List[Dict[str, Any]],
    feedback_threshold_trial_alert_rows: List[Dict[str, Any]],
    feedback_threshold_tuning_rows: List[Dict[str, Any]],
    labeling_skip_rows: List[Dict[str, Any]],
    outcome_spread_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
    blocked_edge_attribution_rows: List[Dict[str, Any]],
    decision_evidence_rows: List[Dict[str, Any]],
    decision_evidence_summary_rows: List[Dict[str, Any]],
    unified_evidence_rows: List[Dict[str, Any]],
    blocked_vs_allowed_expost_rows: List[Dict[str, Any]],
    candidate_model_review_rows: List[Dict[str, Any]],
    trading_quality_evidence_rows: List[Dict[str, Any]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    market_profile_tuning_rows: List[Dict[str, Any]],
    market_profile_patch_readiness_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_rows: List[Dict[str, Any]],
    weekly_tuning_history_overview_rows: List[Dict[str, Any]],
    weekly_decision_evidence_history_overview_rows: List[Dict[str, Any]],
    weekly_edge_calibration_rows: List[Dict[str, Any]],
    weekly_slicing_calibration_rows: List[Dict[str, Any]],
    weekly_risk_calibration_rows: List[Dict[str, Any]],
    weekly_calibration_patch_suggestion_rows: List[Dict[str, Any]],
    weekly_patch_governance_summary_rows: List[Dict[str, Any]],
    weekly_control_timeseries_rows: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_diff_rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "weekly_portfolio_summary.csv": summary_rows,
        "weekly_trade_log.csv": trade_rows,
        "weekly_holdings_change.csv": change_rows,
        "weekly_sector_exposure.csv": sector_rows,
        "weekly_reason_summary.csv": reason_rows,
        "weekly_equity_curve.csv": equity_curve_rows,
        "weekly_execution_summary.csv": broker_summary_rows,
        "weekly_execution_orders.csv": execution_order_rows,
        "weekly_shadow_review_orders.csv": shadow_review_order_rows,
        "weekly_shadow_review_summary.csv": shadow_review_summary_rows,
        "weekly_shadow_feedback_summary.csv": shadow_feedback_rows,
        "weekly_feedback_calibration_summary.csv": feedback_calibration_rows,
        "weekly_feedback_automation_summary.csv": feedback_automation_rows,
        "weekly_feedback_automation_effect_overview.csv": feedback_automation_effect_overview_rows,
        "weekly_feedback_effect_market_summary.csv": feedback_effect_market_summary_rows,
        "weekly_feedback_threshold_suggestion_summary.csv": feedback_threshold_suggestion_rows,
        "weekly_feedback_threshold_history_overview.csv": feedback_threshold_history_overview_rows,
        "weekly_feedback_threshold_effect_overview.csv": feedback_threshold_effect_overview_rows,
        "weekly_feedback_threshold_cohort_overview.csv": feedback_threshold_cohort_overview_rows,
        "weekly_feedback_threshold_trial_alerts.csv": feedback_threshold_trial_alert_rows,
        "weekly_feedback_threshold_tuning_summary.csv": feedback_threshold_tuning_rows,
        "weekly_outcome_labeling_skip_summary.csv": labeling_skip_rows,
        "weekly_outcome_spread_summary.csv": outcome_spread_rows,
        "weekly_edge_realization_summary.csv": edge_realization_rows,
        "weekly_blocked_edge_attribution.csv": blocked_edge_attribution_rows,
        "weekly_decision_evidence.csv": decision_evidence_rows,
        "weekly_decision_evidence_summary.csv": decision_evidence_summary_rows,
        "weekly_unified_evidence.csv": unified_evidence_rows,
        "weekly_blocked_vs_allowed_expost.csv": blocked_vs_allowed_expost_rows,
        "weekly_candidate_model_review.csv": candidate_model_review_rows,
        "weekly_trading_quality_evidence.csv": trading_quality_evidence_rows,
        "weekly_execution_effects.csv": execution_effect_rows,
        "weekly_planned_execution_costs.csv": planned_execution_cost_rows,
        "weekly_execution_session_summary.csv": execution_session_rows,
        "weekly_execution_hotspot_summary.csv": execution_hotspot_rows,
        "weekly_attribution_summary.csv": attribution_rows,
        "weekly_risk_review_summary.csv": risk_review_rows,
        "weekly_risk_feedback_summary.csv": risk_feedback_rows,
        "weekly_execution_feedback_summary.csv": execution_feedback_rows,
        "weekly_market_profile_tuning_summary.csv": market_profile_tuning_rows,
        "weekly_market_profile_patch_readiness.csv": market_profile_patch_readiness_rows,
        "weekly_tuning_dataset.csv": weekly_tuning_dataset_rows,
        "weekly_tuning_history_overview.csv": weekly_tuning_history_overview_rows,
        "weekly_decision_evidence_history_overview.csv": weekly_decision_evidence_history_overview_rows,
        "weekly_edge_calibration_summary.csv": weekly_edge_calibration_rows,
        "weekly_slicing_calibration_summary.csv": weekly_slicing_calibration_rows,
        "weekly_risk_calibration_summary.csv": weekly_risk_calibration_rows,
        "weekly_calibration_patch_suggestions.csv": weekly_calibration_patch_suggestion_rows,
        "weekly_patch_governance_summary.csv": weekly_patch_governance_summary_rows,
        "weekly_control_timeseries.csv": weekly_control_timeseries_rows,
        "weekly_broker_positions.csv": _flatten_broker_positions(broker_latest_rows_by_portfolio),
        "weekly_broker_comparison.csv": broker_diff_rows,
    }


def build_weekly_tuning_dataset_payload(
    *,
    generated_at: str,
    week_label: str,
    window_start: str,
    window_end: str,
    weekly_tuning_dataset_summary: Dict[str, Any],
    decision_evidence_summary_rows: List[Dict[str, Any]],
    trading_quality_evidence_rows: List[Dict[str, Any]],
    unified_evidence_rows: List[Dict[str, Any]],
    blocked_vs_allowed_expost_rows: List[Dict[str, Any]],
    candidate_model_review_rows: List[Dict[str, Any]],
    weekly_tuning_history_overview_rows: List[Dict[str, Any]],
    weekly_decision_evidence_history_overview_rows: List[Dict[str, Any]],
    weekly_edge_calibration_rows: List[Dict[str, Any]],
    weekly_slicing_calibration_rows: List[Dict[str, Any]],
    weekly_risk_calibration_rows: List[Dict[str, Any]],
    weekly_calibration_patch_suggestion_rows: List[Dict[str, Any]],
    weekly_patch_governance_summary_rows: List[Dict[str, Any]],
    weekly_control_timeseries_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "generated_at": str(generated_at or ""),
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "week_label": week_label,
        "window_start": window_start,
        "window_end": window_end,
        "summary": weekly_tuning_dataset_summary,
        "decision_evidence_summary": decision_evidence_summary_rows,
        "trading_quality_evidence": trading_quality_evidence_rows,
        "unified_evidence": unified_evidence_rows,
        "blocked_vs_allowed_expost_review": blocked_vs_allowed_expost_rows,
        "candidate_model_review": candidate_model_review_rows,
        "history_overview": weekly_tuning_history_overview_rows,
        "decision_evidence_history_overview": weekly_decision_evidence_history_overview_rows,
        "edge_calibration_summary": weekly_edge_calibration_rows,
        "slicing_calibration_summary": weekly_slicing_calibration_rows,
        "risk_calibration_summary": weekly_risk_calibration_rows,
        "calibration_patch_suggestions": weekly_calibration_patch_suggestion_rows,
        "patch_governance_summary": weekly_patch_governance_summary_rows,
        "control_timeseries": weekly_control_timeseries_rows,
        "rows": weekly_tuning_dataset_rows,
    }


def build_weekly_rows_artifact_payload(
    *,
    generated_at: str,
    week_label: str,
    window_start: str,
    window_end: str,
    artifact_type: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    artifact_rows = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    return {
        "generated_at": str(generated_at or ""),
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "week_label": str(week_label or ""),
        "window_start": str(window_start or ""),
        "window_end": str(window_end or ""),
        "artifact_type": str(artifact_type or ""),
        "row_count": len(artifact_rows),
        "rows": artifact_rows,
    }


def build_weekly_review_summary_payload(
    *,
    generated_at: str,
    window_start: str,
    window_end: str,
    market_filter: str,
    portfolio_filter: str,
    summary_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    execution_run_rows: List[Dict[str, Any]],
    execution_order_rows: List[Dict[str, Any]],
    shadow_review_order_rows: List[Dict[str, Any]],
    shadow_review_summary_rows: List[Dict[str, Any]],
    shadow_feedback_rows: List[Dict[str, Any]],
    feedback_calibration_rows: List[Dict[str, Any]],
    feedback_automation_rows: List[Dict[str, Any]],
    feedback_automation_effect_overview_rows: List[Dict[str, Any]],
    feedback_effect_market_summary_rows: List[Dict[str, Any]],
    feedback_threshold_suggestion_rows: List[Dict[str, Any]],
    feedback_threshold_history_overview_rows: List[Dict[str, Any]],
    feedback_threshold_effect_overview_rows: List[Dict[str, Any]],
    feedback_threshold_cohort_overview_rows: List[Dict[str, Any]],
    feedback_threshold_trial_alert_rows: List[Dict[str, Any]],
    feedback_threshold_tuning_rows: List[Dict[str, Any]],
    thresholds_config_path: Path,
    labeling_summary: Dict[str, Any],
    labeling_skip_rows: List[Dict[str, Any]],
    outcome_spread_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
    blocked_edge_attribution_rows: List[Dict[str, Any]],
    decision_evidence_summary_rows: List[Dict[str, Any]],
    decision_evidence_rows: List[Dict[str, Any]],
    trading_quality_evidence_rows: List[Dict[str, Any]],
    unified_evidence_rows: List[Dict[str, Any]],
    blocked_vs_allowed_expost_rows: List[Dict[str, Any]],
    candidate_model_review_rows: List[Dict[str, Any]],
    weekly_decision_evidence_history_overview_rows: List[Dict[str, Any]],
    weekly_edge_calibration_rows: List[Dict[str, Any]],
    weekly_slicing_calibration_rows: List[Dict[str, Any]],
    weekly_risk_calibration_rows: List[Dict[str, Any]],
    weekly_calibration_patch_suggestion_rows: List[Dict[str, Any]],
    weekly_patch_governance_summary_rows: List[Dict[str, Any]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    market_profile_tuning_rows: List[Dict[str, Any]],
    market_profile_patch_readiness_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_summary: Dict[str, Any],
    weekly_tuning_history_overview_rows: List[Dict[str, Any]],
    weekly_control_timeseries_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_rows: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_summary_rows: List[Dict[str, Any]],
    broker_diff_rows: List[Dict[str, Any]],
    avg_weekly_return: float,
    avg_max_drawdown: float,
    buy_value_total: float,
    sell_value_total: float,
    best_portfolio: str,
    worst_portfolio: str,
    strategy_context_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "generated_at": str(generated_at or ""),
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "window_start": window_start,
        "window_end": window_end,
        "market_filter": market_filter or "ALL",
        "portfolio_filter": portfolio_filter or "ALL",
        "portfolio_count": len(summary_rows),
        "trade_count": len(trade_rows),
        "execution_run_count": len(execution_run_rows),
        "execution_order_count": len(execution_order_rows),
        "shadow_review_order_count": len(shadow_review_order_rows),
        "shadow_review_portfolio_count": len(shadow_review_summary_rows),
        "shadow_review_summary": shadow_review_summary_rows,
        "shadow_feedback_summary": shadow_feedback_rows,
        "feedback_calibration_summary": feedback_calibration_rows,
        "feedback_automation_summary": feedback_automation_rows,
        "feedback_automation_effect_overview": feedback_automation_effect_overview_rows,
        "feedback_effect_market_summary": feedback_effect_market_summary_rows,
        "feedback_threshold_suggestion_summary": feedback_threshold_suggestion_rows,
        "feedback_threshold_history_overview": feedback_threshold_history_overview_rows,
        "feedback_threshold_effect_overview": feedback_threshold_effect_overview_rows,
        "feedback_threshold_cohort_overview": feedback_threshold_cohort_overview_rows,
        "feedback_threshold_trial_alerts": feedback_threshold_trial_alert_rows,
        "feedback_threshold_tuning_summary": feedback_threshold_tuning_rows,
        "feedback_thresholds_config_path": str(thresholds_config_path),
        "labeling_summary": labeling_summary,
        "labeling_skip_summary": labeling_skip_rows,
        "outcome_spread_summary": outcome_spread_rows,
        "edge_realization_summary": edge_realization_rows,
        "blocked_edge_attribution_summary": blocked_edge_attribution_rows,
        "decision_evidence_summary": decision_evidence_summary_rows,
        "decision_evidence_rows": decision_evidence_rows,
        "trading_quality_evidence": trading_quality_evidence_rows,
        "unified_evidence_rows": unified_evidence_rows,
        "blocked_vs_allowed_expost_review": blocked_vs_allowed_expost_rows,
        "candidate_model_review": candidate_model_review_rows,
        "decision_evidence_history_overview": weekly_decision_evidence_history_overview_rows,
        "edge_calibration_summary": weekly_edge_calibration_rows,
        "slicing_calibration_summary": weekly_slicing_calibration_rows,
        "risk_calibration_summary": weekly_risk_calibration_rows,
        "calibration_patch_suggestions": weekly_calibration_patch_suggestion_rows,
        "patch_governance_summary": weekly_patch_governance_summary_rows,
        "execution_effect_summary": execution_effect_rows,
        "planned_execution_cost_summary": planned_execution_cost_rows,
        "execution_session_summary": execution_session_rows,
        "execution_hotspot_summary": execution_hotspot_rows,
        "attribution_summary": attribution_rows,
        "risk_review_summary": risk_review_rows,
        "risk_feedback_summary": risk_feedback_rows,
        "execution_feedback_summary": execution_feedback_rows,
        "market_profile_tuning_summary": market_profile_tuning_rows,
        "market_profile_patch_readiness_summary": market_profile_patch_readiness_rows,
        "weekly_tuning_dataset_summary": weekly_tuning_dataset_summary,
        "weekly_tuning_history_overview": weekly_tuning_history_overview_rows,
        "weekly_control_timeseries": weekly_control_timeseries_rows,
        "weekly_tuning_dataset": weekly_tuning_dataset_rows,
        "broker_snapshot_portfolio_count": len(broker_latest_rows_by_portfolio),
        "broker_summary_rows": broker_summary_rows,
        "broker_local_diff_rows": broker_diff_rows,
        "broker_snapshot_rows": _flatten_broker_positions(broker_latest_rows_by_portfolio),
        "avg_weekly_return": float(avg_weekly_return),
        "avg_max_drawdown": float(avg_max_drawdown),
        "gross_buy_value_total": float(buy_value_total),
        "gross_sell_value_total": float(sell_value_total),
        "best_portfolio": best_portfolio,
        "worst_portfolio": worst_portfolio,
        "portfolio_strategy_context": strategy_context_rows,
    }


def build_weekly_review_markdown_kwargs(
    *,
    summary_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    broker_diff_rows: List[Dict[str, Any]],
    reason_rows: List[Dict[str, Any]],
    shadow_review_summary_rows: List[Dict[str, Any]],
    shadow_feedback_rows: List[Dict[str, Any]],
    feedback_calibration_rows: List[Dict[str, Any]],
    feedback_automation_rows: List[Dict[str, Any]],
    feedback_effect_market_summary_rows: List[Dict[str, Any]],
    feedback_threshold_suggestion_rows: List[Dict[str, Any]],
    feedback_threshold_history_overview_rows: List[Dict[str, Any]],
    feedback_threshold_effect_overview_rows: List[Dict[str, Any]],
    feedback_threshold_cohort_overview_rows: List[Dict[str, Any]],
    feedback_threshold_trial_alert_rows: List[Dict[str, Any]],
    feedback_threshold_tuning_rows: List[Dict[str, Any]],
    labeling_summary: Dict[str, Any],
    labeling_skip_rows: List[Dict[str, Any]],
    outcome_spread_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
    blocked_edge_attribution_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    weekly_control_timeseries_rows: List[Dict[str, Any]],
    window_label: str,
    decision_evidence_summary_rows: List[Dict[str, Any]],
    candidate_model_review_rows: List[Dict[str, Any]],
    weekly_decision_evidence_history_overview_rows: List[Dict[str, Any]],
    weekly_edge_calibration_rows: List[Dict[str, Any]],
    weekly_slicing_calibration_rows: List[Dict[str, Any]],
    weekly_risk_calibration_rows: List[Dict[str, Any]],
    weekly_calibration_patch_suggestion_rows: List[Dict[str, Any]],
    weekly_patch_governance_summary_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "summary_rows": summary_rows,
        "trade_rows": trade_rows,
        "broker_summary_rows": broker_summary_rows,
        "broker_diff_rows": broker_diff_rows,
        "reason_rows": reason_rows,
        "shadow_summary_rows": shadow_review_summary_rows,
        "shadow_feedback_rows": shadow_feedback_rows,
        "feedback_calibration_rows": feedback_calibration_rows,
        "feedback_automation_rows": feedback_automation_rows,
        "feedback_effect_market_summary_rows": feedback_effect_market_summary_rows,
        "feedback_threshold_suggestion_rows": feedback_threshold_suggestion_rows,
        "feedback_threshold_history_overview_rows": feedback_threshold_history_overview_rows,
        "feedback_threshold_effect_overview_rows": feedback_threshold_effect_overview_rows,
        "feedback_threshold_cohort_overview_rows": feedback_threshold_cohort_overview_rows,
        "feedback_threshold_trial_alert_rows": feedback_threshold_trial_alert_rows,
        "feedback_threshold_tuning_rows": feedback_threshold_tuning_rows,
        "labeling_summary": labeling_summary,
        "labeling_skip_rows": labeling_skip_rows,
        "outcome_spread_rows": outcome_spread_rows,
        "edge_realization_rows": edge_realization_rows,
        "blocked_edge_attribution_rows": blocked_edge_attribution_rows,
        "attribution_rows": attribution_rows,
        "risk_review_rows": risk_review_rows,
        "risk_feedback_rows": risk_feedback_rows,
        "execution_session_rows": execution_session_rows,
        "execution_hotspot_rows": execution_hotspot_rows,
        "execution_feedback_rows": execution_feedback_rows,
        "control_timeseries_rows": weekly_control_timeseries_rows,
        "window_label": window_label,
        "decision_evidence_summary_rows": decision_evidence_summary_rows,
        "candidate_model_review_rows": candidate_model_review_rows,
        "decision_evidence_history_overview_rows": weekly_decision_evidence_history_overview_rows,
        "edge_calibration_rows": weekly_edge_calibration_rows,
        "slicing_calibration_rows": weekly_slicing_calibration_rows,
        "risk_calibration_rows": weekly_risk_calibration_rows,
        "calibration_patch_suggestion_rows": weekly_calibration_patch_suggestion_rows,
        "patch_governance_rows": weekly_patch_governance_summary_rows,
    }


def build_weekly_output_bundle(
    *,
    out_dir: Path,
    week_label: str,
    window_start: str,
    window_end: str,
    window_label: str,
    market_filter: str,
    portfolio_filter: str,
    thresholds_config_path: Path,
    summary_rows: List[Dict[str, Any]],
    trade_rows: List[Dict[str, Any]],
    change_rows: List[Dict[str, Any]],
    sector_rows: List[Dict[str, Any]],
    reason_rows: List[Dict[str, Any]],
    equity_curve_rows: List[Dict[str, Any]],
    broker_summary_rows: List[Dict[str, Any]],
    execution_run_rows: List[Dict[str, Any]],
    execution_order_rows: List[Dict[str, Any]],
    shadow_review_order_rows: List[Dict[str, Any]],
    shadow_review_summary_rows: List[Dict[str, Any]],
    shadow_feedback_rows: List[Dict[str, Any]],
    feedback_calibration_rows: List[Dict[str, Any]],
    feedback_automation_rows: List[Dict[str, Any]],
    feedback_automation_effect_overview_rows: List[Dict[str, Any]],
    feedback_effect_market_summary_rows: List[Dict[str, Any]],
    feedback_threshold_suggestion_rows: List[Dict[str, Any]],
    feedback_threshold_history_overview_rows: List[Dict[str, Any]],
    feedback_threshold_effect_overview_rows: List[Dict[str, Any]],
    feedback_threshold_cohort_overview_rows: List[Dict[str, Any]],
    feedback_threshold_trial_alert_rows: List[Dict[str, Any]],
    feedback_threshold_tuning_rows: List[Dict[str, Any]],
    labeling_summary: Dict[str, Any],
    labeling_skip_rows: List[Dict[str, Any]],
    outcome_spread_rows: List[Dict[str, Any]],
    edge_realization_rows: List[Dict[str, Any]],
    blocked_edge_attribution_rows: List[Dict[str, Any]],
    decision_evidence_rows: List[Dict[str, Any]],
    decision_evidence_summary_rows: List[Dict[str, Any]],
    unified_evidence_rows: List[Dict[str, Any]],
    blocked_vs_allowed_expost_rows: List[Dict[str, Any]],
    candidate_model_review_rows: List[Dict[str, Any]],
    weekly_decision_evidence_history_overview_rows: List[Dict[str, Any]],
    trading_quality_evidence_rows: List[Dict[str, Any]],
    execution_effect_rows: List[Dict[str, Any]],
    planned_execution_cost_rows: List[Dict[str, Any]],
    execution_session_rows: List[Dict[str, Any]],
    execution_hotspot_rows: List[Dict[str, Any]],
    attribution_rows: List[Dict[str, Any]],
    risk_review_rows: List[Dict[str, Any]],
    risk_feedback_rows: List[Dict[str, Any]],
    execution_feedback_rows: List[Dict[str, Any]],
    market_profile_tuning_rows: List[Dict[str, Any]],
    market_profile_patch_readiness_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_rows: List[Dict[str, Any]],
    weekly_tuning_dataset_summary: Dict[str, Any],
    weekly_tuning_history_overview_rows: List[Dict[str, Any]],
    weekly_edge_calibration_rows: List[Dict[str, Any]],
    weekly_slicing_calibration_rows: List[Dict[str, Any]],
    weekly_risk_calibration_rows: List[Dict[str, Any]],
    weekly_calibration_patch_suggestion_rows: List[Dict[str, Any]],
    weekly_patch_governance_summary_rows: List[Dict[str, Any]],
    weekly_control_timeseries_rows: List[Dict[str, Any]],
    broker_latest_rows_by_portfolio: Dict[str, List[Dict[str, Any]]],
    broker_diff_rows: List[Dict[str, Any]],
    strategy_context_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    rollup = _weekly_summary_rollup(summary_rows)
    generated_at = datetime.now(timezone.utc).isoformat()
    csv_artifacts = build_weekly_csv_artifacts(
        summary_rows=summary_rows,
        trade_rows=trade_rows,
        change_rows=change_rows,
        sector_rows=sector_rows,
        reason_rows=reason_rows,
        equity_curve_rows=equity_curve_rows,
        broker_summary_rows=broker_summary_rows,
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
        labeling_skip_rows=labeling_skip_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_attribution_rows=blocked_edge_attribution_rows,
        decision_evidence_rows=decision_evidence_rows,
        decision_evidence_summary_rows=decision_evidence_summary_rows,
        unified_evidence_rows=unified_evidence_rows,
        blocked_vs_allowed_expost_rows=blocked_vs_allowed_expost_rows,
        candidate_model_review_rows=candidate_model_review_rows,
        trading_quality_evidence_rows=trading_quality_evidence_rows,
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
        weekly_tuning_history_overview_rows=weekly_tuning_history_overview_rows,
        weekly_decision_evidence_history_overview_rows=weekly_decision_evidence_history_overview_rows,
        weekly_edge_calibration_rows=weekly_edge_calibration_rows,
        weekly_slicing_calibration_rows=weekly_slicing_calibration_rows,
        weekly_risk_calibration_rows=weekly_risk_calibration_rows,
        weekly_calibration_patch_suggestion_rows=weekly_calibration_patch_suggestion_rows,
        weekly_patch_governance_summary_rows=weekly_patch_governance_summary_rows,
        weekly_control_timeseries_rows=weekly_control_timeseries_rows,
        broker_latest_rows_by_portfolio=broker_latest_rows_by_portfolio,
        broker_diff_rows=broker_diff_rows,
    )
    weekly_tuning_dataset_payload = build_weekly_tuning_dataset_payload(
        generated_at=generated_at,
        week_label=week_label,
        window_start=window_start,
        window_end=window_end,
        weekly_tuning_dataset_summary=weekly_tuning_dataset_summary,
        decision_evidence_summary_rows=decision_evidence_summary_rows,
        trading_quality_evidence_rows=trading_quality_evidence_rows,
        unified_evidence_rows=unified_evidence_rows,
        blocked_vs_allowed_expost_rows=blocked_vs_allowed_expost_rows,
        candidate_model_review_rows=candidate_model_review_rows,
        weekly_tuning_history_overview_rows=weekly_tuning_history_overview_rows,
        weekly_decision_evidence_history_overview_rows=weekly_decision_evidence_history_overview_rows,
        weekly_edge_calibration_rows=weekly_edge_calibration_rows,
        weekly_slicing_calibration_rows=weekly_slicing_calibration_rows,
        weekly_risk_calibration_rows=weekly_risk_calibration_rows,
        weekly_calibration_patch_suggestion_rows=weekly_calibration_patch_suggestion_rows,
        weekly_patch_governance_summary_rows=weekly_patch_governance_summary_rows,
        weekly_control_timeseries_rows=weekly_control_timeseries_rows,
        weekly_tuning_dataset_rows=weekly_tuning_dataset_rows,
    )
    weekly_unified_evidence_payload = build_weekly_rows_artifact_payload(
        generated_at=generated_at,
        week_label=week_label,
        window_start=window_start,
        window_end=window_end,
        artifact_type="weekly_unified_evidence",
        rows=unified_evidence_rows,
    )
    weekly_blocked_vs_allowed_expost_payload = build_weekly_rows_artifact_payload(
        generated_at=generated_at,
        week_label=week_label,
        window_start=window_start,
        window_end=window_end,
        artifact_type="weekly_blocked_vs_allowed_expost",
        rows=blocked_vs_allowed_expost_rows,
    )
    summary_payload = build_weekly_review_summary_payload(
        generated_at=generated_at,
        window_start=window_start,
        window_end=window_end,
        market_filter=market_filter or "ALL",
        portfolio_filter=portfolio_filter or "ALL",
        summary_rows=summary_rows,
        trade_rows=trade_rows,
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
        thresholds_config_path=thresholds_config_path,
        labeling_summary=labeling_summary,
        labeling_skip_rows=labeling_skip_rows,
        outcome_spread_rows=outcome_spread_rows,
        edge_realization_rows=edge_realization_rows,
        blocked_edge_attribution_rows=blocked_edge_attribution_rows,
        decision_evidence_summary_rows=decision_evidence_summary_rows,
        decision_evidence_rows=decision_evidence_rows,
        trading_quality_evidence_rows=trading_quality_evidence_rows,
        unified_evidence_rows=unified_evidence_rows,
        blocked_vs_allowed_expost_rows=blocked_vs_allowed_expost_rows,
        candidate_model_review_rows=candidate_model_review_rows,
        weekly_decision_evidence_history_overview_rows=weekly_decision_evidence_history_overview_rows,
        weekly_edge_calibration_rows=weekly_edge_calibration_rows,
        weekly_slicing_calibration_rows=weekly_slicing_calibration_rows,
        weekly_risk_calibration_rows=weekly_risk_calibration_rows,
        weekly_calibration_patch_suggestion_rows=weekly_calibration_patch_suggestion_rows,
        weekly_patch_governance_summary_rows=weekly_patch_governance_summary_rows,
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
        weekly_tuning_dataset_summary=weekly_tuning_dataset_summary,
        weekly_tuning_history_overview_rows=weekly_tuning_history_overview_rows,
        weekly_control_timeseries_rows=weekly_control_timeseries_rows,
        weekly_tuning_dataset_rows=weekly_tuning_dataset_rows,
        broker_latest_rows_by_portfolio=broker_latest_rows_by_portfolio,
        broker_summary_rows=broker_summary_rows,
        broker_diff_rows=broker_diff_rows,
        avg_weekly_return=float(rollup.get("avg_weekly_return") or 0.0),
        avg_max_drawdown=float(rollup.get("avg_max_drawdown") or 0.0),
        buy_value_total=float(rollup.get("buy_value_total") or 0.0),
        sell_value_total=float(rollup.get("sell_value_total") or 0.0),
        best_portfolio=str(rollup.get("best_portfolio") or ""),
        worst_portfolio=str(rollup.get("worst_portfolio") or ""),
        strategy_context_rows=strategy_context_rows,
    )
    markdown_kwargs = build_weekly_review_markdown_kwargs(
        summary_rows=summary_rows,
        trade_rows=trade_rows,
        broker_summary_rows=broker_summary_rows,
        broker_diff_rows=broker_diff_rows,
        reason_rows=reason_rows,
        shadow_review_summary_rows=shadow_review_summary_rows,
        shadow_feedback_rows=shadow_feedback_rows,
        feedback_calibration_rows=feedback_calibration_rows,
        feedback_automation_rows=feedback_automation_rows,
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
        attribution_rows=attribution_rows,
        risk_review_rows=risk_review_rows,
        risk_feedback_rows=risk_feedback_rows,
        execution_session_rows=execution_session_rows,
        execution_hotspot_rows=execution_hotspot_rows,
        execution_feedback_rows=execution_feedback_rows,
        weekly_control_timeseries_rows=weekly_control_timeseries_rows,
        window_label=window_label,
        decision_evidence_summary_rows=decision_evidence_summary_rows,
        candidate_model_review_rows=candidate_model_review_rows,
        weekly_decision_evidence_history_overview_rows=weekly_decision_evidence_history_overview_rows,
        weekly_edge_calibration_rows=weekly_edge_calibration_rows,
        weekly_slicing_calibration_rows=weekly_slicing_calibration_rows,
        weekly_risk_calibration_rows=weekly_risk_calibration_rows,
        weekly_calibration_patch_suggestion_rows=weekly_calibration_patch_suggestion_rows,
        weekly_patch_governance_summary_rows=weekly_patch_governance_summary_rows,
    )
    summary_fields, artifact_fields = build_weekly_cli_summary_payload(summary_payload, out_dir)
    return {
        "csv_artifacts": csv_artifacts,
        "json_artifacts": {
            "weekly_unified_evidence.json": weekly_unified_evidence_payload,
            "weekly_blocked_vs_allowed_expost.json": weekly_blocked_vs_allowed_expost_payload,
            "weekly_tuning_dataset.json": weekly_tuning_dataset_payload,
            "weekly_review_summary.json": summary_payload,
        },
        "markdown_kwargs": markdown_kwargs,
        "summary_payload": summary_payload,
        "weekly_tuning_dataset_payload": weekly_tuning_dataset_payload,
        "weekly_unified_evidence_payload": weekly_unified_evidence_payload,
        "weekly_blocked_vs_allowed_expost_payload": weekly_blocked_vs_allowed_expost_payload,
        "summary_fields": summary_fields,
        "artifact_fields": artifact_fields,
    }


def write_weekly_csv_artifacts(
    out_dir: Path,
    csv_artifacts: Dict[str, List[Dict[str, Any]]],
) -> None:
    for filename, rows in csv_artifacts.items():
        write_csv(str(out_dir / filename), rows)


def write_weekly_json_artifacts(
    out_dir: Path,
    json_artifacts: Dict[str, Dict[str, Any]],
) -> None:
    for filename, payload in json_artifacts.items():
        write_json(str(out_dir / filename), payload)


def write_weekly_markdown_artifact(
    out_dir: Path,
    markdown_kwargs: Dict[str, Any],
) -> None:
    write_weekly_review_markdown(out_dir / "weekly_review.md", **markdown_kwargs)
