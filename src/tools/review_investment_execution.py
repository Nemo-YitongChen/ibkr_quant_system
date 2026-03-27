from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from ..analysis.report import write_csv, write_json
from ..common.logger import get_logger
from ..common.markets import add_market_args, resolve_market_code, symbol_matches_market

log = get_logger("tools.review_investment_execution")
UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Review broker-backed investment execution KPI from audit.db.")
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--out_dir", default="reports_investment_execution")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--since", default="")
    ap.add_argument("--portfolio_id", default="")
    return ap.parse_args()


def _parse_ts(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _resolve_cutoff(*, days: int, since: str = "") -> Optional[datetime]:
    since_text = str(since or "").strip()
    if since_text:
        cutoff = _parse_ts(since_text)
        if cutoff is None:
            raise ValueError(f"invalid --since timestamp: {since_text}")
        return cutoff
    return None if int(days) <= 0 else datetime.now(UTC) - timedelta(days=int(days))


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _mean(values: Iterable[float]) -> Optional[float]:
    rows = [float(v) for v in values]
    if not rows:
        return None
    return float(sum(rows) / len(rows))


def _median(values: Iterable[float]) -> Optional[float]:
    rows = [float(v) for v in values]
    if not rows:
        return None
    return float(median(rows))


def _ratio(numerator: Any, denominator: Any) -> Optional[float]:
    num = _safe_float(numerator)
    den = _safe_float(denominator)
    if den <= 0:
        return None
    return float(num / den)


def _week_bucket(raw_ts: Any) -> tuple[str, str]:
    dt = _parse_ts(raw_ts)
    if dt is None:
        return "", ""
    iso = dt.isocalendar()
    week_start = datetime.fromisocalendar(int(iso.year), int(iso.week), 1).date().isoformat()
    return f"{int(iso.year)}-W{int(iso.week):02d}", week_start


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


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (str(table),),
    ).fetchone()
    return bool(row)


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except Exception:
        return False
    wanted = str(column or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == wanted for row in rows)


def _normalize_symbol(symbol: str, *, market: str = "") -> str:
    sym = str(symbol or "").upper().strip()
    mkt = str(market or "").upper().strip()
    if not sym:
        return sym
    if sym.endswith(".HK"):
        base = sym[:-3].strip()
        return f"{int(base):04d}.HK" if base.isdigit() else sym
    if mkt == "HK":
        return f"{int(sym):04d}.HK" if sym.isdigit() else f"{sym}.HK"
    if " " in sym:
        parts = sym.split()
        if len(parts) == 2 and len(parts[1]) == 1 and parts[1].isalpha():
            return f"{parts[0]}.{parts[1]}"
    return sym


def _status_breakdown(rows: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = defaultdict(int)
    for row in rows:
        status = str(row.get("status") or "").upper().strip()
        if status:
            counts[status] += 1
    return ",".join(f"{status}:{counts[status]}" for status in sorted(counts))


def _latest_after_positions(
    broker_positions: List[Dict[str, Any]],
    *,
    run_id: str = "",
) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in broker_positions if str(row.get("source") or "").lower() == "after"]
    if run_id:
        rows = [row for row in rows if str(row.get("run_id") or "") == str(run_id)]
    return sorted(rows, key=lambda row: (str(row.get("symbol") or ""), str(row.get("ts") or "")))


def build_investment_execution_report(
    db_path: str,
    *,
    market: str = "",
    days: int = 30,
    since: str = "",
    portfolio_id: str = "",
) -> Dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    market_code = resolve_market_code(market)
    cutoff = _resolve_cutoff(days=int(days), since=since)
    wanted_portfolio = str(portfolio_id or "").strip()

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if not _table_exists(conn, "investment_execution_runs"):
            raise RuntimeError("investment_execution_runs table is missing")

        run_rows_raw = conn.execute(
            """
            SELECT run_id, ts, market, portfolio_id, account_id, report_dir, submitted, order_count,
                   order_value, broker_equity, broker_cash, target_equity, details
            FROM investment_execution_runs
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()

        execution_runs: List[Dict[str, Any]] = []
        for raw in run_rows_raw:
            row = dict(raw)
            ts = _parse_ts(row.get("ts"))
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            row_market = str(row.get("market") or "").upper()
            row_portfolio = str(row.get("portfolio_id") or "").strip()
            if market_code and row_market != market_code:
                continue
            if wanted_portfolio and row_portfolio != wanted_portfolio:
                continue
            execution_runs.append(row)

        run_ids = {str(row.get("run_id") or "") for row in execution_runs if str(row.get("run_id") or "").strip()}

        execution_orders: List[Dict[str, Any]] = []
        if _table_exists(conn, "investment_execution_orders"):
            has_execution_intent = _column_exists(conn, "investment_execution_orders", "execution_intent_json")
            select_execution_intent = ", execution_intent_json" if has_execution_intent else ""
            rows = conn.execute(
                f"""
                SELECT run_id, ts, market, portfolio_id, symbol, action, current_qty, target_qty, delta_qty,
                       ref_price, target_weight, order_value, order_type, broker_order_id, status, reason{select_execution_intent}, details
                FROM investment_execution_orders
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
            for raw in rows:
                row = dict(raw)
                if run_ids and str(row.get("run_id") or "") not in run_ids:
                    continue
                row["symbol"] = _normalize_symbol(str(row.get("symbol") or ""), market=str(row.get("market") or ""))
                execution_orders.append(row)

        broker_positions: List[Dict[str, Any]] = []
        if _table_exists(conn, "investment_broker_positions"):
            rows = conn.execute(
                """
                SELECT run_id, ts, market, portfolio_id, symbol, qty, avg_cost, market_price,
                       market_value, weight, source, details
                FROM investment_broker_positions
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
            for raw in rows:
                row = dict(raw)
                if run_ids and str(row.get("run_id") or "") not in run_ids:
                    continue
                row_market = str(row.get("market") or "").upper().strip()
                row_portfolio = str(row.get("portfolio_id") or "").strip()
                if market_code and row_market and row_market != market_code:
                    continue
                if wanted_portfolio and row_portfolio and row_portfolio != wanted_portfolio:
                    continue
                row["symbol"] = _normalize_symbol(str(row.get("symbol") or ""), market=row_market)
                filter_market = market_code or row_market
                if filter_market and not symbol_matches_market(str(row.get("symbol") or ""), filter_market):
                    continue
                broker_positions.append(row)

        generic_orders: Dict[int, Dict[str, Any]] = {}
        if _table_exists(conn, "orders"):
            rows = conn.execute(
                """
                SELECT ts, account_id, symbol, exchange, currency, action, qty, order_type, order_id,
                       parent_id, status, details, portfolio_id, system_kind, execution_run_id
                FROM orders
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
            for raw in rows:
                row = dict(raw)
                order_id = _safe_int(row.get("order_id"))
                if order_id <= 0:
                    continue
                if str(row.get("system_kind") or "") != "investment_execution" and str(row.get("execution_run_id") or "") not in run_ids:
                    continue
                if run_ids and str(row.get("execution_run_id") or "") not in run_ids:
                    continue
                row["symbol"] = _normalize_symbol(
                    str(row.get("symbol") or ""),
                    market=str(row.get("currency") or "").upper() == "HKD" and "HK" or str(row.get("market") or ""),
                )
                generic_orders[order_id] = row

        order_ids = {int(row.get("broker_order_id") or 0) for row in execution_orders if int(row.get("broker_order_id") or 0) > 0}

        fills: List[Dict[str, Any]] = []
        exec_ids: set[str] = set()
        if _table_exists(conn, "fills"):
            rows = conn.execute(
                """
                SELECT ts, order_id, exec_id, symbol, action, qty, price, pnl, details,
                       expected_price, expected_slippage_bps, actual_slippage_bps, slippage_bps_deviation,
                       portfolio_id, system_kind, execution_run_id
                FROM fills
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
            for raw in rows:
                row = dict(raw)
                if str(row.get("system_kind") or "") != "investment_execution" and str(row.get("execution_run_id") or "") not in run_ids:
                    if _safe_int(row.get("order_id")) not in order_ids:
                        continue
                if run_ids and str(row.get("execution_run_id") or "") not in run_ids and _safe_int(row.get("order_id")) not in order_ids:
                    continue
                market_hint = ""
                order_meta = generic_orders.get(_safe_int(row.get("order_id")))
                if order_meta:
                    market_hint = "HK" if str(order_meta.get("currency") or "").upper() == "HKD" else str(order_meta.get("market") or "")
                row["symbol"] = _normalize_symbol(str(row.get("symbol") or ""), market=market_hint)
                fills.append(row)
                exec_id = str(row.get("exec_id") or "").strip()
                if exec_id:
                    exec_ids.add(exec_id)

        risk_events: List[Dict[str, Any]] = []
        commission_by_exec: Dict[str, float] = defaultdict(float)
        if _table_exists(conn, "risk_events"):
            rows = conn.execute(
                """
                SELECT ts, kind, value, details, symbol, order_id, exec_id, expected_price, actual_price,
                       expected_slippage_bps, actual_slippage_bps, slippage_bps_deviation,
                       portfolio_id, system_kind, execution_run_id
                FROM risk_events
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
            for raw in rows:
                row = dict(raw)
                ts = _parse_ts(row.get("ts"))
                if cutoff is not None and ts is not None and ts < cutoff:
                    continue
                kind = str(row.get("kind") or "").upper().strip()
                order_id = _safe_int(row.get("order_id"))
                exec_id = str(row.get("exec_id") or "").strip()
                run_id = str(row.get("execution_run_id") or "").strip()
                if kind not in {"COMMISSION", "EXECUTION_SLIPPAGE_BPS", "INVESTMENT_ORDER_ERROR"}:
                    continue
                if run_ids and run_id not in run_ids and order_id not in order_ids and exec_id not in exec_ids:
                    continue
                risk_events.append(row)
                if kind == "COMMISSION" and exec_id:
                    commission_by_exec[exec_id] += _safe_float(row.get("value"))

        fills_by_order: Dict[tuple[str, int], List[Dict[str, Any]]] = defaultdict(list)
        fallback_fills_by_order: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for row in fills:
            order_id = _safe_int(row.get("order_id"))
            run_id = str(row.get("execution_run_id") or "")
            fills_by_order[(run_id, order_id)].append(row)
            fallback_fills_by_order[order_id].append(row)

        order_summary_rows: List[Dict[str, Any]] = []
        for row in execution_orders:
            run_id = str(row.get("run_id") or "")
            broker_order_id = _safe_int(row.get("broker_order_id"))
            order_fills = list(fills_by_order.get((run_id, broker_order_id), []))
            if not order_fills and broker_order_id > 0:
                order_fills = [
                    fill for fill in fallback_fills_by_order.get(broker_order_id, [])
                    if not str(fill.get("execution_run_id") or "").strip()
                ]
            fill_qty = sum(_safe_float(fill.get("qty")) for fill in order_fills)
            fill_trade_value = sum(_safe_float(fill.get("qty")) * _safe_float(fill.get("price")) for fill in order_fills)
            commissions = [commission_by_exec.get(str(fill.get("exec_id") or ""), 0.0) for fill in order_fills]
            slippage_values = [_safe_float(fill.get("actual_slippage_bps")) for fill in order_fills if fill.get("actual_slippage_bps") not in (None, "")]
            slippage_dev_values = [_safe_float(fill.get("slippage_bps_deviation")) for fill in order_fills if fill.get("slippage_bps_deviation") not in (None, "")]
            generic_status = str(generic_orders.get(broker_order_id, {}).get("status") or "")
            status = str(generic_status or row.get("status") or "").upper()
            execution_intent = _parse_json_dict(row.get("execution_intent_json"))
            opportunity_status = str(
                execution_intent.get("opportunity_status")
                or _parse_json_dict(row.get("details")).get("opportunity_status")
                or ""
            ).upper()
            opportunity_reason = str(
                execution_intent.get("opportunity_reason")
                or _parse_json_dict(row.get("details")).get("opportunity_reason")
                or ""
            )
            order_summary_rows.append(
                {
                    "run_id": str(row.get("run_id") or ""),
                    "ts": str(row.get("ts") or ""),
                    "market": str(row.get("market") or ""),
                    "portfolio_id": str(row.get("portfolio_id") or ""),
                    "symbol": str(row.get("symbol") or ""),
                    "action": str(row.get("action") or ""),
                    "broker_order_id": broker_order_id,
                    "status": status,
                    "reason": str(row.get("reason") or ""),
                    "execution_intent_json": str(row.get("execution_intent_json") or ""),
                    "opportunity_status": opportunity_status,
                    "opportunity_reason": opportunity_reason,
                    "current_qty": _safe_float(row.get("current_qty")),
                    "target_qty": _safe_float(row.get("target_qty")),
                    "delta_qty": _safe_float(row.get("delta_qty")),
                    "ref_price": _safe_float(row.get("ref_price")),
                    "target_weight": _safe_float(row.get("target_weight")),
                    "order_value": _safe_float(row.get("order_value")),
                    "has_fill_audit": int(bool(order_fills)),
                    "fill_count": len(order_fills),
                    "filled_qty": fill_qty,
                    "filled_trade_value": fill_trade_value,
                    "avg_fill_price": (fill_trade_value / fill_qty) if fill_qty > 0 else None,
                    "commission_total": float(sum(commissions)),
                    "avg_actual_slippage_bps": _mean(slippage_values),
                    "median_actual_slippage_bps": _median(slippage_values),
                    "avg_slippage_bps_deviation": _mean(slippage_dev_values),
                    "median_slippage_bps_deviation": _median(slippage_dev_values),
                    "first_fill_ts": str(order_fills[0].get("ts") or "") if order_fills else "",
                    "last_fill_ts": str(order_fills[-1].get("ts") or "") if order_fills else "",
                }
            )

        run_summary_rows: List[Dict[str, Any]] = []
        latest_run_id = ""
        latest_run_ts = ""
        for row in execution_runs:
            run_id = str(row.get("run_id") or "")
            row_ts = str(row.get("ts") or "")
            if row_ts >= latest_run_ts:
                latest_run_ts = row_ts
                latest_run_id = run_id
            details = _parse_json_dict(row.get("details"))
            nested_summary = dict(details.get("summary") or {}) if isinstance(details.get("summary"), dict) else {}
            run_orders = [item for item in order_summary_rows if str(item.get("run_id") or "") == run_id]
            run_after_positions = _latest_after_positions(broker_positions, run_id=run_id)
            slippage_values = [float(item["avg_actual_slippage_bps"]) for item in run_orders if item.get("avg_actual_slippage_bps") is not None]
            slippage_dev_values = [float(item["avg_slippage_bps_deviation"]) for item in run_orders if item.get("avg_slippage_bps_deviation") is not None]
            run_summary_rows.append(
                {
                    "run_id": run_id,
                    "ts": row_ts,
                    "market": str(row.get("market") or ""),
                    "portfolio_id": str(row.get("portfolio_id") or ""),
                    "submitted": _safe_int(row.get("submitted")),
                    "account_id": str(row.get("account_id") or ""),
                    "report_dir": str(row.get("report_dir") or ""),
                    "planned_order_rows": len(run_orders),
                    "submitted_order_rows": int(sum(1 for item in run_orders if int(item.get("broker_order_id") or 0) > 0)),
                    "filled_order_rows": int(sum(1 for item in run_orders if str(item.get("status") or "").upper() == "FILLED")),
                    "filled_with_audit_rows": int(sum(1 for item in run_orders if int(item.get("has_fill_audit") or 0) == 1)),
                    "error_order_rows": int(sum(1 for item in run_orders if str(item.get("status") or "").upper().startswith("ERROR_"))),
                    "status_breakdown": _status_breakdown(run_orders),
                    "order_value": _safe_float(row.get("order_value")),
                    "filled_trade_value": float(sum(_safe_float(item.get("filled_trade_value")) for item in run_orders)),
                    "commission_total": float(sum(_safe_float(item.get("commission_total")) for item in run_orders)),
                    "avg_actual_slippage_bps": _mean(slippage_values),
                    "median_actual_slippage_bps": _median(slippage_values),
                    "avg_slippage_bps_deviation": _mean(slippage_dev_values),
                    "median_slippage_bps_deviation": _median(slippage_dev_values),
                    "gap_symbols": _safe_int(nested_summary.get("gap_symbols", row.get("order_count"))),
                    "gap_notional": _safe_float(nested_summary.get("gap_notional")),
                    "broker_equity": _safe_float(row.get("broker_equity")),
                    "broker_cash": _safe_float(row.get("broker_cash")),
                    "target_equity": _safe_float(row.get("target_equity")),
                    "broker_holdings_count_after": len(run_after_positions),
                    "broker_holdings_value_after": float(sum(_safe_float(item.get("market_value")) for item in run_after_positions)),
                }
            )

        latest_positions = _latest_after_positions(broker_positions, run_id=latest_run_id)
        symbol_summary_map: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in order_summary_rows:
            key = (str(row.get("market") or ""), str(row.get("portfolio_id") or ""), str(row.get("symbol") or ""))
            bucket = symbol_summary_map.setdefault(
                key,
                {
                    "market": key[0],
                    "portfolio_id": key[1],
                    "symbol": key[2],
                    "order_rows": 0,
                    "filled_order_rows": 0,
                    "buy_order_rows": 0,
                    "sell_order_rows": 0,
                    "planned_order_value": 0.0,
                    "filled_trade_value": 0.0,
                    "commission_total": 0.0,
                    "net_pnl_total": 0.0,
                    "avg_actual_slippage_bps": None,
                    "median_actual_slippage_bps": None,
                },
            )
            bucket["order_rows"] = int(bucket["order_rows"]) + 1
            bucket["filled_order_rows"] = int(bucket["filled_order_rows"]) + int(str(row.get("status") or "").upper() == "FILLED")
            bucket["buy_order_rows"] = int(bucket["buy_order_rows"]) + int(str(row.get("action") or "").upper() == "BUY")
            bucket["sell_order_rows"] = int(bucket["sell_order_rows"]) + int(str(row.get("action") or "").upper() == "SELL")
            bucket["planned_order_value"] = float(bucket["planned_order_value"]) + _safe_float(row.get("order_value"))
            bucket["filled_trade_value"] = float(bucket["filled_trade_value"]) + _safe_float(row.get("filled_trade_value"))
            bucket["commission_total"] = float(bucket["commission_total"]) + _safe_float(row.get("commission_total"))

        pnl_by_symbol: Dict[tuple[str, str, str], float] = defaultdict(float)
        slippage_by_symbol: Dict[tuple[str, str, str], List[float]] = defaultdict(list)
        for row in fills:
            key = (
                str(generic_orders.get(_safe_int(row.get("order_id")), {}).get("market") or market_code or ""),
                str(row.get("portfolio_id") or generic_orders.get(_safe_int(row.get("order_id")), {}).get("portfolio_id") or ""),
                str(row.get("symbol") or ""),
            )
            pnl_by_symbol[key] += _safe_float(row.get("pnl")) - commission_by_exec.get(str(row.get("exec_id") or ""), 0.0)
            if row.get("actual_slippage_bps") not in (None, ""):
                slippage_by_symbol[key].append(_safe_float(row.get("actual_slippage_bps")))

        symbol_summary_rows: List[Dict[str, Any]] = []
        for key, bucket in symbol_summary_map.items():
            slippage_values = slippage_by_symbol.get(key, [])
            bucket["net_pnl_total"] = float(pnl_by_symbol.get(key, 0.0))
            bucket["avg_actual_slippage_bps"] = _mean(slippage_values)
            bucket["median_actual_slippage_bps"] = _median(slippage_values)
            symbol_summary_rows.append(bucket)
        symbol_summary_rows.sort(key=lambda row: (str(row.get("portfolio_id") or ""), -float(row.get("planned_order_value") or 0.0), str(row.get("symbol") or "")))

        fill_rows = list(fills)
        weekly_map: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}

        def _weekly_bucket(week_label: str, week_start: str, market_value: str, portfolio_value: str) -> Dict[str, Any]:
            return weekly_map.setdefault(
                (week_label, week_start, market_value, portfolio_value),
                {
                    "week": week_label,
                    "week_start": week_start,
                    "market": market_value,
                    "portfolio_id": portfolio_value,
                    "execution_run_rows": 0,
                    "submitted_runs": 0,
                    "planned_order_rows": 0,
                    "submitted_order_rows": 0,
                    "filled_order_rows": 0,
                    "filled_with_audit_rows": 0,
                    "blocked_opportunity_rows": 0,
                    "error_order_rows": 0,
                    "fill_rows": 0,
                    "commission_total": 0.0,
                    "realized_gross_pnl": 0.0,
                    "realized_net_pnl": 0.0,
                    "avg_actual_slippage_bps": None,
                    "median_actual_slippage_bps": None,
                    "avg_slippage_bps_deviation": None,
                    "median_slippage_bps_deviation": None,
                    "latest_broker_equity": 0.0,
                    "latest_broker_cash": 0.0,
                    "latest_broker_holdings_count": 0,
                    "_slippage": [],
                    "_slippage_dev": [],
                    "_latest_ts": "",
                },
            )

        for row in run_summary_rows:
            week_label, week_start = _week_bucket(row.get("ts"))
            if not week_label:
                continue
            bucket = _weekly_bucket(
                week_label,
                week_start,
                str(row.get("market") or market_code or "ALL"),
                str(row.get("portfolio_id") or wanted_portfolio or "ALL"),
            )
            bucket["execution_run_rows"] = int(bucket["execution_run_rows"]) + 1
            bucket["submitted_runs"] = int(bucket["submitted_runs"]) + _safe_int(row.get("submitted"))
            row_ts = str(row.get("ts") or "")
            if row_ts >= str(bucket["_latest_ts"] or ""):
                bucket["_latest_ts"] = row_ts
                bucket["latest_broker_equity"] = _safe_float(row.get("broker_equity"))
                bucket["latest_broker_cash"] = _safe_float(row.get("broker_cash"))
                bucket["latest_broker_holdings_count"] = _safe_int(row.get("broker_holdings_count_after"))

        for row in order_summary_rows:
            week_label, week_start = _week_bucket(row.get("ts"))
            if not week_label:
                continue
            bucket = _weekly_bucket(
                week_label,
                week_start,
                str(row.get("market") or market_code or "ALL"),
                str(row.get("portfolio_id") or wanted_portfolio or "ALL"),
            )
            bucket["planned_order_rows"] = int(bucket["planned_order_rows"]) + 1
            bucket["submitted_order_rows"] = int(bucket["submitted_order_rows"]) + int(_safe_int(row.get("broker_order_id")) > 0)
            status = str(row.get("status") or "").upper()
            bucket["filled_order_rows"] = int(bucket["filled_order_rows"]) + int(status == "FILLED")
            bucket["filled_with_audit_rows"] = int(bucket["filled_with_audit_rows"]) + int(int(row.get("has_fill_audit") or 0) == 1)
            bucket["blocked_opportunity_rows"] = int(bucket["blocked_opportunity_rows"]) + int(status == "BLOCKED_OPPORTUNITY")
            bucket["error_order_rows"] = int(bucket["error_order_rows"]) + int(status.startswith("ERROR_"))
            if row.get("avg_actual_slippage_bps") is not None:
                bucket["_slippage"].append(_safe_float(row.get("avg_actual_slippage_bps")))
            if row.get("avg_slippage_bps_deviation") is not None:
                bucket["_slippage_dev"].append(_safe_float(row.get("avg_slippage_bps_deviation")))

        for row in fill_rows:
            week_label, week_start = _week_bucket(row.get("ts"))
            if not week_label:
                continue
            fill_market = market_code or "ALL"
            fill_portfolio = wanted_portfolio or "ALL"
            matching_order = next(
                (
                    order_row
                    for order_row in order_summary_rows
                    if _safe_int(order_row.get("broker_order_id")) > 0
                    and _safe_int(order_row.get("broker_order_id")) == _safe_int(row.get("order_id"))
                ),
                None,
            )
            if matching_order:
                fill_market = str(matching_order.get("market") or fill_market)
                fill_portfolio = str(matching_order.get("portfolio_id") or fill_portfolio)
            bucket = _weekly_bucket(week_label, week_start, fill_market, fill_portfolio)
            bucket["fill_rows"] = int(bucket["fill_rows"]) + 1
            bucket["realized_gross_pnl"] = float(bucket["realized_gross_pnl"]) + _safe_float(row.get("pnl"))
            commission = commission_by_exec.get(str(row.get("exec_id") or ""), 0.0)
            bucket["commission_total"] = float(bucket["commission_total"]) + commission
            bucket["realized_net_pnl"] = float(bucket["realized_net_pnl"]) + (_safe_float(row.get("pnl")) - commission)

        weekly_rows: List[Dict[str, Any]] = []
        for key in sorted(weekly_map.keys()):
            bucket = weekly_map[key]
            slippage_values = list(bucket.pop("_slippage", []))
            slippage_dev_values = list(bucket.pop("_slippage_dev", []))
            bucket["avg_actual_slippage_bps"] = _mean(slippage_values)
            bucket["median_actual_slippage_bps"] = _median(slippage_values)
            bucket["avg_slippage_bps_deviation"] = _mean(slippage_dev_values)
            bucket["median_slippage_bps_deviation"] = _median(slippage_dev_values)
            bucket.pop("_latest_ts", None)
            weekly_rows.append(bucket)
        for row in weekly_rows:
            row["fill_rate_status"] = _ratio(row.get("filled_order_rows"), row.get("submitted_order_rows"))
            row["fill_rate_audit"] = _ratio(row.get("filled_with_audit_rows"), row.get("submitted_order_rows"))
            # Backward-compatible alias: prefer the audit-backed fill rate when available.
            row["fill_rate"] = row.get("fill_rate_audit")

        latest_run = next((row for row in reversed(run_summary_rows) if str(row.get("run_id") or "") == latest_run_id), None)
        filled_order_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "").upper() == "FILLED"))
        filled_with_audit_rows = int(sum(1 for row in order_summary_rows if int(row.get("has_fill_audit") or 0) == 1))
        submitted_order_rows = int(sum(1 for row in order_summary_rows if int(row.get("broker_order_id") or 0) > 0))
        blocked_opportunity_rows = int(sum(1 for row in order_summary_rows if str(row.get("status") or "").upper() == "BLOCKED_OPPORTUNITY"))
        error_rows = [row for row in order_summary_rows if str(row.get("status") or "").upper().startswith("ERROR_")]
        opportunity_status_counts: Dict[str, int] = defaultdict(int)
        for row in order_summary_rows:
            status = str(row.get("opportunity_status") or "").upper().strip()
            if status:
                opportunity_status_counts[status] += 1
        slippage_values = [_safe_float(row.get("actual_slippage_bps")) for row in fill_rows if row.get("actual_slippage_bps") not in (None, "")]
        slippage_dev_values = [_safe_float(row.get("slippage_bps_deviation")) for row in fill_rows if row.get("slippage_bps_deviation") not in (None, "")]
        total_commission = float(sum(commission_by_exec.get(str(row.get("exec_id") or ""), 0.0) for row in fill_rows))
        gross_pnl = float(sum(_safe_float(row.get("pnl")) for row in fill_rows))
        net_pnl = gross_pnl - total_commission

        summary = {
            "generated_at": datetime.now(UTC).isoformat(),
            "market": market_code or "ALL",
            "portfolio_id": wanted_portfolio or "ALL",
            "cutoff": cutoff.isoformat() if cutoff is not None else "",
            "execution_run_rows": len(execution_runs),
            "submitted_runs": int(sum(_safe_int(row.get("submitted")) for row in execution_runs)),
            "planned_order_rows": len(order_summary_rows),
            "submitted_order_rows": submitted_order_rows,
            "filled_order_rows": filled_order_rows,
            "filled_with_audit_rows": filled_with_audit_rows,
            "fill_rows": len(fill_rows),
            "fill_audit_gap_rows": max(0, filled_order_rows - filled_with_audit_rows),
            "blocked_opportunity_rows": blocked_opportunity_rows,
            "error_order_rows": len(error_rows),
            "fill_rate_status": _ratio(filled_order_rows, submitted_order_rows),
            "fill_rate_audit": _ratio(filled_with_audit_rows, submitted_order_rows),
            "fill_rate": _ratio(filled_with_audit_rows, submitted_order_rows),
            "planned_order_value": float(sum(_safe_float(row.get("order_value")) for row in order_summary_rows)),
            "filled_trade_value": float(sum(_safe_float(row.get("qty")) * _safe_float(row.get("price")) for row in fill_rows)),
            "commission_total": total_commission,
            "realized_gross_pnl": gross_pnl,
            "realized_net_pnl": net_pnl,
            "avg_actual_slippage_bps": _mean(slippage_values),
            "median_actual_slippage_bps": _median(slippage_values),
            "avg_slippage_bps_deviation": _mean(slippage_dev_values),
            "median_slippage_bps_deviation": _median(slippage_dev_values),
            "latest_gap_symbols": int((latest_run or {}).get("gap_symbols") or 0),
            "latest_gap_notional": float((latest_run or {}).get("gap_notional") or 0.0),
            "latest_broker_equity": float((latest_run or {}).get("broker_equity") or 0.0),
            "latest_broker_cash": float((latest_run or {}).get("broker_cash") or 0.0),
            "latest_broker_holdings_count": len(latest_positions),
            "latest_broker_holdings_value": float(sum(_safe_float(row.get("market_value")) for row in latest_positions)),
            "latest_status_breakdown": str((latest_run or {}).get("status_breakdown") or ""),
            "error_statuses": ",".join(sorted({str(row.get("status") or "").upper() for row in error_rows})),
            "opportunity_status_breakdown": ",".join(
                f"{status}:{opportunity_status_counts[status]}" for status in sorted(opportunity_status_counts)
            ),
            "weekly_rows": len(weekly_rows),
        }

        return {
            "summary": summary,
            "run_rows": run_summary_rows,
            "weekly_rows": weekly_rows,
            "order_rows": order_summary_rows,
            "fill_rows": fill_rows,
            "risk_event_rows": risk_events,
            "latest_broker_positions": latest_positions,
            "symbol_rows": symbol_summary_rows,
        }
    finally:
        conn.close()


def _write_md(path: Path, report: Dict[str, Any]) -> None:
    summary = dict(report.get("summary") or {})
    run_rows = list(report.get("run_rows") or [])
    order_rows = list(report.get("order_rows") or [])
    fill_rows = list(report.get("fill_rows") or [])
    symbol_rows = list(report.get("symbol_rows") or [])
    broker_rows = list(report.get("latest_broker_positions") or [])
    weekly_rows = list(report.get("weekly_rows") or [])

    lines = [
        "# Investment Execution KPI",
        "",
        f"- Generated: {summary.get('generated_at', '')}",
        f"- Market: {summary.get('market', '')}",
        f"- Portfolio: {summary.get('portfolio_id', '')}",
        f"- Cutoff: {summary.get('cutoff', '') or 'all history'}",
        f"- Execution runs: {int(summary.get('execution_run_rows', 0) or 0)}",
        f"- Submitted runs: {int(summary.get('submitted_runs', 0) or 0)}",
        f"- Planned orders: {int(summary.get('planned_order_rows', 0) or 0)}",
        f"- Submitted orders: {int(summary.get('submitted_order_rows', 0) or 0)}",
        f"- Filled orders (status): {int(summary.get('filled_order_rows', 0) or 0)}",
        f"- Filled orders (audit): {int(summary.get('filled_with_audit_rows', 0) or 0)}",
        f"- Fill rows: {int(summary.get('fill_rows', 0) or 0)}",
        f"- Fill audit gap rows: {int(summary.get('fill_audit_gap_rows', 0) or 0)}",
        f"- Blocked by opportunity: {int(summary.get('blocked_opportunity_rows', 0) or 0)}",
        f"- Error orders: {int(summary.get('error_order_rows', 0) or 0)}",
        f"- Fill rate (status): {summary.get('fill_rate_status') if summary.get('fill_rate_status') is not None else 'n/a'}",
        f"- Fill rate (audit): {summary.get('fill_rate_audit') if summary.get('fill_rate_audit') is not None else 'n/a'}",
        f"- Planned order value: {float(summary.get('planned_order_value', 0.0) or 0.0):.2f}",
        f"- Filled trade value: {float(summary.get('filled_trade_value', 0.0) or 0.0):.2f}",
        f"- Commission total: {float(summary.get('commission_total', 0.0) or 0.0):.4f}",
        f"- Realized gross PnL: {float(summary.get('realized_gross_pnl', 0.0) or 0.0):.4f}",
        f"- Realized net PnL: {float(summary.get('realized_net_pnl', 0.0) or 0.0):.4f}",
        f"- Avg slippage bps: {summary.get('avg_actual_slippage_bps') if summary.get('avg_actual_slippage_bps') is not None else 'n/a'}",
        f"- Median slippage bps: {summary.get('median_actual_slippage_bps') if summary.get('median_actual_slippage_bps') is not None else 'n/a'}",
        f"- Latest gap symbols: {int(summary.get('latest_gap_symbols', 0) or 0)}",
        f"- Latest gap notional: {float(summary.get('latest_gap_notional', 0.0) or 0.0):.2f}",
        f"- Latest broker holdings: {int(summary.get('latest_broker_holdings_count', 0) or 0)}",
        f"- Latest broker holdings value: {float(summary.get('latest_broker_holdings_value', 0.0) or 0.0):.2f}",
        f"- Latest status breakdown: {summary.get('latest_status_breakdown', '') or 'n/a'}",
        f"- Opportunity status breakdown: {summary.get('opportunity_status_breakdown', '') or 'n/a'}",
        f"- Error statuses: {summary.get('error_statuses', '') or 'n/a'}",
        f"- Weekly KPI rows: {int(summary.get('weekly_rows', 0) or 0)}",
        "",
        "## Latest Broker Positions",
    ]
    if not broker_rows:
        lines.append("- (no positions)")
    else:
        for row in broker_rows:
            lines.append(
                f"- {row.get('symbol', '')} qty={float(row.get('qty') or 0.0):.2f} "
                f"weight={float(row.get('weight') or 0.0):.4f} "
                f"market_value={float(row.get('market_value') or 0.0):.2f}"
            )
    lines.append("")
    lines.append("## Weekly KPI")
    if not weekly_rows:
        lines.append("- (no weekly rows)")
    else:
        for row in weekly_rows[-8:]:
            lines.append(
                f"- {row.get('week', '')} runs={int(row.get('execution_run_rows') or 0)} "
                f"submitted_orders={int(row.get('submitted_order_rows') or 0)} "
                f"filled_status={int(row.get('filled_order_rows') or 0)} "
                f"filled_audit={int(row.get('filled_with_audit_rows') or 0)} "
                f"blocked={int(row.get('blocked_opportunity_rows') or 0)} "
                f"net_pnl={float(row.get('realized_net_pnl') or 0.0):.4f} "
                f"commission={float(row.get('commission_total') or 0.0):.4f}"
            )
    lines.append("")
    lines.append("## Latest Runs")
    if not run_rows:
        lines.append("- (no execution runs)")
    else:
        for row in run_rows[-5:]:
            lines.append(
                f"- {row.get('ts', '')} run={row.get('run_id', '')} submitted={int(row.get('submitted') or 0)} "
                f"orders={int(row.get('planned_order_rows') or 0)} "
                f"filled_status={int(row.get('filled_order_rows') or 0)} "
                f"filled_audit={int(row.get('filled_with_audit_rows') or 0)} "
                f"gap={int(row.get('gap_symbols') or 0)} status={row.get('status_breakdown', '') or 'n/a'}"
            )
    lines.append("")
    lines.append("## Recent Orders")
    if not order_rows:
        lines.append("- (no orders)")
    else:
        for row in order_rows[-10:]:
            lines.append(
                f"- {row.get('symbol', '')} {row.get('action', '')} status={row.get('status', '')} "
                f"qty={float(row.get('delta_qty') or 0.0):.2f} filled_qty={float(row.get('filled_qty') or 0.0):.2f} "
                f"order_value={float(row.get('order_value') or 0.0):.2f} commission={float(row.get('commission_total') or 0.0):.4f}"
            )
            if row.get("opportunity_status") or row.get("opportunity_reason"):
                lines.append(
                    f"  opportunity={row.get('opportunity_status') or 'N/A'} {str(row.get('opportunity_reason') or '').strip()}".rstrip()
                )
    lines.append("")
    lines.append("## Recent Fills")
    if not fill_rows:
        lines.append("- (no fills)")
    else:
        for row in fill_rows[-10:]:
            lines.append(
                f"- {row.get('ts', '')} {row.get('symbol', '')} {row.get('action', '')} "
                f"qty={float(row.get('qty') or 0.0):.2f} price={float(row.get('price') or 0.0):.4f} "
                f"pnl={float(row.get('pnl') or 0.0):.4f} slippage={row.get('actual_slippage_bps', 'n/a')}"
            )
    lines.append("")
    lines.append("## Symbol Summary")
    if not symbol_rows:
        lines.append("- (no symbol rows)")
    else:
        for row in symbol_rows[:10]:
            lines.append(
                f"- {row.get('symbol', '')} orders={int(row.get('order_rows') or 0)} "
                f"filled={int(row.get('filled_order_rows') or 0)} planned={float(row.get('planned_order_value') or 0.0):.2f} "
                f"net_pnl={float(row.get('net_pnl_total') or 0.0):.4f}"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    report = build_investment_execution_report(
        args.db,
        market=getattr(args, "market", ""),
        days=int(args.days),
        since=str(args.since or ""),
        portfolio_id=str(args.portfolio_id or ""),
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(str(out_dir / "investment_execution_summary.json"), report["summary"])
    write_csv(str(out_dir / "investment_execution_runs.csv"), report["run_rows"])
    write_csv(str(out_dir / "investment_execution_weekly_summary.csv"), report["weekly_rows"])
    write_csv(str(out_dir / "investment_execution_orders.csv"), report["order_rows"])
    write_csv(str(out_dir / "investment_execution_fills.csv"), report["fill_rows"])
    write_csv(str(out_dir / "investment_execution_risk_events.csv"), report["risk_event_rows"])
    write_csv(str(out_dir / "investment_execution_latest_broker_positions.csv"), report["latest_broker_positions"])
    write_csv(str(out_dir / "investment_execution_symbols.csv"), report["symbol_rows"])
    _write_md(out_dir / "investment_execution_kpi.md", report)

    summary = report["summary"]
    print(
        f"market={summary.get('market')} portfolio={summary.get('portfolio_id')} "
        f"runs={summary.get('execution_run_rows')} orders={summary.get('planned_order_rows')} "
        f"fills={summary.get('fill_rows')} net_pnl={float(summary.get('realized_net_pnl', 0.0) or 0.0):.2f}"
    )
    print(f"summary_json={out_dir / 'investment_execution_summary.json'}")
    print(f"runs_csv={out_dir / 'investment_execution_runs.csv'}")
    print(f"weekly_csv={out_dir / 'investment_execution_weekly_summary.csv'}")
    print(f"orders_csv={out_dir / 'investment_execution_orders.csv'}")
    print(f"fills_csv={out_dir / 'investment_execution_fills.csv'}")
    print(f"markdown={out_dir / 'investment_execution_kpi.md'}")
    log.info("Wrote investment execution KPI -> %s runs=%s orders=%s fills=%s", out_dir / "investment_execution_kpi.md", len(report["run_rows"]), len(report["order_rows"]), len(report["fill_rows"]))


if __name__ == "__main__":
    main()
