from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..analysis.report import write_json
from ..common.markets import add_market_args, resolve_market_code, symbol_matches_market


UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build paper-trading KPI report from audit.db.")
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db", help="Path to audit.db.")
    ap.add_argument("--out_dir", default="reports/paper_kpi", help="Directory for KPI outputs.")
    ap.add_argument("--days", type=int, default=14, help="Lookback window in days; use 0 for all history.")
    ap.add_argument("--since", default="", help="Only include rows at or after this ISO-8601 timestamp.")
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


def _safe_float(val: Any) -> Optional[float]:
    if val in (None, "", "null"):
        return None
    try:
        return float(val)
    except Exception:
        return None


def _safe_int(val: Any) -> Optional[int]:
    if val in (None, "", "null"):
        return None
    try:
        return int(val)
    except Exception:
        return None


def _safe_json_dict(val: Any) -> Dict[str, Any]:
    if not isinstance(val, str) or not val.strip():
        return {}
    try:
        data = json.loads(val)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _infer_market(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    if sym.endswith(".HK") or sym.startswith("HK:"):
        return "HK"
    return "US"


def _parse_signal_audit_reason(reason: Any) -> Tuple[str, str, str]:
    text = str(reason or "")
    parts = text.split("|")
    if len(parts) >= 3:
        return str(parts[0] or "UNKNOWN"), str(parts[1] or "UNKNOWN"), str(parts[2] or "UNKNOWN")
    return "UNKNOWN", "UNKNOWN", "UNKNOWN"


def _parse_detail_tokens(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(text or "").split():
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        out[str(key).strip()] = str(value).strip().rstrip(",")
    return out


def _avg(nums: Iterable[Optional[float]]) -> Optional[float]:
    values = [float(x) for x in nums if x is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _med(nums: Iterable[Optional[float]]) -> Optional[float]:
    values = [float(x) for x in nums if x is not None]
    if not values:
        return None
    return median(values)


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _fmt_num(val: Any, digits: int = 2) -> str:
    if val is None:
        return "n/a"
    try:
        return f"{float(val):.{digits}f}"
    except Exception:
        return "n/a"


def _group_key(market: str, source: str) -> tuple[str, str]:
    return str(market or "UNKNOWN").upper(), str(source or "UNKNOWN").upper()


def _empty_group_row(market: str, source: str) -> Dict[str, Any]:
    return {
        "market": market,
        "source": source,
        "signal_rows": 0,
        "should_trade_rows": 0,
        "risk_allowed_rows": 0,
        "source_exec_block_rows": 0,
        "pretrade_risk_block_rows": 0,
        "entry_guard_block_rows": 0,
        "allocator_block_rows": 0,
        "allocator_qty_zero_rows": 0,
        "short_shadow_block_rows": 0,
        "short_hard_block_rows": 0,
        "parent_order_rows": 0,
        "entry_fill_rows": 0,
        "all_fill_rows": 0,
        "realized_gross_pnl": 0.0,
        "commission_total": 0.0,
        "realized_net_pnl": 0.0,
        "avg_entry_slippage_bps": None,
        "median_entry_slippage_bps": None,
        "avg_entry_slippage_bps_deviation": None,
        "median_entry_slippage_bps_deviation": None,
        "signal_to_trade_rate": None,
        "trade_to_order_rate": None,
        "order_to_fill_rate": None,
    }


def build_paper_kpi_report(db_path: str, *, market: str = "", days: int = 14, since: str = "") -> Dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    market_code = resolve_market_code(market)
    cutoff = _resolve_cutoff(days=int(days), since=since)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        commission_by_exec: Dict[str, float] = defaultdict(float)
        risk_event_rows = conn.execute(
            """
            SELECT id, ts, kind, value, details, symbol, order_id, exec_id
            FROM risk_events
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()

        for row in risk_event_rows:
            ts = _parse_ts(row["ts"])
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            if str(row["kind"] or "") != "COMMISSION":
                continue
            exec_id = str(row["exec_id"] or "")
            if exec_id:
                commission_by_exec[exec_id] += float(_safe_float(row["value"]) or 0.0)

        order_rows = conn.execute(
            """
            SELECT id, ts, symbol, order_id, parent_id, status, action, details
            FROM orders
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()
        order_meta_by_id: Dict[int, Dict[str, Any]] = {}
        for row in order_rows:
            ts = _parse_ts(row["ts"])
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            symbol = str(row["symbol"] or "").upper()
            if market_code and not symbol_matches_market(symbol, market_code):
                continue
            details_json = _safe_json_dict(row["details"])
            market_bucket = _infer_market(symbol)
            source = str(details_json.get("signal_source") or "UNKNOWN").upper()
            order_meta_by_id[int(row["order_id"])] = {
                "ts": row["ts"],
                "symbol": symbol,
                "market": market_bucket,
                "source": source,
                "tag": str(details_json.get("signal_tag") or "UNKNOWN").upper(),
                "parent_id": _safe_int(row["parent_id"]),
                "status": str(row["status"] or ""),
                "action": str(row["action"] or "").upper(),
                "leg": str(details_json.get("leg") or ""),
            }

        groups: Dict[tuple[str, str], Dict[str, Any]] = {}

        def ensure_group(market_bucket: str, source: str) -> Dict[str, Any]:
            key = _group_key(market_bucket, source)
            if key not in groups:
                groups[key] = _empty_group_row(key[0], key[1])
            return groups[key]

        signal_symbol_summary: Dict[tuple[str, str, str], Dict[str, Any]] = {}

        signal_rows = conn.execute(
            """
            SELECT ts, symbol, should_trade, risk_allowed, action, reason, channel
            FROM signals_audit
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()
        for row in signal_rows:
            ts = _parse_ts(row["ts"])
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            symbol = str(row["symbol"] or "").upper()
            if market_code and not symbol_matches_market(symbol, market_code):
                continue
            market_bucket = _infer_market(symbol)
            _tag, source, _channel = _parse_signal_audit_reason(row["reason"])
            group = ensure_group(market_bucket, source)
            group["signal_rows"] += 1
            if int(row["should_trade"] or 0) == 1:
                group["should_trade_rows"] += 1
            if int(row["should_trade"] or 0) == 1 and int(row["risk_allowed"] or 0) == 1:
                group["risk_allowed_rows"] += 1

            sym_key = (market_bucket, source, symbol)
            sym_row = signal_symbol_summary.setdefault(
                sym_key,
                {
                    "market": market_bucket,
                    "source": source,
                    "symbol": symbol,
                    "signal_rows": 0,
                    "should_trade_rows": 0,
                    "risk_allowed_rows": 0,
                    "parent_order_rows": 0,
                    "entry_fill_rows": 0,
                    "realized_net_pnl": 0.0,
                    "avg_entry_slippage_bps": None,
                },
            )
            sym_row["signal_rows"] += 1
            if int(row["should_trade"] or 0) == 1:
                sym_row["should_trade_rows"] += 1
            if int(row["should_trade"] or 0) == 1 and int(row["risk_allowed"] or 0) == 1:
                sym_row["risk_allowed_rows"] += 1

        for row in order_meta_by_id.values():
            if int(row.get("parent_id") or 0) != 0:
                continue
            group = ensure_group(str(row["market"]), str(row["source"]))
            group["parent_order_rows"] += 1
            sym_key = (str(row["market"]), str(row["source"]), str(row["symbol"]))
            if sym_key in signal_symbol_summary:
                signal_symbol_summary[sym_key]["parent_order_rows"] += 1

        entry_slippage_samples: Dict[tuple[str, str], List[Optional[float]]] = defaultdict(list)
        entry_slippage_dev_samples: Dict[tuple[str, str], List[Optional[float]]] = defaultdict(list)
        symbol_entry_slippage_samples: Dict[tuple[str, str, str], List[Optional[float]]] = defaultdict(list)

        fill_rows = conn.execute(
            """
            SELECT id, ts, order_id, exec_id, symbol, action, pnl, actual_slippage_bps, slippage_bps_deviation
            FROM fills
            ORDER BY ts ASC, id ASC
            """
        ).fetchall()
        for row in fill_rows:
            ts = _parse_ts(row["ts"])
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            order_id = int(row["order_id"] or 0)
            order_meta = order_meta_by_id.get(order_id, {})
            symbol = str(order_meta.get("symbol") or row["symbol"] or "").upper()
            if market_code and symbol and not symbol_matches_market(symbol, market_code):
                continue
            market_bucket = str(order_meta.get("market") or _infer_market(symbol))
            source = str(order_meta.get("source") or "UNKNOWN").upper()
            group = ensure_group(market_bucket, source)
            group["all_fill_rows"] += 1
            gross = float(_safe_float(row["pnl"]) or 0.0)
            commission = float(commission_by_exec.get(str(row["exec_id"] or ""), 0.0))
            group["realized_gross_pnl"] += gross
            group["commission_total"] += commission
            group["realized_net_pnl"] += gross - commission

            sym_key = (market_bucket, source, symbol)
            if sym_key in signal_symbol_summary:
                signal_symbol_summary[sym_key]["realized_net_pnl"] += gross - commission

            is_entry_fill = bool(order_meta) and int(order_meta.get("parent_id") or 0) == 0
            if is_entry_fill:
                group["entry_fill_rows"] += 1
                entry_slippage_samples[(market_bucket, source)].append(_safe_float(row["actual_slippage_bps"]))
                entry_slippage_dev_samples[(market_bucket, source)].append(_safe_float(row["slippage_bps_deviation"]))
                symbol_entry_slippage_samples[sym_key].append(_safe_float(row["actual_slippage_bps"]))
                if sym_key in signal_symbol_summary:
                    signal_symbol_summary[sym_key]["entry_fill_rows"] += 1

        for row in risk_event_rows:
            ts = _parse_ts(row["ts"])
            if cutoff is not None and ts is not None and ts < cutoff:
                continue
            kind = str(row["kind"] or "")
            if kind == "COMMISSION":
                continue
            order_meta = order_meta_by_id.get(int(row["order_id"] or 0), {})
            symbol = str(row["symbol"] or order_meta.get("symbol") or "").upper()
            if market_code and symbol and not symbol_matches_market(symbol, market_code):
                continue
            detail_tokens = _parse_detail_tokens(str(row["details"] or ""))
            market_bucket = str(order_meta.get("market") or _infer_market(symbol))
            source = str(order_meta.get("source") or detail_tokens.get("source") or "UNKNOWN").upper()
            group = ensure_group(market_bucket, source)
            if kind == "SOURCE_EXEC_BLOCK":
                group["source_exec_block_rows"] += 1
            elif kind == "PRETRADE_RISK_BLOCK":
                group["pretrade_risk_block_rows"] += 1
            elif kind == "ENTRY_GUARD_BLOCK":
                group["entry_guard_block_rows"] += 1
            elif kind == "ALLOCATOR_BLOCK":
                group["allocator_block_rows"] += 1
            elif kind == "ALLOCATOR_QTY_ZERO":
                group["allocator_qty_zero_rows"] += 1
            elif kind == "SHORT_SAFETY_SHADOW_BLOCK":
                group["short_shadow_block_rows"] += 1
            elif kind == "SHORT_SAFETY_BLOCK":
                group["short_hard_block_rows"] += 1

        group_rows: List[Dict[str, Any]] = []
        for key in sorted(groups):
            row = groups[key]
            market_bucket, source = key
            row["avg_entry_slippage_bps"] = _avg(entry_slippage_samples.get(key, []))
            row["median_entry_slippage_bps"] = _med(entry_slippage_samples.get(key, []))
            row["avg_entry_slippage_bps_deviation"] = _avg(entry_slippage_dev_samples.get(key, []))
            row["median_entry_slippage_bps_deviation"] = _med(entry_slippage_dev_samples.get(key, []))
            row["signal_to_trade_rate"] = (
                float(row["should_trade_rows"]) / float(row["signal_rows"])
                if row["signal_rows"]
                else None
            )
            row["trade_to_order_rate"] = (
                float(row["parent_order_rows"]) / float(row["should_trade_rows"])
                if row["should_trade_rows"]
                else None
            )
            row["order_to_fill_rate"] = (
                float(row["entry_fill_rows"]) / float(row["parent_order_rows"])
                if row["parent_order_rows"]
                else None
            )
            group_rows.append(dict(row))

        symbol_rows: List[Dict[str, Any]] = []
        for key, row in sorted(
            signal_symbol_summary.items(),
            key=lambda item: (-int(item[1]["should_trade_rows"]), item[1]["symbol"]),
        ):
            slippage_samples = symbol_entry_slippage_samples.get(key, [])
            row["avg_entry_slippage_bps"] = _avg(slippage_samples)
            symbol_rows.append(dict(row))

        overview = {
            "db_path": str(path),
            "market_filter": market_code or "ALL",
            "lookback_days": int(days),
            "since_utc": cutoff.isoformat() if cutoff is not None else "",
            "group_count": len(group_rows),
            "signal_rows": sum(int(row["signal_rows"]) for row in group_rows),
            "should_trade_rows": sum(int(row["should_trade_rows"]) for row in group_rows),
            "risk_allowed_rows": sum(int(row["risk_allowed_rows"]) for row in group_rows),
            "parent_order_rows": sum(int(row["parent_order_rows"]) for row in group_rows),
            "entry_fill_rows": sum(int(row["entry_fill_rows"]) for row in group_rows),
            "all_fill_rows": sum(int(row["all_fill_rows"]) for row in group_rows),
            "short_shadow_block_rows": sum(int(row["short_shadow_block_rows"]) for row in group_rows),
            "short_hard_block_rows": sum(int(row["short_hard_block_rows"]) for row in group_rows),
            "source_exec_block_rows": sum(int(row["source_exec_block_rows"]) for row in group_rows),
            "pretrade_risk_block_rows": sum(int(row["pretrade_risk_block_rows"]) for row in group_rows),
            "allocator_qty_zero_rows": sum(int(row["allocator_qty_zero_rows"]) for row in group_rows),
            "allocator_block_rows": sum(int(row["allocator_block_rows"]) for row in group_rows),
            "entry_guard_block_rows": sum(int(row["entry_guard_block_rows"]) for row in group_rows),
            "realized_net_pnl": sum(float(row["realized_net_pnl"]) for row in group_rows),
        }

        return {
            "overview": overview,
            "pipeline_by_market_source": group_rows,
            "symbol_breakdown": symbol_rows,
        }
    finally:
        conn.close()


def write_paper_kpi_outputs(out_dir: str, report: Dict[str, Any]) -> Dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    json_path = out / "paper_kpi_summary.json"
    pipeline_csv = out / "paper_kpi_pipeline.csv"
    symbol_csv = out / "paper_kpi_symbols.csv"
    md_path = out / "paper_kpi.md"

    write_json(str(json_path), report)
    _write_csv(
        pipeline_csv,
        report.get("pipeline_by_market_source", []),
        [
            "market",
            "source",
            "signal_rows",
            "should_trade_rows",
            "risk_allowed_rows",
            "source_exec_block_rows",
            "pretrade_risk_block_rows",
            "entry_guard_block_rows",
            "allocator_block_rows",
            "allocator_qty_zero_rows",
            "short_shadow_block_rows",
            "short_hard_block_rows",
            "parent_order_rows",
            "entry_fill_rows",
            "all_fill_rows",
            "avg_entry_slippage_bps",
            "median_entry_slippage_bps",
            "avg_entry_slippage_bps_deviation",
            "median_entry_slippage_bps_deviation",
            "realized_gross_pnl",
            "commission_total",
            "realized_net_pnl",
            "signal_to_trade_rate",
            "trade_to_order_rate",
            "order_to_fill_rate",
        ],
    )
    _write_csv(
        symbol_csv,
        report.get("symbol_breakdown", []),
        [
            "market",
            "source",
            "symbol",
            "signal_rows",
            "should_trade_rows",
            "risk_allowed_rows",
            "parent_order_rows",
            "entry_fill_rows",
            "avg_entry_slippage_bps",
            "realized_net_pnl",
        ],
    )

    overview = report.get("overview", {})
    pipeline_rows = report.get("pipeline_by_market_source", [])
    top_rows = pipeline_rows[:8]
    lines = [
        "# Paper Trading KPI",
        "",
        "## Overview",
        f"- market filter: {overview.get('market_filter', 'ALL')}",
        f"- since: {overview.get('since_utc') or 'ALL'}",
        f"- signal rows: {int(overview.get('signal_rows', 0) or 0)}",
        f"- should-trade rows: {int(overview.get('should_trade_rows', 0) or 0)}",
        f"- risk-allowed rows: {int(overview.get('risk_allowed_rows', 0) or 0)}",
        f"- parent orders: {int(overview.get('parent_order_rows', 0) or 0)}",
        f"- entry fills: {int(overview.get('entry_fill_rows', 0) or 0)}",
        f"- all fills: {int(overview.get('all_fill_rows', 0) or 0)}",
        f"- source exec blocks: {int(overview.get('source_exec_block_rows', 0) or 0)}",
        f"- pretrade risk blocks: {int(overview.get('pretrade_risk_block_rows', 0) or 0)}",
        f"- short shadow blocks: {int(overview.get('short_shadow_block_rows', 0) or 0)}",
        f"- allocator qty zero: {int(overview.get('allocator_qty_zero_rows', 0) or 0)}",
        f"- realized net pnl: {_fmt_num(overview.get('realized_net_pnl'))}",
        "",
        "## Market/Source",
    ]
    if not top_rows:
        lines.append("- no rows")
    else:
        for row in top_rows:
            lines.append(
                f"- {row.get('market')}/{row.get('source')}: signals={int(row.get('signal_rows', 0) or 0)} "
                f"tradeable={int(row.get('should_trade_rows', 0) or 0)} "
                f"risk_allowed={int(row.get('risk_allowed_rows', 0) or 0)} "
                f"source_block={int(row.get('source_exec_block_rows', 0) or 0)} "
                f"risk_block={int(row.get('pretrade_risk_block_rows', 0) or 0)} "
                f"orders={int(row.get('parent_order_rows', 0) or 0)} "
                f"entry_fills={int(row.get('entry_fill_rows', 0) or 0)} "
                f"shadow={int(row.get('short_shadow_block_rows', 0) or 0)} "
                f"avg_slip={_fmt_num(row.get('avg_entry_slippage_bps'))} "
                f"net_pnl={_fmt_num(row.get('realized_net_pnl'))}"
            )

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "summary_json": str(json_path),
        "pipeline_csv": str(pipeline_csv),
        "symbol_csv": str(symbol_csv),
        "markdown": str(md_path),
    }


def main() -> None:
    args = parse_args()
    report = build_paper_kpi_report(
        args.db,
        market=str(getattr(args, "market", "") or ""),
        days=int(args.days),
        since=str(getattr(args, "since", "") or ""),
    )
    outputs = write_paper_kpi_outputs(args.out_dir, report)
    overview = report.get("overview", {})
    print(
        "market_filter={market_filter} signals={signals} orders={orders} entry_fills={fills} net_pnl={net_pnl}".format(
            market_filter=str(overview.get("market_filter", "ALL")),
            signals=int(overview.get("signal_rows", 0) or 0),
            orders=int(overview.get("parent_order_rows", 0) or 0),
            fills=int(overview.get("entry_fill_rows", 0) or 0),
            net_pnl=_fmt_num(overview.get("realized_net_pnl")),
        )
    )
    for label, path in outputs.items():
        print(f"{label}={path}")


if __name__ == "__main__":
    main()
