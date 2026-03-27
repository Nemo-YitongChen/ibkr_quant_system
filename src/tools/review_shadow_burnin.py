from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

from ..analysis.report import write_json


UTC = timezone.utc


@dataclass
class ShadowEvent:
    id: int
    ts: datetime
    ts_raw: str
    kind: str
    symbol: str
    details: str
    expected_price: Optional[float]
    expected_slippage_bps: Optional[float]
    event_risk_reason: str
    short_borrow_source: str
    risk_snapshot: Dict[str, Any]
    blocked_reasons: List[str]


@dataclass
class EntryFill:
    id: int
    ts: datetime
    ts_raw: str
    order_id: int
    exec_id: str
    symbol: str
    fill_action: str
    order_action: str
    parent_id: Optional[int]
    qty: float
    price: float
    pnl: Optional[float]
    expected_price: Optional[float]
    expected_slippage_bps: Optional[float]
    actual_slippage_bps: Optional[float]
    slippage_bps_deviation: Optional[float]
    event_risk_reason: str
    short_borrow_source: str
    risk_snapshot: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Review shadow burn-in quality from audit.db fills and risk_events.")
    ap.add_argument("--db", default="audit.db", help="Path to audit.db.")
    ap.add_argument("--out_dir", default="reports/shadow_burnin", help="Directory for markdown/json/csv outputs.")
    ap.add_argument("--days", type=int, default=14, help="Lookback days; use 0 to scan all history.")
    ap.add_argument("--match_window_min", type=int, default=30, help="Minutes to match a shadow block to a later short-entry fill.")
    ap.add_argument("--min_sample", type=int, default=10, help="Minimum matched shadow fills before considering hard-block review.")
    ap.add_argument("--slippage_delta_bps_threshold", type=float, default=5.0, help="Shadow-vs-control avg slippage delta threshold.")
    ap.add_argument("--pnl_delta_threshold", type=float, default=0.0, help="Shadow-vs-control avg pnl delta threshold.")
    return ap.parse_args()


def _parse_ts(raw: Any) -> datetime:
    text = str(raw or "").strip()
    if not text:
        return datetime.now(UTC)
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


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
        out = json.loads(val)
    except Exception:
        return {}
    return out if isinstance(out, dict) else {}


def _parse_blocked_reasons(details: str) -> List[str]:
    text = str(details or "")
    marker = "reasons="
    if marker not in text:
        return []
    reasons_text = text.split(marker, 1)[1].strip()
    return [part.strip() for part in reasons_text.split(",") if part.strip()]


def _stats(values: Iterable[Optional[float]]) -> Dict[str, Any]:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return {"count": 0, "avg": None, "median": None, "min": None, "max": None}
    return {
        "count": len(nums),
        "avg": sum(nums) / len(nums),
        "median": median(nums),
        "min": min(nums),
        "max": max(nums),
    }


def _fmt_num(val: Any, digits: int = 2) -> str:
    if val is None:
        return "n/a"
    try:
        return f"{float(val):.{digits}f}"
    except Exception:
        return "n/a"


def _is_short_entry_fill(fill: EntryFill) -> bool:
    if str(fill.order_action or "").upper() == "SELL" and int(fill.parent_id or 0) == 0:
        return True
    return str(fill.fill_action or "").upper() == "SLD"


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def load_shadow_events(conn: sqlite3.Connection, cutoff: Optional[datetime]) -> List[ShadowEvent]:
    q = """
        SELECT id, ts, kind, details, symbol, expected_price, expected_slippage_bps,
               event_risk_reason, short_borrow_source, risk_snapshot_json
        FROM risk_events
        WHERE kind='SHORT_SAFETY_SHADOW_BLOCK'
        ORDER BY ts ASC, id ASC
    """
    rows = conn.execute(q).fetchall()
    out: List[ShadowEvent] = []
    for row in rows:
        ts = _parse_ts(row["ts"])
        if cutoff is not None and ts < cutoff:
            continue
        snapshot = _safe_json_dict(row["risk_snapshot_json"])
        out.append(
            ShadowEvent(
                id=int(row["id"]),
                ts=ts,
                ts_raw=str(row["ts"] or ""),
                kind=str(row["kind"] or ""),
                symbol=str(row["symbol"] or "").upper(),
                details=str(row["details"] or ""),
                expected_price=_safe_float(row["expected_price"]),
                expected_slippage_bps=_safe_float(row["expected_slippage_bps"]),
                event_risk_reason=str(row["event_risk_reason"] or ""),
                short_borrow_source=str(row["short_borrow_source"] or ""),
                risk_snapshot=snapshot,
                blocked_reasons=_parse_blocked_reasons(str(row["details"] or "")),
            )
        )
    return out


def load_entry_fills(conn: sqlite3.Connection, cutoff: Optional[datetime]) -> List[EntryFill]:
    q = """
        SELECT
            f.id,
            f.ts,
            f.order_id,
            f.exec_id,
            f.symbol,
            f.action AS fill_action,
            f.qty,
            f.price,
            f.pnl,
            f.expected_price,
            f.expected_slippage_bps,
            f.actual_slippage_bps,
            f.slippage_bps_deviation,
            f.event_risk_reason,
            f.short_borrow_source,
            f.risk_snapshot_json,
            o.action AS order_action,
            o.parent_id
        FROM fills f
        LEFT JOIN orders o
          ON o.order_id = f.order_id
        ORDER BY f.ts ASC, f.id ASC
    """
    rows = conn.execute(q).fetchall()
    out: List[EntryFill] = []
    for row in rows:
        ts = _parse_ts(row["ts"])
        if cutoff is not None and ts < cutoff:
            continue
        fill = EntryFill(
            id=int(row["id"]),
            ts=ts,
            ts_raw=str(row["ts"] or ""),
            order_id=int(row["order_id"]),
            exec_id=str(row["exec_id"] or ""),
            symbol=str(row["symbol"] or "").upper(),
            fill_action=str(row["fill_action"] or "").upper(),
            order_action=str(row["order_action"] or "").upper(),
            parent_id=_safe_int(row["parent_id"]),
            qty=float(row["qty"] or 0.0),
            price=float(row["price"] or 0.0),
            pnl=_safe_float(row["pnl"]),
            expected_price=_safe_float(row["expected_price"]),
            expected_slippage_bps=_safe_float(row["expected_slippage_bps"]),
            actual_slippage_bps=_safe_float(row["actual_slippage_bps"]),
            slippage_bps_deviation=_safe_float(row["slippage_bps_deviation"]),
            event_risk_reason=str(row["event_risk_reason"] or ""),
            short_borrow_source=str(row["short_borrow_source"] or ""),
            risk_snapshot=_safe_json_dict(row["risk_snapshot_json"]),
        )
        if _is_short_entry_fill(fill):
            out.append(fill)
    return out


def load_risk_event_kind_summary(conn: sqlite3.Connection, cutoff: Optional[datetime]) -> List[Dict[str, Any]]:
    q = """
        SELECT kind, COUNT(*) AS event_count, AVG(value) AS avg_value
        FROM risk_events
        GROUP BY kind
        ORDER BY event_count DESC, kind ASC
    """
    rows = conn.execute(q).fetchall()
    out: List[Dict[str, Any]] = []
    if cutoff is None:
        return [
            {
                "kind": str(row["kind"] or ""),
                "event_count": int(row["event_count"] or 0),
                "avg_value": _safe_float(row["avg_value"]),
            }
            for row in rows
        ]

    counts: Dict[str, List[Optional[float]]] = defaultdict(list)
    for row in conn.execute("SELECT kind, value, ts FROM risk_events ORDER BY ts ASC, id ASC").fetchall():
        ts = _parse_ts(row["ts"])
        if ts < cutoff:
            continue
        counts[str(row["kind"] or "")].append(_safe_float(row["value"]))
    for kind, vals in sorted(counts.items(), key=lambda item: (-len(item[1]), item[0])):
        nums = [v for v in vals if v is not None]
        out.append(
            {
                "kind": kind,
                "event_count": len(vals),
                "avg_value": (sum(nums) / len(nums)) if nums else None,
            }
        )
    return out


def match_shadow_events(
    shadow_events: List[ShadowEvent],
    entry_fills: List[EntryFill],
    match_window_minutes: int,
) -> tuple[List[Dict[str, Any]], Dict[int, int]]:
    fills_by_symbol: Dict[str, List[EntryFill]] = defaultdict(list)
    for fill in entry_fills:
        fills_by_symbol[fill.symbol].append(fill)

    match_window = timedelta(minutes=int(match_window_minutes))
    fill_indices: Dict[str, int] = {symbol: 0 for symbol in fills_by_symbol}
    used_fill_ids: set[int] = set()
    event_rows: List[Dict[str, Any]] = []
    event_to_fill: Dict[int, int] = {}

    for event in shadow_events:
        symbol_fills = fills_by_symbol.get(event.symbol, [])
        start_idx = fill_indices.get(event.symbol, 0)
        while start_idx < len(symbol_fills) and symbol_fills[start_idx].ts < event.ts:
            start_idx += 1

        matched_fill: Optional[EntryFill] = None
        matched_idx: Optional[int] = None
        for idx in range(start_idx, len(symbol_fills)):
            fill = symbol_fills[idx]
            if fill.id in used_fill_ids:
                continue
            if fill.ts < event.ts:
                continue
            if fill.ts - event.ts > match_window:
                break
            matched_fill = fill
            matched_idx = idx
            break

        if matched_fill is not None and matched_idx is not None:
            used_fill_ids.add(matched_fill.id)
            fill_indices[event.symbol] = matched_idx + 1
            event_to_fill[event.id] = matched_fill.id

        risk_snapshot = event.risk_snapshot
        event_rows.append(
            {
                "event_id": event.id,
                "ts": event.ts_raw,
                "symbol": event.symbol,
                "blocked_reasons": ",".join(event.blocked_reasons),
                "blocked_reason_count": len(event.blocked_reasons),
                "expected_price": event.expected_price,
                "expected_slippage_bps": event.expected_slippage_bps,
                "event_risk_reason": event.event_risk_reason,
                "short_borrow_source": event.short_borrow_source,
                "risk_per_share": _safe_float(risk_snapshot.get("risk_per_share")),
                "avg_bar_volume": _safe_float(risk_snapshot.get("avg_bar_volume")),
                "liquidity_haircut": _safe_float(risk_snapshot.get("liquidity_haircut")),
                "match_status": "MATCHED" if matched_fill is not None else "UNMATCHED",
                "matched_fill_id": matched_fill.id if matched_fill is not None else None,
                "matched_fill_ts": matched_fill.ts_raw if matched_fill is not None else "",
                "latency_sec": (matched_fill.ts - event.ts).total_seconds() if matched_fill is not None else None,
                "fill_order_id": matched_fill.order_id if matched_fill is not None else None,
                "fill_exec_id": matched_fill.exec_id if matched_fill is not None else "",
                "fill_qty": matched_fill.qty if matched_fill is not None else None,
                "fill_price": matched_fill.price if matched_fill is not None else None,
                "fill_actual_slippage_bps": matched_fill.actual_slippage_bps if matched_fill is not None else None,
                "fill_slippage_bps_deviation": matched_fill.slippage_bps_deviation if matched_fill is not None else None,
                "fill_pnl": matched_fill.pnl if matched_fill is not None else None,
            }
        )
    return event_rows, event_to_fill


def _group_fill_summary(name: str, fills: List[EntryFill]) -> Dict[str, Any]:
    actual_stats = _stats(fill.actual_slippage_bps for fill in fills)
    deviation_stats = _stats(fill.slippage_bps_deviation for fill in fills)
    pnl_stats = _stats(fill.pnl for fill in fills)
    return {
        "group": name,
        "fill_count": len(fills),
        "avg_actual_slippage_bps": actual_stats["avg"],
        "median_actual_slippage_bps": actual_stats["median"],
        "avg_slippage_bps_deviation": deviation_stats["avg"],
        "median_slippage_bps_deviation": deviation_stats["median"],
        "avg_pnl": pnl_stats["avg"],
        "median_pnl": pnl_stats["median"],
        "avg_expected_slippage_bps": _stats(fill.expected_slippage_bps for fill in fills)["avg"],
        "avg_qty": _stats(fill.qty for fill in fills)["avg"],
    }


def _recommendation(
    matched_fill_count: int,
    control_fill_count: int,
    shadow_avg_slippage: Optional[float],
    control_avg_slippage: Optional[float],
    shadow_avg_pnl: Optional[float],
    control_avg_pnl: Optional[float],
    *,
    min_sample: int,
    slippage_delta_bps_threshold: float,
    pnl_delta_threshold: float,
) -> Dict[str, Any]:
    if matched_fill_count <= 0:
        return {
            "status": "NO_SHADOW_MATCHED_FILLS",
            "reason": "没有 shadow 命中的后续短空成交，先继续收集样本。",
        }
    if matched_fill_count < int(min_sample):
        return {
            "status": "KEEP_SHADOW_SAMPLE_TOO_SMALL",
            "reason": f"命中的 shadow 样本只有 {matched_fill_count} 笔，低于最小样本 {min_sample}。",
        }
    if control_fill_count <= 0:
        return {
            "status": "KEEP_SHADOW_NO_CONTROL",
            "reason": "没有未命中的短空成交对照组，暂时不能切回硬阻断。",
        }

    slippage_delta = None
    pnl_delta = None
    if shadow_avg_slippage is not None and control_avg_slippage is not None:
        slippage_delta = float(shadow_avg_slippage) - float(control_avg_slippage)
    if shadow_avg_pnl is not None and control_avg_pnl is not None:
        pnl_delta = float(shadow_avg_pnl) - float(control_avg_pnl)

    if (
        slippage_delta is not None
        and slippage_delta >= float(slippage_delta_bps_threshold)
    ) or (
        pnl_delta is not None
        and pnl_delta <= -abs(float(pnl_delta_threshold))
    ):
        return {
            "status": "CANDIDATE_FOR_HARD_BLOCK_REVIEW",
            "reason": "shadow 命中的成交显著更差，已具备转硬阻断的复盘价值。",
            "slippage_delta_bps": slippage_delta,
            "pnl_delta": pnl_delta,
        }
    return {
        "status": "KEEP_SHADOW_THRESHOLDS_UNPROVEN",
        "reason": "shadow 命中的成交还没有稳定显示出更差表现，继续观察。",
        "slippage_delta_bps": slippage_delta,
        "pnl_delta": pnl_delta,
    }


def build_shadow_burnin_review(
    db_path: str,
    *,
    days: int = 14,
    match_window_min: int = 30,
    min_sample: int = 10,
    slippage_delta_bps_threshold: float = 5.0,
    pnl_delta_threshold: float = 0.0,
) -> Dict[str, Any]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(str(path))

    cutoff = None
    if int(days) > 0:
        cutoff = datetime.now(UTC) - timedelta(days=int(days))

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        shadow_events = load_shadow_events(conn, cutoff)
        entry_fills = load_entry_fills(conn, cutoff)
        event_rows, event_to_fill = match_shadow_events(shadow_events, entry_fills, match_window_min)
        matched_fill_ids = set(event_to_fill.values())

        fill_rows: List[Dict[str, Any]] = []
        matched_fills: List[EntryFill] = []
        control_fills: List[EntryFill] = []
        for fill in entry_fills:
            shadow_matched = fill.id in matched_fill_ids
            if shadow_matched:
                matched_fills.append(fill)
            else:
                control_fills.append(fill)
            fill_rows.append(
                {
                    "fill_id": fill.id,
                    "ts": fill.ts_raw,
                    "symbol": fill.symbol,
                    "order_id": fill.order_id,
                    "exec_id": fill.exec_id,
                    "order_action": fill.order_action,
                    "fill_action": fill.fill_action,
                    "qty": fill.qty,
                    "price": fill.price,
                    "pnl": fill.pnl,
                    "expected_price": fill.expected_price,
                    "expected_slippage_bps": fill.expected_slippage_bps,
                    "actual_slippage_bps": fill.actual_slippage_bps,
                    "slippage_bps_deviation": fill.slippage_bps_deviation,
                    "event_risk_reason": fill.event_risk_reason,
                    "short_borrow_source": fill.short_borrow_source,
                    "shadow_matched": int(shadow_matched),
                }
            )

        kind_summary = load_risk_event_kind_summary(conn, cutoff)
    finally:
        conn.close()

    reason_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    symbol_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event_row in event_rows:
        reasons = [
            part.strip()
            for part in str(event_row.get("blocked_reasons", "") or "").split(",")
            if part.strip()
        ] or ["(none)"]
        for reason in reasons:
            reason_buckets[reason].append(event_row)
        symbol_buckets[str(event_row.get("symbol", "")).upper()].append(event_row)

    reason_summary: List[Dict[str, Any]] = []
    for reason, rows in sorted(reason_buckets.items(), key=lambda item: (-len(item[1]), item[0])):
        matched_rows = [row for row in rows if row.get("match_status") == "MATCHED"]
        reason_summary.append(
            {
                "blocked_reason": reason,
                "shadow_event_count": len(rows),
                "matched_fill_count": len(matched_rows),
                "matched_rate": (len(matched_rows) / len(rows)) if rows else 0.0,
                "avg_fill_actual_slippage_bps": _stats(row.get("fill_actual_slippage_bps") for row in matched_rows)["avg"],
                "avg_fill_slippage_bps_deviation": _stats(row.get("fill_slippage_bps_deviation") for row in matched_rows)["avg"],
                "avg_fill_pnl": _stats(row.get("fill_pnl") for row in matched_rows)["avg"],
            }
        )

    symbol_summary: List[Dict[str, Any]] = []
    for symbol, rows in sorted(symbol_buckets.items(), key=lambda item: (-len(item[1]), item[0])):
        matched_rows = [row for row in rows if row.get("match_status") == "MATCHED"]
        reasons = Counter()
        for row in rows:
            for reason in str(row.get("blocked_reasons", "") or "").split(","):
                if reason.strip():
                    reasons[reason.strip()] += 1
        symbol_summary.append(
            {
                "symbol": symbol,
                "shadow_event_count": len(rows),
                "matched_fill_count": len(matched_rows),
                "matched_rate": (len(matched_rows) / len(rows)) if rows else 0.0,
                "top_blocked_reasons": ",".join(reason for reason, _ in reasons.most_common(3)),
                "avg_fill_actual_slippage_bps": _stats(row.get("fill_actual_slippage_bps") for row in matched_rows)["avg"],
                "avg_fill_pnl": _stats(row.get("fill_pnl") for row in matched_rows)["avg"],
            }
        )

    fill_group_summary = [
        _group_fill_summary("shadow_matched", matched_fills),
        _group_fill_summary("control", control_fills),
    ]
    shadow_group = fill_group_summary[0]
    control_group = fill_group_summary[1]

    recommendation = _recommendation(
        matched_fill_count=int(shadow_group["fill_count"]),
        control_fill_count=int(control_group["fill_count"]),
        shadow_avg_slippage=shadow_group["avg_actual_slippage_bps"],
        control_avg_slippage=control_group["avg_actual_slippage_bps"],
        shadow_avg_pnl=shadow_group["avg_pnl"],
        control_avg_pnl=control_group["avg_pnl"],
        min_sample=min_sample,
        slippage_delta_bps_threshold=slippage_delta_bps_threshold,
        pnl_delta_threshold=pnl_delta_threshold,
    )

    overview = {
        "db_path": str(path),
        "lookback_days": int(days),
        "match_window_min": int(match_window_min),
        "shadow_event_count": len(shadow_events),
        "matched_shadow_fill_count": int(shadow_group["fill_count"]),
        "unmatched_shadow_event_count": len([row for row in event_rows if row["match_status"] != "MATCHED"]),
        "short_entry_fill_count": len(entry_fills),
        "control_short_fill_count": int(control_group["fill_count"]),
        "shadow_match_rate": (int(shadow_group["fill_count"]) / len(shadow_events)) if shadow_events else 0.0,
        "shadow_avg_actual_slippage_bps": shadow_group["avg_actual_slippage_bps"],
        "control_avg_actual_slippage_bps": control_group["avg_actual_slippage_bps"],
        "shadow_avg_pnl": shadow_group["avg_pnl"],
        "control_avg_pnl": control_group["avg_pnl"],
        "slippage_delta_bps": (
            None
            if shadow_group["avg_actual_slippage_bps"] is None or control_group["avg_actual_slippage_bps"] is None
            else float(shadow_group["avg_actual_slippage_bps"]) - float(control_group["avg_actual_slippage_bps"])
        ),
        "pnl_delta": (
            None
            if shadow_group["avg_pnl"] is None or control_group["avg_pnl"] is None
            else float(shadow_group["avg_pnl"]) - float(control_group["avg_pnl"])
        ),
        "recommendation": recommendation,
    }

    return {
        "overview": overview,
        "risk_event_kind_summary": kind_summary,
        "fill_group_summary": fill_group_summary,
        "shadow_event_matches": event_rows,
        "short_entry_fills": fill_rows,
        "blocked_reason_summary": reason_summary,
        "symbol_summary": symbol_summary,
    }


def write_shadow_burnin_outputs(out_dir: str, review: Dict[str, Any]) -> Dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    summary_json = out / "shadow_burnin_summary.json"
    kind_csv = out / "risk_event_kind_summary.csv"
    group_csv = out / "fill_group_summary.csv"
    event_csv = out / "shadow_event_matches.csv"
    fill_csv = out / "short_entry_fills.csv"
    reason_csv = out / "blocked_reason_summary.csv"
    symbol_csv = out / "symbol_summary.csv"
    md_path = out / "shadow_burnin.md"

    write_json(str(summary_json), review)
    _write_csv(
        kind_csv,
        review.get("risk_event_kind_summary", []),
        ["kind", "event_count", "avg_value"],
    )
    _write_csv(
        group_csv,
        review.get("fill_group_summary", []),
        [
            "group",
            "fill_count",
            "avg_actual_slippage_bps",
            "median_actual_slippage_bps",
            "avg_slippage_bps_deviation",
            "median_slippage_bps_deviation",
            "avg_pnl",
            "median_pnl",
            "avg_expected_slippage_bps",
            "avg_qty",
        ],
    )
    _write_csv(
        event_csv,
        review.get("shadow_event_matches", []),
        [
            "event_id",
            "ts",
            "symbol",
            "blocked_reasons",
            "blocked_reason_count",
            "expected_price",
            "expected_slippage_bps",
            "event_risk_reason",
            "short_borrow_source",
            "risk_per_share",
            "avg_bar_volume",
            "liquidity_haircut",
            "match_status",
            "matched_fill_id",
            "matched_fill_ts",
            "latency_sec",
            "fill_order_id",
            "fill_exec_id",
            "fill_qty",
            "fill_price",
            "fill_actual_slippage_bps",
            "fill_slippage_bps_deviation",
            "fill_pnl",
        ],
    )
    _write_csv(
        fill_csv,
        review.get("short_entry_fills", []),
        [
            "fill_id",
            "ts",
            "symbol",
            "order_id",
            "exec_id",
            "order_action",
            "fill_action",
            "qty",
            "price",
            "pnl",
            "expected_price",
            "expected_slippage_bps",
            "actual_slippage_bps",
            "slippage_bps_deviation",
            "event_risk_reason",
            "short_borrow_source",
            "shadow_matched",
        ],
    )
    _write_csv(
        reason_csv,
        review.get("blocked_reason_summary", []),
        [
            "blocked_reason",
            "shadow_event_count",
            "matched_fill_count",
            "matched_rate",
            "avg_fill_actual_slippage_bps",
            "avg_fill_slippage_bps_deviation",
            "avg_fill_pnl",
        ],
    )
    _write_csv(
        symbol_csv,
        review.get("symbol_summary", []),
        [
            "symbol",
            "shadow_event_count",
            "matched_fill_count",
            "matched_rate",
            "top_blocked_reasons",
            "avg_fill_actual_slippage_bps",
            "avg_fill_pnl",
        ],
    )

    overview = review.get("overview", {})
    group_rows = review.get("fill_group_summary", [])
    shadow_group = group_rows[0] if len(group_rows) > 0 else {}
    control_group = group_rows[1] if len(group_rows) > 1 else {}
    top_reasons = review.get("blocked_reason_summary", [])[:5]

    lines = [
        "# Shadow Burn-In Review",
        "",
        "## Overview",
        f"- shadow events: {int(overview.get('shadow_event_count', 0) or 0)}",
        f"- matched shadow fills: {int(overview.get('matched_shadow_fill_count', 0) or 0)}",
        f"- unmatched shadow events: {int(overview.get('unmatched_shadow_event_count', 0) or 0)}",
        f"- short entry fills: {int(overview.get('short_entry_fill_count', 0) or 0)}",
        f"- control short fills: {int(overview.get('control_short_fill_count', 0) or 0)}",
        f"- match rate: {_fmt_num(overview.get('shadow_match_rate'), 3)}",
        "",
        "## Fill Comparison",
        f"- shadow matched avg actual slippage (bps): {_fmt_num(shadow_group.get('avg_actual_slippage_bps'))}",
        f"- control avg actual slippage (bps): {_fmt_num(control_group.get('avg_actual_slippage_bps'))}",
        f"- slippage delta (bps): {_fmt_num(overview.get('slippage_delta_bps'))}",
        f"- shadow matched avg pnl: {_fmt_num(shadow_group.get('avg_pnl'))}",
        f"- control avg pnl: {_fmt_num(control_group.get('avg_pnl'))}",
        f"- pnl delta: {_fmt_num(overview.get('pnl_delta'))}",
        "",
        "## Recommendation",
        f"- {str((overview.get('recommendation') or {}).get('status', 'UNKNOWN'))}: {str((overview.get('recommendation') or {}).get('reason', ''))}",
        "",
        "## Top Blocked Reasons",
    ]
    if not top_reasons:
        lines.append("- no shadow events")
    else:
        for row in top_reasons:
            lines.append(
                f"- {row.get('blocked_reason')}: events={int(row.get('shadow_event_count', 0) or 0)} "
                f"matched={int(row.get('matched_fill_count', 0) or 0)} "
                f"rate={_fmt_num(row.get('matched_rate'), 3)} "
                f"avg_slip={_fmt_num(row.get('avg_fill_actual_slippage_bps'))} "
                f"avg_pnl={_fmt_num(row.get('avg_fill_pnl'))}"
            )

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {
        "summary_json": str(summary_json),
        "kind_csv": str(kind_csv),
        "group_csv": str(group_csv),
        "event_csv": str(event_csv),
        "fill_csv": str(fill_csv),
        "reason_csv": str(reason_csv),
        "symbol_csv": str(symbol_csv),
        "markdown": str(md_path),
    }


def main() -> None:
    args = parse_args()
    review = build_shadow_burnin_review(
        args.db,
        days=int(args.days),
        match_window_min=int(args.match_window_min),
        min_sample=int(args.min_sample),
        slippage_delta_bps_threshold=float(args.slippage_delta_bps_threshold),
        pnl_delta_threshold=float(args.pnl_delta_threshold),
    )
    outputs = write_shadow_burnin_outputs(args.out_dir, review)
    overview = review.get("overview", {})
    recommendation = overview.get("recommendation", {})
    print(
        "shadow_events={shadow_event_count} matched_shadow_fills={matched_shadow_fill_count} "
        "control_short_fills={control_short_fills} recommendation={status}".format(
            shadow_event_count=int(overview.get("shadow_event_count", 0) or 0),
            matched_shadow_fill_count=int(overview.get("matched_shadow_fill_count", 0) or 0),
            control_short_fills=int(overview.get("control_short_fill_count", 0) or 0),
            status=str(recommendation.get("status", "UNKNOWN")),
        )
    )
    for label, path in outputs.items():
        print(f"{label}={path}")


if __name__ == "__main__":
    main()
