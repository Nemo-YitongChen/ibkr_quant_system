from __future__ import annotations

import json
from typing import Any, Dict, List

from ..common.storage import Storage

STATUS_LABELS = {
    "WATCHING": "观望中",
    "WATCH_NEAR_ENTRY": "接近入场",
    "ENTRY_READY": "可入场",
    "ADD_READY": "可增持",
    "HOLDING": "持有观察",
    "REDUCE_READY": "可减持",
    "DEPRIORITIZED": "取消观望",
    "REMOVED_FROM_WATCH": "移出观望",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_state(
    *,
    symbol: str,
    market: str,
    portfolio_id: str,
    report_dir: str,
    analysis_run_id: str,
    run_kind: str,
    ranked_row: Dict[str, Any] | None,
    opportunity_row: Dict[str, Any] | None,
    held_qty: float,
    observed_ts: str,
) -> Dict[str, Any]:
    ranked_row = dict(ranked_row or {})
    opportunity_row = dict(opportunity_row or {})
    action = str(ranked_row.get("action", opportunity_row.get("action", "WATCH")) or "WATCH").upper()
    entry_status = str(opportunity_row.get("entry_status") or "").upper().strip()
    score = _to_float(ranked_row.get("score"), 0.0)
    reason = str(opportunity_row.get("entry_reason") or ranked_row.get("regime_reason") or "").strip()

    if action == "REDUCE":
        if held_qty > 0:
            status = "REDUCE_READY"
            lifecycle = "REDUCE"
        else:
            status = "DEPRIORITIZED"
            lifecycle = "DEPRIORITIZED"
    elif entry_status == "ADD_ON_PULLBACK" or (entry_status == "ENTRY_NOW" and held_qty > 0):
        status = "ADD_READY"
        lifecycle = "ADD"
    elif entry_status == "ENTRY_NOW":
        status = "ENTRY_READY"
        lifecycle = "ENTRY"
    elif held_qty > 0 and action in {"ACCUMULATE", "HOLD"}:
        status = "HOLDING"
        lifecycle = "HOLD"
    elif entry_status == "NEAR_ENTRY":
        status = "WATCH_NEAR_ENTRY"
        lifecycle = "WATCH"
    elif action in {"WATCH", "ACCUMULATE", "HOLD"}:
        status = "WATCHING"
        lifecycle = "WATCH"
    else:
        status = "DEPRIORITIZED"
        lifecycle = "DEPRIORITIZED"

    details = {
        "score": float(score),
        "held_qty": float(held_qty),
        "action": action,
        "entry_status": entry_status,
        "reason": reason,
        "coverage_scope": "investment_candidates_and_opportunity_scan",
    }
    if ranked_row:
        details["candidate"] = {
            "action": action,
            "score": float(score),
            "regime_state": str(ranked_row.get("regime_state") or ""),
            "last_close": _to_float(ranked_row.get("last_close"), 0.0),
        }
    if opportunity_row:
        details["opportunity"] = {
            "entry_status": entry_status,
            "entry_reason": str(opportunity_row.get("entry_reason") or ""),
            "ref_price": _to_float(opportunity_row.get("ref_price"), 0.0),
            "entry_anchor": _to_float(opportunity_row.get("entry_anchor"), 0.0),
        }
    return {
        "ts": observed_ts,
        "market": str(market or "").upper(),
        "portfolio_id": str(portfolio_id or ""),
        "symbol": str(symbol or "").upper(),
        "analysis_run_id": str(analysis_run_id or ""),
        "status": status,
        "lifecycle": lifecycle,
        "action": action,
        "entry_status": entry_status,
        "score": float(score),
        "held_qty": float(held_qty),
        "report_dir": str(report_dir or ""),
        "run_kind": str(run_kind or "opportunity"),
        "reason": reason,
        "details": details,
    }


def _removed_state(
    *,
    previous_state: Dict[str, Any],
    analysis_run_id: str,
    observed_ts: str,
    run_kind: str,
) -> Dict[str, Any]:
    details = {
        "coverage_scope": "investment_candidates_and_opportunity_scan",
        "previous_state": {
            "status": str(previous_state.get("status") or ""),
            "lifecycle": str(previous_state.get("lifecycle") or ""),
            "action": str(previous_state.get("action") or ""),
        },
    }
    return {
        "ts": observed_ts,
        "market": str(previous_state.get("market") or "").upper(),
        "portfolio_id": str(previous_state.get("portfolio_id") or ""),
        "symbol": str(previous_state.get("symbol") or "").upper(),
        "analysis_run_id": str(analysis_run_id or ""),
        "status": "REMOVED_FROM_WATCH",
        "lifecycle": "REMOVED",
        "action": "",
        "entry_status": "",
        "score": 0.0,
        "held_qty": _to_float(previous_state.get("held_qty"), 0.0),
        "report_dir": str(previous_state.get("report_dir") or ""),
        "run_kind": str(run_kind or "opportunity"),
        "reason": "当前分析输出中已不再覆盖该标的。",
        "details": details,
    }


def _transition_kind(previous_state: Dict[str, Any] | None, current_state: Dict[str, Any]) -> str:
    if not previous_state:
        return "INIT"

    prev_lifecycle = str(previous_state.get("lifecycle") or "").upper()
    curr_lifecycle = str(current_state.get("lifecycle") or "").upper()
    prev_status = str(previous_state.get("status") or "").upper()
    curr_status = str(current_state.get("status") or "").upper()

    if curr_lifecycle == "REMOVED":
        return "REMOVE_FROM_WATCH"
    if prev_lifecycle == "REMOVED" and curr_lifecycle in {"WATCH", "ENTRY", "ADD", "HOLD", "REDUCE", "DEPRIORITIZED"}:
        return "READD_TO_WATCH"
    if prev_lifecycle == "WATCH" and curr_lifecycle == "ENTRY":
        return "WATCH_TO_ENTRY"
    if prev_lifecycle in {"WATCH", "ENTRY", "HOLD"} and curr_lifecycle == "ADD":
        return "TO_ADD"
    if prev_lifecycle != "REDUCE" and curr_lifecycle == "REDUCE":
        return "TO_REDUCE"
    if prev_lifecycle in {"ENTRY", "ADD", "HOLD", "REDUCE"} and curr_lifecycle == "WATCH":
        return "EXIT_TO_WATCH"
    if prev_lifecycle == "WATCH" and curr_lifecycle == "DEPRIORITIZED":
        return "CANCEL_WATCH"
    if prev_lifecycle == "DEPRIORITIZED" and curr_lifecycle == "WATCH":
        return "RESUME_WATCH"
    if prev_status != curr_status:
        return "STATUS_CHANGED"
    return "UNCHANGED"


def _state_label(status: str) -> str:
    return STATUS_LABELS.get(str(status or "").upper(), str(status or "").upper() or "UNKNOWN")


def _states_equal(previous_state: Dict[str, Any] | None, current_state: Dict[str, Any]) -> bool:
    if not previous_state:
        return False
    keys = ("status", "lifecycle", "action", "entry_status", "reason")
    for key in keys:
        if str(previous_state.get(key) or "") != str(current_state.get(key) or ""):
            return False
    return abs(_to_float(previous_state.get("held_qty"), 0.0) - _to_float(current_state.get("held_qty"), 0.0)) < 1e-9


def _event_payload(
    *,
    previous_state: Dict[str, Any] | None,
    current_state: Dict[str, Any],
    event_kind: str,
) -> Dict[str, Any]:
    from_status = str(previous_state.get("status") or "") if previous_state else ""
    to_status = str(current_state.get("status") or "")
    if previous_state:
        summary = f"{current_state['symbol']} {_state_label(from_status)} -> {_state_label(to_status)}"
    else:
        summary = f"{current_state['symbol']} INIT -> {_state_label(to_status)}"
    details = {
        "previous_state": previous_state or {},
        "current_state": current_state,
    }
    return {
        "ts": str(current_state.get("ts") or ""),
        "market": str(current_state.get("market") or "").upper(),
        "portfolio_id": str(current_state.get("portfolio_id") or ""),
        "symbol": str(current_state.get("symbol") or "").upper(),
        "analysis_run_id": str(current_state.get("analysis_run_id") or ""),
        "event_kind": str(event_kind or "STATE_CHANGED"),
        "from_status": from_status,
        "to_status": to_status,
        "from_lifecycle": str(previous_state.get("lifecycle") or "") if previous_state else "",
        "to_lifecycle": str(current_state.get("lifecycle") or ""),
        "action": str(current_state.get("action") or ""),
        "entry_status": str(current_state.get("entry_status") or ""),
        "score": _to_float(current_state.get("score"), 0.0),
        "held_qty": _to_float(current_state.get("held_qty"), 0.0),
        "report_dir": str(current_state.get("report_dir") or ""),
        "run_kind": str(current_state.get("run_kind") or ""),
        "summary": summary,
        "details": details,
    }


def build_analysis_states(
    *,
    market: str,
    portfolio_id: str,
    report_dir: str,
    analysis_run_id: str,
    observed_ts: str,
    ranked_rows: List[Dict[str, Any]],
    opportunity_rows: List[Dict[str, Any]],
    broker_positions: Dict[str, Dict[str, Any]],
    run_kind: str = "opportunity",
) -> List[Dict[str, Any]]:
    ranked_map = {
        str(row.get("symbol") or "").upper(): dict(row)
        for row in list(ranked_rows or [])
        if str(row.get("symbol") or "").strip()
    }
    opportunity_map = {
        str(row.get("symbol") or "").upper(): dict(row)
        for row in list(opportunity_rows or [])
        if str(row.get("symbol") or "").strip()
    }
    symbols = sorted(set(ranked_map) | set(opportunity_map))
    states: List[Dict[str, Any]] = []
    for symbol in symbols:
        held_qty = _to_float((broker_positions.get(symbol) or {}).get("qty"), 0.0)
        states.append(
            _normalize_state(
                symbol=symbol,
                market=market,
                portfolio_id=portfolio_id,
                report_dir=report_dir,
                analysis_run_id=analysis_run_id,
                run_kind=run_kind,
                ranked_row=ranked_map.get(symbol),
                opportunity_row=opportunity_map.get(symbol),
                held_qty=held_qty,
                observed_ts=observed_ts,
            )
        )
    return states


def persist_analysis_states(
    storage: Storage,
    *,
    market: str,
    portfolio_id: str,
    analysis_run_id: str,
    observed_ts: str,
    current_states: List[Dict[str, Any]],
    run_kind: str = "opportunity",
) -> List[Dict[str, Any]]:
    previous_states = storage.get_investment_analysis_state_map(market, portfolio_id=portfolio_id)
    current_map = {str(row.get("symbol") or "").upper(): dict(row) for row in list(current_states or [])}
    for symbol, previous_state in previous_states.items():
        if symbol in current_map:
            continue
        if str(previous_state.get("lifecycle") or "").upper() == "REMOVED":
            continue
        current_map[symbol] = _removed_state(
            previous_state=previous_state,
            analysis_run_id=analysis_run_id,
            observed_ts=observed_ts,
            run_kind=run_kind,
        )

    changed_events: List[Dict[str, Any]] = []
    for symbol in sorted(current_map):
        current_state = dict(current_map[symbol])
        previous_state = dict(previous_states.get(symbol) or {})
        if not _states_equal(previous_state or None, current_state):
            event_kind = _transition_kind(previous_state or None, current_state)
            if event_kind != "UNCHANGED":
                event = _event_payload(
                    previous_state=previous_state or None,
                    current_state=current_state,
                    event_kind=event_kind,
                )
                storage.insert_investment_analysis_event(event)
                changed_events.append(event)
        storage.upsert_investment_analysis_state(current_state)
    return changed_events


def build_and_persist_analysis_chain(
    storage: Storage,
    *,
    market: str,
    portfolio_id: str,
    report_dir: str,
    analysis_run_id: str,
    observed_ts: str,
    ranked_rows: List[Dict[str, Any]],
    opportunity_rows: List[Dict[str, Any]],
    broker_positions: Dict[str, Dict[str, Any]],
    run_kind: str = "opportunity",
) -> Dict[str, Any]:
    current_states = build_analysis_states(
        market=market,
        portfolio_id=portfolio_id,
        report_dir=report_dir,
        analysis_run_id=analysis_run_id,
        observed_ts=observed_ts,
        ranked_rows=ranked_rows,
        opportunity_rows=opportunity_rows,
        broker_positions=broker_positions,
        run_kind=run_kind,
    )
    events = persist_analysis_states(
        storage,
        market=market,
        portfolio_id=portfolio_id,
        analysis_run_id=analysis_run_id,
        observed_ts=observed_ts,
        current_states=current_states,
        run_kind=run_kind,
    )
    lifecycle_counts: Dict[str, int] = {}
    for row in current_states:
        lifecycle = str(row.get("lifecycle") or "").upper()
        lifecycle_counts[lifecycle] = lifecycle_counts.get(lifecycle, 0) + 1
    return {
        "state_count": len(current_states),
        "event_count": len(events),
        "lifecycle_counts": lifecycle_counts,
        "events": events,
        "states": current_states,
        "events_json": json.dumps(events, ensure_ascii=False),
    }
