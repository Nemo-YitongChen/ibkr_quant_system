from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List
import datetime as dt

import yaml

from ..common.logger import get_logger
from ..common.markets import (
    add_market_args,
    load_market_universe_config,
    load_symbols_from_symbol_master,
    market_config_path,
    resolve_market_code,
)
from ..ibkr.connection import IBKRConnection
from ..ibkr.contracts import make_stock_contract
from ..offhours.candidates import load_watchlist_symbols
from ..risk.short_data import fetch_remote_short_data

log = get_logger("tools.sync_short_safety_from_ibkr")
BASE_DIR = Path(__file__).resolve().parents[2]
BORROW_FIELDS = ["symbol", "borrow_fee_bps", "source", "note"]
SAFETY_FIELDS = [
    "symbol",
    "locate_status",
    "ssr_status",
    "spread_bps",
    "has_uptick_data",
    "source",
    "shortable_shares",
    "shortable_level",
    "bid",
    "ask",
    "note",
]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sync short safety reference files from IBKR market data.")
    add_market_args(ap)
    ap.add_argument("--ibkr_config", default="config/ibkr.yaml")
    ap.add_argument("--watchlist_yaml", default="")
    ap.add_argument("--symbols", default="")
    ap.add_argument("--max_symbols", type=int, default=200)
    ap.add_argument("--snapshot_wait_sec", type=float, default=2.5)
    ap.add_argument("--batch_size", type=int, default=40)
    ap.add_argument("--market_data_type", type=int, default=1, help="IB market data type: 1=real-time, 4=delayed-frozen.")
    ap.add_argument("--fallback_market_data_type", type=int, default=4)
    ap.add_argument("--no_delayed_fallback", action="store_true", default=False)
    ap.add_argument("--generic_tick_list", default="236")
    return ap.parse_args()


def _market_data_type_selected_by_user() -> bool:
    return "--market_data_type" in sys.argv or "--fallback_market_data_type" in sys.argv


def _resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, BASE_DIR / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _load_yaml(path_str: str) -> Dict[str, Any]:
    path = _resolve_project_path(path_str)
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _dedupe_keep_order(xs: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in xs:
        sym = str(x).upper().strip()
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


def _symbol_pool(args: argparse.Namespace, ibkr_cfg: Dict[str, Any], market_cfg: Dict[str, Any], market_code: str) -> List[str]:
    if str(args.symbols or "").strip():
        return _dedupe_keep_order([x for x in str(args.symbols).split(",") if x.strip()])[: int(args.max_symbols)]

    watchlist_yaml = str(args.watchlist_yaml or market_cfg.get("seed_watchlist_yaml", ibkr_cfg.get("seed_watchlist_yaml", "")) or "")
    if watchlist_yaml:
        syms = load_watchlist_symbols(str(_resolve_project_path(watchlist_yaml)))
        if syms:
            return _dedupe_keep_order(syms)[: int(args.max_symbols)]

    symbol_master_db = str(market_cfg.get("symbol_master_db", "symbol_master.db"))
    syms = load_symbols_from_symbol_master(_resolve_project_path(symbol_master_db), market_code)
    if syms:
        return _dedupe_keep_order(syms)[: int(args.max_symbols)]

    return _dedupe_keep_order([str(x) for x in ibkr_cfg.get("seed_symbols", ["SPY"])])[: int(args.max_symbols)]


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _ticker_last_price(ticker: Any) -> float | None:
    last = _safe_float(getattr(ticker, "last", None))
    if last is not None and last > 0:
        return last
    try:
        market_price = _safe_float(ticker.marketPrice())
    except Exception:
        market_price = None
    if market_price is not None and market_price > 0:
        return market_price
    return None


def _ticker_prev_close(ticker: Any) -> float | None:
    close = _safe_float(getattr(ticker, "close", None))
    if close is not None and close > 0:
        return close
    return None


def _ticker_shortable_level(ticker: Any) -> float | None:
    try:
        ticks = list(getattr(ticker, "ticks", []) or [])
    except Exception:
        return None
    for tick in reversed(ticks):
        if int(getattr(tick, "tickType", -1)) == 46:
            return _safe_float(getattr(tick, "price", None))
    return None


def _locate_status(shortable_shares: float | None, shortable_level: float | None) -> str:
    if shortable_shares is not None:
        if shortable_shares > 0:
            return "AVAILABLE"
        if shortable_shares == 0:
            return "UNAVAILABLE"
    if shortable_level is not None:
        if shortable_level > 1.5:
            return "AVAILABLE"
        if shortable_level <= 1.0:
            return "UNAVAILABLE"
    return "UNKNOWN"


def _spread_bps(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * 10_000.0


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _batch(xs: List[str], n: int) -> List[List[str]]:
    n = max(1, int(n))
    return [xs[i:i + n] for i in range(0, len(xs), n)]


def _set_market_data_type(ib: Any, market_data_type: int) -> None:
    try:
        ib.reqMarketDataType(int(market_data_type))
    except Exception as e:
        log.warning("reqMarketDataType failed: type=%s error=%s %s", market_data_type, type(e).__name__, e)


def _cancel_market_data(ib: Any, ticker: Any, contract: Any) -> None:
    try:
        req_id = getattr(ticker, "tickerId", None)
        req_map = getattr(getattr(ib, "wrapper", None), "reqId2Ticker", None)
        if req_id is not None and isinstance(req_map, dict) and req_id not in req_map:
            return
        ib.cancelMktData(contract)
    except Exception:
        pass


def _row_has_market_signal(row: Dict[str, Any]) -> bool:
    return any(
        row.get(key) not in (None, "", "UNKNOWN")
        for key in ("shortable_shares", "shortable_level", "bid", "ask", "spread_bps")
    )


def _format_num(v: float | None, *, digits: int = 2, integer: bool = False) -> str:
    if v is None:
        return ""
    if integer:
        return f"{v:.0f}"
    return f"{v:.{digits}f}"


def _empty_rows(symbol: str, source: str, note: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    safety_row = {
        "symbol": symbol,
        "locate_status": "UNKNOWN",
        "ssr_status": "UNKNOWN",
        "spread_bps": "",
        "has_uptick_data": "",
        "source": source,
        "shortable_shares": "",
        "shortable_level": "",
        "bid": "",
        "ask": "",
        "note": note,
    }
    borrow_row = {
        "symbol": symbol,
        "borrow_fee_bps": "",
        "source": source,
        "note": ",".join(
            [x for x in ("IB socket API does not expose borrow fee rate directly.", note) if str(x).strip()]
        ),
    }
    return safety_row, borrow_row


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _write_state(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2, sort_keys=True)


def _next_business_day(day: dt.date) -> dt.date:
    nxt = day + dt.timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += dt.timedelta(days=1)
    return nxt


def _merge_notes(*chunks: str) -> str:
    out: List[str] = []
    seen = set()
    for chunk in chunks:
        for part in str(chunk or "").split(","):
            item = part.strip()
            if item and item not in seen:
                seen.add(item)
                out.append(item)
    return ",".join(out)


def _merge_remote_data(
    batch_rows: Dict[str, Dict[str, Any]],
    remote_rows: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    merged = dict(batch_rows)
    for symbol, remote in remote_rows.items():
        current = merged.setdefault(symbol, {"safety": _empty_rows(symbol, "remote_source", "remote_only")[0], "borrow": _empty_rows(symbol, "remote_source", "remote_only")[1], "meta": {}})
        safety_row = dict(current.get("safety", {}))
        borrow_row = dict(current.get("borrow", {}))

        if getattr(remote, "borrow_fee_bps", None) is not None:
            borrow_row["borrow_fee_bps"] = f"{float(remote.borrow_fee_bps):.2f}"
            borrow_row["source"] = str(getattr(remote, "borrow_source", "") or borrow_row.get("source") or "")
        elif str(getattr(remote, "borrow_source", "") or "").strip():
            borrow_row["source"] = str(remote.borrow_source)
        borrow_row["note"] = _merge_notes(
            str(borrow_row.get("note", "") or ""),
            str(getattr(remote, "borrow_note", "") or ""),
            ",".join(getattr(remote, "notes", []) or []),
        )

        if str(getattr(remote, "ssr_status", "UNKNOWN") or "UNKNOWN").upper() != "UNKNOWN":
            safety_row["ssr_status"] = str(remote.ssr_status).upper()
            safety_row["source"] = str(getattr(remote, "ssr_source", "") or safety_row.get("source") or "")
        if str(getattr(remote, "locate_status", "UNKNOWN") or "UNKNOWN").upper() != "UNKNOWN":
            safety_row["locate_status"] = str(remote.locate_status).upper()
            safety_row["source"] = str(getattr(remote, "locate_source", "") or safety_row.get("source") or "")
        if getattr(remote, "spread_bps", None) is not None:
            safety_row["spread_bps"] = f"{float(remote.spread_bps):.2f}"
            safety_row["source"] = str(getattr(remote, "spread_source", "") or safety_row.get("source") or "")
        if getattr(remote, "has_uptick_data", None) is not None:
            safety_row["has_uptick_data"] = "true" if bool(remote.has_uptick_data) else "false"
            safety_row["source"] = str(getattr(remote, "has_uptick_source", "") or safety_row.get("source") or "")

        safety_row["note"] = _merge_notes(str(safety_row.get("note", "") or ""), ",".join(getattr(remote, "notes", []) or []))
        merged[symbol] = {"safety": safety_row, "borrow": borrow_row, "meta": dict(current.get("meta", {}) or {})}
    return merged


def _apply_rule201_ssr(
    batch_rows: Dict[str, Dict[str, Any]],
    *,
    market_code: str,
    rule_cfg: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    if str(market_code).upper() != "US":
        return batch_rows
    if not bool(dict(rule_cfg or {}).get("enabled", True)):
        return batch_rows

    state_file = str(dict(rule_cfg or {}).get("state_file", "") or "config/reference/short_ssr_state_us.json")
    state_path = _resolve_project_path(state_file)
    state = _load_state(state_path)
    trigger_map = dict(state.get("trigger_dates") or {})
    today = dt.datetime.now(dt.timezone.utc).date()
    updated_trigger_map: Dict[str, str] = {str(sym).upper(): str(val) for sym, val in trigger_map.items() if str(val).strip()}
    merged = dict(batch_rows)

    for symbol, pair in merged.items():
        safety_row = dict(pair.get("safety", {}) or {})
        if str(safety_row.get("ssr_status", "UNKNOWN") or "UNKNOWN").upper() != "UNKNOWN":
            trigger_date_raw = str(trigger_map.get(symbol, "") or "").strip()
            if trigger_date_raw:
                updated_trigger_map[symbol] = trigger_date_raw
            pair["safety"] = safety_row
            continue

        meta = dict(pair.get("meta", {}) or {})
        last_price = _safe_float(meta.get("last_price"))
        prev_close = _safe_float(meta.get("prev_close"))
        triggered_today = bool(prev_close and prev_close > 0 and last_price is not None and last_price <= (prev_close * 0.9))

        trigger_date_raw = str(trigger_map.get(symbol, "") or "").strip()
        active_from_state = False
        if trigger_date_raw:
            try:
                trigger_date = dt.date.fromisoformat(trigger_date_raw)
                active_from_state = today in (trigger_date, _next_business_day(trigger_date))
                if active_from_state:
                    updated_trigger_map[symbol] = trigger_date.isoformat()
                else:
                    updated_trigger_map.pop(symbol, None)
            except Exception:
                active_from_state = False
                updated_trigger_map.pop(symbol, None)

        if triggered_today:
            safety_row["ssr_status"] = "ON"
            safety_row["source"] = str(safety_row.get("source") or "ibkr_rule201")
            safety_row["note"] = _merge_notes(str(safety_row.get("note", "") or ""), "rule201_triggered_today")
            updated_trigger_map[symbol] = today.isoformat()
        elif active_from_state:
            safety_row["ssr_status"] = "ON"
            safety_row["source"] = str(safety_row.get("source") or "ibkr_rule201")
            safety_row["note"] = _merge_notes(str(safety_row.get("note", "") or ""), "rule201_carryover")
        elif prev_close and prev_close > 0 and last_price is not None:
            safety_row["ssr_status"] = "OFF"
            safety_row["source"] = str(safety_row.get("source") or "ibkr_rule201")
            updated_trigger_map.pop(symbol, None)

        pair["safety"] = safety_row

    _write_state(
        state_path,
        {
            "asof_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "trigger_dates": updated_trigger_map,
        },
    )
    return merged


def _collect_batch_rows(
    ib: Any,
    contract_by_symbol: Dict[str, Any],
    symbols: List[str],
    *,
    snapshot_wait_sec: float,
    market_data_type: int,
    generic_tick_list: str,
) -> Dict[str, Dict[str, Any]]:
    _set_market_data_type(ib, market_data_type)
    requests: List[tuple[str, Any, Any]] = []
    out: Dict[str, Dict[str, Any]] = {}

    for symbol in symbols:
        contract = contract_by_symbol.get(symbol)
        if contract is None or not getattr(contract, "conId", 0):
            safety_row, borrow_row = _empty_rows(symbol, f"ibkr_socket_api:type_{market_data_type}", "contract_unqualified")
            out[symbol] = {"safety": safety_row, "borrow": borrow_row, "meta": {}}
            continue
        try:
            ticker = ib.reqMktData(contract, genericTickList=generic_tick_list, snapshot=False, regulatorySnapshot=False)
            requests.append((symbol, contract, ticker))
        except Exception as e:
            safety_row, borrow_row = _empty_rows(
                symbol,
                f"ibkr_socket_api:type_{market_data_type}",
                f"reqMktData_error:{type(e).__name__}",
            )
            out[symbol] = {"safety": safety_row, "borrow": borrow_row, "meta": {}}

    if requests:
        ib.sleep(float(snapshot_wait_sec))

    for symbol, contract, ticker in requests:
        bid = _safe_float(getattr(ticker, "bid", None))
        ask = _safe_float(getattr(ticker, "ask", None))
        shortable_shares = _safe_float(getattr(ticker, "shortableShares", None))
        shortable_level = _ticker_shortable_level(ticker)
        spread = _spread_bps(bid, ask)
        note_parts: List[str] = []
        if shortable_shares is None and shortable_level is None:
            note_parts.append("ibkr_shortable_missing")
        if spread is None:
            note_parts.append("ibkr_bid_ask_missing")

        source = f"ibkr_socket_api:type_{market_data_type}"
        out[symbol] = {
            "safety": {
                "symbol": symbol,
                "locate_status": _locate_status(shortable_shares, shortable_level),
                "ssr_status": "UNKNOWN",
                "spread_bps": _format_num(spread, digits=2),
                "has_uptick_data": "",
                "source": source,
                "shortable_shares": _format_num(shortable_shares, integer=True),
                "shortable_level": _format_num(shortable_level, digits=2),
                "bid": _format_num(bid, digits=4),
                "ask": _format_num(ask, digits=4),
                "note": ",".join(note_parts),
            },
            "borrow": {
                "symbol": symbol,
                "borrow_fee_bps": "",
                "source": source,
                "note": "IB socket API does not expose borrow fee rate directly.",
            },
            "meta": {
                "last_price": _ticker_last_price(ticker),
                "prev_close": _ticker_prev_close(ticker),
            },
        }
        try:
            _cancel_market_data(ib, ticker, contract)
        except Exception:
            pass

    return out


def _merge_missing_market_rows(base_rows: Dict[str, Dict[str, Any]], fallback_rows: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    merged = dict(base_rows)
    for symbol, fallback in fallback_rows.items():
        current = merged.get(symbol)
        if current is None:
            merged[symbol] = fallback
            continue

        current_safety = dict(current["safety"])
        fallback_safety = dict(fallback["safety"])
        filled_any = False
        for key in ("locate_status", "spread_bps", "shortable_shares", "shortable_level", "bid", "ask"):
            if current_safety.get(key) in ("", "UNKNOWN") and fallback_safety.get(key) not in ("", "UNKNOWN"):
                current_safety[key] = fallback_safety.get(key)
                filled_any = True
        if filled_any:
            current_safety["source"] = str(fallback_safety.get("source") or current_safety.get("source") or "")
            note_parts = [str(x) for x in [current_safety.get("note"), f"fallback_from:{fallback_safety.get('source', '')}"] if str(x).strip()]
            current_safety["note"] = ",".join(dict.fromkeys(note_parts))
        merged[symbol] = {
            "safety": current_safety,
            "borrow": current["borrow"],
            "meta": dict(current.get("meta", {}) or {}),
        }
    return merged


def main() -> None:
    args = parse_args()
    market_code = resolve_market_code(getattr(args, "market", ""))
    explicit_cfg = str(args.ibkr_config) if str(args.ibkr_config) != "config/ibkr.yaml" or not market_code else ""
    ibkr_cfg_path = market_config_path(BASE_DIR, market_code, explicit_cfg)
    ibkr_cfg = _load_yaml(str(ibkr_cfg_path))
    market_cfg = load_market_universe_config(BASE_DIR, market_code)
    risk_cfg = _load_yaml(str(ibkr_cfg.get("risk_config", "config/risk.yaml")))

    risk_context = dict(risk_cfg.get("risk_context") or {})
    short_safety = dict(risk_cfg.get("short_safety") or {})
    remote_sources = list(risk_cfg.get("short_data_sources", []) or [])
    rule201_cfg = dict(short_safety.get("ssr_rule201", {}) or {})
    borrow_out = _resolve_project_path(str(risk_context.get("short_borrow_fee_file", "") or "config/reference/short_borrow_fee.csv"))
    safety_out = _resolve_project_path(str(short_safety.get("short_safety_file", "") or "config/reference/short_safety_rules.csv"))

    symbols = _symbol_pool(args, ibkr_cfg, market_cfg, market_code)
    if not symbols:
        log.warning("No symbols resolved for short safety sync.")
        _write_csv(borrow_out, [], BORROW_FIELDS)
        _write_csv(safety_out, [], SAFETY_FIELDS)
        return

    conn = IBKRConnection(ibkr_cfg["host"], int(ibkr_cfg["port"]), int(ibkr_cfg["client_id"]))
    ib = conn.connect()
    preferred_type = int(args.market_data_type)
    fallback_type = int(args.fallback_market_data_type)
    if (
        str(ibkr_cfg.get("mode", "")).strip().lower() == "paper"
        and not _market_data_type_selected_by_user()
        and preferred_type == 1
        and fallback_type == 4
    ):
        preferred_type = 4
        fallback_type = 4
        log.info("Paper mode detected without explicit market data override; using delayed-frozen market data by default.")
    allow_fallback = not bool(args.no_delayed_fallback)
    safety_rows: List[Dict[str, Any]] = []
    borrow_rows: List[Dict[str, Any]] = []

    try:
        contracts = {symbol: make_stock_contract(symbol) for symbol in symbols}
        try:
            ib.qualifyContracts(*list(contracts.values()))
        except Exception as e:
            log.warning("bulk qualifyContracts failed: %s %s", type(e).__name__, e)
        for symbol, contract in list(contracts.items()):
            if getattr(contract, "conId", 0):
                continue
            try:
                qualified = ib.qualifyContracts(contract)
                if qualified:
                    contracts[symbol] = qualified[0]
            except Exception as e:
                log.warning("qualify contract failed: symbol=%s error=%s %s", symbol, type(e).__name__, e)

        for batch_syms in _batch(symbols, int(args.batch_size)):
            batch_rows = _collect_batch_rows(
                ib,
                contracts,
                batch_syms,
                snapshot_wait_sec=float(args.snapshot_wait_sec),
                market_data_type=preferred_type,
                generic_tick_list=str(args.generic_tick_list),
            )
            if allow_fallback and fallback_type != preferred_type:
                fallback_syms = [
                    symbol
                    for symbol in batch_syms
                    if not _row_has_market_signal(batch_rows.get(symbol, {}).get("safety", {}))
                ]
                if fallback_syms:
                    fallback_rows = _collect_batch_rows(
                        ib,
                        contracts,
                        fallback_syms,
                        snapshot_wait_sec=float(args.snapshot_wait_sec),
                        market_data_type=fallback_type,
                        generic_tick_list=str(args.generic_tick_list),
                    )
                    batch_rows = _merge_missing_market_rows(batch_rows, fallback_rows)

            if remote_sources:
                remote_rows = fetch_remote_short_data(batch_syms, remote_sources, market=market_code)
                batch_rows = _merge_remote_data(batch_rows, remote_rows)

            batch_rows = _apply_rule201_ssr(batch_rows, market_code=market_code, rule_cfg=rule201_cfg)

            for symbol in batch_syms:
                pair = batch_rows.get(symbol)
                if pair is None:
                    safety_row, borrow_row = _empty_rows(symbol, "ibkr_socket_api", "batch_row_missing")
                else:
                    safety_row = pair["safety"]
                    borrow_row = pair["borrow"]
                safety_rows.append(safety_row)
                borrow_rows.append(borrow_row)

        _write_csv(borrow_out, borrow_rows, BORROW_FIELDS)
        _write_csv(safety_out, safety_rows, SAFETY_FIELDS)
        log.info("Wrote borrow reference -> %s rows=%s", borrow_out, len(borrow_rows))
        log.info("Wrote short safety reference -> %s rows=%s", safety_out, len(safety_rows))
    finally:
        try:
            if ib.isConnected():
                ib.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
