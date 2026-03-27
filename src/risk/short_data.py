from __future__ import annotations

import csv
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import urlopen

import requests

from ..common.env import load_project_env
from ..common.logger import get_logger
from ..ibkr.contracts import parse_stock_spec

log = get_logger("risk.short_data")
load_project_env()
DEFAULT_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


def _normalize_symbol(raw: Any, market: str = "") -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    market = str(market or "").upper()
    if market == "HK":
        spec = parse_stock_spec(s, default_exchange="SEHK", default_currency="HKD")
        code = str(spec.symbol or "").strip()
        if code.isdigit():
            return f"{int(code):04d}.HK"
        return f"{code}.HK" if code else ""
    if market in ("", "US"):
        if "-" in s:
            base, suffix = s.rsplit("-", 1)
            if base and len(suffix) == 1 and suffix.isalpha():
                return f"{base}.{suffix}"
        if "/" in s:
            base, suffix = s.rsplit("/", 1)
            if base and len(suffix) == 1 and suffix.isalpha():
                return f"{base}.{suffix}"
        if " " in s:
            base, suffix = s.rsplit(" ", 1)
            if base and len(suffix) == 1 and suffix.isalpha():
                return f"{base}.{suffix}"
    return s


def _normalize_bool(raw: Any) -> bool | None:
    if raw in (None, ""):
        return None
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _normalize_ssr(raw: Any) -> str:
    if raw in (None, ""):
        return "UNKNOWN"
    s = str(raw).strip().upper()
    if s in ("1", "TRUE", "YES", "Y", "ON", "ACTIVE", "SSR", "RESTRICTED"):
        return "ON"
    if s in ("0", "FALSE", "NO", "N", "OFF", "CLEAR", "NONE"):
        return "OFF"
    return "UNKNOWN"


def _normalize_locate(raw: Any) -> str:
    if raw in (None, ""):
        return "UNKNOWN"
    s = str(raw).strip().upper()
    if s in ("AVAILABLE", "LOCATED", "YES", "Y", "OPEN", "OK"):
        return "AVAILABLE"
    if s in ("UNAVAILABLE", "BLOCKED", "NO", "N", "CLOSED"):
        return "UNAVAILABLE"
    return "UNKNOWN"


def _safe_float(raw: Any) -> float | None:
    try:
        if raw in (None, ""):
            return None
        return float(raw)
    except Exception:
        return None


def _normalize_fee_bps(raw: Any, unit: str) -> float | None:
    value = _safe_float(raw)
    if value is None:
        return None
    unit = str(unit or "bps").strip().lower()
    if unit == "bps":
        return float(value)
    if unit in ("pct", "percent", "%"):
        return float(value) * 100.0
    if unit in ("decimal", "ratio"):
        return float(value) * 10_000.0
    raise ValueError(f"unsupported borrow_fee_unit={unit}")


def _to_text(payload: bytes, encoding: str) -> str:
    try:
        return payload.decode(encoding)
    except Exception:
        return payload.decode("utf-8", errors="replace")


def _apply_env_headers(headers: Dict[str, Any], header_env: Dict[str, Any]) -> Dict[str, str]:
    out = {str(k): str(v) for k, v in dict(headers or {}).items()}
    for header_name, env_name in dict(header_env or {}).items():
        env_val = os.getenv(str(env_name))
        if env_val:
            out[str(header_name)] = env_val
    return out


def _build_url(url: str, extra_params: Dict[str, Any], param_env: Dict[str, Any]) -> str:
    params = {str(k): str(v) for k, v in dict(extra_params or {}).items()}
    for param_name, env_name in dict(param_env or {}).items():
        env_val = os.getenv(str(env_name))
        if env_val:
            params[str(param_name)] = env_val
    if not params:
        return url
    parts = list(urlparse(url))
    existing = parts[4]
    extra_qs = urlencode(params)
    parts[4] = f"{existing}&{extra_qs}" if existing else extra_qs
    return urlunparse(parts)


def _with_ftp_auth(url: str, username_env: str, password_env: str) -> str:
    if not username_env and not password_env:
        return url
    parsed = urlparse(url)
    if parsed.scheme.lower() != "ftp":
        return url
    if "@" in parsed.netloc:
        return url
    user = os.getenv(str(username_env or ""))
    pwd = os.getenv(str(password_env or ""))
    if not user:
        return url
    auth = user if not pwd else f"{user}:{pwd}"
    netloc = f"{auth}@{parsed.netloc}"
    return urlunparse(parsed._replace(netloc=netloc))


def _fetch_payload(source: Dict[str, Any]) -> bytes:
    url = str(source.get("url", "") or "").strip()
    if not url:
        raise ValueError("source url is required")
    timeout_sec = float(source.get("timeout_sec", 20.0))
    headers = _apply_env_headers(source.get("headers", {}), source.get("headers_from_env", {}))
    url = _build_url(url, source.get("params", {}), source.get("params_from_env", {}))
    scheme = urlparse(url).scheme.lower()

    if scheme in ("http", "https"):
        r = requests.get(url, headers=headers, timeout=timeout_sec)
        r.raise_for_status()
        return r.content

    if scheme == "ftp":
        ftp_url = _with_ftp_auth(
            url,
            str(source.get("username_env", "") or ""),
            str(source.get("password_env", "") or ""),
        )
        with urlopen(ftp_url, timeout=timeout_sec) as resp:
            return resp.read()

    raise ValueError(f"unsupported source scheme={scheme}")


def _extract_rows(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = _fetch_payload(source)
    encoding = str(source.get("encoding", "utf-8") or "utf-8")
    text = _to_text(payload, encoding)
    fmt = str(source.get("format", "csv") or "csv").strip().lower()

    if fmt == "csv":
        delimiter = str(source.get("delimiter", ",") or ",")
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        return [dict(row) for row in reader if isinstance(row, dict)]

    if fmt == "json":
        data = json.loads(text) or []
        root_key = str(source.get("root_key", "") or "").strip()
        if root_key:
            for part in root_key.split("."):
                if isinstance(data, dict):
                    data = data.get(part, [])
                else:
                    data = []
                    break
        if isinstance(data, list):
            return [dict(row) for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            rows = data.get("rows")
            if isinstance(rows, list):
                return [dict(row) for row in rows if isinstance(row, dict)]
        raise ValueError("json source must resolve to a list of rows")

    raise ValueError(f"unsupported source format={fmt}")


def _provider_symbol(symbol: str, provider: str, market: str = "") -> str:
    sym = _normalize_symbol(symbol, market=market)
    provider = str(provider or "").strip().lower()
    if provider in ("yahoo_quote", "iborrowdesk"):
        return sym.replace(".", "-") if str(market or "").upper() in ("", "US") else sym
    return sym


def _fetch_json_http(url: str, *, timeout_sec: float, headers: Dict[str, str] | None = None, params: Dict[str, Any] | None = None) -> Any:
    r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout_sec)
    r.raise_for_status()
    return r.json()


def _provider_rows_yahoo_quote(source: Dict[str, Any], requested: Iterable[str], *, market: str = "") -> List[Dict[str, Any]]:
    symbols = [str(sym).upper().strip() for sym in requested if str(sym).strip()]
    if not symbols:
        return []

    timeout_sec = float(source.get("timeout_sec", 20.0))
    headers = {**DEFAULT_BROWSER_HEADERS, **_apply_env_headers(source.get("headers", {}), source.get("headers_from_env", {}))}
    params = {str(k): str(v) for k, v in dict(source.get("params", {}) or {}).items()}
    url = str(source.get("url", "") or "https://query1.finance.yahoo.com/v7/finance/quote").strip()
    batch_size = max(1, int(source.get("batch_size", 50)))
    out: List[Dict[str, Any]] = []

    for start in range(0, len(symbols), batch_size):
        batch = symbols[start:start + batch_size]
        request_symbols = [_provider_symbol(sym, "yahoo_quote", market=market) for sym in batch]
        payload = _fetch_json_http(
            url,
            timeout_sec=timeout_sec,
            headers=headers,
            params={**params, "symbols": ",".join(request_symbols)},
        )
        rows = list(((payload or {}).get("quoteResponse") or {}).get("result") or [])
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = _normalize_symbol(row.get("symbol"), market=market)
            if not symbol:
                continue
            bid = _safe_float(row.get("bid"))
            ask = _safe_float(row.get("ask"))
            spread_bps = None
            if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    spread_bps = ((ask - bid) / mid) * 10_000.0
            out.append(
                {
                    "symbol": symbol,
                    "spread_bps": spread_bps,
                    "note": str(row.get("marketState", "") or "").strip(),
                }
            )
    return out


def _latest_dict_candidate(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        for key in ("rows", "data", "history", "records", "daily"):
            rows = data.get(key)
            if isinstance(rows, list) and rows:
                for item in reversed(rows):
                    if isinstance(item, dict):
                        return item
        return data
    if isinstance(data, list):
        for item in reversed(data):
            if isinstance(item, dict):
                return item
    return {}


def _provider_rows_iborrowdesk(source: Dict[str, Any], requested: Iterable[str], *, market: str = "") -> List[Dict[str, Any]]:
    symbols = [str(sym).upper().strip() for sym in requested if str(sym).strip()]
    if not symbols:
        return []

    timeout_sec = float(source.get("timeout_sec", 20.0))
    url_template = str(source.get("url", "") or source.get("url_template", "") or "https://www.iborrowdesk.com/api/ticker/{symbol}").strip()
    max_consecutive_failures = max(1, int(source.get("max_consecutive_failures", 5)))
    out: List[Dict[str, Any]] = []
    consecutive_failures = 0

    for symbol in symbols:
        provider_symbol = _provider_symbol(symbol, "iborrowdesk", market=market)
        url = url_template.format(symbol=provider_symbol)
        headers = {
            **DEFAULT_BROWSER_HEADERS,
            "Referer": f"https://www.iborrowdesk.com/report/{provider_symbol}",
            **_apply_env_headers(source.get("headers", {}), source.get("headers_from_env", {})),
        }
        try:
            payload = _fetch_json_http(url, timeout_sec=timeout_sec, headers=headers)
        except Exception as e:
            consecutive_failures += 1
            log.warning("iborrowdesk fetch failed: symbol=%s error=%s %s", symbol, type(e).__name__, e)
            if consecutive_failures >= max_consecutive_failures:
                log.warning("iborrowdesk provider aborted after %s consecutive failures", consecutive_failures)
                break
            continue
        consecutive_failures = 0

        latest = _latest_dict_candidate(payload)
        if not latest:
            continue

        fee_raw = None
        for key in ("fee", "borrowFee", "borrow_fee", "feeRate", "rate"):
            if latest.get(key) not in (None, ""):
                fee_raw = latest.get(key)
                break
        fee_bps = _normalize_fee_bps(fee_raw, "pct") if fee_raw not in (None, "") else None

        available = None
        for key in ("available", "shares", "shortableShares", "shortable_shares"):
            if latest.get(key) not in (None, ""):
                available = _safe_float(latest.get(key))
                break
        locate_status = "UNKNOWN"
        if available is not None:
            locate_status = "AVAILABLE" if available > 0 else "UNAVAILABLE"

        note_parts = []
        for key in ("updated", "timestamp", "date"):
            if latest.get(key) not in (None, ""):
                note_parts.append(f"{key}={latest.get(key)}")

        out.append(
            {
                "symbol": symbol,
                "borrow_fee_bps": fee_bps,
                "locate_status": locate_status,
                "note": ",".join(note_parts),
            }
        )
    return out


def _extract_source_rows(source: Dict[str, Any], requested: Iterable[str], *, market: str = "") -> List[Dict[str, Any]]:
    provider = str(source.get("provider", "") or "").strip().lower()
    if provider == "yahoo_quote":
        return _provider_rows_yahoo_quote(source, requested, market=market)
    if provider == "iborrowdesk":
        return _provider_rows_iborrowdesk(source, requested, market=market)
    return _extract_rows(source)


@dataclass
class RemoteShortDataRecord:
    symbol: str
    borrow_fee_bps: float | None = None
    borrow_source: str = ""
    borrow_note: str = ""
    ssr_status: str = "UNKNOWN"
    ssr_source: str = ""
    locate_status: str = "UNKNOWN"
    locate_source: str = ""
    has_uptick_data: bool | None = None
    has_uptick_source: str = ""
    spread_bps: float | None = None
    spread_source: str = ""
    notes: List[str] = field(default_factory=list)


def fetch_remote_short_data(symbols: Iterable[str], raw_sources: Iterable[Dict[str, Any]], *, market: str = "") -> Dict[str, RemoteShortDataRecord]:
    requested = {_normalize_symbol(sym, market=market) for sym in symbols}
    requested.discard("")
    out: Dict[str, RemoteShortDataRecord] = {}

    for raw_source in raw_sources or []:
        source = dict(raw_source or {})
        if not bool(source.get("enabled", False)):
            continue
        provider = str(source.get("provider", "") or "").strip().lower()
        if provider == "yahoo_quote":
            source.setdefault("symbol_key", "symbol")
            source.setdefault("spread_bps_key", "spread_bps")
            source.setdefault("note_key", "note")
        elif provider == "iborrowdesk":
            source.setdefault("symbol_key", "symbol")
            source.setdefault("borrow_fee_key", "borrow_fee_bps")
            source.setdefault("borrow_fee_unit", "bps")
            source.setdefault("locate_key", "locate_status")
            source.setdefault("note_key", "note")

        source_name = str(source.get("name", "") or source.get("source", "") or source.get("url", "remote_source")).strip()
        try:
            rows = _extract_source_rows(source, requested, market=market)
        except Exception as e:
            log.warning("remote short data source failed: source=%s error=%s %s", source_name, type(e).__name__, e)
            continue

        symbol_key = str(source.get("symbol_key", "symbol") or "symbol")
        borrow_key = str(source.get("borrow_fee_key", "") or "").strip()
        borrow_unit = str(source.get("borrow_fee_unit", "bps") or "bps")
        ssr_key = str(source.get("ssr_key", "") or "").strip()
        locate_key = str(source.get("locate_key", "") or "").strip()
        uptick_key = str(source.get("has_uptick_key", "") or "").strip()
        spread_key = str(source.get("spread_bps_key", "") or "").strip()
        note_key = str(source.get("note_key", "") or "").strip()

        for row in rows:
            symbol = _normalize_symbol(row.get(symbol_key), market=market)
            if not symbol or (requested and symbol not in requested):
                continue

            rec = out.setdefault(symbol, RemoteShortDataRecord(symbol=symbol))
            if note_key:
                note_val = str(row.get(note_key, "") or "").strip()
                if note_val:
                    rec.notes.append(note_val)

            if borrow_key:
                fee_bps = _normalize_fee_bps(row.get(borrow_key), borrow_unit)
                if fee_bps is not None:
                    rec.borrow_fee_bps = fee_bps
                    rec.borrow_source = source_name
                elif row.get(borrow_key) in (None, ""):
                    rec.borrow_source = rec.borrow_source or f"unknown:{source_name}"
                    rec.borrow_note = rec.borrow_note or "borrow_fee_missing"

            if ssr_key:
                ssr_status = _normalize_ssr(row.get(ssr_key))
                if ssr_status != "UNKNOWN" or rec.ssr_status == "UNKNOWN":
                    rec.ssr_status = ssr_status
                    rec.ssr_source = source_name

            if locate_key:
                locate_status = _normalize_locate(row.get(locate_key))
                if locate_status != "UNKNOWN" or rec.locate_status == "UNKNOWN":
                    rec.locate_status = locate_status
                    rec.locate_source = source_name

            if uptick_key:
                uptick = _normalize_bool(row.get(uptick_key))
                if uptick is not None:
                    rec.has_uptick_data = uptick
                    rec.has_uptick_source = source_name

            if spread_key:
                spread_bps = _safe_float(row.get(spread_key))
                if spread_bps is not None:
                    rec.spread_bps = spread_bps
                    rec.spread_source = source_name

    return out
