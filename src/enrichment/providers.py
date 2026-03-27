from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import os
import datetime as dt
import json
import logging
import hashlib
from contextlib import contextmanager
from pathlib import Path

from ..common.env import load_project_env

log = logging.getLogger(__name__)
load_project_env()

KNOWN_NON_EARNINGS_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "IVV",
    "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XBI",
    "ARKK", "SMH", "KWEB", "FXI", "EWH", "TLT", "IEF", "GLD", "SLV",
}


@dataclass
class EnrichmentBundle:
    asof_utc: str
    earnings: Dict[str, Dict[str, Any]]
    macro_events: List[Dict[str, Any]]
    markets: Dict[str, Any]
    market_news: List[Dict[str, Any]]
    fundamentals: Dict[str, Dict[str, Any]]
    macro_indicators: Dict[str, Any]


class EnrichmentProviders:
    """Web data enrichment (optional).

    Designed to be robust:
    - Uses yfinance if installed for earnings + index/sector/VIX moves.
    - Uses TradingEconomics API if TE_API_KEY is present for macro calendar.
    - If a provider isn't available, returns empty data but does not fail the batch.
    """

    def __init__(self):
        self._has_yf = False
        self._finnhub_warned: set[tuple[str, str]] = set()
        try:
            import yfinance as yf  # noqa: F401
            self._has_yf = True
        except Exception:
            self._has_yf = False

    @staticmethod
    def _utc_now() -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    @staticmethod
    def _normalize_yfinance_symbol(raw_symbol: str) -> str:
        symbol = str(raw_symbol or "").upper().strip()
        if not symbol:
            return symbol
        if symbol.endswith(".HK") or symbol.startswith("^"):
            return symbol
        if "." in symbol:
            base, suffix = symbol.rsplit(".", 1)
            if base and len(suffix) == 1 and suffix.isalpha():
                return f"{base}-{suffix}"
        return symbol

    @staticmethod
    def _normalize_finnhub_symbol(raw_symbol: str) -> str:
        return str(raw_symbol or "").upper().strip()

    @classmethod
    def _symbol_market_hint(cls, raw_symbol: str) -> str:
        symbol = cls._normalize_finnhub_symbol(raw_symbol)
        if not symbol:
            return ""
        if symbol.startswith("^"):
            return "INDEX"
        if symbol.endswith(".SS") or symbol.endswith(".SZ") or symbol.startswith("CN:") or symbol.startswith("SSE:") or symbol.startswith("SZSE:"):
            return "CN"
        if symbol.endswith(".HK") or symbol.isdigit():
            return "HK"
        if symbol.endswith(".DE") or symbol.startswith("XETRA:") or symbol.startswith("DE:"):
            return "XETRA"
        if symbol.endswith(".AX") or symbol.startswith("ASX:") or symbol.startswith("AU:"):
            return "ASX"
        if symbol.endswith(".L") or symbol.startswith("LSE:"):
            return "UK"
        return "US"

    @classmethod
    def _finnhub_enabled_for_market(cls, market: str) -> bool:
        return str(market or "").upper().strip() == "US"

    @classmethod
    def _finnhub_enabled_for_symbol(cls, raw_symbol: str) -> bool:
        return cls._symbol_market_hint(raw_symbol) == "US"

    @classmethod
    def _finnhub_symbol_variants(cls, raw_symbol: str) -> List[str]:
        symbol = cls._normalize_finnhub_symbol(raw_symbol)
        if not symbol:
            return []
        variants = [symbol]
        if symbol.endswith(".DE"):
            base = symbol[:-3]
            variants.extend([base, f"{base}-DE", f"XETRA:{base}"])
        elif symbol.endswith(".HK"):
            base = symbol[:-3]
            trimmed = base.lstrip("0") or base
            variants.extend([base, trimmed, f"{trimmed}.HK"])
        elif symbol.endswith(".L"):
            base = symbol[:-2]
            variants.extend([base, f"{base}-L", f"LSE:{base}"])
        deduped: List[str] = []
        seen: set[str] = set()
        for item in variants:
            key = str(item or "").strip().upper()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(key)
        return deduped

    @staticmethod
    def _finnhub_api_key() -> str:
        return str(os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_KEY") or "").strip()

    @staticmethod
    def _finnhub_webhook_secret() -> str:
        return str(os.getenv("FINNHUB_WEBHOOK_SECRET") or os.getenv("FINNHUB_SECRET") or "").strip()

    @staticmethod
    def _finnhub_cache_dir() -> Path:
        path = Path(__file__).resolve().parents[2] / ".cache" / "finnhub"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _generic_cache_dir() -> Path:
        path = Path(__file__).resolve().parents[2] / ".cache" / "enrichment"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _finnhub_cache_path(self, path: str, params: Dict[str, Any]) -> Path:
        raw = json.dumps({"path": str(path), "params": dict(params)}, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return self._finnhub_cache_dir() / f"{digest}.json"

    def _generic_cache_path(self, namespace: str, key: Dict[str, Any]) -> Path:
        raw = json.dumps({"namespace": str(namespace), "key": dict(key)}, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return self._generic_cache_dir() / f"{digest}.json"

    def _read_generic_cache(self, namespace: str, key: Dict[str, Any], ttl_sec: int) -> Any:
        if int(ttl_sec or 0) <= 0:
            return None
        cache_path = self._generic_cache_path(namespace, key)
        if not cache_path.exists():
            return None
        age_sec = (self._utc_now() - dt.datetime.fromtimestamp(cache_path.stat().st_mtime, tz=dt.timezone.utc)).total_seconds()
        if age_sec > max(0, int(ttl_sec)):
            return None
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _write_generic_cache(self, namespace: str, key: Dict[str, Any], payload: Any) -> None:
        cache_path = self._generic_cache_path(namespace, key)
        try:
            cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            return None

    def _read_finnhub_cache(self, path: str, params: Dict[str, Any], ttl_sec: int) -> Any:
        cache_path = self._finnhub_cache_path(path, params)
        if not cache_path.exists():
            return None
        age_sec = (self._utc_now() - dt.datetime.fromtimestamp(cache_path.stat().st_mtime, tz=dt.timezone.utc)).total_seconds()
        if age_sec > max(0, int(ttl_sec)):
            return None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if isinstance(payload, dict) and str(payload.get("_kind") or "") == "error":
            return None
        return payload

    def _write_finnhub_cache(self, path: str, params: Dict[str, Any], payload: Any) -> None:
        cache_path = self._finnhub_cache_path(path, params)
        try:
            cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            return None

    def _finnhub_get(self, path: str, params: Dict[str, Any], *, cache_ttl_sec: int = 0) -> Any:
        api_key = self._finnhub_api_key()
        if not api_key:
            return None
        if int(cache_ttl_sec or 0) > 0:
            cached = self._read_finnhub_cache(path, params, int(cache_ttl_sec))
            if cached is not None:
                return cached
        try:
            import requests  # type: ignore
        except Exception:
            return None
        request_params = dict(params)
        request_params["token"] = api_key
        try:
            resp = requests.get(f"https://finnhub.io/api/v1/{path.lstrip('/')}", params=request_params, timeout=12)
            resp.raise_for_status()
            payload = resp.json()
            if int(cache_ttl_sec or 0) > 0:
                self._write_finnhub_cache(path, params, payload)
            return payload
        except Exception as e:
            status = ""
            detail = ""
            response = getattr(e, "response", None)
            if response is not None:
                try:
                    status = str(getattr(response, "status_code", "") or "")
                except Exception:
                    status = ""
                try:
                    detail = str(getattr(response, "text", "") or "").strip().replace("\n", " ")[:180]
                except Exception:
                    detail = ""
            warn_key = (str(path), status or type(e).__name__)
            if warn_key not in self._finnhub_warned:
                self._finnhub_warned.add(warn_key)
                log.warning(
                    "finnhub request failed path=%s status=%s error=%s detail=%s",
                    path,
                    status or "-",
                    type(e).__name__,
                    detail or "-",
                )
            if int(cache_ttl_sec or 0) > 0 and status in {"403", "429"}:
                self._write_finnhub_cache(
                    path,
                    params,
                    {"_kind": "error", "status": status, "detail": detail, "ts": self._utc_now().isoformat()},
                )
            return None

    @staticmethod
    def _skip_earnings_lookup(raw_symbol: str) -> bool:
        symbol = str(raw_symbol or "").upper().strip()
        if not symbol:
            return True
        if symbol.startswith("^"):
            return True
        return symbol in KNOWN_NON_EARNINGS_SYMBOLS

    @staticmethod
    @contextmanager
    def _suppress_yfinance_logger():
        yf_logger = logging.getLogger("yfinance")
        old_level = yf_logger.level
        try:
            yf_logger.setLevel(logging.CRITICAL)
            yield
        finally:
            yf_logger.setLevel(old_level)

    def fetch_earnings_calendar(self, symbols: List[str], days_ahead: int = 14) -> Dict[str, Dict[str, Any]]:
        """Return next earnings date per symbol (best-effort).

        Notes:
        - yfinance earnings data can be incomplete or occasionally missing.
        - We treat missing as unknown, not safe.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if not self._has_yf:
            return out

        import yfinance as yf  # type: ignore

        now = self._utc_now().date()
        end = now + dt.timedelta(days=int(days_ahead))

        for s in symbols:
            s = str(s).upper().strip()
            if not s:
                continue
            cached = self._read_generic_cache(
                "earnings_calendar",
                {"symbol": s, "days_ahead": int(days_ahead)},
                ttl_sec=6 * 3600,
            )
            if isinstance(cached, dict):
                out[s] = dict(cached)
                continue
            info = {"next_earnings_date": None, "in_window": False, "source": "yfinance", "note": ""}
            if self._skip_earnings_lookup(s):
                info["source"] = "static_skip"
                info["note"] = "earnings_not_applicable_etf_or_index"
                out[s] = info
                self._write_generic_cache("earnings_calendar", {"symbol": s, "days_ahead": int(days_ahead)}, info)
                continue
            earnings_calendar: List[Dict[str, Any]] = []
            if self._finnhub_enabled_for_symbol(s):
                for finnhub_symbol in self._finnhub_symbol_variants(s):
                    finnhub_rows = self._finnhub_get(
                        "calendar/earnings",
                        {"from": str(now), "to": str(end), "symbol": finnhub_symbol},
                        cache_ttl_sec=6 * 3600,
                    )
                    earnings_calendar = list((finnhub_rows or {}).get("earningsCalendar") or []) if isinstance(finnhub_rows, dict) else []
                    if earnings_calendar:
                        break
            if earnings_calendar:
                future = []
                for row in earnings_calendar:
                    try:
                        dd = dt.date.fromisoformat(str(row.get("date") or "").strip())
                    except Exception:
                        continue
                    if dd >= now:
                        future.append(dd)
                if future:
                    nxt = min(future)
                    info["next_earnings_date"] = str(nxt)
                    info["in_window"] = (now <= nxt <= end)
                    info["source"] = "finnhub"
                    out[s] = info
                    self._write_generic_cache("earnings_calendar", {"symbol": s, "days_ahead": int(days_ahead)}, info)
                    continue
            try:
                yf_symbol = self._normalize_yfinance_symbol(s)
                t = yf.Ticker(yf_symbol)
                df = None
                with self._suppress_yfinance_logger():
                    try:
                        df = t.get_earnings_dates(limit=12)
                    except Exception:
                        df = None
                if df is not None and len(df) > 0:
                    # index is datetime-like
                    idx = list(df.index)
                    # choose the first date in the future (or latest if none)
                    future = []
                    for d in idx:
                        try:
                            dd = d.date()
                            if dd >= now:
                                future.append(dd)
                        except Exception:
                            continue
                    if future:
                        nxt = min(future)
                        info["next_earnings_date"] = str(nxt)
                        info["in_window"] = (now <= nxt <= end)
                    else:
                        # if none in future, keep last known
                        try:
                            dd = idx[0].date()
                            info["next_earnings_date"] = str(dd)
                        except Exception:
                            pass
                else:
                    info["note"] = "earnings_dates unavailable"
            except Exception as e:
                info["note"] = f"error: {type(e).__name__}"
            out[s] = info
            self._write_generic_cache("earnings_calendar", {"symbol": s, "days_ahead": int(days_ahead)}, info)

        return out

    def fetch_market_snapshot(self, market: str = "US") -> Dict[str, Any]:
        """Fetch index/sector/VIX snapshot (best-effort).

        Uses yfinance for a small market-specific benchmark set plus a global risk proxy.
        """
        out: Dict[str, Any] = {"source": None}
        market_code = str(market or "US").upper()
        cached = self._read_generic_cache("market_snapshot", {"market": market_code}, ttl_sec=1800)
        if isinstance(cached, dict):
            return dict(cached)
        if not self._has_yf:
            return out

        import yfinance as yf  # type: ignore

        market_tickers = {
            "US": [
                "^VIX", "SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "XLV", "XLE", "XLI",
                "XLP", "XLY", "XBI", "SMH", "TLT", "HYG", "IWF", "IWD", "ARKK",
            ],
            "CN": [
                "000300.SS", "510300.SS", "510500.SS", "159915.SZ", "512100.SS",
                "588000.SS", "600519.SS", "000858.SZ", "601318.SS", "300750.SZ",
            ],
            "HK": [
                "^VIX", "2800.HK", "2828.HK", "2822.HK", "3033.HK", "0700.HK",
                "9988.HK", "1810.HK", "0941.HK", "1299.HK",
            ],
            "ASX": [
                "^VIX", "VAS.AX", "STW.AX", "IOZ.AX", "A200.AX", "QFN.AX",
                "VHY.AX", "CBA.AX", "BHP.AX", "CSL.AX", "WES.AX", "MQG.AX",
            ],
            "XETRA": [
                "^VIX", "EXS1.DE", "EXV1.DE", "SXRV.DE", "SAP.DE", "SIE.DE",
                "ALV.DE", "IFX.DE", "MUV2.DE", "RWE.DE", "DB1.DE",
            ],
            "UK": ["^VIX", "ISF.L", "VOD.L", "HSBA.L", "SHEL.L", "AZN.L", "BARC.L", "BP.L"],
        }
        tickers = market_tickers.get(market_code, market_tickers["US"])
        try:
            data = yf.download(tickers=tickers, period="10d", interval="1d", group_by="ticker", auto_adjust=False, progress=False)
            snap: Dict[str, Any] = {"source": "yfinance", "tickers": {}}
            for tk in tickers:
                try:
                    df = data[tk] if isinstance(data.columns, type(getattr(data, "columns", None))) and tk in data.columns else None
                except Exception:
                    df = None
                try:
                    # yfinance download returns multi-index columns; easiest: use yf.Ticker history for each
                    h = yf.Ticker(tk).history(period="10d", interval="1d")
                    if h is None or len(h) < 2:
                        continue
                    closes = list(h["Close"].dropna().values)
                    last = float(closes[-1])
                    prev = float(closes[-2])
                    ret1 = (last / prev - 1.0) if prev else 0.0
                    ret5 = None
                    if len(closes) >= 6:
                        c5 = float(closes[-6])
                        ret5 = (last / c5 - 1.0) if c5 else 0.0
                    snap["tickers"][tk] = {"close": last, "ret1d": ret1, "ret5d": ret5}
                except Exception:
                    continue
            out = snap
        except Exception as e:
            out = {"source": "yfinance", "error": f"{type(e).__name__}"}

        if out:
            self._write_generic_cache("market_snapshot", {"market": market_code}, out)
        return out

    def fetch_market_news(self, market: str = "US", max_items: int = 8) -> List[Dict[str, Any]]:
        """Fetch a lightweight market-news digest for the selected market."""
        market_code = str(market or "US").upper()
        cached = self._read_generic_cache(
            "market_news",
            {"market": market_code, "max_items": int(max_items)},
            ttl_sec=1800,
        )
        if isinstance(cached, list):
            return [dict(item) for item in cached if isinstance(item, dict)]
        source_symbols = {
            "US": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
            "CN": ["510300.SS", "600519.SS", "000858.SZ", "601318.SS", "300750.SZ"],
            "HK": ["2800.HK", "0700.HK", "9988.HK", "0941.HK"],
            "ASX": ["VAS.AX", "CBA.AX", "BHP.AX", "CSL.AX"],
            "XETRA": ["EXS1.DE", "SAP.DE", "SIE.DE", "ALV.DE"],
            "UK": ["ISF.L", "AZN.L", "SHEL.L", "HSBA.L"],
        }
        symbols = source_symbols.get(market_code, source_symbols["US"])
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        today = self._utc_now().date()
        since = today - dt.timedelta(days=7)
        if self._finnhub_enabled_for_market(market_code):
            for raw_symbol in symbols:
                items = []
                for finnhub_symbol in self._finnhub_symbol_variants(raw_symbol):
                    maybe_items = self._finnhub_get(
                        "company-news",
                        {"symbol": finnhub_symbol, "from": str(since), "to": str(today)},
                        cache_ttl_sec=3 * 3600,
                    )
                    if maybe_items:
                        items = list(maybe_items or [])
                        break
                for item in list(items or []):
                    title = str(item.get("headline") or item.get("title") or "").strip()
                    if not title:
                        continue
                    dedupe_key = title.lower()
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    rows.append(
                        {
                            "symbol": str(raw_symbol).upper(),
                            "title": title,
                            "publisher": str(item.get("source") or item.get("publisher") or "").strip(),
                            "link": str(item.get("url") or "").strip(),
                            "published_utc": item.get("datetime") or item.get("publishedAt") or "",
                            "source": "finnhub",
                        }
                    )
                    if len(rows) >= max(1, int(max_items)):
                        self._write_generic_cache(
                            "market_news",
                            {"market": market_code, "max_items": int(max_items)},
                            rows,
                        )
                        return rows

        if not self._has_yf:
            return rows

        import yfinance as yf  # type: ignore

        for raw_symbol in symbols:
            yf_symbol = self._normalize_yfinance_symbol(raw_symbol)
            try:
                ticker = yf.Ticker(yf_symbol)
                news_items = list(getattr(ticker, "news", []) or [])
            except Exception:
                news_items = []
            for item in news_items:
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                dedupe_key = title.lower()
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                rows.append(
                    {
                        "symbol": str(raw_symbol).upper(),
                        "title": title,
                        "publisher": str(item.get("publisher") or item.get("source") or "").strip(),
                        "link": str(item.get("link") or item.get("canonicalUrl", {}).get("url") or "").strip(),
                        "published_utc": item.get("providerPublishTime") or item.get("pubDate") or "",
                        "source": "yfinance",
                    }
                )
                if len(rows) >= max(1, int(max_items)):
                    self._write_generic_cache(
                        "market_news",
                        {"market": market_code, "max_items": int(max_items)},
                        rows,
                    )
                    return rows
        if rows:
            self._write_generic_cache(
                "market_news",
                {"market": market_code, "max_items": int(max_items)},
                rows,
            )
        return rows

    def fetch_recommendation_trends(self, symbols: List[str], max_symbols: int = 20) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not self._finnhub_api_key():
            return out
        for raw_symbol in symbols[: max(1, int(max_symbols))]:
            cache_key = {"symbol": str(raw_symbol).upper().strip()}
            cached = self._read_generic_cache("recommendation_trends", cache_key, ttl_sec=12 * 3600)
            if isinstance(cached, dict):
                out[str(raw_symbol).upper().strip()] = dict(cached)
                continue
            if not self._finnhub_enabled_for_symbol(raw_symbol):
                continue
            variants = self._finnhub_symbol_variants(raw_symbol)
            if not variants:
                continue
            latest: Dict[str, Any] = {}
            for symbol in variants:
                rows = self._finnhub_get("stock/recommendation", {"symbol": symbol}, cache_ttl_sec=12 * 3600)
                latest = dict((rows or [{}])[0] or {}) if isinstance(rows, list) else {}
                if latest:
                    break
            if not latest:
                continue
            strong_buy = int(latest.get("strongBuy") or 0)
            buy = int(latest.get("buy") or 0)
            hold = int(latest.get("hold") or 0)
            sell = int(latest.get("sell") or 0)
            strong_sell = int(latest.get("strongSell") or 0)
            total = strong_buy + buy + hold + sell + strong_sell
            recommendation_score = 0.0
            if total > 0:
                recommendation_score = (
                    (strong_buy * 1.0 + buy * 0.5 - sell * 0.5 - strong_sell * 1.0) / float(total)
                )
            out[str(raw_symbol).upper().strip()] = {
                "recommendation_source": "finnhub",
                "recommendation_period": str(latest.get("period") or ""),
                "strong_buy": strong_buy,
                "buy": buy,
                "hold": hold,
                "sell": sell,
                "strong_sell": strong_sell,
                "recommendation_total": int(total),
                "recommendation_score": float(recommendation_score),
            }
            self._write_generic_cache(
                "recommendation_trends",
                cache_key,
                out[str(raw_symbol).upper().strip()],
            )
        return out

    def fetch_macro_calendar(self, days_ahead: int = 7) -> List[Dict[str, Any]]:
        """Fetch macro economic calendar events (optional).

        Provider: TradingEconomics (requires TE_API_KEY in env)
        - If no key: returns empty list
        """
        api_key = os.getenv("TE_API_KEY") or os.getenv("TRADING_ECONOMICS_API_KEY")
        if not api_key:
            return []

        # TradingEconomics supports guest keys and paid keys; we treat as opaque
        import requests  # type: ignore

        now = self._utc_now().date()
        end = now + dt.timedelta(days=int(days_ahead))

        url = "https://api.tradingeconomics.com/calendar"
        params = {
            "c": api_key,
            "d1": str(now),
            "d2": str(end),
            "format": "json",
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            js = r.json()
            out: List[Dict[str, Any]] = []
            for ev in js if isinstance(js, list) else []:
                # Normalize keys (best-effort)
                out.append({
                    "date": ev.get("Date") or ev.get("date"),
                    "country": ev.get("Country") or ev.get("country"),
                    "category": ev.get("Category") or ev.get("category"),
                    "event": ev.get("Event") or ev.get("event"),
                    "importance": ev.get("Importance") or ev.get("importance"),
                    "actual": ev.get("Actual") or ev.get("actual"),
                    "previous": ev.get("Previous") or ev.get("previous"),
                    "forecast": ev.get("Forecast") or ev.get("forecast"),
                    "source": "TradingEconomics"
                })
            return out
        except Exception as e:
            log.warning(f"macro calendar fetch failed: {type(e).__name__}")
            return []

    def fetch_macro_indicators(self) -> Dict[str, Any]:
        """Fetch a small stable macro indicator set via FRED when an API key is available."""
        cached = self._read_generic_cache("macro_indicators", {"scope": "default"}, ttl_sec=6 * 3600)
        if isinstance(cached, dict):
            return dict(cached)
        api_key = os.getenv("FRED_API_KEY") or os.getenv("FRED_KEY")
        if not api_key:
            return {}
        try:
            import requests  # type: ignore
        except Exception:
            return {}

        series_map = {
            "fed_funds": "FEDFUNDS",
            "unemployment_rate": "UNRATE",
            "cpi": "CPIAUCSL",
        }
        out: Dict[str, Any] = {"source": "FRED"}
        for label, series_id in series_map.items():
            try:
                r = requests.get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={
                        "series_id": series_id,
                        "api_key": api_key,
                        "file_type": "json",
                        "sort_order": "desc",
                        "limit": 1,
                    },
                    timeout=10,
                )
                r.raise_for_status()
                js = r.json() or {}
                obs = list(js.get("observations") or [])
                if not obs:
                    continue
                value = obs[0].get("value")
                out[label] = float(value) if value not in ("", ".", None) else None
            except Exception as e:
                out[f"{label}_note"] = f"error:{type(e).__name__}"
        if out:
            self._write_generic_cache("macro_indicators", {"scope": "default"}, out)
        return out

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _first_row(data: Any) -> Dict[str, Any]:
        rows = list(data or []) if isinstance(data, list) else []
        return dict(rows[0]) if rows else {}

    def fetch_fundamentals(self, symbols: List[str], max_symbols: int = 40) -> Dict[str, Dict[str, Any]]:
        """Fetch basic fundamentals for investment reporting (best-effort)."""
        out: Dict[str, Dict[str, Any]] = {}
        has_requests = True
        try:
            import requests  # type: ignore
        except Exception:
            has_requests = False
            requests = None  # type: ignore

        fmp_api_key = os.getenv("FMP_API_KEY") or os.getenv("FINANCIAL_MODELING_PREP_API_KEY")
        yf = None
        if self._has_yf:
            import yfinance as yf  # type: ignore

        for raw_symbol in symbols[: max(1, int(max_symbols))]:
            symbol = str(raw_symbol).upper().strip()
            if not symbol:
                continue
            cached = self._read_generic_cache("fundamentals", {"symbol": symbol}, ttl_sec=12 * 3600)
            if isinstance(cached, dict):
                out[symbol] = dict(cached)
                continue
            info: Dict[str, Any] = {"source": "yfinance"}
            if yf is not None:
                try:
                    ticker = yf.Ticker(self._normalize_yfinance_symbol(symbol))
                    fast = {}
                    try:
                        fast = dict(getattr(ticker, "fast_info", {}) or {})
                    except Exception:
                        fast = {}
                    meta = {}
                    try:
                        meta = dict(getattr(ticker, "info", {}) or {})
                    except Exception:
                        meta = {}
                    info.update(
                        {
                            "market_cap": float(fast.get("marketCap") or meta.get("marketCap") or 0.0),
                            "trailing_pe": float(meta.get("trailingPE") or 0.0),
                            "forward_pe": float(meta.get("forwardPE") or 0.0),
                            "dividend_yield": float(meta.get("dividendYield") or 0.0),
                            "beta": float(meta.get("beta") or 0.0),
                            "profit_margin": float(meta.get("profitMargins") or 0.0),
                            "operating_margin": float(meta.get("operatingMargins") or 0.0),
                            "gross_margin": float(meta.get("grossMargins") or 0.0),
                            "revenue_growth": float(meta.get("revenueGrowth") or 0.0),
                            "earnings_growth": float(meta.get("earningsQuarterlyGrowth") or 0.0),
                            "return_on_equity": float(meta.get("returnOnEquity") or 0.0),
                            "sector": str(meta.get("sector") or ""),
                            "industry": str(meta.get("industry") or ""),
                            "country": str(meta.get("country") or ""),
                            "currency": str(meta.get("currency") or ""),
                        }
                    )
                except Exception as e:
                    info["note"] = f"error:{type(e).__name__}"

            # Fallback to FMP stable endpoints when yfinance is missing or incomplete.
            needs_fmp = not info.get("market_cap") or not info.get("sector") or not info.get("industry")
            if fmp_api_key and has_requests and needs_fmp:
                try:
                    profile_resp = requests.get(
                        "https://financialmodelingprep.com/stable/profile",
                        params={"symbol": symbol, "apikey": fmp_api_key},
                        timeout=10,
                    )
                    profile_resp.raise_for_status()
                    profile_row = self._first_row(profile_resp.json())

                    ratios_resp = requests.get(
                        "https://financialmodelingprep.com/stable/ratios-ttm",
                        params={"symbol": symbol, "apikey": fmp_api_key},
                        timeout=10,
                    )
                    ratios_resp.raise_for_status()
                    ratios_row = self._first_row(ratios_resp.json())

                    metrics_resp = requests.get(
                        "https://financialmodelingprep.com/stable/key-metrics-ttm",
                        params={"symbol": symbol, "apikey": fmp_api_key},
                        timeout=10,
                    )
                    metrics_resp.raise_for_status()
                    metrics_row = self._first_row(metrics_resp.json())

                    info.update(
                        {
                            "source": "fmp_stable" if info.get("source") != "yfinance" else "yfinance+fmp_stable",
                            "market_cap": self._as_float(profile_row.get("marketCap") or info.get("market_cap") or 0.0),
                            "trailing_pe": self._as_float(
                                ratios_row.get("peRatioTTM") or profile_row.get("pe") or info.get("trailing_pe") or 0.0
                            ),
                            "forward_pe": self._as_float(info.get("forward_pe") or 0.0),
                            "dividend_yield": self._as_float(
                                ratios_row.get("dividendYielTTM")
                                or profile_row.get("lastDividend")
                                or info.get("dividend_yield")
                                or 0.0
                            ),
                            "beta": self._as_float(profile_row.get("beta") or info.get("beta") or 0.0),
                            "sector": str(profile_row.get("sector") or info.get("sector") or ""),
                            "industry": str(profile_row.get("industry") or info.get("industry") or ""),
                            "country": str(profile_row.get("country") or info.get("country") or ""),
                            "currency": str(profile_row.get("currency") or info.get("currency") or ""),
                            "profit_margin": self._as_float(
                                metrics_row.get("netProfitMarginTTM")
                                or ratios_row.get("netProfitMarginTTM")
                                or info.get("profit_margin")
                                or 0.0
                            ),
                            "operating_margin": self._as_float(
                                ratios_row.get("operatingProfitMarginTTM")
                                or metrics_row.get("operatingMarginTTM")
                                or info.get("operating_margin")
                                or 0.0
                            ),
                            "gross_margin": self._as_float(
                                ratios_row.get("grossProfitMarginTTM")
                                or info.get("gross_margin")
                                or 0.0
                            ),
                            "revenue_growth": self._as_float(
                                metrics_row.get("revenueGrowthTTM")
                                or info.get("revenue_growth")
                                or 0.0
                            ),
                            "return_on_equity": self._as_float(
                                metrics_row.get("returnOnEquityTTM")
                                or ratios_row.get("returnOnEquityTTM")
                                or metrics_row.get("roeTTM")
                                or info.get("return_on_equity")
                                or 0.0
                            ),
                            "price_to_book": self._as_float(
                                ratios_row.get("priceToBookRatioTTM")
                                or metrics_row.get("pbRatioTTM")
                                or 0.0
                            ),
                            "roe": self._as_float(
                                metrics_row.get("returnOnEquityTTM")
                                or ratios_row.get("returnOnEquityTTM")
                                or metrics_row.get("roeTTM")
                                or info.get("roe")
                                or info.get("return_on_equity")
                                or 0.0
                            ),
                        }
                    )
                except Exception as e:
                    existing_note = str(info.get("note") or "").strip()
                    fmp_note = f"fmp_stable_error:{type(e).__name__}"
                    info["note"] = existing_note or fmp_note
            out[symbol] = info
            self._write_generic_cache("fundamentals", {"symbol": symbol}, info)
        return out

    def fetch_all(self, symbols: List[str], market: str = "US") -> EnrichmentBundle:
        now = self._utc_now().isoformat()
        earnings = self.fetch_earnings_calendar(symbols, days_ahead=14)
        macro = self.fetch_macro_calendar(days_ahead=7)
        macro_indicators = self.fetch_macro_indicators()
        markets = self.fetch_market_snapshot(market=market)
        market_news = self.fetch_market_news(market=market)
        return EnrichmentBundle(
            asof_utc=now,
            earnings=earnings,
            macro_events=macro,
            markets=markets,
            market_news=market_news,
            fundamentals={},
            macro_indicators=macro_indicators,
        )

    def collect(self, symbols: List[str], market: str = "US") -> Dict[str, Any]:
        """
        Backward-compatible wrapper used by older tools.
        """
        bundle = self.fetch_all(symbols, market=market)
        return {
            "asof_utc": bundle.asof_utc,
            "earnings": bundle.earnings,
            "macro_events": bundle.macro_events,
            "markets": bundle.markets,
            "market_news": bundle.market_news,
            "fundamentals": bundle.fundamentals,
            "macro_indicators": bundle.macro_indicators,
        }
