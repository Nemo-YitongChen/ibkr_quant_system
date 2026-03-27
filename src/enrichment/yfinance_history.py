from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import List

from ..common.logger import get_logger
from ..ibkr.market_data import OHLCVBar
from .providers import EnrichmentProviders

log = get_logger("enrichment.yfinance_history")

_CACHE_DIR = Path(__file__).resolve().parents[2] / ".cache" / "yfinance_history"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _daily_period_for_days(days: int) -> str:
    lookback_days = max(30, int(days))
    if lookback_days >= 252 * 8:
        return "10y"
    if lookback_days >= 252 * 4:
        return "5y"
    if lookback_days >= 252 * 2:
        return "2y"
    return "1y"


def _daily_period_fallbacks(days: int) -> List[str]:
    preferred = _daily_period_for_days(days)
    order = ["1y", "2y", "5y", "10y"]
    start_idx = order.index(preferred) if preferred in order else 0
    return order[start_idx:]


def _history_cache_path(symbol: str, *, interval: str, period: str) -> Path:
    raw = json.dumps(
        {
            "symbol": EnrichmentProviders._normalize_yfinance_symbol(symbol),
            "interval": str(interval or ""),
            "period": str(period or ""),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return _CACHE_DIR / f"{digest}.json"


def _read_history_cache(symbol: str, *, interval: str, period: str, ttl_sec: int) -> List[OHLCVBar]:
    if int(ttl_sec or 0) <= 0:
        return []
    cache_path = _history_cache_path(symbol, interval=interval, period=period)
    if not cache_path.exists():
        return []
    try:
        age_sec = max(0.0, time.time() - cache_path.stat().st_mtime)
    except Exception:
        age_sec = float(ttl_sec) + 1.0
    if age_sec > max(0, int(ttl_sec)):
        return []
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows: List[OHLCVBar] = []
    for item in list(payload.get("bars") or []):
        try:
            t = datetime.fromisoformat(str(item.get("time") or ""))
        except Exception:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        rows.append(
            OHLCVBar(
                time=t,
                open=float(item.get("open", 0.0) or 0.0),
                high=float(item.get("high", 0.0) or 0.0),
                low=float(item.get("low", 0.0) or 0.0),
                close=float(item.get("close", 0.0) or 0.0),
                volume=float(item.get("volume", 0.0) or 0.0),
            )
        )
    return rows


def _read_stale_history_cache(symbol: str, *, interval: str, periods: List[str]) -> List[OHLCVBar]:
    # stale cache 只在 fresh cache 和在线请求都拿不到数据时兜底使用。
    # 对 snapshot labeling 这类离线回标任务来说，较旧的本地历史通常仍然比“完全没有样本”更有价值。
    best_rows: List[OHLCVBar] = []
    for period in list(periods or []):
        cache_path = _history_cache_path(symbol, interval=interval, period=str(period or ""))
        if not cache_path.exists():
            continue
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows: List[OHLCVBar] = []
        for item in list(payload.get("bars") or []):
            try:
                t = datetime.fromisoformat(str(item.get("time") or ""))
            except Exception:
                continue
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            rows.append(
                OHLCVBar(
                    time=t,
                    open=float(item.get("open", 0.0) or 0.0),
                    high=float(item.get("high", 0.0) or 0.0),
                    low=float(item.get("low", 0.0) or 0.0),
                    close=float(item.get("close", 0.0) or 0.0),
                    volume=float(item.get("volume", 0.0) or 0.0),
                )
            )
        if len(rows) > len(best_rows):
            best_rows = rows
    return best_rows


def _write_history_cache(symbol: str, *, interval: str, period: str, bars: List[OHLCVBar]) -> None:
    cache_path = _history_cache_path(symbol, interval=interval, period=period)
    payload = {
        "bars": [
            {
                "time": bar.time.astimezone(timezone.utc).isoformat(),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            }
            for bar in list(bars or [])
        ]
    }
    try:
        cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return None


def fetch_daily_bars(symbol: str, days: int, *, allow_stale_cache: bool = False) -> List[OHLCVBar]:
    """Best-effort daily history fallback via yfinance.

    This is used only when IBKR historical daily bars are unavailable for a
    market. It keeps the investment research pipeline alive without pretending
    to be a realtime or broker-grade feed.
    """
    yf_symbol = EnrichmentProviders._normalize_yfinance_symbol(symbol)
    lookback_days = max(30, int(days))
    period = _daily_period_for_days(lookback_days)
    cached = _read_history_cache(symbol, interval="1d", period=period, ttl_sec=1800)
    if cached:
        return list(cached[-lookback_days:])
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        stale = _read_stale_history_cache(symbol, interval="1d", periods=_daily_period_fallbacks(lookback_days)) if allow_stale_cache else []
        if stale:
            log.info("using stale yfinance daily cache for %s after import failure", symbol)
            return list(stale[-lookback_days:])
        return []
    try:
        hist = yf.Ticker(yf_symbol).history(period=period, interval="1d", auto_adjust=False)
    except Exception as e:
        log.warning("yfinance daily fallback failed for %s: %s %s", symbol, type(e).__name__, e)
        hist = None

    rows: List[OHLCVBar] = []
    if hist is None or len(hist) == 0:
        stale = _read_stale_history_cache(symbol, interval="1d", periods=_daily_period_fallbacks(lookback_days)) if allow_stale_cache else []
        if stale:
            log.info("using stale yfinance daily cache for %s after empty online history", symbol)
            return list(stale[-lookback_days:])
        return rows
    for idx, row in hist.tail(lookback_days).iterrows():
        try:
            t = idx.to_pydatetime()
        except Exception:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        rows.append(
            OHLCVBar(
                time=t,
                open=float(row.get("Open", 0.0) or 0.0),
                high=float(row.get("High", 0.0) or 0.0),
                low=float(row.get("Low", 0.0) or 0.0),
                close=float(row.get("Close", 0.0) or 0.0),
                volume=float(row.get("Volume", 0.0) or 0.0),
            )
        )
    if rows:
        _write_history_cache(symbol, interval="1d", period=period, bars=rows)
    return rows


def fetch_intraday_bars(symbol: str, interval: str = "5m", days: int = 5) -> List[OHLCVBar]:
    """Best-effort intraday history fallback via yfinance.

    Intended for medium/long-term intraday refinement only. This is not a
    broker-grade realtime feed and should be treated as a delayed/fallback
    source for opportunity and guard checks.
    """
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return []

    yf_symbol = EnrichmentProviders._normalize_yfinance_symbol(symbol)
    use_days = max(1, min(int(days or 1), 60))
    if use_days <= 5:
        period = "5d"
    elif use_days <= 30:
        period = "1mo"
    else:
        period = "60d"
    cached = _read_history_cache(symbol, interval=str(interval or "5m"), period=period, ttl_sec=300)
    if cached:
        return list(cached)
    try:
        hist = yf.Ticker(yf_symbol).history(period=period, interval=str(interval or "5m"), auto_adjust=False)
    except Exception as e:
        log.warning("yfinance intraday fallback failed for %s: %s %s", symbol, type(e).__name__, e)
        return []

    rows: List[OHLCVBar] = []
    if hist is None or len(hist) == 0:
        return rows
    for idx, row in hist.iterrows():
        try:
            t = idx.to_pydatetime()
        except Exception:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        rows.append(
            OHLCVBar(
                time=t,
                open=float(row.get("Open", 0.0) or 0.0),
                high=float(row.get("High", 0.0) or 0.0),
                low=float(row.get("Low", 0.0) or 0.0),
                close=float(row.get("Close", 0.0) or 0.0),
                volume=float(row.get("Volume", 0.0) or 0.0),
            )
        )
    if rows:
        _write_history_cache(symbol, interval=str(interval or "5m"), period=period, bars=rows)
    return rows
