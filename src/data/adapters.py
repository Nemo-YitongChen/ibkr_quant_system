from __future__ import annotations

from dataclasses import dataclass
import inspect
import threading
import time
from typing import Any, List, Tuple

from ..common.logger import get_logger
from ..enrichment.yfinance_history import fetch_daily_bars as fetch_daily_bars_yf
from ..enrichment.yfinance_history import fetch_intraday_bars as fetch_intraday_bars_yf
from ..ibkr.market_data import MarketDataService
from .models import BarData

log = get_logger("data.adapters")


def _to_bar_data(raw_bars: List[Any], source: str) -> List[BarData]:
    out: List[BarData] = []
    for bar in raw_bars or []:
        try:
            out.append(
                BarData(
                    time=bar.time,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(getattr(bar, "volume", 0.0) or 0.0),
                    source=source,
                )
            )
        except Exception:
            continue
    return out


def _is_expected_sync_ib_fallback(error: Exception) -> bool:
    text = str(error or "").strip().lower()
    return (
        "must run on marketdataservice owner thread" in text
        or "requires an event loop" in text
        or "there is no current event loop" in text
    )


@dataclass
class MarketDataAdapter:
    md: MarketDataService
    daily_cache_ttl_sec: int = 1800
    intraday_cache_ttl_sec: int = 120
    serialize_ib_requests: bool = True

    def __post_init__(self) -> None:
        self._daily_cache: dict[tuple[str, int], tuple[float, List[BarData], str]] = {}
        self._intraday_cache: dict[tuple[str, int, int], tuple[float, List[BarData], str]] = {}
        self._cache_lock = threading.Lock()
        self._ib_lock = threading.Lock()

    def _cache_get(
        self,
        cache: dict[tuple[Any, ...], tuple[float, List[BarData], str]],
        key: tuple[Any, ...],
        ttl_sec: int,
    ) -> tuple[List[BarData], str]:
        if int(ttl_sec or 0) <= 0:
            return [], ""
        now = time.time()
        with self._cache_lock:
            cached = cache.get(key)
            if not cached:
                return [], ""
            ts, bars, source = cached
            if (now - ts) > int(ttl_sec):
                cache.pop(key, None)
                return [], ""
            return list(bars), str(source or "")

    def _cache_put(
        self,
        cache: dict[tuple[Any, ...], tuple[float, List[BarData], str]],
        key: tuple[Any, ...],
        bars: List[BarData],
        source: str,
    ) -> None:
        with self._cache_lock:
            cache[key] = (time.time(), list(bars), str(source or ""))

    def register(self, symbol: str, contract: Any) -> None:
        self.md.register(symbol, contract)

    @staticmethod
    def _close_awaitable(value: Any) -> None:
        if inspect.iscoroutine(value):
            try:
                value.close()
            except Exception:
                return

    def get_daily_bars(self, symbol: str, days: int) -> Tuple[List[BarData], str]:
        key = (str(symbol).upper(), int(days))
        cached_bars, cached_source = self._cache_get(self._daily_cache, key, int(self.daily_cache_ttl_sec))
        if cached_bars:
            return cached_bars, cached_source
        try:
            if bool(self.serialize_ib_requests):
                with self._ib_lock:
                    bars = self.md.get_daily_bars(symbol, days=days)
            else:
                bars = self.md.get_daily_bars(symbol, days=days)
            if inspect.isawaitable(bars):
                self._close_awaitable(bars)
                raise RuntimeError("MarketDataService.get_daily_bars returned an awaitable in sync adapter path")
            if bars:
                converted = _to_bar_data(bars, "ibkr")
                self._cache_put(self._daily_cache, key, converted, "ibkr")
                return list(converted), "ibkr"
        except Exception as e:
            if _is_expected_sync_ib_fallback(e):
                log.info("daily bars use yfinance for %s in non-owner sync IB path: %s", symbol, e)
            else:
                log.warning("daily bars fallback to yfinance for %s: %s", symbol, e)
        bars = fetch_daily_bars_yf(symbol, days=days)
        if bars:
            converted = _to_bar_data(bars, "yfinance")
            self._cache_put(self._daily_cache, key, converted, "yfinance")
            return list(converted), "yfinance"
        return [], ""

    def get_5m_bars_with_source(self, symbol: str, need: int = 156, fallback_days: int = 5) -> Tuple[List[BarData], str]:
        key = (str(symbol).upper(), int(need), int(fallback_days))
        cached_bars, cached_source = self._cache_get(self._intraday_cache, key, int(self.intraday_cache_ttl_sec))
        if cached_bars:
            return cached_bars, cached_source
        try:
            if bool(self.serialize_ib_requests):
                with self._ib_lock:
                    bars = self.md.get_5m_bars(symbol, need=need)
            else:
                bars = self.md.get_5m_bars(symbol, need=need)
            if inspect.isawaitable(bars):
                self._close_awaitable(bars)
                raise RuntimeError("MarketDataService.get_5m_bars returned an awaitable in sync adapter path")
            if bars:
                converted = _to_bar_data(bars, "ibkr")
                self._cache_put(self._intraday_cache, key, converted, "ibkr")
                return list(converted), "ibkr"
        except Exception as e:
            if _is_expected_sync_ib_fallback(e):
                log.info("5m bars use yfinance for %s in non-owner sync IB path: %s", symbol, e)
            else:
                log.warning("5m bars fallback to yfinance for %s: %s", symbol, e)
        bars = fetch_intraday_bars_yf(symbol, interval="5m", days=fallback_days)
        if bars:
            converted = _to_bar_data(bars[-max(1, int(need)) :], "yfinance_5m")
            self._cache_put(self._intraday_cache, key, converted, "yfinance_5m")
            return list(converted), "yfinance_5m"
        return [], ""

    def get_5m_bars(self, symbol: str, need: int = 156) -> List[BarData]:
        bars, _ = self.get_5m_bars_with_source(symbol, need=need)
        return bars

    def get_snapshot_price(self, symbol: str) -> float:
        return float(self.md.get_snapshot_price(symbol) or 0.0)
