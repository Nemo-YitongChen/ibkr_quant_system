# src/ibkr/market_data.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
import hashlib
import inspect
import json
from pathlib import Path
import threading
import time
from typing import Dict, List, Optional, Tuple

from ib_insync import IB, Contract, RealTimeBarList

from ..common.logger import get_logger

log = get_logger("ibkr.market_data")


@dataclass
class OHLCVBar:
    time: datetime  # bar start time (tz-aware)
    open: float
    high: float
    low: float
    close: float
    volume: float


def _floor_time(dt: datetime, minutes: int) -> datetime:
    """Floor dt to N-minute boundary."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    discard = timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond,
    )
    return dt - discard


class Realtime5mAggregator:
    """
    Subscribe to IB real-time 5-second bars via reqRealTimeBars()
    and aggregate locally into 5-minute OHLCV bars.
    """

    def __init__(self, ib: IB, use_rth: bool = False, what_to_show: str = "TRADES"):
        self.ib = ib
        self.use_rth = use_rth
        self.what_to_show = what_to_show

        # sym -> (rtBars, completed_5m_bars, current_partial)
        self._subs: Dict[str, RealTimeBarList] = {}
        self._done: Dict[str, List[OHLCVBar]] = {}
        self._cur: Dict[str, Optional[OHLCVBar]] = {}
        self._cur_bucket: Dict[str, Optional[datetime]] = {}

    def start(self, symbol: str, contract: Contract) -> None:
        if symbol in self._subs:
            return

        # IB/ib_insync: barSize must be 5 seconds. :contentReference[oaicite:3]{index=3}
        rt_bars = self.ib.reqRealTimeBars(
            contract=contract,
            barSize=5,
            whatToShow=self.what_to_show,
            useRTH=self.use_rth,
            realTimeBarsOptions=[],
        )
        self._subs[symbol] = rt_bars
        self._done[symbol] = []
        self._cur[symbol] = None
        self._cur_bucket[symbol] = None

        rt_bars.updateEvent += lambda bars, has_new: self._on_rt_update(symbol, bars, has_new)
        log.info(f"Realtime subscription started: {symbol} what={self.what_to_show} useRTH={self.use_rth}")

    def stop(self, symbol: str) -> None:
        bars = self._subs.pop(symbol, None)
        if bars is not None:
            try:
                self.ib.cancelRealTimeBars(bars)
            except Exception:
                pass
        self._done.pop(symbol, None)
        self._cur.pop(symbol, None)
        self._cur_bucket.pop(symbol, None)

    def _on_rt_update(self, symbol: str, bars: RealTimeBarList, has_new: bool) -> None:
        if not bars:
            return
        b = bars[-1]

        # RealTimeBar.time is typically tz-aware; guard anyway.
        t = b.time
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)

        bucket = _floor_time(t, 5)  # 5-minute bucket start

        cur_bucket = self._cur_bucket.get(symbol)
        cur = self._cur.get(symbol)

        if cur_bucket is None or cur is None:
            # start first bucket
            self._cur_bucket[symbol] = bucket
            self._cur[symbol] = OHLCVBar(
                time=bucket,
                open=float(b.open),
                high=float(b.high),
                low=float(b.low),
                close=float(b.close),
                volume=float(getattr(b, "volume", 0.0) or 0.0),
            )
            return

        if bucket != cur_bucket:
            # finalize previous 5m bar
            self._done[symbol].append(cur)
            # start new bucket
            self._cur_bucket[symbol] = bucket
            self._cur[symbol] = OHLCVBar(
                time=bucket,
                open=float(b.open),
                high=float(b.high),
                low=float(b.low),
                close=float(b.close),
                volume=float(getattr(b, "volume", 0.0) or 0.0),
            )
            return

        # update current bucket
        cur.high = max(cur.high, float(b.high))
        cur.low = min(cur.low, float(b.low))
        cur.close = float(b.close)
        cur.volume += float(getattr(b, "volume", 0.0) or 0.0)
        self._cur[symbol] = cur

    def latest_bars(self, symbol: str, limit: int = 200, include_partial: bool = True) -> List[OHLCVBar]:
        done = self._done.get(symbol, [])
        out = done[-limit:].copy()
        if include_partial:
            cur = self._cur.get(symbol)
            if cur is not None:
                out.append(cur)
        return out


class MarketDataService:
    """
    Unified interface:
    - Prefer realtime aggregation (5s -> 5m) for freshness.
    - Fallback to historical 5m (optionally keepUpToDate) if needed.
    """

    def __init__(
        self,
        ib: IB,
        request_timeout_sec: float = 12.0,
        use_rth: bool = False,
        what_to_show: str = "TRADES",
        hist_keep_up_to_date: bool = False,
        hist_retry_attempts: int = 2,
        hist_retry_backoff_sec: float = 1.5,
        hist_5m_cache_ttl_sec: int = 90,
        hist_5m_cache_stale_fallback_sec: int = 900,
        hist_cache_dir: Path | str | None = None,
    ):
        self.ib = ib
        self.request_timeout_sec = request_timeout_sec
        self._owner_thread_id = threading.get_ident()
        self.agg = Realtime5mAggregator(ib, use_rth=use_rth, what_to_show=what_to_show)
        self.hist_keep_up_to_date = hist_keep_up_to_date
        self.hist_retry_attempts = max(1, int(hist_retry_attempts))
        self.hist_retry_backoff_sec = max(0.0, float(hist_retry_backoff_sec))
        self.hist_5m_cache_ttl_sec = max(0, int(hist_5m_cache_ttl_sec))
        self.hist_5m_cache_stale_fallback_sec = max(
            self.hist_5m_cache_ttl_sec,
            int(hist_5m_cache_stale_fallback_sec),
        )
        self._contracts: Dict[str, Contract] = {}
        default_cache_dir = Path(__file__).resolve().parents[2] / ".cache" / "market_data_5m"
        self._hist_cache_dir = Path(hist_cache_dir or default_cache_dir)
        self._hist_cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _same_contract(left: Contract, right: Contract) -> bool:
        return (
            str(getattr(left, "symbol", "") or "") == str(getattr(right, "symbol", "") or "")
            and str(getattr(left, "exchange", "") or "") == str(getattr(right, "exchange", "") or "")
            and str(getattr(left, "currency", "") or "") == str(getattr(right, "currency", "") or "")
        )

    def register(self, symbol: str, contract: Contract) -> None:
        existing = self._contracts.get(symbol)
        if existing is not None and self._same_contract(existing, contract):
            return
        self._contracts[symbol] = contract
        log.info(f"Registered contract for {symbol}: exchange={getattr(contract, 'exchange', '')}")

    def ensure_realtime(self, symbol: str) -> None:
        c = self._contracts[symbol]
        self.agg.start(symbol, c)

    def _contract_for_symbol(self, symbol: str) -> Contract:
        contract = self._contracts.get(symbol)
        if contract is None:
            raise KeyError(f"Contract not registered for symbol={symbol}")
        return contract

    @staticmethod
    def _duration_from_days(days: int) -> str:
        days = max(1, int(days))
        if days >= 365:
            years = max(1, round(days / 365))
            return f"{years} Y"
        return f"{days} D"

    def hist_bars(
        self,
        contract: Contract,
        duration: str,
        bar_size: str,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> List[OHLCVBar]:
        """
        Backward-compatible historical-bar API used by earlier callers.
        """
        key = getattr(contract, "symbol", "") or str(getattr(contract, "conId", ""))
        if key and key not in self._contracts:
            self.register(key, contract)
        self._ensure_sync_ib_call_ready(context="IB.reqHistoricalData(hist_bars)")

        raw = self.ib.reqHistoricalData(
            contract=contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=1 if use_rth else 0,
            formatDate=2,
            keepUpToDate=False,
            timeout=float(self.request_timeout_sec),
        )
        raw = self._ensure_sync_result(raw, context="IB.reqHistoricalData(hist_bars)")

        out: List[OHLCVBar] = []
        for r in raw:
            t = r.date
            if isinstance(t, str):
                continue
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            out.append(
                OHLCVBar(
                    time=t,
                    open=float(r.open),
                    high=float(r.high),
                    low=float(r.low),
                    close=float(r.close),
                    volume=float(getattr(r, "volume", 0.0) or 0.0),
                )
        )
        return out

    @staticmethod
    def _raw_to_ohlcv(raw: List[object]) -> List[OHLCVBar]:
        out: List[OHLCVBar] = []
        for r in raw or []:
            t = getattr(r, "date", None)
            if isinstance(t, str) or t is None:
                continue
            if isinstance(t, date) and not isinstance(t, datetime):
                t = datetime(t.year, t.month, t.day, tzinfo=timezone.utc)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            out.append(
                OHLCVBar(
                    time=t,
                    open=float(getattr(r, "open", 0.0) or 0.0),
                    high=float(getattr(r, "high", 0.0) or 0.0),
                    low=float(getattr(r, "low", 0.0) or 0.0),
                    close=float(getattr(r, "close", 0.0) or 0.0),
                    volume=float(getattr(r, "volume", 0.0) or 0.0),
                )
            )
        return out

    def _historical_5m_cache_key(
        self,
        symbol: str,
        contract: Contract,
        *,
        hist_duration: str,
        hist_bar_size: str,
    ) -> str:
        payload = {
            "symbol": str(symbol or "").upper(),
            "contract_symbol": str(getattr(contract, "symbol", "") or "").upper(),
            "exchange": str(getattr(contract, "exchange", "") or "").upper(),
            "primary_exchange": str(getattr(contract, "primaryExchange", "") or "").upper(),
            "currency": str(getattr(contract, "currency", "") or "").upper(),
            "hist_duration": str(hist_duration or ""),
            "hist_bar_size": str(hist_bar_size or ""),
        }
        return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _historical_5m_cache_path(self, cache_key: str) -> Path:
        return self._hist_cache_dir / f"{cache_key}.json"

    def _write_historical_5m_cache(self, cache_key: str, bars: List[OHLCVBar]) -> None:
        payload = {
            "ts": time.time(),
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
            ],
        }
        try:
            self._historical_5m_cache_path(cache_key).write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            return

    def _read_historical_5m_cache(
        self,
        cache_key: str,
        *,
        max_age_sec: int,
    ) -> Tuple[List[OHLCVBar], Optional[float]]:
        cache_path = self._historical_5m_cache_path(cache_key)
        if not cache_path.exists():
            return [], None
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return [], None
        ts = float(payload.get("ts", 0.0) or 0.0)
        if ts <= 0:
            return [], None
        age_sec = max(0.0, time.time() - ts)
        if age_sec > max(0, int(max_age_sec)):
            return [], age_sec
        out: List[OHLCVBar] = []
        for row in list(payload.get("bars", []) or []):
            try:
                t = datetime.fromisoformat(str(row.get("time", "")))
            except Exception:
                continue
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            out.append(
                OHLCVBar(
                    time=t,
                    open=float(row.get("open", 0.0) or 0.0),
                    high=float(row.get("high", 0.0) or 0.0),
                    low=float(row.get("low", 0.0) or 0.0),
                    close=float(row.get("close", 0.0) or 0.0),
                    volume=float(row.get("volume", 0.0) or 0.0),
                )
            )
        return out, age_sec

    def _cleanup_historical_request(self, raw: object | None) -> None:
        if raw is None or self.ib is None:
            return
        try:
            self.ib.cancelHistoricalData(raw)
        except Exception:
            return

    def _ensure_sync_ib_call_ready(self, *, context: str) -> None:
        current_thread = threading.current_thread()
        if threading.get_ident() != self._owner_thread_id:
            raise RuntimeError(
                f"{context} must run on MarketDataService owner thread; current_thread={current_thread.name}"
            )
        try:
            asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            log.info("%s created event loop on owner thread=%s", context, current_thread.name)

    @staticmethod
    def _ensure_sync_result(raw: object, *, context: str) -> object:
        if inspect.isawaitable(raw):
            if inspect.iscoroutine(raw):
                try:
                    raw.close()
                except Exception:
                    pass
            raise RuntimeError(f"{context} returned an awaitable in sync market-data path")
        return raw

    def _request_historical_5m_with_retries(
        self,
        *,
        symbol: str,
        contract: Contract,
        hist_duration: str,
        hist_bar_size: str,
        keep: bool,
    ) -> List[OHLCVBar]:
        end = "" if keep else ""
        last_error: Exception | None = None
        for attempt in range(1, self.hist_retry_attempts + 1):
            raw = None
            try:
                self._ensure_sync_ib_call_ready(context="IB.reqHistoricalData(5m)")
                raw = self.ib.reqHistoricalData(
                    contract=contract,
                    endDateTime=end,
                    durationStr=hist_duration,
                    barSizeSetting=hist_bar_size,
                    whatToShow="TRADES",
                    useRTH=False,
                    formatDate=2,
                    keepUpToDate=keep,
                    timeout=float(self.request_timeout_sec),
                )
                raw = self._ensure_sync_result(raw, context="IB.reqHistoricalData(5m)")
                out = self._raw_to_ohlcv(raw)
                if not out and attempt < self.hist_retry_attempts:
                    raise RuntimeError("historical_5m_empty")
                return out
            except Exception as e:
                last_error = e
                self._cleanup_historical_request(raw)
                if attempt < self.hist_retry_attempts:
                    backoff = self.hist_retry_backoff_sec * attempt
                    log.warning(
                        "[%s] Historical fallback attempt %s/%s failed: %s; retrying in %.1fs",
                        symbol,
                        attempt,
                        self.hist_retry_attempts,
                        e,
                        backoff,
                    )
                    if backoff > 0:
                        time.sleep(backoff)
                    continue
                break
        if last_error is not None:
            raise last_error
        return []

    def get_5m_bars(
        self,
        symbol: str,
        need: int = 156,
        hist_duration: str = "2 D",
        hist_bar_size: str = "5 mins",
    ) -> List[OHLCVBar]:
        need = max(1, int(need))
        # Try realtime aggregated bars first
        bars = self.agg.latest_bars(symbol, limit=max(need, 200), include_partial=True)
        if len(bars) >= min(need, 30):
            cache_key = self._historical_5m_cache_key(
                symbol,
                self._contracts[symbol],
                hist_duration=hist_duration,
                hist_bar_size=hist_bar_size,
            )
            self._write_historical_5m_cache(cache_key, bars[-max(need, 200) :])
            return bars[-need:]

        # Fallback to historical (note: last bar won't change when market closed)
        c = self._contracts[symbol]
        cache_key = self._historical_5m_cache_key(
            symbol,
            c,
            hist_duration=hist_duration,
            hist_bar_size=hist_bar_size,
        )
        cached, cached_age = self._read_historical_5m_cache(
            cache_key,
            max_age_sec=self.hist_5m_cache_ttl_sec,
        )
        if len(cached) >= min(need, 30):
            log.info("[%s] Using cached historical %s bars age=%.1fs", symbol, hist_bar_size, cached_age or 0.0)
            return cached[-need:]
        keep = bool(self.hist_keep_up_to_date)
        log.info(f"[{symbol}] Realtime bars unavailable/insufficient -> fallback to historical {hist_bar_size} {hist_duration}")
        try:
            out = self._request_historical_5m_with_retries(
                symbol=symbol,
                contract=c,
                hist_duration=hist_duration,
                hist_bar_size=hist_bar_size,
                keep=keep,
            )
            if out:
                self._write_historical_5m_cache(cache_key, out)
            return out[-need:]
        except Exception as e:
            log.warning(f"Historical fallback failed for {symbol}: {e}")
            stale, stale_age = self._read_historical_5m_cache(
                cache_key,
                max_age_sec=self.hist_5m_cache_stale_fallback_sec,
            )
            if len(stale) >= min(need, 30):
                log.warning(
                    "[%s] Using stale cached historical %s bars age=%.1fs after fallback failure",
                    symbol,
                    hist_bar_size,
                    stale_age or 0.0,
                )
                return stale[-need:]
            return bars[-need:]

    def get_daily_bars(
        self,
        symbol: str,
        days: int = 180,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> List[OHLCVBar]:
        """
        Backward-compatible daily historical bar API for offhours analysis.
        """
        contract = self._contract_for_symbol(symbol)
        duration = self._duration_from_days(days)
        self._ensure_sync_ib_call_ready(context="IB.reqHistoricalData(daily)")

        raw = self.ib.reqHistoricalData(
            contract=contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting="1 day",
            whatToShow=what_to_show,
            useRTH=1 if use_rth else 0,
            formatDate=2,
            keepUpToDate=False,
            timeout=float(self.request_timeout_sec),
        )
        raw = self._ensure_sync_result(raw, context="IB.reqHistoricalData(daily)")

        out: List[OHLCVBar] = []
        for r in raw:
            t = r.date
            if isinstance(t, str):
                continue
            if isinstance(t, date) and not isinstance(t, datetime):
                t = datetime(t.year, t.month, t.day, tzinfo=timezone.utc)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            out.append(
                OHLCVBar(
                    time=t,
                    open=float(r.open),
                    high=float(r.high),
                    low=float(r.low),
                    close=float(r.close),
                    volume=float(getattr(r, "volume", 0.0) or 0.0),
                )
            )
        return out

    def get_snapshot_price(self, symbol: str) -> float:
        contract = self._contract_for_symbol(symbol)
        try:
            self._ensure_sync_ib_call_ready(context="IB.reqTickers(snapshot)")
            tickers = self.ib.reqTickers(contract)
        except Exception as e:
            log.warning("snapshot price failed for %s: %s", symbol, e)
            return 0.0
        if not tickers:
            return 0.0
        ticker = tickers[0]
        try:
            market_price = float(ticker.marketPrice() or 0.0)
        except Exception:
            market_price = 0.0
        bid = float(getattr(ticker, "bid", 0.0) or 0.0)
        ask = float(getattr(ticker, "ask", 0.0) or 0.0)
        last = float(getattr(ticker, "last", 0.0) or 0.0)
        close = float(getattr(ticker, "close", 0.0) or 0.0)
        mid = ((bid + ask) / 2.0) if (bid > 0 and ask > 0) else 0.0
        for price in (market_price, last, mid, close, bid, ask):
            if price > 0:
                return float(price)
        return 0.0
