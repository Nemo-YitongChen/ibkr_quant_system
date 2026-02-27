# src/ibkr/market_data.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from ib_insync import IB, Contract, RealTimeBarList

import logging

log = logging.getLogger("ibkr.market_data")


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
    ):
        self.ib = ib
        self.request_timeout_sec = request_timeout_sec
        self.agg = Realtime5mAggregator(ib, use_rth=use_rth, what_to_show=what_to_show)
        self.hist_keep_up_to_date = hist_keep_up_to_date
        self._contracts: Dict[str, Contract] = {}

    def register(self, symbol: str, contract: Contract) -> None:
        self._contracts[symbol] = contract

    def ensure_realtime(self, symbol: str) -> None:
        c = self._contracts[symbol]
        self.agg.start(symbol, c)

    def get_5m_bars(
        self,
        symbol: str,
        need: int = 156,
        hist_duration: str = "2 D",
        hist_bar_size: str = "5 mins",
    ) -> List[OHLCVBar]:
        # Try realtime aggregated bars first
        bars = self.agg.latest_bars(symbol, limit=max(need, 200), include_partial=True)
        if len(bars) >= min(need, 30):
            return bars[-need:]

        # Fallback to historical (note: last bar won't change when market closed)
        c = self._contracts[symbol]
        keep = bool(self.hist_keep_up_to_date)

        # keepUpToDate requires endDateTime='' per ib_insync docs. :contentReference[oaicite:4]{index=4}
        end = "" if keep else ""
        try:
            raw = self.ib.reqHistoricalData(
                contract=c,
                endDateTime=end,
                durationStr=hist_duration,
                barSizeSetting=hist_bar_size,
                whatToShow="TRADES",
                useRTH=False,
                formatDate=2,  # timezone-aware UTC per docs
                keepUpToDate=keep,
                timeout=float(self.request_timeout_sec),
            )
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
            return out[-need:]
        except Exception as e:
            log.warning(f"Historical fallback failed for {symbol}: {e}")
            return bars[-need:]