# src/ibkr/realtime_agg.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Dict, Any

try:
    # ib_insync types
    from ib_insync import IB, Contract  # type: ignore
except Exception:  # pragma: no cover
    IB = Any  # type: ignore
    Contract = Any  # type: ignore


@dataclass
class Bar5m:
    end_time: datetime          # bar end time (UTC recommended)
    open: float
    high: float
    low: float
    close: float
    volume: float
    wap: float
    count: int


class RealTime5mAggregator:
    """
    Subscribe to IBKR RealTimeBars (5-second bars) and aggregate into 5-minute bars.

    Notes:
    - IBKR reqRealTimeBars currently only provides 5-second bars. :contentReference[oaicite:2]{index=2}
    - RealTimeBars pacing is subject to historical pacing limits. :contentReference[oaicite:3]{index=3}
    """

    def __init__(
        self,
        ib: IB,
        contract: Contract,
        symbol: str,
        on_bar5m: Callable[[str, Bar5m], None],
        bucket_sec: int = 300,
        what_to_show: str = "TRADES",
        use_rth: bool = False,
        rt_bar_sec: int = 5,
    ) -> None:
        self.ib = ib
        self.contract = contract
        self.symbol = symbol
        self.on_bar5m = on_bar5m

        self.bucket_sec = bucket_sec
        self.what_to_show = what_to_show
        self.use_rth = use_rth
        self.rt_bar_sec = rt_bar_sec

        self._sub = None
        self._cur_start_utc: Optional[int] = None  # epoch seconds (UTC bucket start)
        self._o = self._h = self._l = self._c = None  # type: ignore
        self._vol = 0.0
        self._wap_v = 0.0
        self._cnt = 0

    def start(self) -> None:
        # ib_insync: reqRealTimeBars returns a RealTimeBarList with updateEvent
        self._sub = self.ib.reqRealTimeBars(
            self.contract,
            barSize=self.rt_bar_sec,
            whatToShow=self.what_to_show,
            useRTH=self.use_rth,
            realTimeBarsOptions=[],
        )
        self._sub.updateEvent += self._on_rt_bar  # type: ignore

    def stop(self) -> None:
        if self._sub is not None:
            try:
                self.ib.cancelRealTimeBars(self._sub.reqId)  # type: ignore
            except Exception:
                pass
            self._sub = None

    @staticmethod
    def _bucket_start(epoch_sec: int, bucket_sec: int) -> int:
        return epoch_sec - (epoch_sec % bucket_sec)

    def _flush_bucket(self, bucket_start: int) -> None:
        if self._o is None:
            return
        end_time_utc = datetime.fromtimestamp(bucket_start + self.bucket_sec, tz=timezone.utc)
        wap = (self._wap_v / self._vol) if self._vol > 0 else float(self._c)
        bar5m = Bar5m(
            end_time=end_time_utc,
            open=float(self._o),
            high=float(self._h),
            low=float(self._l),
            close=float(self._c),
            volume=float(self._vol),
            wap=float(wap),
            count=int(self._cnt),
        )
        self.on_bar5m(self.symbol, bar5m)

    def _reset_bucket(self, new_bucket_start: int, first_bar: Any) -> None:
        self._cur_start_utc = new_bucket_start
        self._o = float(first_bar.open)
        self._h = float(first_bar.high)
        self._l = float(first_bar.low)
        self._c = float(first_bar.close)
        self._vol = float(first_bar.volume)
        self._wap_v = float(first_bar.wap) * float(first_bar.volume)
        self._cnt = int(first_bar.count)

    def _update_bucket(self, rt_bar: Any) -> None:
        self._h = max(float(self._h), float(rt_bar.high))
        self._l = min(float(self._l), float(rt_bar.low))
        self._c = float(rt_bar.close)
        v = float(rt_bar.volume)
        self._vol += v
        self._wap_v += float(rt_bar.wap) * v
        self._cnt += int(rt_bar.count)

    def _on_rt_bar(self, bars: Any, has_new_bar: bool) -> None:
        # bars[-1] is the latest 5-sec bar
        if not has_new_bar:
            return
        if not bars:
            return
        rt = bars[-1]
        # ib_insync realTimeBar has .time as int epoch seconds (UTC)
        t = int(rt.time)

        b0 = self._bucket_start(t, self.bucket_sec)

        if self._cur_start_utc is None:
            self._reset_bucket(b0, rt)
            return

        if b0 == self._cur_start_utc:
            self._update_bucket(rt)
            return

        # bucket advanced: flush old, then roll
        old_start = int(self._cur_start_utc)
        self._flush_bucket(old_start)

        # handle gaps: if jumped multiple buckets, we simply start a new bucket at b0.
        self._reset_bucket(b0, rt)