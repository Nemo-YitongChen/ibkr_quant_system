# src/app/engine.py
from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List, Any

from ib_insync import IB, Stock, Forex  # type: ignore

from ..ibkr.realtime_agg import RealTime5mAggregator, Bar5m

log = logging.getLogger("engine")


@dataclass
class EngineConfig:
    # main loop cadence
    cycle_sec: int = 5

    # market data
    use_realtime_agg: bool = True
    realtime_useRTH: bool = False
    realtime_whatToShow: str = "TRADES"
    request_timeout_sec: float = 5.0

    # universe sizing
    max_short_candidates: int = 30


@dataclass
class SymbolState:
    # epoch seconds of last completed 5m bar end
    last_5m_end: Optional[int] = None
    last_bar5m: Optional[Bar5m] = None


class TradingEngine:
    """
    RealTimeBars -> 5m bucket aggregator driving signal calculation.
    - always_on: every cycle compute signals
    - hot_fill: only fill up to N when there is capacity (already done in UniverseService.build)
    """

    def __init__(
        self,
        ib: IB,
        universe_svc: Any,
        strategy: Any,
        runner: Any,
        cfg: EngineConfig,
        md: Optional[Any] = None,
        **_ignored: Any,  # be tolerant to unknown kwargs to keep iteration stable
    ) -> None:
        self.ib = ib
        self.universe_svc = universe_svc
        self.strategy = strategy
        self.runner = runner
        self.cfg = cfg
        self.md = md  # optional (can be None)

        self._states: Dict[str, SymbolState] = {}
        self._aggs: Dict[str, RealTime5mAggregator] = {}
        self._latest_5m: Dict[str, Bar5m] = {}

        # Phase1: market-data quality stats (per symbol, per UTC day)
        # {symbol: {'day': 'YYYY-MM-DD', 'buckets': int, 'duplicates': int, 'max_gap_sec': int, 'last_end': int}}
        self._quality: Dict[str, Dict[str, Any]] = {}

    def start(self) -> None:
        # ib_insync timeout
        try:
            self.ib.RequestTimeout = float(self.cfg.request_timeout_sec)
        except Exception:
            pass

        if hasattr(self.runner, "start"):
            self.runner.start()

        log.info("Engine started.")

    def _on_bar5m(self, symbol: str, bar5m: Bar5m) -> None:
        self._latest_5m[symbol] = bar5m

    def _ensure_agg(self, symbol: str) -> None:
        if symbol in self._aggs:
            return

        # Very simple contract mapping:
        # - If universe includes pure currency symbols, map them to a pair as you wish
        if symbol.upper() in ("AUD", "USD", "EUR", "JPY"):
            contract = Forex("AUDUSD")
        else:
            contract = Stock(symbol, "SMART", "USD")

        agg = RealTime5mAggregator(
            ib=self.ib,
            contract=contract,
            symbol=symbol,
            on_bar5m=self._on_bar5m,
            bucket_sec=300,  # 5m
            what_to_show=self.cfg.realtime_whatToShow,
            use_rth=self.cfg.realtime_useRTH,
            rt_bar_sec=5,
        )
        agg.start()

        self._aggs[symbol] = agg
        self._states.setdefault(symbol, SymbolState())

    def _maybe_calc_signal(self, symbol: str, tag: str) -> None:
        st = self._states.setdefault(symbol, SymbolState())
        bar = self._latest_5m.get(symbol)
        if not bar:
            return

        end_epoch = int(bar.end_time.timestamp())
        if st.last_5m_end == end_epoch:
            self._update_quality(symbol, end_epoch, is_duplicate=True)
            log.info(
                f"[{symbol}] Same 5m bucket ({bar.end_time.isoformat()}); skip signal calc."
            )
            return

        st.last_5m_end = end_epoch
        st.last_bar5m = bar
        self._update_quality(symbol, end_epoch, is_duplicate=False)

        # Strategy interface:
        # - You should implement strategy.evaluate_from_bar(symbol, bar) in strategy layer
        sig = self.strategy.evaluate_from_bar(symbol, bar)

        if sig is None:
            log.info(f"[{tag}] No trade {symbol}: sig=None")
            return

        # If your sig object contains details, keep your existing logging style outside.
        if getattr(sig, "should_trade", False):
            self.strategy.execute(symbol, sig, self.runner)
        else:
            log.info(f"[{tag}] No trade {symbol}: {sig}")


def _update_quality(self, symbol: str, end_epoch: int, is_duplicate: bool) -> None:
    # Persist per-UTC-day summary into Storage.md_quality (best-effort).
    day = time.strftime("%Y-%m-%d", time.gmtime(end_epoch))
    q = self._quality.get(symbol)
    if q is None or q.get("day") != day:
        q = {"day": day, "buckets": 0, "duplicates": 0, "max_gap_sec": 0, "last_end": None}
        self._quality[symbol] = q

    if is_duplicate:
        q["duplicates"] += 1
        return

    q["buckets"] += 1
    last_end = q.get("last_end")
    if last_end is not None:
        gap = int(end_epoch) - int(last_end)
        # expected gap is 300 sec; track max unexpected gap
        if gap > 300:
            q["max_gap_sec"] = max(int(q["max_gap_sec"]), int(gap))
    q["last_end"] = int(end_epoch)

    # write through to sqlite (non-blocking)
    try:
        storage = getattr(getattr(self.strategy, "orders", None), "storage", None)
        if storage is not None and hasattr(storage, "upsert_md_quality"):
            storage.upsert_md_quality(
                day=q["day"],
                symbol=symbol,
                buckets=int(q["buckets"]),
                duplicates=int(q["duplicates"]),
                max_gap_sec=int(q["max_gap_sec"]),
                last_end_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_epoch)),
            )
    except Exception:
        pass

    def run_forever(self) -> None:
        while True:
            try:
                uni = self.universe_svc.build()
                always_on: List[str] = uni.get("always_on", [])
                short_candidates: List[str] = uni.get("short_candidates", [])

                # 1) always_on 每轮都跟踪 + 算信号
                for sym in always_on:
                    if self.cfg.use_realtime_agg:
                        self._ensure_agg(sym)
                    self._maybe_calc_signal(sym, tag="ALWAYS_ON")

                # 2) 热点/短线候选：仅在有空位时补足到 N（由 universe.build 已补足）
                for sym in short_candidates:
                    if sym in always_on:
                        continue
                    if self.cfg.use_realtime_agg:
                        self._ensure_agg(sym)
                    self._maybe_calc_signal(sym, tag="HOT_FILL")

                self._sleep_with_runner(self.cfg.cycle_sec)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                log.exception(f"Engine loop error: {type(e).__name__} {e}")
                self._sleep_with_runner(self.cfg.cycle_sec)

    def _sleep_with_runner(self, sec: int) -> None:
        t0 = time.time()
        while time.time() - t0 < sec:
            try:
                if hasattr(self.runner, "tick"):
                    self.runner.tick()
            except Exception:
                log.exception("runner tick failed")
            time.sleep(0.2)