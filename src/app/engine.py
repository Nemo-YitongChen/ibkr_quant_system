# src/app/engine.py
from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any

from ib_insync import IB, Stock, Forex  # type: ignore

from ..ibkr.realtime_agg import RealTime5mAggregator, Bar5m

log = logging.getLogger("engine")


@dataclass
class EngineConfig:
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
    last_5m_end: Optional[int] = None  # epoch seconds of last completed 5m bar end
    last_bar5m: Optional[Bar5m] = None


class TradingEngine:
    def __init__(
        self,
        ib: IB,
        universe_svc: Any,
        strategy: Any,
        runner: Any,
        cfg: EngineConfig,
    ) -> None:
        self.ib = ib
        self.universe_svc = universe_svc
        self.strategy = strategy
        self.runner = runner
        self.cfg = cfg

        self._states: Dict[str, SymbolState] = {}
        self._aggs: Dict[str, RealTime5mAggregator] = {}
        self._latest_5m: Dict[str, Bar5m] = {}

    def start(self) -> None:
        # ib_insync timeout
        try:
            self.ib.RequestTimeout = self.cfg.request_timeout_sec
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

        # Contract mapping: keep simple here
        if symbol.upper() in ("AUD", "USD", "EUR", "JPY"):
            # if your universe has pure currency symbols, you likely want explicit pair.
            # keep as-is if you already handle FX elsewhere
            contract = Forex("AUDUSD")
        else:
            contract = Stock(symbol, "SMART", "USD")

        agg = RealTime5mAggregator(
            ib=self.ib,
            contract=contract,
            symbol=symbol,
            on_bar5m=self._on_bar5m,
            bucket_sec=300,
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
            log.info(f"[{symbol}] Same 5m bucket ({bar.end_time.isoformat()}); skip signal calc.")
            return

        st.last_5m_end = end_epoch
        st.last_bar5m = bar

        # ---- call your existing strategy interface here ----
        # Expect your strategy to accept a 5m OHLCV series. Here we only have one bar.
        # If your strategy needs N bars, keep a rolling buffer per symbol (recommended).
        #
        # Minimal stub: strategy.calculate(symbol) should pull needed series from your store.
        sig = self.strategy.evaluate_from_bar(symbol, bar)  # you implement in strategy layer

        if sig is None:
            log.info(f"[{tag}] No trade {symbol}: sig=None")
            return

        # sig should include fields you already log: total_sig/short_sig/mid/thr etc.
        # We'll log generically:
        if getattr(sig, "should_trade", False):
            self.strategy.execute(symbol, sig, self.runner)
        else:
            log.info(f"[{tag}] No trade {symbol}: {sig}")

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

                # 2) hot/短线候选：有空位才补足到 N
                # 你现有 universe.build 已经做了补足 short_candidates，这里直接按 short_candidates 处理
                for sym in short_candidates:
                    if sym in always_on:
                        continue
                    if self.cfg.use_realtime_agg:
                        self._ensure_agg(sym)
                    self._maybe_calc_signal(sym, tag="HOT_FILL")

                # runner maintenance tick
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