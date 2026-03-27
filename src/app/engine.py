# src/app/engine.py
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Dict, Optional, List, Any

from ib_insync import IB, Stock, Forex  # type: ignore

from ..common.logger import get_logger
from ..ibkr.contracts import make_stock_contract
from ..ibkr.realtime_agg import RealTime5mAggregator, Bar5m

log = get_logger("engine")


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

    # v4: when market closed / no realtime updates, use historical 5m bars to drive Phase1 audit
    hist_fallback_enabled: bool = True
    hist_fallback_min_interval_sec: int = 30  # avoid spamming historical requests
    hist_duration: str = "2 D"
    hist_bar_size: str = "5 mins"


@dataclass
class SymbolState:
    # epoch seconds of last completed 5m bar end
    last_5m_end: Optional[int] = None
    last_bar5m: Optional[Bar5m] = None



@dataclass
class _HistBar5m:
    # Adapter to match Bar5m interface (open/high/low/close/volume/end_time)
    open: float
    high: float
    low: float
    close: float
    volume: float
    end_time: datetime


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
        regime_adaptor: Optional[Any] = None,
        **_ignored: Any,  # be tolerant to unknown kwargs to keep iteration stable
    ) -> None:
        self.ib = ib
        self.universe_svc = universe_svc
        self.strategy = strategy
        self.runner = runner
        self.cfg = cfg
        self.md = md  # optional (can be None)
        self.regime_adaptor = regime_adaptor

        self._states: Dict[str, SymbolState] = {}
        self._aggs: Dict[str, RealTime5mAggregator] = {}
        self._latest_5m: Dict[str, Bar5m] = {}

        # v4: historical fallback throttle (symbol -> last_fetch_monotonic)
        self._hist_last_fetch: Dict[str, float] = {}
        self._hist_bucket_key: Dict[str, int] = {}
        self._hist_cached_bar: Dict[str, Optional[_HistBar5m]] = {}

        # v4c: symbols with no realtime permissions (fall back to historical only)
        self._rt_disabled: Dict[str, str] = {}
        self._warmup_attempted: Dict[str, float] = {}

        # Phase1: market-data quality stats (per symbol, per UTC day)
        # {symbol: {'day': 'YYYY-MM-DD', 'buckets': int, 'duplicates': int, 'max_gap_sec': int, 'last_end': int}}
        self._quality: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _is_stale_bar(end_time: datetime, stale_after_sec: int = 1800) -> bool:
        now = datetime.now(timezone.utc)
        return (now - end_time).total_seconds() > stale_after_sec

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
        if symbol in self._rt_disabled:
            return

        # Very simple contract mapping:
        # - If universe includes pure currency symbols, map them to a pair as you wish
        if symbol.upper() in ("AUD", "USD", "EUR", "JPY"):
            contract = Forex("AUDUSD")
        else:
            contract = make_stock_contract(symbol)

        # v4: allow MarketDataService historical fallback to resolve contract
        try:
            if self.md is not None and hasattr(self.md, "register"):
                self.md.register(symbol, contract)
        except Exception:
            pass

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
        try:
            agg.start()
            log.info(f"[{symbol}] realtime aggregator started")
        except Exception as e:
            # If no realtime permissions (e.g., Error 420), fall back to historical audit only
            self._rt_disabled[symbol] = str(e)
            log.warning(f"[{symbol}] realtime disabled -> historical fallback only: {e}")

        self._aggs[symbol] = agg
        self._states.setdefault(symbol, SymbolState())


    def _get_hist_latest_bar(self, symbol: str) -> Optional[_HistBar5m]:
        """Use MarketDataService.get_5m_bars() to fetch latest completed 5m bar (best-effort)."""
        if self.md is None or not hasattr(self.md, "get_5m_bars"):
            return None

        current_bucket = int(time.time() // 300)
        cached_bucket = self._hist_bucket_key.get(symbol)
        if cached_bucket == current_bucket:
            return self._hist_cached_bar.get(symbol)

        now_m = time.monotonic()
        last = self._hist_last_fetch.get(symbol, 0.0)
        if now_m - last < float(self.cfg.hist_fallback_min_interval_sec):
            return self._hist_cached_bar.get(symbol)
        self._hist_last_fetch[symbol] = now_m

        try:
            bars = self.md.get_5m_bars(
                symbol,
                need=200,
                hist_duration=self.cfg.hist_duration,
                hist_bar_size=self.cfg.hist_bar_size,
            )
            if not bars:
                return None

            # bars are OHLCVBar(time=start). Choose last bar; if it's partial, it's fine for audit.
            b = bars[-1]
            start = getattr(b, "time", None)
            if start is None:
                return None
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)

            end_time = start + timedelta(minutes=5)
            out = _HistBar5m(
                open=float(b.open),
                high=float(b.high),
                low=float(b.low),
                close=float(b.close),
                volume=float(getattr(b, "volume", 0.0) or 0.0),
                end_time=end_time,
            )
            self._hist_bucket_key[symbol] = current_bucket
            self._hist_cached_bar[symbol] = out
            return out
        except Exception as e:
            log.info(f"[{symbol}] historical fallback fetch skipped/failed: {e}")
            return None

    def _warmup_symbol(self, symbol: str) -> None:
        if self.md is None or not hasattr(self.md, "get_5m_bars"):
            return
        if not hasattr(self.strategy, "required_bars") or not hasattr(self.strategy, "bar_count") or not hasattr(self.strategy, "preload_bars"):
            return

        need = int(self.strategy.required_bars())
        have = int(self.strategy.bar_count(symbol))
        if have >= need:
            return

        now_m = time.monotonic()
        last_try = self._warmup_attempted.get(symbol, 0.0)
        if now_m - last_try < float(self.cfg.hist_fallback_min_interval_sec):
            return
        self._warmup_attempted[symbol] = now_m

        try:
            bars = self.md.get_5m_bars(
                symbol,
                need=max(need + 20, 120),
                hist_duration=self.cfg.hist_duration,
                hist_bar_size=self.cfg.hist_bar_size,
            )
            if not bars:
                return
            preload = list(bars[:-1]) if len(bars) > 1 else list(bars)
            if not preload:
                return
            appended = int(self.strategy.preload_bars(symbol, preload))
            if appended > 0:
                log.info(f"[{symbol}] warmup loaded {appended} historical bars for signal state")
        except Exception as e:
            log.info(f"[{symbol}] warmup skipped/failed: {e}")

    def _maybe_calc_signal(self, symbol: str, tag: str) -> None:
        st = self._states.setdefault(symbol, SymbolState())
        self._warmup_symbol(symbol)
        bar = self._latest_5m.get(symbol)
        source = "REALTIME"
        if not bar and self.cfg.hist_fallback_enabled:
            # v4: market closed / no realtime updates -> use historical 5m to drive Phase1 audit
            bar = self._get_hist_latest_bar(symbol)
            source = "HIST"
            if bar is not None:
                log.info(f"[{symbol}] using historical fallback bar ending {bar.end_time.isoformat()}")
                if self._is_stale_bar(bar.end_time):
                    log.info(f"[{symbol}] fallback bar is stale; market likely closed or permissions prevent fresh realtime updates")
        if not bar:
            log.info(f"[{symbol}] no realtime or historical bar available this cycle")
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

        # ---- Phase1-C: tag/source for audit (best-effort, no coupling) ----
        try:
            setattr(self.strategy, "_audit_tag", tag)
            setattr(self.strategy, "_audit_source", source)
        except Exception:
            pass

        # Strategy interface:
        # - You should implement strategy.evaluate_from_bar(symbol, bar) in strategy layer
        sig = self.strategy.evaluate_from_bar(symbol, bar)

        if sig is None:
            log.info(f"[{tag}] No trade {symbol}: sig=None")
            return

        log.info(
            f"[{tag}] Signal {symbol}: source={source} should_trade={getattr(sig, 'should_trade', False)} "
            f"action={getattr(sig, 'action', '')} total={getattr(sig, 'total_sig', 0.0):.3f} "
            f"short={getattr(sig, 'short_sig', 0.0):.3f} mid={getattr(sig, 'mid_scale', 0.0):.3f} "
            f"risk_on={getattr(sig, 'risk_on', True)} regime={getattr(sig, 'regime_state', '')} "
            f"regime_reason=\"{getattr(sig, 'regime_reason', '')}\""
        )

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

    def _refresh_risk_context(self, symbols: List[str]) -> None:
        gate = getattr(self.strategy, "gate", None)
        if gate is None or not hasattr(gate, "refresh_trade_context"):
            return
        try:
            gate.refresh_trade_context(symbols)
        except Exception as e:
            log.warning(f"risk context refresh skipped/failed: {type(e).__name__} {e}")

    def run_forever(self) -> None:
        while True:
            try:
                if self.regime_adaptor is not None and self.md is not None:
                    adapted = self.regime_adaptor.refresh_if_due(
                        self.md,
                        storage=getattr(getattr(self.strategy, "orders", None), "storage", None),
                    )
                    try:
                        self.strategy.cfg.mid = adapted
                    except Exception:
                        pass
                uni = self.universe_svc.build()
                always_on: List[str] = uni.get("always_on", [])
                short_candidates: List[str] = uni.get("short_candidates", [])
                self._refresh_risk_context(always_on + short_candidates)

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


    def run(self) -> None:
        """Alias for backward/forward compatibility."""
        return self.run_forever()

    def _sleep_with_runner(self, sec: int) -> None:
        t0 = time.time()
        while time.time() - t0 < sec:
            try:
                if hasattr(self.runner, "tick"):
                    self.runner.tick()
            except Exception:
                log.exception("runner tick failed")
            time.sleep(0.2)
