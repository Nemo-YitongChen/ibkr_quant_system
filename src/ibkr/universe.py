from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import time
import threading
import math

from ib_insync import ScannerSubscription, TagValue  # type: ignore

from ..common.logger import get_logger

log = get_logger("ibkr.universe")


@dataclass
class UniverseConfig:
    max_short_candidates: int = 15

    # scanner setup
    scanner_instrument: str = "STK"
    scanner_location_code: str = "STK.US.MAJOR"
    scanner_location_codes: List[str] = field(default_factory=list)
    scanner_codes: List[str] = field(default_factory=lambda: ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])
    scanner_limit: int = 20
    scanner_enabled: bool = True
    scanner_refresh_sec: int = 120
    scanner_max_codes_per_run: int = 3

    recent_trade_limit: int = 30
    seed_symbols: List[str] = field(default_factory=lambda: ["SPY"])
    seed_batch_enabled: bool = True
    seed_batch_size: int = 40
    seed_rotation_sec: int = 300

    # Phase1-B: cooldown / blacklist (based on md_quality)
    cooldown_enabled: bool = True
    cooldown_minutes: int = 30
    dup_per_bucket_threshold: float = 10.0
    max_gap_sec_threshold: int = 1800
    min_buckets_for_eval: int = 1

    # ---------------- Phase3 (default OFF to avoid changing behavior) ----------------
    phase3_enabled: bool = True

    # Filters (used only when phase3_enabled=True and md is provided)
    phase3_lookback_bars: int = 48              # 48*5m = 4 hours
    phase3_price_min: float = 2.0               # filter penny stocks
    phase3_avg_vol_min: float = 50_000.0        # avg 5m volume
    phase3_atr_pct_min: float = 0.002           # 0.2%
    phase3_volume_log_weight: float = 1.0
    phase3_atr_pct_weight: float = 100.0
    phase3_price_bonus_weight: float = 0.0
    phase3_price_bonus_ref: float = 20.0

    # Ranking / penalties
    phase3_repeat_halflife_min: float = 60.0    # repetition penalty decays over time
    phase3_repeat_penalty: float = 0.25         # penalty strength
    phase3_bad_symbol_cooldown_min: int = 30    # cooldown when fails filters


class UniverseService:
    """
    Universe sources:
    - positions / open trades / recent trades
    - optional scanner hot list
    - seed symbols

    Phase1-B (final):
    - persistent blacklist/cooldown via md_blacklist and md_quality

    Phase3:
    - hot list -> filter + score -> pick Top N that are actually tradable
    """

    def __init__(self, ib, cfg: UniverseConfig, storage=None, md=None):
        self.ib = ib
        self.cfg = cfg
        self.storage = storage
        self.md = md  # optional MarketDataService (historical bars)

        self._scan_lock = threading.Lock()
        self._scan_cache: Dict[str, Tuple[float, List[str]]] = {}
        self._seed_batch_idx: int = 0
        self._seed_batch_last_rotate: float = 0.0

        # Phase1-B: in-memory cooldown (symbol -> epoch seconds)
        self._cooldown_until: Dict[str, float] = {}

        # Phase3: in-memory repetition tracking (symbol -> (count, last_ts))
        self._repeat_state: Dict[str, Tuple[float, float]] = {}
        # Phase3: local cache for computed metrics (symbol -> (ts, metrics))
        self._metrics_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}

    # ---------------- Basic symbol sources ----------------
    def _positions_symbols(self) -> List[str]:
        out: List[str] = []
        try:
            for p in self.ib.positions():
                c = getattr(p, "contract", None)
                sym = getattr(c, "symbol", None)
                if sym:
                    out.append(str(sym))
        except Exception as e:
            log.warning(f"positions() failed: {type(e).__name__} {e}")
        return out

    def _open_trade_symbols(self) -> List[str]:
        out: List[str] = []
        try:
            for t in self.ib.openTrades():
                c = getattr(t, "contract", None)
                sym = getattr(c, "symbol", None)
                if sym:
                    out.append(str(sym))
        except Exception as e:
            log.warning(f"openTrades() failed: {type(e).__name__} {e}")
        return out

    def _recent_traded_symbols(self) -> List[str]:
        out: List[str] = []
        try:
            trades = list(self.ib.trades())
            trades = trades[-max(200, self.cfg.recent_trade_limit * 5):]
            trades.reverse()
            for t in trades:
                c = getattr(t, "contract", None)
                sym = getattr(c, "symbol", None)
                if sym:
                    out.append(str(sym))
                if len(out) >= self.cfg.recent_trade_limit:
                    break
        except Exception as e:
            log.warning(f"trades() failed: {type(e).__name__} {e}")
        return out

    @staticmethod
    def _dedupe_keep_order(xs: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _batched_seeds(self, seeds: List[str]) -> List[str]:
        seeds = self._dedupe_keep_order(seeds)
        if not seeds:
            return seeds
        if not bool(getattr(self.cfg, "seed_batch_enabled", True)):
            return seeds

        batch_size = max(1, int(getattr(self.cfg, "seed_batch_size", 40)))
        if len(seeds) <= batch_size:
            return seeds

        now = time.time()
        rotate_sec = max(1, int(getattr(self.cfg, "seed_rotation_sec", 300)))
        if (now - self._seed_batch_last_rotate) >= rotate_sec:
            self._seed_batch_idx = (self._seed_batch_idx + batch_size) % len(seeds)
            self._seed_batch_last_rotate = now

        start = self._seed_batch_idx
        batch = seeds[start : start + batch_size]
        if len(batch) < batch_size:
            batch.extend(seeds[: batch_size - len(batch)])
        return batch

    # ---------------- Scanner ----------------
    def _scan_once(self, scanner_code: str, limit: int, location_code: Optional[str] = None) -> List[str]:
        sub = ScannerSubscription()
        sub.instrument = str(getattr(self.cfg, "scanner_instrument", "STK"))
        sub.locationCode = str(location_code or getattr(self.cfg, "scanner_location_code", "STK.US.MAJOR"))
        sub.scanCode = scanner_code
        sub.numberOfRows = int(limit)

        tag_values: List[TagValue] = []
        data = None
        log.info(
            "Scanner live request: code=%s limit=%s instrument=%s location=%s",
            scanner_code,
            limit,
            sub.instrument,
            sub.locationCode,
        )
        # reqScannerData blocks until the initial scan payload is available.
        data = self.ib.reqScannerData(sub, tag_values, [])

        syms: List[str] = []
        for row in list(data)[:limit]:
            cd = getattr(row, "contractDetails", None)
            c = getattr(cd, "contract", None) if cd else None
            sym = getattr(c, "symbol", None)
            if sym:
                syms.append(str(sym))
        log.info(f"Scanner live result: code={scanner_code} symbols={len(syms)}")
        return syms

    def _scan_cached(self, scanner_code: str, limit: int, location_code: str) -> List[str]:
        now = time.time()
        cache_key = f"{location_code}|{scanner_code}"
        cached = self._scan_cache.get(cache_key)
        if cached:
            ts, syms = cached
            if now - ts < float(self.cfg.scanner_refresh_sec):
                log.info(f"Scanner cache hit: location={location_code} code={scanner_code} age={now - ts:.1f}s symbols={len(syms)}")
                return syms

        log.info(f"Scanner cache miss: location={location_code} code={scanner_code}")
        syms = self._scan_once(scanner_code, limit, location_code=location_code)
        self._scan_cache[cache_key] = (now, syms)
        return syms

    # ---------------- Phase1-B: persistent blacklist ----------------
    def _refresh_blacklist_from_storage(self) -> None:
        if self.storage is None or not hasattr(self.storage, "get_md_blacklist_active"):
            return
        try:
            now = int(time.time())
            rows = self.storage.get_md_blacklist_active(now)
            for sym, _status, _reason, until_ts, _updated in rows:
                if sym and int(until_ts) > now:
                    self._cooldown_until[str(sym)] = max(self._cooldown_until.get(str(sym), 0.0), float(until_ts))
        except Exception:
            return

    def _persist_blacklist(self, symbol: str, reason: str, minutes: int) -> None:
        if self.storage is None or not hasattr(self.storage, "upsert_md_blacklist"):
            return
        try:
            until_ts = int(time.time() + 60 * int(minutes))
            self.storage.upsert_md_blacklist(str(symbol), "COOLDOWN", str(reason), until_ts)
        except Exception:
            return

    def _refresh_cooldown_from_storage(self) -> None:
        if not getattr(self.cfg, "cooldown_enabled", False):
            return
        if self.storage is None or not hasattr(self.storage, "get_md_quality"):
            return
        try:
            day = time.strftime("%Y-%m-%d", time.gmtime(time.time()))
            rows = self.storage.get_md_quality(day)
            now = time.time()
            for (_day, sym, buckets, duplicates, max_gap_sec, _last_end, _updated_ts) in rows:
                if not sym:
                    continue
                b = int(buckets or 0)
                if b < int(getattr(self.cfg, "min_buckets_for_eval", 1)):
                    continue
                d = int(duplicates or 0)
                score = float(d) / float(b + 1)
                gap = int(max_gap_sec or 0)

                if score > float(getattr(self.cfg, "dup_per_bucket_threshold", 10.0)) or gap > int(getattr(self.cfg, "max_gap_sec_threshold", 1800)):
                    until = now + 60.0 * float(getattr(self.cfg, "cooldown_minutes", 30))
                    self._cooldown_until[str(sym)] = max(self._cooldown_until.get(str(sym), 0.0), until)
                    reason = f"md_quality: dup_per_bucket={score:.2f} gap={gap}s"
                    self._persist_blacklist(str(sym), reason, int(getattr(self.cfg, "cooldown_minutes", 30)))
        except Exception:
            return

    def _apply_cooldown(self, xs: List[str]) -> List[str]:
        if not xs:
            return xs
        now = time.time()
        out: List[str] = []
        for s in xs:
            until = float(self._cooldown_until.get(str(s), 0.0) or 0.0)
            if until > now:
                continue
            out.append(s)
        return out

    # ---------------- Phase3: metrics + filters + scoring ----------------
    def _repeat_penalty_factor(self, symbol: str) -> float:
        """Return [0..1] factor, lower means heavier penalty."""
        now = time.time()
        count, last_ts = self._repeat_state.get(symbol, (0.0, 0.0))

        # exponential decay of count by halflife
        halflife = float(getattr(self.cfg, "phase3_repeat_halflife_min", 60.0)) * 60.0
        if halflife > 0 and last_ts > 0:
            dt = max(0.0, now - last_ts)
            decay = 0.5 ** (dt / halflife)
            count *= decay

        count += 1.0
        self._repeat_state[symbol] = (count, now)

        strength = float(getattr(self.cfg, "phase3_repeat_penalty", 0.25))
        # penalty factor: 1 / (1 + strength*(count-1))
        return 1.0 / (1.0 + strength * max(0.0, count - 1.0))

    def _get_metrics(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        Use MarketDataService.get_5m_bars() to compute:
        - last_close
        - avg_vol (5m volume)
        - atr_pct (ATR / close)
        Cached for short time to avoid spamming historical requests.
        """
        if self.md is None or not hasattr(self.md, "get_5m_bars"):
            return None

        now = time.time()
        cached = self._metrics_cache.get(symbol)
        if cached and (now - cached[0] < 30.0):  # 30s cache
            return cached[1]

        try:
            need = int(getattr(self.cfg, "phase3_lookback_bars", 48))
            bars = self.md.get_5m_bars(symbol, need=need)
            if not bars or len(bars) < max(10, need // 3):
                return None

            closes = [float(b.close) for b in bars]
            highs = [float(b.high) for b in bars]
            lows = [float(b.low) for b in bars]
            vols = [float(getattr(b, "volume", 0.0) or 0.0) for b in bars]

            last_close = float(closes[-1]) if closes else 0.0
            avg_vol = sum(vols) / max(1.0, float(len(vols)))

            # ATR (simple)
            trs: List[float] = []
            for i in range(1, len(bars)):
                tr = max(
                    highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]),
                )
                trs.append(float(tr))
            atr = sum(trs[-20:]) / max(1.0, float(min(20, len(trs))))
            atr_pct = (atr / last_close) if last_close > 0 else 0.0

            m = {"last_close": last_close, "avg_vol": float(avg_vol), "atr_pct": float(atr_pct)}
            self._metrics_cache[symbol] = (now, m)
            return m
        except Exception:
            return None

    def _phase3_filter_and_rank(self, hot_syms: List[str]) -> List[str]:
        """
        Return filtered + ranked hot symbols.
        If md is not available, fall back to original order (no filter/rank).
        """
        if not hot_syms:
            return hot_syms
        if self.md is None:
            return hot_syms

        price_min = float(getattr(self.cfg, "phase3_price_min", 2.0))
        vol_min = float(getattr(self.cfg, "phase3_avg_vol_min", 50_000.0))
        atr_min = float(getattr(self.cfg, "phase3_atr_pct_min", 0.002))

        scored: List[Tuple[float, str, Dict[str, float]]] = []
        for s in hot_syms:
            metrics = self._get_metrics(s)
            if metrics is None:
                # metrics missing -> short cooldown (optional)
                if getattr(self.cfg, "phase3_bad_symbol_cooldown_min", 0) > 0:
                    self._cooldown_until[s] = max(self._cooldown_until.get(s, 0.0), time.time() + 60.0 * int(self.cfg.phase3_bad_symbol_cooldown_min))
                continue

            px = float(metrics["last_close"])
            av = float(metrics["avg_vol"])
            atrp = float(metrics["atr_pct"])

            if px < price_min or av < vol_min or atrp < atr_min:
                # persist a short cooldown so it stops bouncing in/out
                if getattr(self.cfg, "phase3_bad_symbol_cooldown_min", 0) > 0:
                    reason = f"phase3_filter: px={px:.2f} avgv={av:.0f} atr%={atrp:.4f}"
                    self._persist_blacklist(s, reason, int(self.cfg.phase3_bad_symbol_cooldown_min))
                    self._cooldown_until[s] = max(self._cooldown_until.get(s, 0.0), time.time() + 60.0 * int(self.cfg.phase3_bad_symbol_cooldown_min))
                continue

            repeat_factor = self._repeat_penalty_factor(s)
            score, detail = self._phase3_score_symbol(px=px, avg_vol=av, atr_pct=atrp, repeat_factor=repeat_factor)
            scored.append((float(score), s, detail))

        scored.sort(key=lambda x: x[0], reverse=True)
        if scored:
            top = [
                f"{sym}:score={score:.2f},px={detail['price']:.2f},avgv={detail['avg_vol']:.0f},atr%={detail['atr_pct']:.4f},rep={detail['repeat_factor']:.2f}"
                for score, sym, detail in scored[:5]
            ]
            log.info("Phase3 ranked hot symbols: %s", top)
        return [s for _score, s, _detail in scored]

    def _phase3_score_symbol(
        self,
        *,
        px: float,
        avg_vol: float,
        atr_pct: float,
        repeat_factor: float,
    ) -> Tuple[float, Dict[str, float]]:
        volume_log_weight = float(getattr(self.cfg, "phase3_volume_log_weight", 1.0))
        atr_pct_weight = float(getattr(self.cfg, "phase3_atr_pct_weight", 100.0))
        price_bonus_weight = float(getattr(self.cfg, "phase3_price_bonus_weight", 0.0))
        price_bonus_ref = max(1.0, float(getattr(self.cfg, "phase3_price_bonus_ref", 20.0)))

        volume_component = volume_log_weight * math.log(max(1.0, avg_vol))
        atr_component = atr_pct_weight * atr_pct
        price_component = price_bonus_weight * math.log(max(1.0, px / price_bonus_ref))
        raw_score = volume_component + atr_component + price_component
        score = raw_score * repeat_factor

        return score, {
            "price": float(px),
            "avg_vol": float(avg_vol),
            "atr_pct": float(atr_pct),
            "repeat_factor": float(repeat_factor),
            "volume_component": float(volume_component),
            "atr_component": float(atr_component),
            "price_component": float(price_component),
            "raw_score": float(raw_score),
        }

    # ---------------- The critical method: build() ----------------
    def build(self) -> Dict[str, List[str]]:
        holdings = sorted(self._positions_symbols())
        open_syms = sorted(self._open_trade_symbols())
        recent_syms = sorted(self._recent_traded_symbols())

        all_seeds = list(getattr(self.cfg, "seed_symbols", []) or [])
        active_seeds = self._batched_seeds(all_seeds)
        always_on = self._dedupe_keep_order(holdings + open_syms + recent_syms + active_seeds)

        # Phase1-B: persistent blacklist + md_quality cooldown
        self._refresh_blacklist_from_storage()
        self._refresh_cooldown_from_storage()
        always_on = self._apply_cooldown(always_on)

        hot: List[str] = []
        need_fill = len(always_on) < int(self.cfg.max_short_candidates)
        if bool(getattr(self.cfg, "scanner_enabled", True)) and need_fill:
            with self._scan_lock:
                location_codes = scanner_location_codes_from_config(self.cfg)
                codes = list(self.cfg.scanner_codes)[: int(self.cfg.scanner_max_codes_per_run)]
                for location_code in location_codes:
                    for code in codes:
                        try:
                            hot.extend(self._scan_cached(code, limit=int(self.cfg.scanner_limit), location_code=location_code))
                        except Exception as e:
                            log.error(f"scanner location={location_code} code={code} failed: {type(e).__name__} {e}")

        hot_unique = self._dedupe_keep_order(hot)
        hot_unique = self._apply_cooldown(hot_unique)

        # Phase3: filter + rank hot list (default OFF)
        if bool(getattr(self.cfg, "phase3_enabled", False)) and hot_unique:
            hot_unique = self._phase3_filter_and_rank(hot_unique)
            hot_unique = self._apply_cooldown(hot_unique)

        n = int(self.cfg.max_short_candidates)
        short_candidates = list(always_on)
        if len(short_candidates) < n:
            for s in hot_unique:
                if s not in short_candidates:
                    short_candidates.append(s)
                    if len(short_candidates) >= n:
                        break

        log.info(
            f"Universe built: holdings={len(holdings)} seeds_total={len(all_seeds)} seeds_active={len(active_seeds)} always_on={len(always_on)} "
            f"hot={len(hot_unique)} short={len(short_candidates)} "
            f"(scanner_enabled={self.cfg.scanner_enabled}, phase3_enabled={self.cfg.phase3_enabled})"
        )
        if bool(getattr(self.cfg, "scanner_enabled", True)) and need_fill and not hot_unique:
            log.info("Universe hot candidates empty: scanner returned no usable symbols or market data filters removed them")
        if hot_unique:
            log.info(f"Universe hot candidates: {hot_unique[:10]}")
        if short_candidates:
            log.info(f"Universe trading candidates: {short_candidates[:10]}")

        return {
            "holdings": holdings,
            "always_on": always_on,
            "hot": hot_unique,
            "short_candidates": short_candidates,
        }


def scanner_location_codes_from_config(config_like, default: str = "STK.US.MAJOR") -> List[str]:
    if isinstance(config_like, dict):
        raw_codes = list(config_like.get("scanner_location_codes") or [])
        raw_code = config_like.get("scanner_location_code", "")
    else:
        raw_codes = list(getattr(config_like, "scanner_location_codes", []) or [])
        raw_code = getattr(config_like, "scanner_location_code", "")

    seen = set()
    out: List[str] = []
    for raw in list(raw_codes) + [raw_code]:
        code = str(raw or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    if out:
        return out
    fallback = str(default or "").strip()
    return [fallback] if fallback else []
