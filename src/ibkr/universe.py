# src/ibkr/universe.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import time
import logging
import threading

from ib_insync import ScannerSubscription, TagValue  # type: ignore

log = logging.getLogger(__name__)


@dataclass
class UniverseConfig:
    max_short_candidates: int = 15
    scanner_codes: List[str] = field(default_factory=lambda: ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])
    scanner_limit: int = 20
    recent_trade_limit: int = 30

    # ✅ 新增：扫描限频（秒）
    scanner_refresh_sec: int = 120  # 建议 60~300
    # ✅ 新增：每轮最多跑几个 scanner（<=10，建议更小）
    scanner_max_codes_per_run: int = 3


class UniverseService:
    def __init__(self, ib, cfg: UniverseConfig):
        self.ib = ib
        self.cfg = cfg

        self._lock = threading.Lock()
        self._scan_cache: Dict[str, Tuple[float, List[str]]] = {}  # code -> (ts, syms)

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
        out = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _scan_once(self, scanner_code: str, limit: int) -> List[str]:
        sub = ScannerSubscription()
        sub.instrument = "STK"
        sub.locationCode = "STK.US.MAJOR"
        sub.scanCode = scanner_code
        sub.numberOfRows = int(limit)

        tag_values: List[TagValue] = []
        data = None

        try:
            data = self.ib.reqScannerSubscription(sub, tag_values)
            syms: List[str] = []
            for row in list(data)[:limit]:
                cd = getattr(row, "contractDetails", None)
                c = getattr(cd, "contract", None) if cd else None
                sym = getattr(c, "symbol", None)
                if sym:
                    syms.append(str(sym))
            return syms
        finally:
            # ✅ 强制释放 scanner subscription，避免堆积到 10 个上限
            try:
                if data is not None:
                    req_id = getattr(data, "reqId", None)
                    if req_id is not None:
                        self.ib.cancelScannerSubscription(req_id)
                    else:
                        self.ib.cancelScannerSubscription(data)
            except Exception:
                pass

    def _scan_cached(self, scanner_code: str, limit: int) -> List[str]:
        now = time.time()
        ts_syms = self._scan_cache.get(scanner_code)
        if ts_syms:
            ts, syms = ts_syms
            if now - ts < float(self.cfg.scanner_refresh_sec):
                return syms

        syms = self._scan_once(scanner_code, limit)
        self._scan_cache[scanner_code] = (now, syms)
        return syms

    def build(self) -> Dict[str, List[str]]:
        holdings = sorted(self._positions_symbols())
        open_syms = sorted(self._open_trade_symbols())
        recent_syms = sorted(self._recent_traded_symbols())

        always_on = self._dedupe_keep_order(holdings + open_syms + recent_syms)

        hot: List[str] = []

        # ✅ 用锁防并发 build 导致同时开多条 scanner
        with self._lock:
            codes = list(self.cfg.scanner_codes)[: int(self.cfg.scanner_max_codes_per_run)]
            for code in codes:
                try:
                    hot.extend(self._scan_cached(code, limit=int(self.cfg.scanner_limit)))
                except Exception as e:
                    log.error(f"scanner {code} failed: {type(e).__name__} {e}")

        hot_unique = self._dedupe_keep_order(hot)

        n = int(self.cfg.max_short_candidates)
        short_candidates = list(always_on)
        if len(short_candidates) < n:
            for s in hot_unique:
                if s not in short_candidates:
                    short_candidates.append(s)
                    if len(short_candidates) >= n:
                        break

        log.info(
            f"Universe built: holdings={len(holdings)} always_on={len(always_on)} "
            f"hot={len(hot_unique)} short={len(short_candidates)}"
        )

        return {
            "holdings": holdings,
            "always_on": always_on,
            "hot": hot_unique,
            "short_candidates": short_candidates,
        }