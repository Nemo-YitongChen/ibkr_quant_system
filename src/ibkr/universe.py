# src/ibkr/universe.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import logging

log = logging.getLogger(__name__)


@dataclass
class UniverseConfig:
    max_short_candidates: int = 15
    scanner_codes: List[str] = field(default_factory=lambda: ["HOT_BY_VOLUME", "TOP_PERC_GAIN", "TOP_PERC_LOSE"])


class UniverseService:
    def __init__(self, ib, cfg: UniverseConfig):
        self.ib = ib
        self.cfg = cfg

    # ---- you already have these helpers in your codebase ----
    def _positions_symbols(self) -> List[str]:
        # return list of symbols in current positions
        raise NotImplementedError

    def _open_trade_symbols(self) -> List[str]:
        # return list of symbols with open orders/trades
        raise NotImplementedError

    def _recent_traded_symbols(self) -> List[str]:
        # return list of symbols traded recently (anti-repeat + continuity)
        raise NotImplementedError

    def _scan(self, scanner_code: str, limit: int = 20) -> List[str]:
        # return list of hot symbols from IB scanner
        raise NotImplementedError

    @staticmethod
    def _dedupe_keep_order(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
        return out

    # ---- requested aggregated build() ----
    def build(self) -> Dict[str, List[str]]:
        holdings = sorted(self._positions_symbols())
        open_syms = sorted(self._open_trade_symbols())
        recent_syms = sorted(self._recent_traded_symbols())

        # 必跟踪标的：持仓 + 未完成订单/交易 + 最近交易（闭环连续性）
        always_on = self._dedupe_keep_order(holdings + open_syms + recent_syms)

        # 热点：从 scanner 拉取，失败不影响主流程
        hot: List[str] = []
        for code in self.cfg.scanner_codes:
            try:
                hot.extend(self._scan(code, limit=20))
            except Exception as e:
                log.error(f"scanner {code} failed: {type(e).__name__} {e}")

        hot_unique = self._dedupe_keep_order(hot)

        # short_candidates：每轮必须包含 always_on；热点只在有空位时补足到 N
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