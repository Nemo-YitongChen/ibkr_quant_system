# src/portfolio/entry_guard.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

from ib_insync import IB

from ..common.logger import get_logger

log = get_logger("entry_guard")


@dataclass
class GuardConfig:
    cooldown_sec: int = 600            # 10分钟（默认）
    strong_sig: float = 0.95           # 强信号绕过阈值
    sig_improve: float = 0.15          # 相比上次触发，信号增强幅度
    breakout_scale_min: float = 0.60   # 突破型机会 + 中线尺度足够才绕过


class EntryGuard:
    """
    两层防重复：
    1) 硬防重复（idempotency）：有仓位/有未完成交易则不再开仓
    2) 软冷却（cooldown）：默认10分钟防抖，但允许强信号/增强/突破绕过
    """

    def __init__(self, ib: IB, cfg: GuardConfig = GuardConfig()):
        self.ib = ib
        self.cfg = cfg
        # symbol -> (last_ts, last_total_sig, last_mid_scale)
        self._last: Dict[str, Tuple[float, float, float]] = {}

    # ---------- Hard rules ----------
    def _has_position(self, symbol: str) -> bool:
        for p in self.ib.positions():
            c = p.contract
            if getattr(c, "symbol", None) == symbol:
                try:
                    if float(p.position) != 0.0:
                        return True
                except Exception:
                    return True
        return False

    def _has_open_trade_or_order(self, symbol: str) -> bool:
        """
        以 openTrades 为准：更可靠、更快（ib_insync 文档也建议这么做）。
        """
        try:
            for t in self.ib.openTrades():
                if getattr(t.contract, "symbol", None) != symbol:
                    continue
                st = t.orderStatus.status
                remaining = float(t.orderStatus.remaining or 0.0)

                # 只要还有剩余或处在未终结状态，就认为“正在交易中”
                if remaining > 0:
                    return True
                if st in ("PendingSubmit", "PreSubmitted", "Submitted", "ApiPending"):
                    return True
        except Exception:
            pass
        return False

    # ---------- Soft cooldown ----------
    def _cooldown_active(self, symbol: str, now: float) -> Tuple[bool, Optional[Tuple[float, float, float]]]:
        prev = self._last.get(symbol)
        if not prev:
            return False, None
        last_ts, last_sig, last_scale = prev
        if now - last_ts < self.cfg.cooldown_sec:
            return True, prev
        return False, prev

    def record_entry(self, symbol: str, now: float, total_sig: float, mid_scale: float):
        self._last[symbol] = (now, total_sig, mid_scale)

    def can_open_long(
        self,
        symbol: str,
        now: float,
        total_sig: float,
        mid_scale: float,
        breakout: bool,
    ) -> Tuple[bool, str]:
        """
        返回 (allowed, reason)
        """

        # 1) 硬防重复：有仓/有未完成交易/挂单 -> 禁止重复开仓
        if self._has_position(symbol):
            return False, "blocked: has_position"
        if self._has_open_trade_or_order(symbol):
            return False, "blocked: has_open_trade_or_order"

        # 2) 软冷却：默认防抖，但允许绕过
        active, prev = self._cooldown_active(symbol, now)
        if not active:
            return True, "allowed: no_cooldown"

        # 冷却期内：允许绕过的条件（任一满足即可）
        _, last_sig, last_scale = prev  # type: ignore

        if total_sig >= self.cfg.strong_sig:
            return True, "allowed: strong_sig_override"

        if (total_sig - last_sig) >= self.cfg.sig_improve:
            return True, "allowed: sig_improved_override"

        if breakout and mid_scale >= self.cfg.breakout_scale_min:
            return True, "allowed: breakout_scale_override"

        return False, "blocked: cooldown_active"