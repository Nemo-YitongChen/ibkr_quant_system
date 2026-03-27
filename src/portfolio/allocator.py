from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, Tuple

from ..common.logger import get_logger

log = get_logger("portfolio.allocator")

if TYPE_CHECKING:
    from ..risk.model import PreTradeRiskSnapshot


@dataclass
class AllocatorConfig:
    risk_per_trade: float = 0.002
    max_open_positions: int = 8
    max_gross_leverage: float = 1.2
    enable_min_order_qty_floor: bool = False
    min_order_qty: float = 1.0


class PortfolioAllocator:
    """Minimal portfolio gate for position count, leverage, and per-trade sizing."""

    def __init__(self, ib: Any, account: Any, cfg: AllocatorConfig):
        self.ib = ib
        self.account = account
        self.cfg = cfg

    def _netliq(self) -> float:
        try:
            return float(self.account.get_netliq() or 0.0)
        except Exception:
            return 0.0

    def _open_position_count(self) -> int:
        count = 0
        try:
            for p in self.ib.positions():
                if abs(float(getattr(p, "position", 0.0) or 0.0)) > 0:
                    count += 1
        except Exception:
            return 0
        return count

    def _gross_exposure(self) -> float:
        gross = 0.0
        try:
            for p in self.ib.positions():
                qty = abs(float(getattr(p, "position", 0.0) or 0.0))
                avg_cost = abs(float(getattr(p, "avgCost", 0.0) or 0.0))
                if qty > 0 and avg_cost > 0:
                    gross += qty * avg_cost
        except Exception:
            return 0.0
        return gross

    def can_open(self, notional: float) -> Tuple[bool, str]:
        open_positions = self._open_position_count()
        if open_positions >= int(self.cfg.max_open_positions):
            return False, "blocked: max_open_positions"

        netliq = self._netliq()
        if netliq > 0:
            next_gross = self._gross_exposure() + abs(float(notional))
            if (next_gross / netliq) > float(self.cfg.max_gross_leverage):
                return False, "blocked: max_gross_leverage"

        return True, "allowed"

    def size_qty(
        self,
        requested_qty: float,
        entry_price: float,
        stop_loss_pct: float | None = None,
        risk_snapshot: "PreTradeRiskSnapshot | None" = None,
        risk_per_share: float | None = None,
    ) -> float:
        if requested_qty <= 0 or entry_price <= 0:
            return 0.0

        raw_requested_qty = float(requested_qty)
        min_qty_floor = float(self.cfg.min_order_qty) if bool(self.cfg.enable_min_order_qty_floor) else 0.0

        if risk_snapshot is not None:
            requested_qty *= max(0.0, 1.0 - float(risk_snapshot.liquidity_haircut))

        netliq = self._netliq()
        if netliq <= 0:
            return float(requested_qty)

        risk_budget = netliq * float(self.cfg.risk_per_trade)
        per_share_risk = 0.0
        if risk_snapshot is not None and float(risk_snapshot.risk_per_share) > 0:
            per_share_risk = float(risk_snapshot.risk_per_share)
        elif risk_per_share is not None and float(risk_per_share) > 0:
            per_share_risk = float(risk_per_share)
        elif stop_loss_pct is not None and float(stop_loss_pct) > 0:
            per_share_risk = entry_price * float(stop_loss_pct)
        if per_share_risk <= 0:
            if requested_qty >= 1.0:
                return float(int(requested_qty))
            if min_qty_floor >= 1.0 and raw_requested_qty > 0:
                return float(int(min_qty_floor))
            return 0.0

        max_qty = risk_budget / per_share_risk
        sized = min(float(requested_qty), float(max_qty))
        if sized < 1.0:
            if min_qty_floor >= 1.0 and float(max_qty) >= min_qty_floor and raw_requested_qty > 0:
                return float(int(min_qty_floor))
            return 0.0
        rounded = float(int(sized))
        if min_qty_floor >= 1.0 and rounded < min_qty_floor and float(max_qty) >= min_qty_floor and raw_requested_qty > 0:
            return float(int(min_qty_floor))
        return rounded
