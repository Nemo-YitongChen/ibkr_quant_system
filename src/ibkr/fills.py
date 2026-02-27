from __future__ import annotations

from typing import TYPE_CHECKING, Protocol
from ib_insync import IB, Trade, Fill, CommissionReport

from ..common.logger import get_logger
from ..common.storage import Storage
from ..risk.ledger import Ledger

log = get_logger("ibkr.fills")

# 只在类型检查时导入，避免运行时循环导入
if TYPE_CHECKING:
    from ..risk.limits import DailyRiskGate  # noqa: F401


class GateLike(Protocol):
    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None: ...


class FillProcessor:
    def __init__(self, ib: IB, storage: Storage, gate: GateLike):
        self.ib = ib
        self.storage = storage
        self.gate = gate
        self.ledger = Ledger()

        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.commissionReportEvent += self._on_commission  # ib_insync event :contentReference[oaicite:2]{index=2}

        self._gross_by_exec = {}  # execId -> (symbol, realized_gross)

    def _on_exec_details(self, trade: Trade, fill: Fill):
        c = fill.contract
        e = fill.execution
        symbol = getattr(c, "symbol", "")
        action = e.side
        qty = float(e.shares)
        price = float(e.price)

        realized_gross = self.ledger.on_fill(symbol, action, qty, price)
        self._gross_by_exec[str(e.execId)] = (symbol, realized_gross)

        self.storage.insert_fill({
            "order_id": int(e.orderId),
            "exec_id": str(e.execId),
            "symbol": symbol,
            "action": action,
            "qty": qty,
            "price": price,
            "pnl": realized_gross,
            "details": "commission=pending"
        })

        log.info(f"Fill: {symbol} {action} {qty}@{price} realized_gross={realized_gross:.4f}")

    def _on_commission(self, trade: Trade, fill: Fill, report: CommissionReport):
        e = fill.execution
        exec_id = str(e.execId)
        symbol, realized_gross = self._gross_by_exec.get(exec_id, ("", 0.0))

        commission = float(getattr(report, "commission", 0.0) or 0.0)
        realized_net = realized_gross - commission

        self.storage.insert_risk_event("COMMISSION", commission, f"execId={exec_id} symbol={symbol}")

        # 把“金额净PnL”交给 Gate；Gate 内部用 NetLiquidation 折算百分比
        if realized_net != 0.0 or commission != 0.0:
            self.gate.on_trade_closed(
                trade_pnl=realized_net,
                details=f"{symbol} execId={exec_id} commission={commission}"
            )

        log.info(f"Commission: execId={exec_id} commission={commission:.4f} realized_net={realized_net:.4f}")