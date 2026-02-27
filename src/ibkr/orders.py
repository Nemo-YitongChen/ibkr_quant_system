from __future__ import annotations

from dataclasses import dataclass
from ib_insync import IB, Contract, LimitOrder, StopOrder
from ..common.logger import get_logger
from ..common.storage import Storage

log = get_logger("ibkr.orders")

@dataclass
class BracketParams:
    take_profit_pct: float = 0.004
    stop_loss_pct: float = 0.006

class OrderService:
    def __init__(self, ib: IB, account_id: str, storage: Storage):
        self.ib = ib
        self.account_id = account_id
        self.storage = storage
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.errorEvent += self._on_error

    def qualify(self, contract: Contract) -> Contract:
        self.ib.qualifyContracts(contract)
        return contract

    def _on_order_status(self, trade):
        try:
            oid = trade.order.orderId
            st = trade.orderStatus.status
            log.info(f"OrderStatus: orderId={oid} status={st} filled={trade.orderStatus.filled} remaining={trade.orderStatus.remaining}")
            self.storage.update_order_status(oid, st)
        except Exception as e:
            log.error(f"orderStatus handler error: {e}")

    def _on_error(self, reqId, errorCode, errorString, contract):
        msg = errorString or ""

        # ✅ 162：scanner 被我们主动 cancel 时的“信息性”提示（不算错误）
        if errorCode == 162 and "scanner subscription cancelled" in msg.lower():
            log.info(f"IBKR Info: reqId={reqId} code={errorCode} msg={msg}")
            return

        # 其它 162（例如权限/禁用的 scanner type）仍然值得关注
        if errorCode == 162:
            log.warning(f"IBKR Warning: reqId={reqId} code={errorCode} msg={msg}")
            return

        log.error(f"IBKR Error: reqId={reqId} code={errorCode} msg={msg}")

    def place_bracket(self, contract: Contract, action: str, qty: float, entry_price: float, params: BracketParams):
        """
        Fixes:
        1) Pre-assign orderId so children parentId != 0 (ib_insync assigns orderId in placeOrder otherwise). :contentReference[oaicite:3]{index=3}
        2) Explicit tif='DAY' to match IB order preset override message. :contentReference[oaicite:4]{index=4}
        """
        contract = self.qualify(contract)
        action = action.upper()

        if action == "BUY":
            tp_price = round(entry_price * (1 + params.take_profit_pct), 2)
            sl_price = round(entry_price * (1 - params.stop_loss_pct), 2)
            tp_action = "SELL"
            sl_action = "SELL"
        else:
            tp_price = round(entry_price * (1 - params.take_profit_pct), 2)
            sl_price = round(entry_price * (1 + params.stop_loss_pct), 2)
            tp_action = "BUY"
            sl_action = "BUY"

        # --- IMPORTANT: pre-allocate IDs ---
        base_id = self.ib.client.getReqId()
        parent_id = base_id
        tp_id = base_id + 1
        sl_id = base_id + 2

        parent = LimitOrder(action, qty, entry_price, account=self.account_id, transmit=False, tif="DAY")
        parent.orderId = parent_id

        take_profit = LimitOrder(tp_action, qty, tp_price, account=self.account_id, transmit=False, tif="DAY")
        take_profit.orderId = tp_id
        take_profit.parentId = parent_id

        stop_loss = StopOrder(sl_action, qty, sl_price, account=self.account_id, transmit=True, tif="DAY")
        stop_loss.orderId = sl_id
        stop_loss.parentId = parent_id

        # Place in correct sequence (parent, TP, SL-last transmit=True)
        t1 = self.ib.placeOrder(contract, parent)
        t2 = self.ib.placeOrder(contract, take_profit)
        t3 = self.ib.placeOrder(contract, stop_loss)

        return (t1, t2, t3)