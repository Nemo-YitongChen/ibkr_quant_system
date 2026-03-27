from __future__ import annotations

from dataclasses import dataclass
import json
from ib_insync import IB, Contract, LimitOrder, StopOrder
from ..common.logger import get_logger
from ..common.storage import Storage

log = get_logger("ibkr.orders")

@dataclass
class BracketParams:
    take_profit_pct: float = 0.004
    stop_loss_pct: float = 0.006
    take_profit_price: float | None = None
    stop_loss_price: float | None = None

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
        msg = (errorString or "")
        low = msg.lower()

        # --- 420: no market data permission (non-fatal). Downgrade to INFO and return.
        if errorCode == 420:
            log.info(f"IBKR Info: reqId={reqId} code=420 msg={msg} contract={contract}")
            return

        # --- 162: common benign / pacing / duplicate subscription noise (downgrade)
        if errorCode == 162:
            if (
                "scanner subscription cancelled" in low
                or "duplicate scan subscription" in low
                or "historical market data service error message" in low
            ):
                log.info(f"IBKR Info: reqId={reqId} code=162 msg={msg}")
                return

        # Everything else stays as error
        log.error(f"IBKR Error: reqId={reqId} code={errorCode} msg={msg} contract={contract}")

    @staticmethod
    def resolve_bracket_prices(action: str, entry_price: float, params: BracketParams) -> tuple[float, float, str, str]:
        action = str(action or "").upper()
        tp_action = "SELL" if action == "BUY" else "BUY"
        sl_action = "SELL" if action == "BUY" else "BUY"

        explicit_tp = float(params.take_profit_price or 0.0)
        explicit_sl = float(params.stop_loss_price or 0.0)
        if explicit_tp > 0 and explicit_sl > 0:
            return round(explicit_tp, 2), round(explicit_sl, 2), tp_action, sl_action

        if action == "BUY":
            tp_price = round(entry_price * (1 + params.take_profit_pct), 2)
            sl_price = round(entry_price * (1 - params.stop_loss_pct), 2)
        else:
            tp_price = round(entry_price * (1 - params.take_profit_pct), 2)
            sl_price = round(entry_price * (1 + params.stop_loss_pct), 2)
        return tp_price, sl_price, tp_action, sl_action

    def place_bracket(
        self,
        contract: Contract,
        action: str,
        qty: float,
        entry_price: float,
        params: BracketParams,
        risk_snapshot=None,
        signal_reason: str = "",
        signal_tag: str = "",
        signal_source: str = "",
    ):
        """
        Fixes:
        1) Pre-assign orderId so children parentId != 0 (ib_insync assigns orderId in placeOrder otherwise). :contentReference[oaicite:3]{index=3}
        2) Explicit tif='DAY' to match IB order preset override message. :contentReference[oaicite:4]{index=4}
        """
        contract = self.qualify(contract)
        action = action.upper()
        tp_price, sl_price, tp_action, sl_action = self.resolve_bracket_prices(action, float(entry_price), params)

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

        risk_payload = {}
        if risk_snapshot is not None and hasattr(risk_snapshot, "to_dict"):
            try:
                risk_payload = risk_snapshot.to_dict()
            except Exception:
                risk_payload = {}

        symbol = str(getattr(contract, "symbol", "") or "")
        exchange = str(getattr(contract, "exchange", "") or "")
        currency = str(getattr(contract, "currency", "") or "")
        parent_details = json.dumps(
            {
                "expected_price": float(entry_price),
                "expected_slippage_bps": float(risk_payload.get("slippage_bps", 0.0) or 0.0),
                "risk_snapshot": risk_payload,
                "signal_reason": signal_reason,
                "signal_tag": signal_tag,
                "signal_source": signal_source,
                "leg": "parent",
                "stop_loss_price": float(sl_price),
                "take_profit_price": float(tp_price),
            },
            ensure_ascii=False,
        )
        tp_details = json.dumps(
            {
                "expected_price": float(tp_price),
                "risk_snapshot": risk_payload,
                "signal_reason": signal_reason,
                "signal_tag": signal_tag,
                "signal_source": signal_source,
                "leg": "take_profit",
            },
            ensure_ascii=False,
        )
        sl_details = json.dumps(
            {
                "expected_price": float(sl_price),
                "risk_snapshot": risk_payload,
                "signal_reason": signal_reason,
                "signal_tag": signal_tag,
                "signal_source": signal_source,
                "leg": "stop_loss",
            },
            ensure_ascii=False,
        )

        self.storage.insert_order(
            {
                "account_id": self.account_id,
                "symbol": symbol,
                "exchange": exchange,
                "currency": currency,
                "action": action,
                "qty": qty,
                "order_type": "LMT",
                "order_id": parent_id,
                "parent_id": 0,
                "status": "CREATED",
                "details": parent_details,
            }
        )
        self.storage.insert_order(
            {
                "account_id": self.account_id,
                "symbol": symbol,
                "exchange": exchange,
                "currency": currency,
                "action": tp_action,
                "qty": qty,
                "order_type": "LMT",
                "order_id": tp_id,
                "parent_id": parent_id,
                "status": "CREATED",
                "details": tp_details,
            }
        )
        self.storage.insert_order(
            {
                "account_id": self.account_id,
                "symbol": symbol,
                "exchange": exchange,
                "currency": currency,
                "action": sl_action,
                "qty": qty,
                "order_type": "STP",
                "order_id": sl_id,
                "parent_id": parent_id,
                "status": "CREATED",
                "details": sl_details,
            }
        )

        # Place in correct sequence (parent, TP, SL-last transmit=True)
        t1 = self.ib.placeOrder(contract, parent)
        t2 = self.ib.placeOrder(contract, take_profit)
        t3 = self.ib.placeOrder(contract, stop_loss)

        return (t1, t2, t3)
