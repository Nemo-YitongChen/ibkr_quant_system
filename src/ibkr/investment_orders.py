from __future__ import annotations

from dataclasses import dataclass
import json

from ib_insync import IB, Contract, LimitOrder, MarketOrder

from ..common.logger import get_logger
from ..common.storage import Storage

log = get_logger("ibkr.investment_orders")


@dataclass
class InvestmentOrderParams:
    order_type: str = "MKT"
    ref_price: float = 0.0
    limit_price_buffer_bps: float = 15.0
    tif: str = "DAY"
    outside_rth: bool = False
    route_exchange: str = ""
    include_overnight: bool = False


class InvestmentOrderService:
    HEALTH_INFO_CODES = {162, 2104, 2106, 2108, 2119, 1102, 10167}
    HEALTH_ERROR_CODES = {1100, 165, 322, 354, 2103, 2105, 2157, 2158}

    def __init__(
        self,
        ib: IB,
        account_id: str,
        storage: Storage,
        *,
        market: str = "",
        portfolio_id: str = "",
        system_kind: str = "investment_execution",
    ):
        self.ib = ib
        self.account_id = account_id
        self.storage = storage
        self.market = str(market or "").upper()
        self.portfolio_id = str(portfolio_id or "")
        self.system_kind = str(system_kind or "investment_execution")
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.errorEvent += self._on_error

    def qualify(self, contract: Contract) -> Contract:
        self.ib.qualifyContracts(contract)
        return contract

    def _on_order_status(self, trade):
        try:
            oid = int(trade.order.orderId)
            status = str(trade.orderStatus.status or "")
            self.storage.update_order_status(oid, status)
            self.storage.update_investment_execution_order_status(oid, status)
            log.info(
                "InvestmentOrderStatus: orderId=%s status=%s filled=%s remaining=%s",
                oid,
                status,
                trade.orderStatus.filled,
                trade.orderStatus.remaining,
            )
        except Exception as e:
            log.error("investment orderStatus handler error: %s", e)

    def _on_error(self, reqId, errorCode, errorString, contract):
        msg = str(errorString or "")
        code = int(errorCode or 0)
        req_id = int(reqId or 0)
        contract_symbol = str(getattr(contract, "symbol", "") or "").upper().strip()
        if contract_symbol and self.market == "HK" and contract_symbol.isdigit():
            contract_symbol = f"{int(contract_symbol):04d}.HK"
        if code in (self.HEALTH_INFO_CODES | self.HEALTH_ERROR_CODES):
            details = f"reqId={reqId} code={errorCode} msg={msg}"
            if contract_symbol:
                details = f"{details} symbol={contract_symbol}"
            self.storage.insert_risk_event(
                "IBKR_HEALTH_EVENT",
                float(code),
                details,
                symbol=contract_symbol or None,
                portfolio_id=self.portfolio_id or None,
                system_kind=self.system_kind,
            )
        if req_id > 0:
            status = f"ERROR_{code}"
            self.storage.update_order_status(req_id, status)
            self.storage.update_investment_execution_order_status(req_id, status)
            order_meta = self.storage.get_order_by_order_id(req_id)
            self.storage.insert_risk_event(
                "INVESTMENT_ORDER_ERROR",
                float(code),
                f"reqId={reqId} code={errorCode} msg={msg}",
                symbol=str(order_meta.get("symbol") or ""),
                order_id=req_id,
                portfolio_id=order_meta.get("portfolio_id"),
                system_kind=order_meta.get("system_kind"),
                execution_run_id=order_meta.get("execution_run_id"),
            )
        if code in {420} | self.HEALTH_INFO_CODES:
            log.info("IBKR Info: reqId=%s code=%s msg=%s contract=%s", reqId, errorCode, msg, contract)
            return
        log.error("IBKR Error: reqId=%s code=%s msg=%s contract=%s", reqId, errorCode, msg, contract)

    @staticmethod
    def _resolve_limit_price(action: str, ref_price: float, buffer_bps: float) -> float:
        price = float(ref_price or 0.0)
        if price <= 0:
            return 0.0
        bump = max(0.0, float(buffer_bps)) / 10000.0
        if str(action).upper() == "BUY":
            return round(price * (1.0 + bump), 2)
        return round(price * (1.0 - bump), 2)

    def place_rebalance_order(
        self,
        contract: Contract,
        *,
        symbol: str,
        action: str,
        qty: float,
        params: InvestmentOrderParams,
        portfolio_id: str,
        execution_run_id: str,
        plan_row: dict,
        system_kind: str = "investment_execution",
        signal_source: str = "investment_execution",
    ):
        contract = self.qualify(contract)
        action = str(action).upper()
        qty = float(qty)
        order_id = self.ib.client.getReqId()
        route_exchange = str(params.route_exchange or "").strip().upper()
        include_overnight = bool(params.include_overnight)
        order_type = str(params.order_type or "MKT").upper()
        if include_overnight and order_type != "LMT":
            order_type = "LMT"
        if route_exchange:
            try:
                contract.exchange = route_exchange
            except Exception:
                pass
        limit_price = 0.0
        if order_type == "LMT":
            limit_price = self._resolve_limit_price(action, float(params.ref_price or 0.0), float(params.limit_price_buffer_bps))
            order = LimitOrder(action, qty, limit_price, account=self.account_id, tif=str(params.tif or "DAY"))
        else:
            order = MarketOrder(action, qty, account=self.account_id, tif=str(params.tif or "DAY"))
        order.orderId = order_id
        order.outsideRth = bool(params.outside_rth)
        try:
            order.includeOvernight = bool(include_overnight)
        except Exception:
            pass

        details = json.dumps(
            {
                "system_kind": str(system_kind),
                "portfolio_id": portfolio_id,
                "execution_run_id": execution_run_id,
                "plan_row": plan_row,
                "expected_price": float(params.ref_price or 0.0),
                "limit_price": float(limit_price or 0.0),
                "signal_reason": str(plan_row.get("reason") or ""),
                "signal_source": str(signal_source),
                "leg": "rebalance",
                "route_exchange": route_exchange,
                "include_overnight": include_overnight,
                "tif": str(params.tif or "DAY"),
                "outside_rth": bool(params.outside_rth),
            },
            ensure_ascii=False,
        )
        self.storage.insert_order(
            {
                "account_id": self.account_id,
                "symbol": str(symbol).upper(),
                "exchange": str(getattr(contract, "exchange", "") or ""),
                "currency": str(getattr(contract, "currency", "") or ""),
                "action": action,
                "qty": qty,
                "order_type": order_type,
                "order_id": order_id,
                "parent_id": 0,
                "status": "CREATED",
                "portfolio_id": portfolio_id,
                "system_kind": str(system_kind),
                "execution_run_id": execution_run_id,
                "details": details,
            }
        )
        self.storage.insert_investment_execution_order(
            {
                "run_id": execution_run_id,
                "market": str(plan_row.get("market") or ""),
                "portfolio_id": portfolio_id,
                "symbol": str(symbol).upper(),
                "action": action,
                "current_qty": float(plan_row.get("current_qty") or 0.0),
                "target_qty": float(plan_row.get("target_qty") or 0.0),
                "delta_qty": float(plan_row.get("delta_qty") or 0.0),
                "ref_price": float(plan_row.get("ref_price") or 0.0),
                "target_weight": float(plan_row.get("target_weight") or 0.0),
                "order_value": float(plan_row.get("order_value") or 0.0),
                "order_type": order_type,
                "broker_order_id": int(order_id),
                "status": "CREATED",
                "reason": str(plan_row.get("reason") or ""),
                "details": details,
            }
        )
        return self.ib.placeOrder(contract, order)
