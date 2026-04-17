from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Protocol
from ib_insync import IB, Trade, Fill, CommissionReport

from ..common.logger import get_logger
from ..common.storage import Storage
from ..risk.ledger import Ledger, normalize_action
from ..risk.model import execution_slippage_bps

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

    @staticmethod
    def _normalize_exec_symbol(
        raw_symbol: str,
        *,
        order_meta: dict | None = None,
        exchange: str = "",
        currency: str = "",
    ) -> str:
        meta = order_meta if isinstance(order_meta, dict) else {}
        stored_symbol = str(meta.get("symbol") or "").upper().strip()
        if stored_symbol:
            return stored_symbol

        symbol = str(raw_symbol or "").upper().strip()
        exch = str(exchange or meta.get("exchange") or "").upper().strip()
        curr = str(currency or meta.get("currency") or "").upper().strip()

        if curr == "HKD" or exch == "SEHK" or symbol.endswith(".HK"):
            code = symbol[:-3].strip() if symbol.endswith(".HK") else symbol
            if code.isdigit():
                return f"{int(code):04d}.HK"
            return f"{code}.HK" if code else symbol

        if " " in symbol:
            parts = symbol.split()
            if len(parts) == 2 and len(parts[1]) == 1 and parts[1].isalpha():
                return f"{parts[0]}.{parts[1]}"
        return symbol

    def _on_exec_details(self, trade: Trade, fill: Fill):
        c = fill.contract
        e = fill.execution
        order_meta = self.storage.get_order_by_order_id(int(e.orderId))
        symbol = self._normalize_exec_symbol(
            getattr(c, "symbol", ""),
            order_meta=order_meta,
            exchange=str(getattr(c, "exchange", "") or ""),
            currency=str(getattr(c, "currency", "") or ""),
        )
        raw_action = str(e.side or "")
        action = normalize_action(raw_action)
        qty = float(e.shares)
        price = float(e.price)

        realized_gross = self.ledger.on_fill(symbol, action, qty, price)
        self._gross_by_exec[str(e.execId)] = (symbol, realized_gross)

        details_json = order_meta.get("details_json", {}) if isinstance(order_meta, dict) else {}
        expected_price = float(details_json.get("expected_price", 0.0) or 0.0)
        expected_slippage_bps = float(details_json.get("expected_slippage_bps", 0.0) or 0.0)
        actual_slippage = execution_slippage_bps(action, expected_price, price)
        slippage_deviation = actual_slippage - expected_slippage_bps
        risk_snapshot = details_json.get("risk_snapshot", {})
        event_risk_reason = str(risk_snapshot.get("event_risk_reason", "") or "")
        short_borrow_source = str(risk_snapshot.get("short_borrow_source", "") or "")
        risk_snapshot_json = json.dumps(risk_snapshot, ensure_ascii=False) if risk_snapshot else ""
        portfolio_id = str(order_meta.get("portfolio_id", "") or details_json.get("portfolio_id", "") or "")
        system_kind = str(order_meta.get("system_kind", "") or details_json.get("system_kind", "") or "")
        execution_run_id = str(order_meta.get("execution_run_id", "") or details_json.get("execution_run_id", "") or "")
        order_submit_ts = str(order_meta.get("ts", "") or "").strip()
        fill_ts = datetime.now(timezone.utc).isoformat()
        fill_delay_seconds = self._fill_delay_seconds(order_submit_ts, fill_ts)

        self.storage.insert_fill({
            "ts": fill_ts,
            "order_id": int(e.orderId),
            "exec_id": str(e.execId),
            "symbol": symbol,
            "action": action,
            "qty": qty,
            "price": price,
            "pnl": realized_gross,
            "details": f"commission=pending raw_action={raw_action}",
            "expected_price": expected_price,
            "expected_slippage_bps": expected_slippage_bps,
            "actual_slippage_bps": actual_slippage,
            "slippage_bps_deviation": slippage_deviation,
            "event_risk_reason": event_risk_reason,
            "short_borrow_source": short_borrow_source,
            "risk_snapshot_json": risk_snapshot_json,
            "portfolio_id": portfolio_id,
            "system_kind": system_kind,
            "execution_run_id": execution_run_id,
            "order_submit_ts": order_submit_ts,
            "fill_delay_seconds": fill_delay_seconds,
        })

        self.storage.insert_risk_event(
            "EXECUTION_SLIPPAGE_BPS",
            float(actual_slippage),
            f"execId={e.execId} symbol={symbol}",
            symbol=symbol,
            order_id=int(e.orderId),
            exec_id=str(e.execId),
            expected_price=expected_price,
            actual_price=price,
            expected_slippage_bps=expected_slippage_bps,
            actual_slippage_bps=actual_slippage,
            slippage_bps_deviation=slippage_deviation,
            event_risk_reason=event_risk_reason or None,
            short_borrow_source=short_borrow_source or None,
            risk_snapshot_json=risk_snapshot_json or None,
            portfolio_id=portfolio_id or None,
            system_kind=system_kind or None,
            execution_run_id=execution_run_id or None,
        )

        log.info(f"Fill: {symbol} {action} {qty}@{price} realized_gross={realized_gross:.4f}")

    def _on_commission(self, trade: Trade, fill: Fill, report: CommissionReport):
        e = fill.execution
        exec_id = str(e.execId)
        order_meta = self.storage.get_order_by_order_id(int(e.orderId))
        fallback_symbol, realized_gross = self._gross_by_exec.get(exec_id, ("", 0.0))
        symbol = self._normalize_exec_symbol(
            fallback_symbol or str(getattr(fill.contract, "symbol", "") or ""),
            order_meta=order_meta,
            exchange=str(getattr(fill.contract, "exchange", "") or ""),
            currency=str(getattr(fill.contract, "currency", "") or ""),
        )
        portfolio_id = str(order_meta.get("portfolio_id", "") or "")
        system_kind = str(order_meta.get("system_kind", "") or "")
        execution_run_id = str(order_meta.get("execution_run_id", "") or "")

        commission = float(getattr(report, "commission", 0.0) or 0.0)
        realized_net = realized_gross - commission

        self.storage.insert_risk_event(
            "COMMISSION",
            commission,
            f"execId={exec_id} symbol={symbol}",
            symbol=symbol,
            order_id=int(e.orderId),
            exec_id=exec_id,
            portfolio_id=portfolio_id or None,
            system_kind=system_kind or None,
            execution_run_id=execution_run_id or None,
        )

        # 把“金额净PnL”交给 Gate；Gate 内部用 NetLiquidation 折算百分比
        if realized_net != 0.0 or commission != 0.0:
            self.gate.on_trade_closed(
                trade_pnl=realized_net,
                details=f"{symbol} execId={exec_id} commission={commission}"
            )

        log.info(f"Commission: execId={exec_id} commission={commission:.4f} realized_net={realized_net:.4f}")

    @staticmethod
    def _fill_delay_seconds(order_submit_ts: str, fill_ts: str) -> float | None:
        submit_raw = str(order_submit_ts or "").strip()
        fill_raw = str(fill_ts or "").strip()
        if not submit_raw or not fill_raw:
            return None
        try:
            submit_dt = datetime.fromisoformat(submit_raw.replace("Z", "+00:00"))
            fill_dt = datetime.fromisoformat(fill_raw.replace("Z", "+00:00"))
        except Exception:
            return None
        if submit_dt.tzinfo is None:
            submit_dt = submit_dt.replace(tzinfo=timezone.utc)
        if fill_dt.tzinfo is None:
            fill_dt = fill_dt.replace(tzinfo=timezone.utc)
        return max(0.0, float((fill_dt - submit_dt).total_seconds()))
