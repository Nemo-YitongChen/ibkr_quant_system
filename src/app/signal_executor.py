from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Optional

from ..common.logger import get_logger
from ..ibkr.contracts import make_stock_contract
from ..ibkr.orders import BracketParams
from ..risk.short_safety import ShortSafetyGate

log = get_logger("app.signal_executor")


class SignalExecutor:
    def __init__(
        self,
        *,
        orders: Any,
        cfg: Any,
        entry_guard: Any = None,
        allocator: Any = None,
        short_safety_gate: Optional[ShortSafetyGate] = None,
    ) -> None:
        self.orders = orders
        self.cfg = cfg
        self.entry_guard = entry_guard
        self.allocator = allocator
        self.short_safety_gate = short_safety_gate

    def _record_execution_event(
        self,
        kind: str,
        sig: Any,
        *,
        symbol: str,
        value: float = 0.0,
        details_suffix: str = "",
        event_risk_reason: str | None = None,
        short_borrow_source: str | None = None,
    ) -> None:
        try:
            storage = getattr(self.orders, "storage", None)
            if storage is None or not hasattr(storage, "insert_risk_event"):
                return
            payload = getattr(sig.risk_snapshot, "to_dict", lambda: {})()
            detail = (
                f"symbol={symbol} tag={sig.audit_tag or 'NA'} source={sig.audit_source or 'NA'} "
                f"channel={sig.channel or 'NA'}"
            )
            if details_suffix:
                detail = f"{detail} {details_suffix}"
            storage.insert_risk_event(
                kind,
                float(value),
                detail,
                symbol=symbol,
                expected_price=float(sig.entry_price),
                expected_slippage_bps=float(getattr(sig.risk_snapshot, "slippage_bps", 0.0) or 0.0),
                event_risk_reason=str(
                    event_risk_reason
                    if event_risk_reason is not None
                    else getattr(sig.risk_snapshot, "event_risk_reason", "") or ""
                ) or None,
                short_borrow_source=str(
                    short_borrow_source
                    if short_borrow_source is not None
                    else getattr(sig.risk_snapshot, "short_borrow_source", "") or ""
                ) or None,
                risk_snapshot_json=json.dumps(payload, ensure_ascii=False) if payload else None,
            )
        except Exception:
            pass

    def execute(self, symbol: str, sig: Any, runner: Any) -> None:
        if not sig.should_trade:
            return

        runtime_mode = str(getattr(self.cfg, "runtime_mode", "") or "").strip().lower()
        allowed_sources = {str(x or "").upper() for x in list(getattr(self.cfg, "paper_allowed_execution_sources", []) or [])}
        sig_source = str(sig.audit_source or "").upper()
        live_requires_realtime_source = runtime_mode == "live" and sig_source != "REALTIME"
        paper_source_blocked = runtime_mode == "paper" and allowed_sources and sig_source not in allowed_sources
        source_exec_allowed = not live_requires_realtime_source and not paper_source_blocked

        if bool(getattr(self.cfg, "enforce_pretrade_risk_gate", True)) and sig.risk_snapshot is not None and not bool(sig.risk_snapshot.allowed):
            self._record_execution_event(
                "PRETRADE_RISK_BLOCK",
                sig,
                symbol=symbol,
                value=float(getattr(sig.risk_snapshot, "risk_per_share", 0.0) or 0.0),
                details_suffix=f"reasons={','.join(getattr(sig.risk_snapshot, 'block_reasons', []) or [])}",
            )
            log.info("[%s] entry blocked by pretrade risk gate: %s", symbol, getattr(sig.risk_snapshot, "block_reasons", []))
            return

        qty = float(sig.qty)
        short_decision = None
        if sig.action == "SELL" and self.short_safety_gate is not None:
            avg_bar_volume = float(getattr(sig.risk_snapshot, "avg_bar_volume", 0.0) or 0.0)
            short_decision = self.short_safety_gate.evaluate(
                symbol,
                now=datetime.utcnow().astimezone(),
                avg_bar_volume=avg_bar_volume,
                action=sig.action,
                enforce_timing=True,
            )
            if not short_decision.allowed:
                event_name = "SHORT_SAFETY_SHADOW_BLOCK" if bool(getattr(self.short_safety_gate.cfg, "shadow_mode", False)) else "SHORT_SAFETY_BLOCK"
                shadow_mode = bool(getattr(self.short_safety_gate.cfg, "shadow_mode", False))
                should_record_shadow = source_exec_allowed or not shadow_mode
                if should_record_shadow:
                    self._record_execution_event(
                        event_name,
                        sig,
                        symbol=symbol,
                        value=float(getattr(sig.risk_snapshot, "risk_per_share", 0.0) or 0.0),
                        details_suffix=f"reasons={short_decision.blocked_reason_text()}",
                        event_risk_reason=str(short_decision.event_risk_reason or ""),
                    )
                if shadow_mode:
                    if not should_record_shadow:
                        log.info("[%s] short safety shadow skipped for non-executable source: %s", symbol, sig_source)
                    else:
                        log.info("[%s] short safety shadow block recorded: %s", symbol, short_decision.blocked_reason_text())
                else:
                    log.info("[%s] short blocked by safety gate: %s", symbol, short_decision.blocked_reason_text())
                    return
            qty *= float(short_decision.qty_multiplier)
            if qty <= 0:
                self._record_execution_event(
                    "SHORT_SAFETY_QTY_ZERO",
                    sig,
                    symbol=symbol,
                    details_suffix=f"qty_multiplier={float(short_decision.qty_multiplier):.3f}",
                    event_risk_reason=str(short_decision.event_risk_reason or ""),
                )
                log.info("[%s] short reduced to zero by safety gate", symbol)
                return

        if not source_exec_allowed:
            self._record_execution_event(
                "SOURCE_EXEC_BLOCK",
                sig,
                symbol=symbol,
                details_suffix=f"allowed_sources={','.join(sorted(allowed_sources))}",
            )
            log.info("[%s] entry blocked by source gate: source=%s allowed=%s", symbol, sig_source, sorted(allowed_sources))
            return

        now = time.time()
        breakout = abs(float(sig.short_sig)) >= float(self.cfg.short_threshold)
        if self.entry_guard is not None and hasattr(self.entry_guard, "can_open_trade"):
            allowed, reason = self.entry_guard.can_open_trade(
                symbol=symbol,
                now=now,
                total_sig=float(sig.total_sig),
                mid_scale=float(sig.mid_scale),
                breakout=breakout,
            )
            if not allowed:
                self._record_execution_event(
                    "ENTRY_GUARD_BLOCK",
                    sig,
                    symbol=symbol,
                    details_suffix=f"reason={reason}",
                )
                log.info("[%s] entry blocked by guard: %s", symbol, reason)
                return

        if self.allocator is not None:
            qty = float(
                self.allocator.size_qty(
                    requested_qty=qty,
                    entry_price=float(sig.entry_price),
                    risk_snapshot=sig.risk_snapshot,
                )
            )
            allowed, reason = self.allocator.can_open(notional=qty * float(sig.entry_price))
            if qty <= 0:
                self._record_execution_event(
                    "ALLOCATOR_QTY_ZERO",
                    sig,
                    symbol=symbol,
                    value=float(getattr(sig.risk_snapshot, "risk_per_share", 0.0) or 0.0),
                    details_suffix=f"requested_qty={float(sig.qty):.3f} sized_qty={float(qty):.3f}",
                )
                log.info("[%s] entry blocked by allocator: qty=%s", symbol, qty)
                return
            if not allowed:
                self._record_execution_event(
                    "ALLOCATOR_BLOCK",
                    sig,
                    symbol=symbol,
                    value=float(qty * float(sig.entry_price)),
                    details_suffix=f"reason={reason} qty={float(qty):.3f}",
                )
                log.info("[%s] entry blocked by allocator: %s qty=%s", symbol, reason, qty)
                return

        contract = make_stock_contract(symbol)
        order_entry_price = float(getattr(sig.risk_snapshot, "expected_fill_price", 0.0) or 0.0)
        if order_entry_price <= 0:
            order_entry_price = float(sig.entry_price)
        params = BracketParams(
            take_profit_pct=self.cfg.take_profit_pct,
            stop_loss_pct=self.cfg.stop_loss_pct,
            take_profit_price=float(getattr(sig.risk_snapshot, "take_profit_price", 0.0) or 0.0),
            stop_loss_price=float(getattr(sig.risk_snapshot, "stop_price", 0.0) or 0.0),
        )
        log.info("[%s] placing bracket: action=%s qty=%s entry=%.2f reason=%s", symbol, sig.action, qty, order_entry_price, sig.reason)
        trades = self.orders.place_bracket(
            contract=contract,
            action=sig.action,
            qty=qty,
            entry_price=order_entry_price,
            params=params,
            risk_snapshot=sig.risk_snapshot,
            signal_reason=sig.reason,
            signal_tag=sig.audit_tag,
            signal_source=sig.audit_source,
        )
        try:
            if self.entry_guard is not None and hasattr(self.entry_guard, "record_entry"):
                self.entry_guard.record_entry(symbol, now, float(sig.total_sig), float(sig.mid_scale))
            if hasattr(runner, "watch_entry_order") and trades:
                parent_trade = trades[0]
                oid = int(parent_trade.order.orderId)
                runner.watch_entry_order(oid, meta={"symbol": symbol, "reason": sig.reason})
        except Exception:
            pass
