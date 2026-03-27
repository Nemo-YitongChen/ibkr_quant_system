from __future__ import annotations

from dataclasses import dataclass, field
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..common.logger import get_logger
from ..events.models import RiskDecision, SignalDecision
from ..ibkr.contracts import make_stock_contract
from ..risk.model import PreTradeRiskSnapshot, TradeRiskConfig, TradeRiskModel
from ..regime.state import RegimeStateV2
from ..risk.short_safety import ShortSafetyGate
from .short_mean_reversion import MRConfig, signal as mr_signal
from .short_breakout import BOConfig, signal as bo_signal
from .mid_regime import RegimeConfig, evaluate_regime, to_regime_state_v2
from ..signals.fusion import fuse
from ..ibkr.orders import BracketParams

log = get_logger("strategy.engine")


@dataclass
class StrategyConfig:
    trade_threshold: float = 0.65
    base_qty: float = 1.0
    take_profit_pct: float = 0.004
    stop_loss_pct: float = 0.006
    runtime_mode: str = ""
    paper_allowed_execution_sources: List[str] = field(default_factory=lambda: ["REALTIME"])
    enforce_pretrade_risk_gate: bool = True

    # Phase2: Pure-Short channel (short_sig-only)
    enable_pure_short: bool = True
    short_threshold: float = 0.45

    # Phase2: mid as risk modulator (soft)
    # If mid_scale <= mid_soft_floor -> use mid_qty_min multiplier
    mid_soft_floor: float = 0.0
    mid_qty_min: float = 0.25
    mid_qty_max: float = 1.25

    risk: TradeRiskConfig = field(default_factory=TradeRiskConfig)
    mr: MRConfig = field(default_factory=MRConfig)
    bo: BOConfig = field(default_factory=BOConfig)
    mid: RegimeConfig = field(default_factory=RegimeConfig)


@dataclass
class TradeSignal:
    should_trade: bool
    action: str          # BUY / SELL
    qty: float
    entry_price: float
    total_sig: float
    short_sig: float
    mid_scale: float
    risk_on: bool = True
    regime_state: str = ""
    regime_reason: str = ""
    reason: str = ""
    channel: str = ""
    threshold_used: float = 0.0
    audit_tag: str = ""
    audit_source: str = ""
    risk_snapshot: Optional[PreTradeRiskSnapshot] = None
    regime_state_v2: Optional[RegimeStateV2] = None
    signal_decision: Optional[SignalDecision] = None
    risk_decision: Optional[RiskDecision] = None


class EngineStrategy:
    """
    Adapter used by app.engine.TradingEngine:
    - keep rolling OHLCV per symbol
    - compute short_sig (MR+BO), mid_scale (regime), total_sig (fuse)
    - threshold -> TradeSignal
    - execute via OrderService.place_bracket
    """

    def __init__(
        self,
        *,
        orders: Any,
        gate: Any,
        cfg: Optional[StrategyConfig] = None,
        max_bars: int = 600,
        entry_guard: Any = None,
        allocator: Any = None,
        short_safety_gate: Optional[ShortSafetyGate] = None,
    ):
        self.orders = orders
        self.gate = gate
        self.cfg = cfg or StrategyConfig()
        self.max_bars = int(max_bars)
        self.entry_guard = entry_guard
        self.allocator = allocator
        self.short_safety_gate = short_safety_gate
        self.risk_model = TradeRiskModel(self.cfg.risk)

        self._open: Dict[str, List[float]] = {}
        self._high: Dict[str, List[float]] = {}
        self._low: Dict[str, List[float]] = {}
        self._close: Dict[str, List[float]] = {}
        self._vol: Dict[str, List[float]] = {}

        # Phase1-C: set by engine (best-effort)
        self._audit_tag: str = ""
        self._audit_source: str = ""

    def required_bars(self) -> int:
        return max(
            int(self.cfg.mr.lookback),
            int(self.cfg.bo.lookback) + int(self.cfg.bo.confirm),
            int(self.cfg.mid.ma_slow) + 2,
        )

    def bar_count(self, symbol: str) -> int:
        return len(self._close.get(symbol, []))

    def preload_bars(self, symbol: str, bars: List[Any]) -> int:
        appended = 0
        for bar in bars:
            self._append_bar(symbol, bar)
            appended += 1
        return appended

    def _append_bar(self, symbol: str, bar: Any) -> None:
        self._open.setdefault(symbol, []).append(float(bar.open))
        self._high.setdefault(symbol, []).append(float(bar.high))
        self._low.setdefault(symbol, []).append(float(bar.low))
        self._close.setdefault(symbol, []).append(float(bar.close))
        self._vol.setdefault(symbol, []).append(float(getattr(bar, "volume", 0.0) or 0.0))

        for d in (self._open, self._high, self._low, self._close, self._vol):
            xs = d[symbol]
            if len(xs) > self.max_bars:
                del xs[:-self.max_bars]

    @staticmethod
    def _clip(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def _gate_event_risk(self, symbol: str) -> str:
        gate = self.gate
        if gate is None:
            return "NONE"
        if hasattr(gate, "event_risk_for"):
            try:
                return str(gate.event_risk_for(symbol) or "NONE").upper()
            except Exception:
                return "NONE"
        if hasattr(gate, "event_risk"):
            try:
                return str(getattr(gate, "event_risk") or "NONE").upper()
            except Exception:
                return "NONE"
        return "NONE"

    def _gate_short_borrow_fee_bps(self, symbol: str) -> float:
        gate = self.gate
        if gate is None:
            return 0.0
        if hasattr(gate, "short_borrow_fee_bps_for"):
            try:
                return float(gate.short_borrow_fee_bps_for(symbol) or 0.0)
            except Exception:
                return 0.0
        if hasattr(gate, "short_borrow_fee_bps"):
            try:
                return float(getattr(gate, "short_borrow_fee_bps") or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _gate_event_risk_reason(self, symbol: str) -> str:
        gate = self.gate
        if gate is None:
            return ""
        if hasattr(gate, "event_risk_reason_for"):
            try:
                return str(gate.event_risk_reason_for(symbol) or "")
            except Exception:
                return ""
        return ""

    def _gate_short_borrow_source(self, symbol: str) -> str:
        gate = self.gate
        if gate is None:
            return ""
        if hasattr(gate, "short_borrow_source_for"):
            try:
                return str(gate.short_borrow_source_for(symbol) or "")
            except Exception:
                return ""
        return ""

    def _record_execution_event(
        self,
        kind: str,
        sig: TradeSignal,
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

    def evaluate_from_bar(self, symbol: str, bar: Any) -> Optional[TradeSignal]:
        self._append_bar(symbol, bar)

        close = self._close[symbol]
        high = self._high[symbol]
        low = self._low[symbol]

        s_mr = mr_signal(close, self.cfg.mr)
        s_bo = bo_signal(high, low, close, self.cfg.bo)
        short_sig = 0.6 * s_mr + 0.4 * s_bo

        regime_state = evaluate_regime(close, self.cfg.mid)
        regime_state_v2 = to_regime_state_v2(regime_state)
        mid_scale = float(regime_state.scale)
        risk_on = bool(regime_state.risk_on)

        can_short = True
        if self.gate is not None and hasattr(self.gate, "can_trade_short"):
            can_short = bool(self.gate.can_trade_short())

        total = fuse(short_sig=short_sig, long_sig=0.0, mid_scale=mid_scale, can_trade_short=True)

        # -------- Phase2: pick entry channel (Pure-Short or Total) --------
        channel = "NONE"
        used_thr = float(self.cfg.trade_threshold)
        should_trade = False
        action = ""

        # 1) Pure-Short channel: short_sig-only
        if bool(getattr(self.cfg, "enable_pure_short", True)) and abs(float(short_sig)) >= float(self.cfg.short_threshold):
            channel = "PURE_SHORT"
            used_thr = float(self.cfg.short_threshold)
            should_trade = True
            action = "BUY" if float(short_sig) > 0 else "SELL"

        # 2) Total channel: existing total_sig threshold
        elif abs(float(total)) >= float(self.cfg.trade_threshold):
            channel = "TOTAL"
            used_thr = float(self.cfg.trade_threshold)
            should_trade = True
            action = "BUY" if float(total) > 0 else "SELL"

        # -------- Phase2: mid as risk modulator (soft) -> qty multiplier --------
        m = float(mid_scale)
        if m <= float(self.cfg.mid_soft_floor):
            qty_mult = float(self.cfg.mid_qty_min)
        else:
            # Conservative linear map to [min,max]
            t = self._clip((m - float(self.cfg.mid_soft_floor)) / 1.0, 0.0, 1.0)
            qty_mult = float(self.cfg.mid_qty_min) + t * (float(self.cfg.mid_qty_max) - float(self.cfg.mid_qty_min))

        qty = float(self.cfg.base_qty) * float(qty_mult)
        risk_action = action or ("SELL" if float(total) < 0 else "BUY")
        risk_snapshot = self.risk_model.build_snapshot(
            symbol=symbol,
            action=risk_action,
            entry_price=float(bar.close),
            highs=high,
            lows=low,
            closes=close,
            volumes=self._vol[symbol],
            can_short=can_short,
            event_risk=self._gate_event_risk(symbol),
            event_risk_reason=self._gate_event_risk_reason(symbol),
            short_borrow_fee_bps=self._gate_short_borrow_fee_bps(symbol),
            short_borrow_source=self._gate_short_borrow_source(symbol),
        )
        signal_decision = SignalDecision(
            symbol=symbol,
            market=str(getattr(self.cfg, "market", "") or ""),
            strategy="engine_strategy",
            long_score=0.0,
            short_score=float(short_sig),
            total_score=float(total),
            regime_state=regime_state_v2.to_dict(),
            gates_passed=["signal_threshold"] if should_trade else [],
            gates_blocked=[],
            action=action if should_trade else "",
            reasons=[str(regime_state.reason)],
            context={"channel": channel, "threshold_used": float(used_thr)},
        )
        risk_decision = RiskDecision(
            symbol=symbol,
            market=str(getattr(self.cfg, "market", "") or ""),
            allowed=bool(risk_snapshot.allowed),
            sizing_result={"base_qty": float(self.cfg.base_qty), "qty_mult": float(qty_mult), "requested_qty": float(qty)},
            block_reasons=list(getattr(risk_snapshot, "block_reasons", []) or []),
            reason_codes=list(getattr(risk_snapshot, "block_reasons", []) or []),
            context=risk_snapshot.to_dict(),
        )

        # ---- Phase1: signal audit persistence ----
        try:
            storage = getattr(self.orders, 'storage', None)
            if storage is not None and hasattr(storage, 'insert_signal_audit'):
                last3 = close[-3:] if len(close) >= 3 else close[:]
                window = 20
                if len(high) >= window and len(low) >= window:
                    hi = max(high[-window:])
                    lo = min(low[-window:])
                    range20 = (hi - lo) / float(close[-1]) if float(close[-1]) else 0.0
                else:
                    range20 = 0.0

                # Phase1-C(+): carry engine tag/source + phase2 channel into reason
                audit_reason = ""
                try:
                    audit_reason = f"{self._audit_tag}|{self._audit_source}|{channel}|thr={used_thr:.3f}|qmul={qty_mult:.2f}"
                except Exception:
                    audit_reason = ""

                storage.insert_signal_audit({
                    'symbol': symbol,
                    'bar_end_time': getattr(bar, 'end_time', None).isoformat() if getattr(bar, 'end_time', None) else '',
                    'o': float(bar.open), 'h': float(bar.high), 'l': float(bar.low), 'c': float(bar.close),
                    'v': float(getattr(bar, 'volume', 0.0) or 0.0),
                    'last3_close': json.dumps([float(x) for x in last3]),
                    'range20': float(range20),
                    'mr_sig': float(s_mr),
                    'bo_sig': float(s_bo),
                    'short_sig': float(short_sig),
                    'mid_scale': float(mid_scale),
                    'total_sig': float(total),

                    # Phase2 reflected fields
                    'threshold': float(used_thr),
                    'should_trade': 1 if should_trade else 0,
                    'action': action if should_trade else '',

                    'reason': audit_reason,

                    # keep your existing extra fields (Phase1-C)
                    'channel': channel,
                    'can_trade_short': 1 if can_short else 0,
                    'risk_gate': 'OK' if can_short else 'BLOCKED',
                    'atr_stop': float(risk_snapshot.atr_stop),
                    'slippage_bps': float(risk_snapshot.slippage_bps),
                    'gap_addon_pct': float(risk_snapshot.gap_addon_pct),
                    'liquidity_haircut': float(risk_snapshot.liquidity_haircut),
                    'event_risk': str(risk_snapshot.event_risk),
                    'event_risk_reason': str(risk_snapshot.event_risk_reason),
                    'short_borrow_fee_bps': float(risk_snapshot.short_borrow_fee_bps),
                    'short_borrow_source': str(risk_snapshot.short_borrow_source),
                    'risk_allowed': 1 if risk_snapshot.allowed else 0,
                    'block_reasons': json.dumps(risk_snapshot.block_reasons, ensure_ascii=False),
                    'risk_snapshot_json': json.dumps(risk_snapshot.to_dict(), ensure_ascii=False),
                    'regime_state_v2_json': json.dumps(regime_state_v2.to_dict(), ensure_ascii=False),
                    'signal_decision_json': json.dumps(signal_decision.to_dict(), ensure_ascii=False),
                    'risk_decision_json': json.dumps(risk_decision.to_dict(), ensure_ascii=False),
                })
        except Exception:
            # never block trading on audit writes
            pass

        # -------- decision output --------
        if not should_trade:
            return TradeSignal(
                should_trade=False,
                action="",
                qty=0.0,
                entry_price=float(bar.close),
                total_sig=float(total),
                short_sig=float(short_sig),
                mid_scale=float(mid_scale),
                risk_on=bool(risk_on),
                regime_state=str(regime_state.state),
                regime_reason=str(regime_state.reason),
                reason=f"{channel}|no_trigger|{regime_state.reason}",
                channel=channel,
                threshold_used=float(used_thr),
                audit_tag=str(getattr(self, "_audit_tag", "") or ""),
                audit_source=str(getattr(self, "_audit_source", "") or ""),
                risk_snapshot=risk_snapshot,
                regime_state_v2=regime_state_v2,
                signal_decision=signal_decision,
                risk_decision=risk_decision,
            )

        if qty <= 0:
            return TradeSignal(
                should_trade=False,
                action=action,
                qty=0.0,
                entry_price=float(bar.close),
                total_sig=float(total),
                short_sig=float(short_sig),
                mid_scale=float(mid_scale),
                risk_on=bool(risk_on),
                regime_state=str(regime_state.state),
                regime_reason=str(regime_state.reason),
                reason="qty<=0",
                channel=channel,
                threshold_used=float(used_thr),
                audit_tag=str(getattr(self, "_audit_tag", "") or ""),
                audit_source=str(getattr(self, "_audit_source", "") or ""),
                risk_snapshot=risk_snapshot,
                regime_state_v2=regime_state_v2,
                signal_decision=signal_decision,
                risk_decision=risk_decision,
            )

        return TradeSignal(
            should_trade=True,
            action=action,
            qty=float(qty),
            entry_price=float(bar.close),
            total_sig=float(total),
            short_sig=float(short_sig),
            mid_scale=float(mid_scale),
            risk_on=bool(risk_on),
            regime_state=str(regime_state.state),
            regime_reason=str(regime_state.reason),
            reason=f"{channel} total={total:.3f} short={short_sig:.3f} mid={mid_scale:.2f} thr={used_thr:.3f} qmul={qty_mult:.2f} regime={regime_state.state}",
            channel=channel,
            threshold_used=float(used_thr),
            audit_tag=str(getattr(self, "_audit_tag", "") or ""),
            audit_source=str(getattr(self, "_audit_source", "") or ""),
            risk_snapshot=risk_snapshot,
            regime_state_v2=regime_state_v2,
            signal_decision=signal_decision,
            risk_decision=risk_decision,
        )

    def execute(self, symbol: str, sig: TradeSignal, runner: Any) -> None:
        if not sig.should_trade:
            return

        runtime_mode = str(getattr(self.cfg, "runtime_mode", "") or "").strip().lower()
        allowed_sources = {str(x or "").upper() for x in list(getattr(self.cfg, "paper_allowed_execution_sources", []) or [])}
        sig_source = str(sig.audit_source or "").upper()
        source_exec_allowed = not (runtime_mode == "paper" and allowed_sources and sig_source not in allowed_sources)

        if bool(getattr(self.cfg, "enforce_pretrade_risk_gate", True)) and sig.risk_snapshot is not None and not bool(sig.risk_snapshot.allowed):
            self._record_execution_event(
                "PRETRADE_RISK_BLOCK",
                sig,
                symbol=symbol,
                value=float(getattr(sig.risk_snapshot, "risk_per_share", 0.0) or 0.0),
                details_suffix=f"reasons={','.join(getattr(sig.risk_snapshot, 'block_reasons', []) or [])}",
            )
            log.info(f"[{symbol}] entry blocked by pretrade risk gate: {getattr(sig.risk_snapshot, 'block_reasons', [])}")
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
                        log.info(f"[{symbol}] short safety shadow skipped for non-executable source: {sig_source}")
                    else:
                        log.info(f"[{symbol}] short safety shadow block recorded: {short_decision.blocked_reason_text()}")
                else:
                    log.info(f"[{symbol}] short blocked by safety gate: {short_decision.blocked_reason_text()}")
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
                log.info(f"[{symbol}] short reduced to zero by safety gate")
                return

        if not source_exec_allowed:
            self._record_execution_event(
                "SOURCE_EXEC_BLOCK",
                sig,
                symbol=symbol,
                details_suffix=f"allowed_sources={','.join(sorted(allowed_sources))}",
            )
            log.info(f"[{symbol}] entry blocked by source gate: source={sig_source} allowed={sorted(allowed_sources)}")
            return

        now = __import__("time").time()
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
                log.info(f"[{symbol}] entry blocked by guard: {reason}")
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
                log.info(f"[{symbol}] entry blocked by allocator: qty={qty}")
                return
            if not allowed:
                self._record_execution_event(
                    "ALLOCATOR_BLOCK",
                    sig,
                    symbol=symbol,
                    value=float(qty * float(sig.entry_price)),
                    details_suffix=f"reason={reason} qty={float(qty):.3f}",
                )
                log.info(f"[{symbol}] entry blocked by allocator: {reason} qty={qty}")
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
        log.info(f"[{symbol}] placing bracket: action={sig.action} qty={qty} entry={order_entry_price:.2f} reason={sig.reason}")
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
