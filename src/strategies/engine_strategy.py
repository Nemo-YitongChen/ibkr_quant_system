from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from ..common.logger import get_logger
from ..common.signal_audit import SignalAuditContext, SignalAuditWriter
from ..events.models import RiskDecision, SignalDecision
from ..risk.model import PreTradeRiskSnapshot, TradeRiskConfig, TradeRiskModel
from ..regime.state import RegimeStateV2
from ..risk.short_safety import ShortSafetyGate
from .short_mean_reversion import MRConfig, signal as mr_signal
from .short_breakout import BOConfig, signal as bo_signal
from .mid_regime import RegimeConfig, evaluate_regime, to_regime_state_v2
from ..signals.fusion import fuse

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
    - keep a compatibility execute() shim while execution lives in SignalExecutor
    """

    def __init__(
        self,
        *,
        orders: Any = None,
        gate: Any = None,
        cfg: Optional[StrategyConfig] = None,
        max_bars: int = 600,
        entry_guard: Any = None,
        allocator: Any = None,
        short_safety_gate: Optional[ShortSafetyGate] = None,
        executor: Any = None,
        audit_writer: Any = None,
    ):
        self.orders = orders
        self.gate = gate
        self.cfg = cfg or StrategyConfig()
        self.max_bars = int(max_bars)
        self.entry_guard = entry_guard
        self.allocator = allocator
        self.short_safety_gate = short_safety_gate
        self.executor = executor
        self.audit_writer = audit_writer
        self.risk_model = TradeRiskModel(self.cfg.risk)

        storage = getattr(self.orders, "storage", None)
        if self.audit_writer is None and storage is not None:
            self.audit_writer = SignalAuditWriter(storage)
        self.storage = getattr(self.audit_writer, "storage", None) or storage

        if self.executor is None and self.orders is not None:
            try:
                from ..app.signal_executor import SignalExecutor
            except ImportError:
                from app.signal_executor import SignalExecutor
            self.executor = SignalExecutor(
                orders=self.orders,
                cfg=self.cfg,
                entry_guard=self.entry_guard,
                allocator=self.allocator,
                short_safety_gate=self.short_safety_gate,
            )

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

        if self.audit_writer is not None:
            self.audit_writer.write(
                SignalAuditContext(
                    symbol=symbol,
                    bar=bar,
                    closes=close,
                    highs=high,
                    lows=low,
                    mr_sig=float(s_mr),
                    bo_sig=float(s_bo),
                    short_sig=float(short_sig),
                    mid_scale=float(mid_scale),
                    total_sig=float(total),
                    threshold_used=float(used_thr),
                    should_trade=bool(should_trade),
                    action=action,
                    channel=channel,
                    qty_multiplier=float(qty_mult),
                    can_trade_short=bool(can_short),
                    risk_snapshot=risk_snapshot,
                    regime_state_v2=regime_state_v2,
                    signal_decision=signal_decision,
                    risk_decision=risk_decision,
                    audit_tag=str(getattr(self, "_audit_tag", "") or ""),
                    audit_source=str(getattr(self, "_audit_source", "") or ""),
                )
            )

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
        if self.executor is None:
            log.info("[%s] no executor configured; skip trade execution", symbol)
            return
        self.executor.execute(symbol, sig, runner)
