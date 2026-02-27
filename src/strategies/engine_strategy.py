# src/strategies/engine_strategy.py
from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Dict, List, Optional, Any

from ib_insync import Stock  # type: ignore

from .short_mean_reversion import MRConfig, signal as mr_signal
from .short_breakout import BOConfig, signal as bo_signal
from .mid_regime import RegimeConfig, regime as mid_regime
from ..signals.fusion import fuse
from ..ibkr.orders import BracketParams


@dataclass
class StrategyConfig:
    trade_threshold: float = 0.65
    base_qty: float = 1.0
    take_profit_pct: float = 0.004
    stop_loss_pct: float = 0.006

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
    reason: str = ""


class EngineStrategy:
    """
    Adapter used by app.engine.TradingEngine:
    - keep rolling OHLCV per symbol
    - compute short_sig (MR+BO), mid_scale (regime), total_sig (fuse)
    - threshold -> TradeSignal
    - execute via OrderService.place_bracket
    """

    def __init__(self, *, orders: Any, gate: Any, cfg: Optional[StrategyConfig] = None, max_bars: int = 600):
        self.orders = orders
        self.gate = gate
        self.cfg = cfg or StrategyConfig()
        self.max_bars = int(max_bars)

        self._open: Dict[str, List[float]] = {}
        self._high: Dict[str, List[float]] = {}
        self._low: Dict[str, List[float]] = {}
        self._close: Dict[str, List[float]] = {}
        self._vol: Dict[str, List[float]] = {}

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

    def evaluate_from_bar(self, symbol: str, bar: Any) -> Optional[TradeSignal]:
        self._append_bar(symbol, bar)

        close = self._close[symbol]
        high = self._high[symbol]
        low = self._low[symbol]

        s_mr = mr_signal(close, self.cfg.mr)
        s_bo = bo_signal(high, low, close, self.cfg.bo)
        short_sig = 0.6 * s_mr + 0.4 * s_bo

        _risk_on, mid_scale = mid_regime(close, self.cfg.mid)

        can_short = True
        if self.gate is not None and hasattr(self.gate, "can_trade_short"):
            can_short = bool(self.gate.can_trade_short())

        total = fuse(short_sig=short_sig, long_sig=0.0, mid_scale=mid_scale, can_trade_short=can_short)

        thr = float(self.cfg.trade_threshold)

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
                    'threshold': float(self.cfg.trade_threshold),
                    'should_trade': 1 if abs(float(total)) >= float(self.cfg.trade_threshold) else 0,
                    'action': 'BUY' if float(total) > 0 else 'SELL' if float(total) < 0 else '',
                    'reason': ''
                })
        except Exception:
            # never block trading on audit writes
            pass

        if abs(total) < thr:
            return TradeSignal(
                should_trade=False,
                action="",
                qty=0.0,
                entry_price=float(bar.close),
                total_sig=float(total),
                short_sig=float(short_sig),
                mid_scale=float(mid_scale),
                reason=f"|total|<{thr}",
            )

        action = "BUY" if total > 0 else "SELL"
        if action == "SELL" and not can_short:
            return TradeSignal(
                should_trade=False,
                action="SELL",
                qty=0.0,
                entry_price=float(bar.close),
                total_sig=float(total),
                short_sig=float(short_sig),
                mid_scale=float(mid_scale),
                reason="short_blocked",
            )

        qty = float(self.cfg.base_qty)
        return TradeSignal(
            should_trade=True,
            action=action,
            qty=qty,
            entry_price=float(bar.close),
            total_sig=float(total),
            short_sig=float(short_sig),
            mid_scale=float(mid_scale),
            reason=f"total={total:.3f} short={short_sig:.3f} mid={mid_scale:.2f}",
        )

    def execute(self, symbol: str, sig: TradeSignal, runner: Any) -> None:
        if not sig.should_trade:
            return
        contract = Stock(symbol, "SMART", "USD")
        params = BracketParams(take_profit_pct=self.cfg.take_profit_pct, stop_loss_pct=self.cfg.stop_loss_pct)
        trades = self.orders.place_bracket(
            contract=contract,
            action=sig.action,
            qty=sig.qty,
            entry_price=sig.entry_price,
            params=params,
        )
        try:
            if hasattr(runner, "watch_entry_order") and trades:
                parent_trade = trades[0]
                oid = int(parent_trade.order.orderId)
                runner.watch_entry_order(oid, meta={"symbol": symbol, "reason": sig.reason})
        except Exception:
            pass