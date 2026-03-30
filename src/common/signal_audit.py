from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Sequence


@dataclass
class SignalAuditContext:
    symbol: str
    bar: Any
    closes: Sequence[float]
    highs: Sequence[float]
    lows: Sequence[float]
    mr_sig: float
    bo_sig: float
    short_sig: float
    mid_scale: float
    total_sig: float
    threshold_used: float
    should_trade: bool
    action: str
    channel: str
    qty_multiplier: float
    can_trade_short: bool
    risk_snapshot: Any
    regime_state_v2: Any
    signal_decision: Any
    risk_decision: Any
    audit_tag: str = ""
    audit_source: str = ""


class SignalAuditWriter:
    def __init__(self, storage: Any) -> None:
        self.storage = storage

    @staticmethod
    def _range20(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], *, window: int = 20) -> float:
        if len(highs) < window or len(lows) < window or not closes:
            return 0.0
        hi = max(float(x) for x in highs[-window:])
        lo = min(float(x) for x in lows[-window:])
        last_close = float(closes[-1])
        return (hi - lo) / last_close if last_close else 0.0

    @staticmethod
    def _audit_reason(audit_tag: str, audit_source: str, channel: str, threshold_used: float, qty_multiplier: float) -> str:
        return f"{audit_tag}|{audit_source}|{channel}|thr={threshold_used:.3f}|qmul={qty_multiplier:.2f}"

    def write(self, context: SignalAuditContext) -> None:
        try:
            if self.storage is None or not hasattr(self.storage, "insert_signal_audit"):
                return
            closes = [float(x) for x in list(context.closes or [])]
            last3 = closes[-3:] if len(closes) >= 3 else closes[:]
            risk_snapshot = context.risk_snapshot
            row = {
                "symbol": context.symbol,
                "bar_end_time": getattr(context.bar, "end_time", None).isoformat() if getattr(context.bar, "end_time", None) else "",
                "o": float(context.bar.open),
                "h": float(context.bar.high),
                "l": float(context.bar.low),
                "c": float(context.bar.close),
                "v": float(getattr(context.bar, "volume", 0.0) or 0.0),
                "last3_close": json.dumps(last3),
                "range20": float(self._range20(context.highs, context.lows, closes)),
                "mr_sig": float(context.mr_sig),
                "bo_sig": float(context.bo_sig),
                "short_sig": float(context.short_sig),
                "mid_scale": float(context.mid_scale),
                "total_sig": float(context.total_sig),
                "threshold": float(context.threshold_used),
                "should_trade": 1 if context.should_trade else 0,
                "action": context.action if context.should_trade else "",
                "reason": self._audit_reason(
                    str(context.audit_tag or ""),
                    str(context.audit_source or ""),
                    str(context.channel or ""),
                    float(context.threshold_used),
                    float(context.qty_multiplier),
                ),
                "channel": str(context.channel or ""),
                "can_trade_short": 1 if context.can_trade_short else 0,
                "risk_gate": "OK" if context.can_trade_short else "BLOCKED",
                "atr_stop": float(risk_snapshot.atr_stop),
                "slippage_bps": float(risk_snapshot.slippage_bps),
                "gap_addon_pct": float(risk_snapshot.gap_addon_pct),
                "liquidity_haircut": float(risk_snapshot.liquidity_haircut),
                "event_risk": str(risk_snapshot.event_risk),
                "event_risk_reason": str(risk_snapshot.event_risk_reason),
                "short_borrow_fee_bps": float(risk_snapshot.short_borrow_fee_bps),
                "short_borrow_source": str(risk_snapshot.short_borrow_source),
                "risk_allowed": 1 if risk_snapshot.allowed else 0,
                "block_reasons": json.dumps(risk_snapshot.block_reasons, ensure_ascii=False),
                "risk_snapshot_json": json.dumps(risk_snapshot.to_dict(), ensure_ascii=False),
                "regime_state_v2_json": json.dumps(context.regime_state_v2.to_dict(), ensure_ascii=False),
                "signal_decision_json": json.dumps(context.signal_decision.to_dict(), ensure_ascii=False),
                "risk_decision_json": json.dumps(context.risk_decision.to_dict(), ensure_ascii=False),
            }
            self.storage.insert_signal_audit(row)
        except Exception:
            pass
