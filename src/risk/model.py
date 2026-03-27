from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Sequence


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _avg(xs: Sequence[float]) -> float:
    if not xs:
        return 0.0
    return sum(float(x) for x in xs) / float(len(xs))


def _atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], n: int) -> float:
    if len(closes) < max(2, n + 1) or len(highs) != len(closes) or len(lows) != len(closes):
        return 0.0
    trs: List[float] = []
    for i in range(1, len(closes)):
        hi = float(highs[i])
        lo = float(lows[i])
        prev = float(closes[i - 1])
        trs.append(max(hi - lo, abs(hi - prev), abs(lo - prev)))
    if not trs:
        return 0.0
    return _avg(trs[-int(max(1, n)):])


@dataclass
class TradeRiskConfig:
    atr_window: int = 14
    atr_stop_mult: float = 1.2
    min_stop_loss_pct: float = 0.006
    slippage_floor_bps: float = 4.0
    slippage_atr_weight: float = 0.35
    slippage_liquidity_weight: float = 12.0
    gap_floor_pct: float = 0.002
    gap_atr_mult: float = 0.35
    short_gap_extra_mult: float = 1.25
    liquidity_window: int = 20
    liquidity_target_bar_volume: float = 50_000.0
    min_avg_bar_volume: float = 5_000.0
    liquidity_haircut_max: float = 0.75
    liquidity_block_threshold: float = 0.90
    liquidity_stop_addon_max_pct: float = 0.003
    short_borrow_stop_addon_mult: float = 0.25
    max_short_borrow_addon_pct: float = 0.004
    take_profit_risk_mult: float = 1.8
    max_short_borrow_fee_bps: float = 150.0
    blocked_event_risks: List[str] = field(default_factory=lambda: ["HIGH", "BLOCK"])

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "TradeRiskConfig":
        raw = raw or {}
        out = {}
        for key in cls.__dataclass_fields__:
            if key not in raw:
                continue
            if key == "blocked_event_risks":
                out[key] = [str(x).upper() for x in raw[key]]
            else:
                out[key] = raw[key]
        return cls(**out)


@dataclass
class PreTradeRiskSnapshot:
    symbol: str
    action: str
    entry_price: float
    atr_stop: float
    slippage_bps: float
    gap_addon_pct: float
    liquidity_haircut: float
    slippage_addon_price: float
    gap_addon_price: float
    liquidity_addon_price: float
    short_addon_price: float
    stop_distance: float
    take_profit_distance: float
    stop_price: float
    take_profit_price: float
    event_risk: str
    event_risk_reason: str
    short_borrow_fee_bps: float
    short_borrow_source: str
    allowed: bool
    block_reasons: List[str] = field(default_factory=list)
    atr_pct: float = 0.0
    avg_bar_volume: float = 0.0
    risk_per_share: float = 0.0
    expected_fill_price: float = 0.0
    model_version: str = "trade_risk_v2"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TradeRiskModel:
    def __init__(self, cfg: TradeRiskConfig | None = None):
        self.cfg = cfg or TradeRiskConfig()

    def build_snapshot(
        self,
        *,
        symbol: str,
        action: str,
        entry_price: float,
        highs: Sequence[float],
        lows: Sequence[float],
        closes: Sequence[float],
        volumes: Sequence[float],
        can_short: bool = True,
        event_risk: str = "NONE",
        event_risk_reason: str = "",
        short_borrow_fee_bps: float = 0.0,
        short_borrow_source: str = "",
    ) -> PreTradeRiskSnapshot:
        action = str(action or "BUY").upper()
        event_risk = str(event_risk or "NONE").upper()
        event_risk_reason = str(event_risk_reason or "")
        entry_price = float(entry_price or 0.0)

        atr = _atr(highs, lows, closes, int(self.cfg.atr_window))
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.0
        atr_stop = max(
            entry_price * float(self.cfg.min_stop_loss_pct),
            atr * float(self.cfg.atr_stop_mult),
        )

        vol_window = int(max(1, self.cfg.liquidity_window))
        avg_bar_volume = _avg([float(x) for x in volumes[-vol_window:]])
        target_bar_volume = max(1.0, float(self.cfg.liquidity_target_bar_volume))
        liquidity_ratio = _clip(avg_bar_volume / target_bar_volume, 0.0, 1.0)
        liquidity_haircut = _clip(
            1.0 - liquidity_ratio,
            0.0,
            float(self.cfg.liquidity_haircut_max),
        )

        slippage_bps = float(self.cfg.slippage_floor_bps)
        slippage_bps += atr_pct * 10_000.0 * float(self.cfg.slippage_atr_weight)
        slippage_bps += liquidity_haircut * float(self.cfg.slippage_liquidity_weight)
        if action == "SELL" and short_borrow_fee_bps > 0:
            slippage_bps += min(10.0, float(short_borrow_fee_bps) * 0.05)

        gap_addon_pct = max(
            float(self.cfg.gap_floor_pct),
            atr_pct * float(self.cfg.gap_atr_mult),
        )
        if action == "SELL":
            gap_addon_pct *= float(self.cfg.short_gap_extra_mult)

        slippage_addon_price = entry_price * (float(slippage_bps) / 10_000.0)
        gap_addon_price = entry_price * float(gap_addon_pct)
        liquidity_addon_price = entry_price * float(liquidity_haircut) * float(self.cfg.liquidity_stop_addon_max_pct)
        short_addon_pct = 0.0
        if action == "SELL" and float(short_borrow_fee_bps) > 0:
            short_addon_pct = min(
                float(self.cfg.max_short_borrow_addon_pct),
                (float(short_borrow_fee_bps) / 10_000.0) * float(self.cfg.short_borrow_stop_addon_mult),
            )
        short_addon_price = entry_price * float(short_addon_pct)

        block_reasons: List[str] = []
        if action == "SELL" and not can_short:
            block_reasons.append("short_gate_blocked")
        if event_risk in {str(x).upper() for x in self.cfg.blocked_event_risks}:
            block_reasons.append(f"event_risk:{event_risk.lower()}")
        if avg_bar_volume < float(self.cfg.min_avg_bar_volume):
            block_reasons.append("liquidity_too_thin")
        if liquidity_haircut >= float(self.cfg.liquidity_block_threshold):
            block_reasons.append("liquidity_haircut_block")
        if action == "SELL" and float(short_borrow_fee_bps) > float(self.cfg.max_short_borrow_fee_bps):
            block_reasons.append("borrow_fee_too_high")

        stop_distance = float(atr_stop) + float(gap_addon_price) + float(slippage_addon_price) + float(liquidity_addon_price) + float(short_addon_price)
        take_profit_distance = max(float(atr_stop), float(stop_distance) * float(self.cfg.take_profit_risk_mult))
        if action == "BUY":
            stop_price = max(0.0, entry_price - stop_distance)
            take_profit_price = entry_price + take_profit_distance
            expected_fill_price = entry_price + slippage_addon_price
        else:
            stop_price = entry_price + stop_distance
            take_profit_price = max(0.0, entry_price - take_profit_distance)
            expected_fill_price = entry_price - slippage_addon_price

        risk_per_share = float(stop_distance)

        return PreTradeRiskSnapshot(
            symbol=str(symbol).upper(),
            action=action,
            entry_price=entry_price,
            atr_stop=float(atr_stop),
            slippage_bps=float(slippage_bps),
            gap_addon_pct=float(gap_addon_pct),
            liquidity_haircut=float(liquidity_haircut),
            slippage_addon_price=float(slippage_addon_price),
            gap_addon_price=float(gap_addon_price),
            liquidity_addon_price=float(liquidity_addon_price),
            short_addon_price=float(short_addon_price),
            stop_distance=float(stop_distance),
            take_profit_distance=float(take_profit_distance),
            stop_price=float(stop_price),
            take_profit_price=float(take_profit_price),
            event_risk=event_risk,
            event_risk_reason=event_risk_reason,
            short_borrow_fee_bps=float(short_borrow_fee_bps),
            short_borrow_source=str(short_borrow_source or ""),
            allowed=not block_reasons,
            block_reasons=block_reasons,
            atr_pct=float(atr_pct),
            avg_bar_volume=float(avg_bar_volume),
            risk_per_share=float(risk_per_share),
            expected_fill_price=float(expected_fill_price),
        )


def execution_slippage_bps(action: str, reference_price: float, actual_price: float) -> float:
    reference_price = float(reference_price or 0.0)
    actual_price = float(actual_price or 0.0)
    if reference_price <= 0:
        return 0.0
    action = str(action or "").upper()
    if action == "BOT":
        action = "BUY"
    elif action == "SLD":
        action = "SELL"
    raw = ((actual_price - reference_price) / reference_price) * 10_000.0
    return float(raw if str(action or "").upper() == "BUY" else -raw)
