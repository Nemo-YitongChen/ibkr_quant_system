from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..common.logger import get_logger
from ..enrichment.yfinance_history import fetch_daily_bars as fetch_daily_bars_yf
from ..ibkr.contracts import make_stock_contract
from .mid_regime import RegimeConfig

log = get_logger("strategy.regime_adaptor")


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _pct(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return (b / a) - 1.0


def _sma(xs: list[float], n: int) -> float:
    if n <= 0 or len(xs) < n:
        return 0.0
    window = xs[-n:]
    return sum(window) / float(n)


def _atr_pct(highs: list[float], lows: list[float], closes: list[float], n: int = 14) -> float:
    if len(closes) < n + 1:
        return 0.0
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    atr = sum(trs[-n:]) / max(1, min(len(trs), n))
    last = float(closes[-1]) if closes else 0.0
    return atr / last if last > 0 else 0.0


@dataclass
class RegimeAdaptConfig:
    enabled: bool = True
    refresh_sec: int = 900
    benchmark_symbol: str = "SPY"
    lookback_days: int = 220
    fast_ma: int = 20
    slow_ma: int = 60
    momentum_lookback: int = 20
    drawdown_lookback: int = 60
    atr_window: int = 14
    bull_atr_pct_max: float = 0.018
    riskoff_atr_pct_min: float = 0.028
    riskoff_drawdown_min: float = -0.08
    riskoff_momentum_min: float = -0.05
    riskon_momentum_min: float = 0.03
    riskon_drawdown_min: float = -0.03
    risk_on_threshold_shift_bull: float = -0.05
    risk_on_threshold_shift_neutral: float = 0.0
    risk_on_threshold_shift_riskoff: float = 0.08
    hard_risk_off_threshold_shift_riskoff: float = 0.10
    scale_floor_mult_bull: float = 1.00
    scale_floor_mult_neutral: float = 0.90
    scale_floor_mult_riskoff: float = 0.75
    scale_neutral_mult_bull: float = 1.08
    scale_neutral_mult_neutral: float = 1.00
    scale_neutral_mult_riskoff: float = 0.80
    scale_bull_mult_bull: float = 1.10
    scale_bull_mult_neutral: float = 0.95
    scale_bull_mult_riskoff: float = 0.70
    scale_bear_mult_bull: float = 1.00
    scale_bear_mult_neutral: float = 0.95
    scale_bear_mult_riskoff: float = 0.70
    vol_weight_mult_bull: float = 0.90
    vol_weight_mult_neutral: float = 1.00
    vol_weight_mult_riskoff: float = 1.20
    drawdown_weight_mult_bull: float = 0.95
    drawdown_weight_mult_neutral: float = 1.00
    drawdown_weight_mult_riskoff: float = 1.25

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "RegimeAdaptConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


@dataclass
class RegimeMarketSnapshot:
    market: str
    benchmark_symbol: str
    as_of: str
    last_close: float
    fast_ma: float
    slow_ma: float
    momentum: float
    atr_pct: float
    drawdown: float
    state: str
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RegimeAdaptor:
    def __init__(self, market: str, base_cfg: RegimeConfig, adapt_cfg: RegimeAdaptConfig):
        self.market = str(market or "DEFAULT").upper()
        self.base_cfg = base_cfg
        self.adapt_cfg = adapt_cfg
        self._last_refresh_monotonic: float = 0.0
        self._adapted_cfg: RegimeConfig = base_cfg
        self._snapshot: Optional[RegimeMarketSnapshot] = None

    @property
    def adapted_cfg(self) -> RegimeConfig:
        return self._adapted_cfg

    @property
    def snapshot(self) -> Optional[RegimeMarketSnapshot]:
        return self._snapshot

    def refresh_if_due(self, md: Any, storage: Any = None, force: bool = False) -> RegimeConfig:
        if not self.adapt_cfg.enabled:
            self._adapted_cfg = self.base_cfg
            return self._adapted_cfg

        now_m = __import__("time").monotonic()
        if not force and self._last_refresh_monotonic > 0 and now_m - self._last_refresh_monotonic < float(self.adapt_cfg.refresh_sec):
            return self._adapted_cfg

        self._last_refresh_monotonic = now_m
        try:
            snapshot = self._build_snapshot(md)
            self._snapshot = snapshot
            self._adapted_cfg = self._adapt(snapshot)
            log.info(
                "[%s] regime adaptor state=%s benchmark=%s reason=%s",
                self.market,
                snapshot.state,
                snapshot.benchmark_symbol,
                snapshot.reason,
            )
            if storage is not None and hasattr(storage, "upsert_regime_state"):
                storage.upsert_regime_state(
                    market=self.market,
                    regime_state=snapshot.state,
                    snapshot=snapshot.to_dict(),
                    adapted_cfg=asdict(self._adapted_cfg),
                )
        except Exception as e:
            log.warning("[%s] regime adaptor refresh failed: %s %s", self.market, type(e).__name__, e)
            self._adapted_cfg = self.base_cfg
        return self._adapted_cfg

    def _build_snapshot(self, md: Any) -> RegimeMarketSnapshot:
        symbol = str(self.adapt_cfg.benchmark_symbol).upper()
        try:
            md.register(symbol, make_stock_contract(symbol))
        except Exception:
            pass
        try:
            bars = md.get_daily_bars(symbol, days=int(self.adapt_cfg.lookback_days))
        except Exception:
            bars = []
        if not bars:
            bars = fetch_daily_bars_yf(symbol, days=int(self.adapt_cfg.lookback_days))
        if not bars or len(bars) < max(self.adapt_cfg.slow_ma, self.adapt_cfg.drawdown_lookback, self.adapt_cfg.momentum_lookback) + 2:
            raise ValueError(f"insufficient benchmark bars for {symbol}")

        closes = [float(b.close) for b in bars if getattr(b, "close", None) is not None]
        highs = [float(b.high) for b in bars if getattr(b, "high", None) is not None]
        lows = [float(b.low) for b in bars if getattr(b, "low", None) is not None]
        if len(closes) < max(self.adapt_cfg.slow_ma, self.adapt_cfg.drawdown_lookback, self.adapt_cfg.momentum_lookback) + 2:
            raise ValueError(f"filtered benchmark bars insufficient for {symbol}")

        last = float(closes[-1])
        fast_ma = _sma(closes, int(self.adapt_cfg.fast_ma))
        slow_ma = _sma(closes, int(self.adapt_cfg.slow_ma))
        momentum = _pct(float(closes[-(int(self.adapt_cfg.momentum_lookback) + 1)]), last)
        peak = max(float(x) for x in closes[-int(self.adapt_cfg.drawdown_lookback):])
        drawdown = _pct(peak, last)
        atr_pct = _atr_pct(highs, lows, closes, int(self.adapt_cfg.atr_window))

        if (
            last >= slow_ma
            and fast_ma >= slow_ma
            and momentum >= float(self.adapt_cfg.riskon_momentum_min)
            and atr_pct <= float(self.adapt_cfg.bull_atr_pct_max)
            and drawdown >= float(self.adapt_cfg.riskon_drawdown_min)
        ):
            state = "RISK_ON"
        elif (
            last < slow_ma
            or momentum <= float(self.adapt_cfg.riskoff_momentum_min)
            or atr_pct >= float(self.adapt_cfg.riskoff_atr_pct_min)
            or drawdown <= float(self.adapt_cfg.riskoff_drawdown_min)
        ):
            state = "RISK_OFF"
        else:
            state = "NEUTRAL"

        reason = (
            f"{state} last={last:.2f} fast_ma={fast_ma:.2f} slow_ma={slow_ma:.2f} "
            f"mom={momentum:.3f} atr_pct={atr_pct:.3f} dd={drawdown:.3f}"
        )
        return RegimeMarketSnapshot(
            market=self.market,
            benchmark_symbol=symbol,
            as_of=datetime.now(timezone.utc).isoformat(),
            last_close=last,
            fast_ma=fast_ma,
            slow_ma=slow_ma,
            momentum=momentum,
            atr_pct=atr_pct,
            drawdown=drawdown,
            state=state,
            reason=reason,
        )

    def _adapt(self, snapshot: RegimeMarketSnapshot) -> RegimeConfig:
        base = self.base_cfg
        adapted = RegimeConfig(**asdict(base))
        state = snapshot.state

        if state == "RISK_ON":
            rot = {
                "risk_shift": float(self.adapt_cfg.risk_on_threshold_shift_bull),
                "hard_shift": 0.0,
                "floor_mult": float(self.adapt_cfg.scale_floor_mult_bull),
                "neutral_mult": float(self.adapt_cfg.scale_neutral_mult_bull),
                "bull_mult": float(self.adapt_cfg.scale_bull_mult_bull),
                "bear_mult": float(self.adapt_cfg.scale_bear_mult_bull),
                "vol_mult": float(self.adapt_cfg.vol_weight_mult_bull),
                "dd_mult": float(self.adapt_cfg.drawdown_weight_mult_bull),
            }
        elif state == "RISK_OFF":
            rot = {
                "risk_shift": float(self.adapt_cfg.risk_on_threshold_shift_riskoff),
                "hard_shift": float(self.adapt_cfg.hard_risk_off_threshold_shift_riskoff),
                "floor_mult": float(self.adapt_cfg.scale_floor_mult_riskoff),
                "neutral_mult": float(self.adapt_cfg.scale_neutral_mult_riskoff),
                "bull_mult": float(self.adapt_cfg.scale_bull_mult_riskoff),
                "bear_mult": float(self.adapt_cfg.scale_bear_mult_riskoff),
                "vol_mult": float(self.adapt_cfg.vol_weight_mult_riskoff),
                "dd_mult": float(self.adapt_cfg.drawdown_weight_mult_riskoff),
            }
        else:
            rot = {
                "risk_shift": float(self.adapt_cfg.risk_on_threshold_shift_neutral),
                "hard_shift": 0.0,
                "floor_mult": float(self.adapt_cfg.scale_floor_mult_neutral),
                "neutral_mult": float(self.adapt_cfg.scale_neutral_mult_neutral),
                "bull_mult": float(self.adapt_cfg.scale_bull_mult_neutral),
                "bear_mult": float(self.adapt_cfg.scale_bear_mult_neutral),
                "vol_mult": float(self.adapt_cfg.vol_weight_mult_neutral),
                "dd_mult": float(self.adapt_cfg.drawdown_weight_mult_neutral),
            }

        adapted.risk_on_threshold = _clip(float(base.risk_on_threshold) + rot["risk_shift"], 0.25, 0.85)
        adapted.hard_risk_off_threshold = _clip(float(base.hard_risk_off_threshold) + rot["hard_shift"], 0.05, adapted.risk_on_threshold - 0.05)
        adapted.scale_floor = _clip(float(base.scale_floor) * rot["floor_mult"], 0.05, 0.50)
        adapted.scale_neutral = _clip(float(base.scale_neutral) * rot["neutral_mult"], adapted.scale_floor, 0.95)
        adapted.scale_bull = _clip(float(base.scale_bull) * rot["bull_mult"], adapted.scale_neutral, 1.00)
        adapted.scale_bear = _clip(float(base.scale_bear) * rot["bear_mult"], adapted.scale_floor, adapted.scale_neutral)
        adapted.volatility_weight = _clip(float(base.volatility_weight) * rot["vol_mult"], 0.05, 0.50)
        adapted.drawdown_weight = _clip(float(base.drawdown_weight) * rot["dd_mult"], 0.05, 0.50)

        leftover = max(0.01, 1.0 - adapted.volatility_weight - adapted.drawdown_weight)
        base_remaining = max(1e-9, float(base.trend_weight) + float(base.momentum_weight))
        adapted.trend_weight = leftover * float(base.trend_weight) / base_remaining
        adapted.momentum_weight = leftover * float(base.momentum_weight) / base_remaining
        return adapted
