from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any, Dict, Iterable, List, Sequence

from ..ibkr.market_data import OHLCVBar
from ..strategies.mid_regime import RegimeConfig, evaluate_regime
from .investment import InvestmentScoringConfig, score_investment_candidate


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _max_drawdown(xs: Sequence[float]) -> float:
    if not xs:
        return 0.0
    peak = float(xs[0])
    mdd = 0.0
    for value in xs:
        peak = max(peak, float(value))
        if peak > 0:
            mdd = min(mdd, (float(value) - peak) / peak)
    return float(mdd)


def _trend_slope(closes: Sequence[float]) -> float:
    if len(closes) < 30:
        return 0.0
    start = float(closes[0])
    end = float(closes[-1])
    if start <= 0:
        return 0.0
    return float((end - start) / start)


@dataclass
class InvestmentBacktestConfig:
    holding_periods_days: tuple[int, ...] = (30, 60, 90)
    sample_step_days: int = 5
    min_trade_samples: int = 3
    min_history_bars: int = 320
    mid_lookback_days: int = 180
    long_drawdown_window: int = 252

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentBacktestConfig":
        raw = dict(raw or {})
        if "holding_periods_days" in raw and isinstance(raw["holding_periods_days"], Iterable):
            raw["holding_periods_days"] = tuple(int(x) for x in raw["holding_periods_days"])
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def compute_investment_backtest_from_bars(
    symbol: str,
    bars: Sequence[OHLCVBar],
    *,
    scoring_cfg: InvestmentScoringConfig | None = None,
    regime_cfg: RegimeConfig | None = None,
    cfg: InvestmentBacktestConfig | None = None,
) -> Dict[str, Any]:
    cfg = cfg or InvestmentBacktestConfig()
    scoring_cfg = scoring_cfg or InvestmentScoringConfig()
    regime_cfg = regime_cfg or RegimeConfig()
    bars = list(bars or [])
    horizons = tuple(sorted(max(1, int(x)) for x in cfg.holding_periods_days))
    max_horizon = max(horizons) if horizons else 0

    closes = [float(b.close) for b in bars if getattr(b, "close", None) is not None]
    if len(closes) < max(int(cfg.min_history_bars), 200 + max_horizon + 1):
        return {"symbol": str(symbol).upper(), "bt_signal_samples": 0}

    min_idx = max(200, int(cfg.mid_lookback_days), int(cfg.long_drawdown_window))
    sample_step = max(1, int(cfg.sample_step_days))
    forward_returns: Dict[int, List[float]] = {h: [] for h in horizons}

    for idx in range(min_idx, len(closes) - max_horizon, sample_step):
        history = closes[: idx + 1]
        last = float(history[-1])
        ma200 = sum(history[-200:]) / 200.0 if len(history) >= 200 else 0.0
        trend_vs_ma200 = (last - ma200) / ma200 if ma200 > 0 else 0.0
        drawdown = _max_drawdown(history[-int(cfg.long_drawdown_window) :])
        long_row = {
            "symbol": str(symbol).upper(),
            "long_score": float(trend_vs_ma200 + drawdown),
            "trend_vs_ma200": float(trend_vs_ma200),
            "mdd_1y": float(drawdown),
            "last_close": float(last),
            "rebalance_flag": 1 if trend_vs_ma200 < -0.08 or drawdown < -0.25 else 0,
        }

        mid_window = history[-int(cfg.mid_lookback_days) :]
        regime_state = evaluate_regime(list(mid_window), regime_cfg)
        mid_row = {
            "symbol": str(symbol).upper(),
            "mid_scale": float(regime_state.scale),
            "risk_on": bool(regime_state.risk_on),
            "regime_state": str(regime_state.state),
            "regime_reason": str(regime_state.reason),
            "regime_composite": float(regime_state.composite),
            "trend_slope_60d": float(_trend_slope(history[-60:])),
            "last_close": float(last),
        }

        snapshot = score_investment_candidate(
            long_row,
            mid_row,
            vix=0.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            cfg=scoring_cfg,
        )
        if str(snapshot.get("action", "WATCH")).upper() not in {"ACCUMULATE", "HOLD"}:
            continue

        for horizon in horizons:
            future_close = float(closes[idx + horizon])
            if last <= 0:
                continue
            forward_returns[horizon].append((future_close / last) - 1.0)

    samples = max((len(vals) for vals in forward_returns.values()), default=0)
    result: Dict[str, Any] = {
        "symbol": str(symbol).upper(),
        "bt_signal_samples": int(samples),
        "bt_min_samples_ok": int(samples >= int(cfg.min_trade_samples)),
    }
    for horizon in horizons:
        vals = list(forward_returns.get(horizon, []))
        hit_rate = float(sum(1 for v in vals if v > 0.0) / len(vals)) if vals else 0.0
        result[f"bt_samples_{horizon}d"] = int(len(vals))
        result[f"bt_avg_ret_{horizon}d"] = float(_mean(vals))
        result[f"bt_median_ret_{horizon}d"] = float(median(vals) if vals else 0.0)
        result[f"bt_hit_rate_{horizon}d"] = float(hit_rate)
    return result
