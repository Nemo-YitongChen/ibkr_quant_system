# src/strategies/mid_regime.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List

@dataclass
class RegimeConfig:
    ma_fast: int = 24   # 24根5m≈2小时
    ma_slow: int = 72   # 72根5m≈6小时
    vol_lookback: int = 72
    vol_max: float = 0.015   # 6小时收益波动阈值（占位）

def sma(xs: List[float], n: int) -> float:
    if len(xs) < n:
        return 0.0
    w = xs[-n:]
    return sum(w)/n

def stdev_ret(close: List[float], n: int) -> float:
    if len(close) < n+1:
        return 0.0
    rets = []
    for i in range(-n, 0):
        r = (close[i] / close[i-1]) - 1.0
        rets.append(r)
    m = sum(rets)/len(rets)
    v = sum((r-m)**2 for r in rets)/max(1, len(rets)-1)
    return v**0.5

def regime(close: List[float], cfg: RegimeConfig) -> tuple[bool, float]:
    """
    returns: (risk_on, scale in [0,1])
    """
    if len(close) < cfg.ma_slow + 2:
        return True, 0.5

    fast = sma(close, cfg.ma_fast)
    slow = sma(close, cfg.ma_slow)

    trend = 1.0 if fast > slow else 0.0
    vol = stdev_ret(close, cfg.vol_lookback)

    # 波动过大：降低短线规模
    if vol >= cfg.vol_max:
        return True, 0.25

    # 趋势不佳：短线也降档
    return True, 0.75 if trend > 0 else 0.4