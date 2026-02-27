# src/strategies/short_breakout.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List

@dataclass
class BOConfig:
    lookback: int = 48     # 48根5m≈4小时
    confirm: int = 1       # 连续确认根数（先简化）

def signal(high: List[float], low: List[float], close: List[float], cfg: BOConfig) -> float:
    if len(close) < cfg.lookback + cfg.confirm:
        return 0.0

    hh = max(high[-cfg.lookback - cfg.confirm : -cfg.confirm])
    ll = min(low[-cfg.lookback - cfg.confirm : -cfg.confirm])

    # 突破上轨：做多
    if close[-1] > hh:
        return 1.0
    # 跌破下轨：做空（同样可在 engine 里限制不做空）
    if close[-1] < ll:
        return -1.0
    return 0.0