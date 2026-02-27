# src/strategies/short_mean_reversion.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List

import math

@dataclass
class MRConfig:
    lookback: int = 60          # 60根5m≈5小时
    entry_z: float = 1.2
    exit_z: float = 0.3

def mean(xs: List[float]) -> float:
    return sum(xs) / max(1, len(xs))

def std(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(max(v, 0.0))

def signal(close: List[float], cfg: MRConfig) -> float:
    if len(close) < cfg.lookback:
        return 0.0
    window = close[-cfg.lookback:]
    m = mean(window)
    s = std(window)
    if s == 0:
        return 0.0
    z = (close[-1] - m) / s

    # 价格高于均值很多：倾向做空（这里先只做多/不做空可在 engine 里限制）
    if z >= cfg.entry_z:
        return -1.0
    if z <= -cfg.entry_z:
        return 1.0
    if abs(z) <= cfg.exit_z:
        return 0.0

    # 中间区间：线性缩放
    return max(-1.0, min(1.0, -z / (cfg.entry_z * 1.5)))