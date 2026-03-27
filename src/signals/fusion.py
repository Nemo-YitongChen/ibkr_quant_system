# src/signals/fusion.py
from __future__ import annotations

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def fuse(short_sig: float, long_sig: float, mid_scale: float, can_trade_short: bool) -> float:
    """
    输出 total_sig ∈ [-1, 1]
    设计：
    - 纯短线：short_sig 是主驱动
    - 中期：mid_scale 做过滤与调权（不是硬性必须同向）
    """

    s = clamp(short_sig, -1.0, 1.0)
    m = clamp(mid_scale, 0.0, 1.0)

    # Short bans should only suppress short exposure, not long exposure.
    if not can_trade_short and s < 0.0:
        s = 0.0

    # 中期过滤：m 太差时，短线“追涨型信号”不让做（保留反转/均值回归的机会）
    # 这里假设 short_sig > 0 表示做多倾向；若你未来做空，逻辑可对称扩展。
    if m < 0.25 and s > 0.6:
        return 0.0  # 强烈逆风不追

    # 调权：中期越强，短线权重越大（允许更积极）
    w_short = 0.85 + 0.10 * (m - 0.5)   # 0.80 ~ 0.90
    w_long  = 0.10                      # 长线暂时占位
    w_bias  = 0.05 * (m - 0.5)          # 中期给一点偏置（弱则略降）

    total = w_short * s + w_long * clamp(long_sig, -1.0, 1.0) + w_bias
    return clamp(total, -1.0, 1.0)
