# src/risk/ledger.py
from dataclasses import dataclass
from typing import Dict

@dataclass
class PositionState:
    qty: float = 0.0
    avg_price: float = 0.0

class Ledger:
    """
    简化持仓台账：用于根据成交计算 realized PnL（不含佣金）
    约定：
      - qty > 0 表示多头，qty < 0 表示空头
    """
    def __init__(self):
        self.pos: Dict[str, PositionState] = {}

    def on_fill(self, symbol: str, action: str, qty: float, price: float) -> float:
        """
        返回本次成交产生的 realized PnL（不含佣金）。
        """
        action = action.upper()
        signed_qty = qty if action == "BUY" else -qty

        st = self.pos.get(symbol, PositionState())
        realized = 0.0

        # 如果当前无仓，直接开仓
        if st.qty == 0:
            st.qty = signed_qty
            st.avg_price = price
            self.pos[symbol] = st
            return 0.0

        # 同方向加仓：更新均价
        if (st.qty > 0 and signed_qty > 0) or (st.qty < 0 and signed_qty < 0):
            new_qty = st.qty + signed_qty
            st.avg_price = (st.avg_price * abs(st.qty) + price * abs(signed_qty)) / abs(new_qty)
            st.qty = new_qty
            self.pos[symbol] = st
            return 0.0

        # 反向成交：先平仓一部分或全部
        close_qty = min(abs(st.qty), abs(signed_qty))
        # 多头被卖出平仓：pnl = (sell - avg) * close_qty
        if st.qty > 0 and signed_qty < 0:
            realized = (price - st.avg_price) * close_qty
        # 空头被买入平仓：pnl = (avg - buy) * close_qty
        elif st.qty < 0 and signed_qty > 0:
            realized = (st.avg_price - price) * close_qty

        remaining_qty = st.qty + signed_qty  # 因 signed_qty 反向，会减少绝对仓位或反转

        # 完全平仓
        if remaining_qty == 0:
            st.qty = 0.0
            st.avg_price = 0.0
        # 反转开仓（剩余方向变了）：以本次成交价作为新开仓均价
        elif (st.qty > 0 and remaining_qty < 0) or (st.qty < 0 and remaining_qty > 0):
            st.qty = remaining_qty
            st.avg_price = price
        else:
            # 部分平仓，均价不变
            st.qty = remaining_qty

        self.pos[symbol] = st
        return realized