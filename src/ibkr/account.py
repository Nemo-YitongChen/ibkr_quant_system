from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from ib_insync import IB

from ..common.logger import get_logger

log = get_logger("ibkr.account")


@dataclass
class NetLiqState:
    value: Optional[float] = None
    currency: Optional[str] = None


class AccountService:
    """
    B2: 获取 NetLiquidation 用于把当日PnL(金额)转换为百分比。

    注意：不同 ib_insync 版本里 IB.reqAccountSummary 的签名可能不同，
    文档也建议优先使用 accountSummary()（阻塞式）来获取并保持更新。 :contentReference[oaicite:2]{index=2}
    """

    def __init__(self, ib: IB, account_id: str):
        self.ib = ib
        self.account_id = account_id
        self.netliq = NetLiqState()
        self._started = False
        self._last_refresh = 0.0

    def start(self):
        # 对于 accountSummary() 方案，start 只做一次初始化刷新
        if self._started:
            return
        self._started = True
        self.refresh(force=True)

    def stop(self):
        self._started = False

    def refresh(self, force: bool = False):
        """
        拉取一次 accountSummary() 并提取 NetLiquidation。
        accountSummary() 是阻塞式，会在数据填充完成后返回。 :contentReference[oaicite:3]{index=3}
        """
        if not self._started and not force:
            return

        rows = self.ib.accountSummary()  # blocking
        # rows: List[AccountValue] with fields: account, tag, value, currency
        # NetLiquidation 是标准 tag :contentReference[oaicite:4]{index=4}
        candidates = [r for r in rows if r.tag == "NetLiquidation"]

        # 优先匹配指定 account_id；若没有就取第一条（单账户时常见）
        row = None
        for r in candidates:
            if r.account == self.account_id:
                row = r
                break
        if row is None and candidates:
            row = candidates[0]

        if row:
            try:
                v = float(row.value)
                self.netliq.value = v
                self.netliq.currency = row.currency
                log.info(f"NetLiquidation updated: {v} {row.currency} (account={row.account})")
            except Exception:
                pass

    def get_netliq(self) -> Optional[float]:
        return self.netliq.value