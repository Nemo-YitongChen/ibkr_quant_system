from __future__ import annotations

from ..common.logger import get_logger
from ..common.storage import Storage
from ..ibkr.account import AccountService

log = get_logger("risk.gate")


class DailyRiskGate:
    """
    B2: 用 NetLiquidation 把当日累计净PnL（金额）转换为百分比再判断停机。
    NetLiquidation 是 AccountSummaryTags 的标准字段。 :contentReference[oaicite:4]{index=4}
    """

    def __init__(
        self,
        storage: Storage,
        account: AccountService,
        daily_loss_limit_short_pct: float,
        max_consecutive_losses: int,
    ):
        self.storage = storage
        self.account = account
        self.daily_loss_limit_short_pct = daily_loss_limit_short_pct
        self.max_consecutive_losses = max_consecutive_losses

        self.short_pnl_abs_today = 0.0
        self.consecutive_losses = 0
        self.short_trading_enabled = True

    def _netliq(self) -> float | None:
        return self.account.get_netliq()

    def _pnl_pct_today(self) -> float | None:
        netliq = self._netliq()
        if not netliq or netliq <= 0:
            return None
        return self.short_pnl_abs_today / netliq

    def on_trade_closed(self, trade_pnl: float, details: str = ""):
        """
        trade_pnl：净PnL金额（已扣佣金），由 FillProcessor 提供。
        """
        self.short_pnl_abs_today += trade_pnl

        if trade_pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0

        pnl_pct = self._pnl_pct_today()
        netliq = self._netliq()

        # 1) 日内亏损百分比停机（只有拿到 netliq 才判断，避免误触发）
        if pnl_pct is not None and pnl_pct <= self.daily_loss_limit_short_pct:
            self.short_trading_enabled = False
            self.storage.insert_risk_event(
                "SHORT_DAILY_LOSS_STOP_PCT",
                float(pnl_pct),
                f"netliq={netliq} pnl_abs={self.short_pnl_abs_today} {details}",
            )
            log.warning(
                f"Short trading STOPPED: pnl_abs={self.short_pnl_abs_today:.4f}, "
                f"netliq={netliq:.2f}, pnl_pct={pnl_pct:.4%}"
            )

        # 2) 连亏停机
        if self.consecutive_losses >= self.max_consecutive_losses:
            self.short_trading_enabled = False
            self.storage.insert_risk_event(
                "SHORT_CONSEC_LOSS_STOP",
                float(self.consecutive_losses),
                details,
            )
            log.warning(f"Short trading STOPPED: consecutive losses={self.consecutive_losses}")

    def can_trade_short(self) -> bool:
        return self.short_trading_enabled