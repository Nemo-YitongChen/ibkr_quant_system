from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Dict, Iterable, List

from ..common.logger import get_logger
from ..common.storage import Storage
from ..ibkr.account import AccountService

log = get_logger("risk.gate")


@dataclass
class RiskContextConfig:
    refresh_sec: int = 1800
    macro_block_importance: List[str] = field(default_factory=lambda: ["high", "3"])
    block_on_earnings: bool = True
    short_borrow_fee_bps_default: float = 0.0
    blocked_short_borrow_fee_bps: float = 10000.0
    short_borrow_fee_bps: Dict[str, float] = field(default_factory=dict)
    short_borrow_fee_sources: Dict[str, str] = field(default_factory=dict)
    blocked_short_symbols: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "RiskContextConfig":
        raw = raw or {}
        out: Dict[str, Any] = {}
        for key in cls.__dataclass_fields__:
            if key not in raw:
                continue
            if key == "macro_block_importance":
                out[key] = [str(x).lower() for x in raw[key]]
            elif key == "short_borrow_fee_bps":
                out[key] = {str(sym).upper(): float(val) for sym, val in dict(raw[key]).items()}
            elif key == "short_borrow_fee_sources":
                out[key] = {str(sym).upper(): str(val) for sym, val in dict(raw[key]).items()}
            elif key == "blocked_short_symbols":
                out[key] = [str(x).upper() for x in raw[key]]
            else:
                out[key] = raw[key]
        return cls(**out)


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
        context_cfg: RiskContextConfig | None = None,
        providers: Any = None,
        market: str = "",
    ):
        self.storage = storage
        self.account = account
        self.daily_loss_limit_short_pct = daily_loss_limit_short_pct
        self.max_consecutive_losses = max_consecutive_losses
        self.context_cfg = context_cfg or RiskContextConfig()
        self.providers = providers
        self.market = str(market or "").upper()

        self.short_pnl_abs_today = 0.0
        self.consecutive_losses = 0
        self.short_trading_enabled = True
        self._event_risk_by_symbol: Dict[str, str] = {}
        self._event_reason_by_symbol: Dict[str, str] = {}
        self._borrow_fee_bps_by_symbol: Dict[str, float] = {}
        self._borrow_fee_source_by_symbol: Dict[str, str] = {}
        self._last_context_refresh_ts: float = 0.0
        self._macro_high_risk: bool = False

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

    def event_risk_for(self, symbol: str) -> str:
        return str(self._event_risk_by_symbol.get(str(symbol).upper(), "NONE"))

    def event_risk_reason_for(self, symbol: str) -> str:
        return str(self._event_reason_by_symbol.get(str(symbol).upper(), ""))

    def short_borrow_fee_bps_for(self, symbol: str) -> float:
        sym = str(symbol).upper()
        if sym in self._borrow_fee_bps_by_symbol:
            return float(self._borrow_fee_bps_by_symbol[sym])
        return float(self.context_cfg.short_borrow_fee_bps_default)

    def short_borrow_source_for(self, symbol: str) -> str:
        return str(self._borrow_fee_source_by_symbol.get(str(symbol).upper(), "default"))

    def refresh_trade_context(self, symbols: Iterable[str]) -> None:
        syms = [str(sym).upper() for sym in symbols if str(sym).strip()]
        if not syms:
            return
        syms = list(dict.fromkeys(syms))

        for sym in syms:
            self._borrow_fee_bps_by_symbol[sym] = float(
                self.context_cfg.short_borrow_fee_bps.get(sym, self.context_cfg.short_borrow_fee_bps_default)
            )
            self._borrow_fee_source_by_symbol[sym] = str(self.context_cfg.short_borrow_fee_sources.get(sym, "default"))
            if sym in self.context_cfg.blocked_short_symbols:
                self._borrow_fee_bps_by_symbol[sym] = float(self.context_cfg.blocked_short_borrow_fee_bps)
                self._borrow_fee_source_by_symbol[sym] = "blocked_short_symbols"

        now = time.time()
        if (now - self._last_context_refresh_ts) < float(self.context_cfg.refresh_sec):
            return

        self._last_context_refresh_ts = now
        if self.providers is None:
            return

        try:
            bundle = self.providers.collect(symbols=syms, market=self.market)
        except Exception as e:
            self.storage.insert_risk_event(
                "RISK_CONTEXT_REFRESH_ERROR",
                0.0,
                f"provider_error={type(e).__name__}",
            )
            log.warning(f"risk context refresh failed: {type(e).__name__} {e}")
            return

        earnings = bundle.get("earnings", {}) if isinstance(bundle, dict) else {}
        macro_events = bundle.get("macro_events", []) if isinstance(bundle, dict) else []
        macro_imp = {str(x).lower() for x in self.context_cfg.macro_block_importance}
        self._macro_high_risk = any(str(ev.get("importance", "")).lower() in macro_imp for ev in macro_events if isinstance(ev, dict))

        high_count = 0
        for sym in syms:
            event_risk = "NONE"
            reasons: List[str] = []
            info = earnings.get(sym, {}) if isinstance(earnings, dict) else {}
            if bool(self.context_cfg.block_on_earnings) and bool(info.get("in_14d", info.get("in_window", False))):
                event_risk = "HIGH"
                nxt = str(info.get("next_earnings_date") or "").strip()
                reasons.append(f"earnings:{nxt or 'window'}")
            if self._macro_high_risk:
                event_risk = "HIGH"
                reasons.append("macro_calendar_high")
            self._event_risk_by_symbol[sym] = event_risk
            self._event_reason_by_symbol[sym] = ",".join(reasons)
            if event_risk == "HIGH":
                high_count += 1

        self.storage.insert_risk_event(
            "RISK_CONTEXT_REFRESH",
            float(high_count),
            f"symbols={len(syms)} macro_high={int(self._macro_high_risk)}",
        )
