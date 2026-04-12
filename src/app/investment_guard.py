from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from ..analysis.investment_portfolio import InvestmentPaperConfig
from ..app.investment_engine import InvestmentExecutionEngine, _to_float
from ..analysis.report import write_csv, write_json
from ..common.adaptive_strategy import AdaptiveStrategyConfig, adaptive_strategy_context
from ..common.logger import get_logger
from ..common.market_structure import MarketStructureConfig, market_structure_summary
from ..common.storage import Storage
from ..common.user_explanations import annotate_guard_user_explanation
from ..data import MarketDataAdapter
from ..ibkr.contracts import make_stock_contract
from ..ibkr.investment_orders import InvestmentOrderParams
from ..ibkr.market_data import MarketDataService, OHLCVBar
from ..offhours.ib_setup import register_contracts, set_delayed_frozen
from ..portfolio.investment_allocator import InvestmentExecutionConfig, load_lot_size_map

log = get_logger("app.investment_guard")


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _round_lot_qty(raw_qty: float, lot_size: int) -> int:
    lot = max(1, int(lot_size or 1))
    qty = int(abs(raw_qty) // lot) * lot
    return max(0, int(qty))


def _avg_true_range(bars: List[OHLCVBar], lookback: int) -> float:
    if len(bars) < 2:
        return 0.0
    trs: List[float] = []
    prev_close = float(bars[0].close)
    for bar in bars[1:]:
        high = float(bar.high)
        low = float(bar.low)
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = float(bar.close)
    if not trs:
        return 0.0
    use = trs[-max(1, int(lookback)) :]
    return float(sum(use) / len(use))


@dataclass
class InvestmentGuardConfig:
    history_days: int = 90
    trailing_high_lookback_days: int = 60
    atr_lookback_days: int = 20
    stop_loss_pct: float = 0.08
    trailing_stop_pct: float = 0.06
    trailing_stop_min_gain_pct: float = 0.08
    atr_stop_mult: float = 2.2
    take_profit_pct: float = 0.18
    take_profit_pullback_pct: float = 0.035
    trim_fraction: float = 0.33
    max_actions_per_run: int = 2
    allow_stop_loss: bool = True
    allow_take_profit: bool = True
    min_trade_value: float = 0.0
    use_intraday_5m: bool = True
    intraday_lookback_bars: int = 24

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentGuardConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def build_investment_guard_orders(
    current_positions: Dict[str, Dict[str, Any]],
    *,
    metrics_by_symbol: Dict[str, Dict[str, Any]],
    broker_equity: float,
    execution_cfg: InvestmentExecutionConfig,
    guard_cfg: InvestmentGuardConfig,
    lot_size_map: Dict[str, int] | None = None,
) -> List[Dict[str, Any]]:
    lot_size_map = {str(k).upper(): int(v) for k, v in (lot_size_map or {}).items()}
    min_trade_value = max(float(guard_cfg.min_trade_value), float(execution_cfg.min_trade_value))
    rows: List[Dict[str, Any]] = []

    for symbol, current in sorted((current_positions or {}).items()):
        symbol = str(symbol).upper()
        qty = _to_float(current.get("qty"), 0.0)
        avg_cost = _to_float(current.get("avg_cost"), 0.0)
        if qty <= 0 or avg_cost <= 0:
            continue

        metrics = dict(metrics_by_symbol.get(symbol) or {})
        ref_price = _to_float(metrics.get("ref_price"), _to_float(current.get("market_price"), avg_cost))
        if ref_price <= 0:
            continue

        recent_high = _to_float(metrics.get("recent_high"), ref_price)
        atr = max(0.0, _to_float(metrics.get("atr"), 0.0))
        pnl_pct = (ref_price - avg_cost) / avg_cost if avg_cost > 0 else 0.0
        pullback_from_high = ((recent_high - ref_price) / recent_high) if recent_high > 0 else 0.0

        fixed_stop = avg_cost * (1.0 - float(guard_cfg.stop_loss_pct))
        trailing_stop = 0.0
        if pnl_pct >= float(guard_cfg.trailing_stop_min_gain_pct) and recent_high > 0:
            trailing_stop = recent_high * (1.0 - float(guard_cfg.trailing_stop_pct))
        atr_stop = 0.0
        if pnl_pct > 0 and recent_high > 0 and atr > 0:
            atr_stop = recent_high - float(guard_cfg.atr_stop_mult) * atr
        effective_stop = max(fixed_stop, trailing_stop, atr_stop)
        lot_size = lot_size_map.get(symbol, max(1, int(execution_cfg.lot_size)))

        reason = ""
        trigger_price = 0.0
        sell_qty = 0

        if bool(guard_cfg.allow_stop_loss) and effective_stop > 0 and ref_price <= effective_stop:
            sell_qty = _round_lot_qty(qty, lot_size)
            trigger_price = effective_stop
            if effective_stop == trailing_stop and trailing_stop > 0:
                reason = "guard_trailing_stop"
            elif effective_stop == atr_stop and atr_stop > 0:
                reason = "guard_atr_stop"
            else:
                reason = "guard_stop_loss"
        elif (
            bool(guard_cfg.allow_take_profit)
            and pnl_pct >= float(guard_cfg.take_profit_pct)
            and pullback_from_high >= float(guard_cfg.take_profit_pullback_pct)
        ):
            sell_qty = _round_lot_qty(qty * float(guard_cfg.trim_fraction), lot_size)
            if sell_qty <= 0 and bool(execution_cfg.allow_min_lot_sell_override) and qty >= lot_size:
                sell_qty = int(lot_size)
            trigger_price = recent_high * (1.0 - float(guard_cfg.take_profit_pullback_pct))
            reason = "guard_take_profit_trim"

        if sell_qty <= 0:
            continue

        order_value = float(sell_qty * ref_price)
        if order_value < min_trade_value:
            continue

        rows.append(
            {
                "symbol": symbol,
                "action": "SELL",
                "current_qty": float(qty),
                "target_qty": float(max(0.0, qty - sell_qty)),
                "delta_qty": float(sell_qty),
                "ref_price": float(ref_price),
                "target_weight": 0.0,
                "order_value": float(order_value),
                "reason": reason,
                "pnl_pct": float(pnl_pct),
                "recent_high": float(recent_high),
                "pullback_from_high": float(pullback_from_high),
                "atr": float(atr),
                "fixed_stop": float(fixed_stop),
                "trailing_stop": float(trailing_stop),
                "atr_stop": float(atr_stop),
                "trigger_price": float(trigger_price),
                "market": "",
            }
        )

    rows.sort(
        key=lambda row: (
            0 if "stop" in str(row.get("reason", "")) else 1,
            -abs(float(row.get("pnl_pct") or 0.0)),
            -float(row.get("order_value") or 0.0),
        )
    )
    return rows[: max(0, int(guard_cfg.max_actions_per_run))]


@dataclass
class InvestmentGuardResult:
    run_id: str
    portfolio_id: str
    market: str
    report_dir: str
    submitted: bool
    order_count: int
    stop_count: int
    take_profit_count: int
    market_rules: str = ""
    adaptive_guard_status: str = ""


class InvestmentGuardEngine:
    def __init__(
        self,
        *,
        ib,
        account_id: str,
        storage: Storage,
        market: str,
        portfolio_id: str,
        execution_cfg: InvestmentExecutionConfig,
        guard_cfg: InvestmentGuardConfig,
        market_structure: MarketStructureConfig | None = None,
        adaptive_strategy: AdaptiveStrategyConfig | None = None,
    ):
        self.ib = ib
        self.storage = storage
        self.market = str(market).upper()
        self.portfolio_id = str(portfolio_id)
        self.execution_cfg = execution_cfg
        self.guard_cfg = guard_cfg
        self.market_structure = market_structure or MarketStructureConfig(market=self.market)
        self.adaptive_strategy = adaptive_strategy
        self.execution_engine = InvestmentExecutionEngine(
            ib=ib,
            account_id=account_id,
            storage=storage,
            market=market,
            portfolio_id=portfolio_id,
            paper_cfg=InvestmentPaperConfig(),
            execution_cfg=execution_cfg,
            market_structure=self.market_structure,
        )
        self.md = MarketDataService(ib)
        self.data_adapter = MarketDataAdapter(self.md)

    def _position_metrics(self, positions: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        symbols = sorted(str(sym).upper() for sym, pos in positions.items() if _to_float(pos.get("qty"), 0.0) > 0)
        if not symbols:
            return {}
        set_delayed_frozen(self.ib)
        register_contracts(self.ib, self.md, symbols)
        out: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            daily_bars, _ = self.data_adapter.get_daily_bars(symbol, days=int(self.guard_cfg.history_days))
            bars = [
                OHLCVBar(
                    time=bar.time,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(bar.volume),
                )
                for bar in daily_bars
            ]
            recent = bars[-max(1, int(self.guard_cfg.trailing_high_lookback_days)) :] if bars else []
            recent_high = max((float(bar.high) for bar in recent), default=0.0)
            atr = _avg_true_range(bars, int(self.guard_cfg.atr_lookback_days))
            intraday_bars = []
            intraday_source = ""
            intraday_close = 0.0
            if bool(self.guard_cfg.use_intraday_5m):
                intraday_bars, intraday_source = self.data_adapter.get_5m_bars_with_source(
                    symbol,
                    need=max(6, int(self.guard_cfg.intraday_lookback_bars)),
                    fallback_days=5,
                )
                intraday_closes = [float(bar.close) for bar in intraday_bars if getattr(bar, "close", None) is not None]
                if intraday_closes:
                    intraday_close = float(intraday_closes[-1])
            snapshot_price = self.md.get_snapshot_price(symbol)
            close_fallback = float(bars[-1].close) if bars else 0.0
            ref_price = float(snapshot_price or intraday_close or close_fallback or _to_float(positions.get(symbol, {}).get("market_price"), 0.0))
            if snapshot_price:
                ref_price_source = "ibkr_snapshot"
            elif intraday_close:
                ref_price_source = str(intraday_source or "intraday_5m")
            else:
                ref_price_source = "daily_close"
            out[symbol] = {
                "ref_price": ref_price,
                "ref_price_source": ref_price_source,
                "recent_high": float(recent_high or close_fallback),
                "atr": float(atr),
                "history_bars": int(len(bars)),
                "intraday_bars_5m": int(len(intraday_bars)),
                "intraday_close_5m": float(intraday_close),
            }
        return out

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _adaptive_guard_runtime_summary(self, report_path: Path) -> Dict[str, Any]:
        if self.adaptive_strategy is None:
            return {}
        market_sentiment = self._read_json(report_path / "market_sentiment.json")
        label = str(market_sentiment.get("label", "") or "").strip().upper()
        guard_status = "CLEAR"
        reason = "当前未检测到需要额外解释的防守环境。"
        if label == "DEFENSIVE":
            guard_status = "DEFENSIVE_REGIME"
            reason = "当前市场处于防守阶段；guard 动作优先按去风险或锁定利润来理解，不应把它视为重新加仓信号。"
        elif label == "BALANCED":
            guard_status = "BALANCED_REGIME"
            reason = "当前市场中性偏谨慎；guard 结果主要用于保守保护已有仓位。"
        elif label == "RISK_ON":
            guard_status = "RISK_ON"
            reason = "当前市场偏积极；guard 结果仍以保护收益和控制回撤为主。"
        return {
            "label": label or "-",
            "guard_status": guard_status,
            "reason": reason,
            "market_sentiment_score": float(market_sentiment.get("score", 0.0) or 0.0),
        }

    @staticmethod
    def _write_md(path: Path, summary: Dict[str, Any], order_rows: List[Dict[str, Any]]) -> None:
        market_rules = dict(summary.get("market_structure", {}) or {})
        adaptive_strategy = dict(summary.get("adaptive_strategy", {}) or {})
        adaptive_guard = dict(summary.get("adaptive_guard", {}) or {})
        lines = [
            "# Investment Guard Report",
            "",
            f"- Generated: {summary.get('ts', '')}",
            f"- Market: {summary.get('market', '')}",
            f"- Portfolio: {summary.get('portfolio_id', '')}",
            f"- Submitted: {summary.get('submitted', False)}",
            f"- Order count: {int(summary.get('order_count', 0) or 0)}",
            f"- Stop actions: {int(summary.get('stop_count', 0) or 0)}",
            f"- Take-profit actions: {int(summary.get('take_profit_count', 0) or 0)}",
            "",
        ]
        if market_rules:
            lines.extend(
                [
                    "## Market Rules",
                    "",
                    f"- Summary: {market_rules.get('summary_text', '-')}",
                    f"- Settlement: {market_rules.get('settlement_cycle', 'N/A')} | day_turnaround_allowed={bool(market_rules.get('day_turnaround_allowed', False))}",
                    f"- Buy lot: {int(market_rules.get('buy_lot_multiple', 1) or 1)} | fee_floor_one_side_bps={float(market_rules.get('fee_floor_one_side_bps', 0.0) or 0.0):.2f}",
                    "",
                ]
            )
        if adaptive_strategy:
            lines.extend(
                [
                    "## Adaptive Strategy",
                    "",
                    f"- Summary: {adaptive_strategy.get('summary_text', '-')}",
                    f"- Guard status: {adaptive_guard.get('guard_status', '-')}",
                    f"- Reason: {adaptive_guard.get('reason', '-')}",
                    "",
                ]
            )
        lines.extend(
            [
            "## Guard Orders",
            ]
        )
        if not order_rows:
            lines.append("- (no guard actions)")
        else:
            for row in order_rows:
                lines.append(
                    f"- {row['action']} {row['symbol']} qty={float(row.get('delta_qty', 0.0) or 0.0):.0f} "
                    f"ref={float(row.get('ref_price', 0.0) or 0.0):.2f} ref_source={row.get('ref_price_source', '')} "
                    f"reason={row.get('user_reason_label', row.get('reason', ''))} "
                    f"pnl_pct={float(row.get('pnl_pct', 0.0) or 0.0):.3f}"
                )
                detail = str(row.get("user_reason", "") or row.get("adaptive_strategy_note", "") or "").strip()
                if detail:
                    lines.append(f"  {detail}")
        path.write_text("\n".join(lines), encoding="utf-8")

    def run(self, *, report_dir: str, submit: bool = False) -> InvestmentGuardResult:
        report_path = Path(report_dir)
        broker_account = self.execution_engine._account_snapshot()
        broker_equity = float(broker_account.get("netliq", 0.0) or 0.0)
        broker_cash = float(broker_account.get("cash", 0.0) or 0.0)
        market_rules = market_structure_summary(self.market_structure, broker_equity=broker_equity)
        adaptive_guard = self._adaptive_guard_runtime_summary(report_path)
        positions_before = self.execution_engine._broker_positions()
        lot_size_map = load_lot_size_map(self.execution_cfg.lot_size_file)
        metrics = self._position_metrics(positions_before)
        order_rows = build_investment_guard_orders(
            positions_before,
            metrics_by_symbol=metrics,
            broker_equity=broker_equity,
            execution_cfg=self.execution_cfg,
            guard_cfg=self.guard_cfg,
            lot_size_map=lot_size_map,
        )
        run_id = f"{self.market}-guard-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        stop_count = sum(1 for row in order_rows if "stop" in str(row.get("reason", "")))
        take_profit_count = sum(1 for row in order_rows if str(row.get("reason", "")).startswith("guard_take_profit"))
        self.storage.insert_investment_execution_run(
            {
                "run_id": run_id,
                "market": self.market,
                "portfolio_id": self.portfolio_id,
                "account_id": self.execution_engine.account_id,
                "report_dir": str(report_path),
                "submitted": int(bool(submit)),
                "order_count": int(len(order_rows)),
                "order_value": float(sum(float(row.get("order_value") or 0.0) for row in order_rows)),
                "broker_equity": float(broker_equity),
                "broker_cash": float(broker_cash),
                "target_equity": 0.0,
                "details": json.dumps(
                    {
                        "execution_kind": "guard",
                        "metrics": metrics,
                    },
                    ensure_ascii=False,
                ),
            }
        )
        self.execution_engine._snapshot_broker_positions(run_id, positions_before, source="before", equity=broker_equity)

        for row in order_rows:
            row["market"] = self.market
            metric = dict(metrics.get(str(row.get("symbol", "")).upper()) or {})
            row["ref_price_source"] = str(metric.get("ref_price_source") or "")
            row["intraday_bars_5m"] = int(metric.get("intraday_bars_5m") or 0)
            row["intraday_close_5m"] = float(metric.get("intraday_close_5m") or 0.0)
            if str(adaptive_guard.get("guard_status", "") or "") == "DEFENSIVE_REGIME":
                if str(row.get("reason", "") or "").startswith("guard_take_profit"):
                    row["adaptive_strategy_note"] = "防守环境下优先锁定部分利润，避免把已有浮盈重新暴露给回撤。"
                else:
                    row["adaptive_strategy_note"] = "防守环境下保护性止损优先，先控制已有仓位风险。"
            annotate_guard_user_explanation(row)
            self.storage.insert_risk_event(
                "INVESTMENT_GUARD_TRIGGER",
                float(row.get("pnl_pct") or 0.0),
                f"symbol={row['symbol']} reason={row['reason']} ref={float(row.get('ref_price') or 0.0):.2f}",
                symbol=row["symbol"],
                portfolio_id=self.portfolio_id,
                system_kind="investment_guard",
                execution_run_id=run_id,
            )
            if not submit:
                self.storage.insert_investment_execution_order(
                    {
                        "run_id": run_id,
                        "market": self.market,
                        "portfolio_id": self.portfolio_id,
                        "symbol": row["symbol"],
                        "action": row["action"],
                        "current_qty": float(row.get("current_qty") or 0.0),
                        "target_qty": float(row.get("target_qty") or 0.0),
                        "delta_qty": float(row.get("delta_qty") or 0.0),
                        "ref_price": float(row.get("ref_price") or 0.0),
                        "target_weight": 0.0,
                        "order_value": float(row.get("order_value") or 0.0),
                        "order_type": str(self.execution_cfg.order_type),
                        "broker_order_id": 0,
                        "status": "PLANNED",
                        "reason": str(row.get("reason") or ""),
                        "details": json.dumps({"submitted": False, "execution_kind": "guard"}, ensure_ascii=False),
                    }
                )
                row["status"] = "PLANNED"
                continue

            contract = make_stock_contract(row["symbol"])
            trade = self.execution_engine.order_service.place_rebalance_order(
                contract,
                symbol=row["symbol"],
                action=row["action"],
                qty=float(row.get("delta_qty") or 0.0),
                params=InvestmentOrderParams(
                    order_type=str(self.execution_cfg.order_type),
                    ref_price=float(row.get("ref_price") or 0.0),
                    limit_price_buffer_bps=float(self.execution_cfg.limit_price_buffer_bps),
                    outside_rth=False,
                ),
                portfolio_id=self.portfolio_id,
                execution_run_id=run_id,
                plan_row=row,
                system_kind="investment_guard",
                signal_source="investment_guard",
            )
            row["broker_order_id"] = int(trade.order.orderId)
            row["status"] = "SUBMITTED"

        if submit and order_rows:
            deadline = datetime.now(timezone.utc).timestamp() + float(self.execution_cfg.wait_fill_sec)
            while datetime.now(timezone.utc).timestamp() < deadline:
                self.ib.sleep(float(self.execution_cfg.poll_interval_sec))

        positions_after = self.execution_engine._broker_positions()
        self.execution_engine._snapshot_broker_positions(run_id, positions_after, source="after", equity=broker_equity)
        summary = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "market": self.market,
            "portfolio_id": self.portfolio_id,
            "report_dir": str(report_path),
            "submitted": bool(submit),
            "broker_equity": float(broker_equity),
            "broker_cash": float(broker_cash),
            "order_count": int(len(order_rows)),
            "order_value": float(sum(float(row.get("order_value") or 0.0) for row in order_rows)),
            "stop_count": int(stop_count),
            "take_profit_count": int(take_profit_count),
            "market_structure": market_rules,
            "adaptive_strategy": adaptive_strategy_context(self.adaptive_strategy) if self.adaptive_strategy is not None else {},
            "adaptive_guard": adaptive_guard,
        }
        self.storage.update_investment_execution_run(
            run_id,
            details=json.dumps({"execution_kind": "guard", "metrics": metrics, "summary": summary}, ensure_ascii=False),
        )
        report_path.mkdir(parents=True, exist_ok=True)
        write_csv(str(report_path / "investment_guard_plan.csv"), order_rows)
        write_json(str(report_path / "investment_guard_summary.json"), summary)
        self._write_md(report_path / "investment_guard_report.md", summary, order_rows)
        log.info(
            "Investment guard complete: submitted=%s orders=%s stop_count=%s take_profit_count=%s",
            submit,
            len(order_rows),
            stop_count,
            take_profit_count,
        )
        return InvestmentGuardResult(
            run_id=run_id,
            portfolio_id=self.portfolio_id,
            market=self.market,
            report_dir=str(report_path),
            submitted=bool(submit),
            order_count=int(len(order_rows)),
            stop_count=int(stop_count),
            take_profit_count=int(take_profit_count),
            market_rules=str(market_rules.get("summary_text", "") or ""),
            adaptive_guard_status=str(adaptive_guard.get("guard_status", "") or ""),
        )
