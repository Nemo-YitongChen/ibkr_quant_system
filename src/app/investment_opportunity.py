from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from ..analysis.report import write_csv, write_json
from ..analysis.tracking import build_and_persist_analysis_chain
from ..app.investment_engine import InvestmentExecutionEngine, _to_float
from ..common.adaptive_strategy import (
    AdaptiveStrategyConfig,
    adaptive_strategy_context,
    align_opportunity_config_with_adaptive_strategy,
    apply_adaptive_defensive_opportunity_policy,
)
from ..common.logger import get_logger
from ..common.market_structure import MarketStructureConfig, market_structure_summary
from ..common.storage import Storage
from ..common.user_explanations import annotate_opportunity_user_explanation
from ..data import MarketDataAdapter
from ..ibkr.market_data import OHLCVBar
from ..offhours.ib_setup import market_data_service_from_config, register_contracts, set_delayed_frozen
from ..portfolio.investment_allocator import InvestmentExecutionConfig
from ..analysis.investment_portfolio import InvestmentPaperConfig

log = get_logger("app.investment_opportunity")

_EXTERNAL_MARKET_DATA_FIRST_MARKETS = {"XETRA"}


def _sma(values: List[float], window: int) -> float:
    if window <= 0 or len(values) < window:
        return 0.0
    use = values[-window:]
    return float(sum(use) / len(use))


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
    use = trs[-max(1, int(lookback)) :]
    return float(sum(use) / len(use)) if use else 0.0


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    return bool(value)


@dataclass
class InvestmentOpportunityConfig:
    history_days: int = 180
    ma_fast_days: int = 20
    ma_slow_days: int = 50
    atr_lookback_days: int = 14
    recent_high_lookback_days: int = 60
    max_candidates: int = 10
    min_score: float = 0.10
    pullback_entry_pct: float = 0.025
    ma_buffer_pct: float = 0.01
    atr_discount_mult: float = 0.35
    etf_pullback_entry_pct: float = 0.012
    etf_ma_buffer_pct: float = 0.012
    etf_atr_discount_mult: float = 0.15
    include_hold_candidates: bool = True
    use_intraday_5m: bool = True
    intraday_lookback_bars: int = 24
    prefer_external_market_data: bool | None = None

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentOpportunityConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def _positive_float(value: Any, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(fallback)
    if parsed > 0:
        return parsed
    return float(fallback)


def _anchor_components(
    *,
    last_close: float,
    ma_fast: float,
    atr: float,
    pullback_entry_pct: float,
    ma_buffer_pct: float,
    atr_discount_mult: float,
) -> Dict[str, float]:
    anchor_from_pullback = float(last_close) * (1.0 - float(pullback_entry_pct)) if last_close > 0 else 0.0
    anchor_from_ma = float(ma_fast) * (1.0 + float(ma_buffer_pct)) if ma_fast > 0 else anchor_from_pullback
    anchor_from_atr = float(last_close) - float(atr_discount_mult) * float(atr) if last_close > 0 and atr > 0 else anchor_from_pullback
    return {
        "pullback": float(anchor_from_pullback),
        "ma": float(anchor_from_ma),
        "atr": float(anchor_from_atr),
    }


def _select_anchor(components: Dict[str, float], *, mode: str) -> tuple[float, str]:
    usable = {key: float(value) for key, value in components.items() if float(value or 0.0) > 0.0}
    if not usable:
        return 0.0, ""
    if mode == "max":
        selected = max(usable, key=usable.get)
    else:
        selected = min(usable, key=usable.get)
    return float(usable[selected]), str(selected)


def _gap_pct(ref_price: float, anchor: float) -> float:
    if ref_price <= 0 or anchor <= 0:
        return 0.0
    return round(max(0.0, (float(ref_price) - float(anchor)) / float(ref_price) * 100.0), 4)


def _entry_anchor_diagnostics(
    cfg: InvestmentOpportunityConfig,
    *,
    asset_class: str,
    last_close: float,
    ref_price: float,
    ma_fast: float,
    ma_slow: float,
    atr: float,
    regime_state: str = "",
) -> Dict[str, Any]:
    base_components = _anchor_components(
        last_close=float(last_close),
        ma_fast=float(ma_fast),
        atr=float(atr),
        pullback_entry_pct=float(cfg.pullback_entry_pct),
        ma_buffer_pct=float(cfg.ma_buffer_pct),
        atr_discount_mult=float(cfg.atr_discount_mult),
    )
    base_anchor, base_selected = _select_anchor(base_components, mode="min")
    regime = str(regime_state or "").strip().upper()
    trend_ok = (ma_slow <= 0.0 or ref_price >= ma_slow) and regime not in {"RISK_OFF", "BEAR", "STRESS"}
    is_etf = str(asset_class or "").strip().lower() == "etf"

    selected_components = dict(base_components)
    selected_anchor = float(base_anchor)
    selected_component = str(base_selected)
    profile = "STANDARD_CONSERVATIVE"
    selection_rule = "min_of_standard_components"

    if is_etf and trend_ok:
        etf_components = _anchor_components(
            last_close=float(last_close),
            ma_fast=float(ma_fast),
            atr=float(atr),
            pullback_entry_pct=_positive_float(cfg.etf_pullback_entry_pct, float(cfg.pullback_entry_pct)),
            ma_buffer_pct=_positive_float(cfg.etf_ma_buffer_pct, float(cfg.ma_buffer_pct)),
            atr_discount_mult=_positive_float(cfg.etf_atr_discount_mult, float(cfg.atr_discount_mult)),
        )
        selected_anchor, selected_component = _select_anchor(etf_components, mode="max")
        selected_components = dict(etf_components)
        profile = "ETF_TREND_PULLBACK"
        selection_rule = "max_of_etf_components"

    return {
        "entry_anchor": float(selected_anchor),
        "entry_anchor_profile": profile,
        "entry_anchor_selection_rule": selection_rule,
        "entry_anchor_selected_component": selected_component,
        "entry_anchor_gap_pct": _gap_pct(float(ref_price), float(selected_anchor)),
        "base_entry_anchor": float(base_anchor),
        "base_entry_anchor_selected_component": str(base_selected),
        "base_entry_anchor_gap_pct": _gap_pct(float(ref_price), float(base_anchor)),
        "entry_anchor_components": selected_components,
        "base_entry_anchor_components": base_components,
    }


@dataclass
class InvestmentOpportunityResult:
    market: str
    portfolio_id: str
    report_dir: str
    entry_now_count: int
    near_entry_count: int
    wait_count: int
    market_structure_wait_count: int = 0
    adaptive_strategy_wait_count: int = 0
    market_rules: str = ""


class InvestmentOpportunityEngine:
    def __init__(
        self,
        *,
        ib,
        account_id: str,
        storage: Storage,
        market: str,
        portfolio_id: str,
        execution_cfg: InvestmentExecutionConfig,
        opportunity_cfg: InvestmentOpportunityConfig,
        market_structure: MarketStructureConfig | None = None,
        adaptive_strategy: AdaptiveStrategyConfig | None = None,
        market_data_cfg: Dict[str, Any] | None = None,
    ):
        self.ib = ib
        self.storage = storage
        self.market = str(market).upper()
        self.portfolio_id = str(portfolio_id)
        self.execution_cfg = execution_cfg
        self.adaptive_strategy = adaptive_strategy
        self.opportunity_cfg = align_opportunity_config_with_adaptive_strategy(opportunity_cfg, adaptive_strategy)
        self.market_structure = market_structure or MarketStructureConfig(market=self.market)
        configured_external_first = getattr(self.opportunity_cfg, "prefer_external_market_data", None)
        if configured_external_first is None:
            self.prefer_external_market_data = self.market in _EXTERNAL_MARKET_DATA_FIRST_MARKETS
        else:
            self.prefer_external_market_data = bool(configured_external_first)
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
        self.md = market_data_service_from_config(ib, market_data_cfg)
        self.data_adapter = MarketDataAdapter(
            self.md,
            prefer_yfinance_daily=bool(self.prefer_external_market_data),
            prefer_yfinance_intraday=bool(self.prefer_external_market_data),
        )

    @staticmethod
    def _read_csv(path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            return [dict(row) for row in csv.DictReader(f)]

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _candidate_metrics(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        if not symbols:
            return {}
        if not bool(self.prefer_external_market_data):
            set_delayed_frozen(self.ib)
            register_contracts(self.ib, self.md, symbols)
        out: Dict[str, Dict[str, Any]] = {}
        for symbol in symbols:
            bars, _ = self.data_adapter.get_daily_bars(symbol, days=int(self.opportunity_cfg.history_days))
            closes = [float(bar.close) for bar in bars if getattr(bar, "close", None) is not None]
            recent = bars[-max(1, int(self.opportunity_cfg.recent_high_lookback_days)) :] if bars else []
            recent_high = max((float(bar.high) for bar in recent), default=0.0)
            atr = _avg_true_range(bars, int(self.opportunity_cfg.atr_lookback_days))
            intraday_bars = []
            intraday_close = 0.0
            intraday_sma_6 = 0.0
            intraday_source = ""
            if bool(self.opportunity_cfg.use_intraday_5m):
                intraday_bars, intraday_source = self.data_adapter.get_5m_bars_with_source(
                    symbol,
                    need=max(6, int(self.opportunity_cfg.intraday_lookback_bars)),
                    fallback_days=5,
                )
                intraday_closes = [float(bar.close) for bar in intraday_bars if getattr(bar, "close", None) is not None]
                if intraday_closes:
                    intraday_close = float(intraday_closes[-1])
                    intraday_sma_6 = _sma(intraday_closes, min(6, len(intraday_closes)))
            snapshot_price = 0.0
            if not bool(self.prefer_external_market_data):
                try:
                    snapshot_price = self.md.get_snapshot_price(symbol)
                except Exception:
                    snapshot_price = 0.0
            last_close = float(closes[-1]) if closes else 0.0
            ref_price = float(snapshot_price or intraday_close or last_close)
            if snapshot_price:
                ref_price_source = "ibkr_snapshot"
            elif intraday_close:
                ref_price_source = str(intraday_source or "intraday_5m")
            else:
                ref_price_source = "daily_close"
            out[symbol] = {
                "ref_price": ref_price,
                "ref_price_source": ref_price_source,
                "last_close": float(last_close),
                "ma_fast": _sma(closes, int(self.opportunity_cfg.ma_fast_days)),
                "ma_slow": _sma(closes, int(self.opportunity_cfg.ma_slow_days)),
                "recent_high": float(recent_high or last_close),
                "atr": float(atr),
                "history_bars": int(len(bars)),
                "intraday_bars_5m": int(len(intraday_bars)),
                "intraday_close_5m": float(intraday_close),
                "intraday_sma_6_5m": float(intraday_sma_6),
            }
        return out

    @staticmethod
    def _write_md(path: Path, summary: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
        market_rules = dict(summary.get("market_structure", {}) or {})
        adaptive_rules = dict(summary.get("adaptive_strategy", {}) or {})
        lines = [
            "# Investment Opportunity Scan",
            "",
            f"- Generated: {summary.get('ts', '')}",
            f"- Market: {summary.get('market', '')}",
            f"- Portfolio: {summary.get('portfolio_id', '')}",
            f"- Entry now: {int(summary.get('entry_now_count', 0) or 0)}",
            f"- Near entry: {int(summary.get('near_entry_count', 0) or 0)}",
            f"- Wait: {int(summary.get('wait_count', 0) or 0)}",
            f"- Market-structure waits: {int(summary.get('market_structure_wait_count', 0) or 0)}",
            f"- Adaptive-strategy waits: {int(summary.get('adaptive_strategy_wait_count', 0) or 0)}",
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
        if adaptive_rules:
            lines.extend(
                [
                    "## Adaptive Strategy",
                    "",
                    f"- Summary: {adaptive_rules.get('summary_text', '-')}",
                    f"- Pullback trend MA: {int(dict(adaptive_rules.get('pullback', {}) or {}).get('trend_ma_window', 0) or 0)}",
                    f"- Defensive threshold raise: {float(dict(adaptive_rules.get('defensive', {}) or {}).get('raise_entry_threshold_pct', 0.0) or 0.0) * 100.0:.0f}%",
                    "",
                ]
            )
        market_news = list(summary.get("market_news", []) or [])
        if market_news:
            digest = []
            for item in market_news[:3]:
                title = str(item.get("title", "") or "").strip()
                publisher = str(item.get("publisher", "") or "").strip()
                if title:
                    digest.append(f"{publisher + ':' if publisher else ''}{title}")
            if digest:
                lines.append("- 市场消息: " + " | ".join(digest))
                lines.append("")
        lines.append("## Opportunities")
        if not rows:
            lines.append("- (no candidates)")
        else:
            for row in rows:
                lines.append(
                    f"- **{row['symbol']}** status={row.get('user_reason_label', row['entry_status'])} action={row['action']} "
                    f"score={float(row.get('score', 0.0) or 0.0):.3f} "
                    f"ref_price={float(row.get('ref_price', 0.0) or 0.0):.2f} "
                    f"ref_source={row.get('ref_price_source', '')} "
                    f"entry_anchor={float(row.get('entry_anchor', 0.0) or 0.0):.2f} "
                    f"anchor_gap={float(row.get('entry_anchor_gap_pct', 0.0) or 0.0):.2f}% "
                    f"anchor_profile={row.get('entry_anchor_profile', '')} "
                    f"ma20={float(row.get('ma_fast', 0.0) or 0.0):.2f} "
                    f"ma50={float(row.get('ma_slow', 0.0) or 0.0):.2f}"
                )
                lines.append(f"  {row.get('user_reason', row.get('entry_reason', ''))}")
                if int(row.get("recommendation_total", 0) or 0) > 0:
                    lines.append(
                        "  分析师预期: "
                        f"rec_score={float(row.get('recommendation_score', 0.0) or 0.0):.2f} "
                        f"SB={int(row.get('strong_buy', 0) or 0)} "
                        f"B={int(row.get('buy', 0) or 0)} "
                        f"H={int(row.get('hold', 0) or 0)} "
                        f"S={int(row.get('sell', 0) or 0)} "
                        f"SS={int(row.get('strong_sell', 0) or 0)}"
                    )
                if bool(row.get("earnings_in_14d", False)):
                    lines.append("  事件提示: 财报窗口临近，本次机会仅作观察，不建议激进追单。")
        path.write_text("\n".join(lines), encoding="utf-8")

    def _apply_market_structure_guidance(
        self,
        rows: List[Dict[str, Any]],
        *,
        broker_equity: float,
    ) -> List[Dict[str, Any]]:
        preferred_asset_classes = {
            str(item).strip().lower()
            for item in list(self.market_structure.portfolio_preferences.small_account_preferred_asset_classes or [])
            if str(item).strip()
        }
        threshold = float(self.market_structure.account_rules.prefer_etf_only_below_equity or 0.0)
        gated_statuses = {"ENTRY_NOW", "ADD_ON_PULLBACK", "NEAR_ENTRY"}
        for row in rows:
            entry_status = str(row.get("entry_status", "") or "").upper()
            row["market_structure_status"] = "CLEAR"
            row["market_structure_reason"] = ""
            if entry_status not in gated_statuses:
                annotate_opportunity_user_explanation(row)
                continue
            if bool(self.market_structure.research_only):
                row["entry_status"] = "WAIT_MARKET_RULE"
                row["entry_reason"] = "当前市场在本项目中仍为 research-only，先保留研究结论，不做交易进场。"
                row["market_structure_status"] = "RESEARCH_ONLY"
                row["market_structure_reason"] = "research_only market in current project scope"
                annotate_opportunity_user_explanation(row)
                continue
            if not self.market_structure.small_account_requires_etf_first(broker_equity):
                annotate_opportunity_user_explanation(row)
                continue
            asset_class = str(row.get("asset_class", "") or "").strip().lower()
            if asset_class and asset_class in preferred_asset_classes:
                row["market_structure_status"] = "CLEAR"
                row["market_structure_reason"] = (
                    f"equity={float(broker_equity):.2f} below threshold {threshold:.2f}, asset_class={asset_class}"
                )
                annotate_opportunity_user_explanation(row)
                continue
            row["entry_status"] = "WAIT_ACCOUNT_RULE"
            row["entry_reason"] = (
                f"当前账户权益低于 {threshold:.2f}，先优先 ETF / 高流动性基础标的，再考虑单只股票。"
            )
            row["market_structure_status"] = "SMALL_ACCOUNT_ETF_FIRST"
            row["market_structure_reason"] = (
                f"equity={float(broker_equity):.2f} below threshold {threshold:.2f}; "
                f"asset_class={asset_class or 'unknown'} not in {sorted(preferred_asset_classes)}"
            )
            annotate_opportunity_user_explanation(row)
        return rows

    def _effective_broker_equity(self, broker_account: Dict[str, Any]) -> tuple[float, float, float]:
        broker_equity_raw = float(broker_account.get("netliq", 0.0) or 0.0)
        account_equity_cap = max(0.0, float(getattr(self.execution_cfg, "account_equity_cap", 0.0) or 0.0))
        if account_equity_cap > 0.0:
            return min(broker_equity_raw, account_equity_cap), broker_equity_raw, account_equity_cap
        return broker_equity_raw, broker_equity_raw, 0.0

    def run(self, *, report_dir: str) -> InvestmentOpportunityResult:
        report_path = Path(report_dir)
        ranked_rows = self._read_csv(report_path / "investment_candidates.csv")
        plan_rows = self._read_csv(report_path / "investment_plan.csv")
        enrichment = self._read_json(report_path / "enrichment.json")
        if not ranked_rows:
            raise ValueError(f"missing investment_candidates.csv in {report_path}")
        plan_map = {str(row.get("symbol", "")).upper(): dict(row) for row in plan_rows}
        broker_account = self.execution_engine._account_snapshot()
        broker_equity, broker_equity_raw, account_equity_cap = self._effective_broker_equity(broker_account)
        broker_positions = self.execution_engine._broker_positions()

        eligible: List[Dict[str, Any]] = []
        for row in ranked_rows:
            action = str(row.get("action", "WATCH") or "WATCH").upper()
            score = _to_float(row.get("score"), 0.0)
            if action == "ACCUMULATE" or (self.opportunity_cfg.include_hold_candidates and action == "HOLD"):
                if score >= float(self.opportunity_cfg.min_score):
                    eligible.append(row)
        eligible = eligible[: max(0, int(self.opportunity_cfg.max_candidates))]

        symbols = [str(row.get("symbol", "")).upper() for row in eligible if str(row.get("symbol", "")).strip()]
        metrics = self._candidate_metrics(symbols)
        rows: List[Dict[str, Any]] = []
        for row in eligible:
            symbol = str(row.get("symbol", "")).upper()
            metric = dict(metrics.get(symbol) or {})
            if not metric:
                continue
            action = str(row.get("action", "WATCH") or "WATCH").upper()
            ref_price = _to_float(metric.get("ref_price"), _to_float(row.get("last_close"), 0.0))
            last_close = _to_float(metric.get("last_close"), _to_float(row.get("last_close"), 0.0))
            ma_fast = _to_float(metric.get("ma_fast"), 0.0)
            ma_slow = _to_float(metric.get("ma_slow"), 0.0)
            atr = _to_float(metric.get("atr"), 0.0)
            held_qty = _to_float((broker_positions.get(symbol) or {}).get("qty"), 0.0)
            earnings_in_14d = _to_bool(row.get("earnings_in_14d", False))
            anchor_diagnostics = _entry_anchor_diagnostics(
                self.opportunity_cfg,
                asset_class=str(row.get("asset_class", "") or ""),
                last_close=float(last_close),
                ref_price=float(ref_price),
                ma_fast=float(ma_fast),
                ma_slow=float(ma_slow),
                atr=float(atr),
                regime_state=str(row.get("regime_state", "") or ""),
            )
            entry_anchor = float(anchor_diagnostics.get("entry_anchor", 0.0) or 0.0)

            if earnings_in_14d:
                entry_status = "WAIT_EVENT"
                entry_reason = "财报窗口临近，先等事件风险释放后再评估进场。"
            elif ma_slow > 0 and ref_price < ma_slow:
                entry_status = "WAIT_TREND"
                entry_reason = "价格仍低于中期均线，先等趋势重新站稳。"
            elif ref_price <= entry_anchor:
                entry_status = "ENTRY_NOW" if held_qty <= 0 else "ADD_ON_PULLBACK"
                entry_reason = "当前价格已经回到计划中的分批进场带，可以考虑温和分批买入。"
            elif ref_price <= (entry_anchor * 1.01):
                entry_status = "NEAR_ENTRY"
                entry_reason = "价格接近理想进场带，可继续观察下一次回落确认。"
            else:
                entry_status = "WAIT_PULLBACK"
                entry_reason = "当前价格偏离理想进场带，暂时不追价。"

            rows.append(
                {
                    "symbol": symbol,
                    "action": action,
                    "score": _to_float(row.get("score"), 0.0),
                    "entry_style": str((plan_map.get(symbol) or {}).get("entry_style", "") or ""),
                    "ref_price": float(ref_price),
                    "last_close": float(last_close),
                    "entry_anchor": float(entry_anchor),
                    "entry_anchor_profile": str(anchor_diagnostics.get("entry_anchor_profile", "") or ""),
                    "entry_anchor_selection_rule": str(anchor_diagnostics.get("entry_anchor_selection_rule", "") or ""),
                    "entry_anchor_selected_component": str(anchor_diagnostics.get("entry_anchor_selected_component", "") or ""),
                    "entry_anchor_gap_pct": float(anchor_diagnostics.get("entry_anchor_gap_pct", 0.0) or 0.0),
                    "base_entry_anchor": float(anchor_diagnostics.get("base_entry_anchor", 0.0) or 0.0),
                    "base_entry_anchor_selected_component": str(anchor_diagnostics.get("base_entry_anchor_selected_component", "") or ""),
                    "base_entry_anchor_gap_pct": float(anchor_diagnostics.get("base_entry_anchor_gap_pct", 0.0) or 0.0),
                    "entry_anchor_components_json": json.dumps(anchor_diagnostics.get("entry_anchor_components", {}) or {}, sort_keys=True),
                    "base_entry_anchor_components_json": json.dumps(anchor_diagnostics.get("base_entry_anchor_components", {}) or {}, sort_keys=True),
                    "ma_fast": float(ma_fast),
                    "ma_slow": float(ma_slow),
                    "atr": float(atr),
                    "current_qty": float(held_qty),
                    "earnings_in_14d": bool(earnings_in_14d),
                    "recommendation_score": float(_to_float(row.get("recommendation_score"), 0.0)),
                    "strong_buy": int(_to_float(row.get("strong_buy"), 0.0)),
                    "buy": int(_to_float(row.get("buy"), 0.0)),
                    "hold": int(_to_float(row.get("hold"), 0.0)),
                    "sell": int(_to_float(row.get("sell"), 0.0)),
                    "strong_sell": int(_to_float(row.get("strong_sell"), 0.0)),
                    "recommendation_total": int(_to_float(row.get("recommendation_total"), 0.0)),
                    "entry_status": entry_status,
                    "entry_reason": entry_reason,
                    "asset_class": str(row.get("asset_class", "") or ""),
                    "regime_state": str(row.get("regime_state", "") or ""),
                    "market_sentiment": str(row.get("market_sentiment", "") or ""),
                    "ref_price_source": str(metric.get("ref_price_source") or ""),
                    "intraday_bars_5m": int(metric.get("intraday_bars_5m") or 0),
                    "intraday_close_5m": float(metric.get("intraday_close_5m") or 0.0),
                    "intraday_sma_6_5m": float(metric.get("intraday_sma_6_5m") or 0.0),
                    "market": self.market,
                }
            )

        rows = self._apply_market_structure_guidance(rows, broker_equity=broker_equity)
        rows = apply_adaptive_defensive_opportunity_policy(rows, self.adaptive_strategy)
        rows = [annotate_opportunity_user_explanation(dict(row)) for row in rows]
        priority = {
            "ENTRY_NOW": 0,
            "ADD_ON_PULLBACK": 1,
            "NEAR_ENTRY": 2,
            "WAIT_ACCOUNT_RULE": 3,
            "WAIT_DEFENSIVE_REGIME": 4,
            "WAIT_MARKET_RULE": 5,
            "WAIT_PULLBACK": 6,
            "WAIT_TREND": 7,
            "WAIT_EVENT": 8,
        }
        rows.sort(key=lambda row: (priority.get(str(row.get("entry_status", "")), 9), -float(row.get("score", 0.0) or 0.0)))

        observed_ts = datetime.now(timezone.utc).isoformat()
        analysis_run_id = f"{self.market}-analysis-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        analysis_tracking = build_and_persist_analysis_chain(
            self.storage,
            market=self.market,
            portfolio_id=self.portfolio_id,
            report_dir=str(report_path),
            analysis_run_id=analysis_run_id,
            observed_ts=observed_ts,
            ranked_rows=ranked_rows,
            opportunity_rows=rows,
            broker_positions=broker_positions,
            run_kind="opportunity",
        )
        market_rules = market_structure_summary(self.market_structure, broker_equity=broker_equity)
        market_structure_wait_count = int(
            sum(1 for row in rows if str(row.get("market_structure_status", "") or "").strip().upper() in {"RESEARCH_ONLY", "SMALL_ACCOUNT_ETF_FIRST"})
        )
        adaptive_strategy_wait_count = int(
            sum(1 for row in rows if str(row.get("adaptive_strategy_status", "") or "").strip().upper() == "DEFENSIVE_REGIME_CAP")
        )
        summary = {
            "ts": observed_ts,
            "market": self.market,
            "portfolio_id": self.portfolio_id,
            "entry_now_count": int(sum(1 for row in rows if str(row.get("entry_status", "")).upper() in {"ENTRY_NOW", "ADD_ON_PULLBACK"})),
            "near_entry_count": int(sum(1 for row in rows if str(row.get("entry_status", "")).upper() == "NEAR_ENTRY")),
            "wait_count": int(sum(1 for row in rows if str(row.get("entry_status", "")).upper().startswith("WAIT"))),
            "market_structure_wait_count": market_structure_wait_count,
            "adaptive_strategy_wait_count": adaptive_strategy_wait_count,
            "market_news": list(enrichment.get("market_news", []) or []),
            "analysis_run_id": analysis_run_id,
            "analysis_state_count": int(analysis_tracking.get("state_count", 0) or 0),
            "analysis_event_count": int(analysis_tracking.get("event_count", 0) or 0),
            "analysis_lifecycle_counts": dict(analysis_tracking.get("lifecycle_counts", {}) or {}),
            "market_structure": market_rules,
            "adaptive_strategy": adaptive_strategy_context(self.adaptive_strategy) if self.adaptive_strategy is not None else {},
            "broker_equity": float(broker_equity),
            "broker_equity_raw": float(broker_equity_raw),
            "account_equity_cap": float(account_equity_cap),
        }
        write_csv(str(report_path / "investment_opportunity_scan.csv"), rows)
        write_json(str(report_path / "investment_opportunity_summary.json"), summary)
        self._write_md(report_path / "investment_opportunity_report.md", summary, rows)
        log.info(
            "Investment opportunity scan complete: market=%s portfolio=%s entry_now=%s near=%s wait=%s analysis_events=%s",
            self.market,
            self.portfolio_id,
            summary["entry_now_count"],
            summary["near_entry_count"],
            summary["wait_count"],
            summary["analysis_event_count"],
        )
        return InvestmentOpportunityResult(
            market=self.market,
            portfolio_id=self.portfolio_id,
            report_dir=str(report_path),
            entry_now_count=int(summary["entry_now_count"]),
            near_entry_count=int(summary["near_entry_count"]),
            wait_count=int(summary["wait_count"]),
            market_structure_wait_count=market_structure_wait_count,
            adaptive_strategy_wait_count=adaptive_strategy_wait_count,
            market_rules=str(market_rules.get("summary_text", "") or ""),
        )
