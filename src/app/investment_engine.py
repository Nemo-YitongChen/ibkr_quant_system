from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..analysis.investment_portfolio import InvestmentPaperConfig, build_target_allocations
from ..analysis.report import write_csv, write_json
from ..common.account_profile import AccountProfilesConfig, apply_account_profile
from ..common.adaptive_strategy import (
    adaptive_strategy_effective_control_fields,
    adaptive_strategy_effective_controls,
    adaptive_strategy_summary_fields,
    apply_active_market_execution_overrides,
    apply_active_market_risk_overrides,
    apply_adaptive_strategy_execution_controls,
    load_report_adaptive_strategy_payload,
)
from ..common.artifact_contracts import ARTIFACT_SCHEMA_VERSION
from ..common.market_structure import MarketStructureConfig
from ..common.markets import market_timezone_name, symbol_matches_market
from ..common.logger import get_logger
from ..common.storage import Storage, build_investment_risk_history_row
from ..common.user_explanations import annotate_execution_user_explanation
from ..events.models import ExecutionIntent
from ..ibkr.contracts import make_stock_contract
from ..ibkr.fills import FillProcessor
from ..ibkr.investment_orders import InvestmentOrderParams, InvestmentOrderService
from ..portfolio.investment_allocator import (
    InvestmentExecutionConfig,
    build_investment_rebalance_orders,
    load_lot_size_map,
)

log = get_logger("app.investment_engine")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _is_placeholder_account_id(value: str) -> bool:
    account_id = str(value or "").strip().upper()
    return (not account_id) or ("XXXX" in account_id)


class _NoOpGate:
    def on_trade_closed(self, trade_pnl: float, details: str = "") -> None:
        return None


@dataclass
class InvestmentExecutionResult:
    run_id: str
    portfolio_id: str
    market: str
    report_dir: str
    submitted: bool
    broker_equity: float
    broker_cash: float
    target_equity: float
    order_count: int
    order_value: float
    gap_symbols: int
    gap_notional: float
    account_profile_name: str = ""
    account_profile_label: str = ""


@dataclass(frozen=True)
class ExecutionSessionProfile:
    session_bucket: str
    session_label: str
    execution_style: str
    aggressiveness: float
    participation_scale: float
    limit_buffer_scale: float


class InvestmentExecutionEngine:
    def __init__(
        self,
        *,
        ib,
        account_id: str,
        storage: Storage,
        market: str,
        portfolio_id: str,
        paper_cfg: InvestmentPaperConfig,
        execution_cfg: InvestmentExecutionConfig,
        market_structure: MarketStructureConfig | None = None,
        account_profiles: AccountProfilesConfig | None = None,
    ):
        self.ib = ib
        self.account_id = str(account_id)
        self.storage = storage
        self.market = str(market).upper()
        self.portfolio_id = str(portfolio_id)
        self.paper_cfg = paper_cfg
        self.execution_cfg = execution_cfg
        self.market_structure = market_structure or MarketStructureConfig(market=self.market)
        self.account_profiles = account_profiles or AccountProfilesConfig()
        self.order_service = InvestmentOrderService(
            ib,
            self.account_id,
            storage,
            market=self.market,
            portfolio_id=self.portfolio_id,
        )
        self.fill_processor = FillProcessor(ib, storage, _NoOpGate())

    def _require_valid_account_id(self) -> str:
        account_id = str(self.account_id or "").strip()
        if _is_placeholder_account_id(account_id):
            raise ValueError(
                "IBKR account_id is still a placeholder. Set the real paper account id in config/ibkr_<market>.yaml before broker-synced execution."
            )
        return account_id

    @staticmethod
    def _read_csv(path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            return [dict(row) for row in csv.DictReader(f)]

    @staticmethod
    def _normalize_direction(value: Any, default: str = "LONG") -> str:
        text = str(value or default).strip().upper()
        return text if text in {"LONG", "SHORT"} else str(default or "LONG").upper()

    @classmethod
    def _read_candidate_map(cls, report_path: Path) -> Dict[tuple[str, str], Dict[str, Any]]:
        candidate_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        sources = (
            (report_path / "investment_candidates.csv", "LONG"),
            (report_path / "investment_short_candidates.csv", "SHORT"),
        )
        for path, default_direction in sources:
            for row in cls._read_csv(path):
                symbol = str(row.get("symbol") or "").upper().strip()
                if not symbol:
                    continue
                direction = cls._normalize_direction(row.get("direction"), default_direction)
                candidate_map[(symbol, direction)] = dict(row)
        return candidate_map

    @classmethod
    def _read_report_books(cls, report_path: Path) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        candidates = cls._read_csv(report_path / "investment_candidates.csv")
        candidates.extend(cls._read_csv(report_path / "investment_short_candidates.csv"))
        plans = cls._read_csv(report_path / "investment_plan.csv")
        plans.extend(cls._read_csv(report_path / "investment_short_plan.csv"))
        return candidates, plans

    @staticmethod
    def _is_long_entry_order(row: Dict[str, Any]) -> bool:
        action = str(row.get("action") or "").upper()
        current_qty = _to_float(row.get("current_qty"), 0.0)
        target_qty = _to_float(row.get("target_qty"), 0.0)
        target_weight = _to_float(row.get("target_weight"), 0.0)
        return action == "BUY" and target_weight > 0.0 and target_qty > 0.0 and target_qty > current_qty

    @staticmethod
    def _is_short_entry_order(row: Dict[str, Any]) -> bool:
        action = str(row.get("action") or "").upper()
        current_qty = _to_float(row.get("current_qty"), 0.0)
        target_qty = _to_float(row.get("target_qty"), 0.0)
        target_weight = _to_float(row.get("target_weight"), 0.0)
        return action == "SELL" and target_weight < 0.0 and target_qty < 0.0 and target_qty < current_qty

    @staticmethod
    def _parse_details(value: Any) -> Dict[str, Any]:
        if not isinstance(value, str) or not value:
            return {}
        try:
            data = json.loads(value)
            return dict(data) if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _market_session_bounds(market: str) -> tuple[int, int, int, int]:
        # 这里不追求交易所级精确日历，只区分开盘/午盘/尾盘三段，用于 lite 拆单风格。
        code = str(market or "").upper().strip()
        if code == "US":
            return 9, 30, 16, 0
        if code in {"HK", "CN"}:
            return 9, 30, 16, 0
        if code == "ASX":
            return 10, 0, 16, 0
        if code == "XETRA":
            return 9, 0, 17, 30
        return 9, 30, 16, 0

    def _current_execution_session_profile(self) -> ExecutionSessionProfile:
        tz_name = market_timezone_name(self.market, "UTC")
        local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(str(tz_name or "UTC")))
        open_h, open_m, close_h, close_m = self._market_session_bounds(self.market)
        open_min = open_h * 60 + open_m
        close_min = close_h * 60 + close_m
        now_min = int(local_now.hour * 60 + local_now.minute)
        from_open = now_min - open_min
        to_close = close_min - now_min
        if from_open <= 60:
            return ExecutionSessionProfile(
                session_bucket="OPEN",
                session_label="开盘",
                execution_style="TWAP_LITE_OPEN",
                aggressiveness=0.72,
                participation_scale=float(self.execution_cfg.open_session_participation_scale or 0.70),
                limit_buffer_scale=float(self.execution_cfg.open_session_limit_buffer_scale or 1.25),
            )
        if to_close <= 60:
            return ExecutionSessionProfile(
                session_bucket="CLOSE",
                session_label="尾盘",
                execution_style="VWAP_LITE_CLOSE",
                aggressiveness=0.90,
                participation_scale=float(self.execution_cfg.close_session_participation_scale or 0.85),
                limit_buffer_scale=float(self.execution_cfg.close_session_limit_buffer_scale or 1.10),
            )
        return ExecutionSessionProfile(
            session_bucket="MIDDAY",
            session_label="午盘",
            execution_style="VWAP_LITE_MIDDAY",
            aggressiveness=0.55,
            participation_scale=float(self.execution_cfg.midday_session_participation_scale or 1.00),
            limit_buffer_scale=float(self.execution_cfg.midday_session_limit_buffer_scale or 0.85),
        )

    def _execution_hotspot_penalty_map(self) -> Dict[str, Dict[str, Any]]:
        raw_rows = self.execution_cfg.execution_hotspot_penalties or []
        rows: List[Dict[str, Any]] = []
        if isinstance(raw_rows, str) and str(raw_rows).strip():
            try:
                parsed = json.loads(raw_rows)
            except Exception:
                parsed = []
            if isinstance(parsed, list):
                rows = [dict(item) for item in parsed if isinstance(item, dict)]
        elif isinstance(raw_rows, (list, tuple)):
            rows = [dict(item) for item in raw_rows if isinstance(item, dict)]

        out: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            if not symbol:
                continue
            out[symbol] = dict(row)
        return out

    def _execution_hotspot_defer_buckets(self) -> set[str]:
        raw = self.execution_cfg.execution_hotspot_defer_session_buckets or ()
        if isinstance(raw, str):
            values = [part.strip() for part in str(raw).split(",")]
        else:
            values = [str(part).strip() for part in list(raw)]
        return {value.upper() for value in values if value}

    def _risk_alert_defer_buckets(self) -> set[str]:
        raw = self.execution_cfg.risk_alert_defer_session_buckets or ()
        if isinstance(raw, str):
            values = [part.strip() for part in str(raw).split(",")]
        else:
            values = [str(part).strip() for part in list(raw)]
        return {value.upper() for value in values if value}

    def _current_portfolio_risk_alert_summary(self) -> Dict[str, Any]:
        if not bool(self.execution_cfg.risk_alert_guard_enabled):
            return {}

        rows = self.storage.get_recent_investment_risk_history(
            self.market,
            self.portfolio_id,
            source_kind="execution",
            limit=2,
        )
        if not rows:
            rows = self.storage.get_recent_investment_risk_history(
                self.market,
                self.portfolio_id,
                source_kind="paper",
                limit=2,
            )
        if not rows:
            return {}

        latest = dict(rows[0])
        latest_ts = str(latest.get("ts") or "").strip()
        if latest_ts:
            try:
                latest_dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                max_age = max(1, int(self.execution_cfg.risk_alert_history_max_age_hours or 96))
                if latest_dt < (datetime.now(timezone.utc) - timedelta(hours=max_age)):
                    return {}
            except Exception:
                pass

        previous = dict(rows[1]) if len(rows) > 1 else {}
        latest_scale = _to_float(latest.get("dynamic_scale"), 1.0)
        latest_net = _to_float(latest.get("dynamic_net_exposure"), 0.0)
        latest_gross = _to_float(latest.get("dynamic_gross_exposure"), 0.0)
        latest_corr = _to_float(latest.get("avg_pair_correlation"), 0.0)
        latest_stress = _to_float(latest.get("stress_worst_loss"), 0.0)
        previous_scale = _to_float(previous.get("dynamic_scale"), latest_scale)
        previous_corr = _to_float(previous.get("avg_pair_correlation"), latest_corr)
        previous_stress = _to_float(previous.get("stress_worst_loss"), latest_stress)
        scale_delta = float(latest_scale - previous_scale)
        corr_delta = float(latest_corr - previous_corr)
        stress_delta = float(latest_stress - previous_stress)
        tightening = (
            scale_delta <= float(self.execution_cfg.risk_alert_scale_watch_delta or -0.05)
            or (latest_net > 0.0 and latest_net <= 0.70)
            or (latest_gross > 0.0 and latest_gross <= 0.75)
        )
        if (
            latest_scale <= float(self.execution_cfg.risk_alert_scale_alert_threshold or 0.75)
            or latest_corr >= float(self.execution_cfg.risk_alert_corr_alert_threshold or 0.62)
            or latest_stress >= float(self.execution_cfg.risk_alert_stress_alert_threshold or 0.085)
        ):
            alert_level = "ALERT"
        elif (
            tightening
            or corr_delta >= float(self.execution_cfg.risk_alert_corr_watch_delta or 0.04)
            or stress_delta >= float(self.execution_cfg.risk_alert_stress_watch_delta or 0.01)
        ):
            alert_level = "WATCH"
        else:
            alert_level = "STABLE"

        reason_parts: List[str] = []
        if latest_scale <= float(self.execution_cfg.risk_alert_scale_alert_threshold or 0.75):
            reason_parts.append("动态 scale 偏低")
        if latest_corr >= float(self.execution_cfg.risk_alert_corr_alert_threshold or 0.62):
            reason_parts.append("平均相关性偏高")
        if latest_stress >= float(self.execution_cfg.risk_alert_stress_alert_threshold or 0.085):
            reason_parts.append("最差 stress 损失偏高")
        if corr_delta >= float(self.execution_cfg.risk_alert_corr_watch_delta or 0.04):
            reason_parts.append("相关性继续抬升")
        if stress_delta >= float(self.execution_cfg.risk_alert_stress_watch_delta or 0.01):
            reason_parts.append("stress 损失继续恶化")
        if tightening and not reason_parts:
            reason_parts.append("风险预算最近继续收紧")
        if not reason_parts:
            reason_parts.append("组合风险预算暂时平稳")

        return {
            "source_label": str(latest.get("source_label") or ""),
            "alert_level": alert_level,
            "trend_label": "收紧" if tightening else "稳定",
            "latest_ts": latest_ts,
            "dynamic_scale": latest_scale,
            "dynamic_net_exposure": latest_net,
            "dynamic_gross_exposure": latest_gross,
            "avg_pair_correlation": latest_corr,
            "stress_worst_loss": latest_stress,
            "stress_worst_scenario_label": str(latest.get("stress_worst_scenario_label") or ""),
            "scale_delta": scale_delta if previous else 0.0,
            "corr_delta": corr_delta if previous else 0.0,
            "stress_delta": stress_delta if previous else 0.0,
            "diagnosis": "；".join(reason_parts),
        }

    def _apply_portfolio_risk_alert_gates(
        self,
        order_rows: List[Dict[str, Any]],
        *,
        broker_equity: float,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        summary = self._current_portfolio_risk_alert_summary()
        if not summary or str(summary.get("alert_level") or "") == "STABLE":
            return order_rows, [], summary

        session = self._current_execution_session_profile()
        defer_buckets = self._risk_alert_defer_buckets()
        alert_level = str(summary.get("alert_level") or "WATCH")
        if alert_level == "ALERT":
            adv_scale = max(0.20, min(1.0, float(self.execution_cfg.risk_alert_alert_adv_participation_scale or 0.65)))
            split_scale = max(0.20, min(1.0, float(self.execution_cfg.risk_alert_alert_split_trigger_scale or 0.65)))
            limit_buffer_scale = max(1.0, float(self.execution_cfg.risk_alert_alert_limit_buffer_scale or 1.30))
        else:
            adv_scale = max(0.20, min(1.0, float(self.execution_cfg.risk_alert_watch_adv_participation_scale or 0.85)))
            split_scale = max(0.20, min(1.0, float(self.execution_cfg.risk_alert_watch_split_trigger_scale or 0.85)))
            limit_buffer_scale = max(1.0, float(self.execution_cfg.risk_alert_watch_limit_buffer_scale or 1.10))
        force_min_slices = max(1, int(self.execution_cfg.risk_alert_force_min_slices_alert or 1))
        manual_review_threshold = max(0.0, float(self.execution_cfg.risk_alert_manual_review_order_value_pct or 0.0))

        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        for row in order_rows:
            if not (self._is_long_entry_order(row) or self._is_short_entry_order(row)):
                filtered.append(row)
                continue

            order_value = abs(_to_float(row.get("order_value"), 0.0))
            order_pct = (order_value / max(float(broker_equity), 1e-9)) if float(broker_equity) > 0.0 else 0.0
            if alert_level == "ALERT" and manual_review_threshold > 0.0 and order_pct >= manual_review_threshold:
                blocked_row = dict(row)
                blocked_row["status"] = "REVIEW_REQUIRED"
                blocked_row["manual_review_status"] = "REVIEW_REQUIRED"
                blocked_row["manual_review_reason"] = "组合风险告警期间，大额新单需要人工确认。"
                blocked_row["risk_alert_applied"] = True
                blocked_row["risk_alert_status"] = "REVIEW_REQUIRED"
                blocked_row["risk_alert_level"] = alert_level
                blocked_row["risk_alert_trend_label"] = str(summary.get("trend_label") or "")
                blocked_row["risk_alert_reason"] = str(summary.get("diagnosis") or "")
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|risk_alert_review".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            if alert_level == "ALERT" and session.session_bucket in defer_buckets:
                blocked_row = dict(row)
                blocked_row["status"] = "DEFERRED_RISK_ALERT"
                blocked_row["risk_alert_applied"] = True
                blocked_row["risk_alert_status"] = "DEFERRED"
                blocked_row["risk_alert_level"] = alert_level
                blocked_row["risk_alert_trend_label"] = str(summary.get("trend_label") or "")
                blocked_row["risk_alert_reason"] = (
                    f"组合风险告警期间，{session.session_label}优先延后新增仓位。{str(summary.get('diagnosis') or '')}"
                ).strip()
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|risk_alert_defer".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            slowed_row = dict(row)
            slowed_row["risk_alert_applied"] = True
            slowed_row["risk_alert_status"] = "SLOWED"
            slowed_row["risk_alert_level"] = alert_level
            slowed_row["risk_alert_trend_label"] = str(summary.get("trend_label") or "")
            slowed_row["risk_alert_reason"] = str(summary.get("diagnosis") or "")
            slowed_row["risk_alert_adv_scale"] = adv_scale
            slowed_row["risk_alert_split_trigger_scale"] = split_scale
            slowed_row["risk_alert_limit_buffer_scale"] = limit_buffer_scale
            slowed_row["risk_alert_force_min_slices"] = force_min_slices
            slowed_row["risk_alert_force_limit_order"] = bool(self.execution_cfg.risk_alert_force_limit_order)
            slowed_row["reason"] = f"{str(row.get('reason') or '')}|risk_alert_slowdown".strip("|")
            filtered.append(slowed_row)
        return filtered, blocked, summary

    @classmethod
    def _build_priority_context_map(
        cls,
        candidates: List[Dict[str, Any]],
        plans: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for row in list(candidates or []) + list(plans or []):
            symbol = str(row.get("symbol") or "").upper().strip()
            if not symbol:
                continue
            current = merged.setdefault(symbol, {})
            for key in (
                "score",
                "score_before_cost",
                "model_recommendation_score",
                "execution_score",
                "expected_edge_threshold",
                "expected_edge_score",
                "expected_edge_bps",
                "expected_cost_bps",
                "spread_proxy_bps",
                "slippage_proxy_bps",
                "commission_proxy_bps",
                "avg_daily_volume",
                "avg_daily_dollar_volume",
                "liquidity_score",
                "direction",
            ):
                value = row.get(key)
                if value not in (None, ""):
                    current[key] = value
        return merged

    @staticmethod
    def _round_split_qty(total_qty: float, lot_size: int, *, allow_fractional: bool, decimals: int) -> float:
        total_qty = max(0.0, float(total_qty))
        if allow_fractional and int(lot_size or 1) <= 1:
            scale = float(10 ** max(0, int(decimals or 0)))
            return float(int(total_qty * scale) / scale)
        lot = max(1, int(lot_size or 1))
        return float(int(total_qty // lot) * lot)

    def _cost_breakdown(self, row: Dict[str, Any], *, order_value: float) -> Dict[str, float]:
        spread_bps = max(0.0, _to_float(row.get("spread_proxy_bps"), 0.0))
        slippage_bps = max(0.0, _to_float(row.get("slippage_proxy_bps"), 0.0))
        commission_bps = max(0.0, _to_float(row.get("commission_proxy_bps"), 0.0))
        expected_cost_bps = max(0.0, _to_float(row.get("expected_cost_bps"), spread_bps + slippage_bps + commission_bps))
        order_value = abs(float(order_value or 0.0))
        return {
            "spread_proxy_bps": float(spread_bps),
            "slippage_proxy_bps": float(slippage_bps),
            "commission_proxy_bps": float(commission_bps),
            "expected_cost_bps": float(expected_cost_bps),
            "expected_spread_cost": float(order_value * spread_bps / 10000.0),
            "expected_slippage_cost": float(order_value * slippage_bps / 10000.0),
            "expected_commission_cost": float(order_value * commission_bps / 10000.0),
            "expected_cost_value": float(order_value * expected_cost_bps / 10000.0),
        }

    def _effective_buy_lot_multiple(self, row: Dict[str, Any]) -> int:
        rule_lot = max(1, int(self.market_structure.order_rules.buy_lot_multiple or 1))
        row_lot = max(1, int(_to_float(row.get("lot_size"), self.execution_cfg.lot_size)))
        if str(row.get("action") or "").upper() == "BUY":
            return max(rule_lot, row_lot)
        return row_lot

    @staticmethod
    def _qty_is_multiple(qty: float, multiple: int) -> bool:
        lot = max(1, int(multiple or 1))
        if lot <= 1:
            return True
        qty_value = abs(_to_float(qty, 0.0))
        if qty_value <= 1e-9:
            return True
        nearest = round(qty_value / float(lot))
        return abs(qty_value - (nearest * float(lot))) <= 1e-6

    @staticmethod
    def _liquidity_bucket(row: Dict[str, Any]) -> str:
        adv_value = max(0.0, _to_float(row.get("avg_daily_dollar_volume"), 0.0))
        liquidity_score = max(0.0, _to_float(row.get("liquidity_score"), 0.0))
        expected_cost_bps = max(0.0, _to_float(row.get("expected_cost_bps"), 0.0))
        if adv_value >= 20_000_000.0 and liquidity_score >= 0.75 and expected_cost_bps <= 12.0:
            return "DEEP"
        if adv_value >= 3_000_000.0 and liquidity_score >= 0.45 and expected_cost_bps <= 24.0:
            return "CORE"
        if adv_value <= 750_000.0 or liquidity_score <= 0.30 or expected_cost_bps >= 45.0:
            return "STRESSED"
        return "THIN"

    def _dynamic_execution_context(
        self,
        row: Dict[str, Any],
        *,
        session: ExecutionSessionProfile,
    ) -> Dict[str, Any]:
        is_entry = self._is_long_entry_order(row) or self._is_short_entry_order(row)
        liquidity_bucket = self._liquidity_bucket(row)
        adv_value = max(0.0, _to_float(row.get("avg_daily_dollar_volume"), 0.0))
        order_value = abs(_to_float(row.get("order_value"), 0.0))
        order_adv_pct = float(order_value / max(adv_value, 1e-9)) if adv_value > 0.0 else 0.0
        order_rules = self.market_structure.order_rules
        buy_lot_multiple = self._effective_buy_lot_multiple(row)
        odd_lot_discount_risk = bool(order_rules.odd_lot_discount_risk)
        price_limit_pct = max(0.0, float(order_rules.price_limit_pct or 0.0))
        day_turnaround_allowed = bool(order_rules.day_turnaround_allowed)
        market_rule_notes: List[str] = []

        session_edge_add = {"OPEN": 2.0, "MIDDAY": 0.0, "CLOSE": 1.0}.get(session.session_bucket, 0.0)
        liquidity_edge_add = {"DEEP": 0.0, "CORE": 1.5, "THIN": 4.0, "STRESSED": 8.0}.get(liquidity_bucket, 2.5)
        liquidity_buffer_add = {"DEEP": 0.0, "CORE": 0.5, "THIN": 2.0, "STRESSED": 4.0}.get(liquidity_bucket, 1.0)

        size_edge_add = 0.0
        size_buffer_add = 0.0
        if order_adv_pct >= 0.05:
            size_edge_add = 8.0
            size_buffer_add = 4.0
            market_rule_notes.append("order_gt_5pct_adv")
        elif order_adv_pct >= 0.02:
            size_edge_add = 5.0
            size_buffer_add = 2.5
            market_rule_notes.append("order_gt_2pct_adv")
        elif order_adv_pct >= 0.01:
            size_edge_add = 2.0
            size_buffer_add = 1.0
            market_rule_notes.append("order_gt_1pct_adv")

        market_rule_edge_add = 0.0
        market_rule_buffer_add = 0.0
        if odd_lot_discount_risk:
            market_rule_edge_add += 1.5
            market_rule_buffer_add += 1.0
            market_rule_notes.append("odd_lot_discount_risk")
        if price_limit_pct > 0.0:
            market_rule_edge_add += min(6.0, price_limit_pct * 0.4)
            market_rule_buffer_add += min(3.0, price_limit_pct * 0.2)
            market_rule_notes.append("price_limit_market")
        if is_entry and not day_turnaround_allowed:
            market_rule_edge_add += 2.0
            market_rule_buffer_add += 1.0
            market_rule_notes.append("no_day_turnaround")
        if buy_lot_multiple >= 100 and str(row.get("action") or "").upper() == "BUY":
            market_rule_edge_add += 1.5
            market_rule_buffer_add += 0.5
            market_rule_notes.append("board_lot_buy")

        dynamic_adv_scale = {"DEEP": 1.00, "CORE": 0.90, "THIN": 0.70, "STRESSED": 0.50}.get(liquidity_bucket, 0.75)
        dynamic_split_scale = {"DEEP": 1.00, "CORE": 0.85, "THIN": 0.65, "STRESSED": 0.50}.get(liquidity_bucket, 0.70)
        dynamic_limit_buffer_scale = 1.0 + {"OPEN": 0.15, "MIDDAY": 0.0, "CLOSE": 0.10}.get(session.session_bucket, 0.0)
        dynamic_limit_buffer_scale += {"DEEP": 0.0, "CORE": 0.05, "THIN": 0.20, "STRESSED": 0.35}.get(liquidity_bucket, 0.10)
        dynamic_force_min_slices = 1

        if odd_lot_discount_risk:
            dynamic_adv_scale -= 0.10
            dynamic_split_scale -= 0.10
            dynamic_limit_buffer_scale += 0.10
            dynamic_force_min_slices = max(dynamic_force_min_slices, 2)
        if price_limit_pct > 0.0:
            dynamic_adv_scale -= 0.10
            dynamic_split_scale -= 0.15
            dynamic_limit_buffer_scale += 0.20
            dynamic_force_min_slices = max(dynamic_force_min_slices, 2)
        if is_entry and not day_turnaround_allowed:
            dynamic_adv_scale -= 0.05
            dynamic_split_scale -= 0.05
            dynamic_limit_buffer_scale += 0.08
        if order_adv_pct >= 0.02:
            dynamic_adv_scale -= 0.15
            dynamic_split_scale -= 0.15
            dynamic_limit_buffer_scale += 0.10
            dynamic_force_min_slices += 1
        elif order_adv_pct >= 0.01:
            dynamic_adv_scale -= 0.08
            dynamic_split_scale -= 0.08
            dynamic_limit_buffer_scale += 0.05
        if liquidity_bucket == "THIN":
            dynamic_force_min_slices = max(dynamic_force_min_slices, 2)
        elif liquidity_bucket == "STRESSED":
            dynamic_force_min_slices = max(dynamic_force_min_slices, 3)

        dynamic_adv_scale = max(0.20, min(1.05, float(dynamic_adv_scale)))
        dynamic_split_scale = max(0.15, min(1.05, float(dynamic_split_scale)))
        dynamic_limit_buffer_scale = max(1.0, float(dynamic_limit_buffer_scale))
        dynamic_force_min_slices = int(min(max(1, int(self.execution_cfg.max_slices_per_symbol or 1)), dynamic_force_min_slices))
        dynamic_prefer_limit_order = bool(
            odd_lot_discount_risk
            or price_limit_pct > 0.0
            or liquidity_bucket in {"THIN", "STRESSED"}
        )
        base_min_expected_edge_bps = max(0.0, float(self.execution_cfg.min_expected_edge_bps or 0.0))
        base_edge_cost_buffer_bps = max(0.0, float(self.execution_cfg.edge_cost_buffer_bps or 0.0))
        dynamic_edge_floor_bps = float(
            base_min_expected_edge_bps + session_edge_add + liquidity_edge_add + size_edge_add + market_rule_edge_add
        )
        dynamic_edge_buffer_bps = float(
            base_edge_cost_buffer_bps + liquidity_buffer_add + size_buffer_add + market_rule_buffer_add
        )
        return {
            "dynamic_liquidity_bucket": liquidity_bucket,
            "dynamic_order_adv_pct": float(order_adv_pct),
            "market_rule_buy_lot_multiple": int(buy_lot_multiple),
            "market_rule_odd_lot_discount_risk": bool(odd_lot_discount_risk),
            "market_rule_day_turnaround_allowed": bool(day_turnaround_allowed),
            "market_rule_price_limit_pct": float(price_limit_pct),
            "market_rule_research_only": bool(self.market_structure.research_only),
            "dynamic_market_rule_notes": ",".join(market_rule_notes),
            "dynamic_edge_floor_bps": float(dynamic_edge_floor_bps),
            "dynamic_edge_buffer_bps": float(dynamic_edge_buffer_bps),
            "dynamic_adv_scale": float(dynamic_adv_scale),
            "dynamic_split_trigger_scale": float(dynamic_split_scale),
            "dynamic_limit_buffer_scale": float(dynamic_limit_buffer_scale),
            "dynamic_force_min_slices": int(dynamic_force_min_slices),
            "dynamic_prefer_limit_order": bool(dynamic_prefer_limit_order),
            "dynamic_session_edge_add_bps": float(session_edge_add),
            "dynamic_liquidity_edge_add_bps": float(liquidity_edge_add),
            "dynamic_market_rule_edge_add_bps": float(market_rule_edge_add),
            "dynamic_size_edge_add_bps": float(size_edge_add),
            "dynamic_liquidity_buffer_add_bps": float(liquidity_buffer_add),
            "dynamic_market_rule_buffer_add_bps": float(market_rule_buffer_add),
            "dynamic_size_buffer_add_bps": float(size_buffer_add),
        }

    def _apply_market_rule_gates(
        self,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        session = self._current_execution_session_profile()
        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        for row in order_rows:
            enriched_row = dict(row)
            enriched_row.update(self._dynamic_execution_context(enriched_row, session=session))

            if self.market_structure.research_only and (self._is_long_entry_order(enriched_row) or self._is_short_entry_order(enriched_row)):
                blocked_row = dict(enriched_row)
                blocked_row["status"] = "BLOCKED_MARKET_RULE"
                blocked_row["market_rule_status"] = "BLOCKED_RESEARCH_ONLY"
                blocked_row["market_rule_reason"] = "当前市场仍是 research-only，执行链只保留研究输出。"
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|market_rule_research_only".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            buy_lot_multiple = int(enriched_row.get("market_rule_buy_lot_multiple", 1) or 1)
            if str(enriched_row.get("action") or "").upper() == "BUY" and not self._qty_is_multiple(enriched_row.get("delta_qty"), buy_lot_multiple):
                blocked_row = dict(enriched_row)
                blocked_row["status"] = "BLOCKED_MARKET_RULE"
                blocked_row["market_rule_status"] = "BLOCKED_BOARD_LOT"
                blocked_row["market_rule_reason"] = (
                    f"买入数量 {float(_to_float(enriched_row.get('delta_qty'), 0.0)):.4f} 不是 board lot {buy_lot_multiple} 的整数倍。"
                )
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|market_rule_board_lot".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            if self._is_short_entry_order(enriched_row) and not bool(enriched_row.get("market_rule_day_turnaround_allowed", True)):
                blocked_row = dict(enriched_row)
                blocked_row["status"] = "BLOCKED_MARKET_RULE"
                blocked_row["market_rule_status"] = "BLOCKED_SHORT_ENTRY"
                blocked_row["market_rule_reason"] = "当前市场规则不支持这类自动 short entry / same-day turnaround 行为。"
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|market_rule_short_entry".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            enriched_row["market_rule_status"] = "RULES_OK"
            enriched_row["market_rule_reason"] = (
                f"liquidity={str(enriched_row.get('dynamic_liquidity_bucket') or '-')} "
                f"adv={float(enriched_row.get('dynamic_order_adv_pct', 0.0) or 0.0):.3f} "
                f"notes={str(enriched_row.get('dynamic_market_rule_notes') or '-')}"
            )
            filtered.append(enriched_row)
        return filtered, blocked

    def _split_ratio_weights(self, slice_count: int, session: ExecutionSessionProfile) -> List[float]:
        count = max(1, int(slice_count))
        if count == 1:
            return [1.0]
        if session.session_bucket == "OPEN":
            # 开盘更偏向 TWAP：先把大单拆散，减少第一分钟冲击。
            weights = [1.0 for _ in range(count)]
        elif session.session_bucket == "CLOSE":
            # 尾盘时间更短，允许前几笔更积极一些，避免尾盘来不及成交。
            weights = [float(count - idx) for idx in range(count)]
        else:
            # 午盘流动性相对平稳，做一个近似的 VWAP-lite 中间偏重分布。
            center = (count - 1) / 2.0
            weights = [max(0.6, 1.2 - abs(idx - center) * 0.35) for idx in range(count)]
        total = sum(weights) or 1.0
        return [float(weight / total) for weight in weights]

    def _build_order_details_payload(self, row: Dict[str, Any], *, submitted: bool) -> Dict[str, Any]:
        return {
            "submitted": bool(submitted),
            "parent_order_key": str(row.get("parent_order_key", "") or ""),
            "user_reason_label": str(row.get("user_reason_label", "") or ""),
            "user_reason": str(row.get("user_reason", "") or ""),
            "quality_status": row.get("quality_status", ""),
            "quality_reason": row.get("quality_reason", ""),
            "manual_review_status": row.get("manual_review_status", ""),
            "manual_review_reason": row.get("manual_review_reason", ""),
            "shadow_review_status": row.get("shadow_review_status", ""),
            "shadow_review_reason": row.get("shadow_review_reason", ""),
            "opportunity_status": row.get("opportunity_status", ""),
            "opportunity_reason": row.get("opportunity_reason", ""),
            "priority_score": float(row.get("priority_score", 0.0) or 0.0),
            "score": float(row.get("score", 0.0) or 0.0),
            "score_before_cost": float(row.get("score_before_cost", row.get("score", 0.0)) or 0.0),
            "execution_score": float(row.get("execution_score", 0.0) or 0.0),
            "expected_edge_threshold": float(row.get("expected_edge_threshold", 0.0) or 0.0),
            "expected_edge_score": float(row.get("expected_edge_score", 0.0) or 0.0),
            "expected_edge_bps": float(row.get("expected_edge_bps", 0.0) or 0.0),
            "edge_gate_threshold_bps": float(row.get("edge_gate_threshold_bps", 0.0) or 0.0),
            "edge_gate_base_min_expected_edge_bps": float(row.get("edge_gate_base_min_expected_edge_bps", 0.0) or 0.0),
            "edge_gate_dynamic_floor_bps": float(row.get("edge_gate_dynamic_floor_bps", 0.0) or 0.0),
            "edge_gate_base_buffer_bps": float(row.get("edge_gate_base_buffer_bps", 0.0) or 0.0),
            "edge_gate_dynamic_buffer_bps": float(row.get("edge_gate_dynamic_buffer_bps", 0.0) or 0.0),
            "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
            "spread_proxy_bps": float(row.get("spread_proxy_bps", 0.0) or 0.0),
            "slippage_proxy_bps": float(row.get("slippage_proxy_bps", 0.0) or 0.0),
            "commission_proxy_bps": float(row.get("commission_proxy_bps", 0.0) or 0.0),
            "expected_cost_value": float(row.get("expected_cost_value", 0.0) or 0.0),
            "expected_spread_cost": float(row.get("expected_spread_cost", 0.0) or 0.0),
            "expected_slippage_cost": float(row.get("expected_slippage_cost", 0.0) or 0.0),
            "expected_commission_cost": float(row.get("expected_commission_cost", 0.0) or 0.0),
            "avg_daily_volume": float(row.get("avg_daily_volume", 0.0) or 0.0),
            "avg_daily_dollar_volume": float(row.get("avg_daily_dollar_volume", 0.0) or 0.0),
            "execution_style": str(row.get("execution_style", "") or ""),
            "session_bucket": str(row.get("session_bucket", "") or ""),
            "session_label": str(row.get("session_label", "") or ""),
            "execution_aggressiveness": float(row.get("execution_aggressiveness", 0.0) or 0.0),
            "dynamic_liquidity_bucket": str(row.get("dynamic_liquidity_bucket", "") or ""),
            "dynamic_order_adv_pct": float(row.get("dynamic_order_adv_pct", 0.0) or 0.0),
            "dynamic_market_rule_notes": str(row.get("dynamic_market_rule_notes", "") or ""),
            "dynamic_adv_scale": float(row.get("dynamic_adv_scale", 0.0) or 0.0),
            "dynamic_split_trigger_scale": float(row.get("dynamic_split_trigger_scale", 0.0) or 0.0),
            "dynamic_limit_buffer_scale": float(row.get("dynamic_limit_buffer_scale", 0.0) or 0.0),
            "dynamic_force_min_slices": int(row.get("dynamic_force_min_slices", 1) or 1),
            "dynamic_prefer_limit_order": bool(row.get("dynamic_prefer_limit_order", False)),
            "market_rule_status": str(row.get("market_rule_status", "") or ""),
            "market_rule_reason": str(row.get("market_rule_reason", "") or ""),
            "market_rule_buy_lot_multiple": int(row.get("market_rule_buy_lot_multiple", 1) or 1),
            "market_rule_price_limit_pct": float(row.get("market_rule_price_limit_pct", 0.0) or 0.0),
            "market_rule_day_turnaround_allowed": bool(row.get("market_rule_day_turnaround_allowed", True)),
            "market_rule_research_only": bool(row.get("market_rule_research_only", False)),
            "adv_participation_pct": float(row.get("adv_participation_pct", 0.0) or 0.0),
            "adv_cap_order_value": float(row.get("adv_cap_order_value", 0.0) or 0.0),
            "adv_capped": bool(row.get("adv_capped", False)),
            "risk_alert_applied": bool(row.get("risk_alert_applied", False)),
            "risk_alert_status": str(row.get("risk_alert_status", "") or ""),
            "risk_alert_level": str(row.get("risk_alert_level", "") or ""),
            "risk_alert_trend_label": str(row.get("risk_alert_trend_label", "") or ""),
            "risk_alert_reason": str(row.get("risk_alert_reason", "") or ""),
            "hotspot_penalty_applied": bool(row.get("hotspot_penalty_applied", False)),
            "hotspot_penalty_status": str(row.get("hotspot_penalty_status", "") or ""),
            "hotspot_penalty_reason": str(row.get("hotspot_penalty_reason", "") or ""),
            "hotspot_penalty_execution_penalty": float(row.get("hotspot_penalty_execution_penalty", 0.0) or 0.0),
            "hotspot_penalty_expected_cost_bps_add": float(row.get("hotspot_penalty_expected_cost_bps_add", 0.0) or 0.0),
            "hotspot_penalty_slippage_proxy_bps_add": float(row.get("hotspot_penalty_slippage_proxy_bps_add", 0.0) or 0.0),
            "hotspot_penalty_session_labels": str(row.get("hotspot_penalty_session_labels", "") or ""),
            "slice_count": int(row.get("slice_count", 1) or 1),
            "slice_index": int(row.get("slice_index", 1) or 1),
            "parent_order_value": float(row.get("parent_order_value", row.get("order_value", 0.0)) or 0.0),
            "execution_order_type": str(row.get("execution_order_type", "") or ""),
            "limit_price_buffer_bps_effective": float(row.get("limit_price_buffer_bps_effective", 0.0) or 0.0),
        }

    def _build_execution_order_storage_row(
        self,
        *,
        run_id: str,
        row: Dict[str, Any],
        broker_order_id: int,
        status: str,
        details_payload: Dict[str, Any],
        execution_intent_json: str,
    ) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "market": self.market,
            "portfolio_id": self.portfolio_id,
            "symbol": row["symbol"],
            "action": row["action"],
            "current_qty": float(row.get("current_qty") or 0.0),
            "target_qty": float(row.get("target_qty") or 0.0),
            "delta_qty": float(row.get("delta_qty") or 0.0),
            "ref_price": float(row.get("ref_price") or 0.0),
            "target_weight": float(row.get("target_weight") or 0.0),
            "order_value": float(row.get("order_value") or 0.0),
            "order_type": str(row.get("execution_order_type") or self.execution_cfg.order_type),
            "broker_order_id": int(broker_order_id),
            "status": str(status or row.get("status") or "PLANNED"),
            "reason": str(row.get("reason") or ""),
            "score_before_cost": float(row.get("score_before_cost", row.get("score", 0.0)) or 0.0),
            "expected_cost_bps": float(row.get("expected_cost_bps", 0.0) or 0.0),
            "expected_edge_threshold": float(row.get("expected_edge_threshold", 0.0) or 0.0),
            "expected_edge_score": float(row.get("expected_edge_score", 0.0) or 0.0),
            "expected_edge_bps": float(row.get("expected_edge_bps", 0.0) or 0.0),
            "edge_gate_threshold_bps": float(row.get("edge_gate_threshold_bps", 0.0) or 0.0),
            "session_bucket": str(row.get("session_bucket") or ""),
            "session_label": str(row.get("session_label") or ""),
            "execution_style": str(row.get("execution_style") or ""),
            "execution_intent_json": execution_intent_json,
            "details": json.dumps(details_payload, ensure_ascii=False),
        }

    def _apply_execution_hotspot_gates(
        self,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        penalty_map = self._execution_hotspot_penalty_map()
        if not penalty_map:
            return order_rows, []

        session = self._current_execution_session_profile()
        defer_buckets = self._execution_hotspot_defer_buckets()
        base_adv_scale = max(0.25, min(1.0, float(self.execution_cfg.execution_hotspot_adv_participation_scale or 0.70)))
        base_split_scale = max(0.25, min(1.0, float(self.execution_cfg.execution_hotspot_split_trigger_scale or 0.70)))
        base_limit_buffer_scale = max(1.0, float(self.execution_cfg.execution_hotspot_limit_buffer_scale or 1.25))
        base_force_min_slices = max(1, int(self.execution_cfg.execution_hotspot_force_min_slices or 1))
        max_slices = max(1, int(self.execution_cfg.max_slices_per_symbol or 1))
        defer_min_execution_penalty = max(0.0, float(self.execution_cfg.execution_hotspot_defer_min_execution_penalty or 0.0))
        defer_min_expected_cost_bps = max(0.0, float(self.execution_cfg.execution_hotspot_defer_min_expected_cost_bps_add or 0.0))

        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        for row in order_rows:
            if not (self._is_long_entry_order(row) or self._is_short_entry_order(row)):
                filtered.append(row)
                continue

            symbol = str(row.get("symbol") or "").upper().strip()
            penalty = dict(penalty_map.get(symbol) or {})
            if not penalty:
                filtered.append(row)
                continue

            execution_penalty = max(0.0, _to_float(penalty.get("execution_penalty"), 0.0))
            expected_cost_bps_add = max(0.0, _to_float(penalty.get("expected_cost_bps_add"), 0.0))
            slippage_proxy_bps_add = max(0.0, _to_float(penalty.get("slippage_proxy_bps_add"), 0.0))
            session_count = max(1, int(_to_float(penalty.get("session_count"), 0.0) or 1))
            session_labels = str(penalty.get("session_labels") or "").strip()
            penalty_reason = str(penalty.get("reason") or "execution_hotspot_penalty").strip()

            # 开盘/尾盘如果本周已经被反复证明成本过高，就直接延后到后续轮次；
            # 其他时段则保守执行，而不是简单拉黑。
            should_defer = (
                session.session_bucket in defer_buckets
                and (
                    execution_penalty >= defer_min_execution_penalty
                    or expected_cost_bps_add >= defer_min_expected_cost_bps
                )
            )
            if should_defer:
                blocked_row = dict(row)
                blocked_row["status"] = "DEFERRED_EXECUTION_HOTSPOT"
                blocked_row["hotspot_penalty_applied"] = True
                blocked_row["hotspot_penalty_status"] = "DEFERRED"
                blocked_row["hotspot_penalty_reason"] = (
                    f"{session.session_label}存在重复执行热点，延后到后续轮次再执行。"
                )
                blocked_row["hotspot_penalty_execution_penalty"] = float(execution_penalty)
                blocked_row["hotspot_penalty_expected_cost_bps_add"] = float(expected_cost_bps_add)
                blocked_row["hotspot_penalty_slippage_proxy_bps_add"] = float(slippage_proxy_bps_add)
                blocked_row["hotspot_penalty_session_labels"] = session_labels
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|execution_hotspot_defer".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            penalty_strength = min(0.35, execution_penalty)
            slowed_row = dict(row)
            slowed_row["hotspot_penalty_applied"] = True
            slowed_row["hotspot_penalty_status"] = "SLOWED"
            slowed_row["hotspot_penalty_reason"] = (
                f"本周执行热点要求降低参与率并增加拆单，原因={penalty_reason or 'execution_hotspot'}。"
            )
            slowed_row["hotspot_penalty_execution_penalty"] = float(execution_penalty)
            slowed_row["hotspot_penalty_expected_cost_bps_add"] = float(expected_cost_bps_add)
            slowed_row["hotspot_penalty_slippage_proxy_bps_add"] = float(slippage_proxy_bps_add)
            slowed_row["hotspot_penalty_session_labels"] = session_labels
            annotate_execution_user_explanation(slowed_row)
            slowed_row["hotspot_adv_scale"] = round(max(0.25, min(1.0, base_adv_scale - penalty_strength * 0.75)), 6)
            slowed_row["hotspot_split_trigger_scale"] = round(max(0.20, min(1.0, base_split_scale - penalty_strength * 0.55)), 6)
            slowed_row["hotspot_limit_buffer_scale"] = round(
                max(1.0, base_limit_buffer_scale + min(0.40, expected_cost_bps_add / 30.0)),
                6,
            )
            slowed_row["hotspot_force_min_slices"] = int(
                min(max_slices, max(base_force_min_slices, 1 + min(session_count, 3)))
            )
            slowed_row["hotspot_force_limit_order"] = bool(self.execution_cfg.execution_hotspot_force_limit_order)
            slowed_row["reason"] = f"{str(row.get('reason') or '')}|execution_hotspot_slowdown".strip("|")
            filtered.append(slowed_row)
        return filtered, blocked

    def _split_execution_orders(
        self,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        session = self._current_execution_session_profile()
        expanded: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        # 这里先做“流动性约束 + 分片计划”，把大单变成更容易成交、也更容易复盘的执行子单。
        for parent_index, row in enumerate(order_rows, start=1):
            ref_price = max(0.0, _to_float(row.get("ref_price"), 0.0))
            delta_qty = abs(_to_float(row.get("delta_qty"), 0.0))
            parent_order_key = f"{self.market}:{self.portfolio_id}:parent:{parent_index}"
            if ref_price <= 0.0 or delta_qty <= 0.0:
                blocked_row = dict(row)
                blocked_row["parent_order_key"] = parent_order_key
                blocked_row["status"] = "BLOCKED_LIQUIDITY"
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|invalid_price_or_qty".strip("|")
                blocked.append(blocked_row)
                continue

            lot_size = max(
                1,
                int(
                    _to_float(
                        row.get("market_rule_buy_lot_multiple"),
                        _to_float(row.get("lot_size"), self.execution_cfg.lot_size),
                    )
                ),
            )
            adv_value = max(0.0, _to_float(row.get("avg_daily_dollar_volume"), 0.0))
            adv_cap_order_value = 0.0
            adv_participation_pct = 0.0
            dynamic_adv_scale = max(0.20, _to_float(row.get("dynamic_adv_scale"), 1.0))
            dynamic_split_trigger_scale = max(0.15, _to_float(row.get("dynamic_split_trigger_scale"), 1.0))
            dynamic_limit_buffer_scale = max(1.0, _to_float(row.get("dynamic_limit_buffer_scale"), 1.0))
            dynamic_force_min_slices = max(1, int(_to_float(row.get("dynamic_force_min_slices"), 1)))
            dynamic_prefer_limit_order = bool(row.get("dynamic_prefer_limit_order", False))
            hotspot_adv_scale = max(0.10, _to_float(row.get("hotspot_adv_scale"), 1.0))
            hotspot_split_trigger_scale = max(0.10, _to_float(row.get("hotspot_split_trigger_scale"), 1.0))
            hotspot_limit_buffer_scale = max(1.0, _to_float(row.get("hotspot_limit_buffer_scale"), 1.0))
            hotspot_force_min_slices = max(1, int(_to_float(row.get("hotspot_force_min_slices"), 1)))
            hotspot_force_limit_order = bool(row.get("hotspot_force_limit_order", False))
            risk_alert_adv_scale = max(0.10, _to_float(row.get("risk_alert_adv_scale"), 1.0))
            risk_alert_split_trigger_scale = max(0.10, _to_float(row.get("risk_alert_split_trigger_scale"), 1.0))
            risk_alert_limit_buffer_scale = max(1.0, _to_float(row.get("risk_alert_limit_buffer_scale"), 1.0))
            risk_alert_force_min_slices = max(1, int(_to_float(row.get("risk_alert_force_min_slices"), 1)))
            risk_alert_force_limit_order = bool(row.get("risk_alert_force_limit_order", False))
            if adv_value > 0.0:
                adv_cap_order_value = float(
                    adv_value
                    * max(0.0, _to_float(self.execution_cfg.adv_max_participation_pct, 0.05))
                    * max(0.10, float(session.participation_scale))
                    * float(dynamic_adv_scale)
                    * min(float(hotspot_adv_scale), float(risk_alert_adv_scale))
                )
                adv_participation_pct = float(abs(_to_float(row.get("order_value"), 0.0)) / max(adv_value, 1e-9))

            capped_order_value = abs(_to_float(row.get("order_value"), 0.0))
            adv_capped = False
            if adv_cap_order_value > 0.0:
                capped_order_value = min(capped_order_value, adv_cap_order_value)
                adv_capped = capped_order_value + 1e-9 < abs(_to_float(row.get("order_value"), 0.0))

            capped_qty = delta_qty
            if adv_capped:
                capped_qty = self._round_split_qty(
                    capped_order_value / ref_price,
                    lot_size,
                    allow_fractional=bool(self.execution_cfg.allow_fractional_qty),
                    decimals=int(self.execution_cfg.fractional_qty_decimals or 0),
                )
                capped_order_value = float(capped_qty * ref_price)
            if capped_qty <= 0.0 or capped_order_value < float(self.execution_cfg.min_trade_value):
                blocked_row = dict(row)
                blocked_row["parent_order_key"] = parent_order_key
                blocked_row["status"] = "BLOCKED_LIQUIDITY"
                blocked_row["adv_capped"] = bool(adv_capped)
                blocked_row["adv_cap_order_value"] = float(adv_cap_order_value)
                blocked_row["adv_participation_pct"] = float(adv_participation_pct)
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|adv_liquidity_cap".strip("|")
                blocked.append(blocked_row)
                continue

            split_trigger_value = 0.0
            if adv_value > 0.0:
                split_trigger_value = float(
                    adv_value
                    * max(0.0, _to_float(self.execution_cfg.adv_split_trigger_pct, 0.02))
                    * max(0.10, float(session.participation_scale))
                    * float(dynamic_split_trigger_scale)
                    * min(float(hotspot_split_trigger_scale), float(risk_alert_split_trigger_scale))
                )
            slice_count = 1
            if split_trigger_value > 0.0 and capped_order_value > split_trigger_value + 1e-9:
                slice_count = int(min(
                    max(2, int((capped_order_value / max(split_trigger_value, 1e-9)) + 0.9999)),
                    max(1, int(self.execution_cfg.max_slices_per_symbol or 1)),
                ))
            slice_count = int(min(max(1, int(self.execution_cfg.max_slices_per_symbol or 1)), max(slice_count, dynamic_force_min_slices)))
            if bool(row.get("hotspot_penalty_applied", False)):
                slice_count = int(min(max(1, int(self.execution_cfg.max_slices_per_symbol or 1)), max(slice_count, hotspot_force_min_slices)))
            if bool(row.get("risk_alert_applied", False)):
                slice_count = int(min(max(1, int(self.execution_cfg.max_slices_per_symbol or 1)), max(slice_count, risk_alert_force_min_slices)))
            slice_ratios = self._split_ratio_weights(slice_count, session)
            remaining_qty = float(capped_qty)
            signed_direction = 1.0 if str(row.get("action") or "").upper() == "BUY" else -1.0
            cumulative_qty = 0.0
            base_buffer_bps = (
                float(self.execution_cfg.limit_price_buffer_bps or 0.0)
                * float(session.limit_buffer_scale)
                * float(dynamic_limit_buffer_scale)
                * max(float(hotspot_limit_buffer_scale), float(risk_alert_limit_buffer_scale))
            )
            execution_order_type = str(self.execution_cfg.order_type or "MKT").upper()
            if (
                dynamic_prefer_limit_order
                or hotspot_force_limit_order
                or risk_alert_force_limit_order
                or (slice_count > 1 and bool(self.execution_cfg.prefer_limit_orders_for_sliced_execution))
            ):
                execution_order_type = "LMT"

            for idx, ratio in enumerate(slice_ratios, start=1):
                if idx == slice_count:
                    child_abs_qty = float(remaining_qty)
                else:
                    child_abs_qty = self._round_split_qty(
                        capped_qty * ratio,
                        lot_size,
                        allow_fractional=bool(self.execution_cfg.allow_fractional_qty),
                        decimals=int(self.execution_cfg.fractional_qty_decimals or 0),
                    )
                    child_abs_qty = min(child_abs_qty, remaining_qty)
                if child_abs_qty <= 0.0:
                    continue
                remaining_qty = max(0.0, remaining_qty - child_abs_qty)
                signed_child_qty = float(child_abs_qty * signed_direction)
                cumulative_qty += signed_child_qty
                child_row = dict(row)
                child_row["parent_order_key"] = parent_order_key
                child_row["delta_qty"] = abs(float(child_abs_qty))
                child_row["order_value"] = float(abs(child_abs_qty) * ref_price)
                child_row["target_qty"] = float(_to_float(row.get("current_qty"), 0.0) + cumulative_qty)
                child_row["execution_style"] = session.execution_style
                child_row["session_bucket"] = session.session_bucket
                child_row["session_label"] = session.session_label
                child_row["execution_aggressiveness"] = float(session.aggressiveness)
                child_row["adv_participation_pct"] = float(
                    child_row["order_value"] / max(adv_value, 1e-9) if adv_value > 0.0 else 0.0
                )
                child_row["adv_cap_order_value"] = float(adv_cap_order_value)
                child_row["adv_capped"] = bool(adv_capped)
                child_row["risk_alert_applied"] = bool(row.get("risk_alert_applied", False))
                child_row["risk_alert_status"] = str(row.get("risk_alert_status", "") or "")
                child_row["risk_alert_level"] = str(row.get("risk_alert_level", "") or "")
                child_row["risk_alert_trend_label"] = str(row.get("risk_alert_trend_label", "") or "")
                child_row["risk_alert_reason"] = str(row.get("risk_alert_reason", "") or "")
                child_row["hotspot_penalty_applied"] = bool(row.get("hotspot_penalty_applied", False))
                child_row["hotspot_penalty_status"] = str(row.get("hotspot_penalty_status", "") or "")
                child_row["hotspot_penalty_reason"] = str(row.get("hotspot_penalty_reason", "") or "")
                child_row["hotspot_penalty_execution_penalty"] = float(row.get("hotspot_penalty_execution_penalty", 0.0) or 0.0)
                child_row["hotspot_penalty_expected_cost_bps_add"] = float(row.get("hotspot_penalty_expected_cost_bps_add", 0.0) or 0.0)
                child_row["hotspot_penalty_slippage_proxy_bps_add"] = float(row.get("hotspot_penalty_slippage_proxy_bps_add", 0.0) or 0.0)
                child_row["hotspot_penalty_session_labels"] = str(row.get("hotspot_penalty_session_labels", "") or "")
                child_row["slice_count"] = int(slice_count)
                child_row["slice_index"] = int(idx)
                child_row["parent_order_value"] = float(_to_float(row.get("order_value"), 0.0))
                child_row["execution_order_type"] = execution_order_type
                child_row["limit_price_buffer_bps_effective"] = round(float(base_buffer_bps * (0.85 + session.aggressiveness * 0.35)), 6)
                child_row.update(self._cost_breakdown(child_row, order_value=child_row["order_value"]))
                child_row["reason"] = f"{str(row.get('reason') or '')}|{session.execution_style.lower()}".strip("|")
                expanded.append(child_row)
        return expanded, blocked

    def _intent_from_row(self, row: Dict[str, Any]) -> ExecutionIntent:
        reason = str(row.get("reason") or "").strip()
        return ExecutionIntent(
            symbol=str(row.get("symbol") or "").upper(),
            market=self.market,
            action=str(row.get("action") or "").upper(),
            current_qty=float(row.get("current_qty") or 0.0),
            target_qty=float(row.get("target_qty") or 0.0),
            delta_qty=float(row.get("delta_qty") or 0.0),
            target_weight=float(row.get("target_weight") or 0.0),
            ref_price=float(row.get("ref_price") or 0.0),
            order_value=float(row.get("order_value") or 0.0),
            status=str(row.get("status") or "PLANNED"),
            reasons=[part for part in reason.split("|") if part],
            opportunity_status=str(row.get("opportunity_status") or ""),
            opportunity_reason=str(row.get("opportunity_reason") or ""),
            metadata={
                "portfolio_id": self.portfolio_id,
                "quality_status": str(row.get("quality_status") or ""),
                "quality_reason": str(row.get("quality_reason") or ""),
                "manual_review_status": str(row.get("manual_review_status") or ""),
                "manual_review_reason": str(row.get("manual_review_reason") or ""),
                "shadow_review_status": str(row.get("shadow_review_status") or ""),
                "shadow_review_reason": str(row.get("shadow_review_reason") or ""),
                "risk_alert_status": str(row.get("risk_alert_status") or ""),
                "risk_alert_reason": str(row.get("risk_alert_reason") or ""),
                "hotspot_penalty_status": str(row.get("hotspot_penalty_status") or ""),
                "hotspot_penalty_reason": str(row.get("hotspot_penalty_reason") or ""),
                "market_rule_status": str(row.get("market_rule_status") or ""),
                "market_rule_reason": str(row.get("market_rule_reason") or ""),
            },
        )

    def _normalize_broker_symbol(self, contract) -> str:
        symbol = str(getattr(contract, "symbol", "") or "").upper().strip()
        exchange = str(getattr(contract, "exchange", "") or "").upper().strip()
        currency = str(getattr(contract, "currency", "") or "").upper().strip()
        if self.market == "HK" or exchange == "SEHK" or currency == "HKD":
            if symbol.isdigit():
                return f"{int(symbol):04d}.HK"
            if symbol.endswith(".HK"):
                return symbol
            return f"{symbol}.HK"
        if " " in symbol:
            parts = symbol.split()
            if len(parts) == 2 and len(parts[1]) == 1 and parts[1].isalpha():
                return f"{parts[0]}.{parts[1]}"
        return symbol

    def _account_snapshot(self) -> Dict[str, float]:
        account_id = self._require_valid_account_id()
        cached = self.storage.get_latest_account_snapshot(
            account_id,
            max_age_sec=int(self.execution_cfg.account_snapshot_ttl_sec or 0),
        )
        if cached:
            return {
                "netliq": float(cached.get("netliq", 0.0) or 0.0),
                "cash": float(cached.get("cash", 0.0) or 0.0),
                "buying_power": float(cached.get("buying_power", 0.0) or 0.0),
            }

        stale_cached = self.storage.get_latest_account_snapshot(account_id)
        rows = []
        try:
            rows = self.ib.accountSummary(account_id)
        except Exception:
            rows = []
        if not rows:
            rows = self.ib.accountSummary()
        usable_rows = [row for row in rows if str(getattr(row, "account", "") or "").strip() == account_id]
        if not usable_rows:
            visible_accounts = sorted({str(getattr(row, "account", "") or "").strip() for row in rows if str(getattr(row, "account", "") or "").strip()})
            if stale_cached:
                self.storage.insert_risk_event(
                    "ACCOUNT_SNAPSHOT_STALE_FALLBACK",
                    1.0,
                    f"accountSummary missing configured account_id={account_id}; using stale snapshot visible_accounts={visible_accounts}",
                    portfolio_id=self.portfolio_id,
                    system_kind="investment_execution",
                )
                return {
                    "netliq": float(stale_cached.get("netliq", 0.0) or 0.0),
                    "cash": float(stale_cached.get("cash", 0.0) or 0.0),
                    "buying_power": float(stale_cached.get("buying_power", 0.0) or 0.0),
                }
            raise ValueError(
                f"Configured IBKR account_id={account_id} was not found in accountSummary. Visible accounts={visible_accounts}"
            )
        tags: Dict[str, float] = {}
        for row in usable_rows:
            try:
                tags[str(row.tag)] = float(row.value)
            except Exception:
                continue
        snapshot = {
            "netliq": float(tags.get("NetLiquidation", 0.0) or 0.0),
            "cash": float(tags.get("TotalCashValue", tags.get("AvailableFunds", 0.0)) or 0.0),
            "buying_power": float(tags.get("BuyingPower", 0.0) or 0.0),
        }
        self.storage.insert_account_snapshot(
            {
                "account_id": account_id,
                "netliq": float(snapshot["netliq"]),
                "cash": float(snapshot["cash"]),
                "buying_power": float(snapshot["buying_power"]),
                "details": {"source": "ibkr_accountSummary", "market": self.market, "portfolio_id": self.portfolio_id},
            }
        )
        return snapshot

    def _broker_positions(self) -> Dict[str, Dict[str, Any]]:
        account_id = self._require_valid_account_id()
        out: Dict[str, Dict[str, Any]] = {}
        market_filter = str(self.market or "").upper()
        try:
            rows = list(self.ib.portfolio(account_id))
        except Exception:
            rows = []
        if not rows:
            try:
                rows = list(self.ib.portfolio())
            except Exception:
                rows = []
        if rows:
            matched_rows = [row for row in rows if str(getattr(row, "account", "") or "").strip() == account_id]
            if not matched_rows:
                visible_accounts = sorted({str(getattr(row, "account", "") or "").strip() for row in rows if str(getattr(row, "account", "") or "").strip()})
                raise ValueError(
                    f"Configured IBKR account_id={account_id} was not found in portfolio rows. Visible accounts={visible_accounts}"
                )
            for row in matched_rows:
                symbol = self._normalize_broker_symbol(row.contract)
                if market_filter and market_filter != "DEFAULT" and not symbol_matches_market(symbol, market_filter):
                    continue
                out[symbol] = {
                    "qty": _to_float(getattr(row, "position", 0.0)),
                    "avg_cost": _to_float(getattr(row, "averageCost", 0.0)),
                    "market_price": _to_float(getattr(row, "marketPrice", 0.0)),
                    "market_value": _to_float(getattr(row, "marketValue", 0.0)),
                }
            return out

        fallback_rows = list(self.ib.positions())
        if not fallback_rows:
            return out
        matched_rows = [row for row in fallback_rows if str(getattr(row, "account", "") or "").strip() == account_id]
        if not matched_rows:
            visible_accounts = sorted({str(getattr(row, "account", "") or "").strip() for row in fallback_rows if str(getattr(row, "account", "") or "").strip()})
            raise ValueError(
                f"Configured IBKR account_id={account_id} was not found in positions rows. Visible accounts={visible_accounts}"
            )
        for row in matched_rows:
            symbol = self._normalize_broker_symbol(row.contract)
            if market_filter and market_filter != "DEFAULT" and not symbol_matches_market(symbol, market_filter):
                continue
            out[symbol] = {
                "qty": _to_float(getattr(row, "position", 0.0)),
                "avg_cost": _to_float(getattr(row, "avgCost", 0.0)),
                "market_price": 0.0,
                "market_value": 0.0,
            }
        return out

    def _target_qty_map(
        self,
        *,
        target_weights: Dict[str, float],
        price_map: Dict[str, float],
        investable_equity: float,
        lot_size_map: Dict[str, int],
    ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for symbol, weight in target_weights.items():
            price = _to_float(price_map.get(symbol), 0.0)
            if price <= 0:
                continue
            lot_size = max(1, int(lot_size_map.get(symbol, self.execution_cfg.lot_size)))
            raw_qty = abs(investable_equity * float(weight)) / price
            if bool(self.execution_cfg.allow_fractional_qty) and lot_size <= 1:
                precision = max(0, int(self.execution_cfg.fractional_qty_decimals or 0))
                scale = float(10**precision)
                qty = float(int(raw_qty * scale) / scale)
            else:
                qty = float(int(raw_qty // lot_size) * lot_size)
            if qty <= 0.0:
                continue
            out[str(symbol).upper()] = float(qty if float(weight) >= 0.0 else -qty)
        return out

    @staticmethod
    def _additional_gross_exposure_notional(
        *,
        current_qty: float,
        target_qty: float,
        ref_price: float,
    ) -> float:
        price = max(0.0, _to_float(ref_price, 0.0))
        if price <= 0.0:
            return 0.0
        current_abs = abs(_to_float(current_qty, 0.0))
        target_abs = abs(_to_float(target_qty, 0.0))
        if target_abs <= current_abs + 1e-9:
            return 0.0
        return float((target_abs - current_abs) * price)

    def _target_capital_gap(
        self,
        *,
        current_positions: Dict[str, Dict[str, Any]],
        target_qty_map: Dict[str, float],
        price_map: Dict[str, float],
    ) -> float:
        total = 0.0
        for symbol in sorted(set(current_positions) | set(target_qty_map)):
            current = dict(current_positions.get(symbol) or {})
            ref_price = _to_float(
                price_map.get(symbol),
                _to_float(current.get("market_price"), _to_float(current.get("last_price"), _to_float(current.get("avg_cost"), 0.0))),
            )
            total += self._additional_gross_exposure_notional(
                current_qty=_to_float(current.get("qty"), 0.0),
                target_qty=_to_float(target_qty_map.get(symbol), 0.0),
                ref_price=ref_price,
            )
        return float(total)

    def _planned_deployment_value(self, order_rows: List[Dict[str, Any]]) -> float:
        total = 0.0
        for row in order_rows:
            total += self._additional_gross_exposure_notional(
                current_qty=_to_float(row.get("current_qty"), 0.0),
                target_qty=_to_float(row.get("target_qty"), 0.0),
                ref_price=_to_float(row.get("ref_price"), 0.0),
            )
        return float(total)

    def _apply_expected_edge_gates(
        self,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if not bool(self.execution_cfg.edge_gate_enabled):
            return order_rows, []

        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        base_min_expected_edge_bps = max(0.0, float(self.execution_cfg.min_expected_edge_bps or 0.0))
        base_edge_cost_buffer_bps = max(0.0, float(self.execution_cfg.edge_cost_buffer_bps or 0.0))
        edge_score_to_bps_scale = max(1e-6, float(self.execution_cfg.edge_score_to_bps_scale or 140.0))

        for row in order_rows:
            is_entry = self._is_long_entry_order(row) or self._is_short_entry_order(row)
            if not is_entry:
                filtered.append(row)
                continue

            expected_cost_bps = max(0.0, _to_float(row.get("expected_cost_bps"), 0.0))
            expected_edge_threshold = max(
                0.0,
                _to_float(
                    row.get("expected_edge_threshold"),
                    _to_float(row.get("plan_no_trade_band_threshold"), 0.0),
                ),
            )
            score_before_cost = _to_float(
                row.get("score_before_cost"),
                _to_float(row.get("score"), 0.0),
            )
            expected_edge_score = max(
                0.0,
                _to_float(
                    row.get("expected_edge_score"),
                    max(0.0, score_before_cost - expected_edge_threshold),
                ),
            )
            expected_edge_bps = max(
                0.0,
                _to_float(row.get("expected_edge_bps"), expected_edge_score * edge_score_to_bps_scale),
            )
            dynamic_edge_floor_bps = max(
                base_min_expected_edge_bps,
                _to_float(row.get("dynamic_edge_floor_bps"), base_min_expected_edge_bps),
            )
            dynamic_edge_buffer_bps = max(
                base_edge_cost_buffer_bps,
                _to_float(row.get("dynamic_edge_buffer_bps"), base_edge_cost_buffer_bps),
            )
            required_edge_bps = max(dynamic_edge_floor_bps, expected_cost_bps + dynamic_edge_buffer_bps)

            row["expected_edge_threshold"] = float(expected_edge_threshold)
            row["expected_edge_score"] = float(expected_edge_score)
            row["expected_edge_bps"] = float(expected_edge_bps)
            row["edge_gate_base_min_expected_edge_bps"] = float(base_min_expected_edge_bps)
            row["edge_gate_dynamic_floor_bps"] = float(dynamic_edge_floor_bps)
            row["edge_gate_base_buffer_bps"] = float(base_edge_cost_buffer_bps)
            row["edge_gate_dynamic_buffer_bps"] = float(dynamic_edge_buffer_bps)
            row["edge_gate_threshold_bps"] = float(required_edge_bps)
            row["edge_gate_status"] = "PASS"
            row["edge_gate_reason"] = (
                f"expected_edge={expected_edge_bps:.1f}bps >= required={required_edge_bps:.1f}bps "
                f"(floor={dynamic_edge_floor_bps:.1f}; cost={expected_cost_bps:.1f} + buffer={dynamic_edge_buffer_bps:.1f})"
            )
            if expected_edge_bps + 1e-9 >= required_edge_bps:
                annotate_execution_user_explanation(row)
                filtered.append(row)
                continue

            blocked_row = dict(row)
            blocked_row["status"] = "BLOCKED_EDGE"
            blocked_row["edge_gate_status"] = "BLOCKED"
            blocked_row["edge_gate_reason"] = (
                f"expected_edge={expected_edge_bps:.1f}bps < required={required_edge_bps:.1f}bps "
                f"(floor={dynamic_edge_floor_bps:.1f}; cost={expected_cost_bps:.1f} + buffer={dynamic_edge_buffer_bps:.1f})"
            )
            blocked_row["reason"] = f"{str(row.get('reason') or '')}|edge_gate".strip("|")
            annotate_execution_user_explanation(blocked_row)
            blocked.append(blocked_row)
        return filtered, blocked

    def _apply_opportunity_gates(
        self,
        report_path: Path,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        scan_path = report_path / "investment_opportunity_scan.csv"
        if not scan_path.exists():
            return order_rows, []

        scan_rows = self._read_csv(scan_path)
        if not scan_rows:
            return order_rows, []

        allowed_statuses = {
            str(status or "").strip().upper()
            for status in (self.execution_cfg.allowed_opportunity_statuses or ())
            if str(status or "").strip()
        }
        scan_map = {str(row.get("symbol") or "").upper(): dict(row) for row in scan_rows if str(row.get("symbol") or "").strip()}
        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        for row in order_rows:
            if not self._is_long_entry_order(row):
                filtered.append(row)
                continue
            symbol = str(row.get("symbol") or "").upper()
            opp = dict(scan_map.get(symbol) or {})
            status = str(opp.get("entry_status") or "").upper()
            if status and status in allowed_statuses:
                row["opportunity_status"] = status
                row["opportunity_reason"] = str(opp.get("entry_reason") or "")
                annotate_execution_user_explanation(row)
                filtered.append(row)
                continue
            blocked_row = dict(row)
            blocked_row["status"] = "BLOCKED_OPPORTUNITY"
            blocked_row["opportunity_status"] = status or "MISSING"
            blocked_row["opportunity_reason"] = str(opp.get("entry_reason") or "机会扫描未给出允许的进场状态。")
            blocked_row["reason"] = (
                f"{str(row.get('reason') or '')}|opportunity_"
                f"{(status or 'missing').lower()}"
            ).strip("|")
            annotate_execution_user_explanation(blocked_row)
            blocked.append(blocked_row)
        return filtered, blocked

    def _apply_quality_gates(
        self,
        report_path: Path,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        candidate_map = self._read_candidate_map(report_path)
        if not candidate_map:
            return order_rows, []
        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        min_model_score = float(self.execution_cfg.min_model_recommendation_score or 0.0)
        min_execution_score = float(self.execution_cfg.min_execution_score or 0.0)
        require_execution_ready = bool(self.execution_cfg.require_execution_ready)

        for row in order_rows:
            if self._is_long_entry_order(row):
                direction = "LONG"
            elif self._is_short_entry_order(row):
                direction = "SHORT"
            else:
                filtered.append(row)
                continue

            symbol = str(row.get("symbol") or "").upper()
            candidate = dict(candidate_map.get((symbol, direction)) or {})
            block_reasons: List[str] = []
            if not candidate:
                block_reasons.append(f"missing_{direction.lower()}_candidate")
                model_score = 0.0
                execution_score = 0.0
                execution_ready = False
            else:
                model_score = _to_float(candidate.get("model_recommendation_score"), _to_float(candidate.get("score"), 0.0))
                execution_score = _to_float(candidate.get("execution_score"), 0.0)
                raw_ready = candidate.get("execution_ready")
                execution_ready = (
                    _is_truthy(raw_ready)
                    if str(raw_ready or "").strip()
                    else (execution_score >= min_execution_score and model_score >= min_model_score)
                )
            if model_score < min_model_score:
                block_reasons.append(f"model<{min_model_score:.2f}")
            if execution_score < min_execution_score:
                block_reasons.append(f"execution<{min_execution_score:.2f}")
            if require_execution_ready and not execution_ready:
                block_reasons.append("execution_not_ready")
            if direction == "SHORT" and candidate and not _is_truthy(candidate.get("short_execution_allowed", True)):
                block_reasons.append("short_execution_not_allowed")
            if block_reasons:
                blocked_row = dict(row)
                blocked_row["status"] = "BLOCKED_QUALITY"
                blocked_row["quality_status"] = "LOW_QUALITY"
                blocked_row["quality_reason"] = (
                    f"model={model_score:.3f} exec={execution_score:.3f} ready={int(bool(execution_ready))}; "
                    f"{', '.join(block_reasons)}"
                )
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|quality_gate".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue
            row["quality_status"] = "QUALITY_OK"
            row["quality_reason"] = (
                f"model={model_score:.3f} exec={execution_score:.3f} ready={int(bool(execution_ready))}"
            )
            annotate_execution_user_explanation(row)
            filtered.append(row)
        return filtered, blocked

    def _apply_manual_review_gates(
        self,
        order_rows: List[Dict[str, Any]],
        *,
        broker_equity: float,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if not bool(self.execution_cfg.manual_review_enabled) or float(broker_equity) <= 0.0:
            return order_rows, []

        threshold = max(0.0, float(self.execution_cfg.manual_review_order_value_pct or 0.0))
        if threshold <= 0.0:
            return order_rows, []

        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        for row in order_rows:
            order_value = abs(_to_float(row.get("order_value"), 0.0))
            order_pct = order_value / float(broker_equity)
            if order_pct > threshold + 1e-9:
                blocked_row = dict(row)
                blocked_row["status"] = "REVIEW_REQUIRED"
                blocked_row["manual_review_status"] = "REVIEW_REQUIRED"
                blocked_row["manual_review_reason"] = (
                    f"single order {order_pct:.3f} exceeds auto-submit threshold {threshold:.3f}"
                )
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|manual_review".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue
            row["manual_review_status"] = "AUTO_OK"
            row["manual_review_reason"] = f"single order {order_pct:.3f} within threshold {threshold:.3f}"
            annotate_execution_user_explanation(row)
            filtered.append(row)
        return filtered, blocked

    def _apply_shadow_ml_review_gates(
        self,
        report_path: Path,
        order_rows: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if not bool(self.execution_cfg.shadow_ml_review_enabled):
            return order_rows, []

        candidate_map = self._read_candidate_map(report_path)
        if not candidate_map:
            return order_rows, []

        min_score = float(self.execution_cfg.shadow_ml_min_score_auto_submit or 0.0)
        min_prob = float(self.execution_cfg.shadow_ml_min_positive_prob_auto_submit or 0.0)
        min_training_samples = max(0, int(self.execution_cfg.shadow_ml_min_training_samples or 0))
        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []

        for row in order_rows:
            if self._is_long_entry_order(row):
                direction = "LONG"
            elif self._is_short_entry_order(row):
                direction = "SHORT"
            else:
                filtered.append(row)
                continue

            symbol = str(row.get("symbol") or "").upper()
            candidate = dict(candidate_map.get((symbol, direction)) or {})
            if not candidate:
                filtered.append(row)
                continue

            shadow_enabled = _is_truthy(candidate.get("shadow_ml_enabled"))
            shadow_score = _to_float(candidate.get("shadow_ml_score"), 0.0)
            shadow_prob = _to_float(candidate.get("shadow_ml_positive_prob"), 0.0)
            shadow_training_samples = int(_to_float(candidate.get("shadow_ml_training_samples"), 0.0))
            shadow_reason = str(candidate.get("shadow_ml_reason") or "").strip()
            if not shadow_enabled or shadow_training_samples < min_training_samples:
                row["shadow_review_status"] = "SHADOW_BYPASS"
                row["shadow_review_reason"] = (
                    f"enabled={int(bool(shadow_enabled))} samples={shadow_training_samples} "
                    f"min_samples={min_training_samples} reason={shadow_reason or 'n/a'}"
                )
                filtered.append(row)
                continue

            block_reasons: List[str] = []
            if shadow_score < min_score:
                block_reasons.append(f"shadow_score<{min_score:.2f}")
            if shadow_prob < min_prob:
                block_reasons.append(f"shadow_prob<{min_prob:.2f}")
            if block_reasons:
                blocked_row = dict(row)
                blocked_row["status"] = "REVIEW_REQUIRED"
                blocked_row["manual_review_status"] = "REVIEW_REQUIRED"
                blocked_row["shadow_review_status"] = "REVIEW_REQUIRED"
                blocked_row["shadow_review_reason"] = (
                    f"score={shadow_score:.3f} prob={shadow_prob:.3f} samples={shadow_training_samples}; "
                    f"{', '.join(block_reasons)}"
                )
                blocked_row["manual_review_reason"] = (
                    "shadow ML burn-in requires review: "
                    f"score={shadow_score:.3f} prob={shadow_prob:.3f} samples={shadow_training_samples}"
                )
                blocked_row["reason"] = f"{str(row.get('reason') or '')}|shadow_ml_review".strip("|")
                annotate_execution_user_explanation(blocked_row)
                blocked.append(blocked_row)
                continue

            row["shadow_review_status"] = "AUTO_OK"
            row["shadow_review_reason"] = (
                f"score={shadow_score:.3f} prob={shadow_prob:.3f} samples={shadow_training_samples}"
            )
            annotate_execution_user_explanation(row)
            filtered.append(row)
        return filtered, blocked

    def _apply_market_structure_review_gates(
        self,
        report_path: Path,
        order_rows: List[Dict[str, Any]],
        *,
        broker_equity: float,
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        if not self.market_structure.small_account_requires_etf_first(broker_equity):
            return order_rows, []

        allowed_asset_classes = {
            str(item).strip().lower()
            for item in list(self.market_structure.portfolio_preferences.small_account_preferred_asset_classes or [])
            if str(item).strip()
        }
        if not allowed_asset_classes:
            return order_rows, []

        candidate_map = self._read_candidate_map(report_path)
        if not candidate_map:
            return order_rows, []

        filtered: List[Dict[str, Any]] = []
        blocked: List[Dict[str, Any]] = []
        threshold = float(self.market_structure.account_rules.prefer_etf_only_below_equity or 0.0)
        for row in order_rows:
            if not self._is_long_entry_order(row):
                filtered.append(row)
                continue

            symbol = str(row.get("symbol") or "").upper()
            candidate = dict(candidate_map.get((symbol, "LONG")) or {})
            asset_class = str(candidate.get("asset_class") or "").strip().lower()
            if asset_class and asset_class in allowed_asset_classes:
                row["market_structure_review_status"] = "AUTO_OK"
                row["market_structure_review_reason"] = (
                    f"equity={float(broker_equity):.2f} below ETF-first threshold {threshold:.2f}, asset_class={asset_class}"
                )
                filtered.append(row)
                continue

            blocked_row = dict(row)
            blocked_row["status"] = "REVIEW_REQUIRED"
            blocked_row["manual_review_status"] = "REVIEW_REQUIRED"
            blocked_row["market_structure_review_status"] = "REVIEW_REQUIRED"
            blocked_row["market_structure_review_reason"] = (
                f"equity={float(broker_equity):.2f} below ETF-first threshold {threshold:.2f}; "
                f"asset_class={asset_class or 'unknown'} not in {sorted(allowed_asset_classes)}"
            )
            blocked_row["manual_review_reason"] = (
                "market structure review required: "
                f"small-account flow prefers {', '.join(sorted(allowed_asset_classes))}"
            )
            blocked_row["reason"] = f"{str(row.get('reason') or '')}|market_structure_review".strip("|")
            annotate_execution_user_explanation(blocked_row)
            blocked.append(blocked_row)
        return filtered, blocked

    def _snapshot_broker_positions(self, run_id: str, positions: Dict[str, Dict[str, Any]], *, source: str, equity: float) -> None:
        for symbol, pos in positions.items():
            market_value = _to_float(pos.get("market_value"), _to_float(pos.get("qty")) * _to_float(pos.get("market_price"), _to_float(pos.get("avg_cost"))))
            weight = (market_value / equity) if equity > 0 else 0.0
            self.storage.insert_investment_broker_position(
                {
                    "run_id": run_id,
                    "market": self.market,
                    "portfolio_id": self.portfolio_id,
                    "symbol": symbol,
                    "qty": _to_float(pos.get("qty")),
                    "avg_cost": _to_float(pos.get("avg_cost")),
                    "market_price": _to_float(pos.get("market_price")),
                    "market_value": float(market_value),
                    "weight": float(weight),
                    "source": source,
                    "details": json.dumps({"account_id": self.account_id}, ensure_ascii=False),
                }
            )

    def sync_broker_snapshot(self, *, report_dir: str = "") -> Dict[str, Any]:
        report_path = Path(report_dir) if str(report_dir or "").strip() else Path(".")
        account = self._account_snapshot()
        broker_equity = float(account.get("netliq", 0.0) or 0.0)
        broker_cash = float(account.get("cash", 0.0) or 0.0)
        positions_after = self._broker_positions()

        run_id = f"{self.market}-broker-snapshot-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        details = {
            "source": "broker_snapshot_sync",
            "position_count": int(len(positions_after)),
        }
        self.storage.insert_investment_execution_run(
            {
                "run_id": run_id,
                "market": self.market,
                "portfolio_id": self.portfolio_id,
                "account_id": self.account_id,
                "report_dir": str(report_path),
                "submitted": 0,
                "order_count": 0,
                "order_value": 0.0,
                "broker_equity": float(broker_equity),
                "broker_cash": float(broker_cash),
                "target_equity": 0.0,
                "details": json.dumps(details, ensure_ascii=False),
            }
        )
        self._snapshot_broker_positions(run_id, positions_after, source="after", equity=broker_equity)

        summary = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "market": self.market,
            "portfolio_id": self.portfolio_id,
            "account_id": self.account_id,
            "report_dir": str(report_path),
            "source": "broker_snapshot_sync",
            "broker_equity": float(broker_equity),
            "broker_cash": float(broker_cash),
            "position_count": int(len(positions_after)),
        }
        self.storage.update_investment_execution_run(
            run_id,
            details=json.dumps({"source": "broker_snapshot_sync", "summary": summary}, ensure_ascii=False),
        )
        if str(report_dir or "").strip():
            write_json(str(report_path / "investment_broker_snapshot_summary.json"), summary)
        log.info(
            "Investment broker snapshot sync complete: market=%s portfolio=%s positions=%s equity=%.2f cash=%.2f",
            self.market,
            self.portfolio_id,
            len(positions_after),
            broker_equity,
            broker_cash,
        )
        return summary

    @staticmethod
    def _write_md(path: Path, summary: Dict[str, Any], order_rows: List[Dict[str, Any]]) -> None:
        def _intent_for_row(row: Dict[str, Any]) -> Dict[str, Any]:
            raw = row.get("execution_intent_json")
            if not isinstance(raw, str) or not raw:
                return {}
            try:
                data = json.loads(raw)
                return dict(data) if isinstance(data, dict) else {}
            except Exception:
                return {}

        strategy_name = str(summary.get("adaptive_strategy_display_name") or summary.get("adaptive_strategy_name") or "").strip()
        strategy_summary = str(summary.get("adaptive_strategy_summary") or "").strip()
        strategy_runtime_note = str(summary.get("adaptive_strategy_runtime_note") or "").strip()
        strategy_market_note = str(summary.get("adaptive_strategy_active_market_note") or "").strip()
        strategy_control_note = str(summary.get("strategy_effective_controls_note") or "").strip()
        lines = [
            "# Investment Execution Report",
            "",
            f"- Generated: {summary.get('ts', '')}",
            f"- Market: {summary.get('market', '')}",
            f"- Portfolio: {summary.get('portfolio_id', '')}",
            f"- Submitted: {summary.get('submitted', False)}",
            f"- Broker equity: {float(summary.get('broker_equity', 0.0) or 0.0):.2f}",
            f"- Broker cash: {float(summary.get('broker_cash', 0.0) or 0.0):.2f}",
            f"- Target equity: {float(summary.get('target_equity', 0.0) or 0.0):.2f}",
            f"- Account profile: {str(dict(summary.get('account_profile', {}) or {}).get('summary_text', '-') or '-')}",
            f"- Target invested weight: {float(summary.get('target_invested_weight', 0.0) or 0.0):.3f}",
            f"- Target capital: {float(summary.get('target_capital', 0.0) or 0.0):.2f}",
            f"- Theoretical execution capacity: {float(summary.get('theoretical_execution_capacity', 0.0) or 0.0):.2f}",
            f"- Planned deployment value: {float(summary.get('planned_order_value', 0.0) or 0.0):.2f}",
            f"- Idle capital gap: {float(summary.get('idle_capital_gap', 0.0) or 0.0):.2f}",
            f"- Session: {str(summary.get('execution_session_label', '') or '-')} "
            f"style={str(summary.get('execution_style', '') or '-')}",
            f"- Dynamic net exposure cap: {float(summary.get('risk_dynamic_net_exposure', 0.0) or 0.0):.3f}",
            f"- Dynamic gross exposure cap: {float(summary.get('risk_dynamic_gross_exposure', 0.0) or 0.0):.3f}",
            f"- Avg pair correlation: {float(summary.get('risk_avg_pair_correlation', 0.0) or 0.0):.2f}",
            f"- Worst stress: {str(summary.get('risk_stress_worst_scenario_label', '') or '-')} "
            f"loss={float(summary.get('risk_stress_worst_loss', 0.0) or 0.0):.3f}",
            f"- Order count: {int(summary.get('order_count', 0) or 0)}",
            f"- Blocked total: {int(summary.get('blocked_order_count', 0) or 0)}",
            f"- Blocked by market rule: {int(summary.get('blocked_market_rule_order_count', 0) or 0)}",
            f"- Blocked by edge: {int(summary.get('blocked_edge_order_count', 0) or 0)}",
            f"- Blocked by quality: {int(summary.get('blocked_quality_order_count', 0) or 0)}",
            f"- Blocked by opportunity: {int(summary.get('blocked_opportunity_order_count', 0) or 0)}",
            f"- Blocked by liquidity: {int(summary.get('blocked_liquidity_order_count', 0) or 0)}",
            f"- Blocked by risk alert: {int(summary.get('blocked_risk_alert_order_count', 0) or 0)}",
            f"- Risk alert manual review: {int(summary.get('blocked_risk_alert_manual_review_order_count', 0) or 0)}",
            f"- Risk alert deferred: {int(summary.get('blocked_risk_alert_deferred_order_count', 0) or 0)}",
            f"- Deferred by hotspot: {int(summary.get('blocked_hotspot_penalty_order_count', 0) or 0)}",
            f"- Needs manual review: {int(summary.get('blocked_manual_review_order_count', 0) or 0)}",
            f"- Shadow ML review required: {int(summary.get('blocked_shadow_review_order_count', 0) or 0)}",
            f"- Market-structure review required: {int(summary.get('blocked_market_structure_review_order_count', 0) or 0)}",
            f"- Size review required: {int(summary.get('blocked_size_review_order_count', 0) or 0)}",
            f"- Parent orders: {int(summary.get('parent_order_count', 0) or 0)}",
            f"- Split child orders: {int(summary.get('split_order_count', 0) or 0)}",
            f"- ADV capped orders: {int(summary.get('adv_capped_order_count', 0) or 0)}",
            f"- Risk alert slowed parent orders: {int(summary.get('risk_alert_slowed_order_count', 0) or 0)}",
            f"- Hotspot slowed parent orders: {int(summary.get('hotspot_slowed_order_count', 0) or 0)}",
            f"- Risk alert: {str(summary.get('risk_alert_level', '') or '-')} "
            f"{str(summary.get('risk_alert_trend_label', '') or '-')} "
            f"source={str(summary.get('risk_alert_source_label', '') or '-')}",
            f"- Gross order value: {float(summary.get('order_value', 0.0) or 0.0):.2f}",
            f"- Planned spread cost: {float(summary.get('planned_spread_cost_total', 0.0) or 0.0):.2f}",
            f"- Planned slippage cost: {float(summary.get('planned_slippage_cost_total', 0.0) or 0.0):.2f}",
            f"- Planned commission cost: {float(summary.get('planned_commission_cost_total', 0.0) or 0.0):.2f}",
            f"- Planned execution cost: {float(summary.get('planned_execution_cost_total', 0.0) or 0.0):.2f}",
            f"- Gap symbols after snapshot: {int(summary.get('gap_symbols', 0) or 0)}",
            f"- Gap notional after snapshot: {float(summary.get('gap_notional', 0.0) or 0.0):.2f}",
            f"- Risk alert diagnosis: {str(summary.get('risk_alert_diagnosis', '') or '-')}",
            "",
        ]
        if strategy_name or strategy_summary or strategy_runtime_note:
            lines.extend(
                [
                    "## Strategy",
                    f"- Framework: {strategy_name or '-'}",
                ]
            )
        if strategy_summary:
            lines.append(f"- Summary: {strategy_summary}")
        if strategy_market_note:
            lines.append(f"- Market profile: {strategy_market_note}")
        if strategy_runtime_note:
            lines.append(f"- Runtime: {strategy_runtime_note}")
            if strategy_control_note:
                lines.append(f"- Effective controls: {strategy_control_note}")
            lines.append("")
        lines.append("## Orders")
        if not order_rows:
            lines.append("- (no orders)")
        else:
            for row in order_rows:
                intent = _intent_for_row(row)
                reasons = [str(x).strip() for x in list(intent.get("reasons", []) or []) if str(x).strip()]
                opp_status = str(intent.get("opportunity_status") or row.get("opportunity_status") or "").strip()
                opp_reason = str(intent.get("opportunity_reason") or row.get("opportunity_reason") or "").strip()
                lines.append(
                    f"- {row['action']} {row['symbol']} delta_qty={float(row.get('delta_qty', 0.0) or 0.0):.0f} "
                    f"ref={float(row.get('ref_price', 0.0) or 0.0):.2f} value={float(row.get('order_value', 0.0) or 0.0):.2f} "
                    f"status={row.get('status', '')} reason={row.get('user_reason_label', row.get('reason', ''))}"
                )
                if str(row.get("user_reason", "") or "").strip():
                    lines.append(f"  explain: {str(row.get('user_reason', '') or '').strip()}")
                lines.append(
                    f"  plan_cost: expected_bps={float(row.get('expected_cost_bps', 0.0) or 0.0):.1f} "
                    f"expected_cost={float(row.get('expected_cost_value', 0.0) or 0.0):.2f} "
                    f"style={str(row.get('execution_style', '') or '-')} "
                    f"slice={int(row.get('slice_index', 1) or 1)}/{int(row.get('slice_count', 1) or 1)} "
                    f"adv_cap={float(row.get('adv_cap_order_value', 0.0) or 0.0):.2f}"
                )
                if reasons:
                    lines.append(f"  intent reasons: {', '.join(reasons[:4])}")
                quality_status = str(intent.get("metadata", {}).get("quality_status") or row.get("quality_status") or "").strip()
                quality_reason = str(intent.get("metadata", {}).get("quality_reason") or row.get("quality_reason") or "").strip()
                if quality_status or quality_reason:
                    lines.append(f"  quality: {quality_status or 'N/A'} {quality_reason}".rstrip())
                review_status = str(intent.get("metadata", {}).get("manual_review_status") or row.get("manual_review_status") or "").strip()
                review_reason = str(intent.get("metadata", {}).get("manual_review_reason") or row.get("manual_review_reason") or "").strip()
                if review_status or review_reason:
                    lines.append(f"  review: {review_status or 'N/A'} {review_reason}".rstrip())
                shadow_review_status = str(intent.get("metadata", {}).get("shadow_review_status") or row.get("shadow_review_status") or "").strip()
                shadow_review_reason = str(intent.get("metadata", {}).get("shadow_review_reason") or row.get("shadow_review_reason") or "").strip()
                if shadow_review_status or shadow_review_reason:
                    lines.append(f"  shadow_review: {shadow_review_status or 'N/A'} {shadow_review_reason}".rstrip())
                risk_alert_status = str(intent.get("metadata", {}).get("risk_alert_status") or row.get("risk_alert_status") or "").strip()
                risk_alert_reason = str(intent.get("metadata", {}).get("risk_alert_reason") or row.get("risk_alert_reason") or "").strip()
                if risk_alert_status or risk_alert_reason:
                    lines.append(f"  risk_alert: {risk_alert_status or 'N/A'} {risk_alert_reason}".rstrip())
                hotspot_status = str(intent.get("metadata", {}).get("hotspot_penalty_status") or row.get("hotspot_penalty_status") or "").strip()
                hotspot_reason = str(intent.get("metadata", {}).get("hotspot_penalty_reason") or row.get("hotspot_penalty_reason") or "").strip()
                if hotspot_status or hotspot_reason:
                    lines.append(f"  hotspot: {hotspot_status or 'N/A'} {hotspot_reason}".rstrip())
                market_rule_status = str(intent.get("metadata", {}).get("market_rule_status") or row.get("market_rule_status") or "").strip()
                market_rule_reason = str(intent.get("metadata", {}).get("market_rule_reason") or row.get("market_rule_reason") or "").strip()
                if market_rule_status or market_rule_reason:
                    lines.append(f"  market_rule: {market_rule_status or 'N/A'} {market_rule_reason}".rstrip())
                if opp_status or opp_reason:
                    lines.append(f"  opportunity: {opp_status or 'N/A'} {opp_reason}".rstrip())
        path.write_text("\n".join(lines), encoding="utf-8")

    def run(self, *, report_dir: str, submit: bool = False) -> InvestmentExecutionResult:
        report_path = Path(report_dir)
        candidates, plans = self._read_report_books(report_path)
        if not candidates or not plans:
            raise ValueError(f"investment report files not found or empty under {report_path}")
        strategy_payload = load_report_adaptive_strategy_payload(report_path)
        strategy_fields = adaptive_strategy_summary_fields(strategy_payload)

        price_map = {
            str(row.get("symbol") or "").upper(): _to_float(row.get("last_close", 0.0))
            for row in candidates
            if str(row.get("symbol") or "").strip()
        }
        effective_paper_cfg = apply_active_market_risk_overrides(self.paper_cfg, strategy_payload)
        target_weights, risk_overlay = build_target_allocations(candidates, plans, cfg=effective_paper_cfg, return_details=True)
        account = self._account_snapshot()
        broker_equity = float(account.get("netliq", 0.0) or 0.0)
        broker_cash = float(account.get("cash", 0.0) or 0.0)
        base_execution_cfg = self.execution_cfg
        base_execution_cfg = apply_active_market_execution_overrides(base_execution_cfg, strategy_payload)
        effective_execution_cfg, account_profile = apply_account_profile(
            base_execution_cfg,
            self.account_profiles,
            broker_equity=broker_equity,
        )
        strategy_controls = adaptive_strategy_effective_controls(
            strategy_payload,
            portfolio_equity=broker_equity,
            base_target_invested_weight=float(sum(abs(float(v)) for v in target_weights.values())),
            base_account_allocation_pct=float(effective_execution_cfg.account_allocation_pct),
            base_max_order_value_pct=float(effective_execution_cfg.max_order_value_pct),
        )
        effective_execution_cfg = apply_adaptive_strategy_execution_controls(effective_execution_cfg, strategy_controls)
        strategy_control_fields = adaptive_strategy_effective_control_fields(strategy_controls)
        self.execution_cfg = effective_execution_cfg
        try:
            lot_size_map = load_lot_size_map(self.execution_cfg.lot_size_file)
            current_positions = self._broker_positions()
            priority_context_map = self._build_priority_context_map(candidates, plans)
            raw_order_rows = build_investment_rebalance_orders(
                current_positions,
                price_map=price_map,
                target_weights=target_weights,
                broker_equity=broker_equity,
                broker_cash=broker_cash,
                cfg=self.execution_cfg,
                lot_size_map=lot_size_map,
                priority_context_map=priority_context_map,
            )
            market_rule_allowed, market_rule_blocked = self._apply_market_rule_gates(raw_order_rows)
            edge_allowed, edge_blocked = self._apply_expected_edge_gates(market_rule_allowed)
            quality_allowed, quality_blocked = self._apply_quality_gates(report_path, edge_allowed)
            opportunity_allowed, opportunity_blocked = self._apply_opportunity_gates(report_path, quality_allowed)
            shadow_review_allowed, shadow_review_blocked = self._apply_shadow_ml_review_gates(report_path, opportunity_allowed)
            structure_review_allowed, structure_review_blocked = self._apply_market_structure_review_gates(
                report_path,
                shadow_review_allowed,
                broker_equity=broker_equity,
            )
            risk_alert_allowed, risk_alert_blocked, risk_alert_summary = self._apply_portfolio_risk_alert_gates(
                structure_review_allowed,
                broker_equity=broker_equity,
            )
            hotspot_allowed, hotspot_blocked = self._apply_execution_hotspot_gates(risk_alert_allowed)
            split_allowed, liquidity_blocked = self._split_execution_orders(hotspot_allowed)
            order_rows, size_review_blocked = self._apply_manual_review_gates(
                split_allowed,
                broker_equity=broker_equity,
            )
            risk_alert_manual_review_blocked = [
                dict(row)
                for row in risk_alert_blocked
                if str(row.get("manual_review_status") or "").upper() == "REVIEW_REQUIRED"
            ]
            risk_alert_deferred_blocked = [
                dict(row)
                for row in risk_alert_blocked
                if str(row.get("status") or "").upper() == "DEFERRED_RISK_ALERT"
            ]
            manual_review_blocked = (
                list(shadow_review_blocked)
                + list(structure_review_blocked)
                + list(size_review_blocked)
                + list(risk_alert_manual_review_blocked)
            )
            blocked_rows = (
                list(market_rule_blocked)
                + list(edge_blocked)
                + list(quality_blocked)
                + list(opportunity_blocked)
                + list(risk_alert_deferred_blocked)
                + list(hotspot_blocked)
                + list(liquidity_blocked)
                + list(manual_review_blocked)
            )
            order_rows = [annotate_execution_user_explanation(dict(row)) for row in order_rows]
            blocked_rows = [annotate_execution_user_explanation(dict(row)) for row in blocked_rows]

            run_id = f"{self.market}-exec-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
            reserve_cash = max(float(self.execution_cfg.cash_buffer_floor), broker_equity * float(self.execution_cfg.min_cash_buffer_pct))
            account_target_capital = broker_equity * max(0.0, min(1.0, float(self.execution_cfg.account_allocation_pct)))
            target_equity = max(0.0, min(account_target_capital, broker_equity - reserve_cash))
            target_qty_map = self._target_qty_map(
                target_weights=target_weights,
                price_map=price_map,
                investable_equity=target_equity,
                lot_size_map=lot_size_map,
            )
            target_capital = self._target_capital_gap(
                current_positions=current_positions,
                target_qty_map=target_qty_map,
                price_map=price_map,
            )
            theoretical_execution_capacity = min(
                float(target_capital),
                float(broker_equity) * max(0.0, float(self.execution_cfg.max_order_value_pct)) * max(0, int(self.execution_cfg.max_orders_per_run)),
            )
            planned_order_value = self._planned_deployment_value(order_rows)
            idle_capital_gap = max(0.0, float(target_capital) - float(planned_order_value))
            planned_spread_cost_total = float(sum(_to_float(row.get("expected_spread_cost"), 0.0) for row in order_rows))
            planned_slippage_cost_total = float(sum(_to_float(row.get("expected_slippage_cost"), 0.0) for row in order_rows))
            planned_commission_cost_total = float(sum(_to_float(row.get("expected_commission_cost"), 0.0) for row in order_rows))
            planned_execution_cost_total = float(sum(_to_float(row.get("expected_cost_value"), 0.0) for row in order_rows))
            parent_order_keys = {
                str(row.get("parent_order_key") or f"fallback:{idx}")
                for idx, row in enumerate(order_rows, start=1)
            }
            adv_capped_parent_keys = {
                str(row.get("parent_order_key") or f"fallback:{idx}")
                for idx, row in enumerate(order_rows, start=1)
                if bool(row.get("adv_capped", False))
            }
            hotspot_slowed_parent_keys = {
                str(row.get("parent_order_key") or f"fallback:{idx}")
                for idx, row in enumerate(order_rows, start=1)
                if bool(row.get("hotspot_penalty_applied", False))
            }
            risk_alert_slowed_parent_keys = {
                str(row.get("parent_order_key") or f"fallback:{idx}")
                for idx, row in enumerate(order_rows, start=1)
                if bool(row.get("risk_alert_applied", False))
            }
            split_order_count = int(max(0, len(order_rows) - len(parent_order_keys)))
            adv_capped_order_count = int(len(adv_capped_parent_keys))
            hotspot_slowed_order_count = int(len(hotspot_slowed_parent_keys))
            risk_alert_slowed_order_count = int(len(risk_alert_slowed_parent_keys))
            session_profile = self._current_execution_session_profile()
            self.storage.insert_investment_execution_run(
                {
                    "run_id": run_id,
                    "market": self.market,
                    "portfolio_id": self.portfolio_id,
                    "account_id": self.account_id,
                    "report_dir": str(report_path),
                    "submitted": int(bool(submit)),
                    "order_count": int(len(order_rows)),
                    "order_value": float(sum(float(row.get("order_value") or 0.0) for row in order_rows)),
                    "broker_equity": float(broker_equity),
                    "broker_cash": float(broker_cash),
                    "target_equity": float(target_equity),
                    "details": json.dumps(
                        {
                            "target_weights": target_weights,
                            "risk_overlay": risk_overlay,
                            "blocked_order_count": int(len(blocked_rows)),
                            "blocked_market_rule_order_count": int(len(market_rule_blocked)),
                            "blocked_edge_order_count": int(len(edge_blocked)),
                            "blocked_manual_review_order_count": int(len(manual_review_blocked)),
                            "blocked_shadow_review_order_count": int(len(shadow_review_blocked)),
                            "blocked_market_structure_review_order_count": int(len(structure_review_blocked)),
                            "blocked_size_review_order_count": int(len(size_review_blocked)),
                            "blocked_risk_alert_order_count": int(len(risk_alert_blocked)),
                            "blocked_risk_alert_manual_review_order_count": int(len(risk_alert_manual_review_blocked)),
                            "blocked_risk_alert_deferred_order_count": int(len(risk_alert_deferred_blocked)),
                            "blocked_hotspot_penalty_order_count": int(len(hotspot_blocked)),
                            "blocked_liquidity_order_count": int(len(liquidity_blocked)),
                            "target_capital": float(target_capital),
                            "theoretical_execution_capacity": float(theoretical_execution_capacity),
                            "planned_order_value": float(planned_order_value),
                            "idle_capital_gap": float(idle_capital_gap),
                            "planned_spread_cost_total": float(planned_spread_cost_total),
                            "planned_slippage_cost_total": float(planned_slippage_cost_total),
                            "planned_commission_cost_total": float(planned_commission_cost_total),
                            "planned_execution_cost_total": float(planned_execution_cost_total),
                            "split_order_count": int(split_order_count),
                            "adv_capped_order_count": int(adv_capped_order_count),
                            "risk_alert_slowed_order_count": int(risk_alert_slowed_order_count),
                            "hotspot_slowed_order_count": int(hotspot_slowed_order_count),
                            "risk_alert_summary": dict(risk_alert_summary or {}),
                            "execution_session_bucket": session_profile.session_bucket,
                            "execution_session_label": session_profile.session_label,
                            "execution_style": session_profile.execution_style,
                            "account_profile": dict(account_profile or {}),
                            "strategy_effective_controls": dict(strategy_controls or {}),
                        },
                        ensure_ascii=False,
                    ),
                }
            )
            self._snapshot_broker_positions(run_id, current_positions, source="before", equity=broker_equity)

            for row in blocked_rows:
                intent = self._intent_from_row(row)
                intent_json = json.dumps(intent.to_dict(), ensure_ascii=False)
                row["execution_intent_json"] = intent_json
                details_payload = self._build_order_details_payload(row, submitted=False)
                self.storage.insert_investment_execution_order(
                    self._build_execution_order_storage_row(
                        run_id=run_id,
                        row=row,
                        broker_order_id=0,
                        status=str(row.get("status") or "BLOCKED"),
                        details_payload=details_payload,
                        execution_intent_json=intent_json,
                    )
                )

            for row in order_rows:
                row["market"] = self.market
                intent = self._intent_from_row(row)
                if not submit:
                    details_payload = self._build_order_details_payload(row, submitted=False)
                    self.storage.insert_investment_execution_order(
                        self._build_execution_order_storage_row(
                            run_id=run_id,
                            row=row,
                            broker_order_id=0,
                            status="PLANNED",
                            details_payload=details_payload,
                            execution_intent_json=json.dumps(intent.to_dict(), ensure_ascii=False),
                        )
                    )
                    row["status"] = "PLANNED"
                    row["execution_intent_json"] = json.dumps(intent.to_dict(), ensure_ascii=False)
                    continue

                contract = make_stock_contract(row["symbol"])
                trade = self.order_service.place_rebalance_order(
                    contract,
                    symbol=row["symbol"],
                    action=row["action"],
                    qty=float(row.get("delta_qty") or 0.0),
                    params=InvestmentOrderParams(
                        order_type=str(row.get("execution_order_type") or self.execution_cfg.order_type),
                        ref_price=float(row.get("ref_price") or 0.0),
                        limit_price_buffer_bps=float(row.get("limit_price_buffer_bps_effective") or self.execution_cfg.limit_price_buffer_bps),
                        tif=str(self.execution_cfg.tif or "DAY"),
                        outside_rth=bool(self.execution_cfg.outside_rth),
                        route_exchange=str(self.execution_cfg.route_exchange or ""),
                        include_overnight=bool(self.execution_cfg.include_overnight),
                    ),
                    portfolio_id=self.portfolio_id,
                    execution_run_id=run_id,
                    plan_row=row,
                )
                row["broker_order_id"] = int(trade.order.orderId)
                row["status"] = "SUBMITTED"
                row["execution_intent_json"] = json.dumps(intent.to_dict(), ensure_ascii=False)
                if (
                    int(row.get("slice_count", 1) or 1) > 1
                    and int(row.get("slice_index", 1) or 1) < int(row.get("slice_count", 1) or 1)
                    and float(self.execution_cfg.split_order_pause_sec or 0.0) > 0.0
                ):
                    self.ib.sleep(float(self.execution_cfg.split_order_pause_sec))

            if submit and order_rows:
                deadline = datetime.now(timezone.utc).timestamp() + float(self.execution_cfg.wait_fill_sec)
                while datetime.now(timezone.utc).timestamp() < deadline:
                    self.ib.sleep(float(self.execution_cfg.poll_interval_sec))

            positions_after = self._broker_positions()
            self._snapshot_broker_positions(run_id, positions_after, source="after", equity=broker_equity)
            gap_symbols = 0
            gap_notional = 0.0
            for symbol in sorted(set(target_qty_map) | set(positions_after)):
                actual_qty = _to_float(positions_after.get(symbol, {}).get("qty"), 0.0)
                target_qty = _to_float(target_qty_map.get(symbol), 0.0)
                if abs(actual_qty - target_qty) < 1e-9:
                    continue
                gap_symbols += 1
                ref_price = _to_float(price_map.get(symbol), _to_float(positions_after.get(symbol, {}).get("market_price"), 0.0))
                gap_notional += abs(actual_qty - target_qty) * max(0.0, ref_price)

            summary = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "generated_at": "",
                "schema_version": ARTIFACT_SCHEMA_VERSION,
                "run_id": run_id,
                "market": self.market,
                "portfolio_id": self.portfolio_id,
                "report_dir": str(report_path),
                "submitted": bool(submit),
                "broker_equity": float(broker_equity),
                "broker_cash": float(broker_cash),
                "target_equity": float(target_equity),
                "target_invested_weight": float(sum(abs(float(v)) for v in target_weights.values())),
                "target_net_weight": float(sum(float(v) for v in target_weights.values())),
                "account_profile": dict(account_profile or {}),
                "target_capital": float(target_capital),
                "theoretical_execution_capacity": float(theoretical_execution_capacity),
                "planned_order_value": float(planned_order_value),
                "idle_capital_gap": float(idle_capital_gap),
                "risk_overlay_enabled": bool(risk_overlay.get("enabled", False)),
                "risk_base_net_exposure": float(risk_overlay.get("base_net_exposure", 0.0) or 0.0),
                "risk_base_gross_exposure": float(risk_overlay.get("base_gross_exposure", 0.0) or 0.0),
                "risk_base_short_exposure": float(risk_overlay.get("base_short_exposure", 0.0) or 0.0),
                "risk_market_profile_budget_net_exposure": float(risk_overlay.get("market_profile_net_exposure_budget", 0.0) or 0.0),
                "risk_market_profile_budget_gross_exposure": float(risk_overlay.get("market_profile_gross_exposure_budget", 0.0) or 0.0),
                "risk_market_profile_budget_short_exposure": float(risk_overlay.get("market_profile_short_exposure_budget", 0.0) or 0.0),
                "risk_dynamic_scale": float(risk_overlay.get("dynamic_scale", 1.0) or 1.0),
                "risk_dynamic_net_exposure": float(risk_overlay.get("dynamic_net_exposure", 0.0) or 0.0),
                "risk_dynamic_gross_exposure": float(risk_overlay.get("dynamic_gross_exposure", 0.0) or 0.0),
                "risk_dynamic_short_exposure": float(risk_overlay.get("dynamic_short_exposure", 0.0) or 0.0),
                "risk_market_profile_budget_net_tightening": float(risk_overlay.get("market_profile_budget_tightening_net", 0.0) or 0.0),
                "risk_market_profile_budget_gross_tightening": float(risk_overlay.get("market_profile_budget_tightening_gross", 0.0) or 0.0),
                "risk_throttle_net_tightening": float(risk_overlay.get("throttle_net_tightening", 0.0) or 0.0),
                "risk_throttle_gross_tightening": float(risk_overlay.get("throttle_gross_tightening", 0.0) or 0.0),
                "risk_recovery_active": bool(risk_overlay.get("recovery_active", False)),
                "risk_recovery_bonus_scale": float(risk_overlay.get("recovery_bonus_scale", 0.0) or 0.0),
                "risk_recovery_net_credit": float(risk_overlay.get("recovery_net_credit", 0.0) or 0.0),
                "risk_recovery_gross_credit": float(risk_overlay.get("recovery_gross_credit", 0.0) or 0.0),
                "risk_dominant_throttle_layer": str(risk_overlay.get("dominant_throttle_layer", "") or ""),
                "risk_dominant_throttle_layer_label": str(risk_overlay.get("dominant_throttle_layer_label", "") or ""),
                "risk_layered_throttle_text": str(risk_overlay.get("layered_throttle_text", "") or ""),
                "risk_net_exposure_tightening": float(
                    max(
                        0.0,
                        float(risk_overlay.get("base_net_exposure", 0.0) or 0.0)
                        - float(risk_overlay.get("dynamic_net_exposure", 0.0) or 0.0),
                    )
                ),
                "risk_gross_exposure_tightening": float(
                    max(
                        0.0,
                        float(risk_overlay.get("base_gross_exposure", 0.0) or 0.0)
                        - float(risk_overlay.get("dynamic_gross_exposure", 0.0) or 0.0),
                    )
                ),
                "risk_short_exposure_tightening": float(
                    max(
                        0.0,
                        float(risk_overlay.get("base_short_exposure", 0.0) or 0.0)
                        - float(risk_overlay.get("dynamic_short_exposure", 0.0) or 0.0),
                    )
                ),
                "risk_applied_net_exposure": float(risk_overlay.get("applied_net_exposure", 0.0) or 0.0),
                "risk_applied_gross_exposure": float(risk_overlay.get("applied_gross_exposure", 0.0) or 0.0),
                "risk_avg_pair_correlation": float(risk_overlay.get("final_avg_pair_correlation", risk_overlay.get("avg_pair_correlation", 0.0)) or 0.0),
                "risk_max_pair_correlation": float(risk_overlay.get("final_max_pair_correlation", risk_overlay.get("max_pair_correlation", 0.0)) or 0.0),
                "risk_stress_index_drop_loss": float(
                    dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("index_drop", {}).get("loss", 0.0) or 0.0
                ),
                "risk_stress_volatility_spike_loss": float(
                    dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("volatility_spike", {}).get("loss", 0.0) or 0.0
                ),
                "risk_stress_liquidity_shock_loss": float(
                    dict(risk_overlay.get("final_stress_scenarios", {}) or risk_overlay.get("stress_scenarios", {})).get("liquidity_shock", {}).get("loss", 0.0) or 0.0
                ),
                "risk_stress_worst_loss": float(risk_overlay.get("final_stress_worst_loss", risk_overlay.get("stress_worst_loss", 0.0)) or 0.0),
                "risk_stress_worst_scenario": str(risk_overlay.get("final_stress_worst_scenario", risk_overlay.get("stress_worst_scenario", "")) or ""),
                "risk_stress_worst_scenario_label": str(
                    risk_overlay.get("final_stress_worst_scenario_label", risk_overlay.get("stress_worst_scenario_label", "")) or ""
                ),
                "risk_returns_based_enabled": bool(risk_overlay.get("final_returns_based_enabled", risk_overlay.get("returns_based_enabled", False))),
                "risk_returns_based_symbol_count": int(risk_overlay.get("final_returns_based_symbol_count", risk_overlay.get("returns_based_symbol_count", 0)) or 0),
                "risk_returns_based_sample_size": int(risk_overlay.get("final_returns_based_sample_size", risk_overlay.get("returns_based_sample_size", 0)) or 0),
                "risk_returns_based_var_95_1d": float(
                    risk_overlay.get("final_returns_based_var_95_1d", risk_overlay.get("returns_based_var_95_1d", 0.0)) or 0.0
                ),
                "risk_returns_based_portfolio_vol_1d": float(
                    risk_overlay.get("final_returns_based_portfolio_vol_1d", risk_overlay.get("returns_based_portfolio_vol_1d", 0.0)) or 0.0
                ),
                "risk_returns_based_downside_vol_1d": float(
                    risk_overlay.get("final_returns_based_downside_vol_1d", risk_overlay.get("returns_based_downside_vol_1d", 0.0)) or 0.0
                ),
                "risk_correlation_source": str(risk_overlay.get("correlation_source", "") or ""),
                "risk_top_sector_share": float(risk_overlay.get("top_sector_share", 0.0) or 0.0),
                "risk_notes": list(risk_overlay.get("notes", []) or []),
                "risk_correlation_reduced_symbols": list(risk_overlay.get("correlation_reduced_symbols", []) or []),
                "order_count": int(len(order_rows)),
                "blocked_order_count": int(len(blocked_rows)),
                "blocked_market_rule_order_count": int(len(market_rule_blocked)),
                "blocked_edge_order_count": int(len(edge_blocked)),
                "blocked_quality_order_count": int(len(quality_blocked)),
                "blocked_opportunity_order_count": int(len(opportunity_blocked)),
                "blocked_liquidity_order_count": int(len(liquidity_blocked)),
                "blocked_hotspot_penalty_order_count": int(len(hotspot_blocked)),
                "blocked_manual_review_order_count": int(len(manual_review_blocked)),
                "blocked_shadow_review_order_count": int(len(shadow_review_blocked)),
                "blocked_market_structure_review_order_count": int(len(structure_review_blocked)),
                "blocked_size_review_order_count": int(len(size_review_blocked)),
                "blocked_risk_alert_order_count": int(len(risk_alert_blocked)),
                "blocked_risk_alert_manual_review_order_count": int(len(risk_alert_manual_review_blocked)),
                "blocked_risk_alert_deferred_order_count": int(len(risk_alert_deferred_blocked)),
                "parent_order_count": int(len(raw_order_rows)),
                "split_order_count": int(split_order_count),
                "adv_capped_order_count": int(adv_capped_order_count),
                "risk_alert_slowed_order_count": int(risk_alert_slowed_order_count),
                "hotspot_slowed_order_count": int(hotspot_slowed_order_count),
                "risk_alert_level": str(risk_alert_summary.get("alert_level", "") or ""),
                "risk_alert_trend_label": str(risk_alert_summary.get("trend_label", "") or ""),
                "risk_alert_source_label": str(risk_alert_summary.get("source_label", "") or ""),
                "risk_alert_diagnosis": str(risk_alert_summary.get("diagnosis", "") or ""),
                "execution_session_bucket": str(session_profile.session_bucket),
                "execution_session_label": str(session_profile.session_label),
                "execution_style": str(session_profile.execution_style),
                "planned_spread_cost_total": float(planned_spread_cost_total),
                "planned_slippage_cost_total": float(planned_slippage_cost_total),
                "planned_commission_cost_total": float(planned_commission_cost_total),
                "planned_execution_cost_total": float(planned_execution_cost_total),
                "order_value": float(sum(float(row.get("order_value") or 0.0) for row in order_rows)),
                "gap_symbols": int(gap_symbols),
                "gap_notional": float(gap_notional),
            }
            summary["generated_at"] = str(summary["ts"])
            summary.update(strategy_fields)
            summary.update(strategy_control_fields)
            self.storage.update_investment_execution_run(
                run_id,
                details=json.dumps(
                    {
                        "target_weights": target_weights,
                        "risk_overlay": risk_overlay,
                        "strategy_effective_controls": strategy_controls,
                        "summary": summary,
                    },
                    ensure_ascii=False,
                ),
            )
            self.storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id=run_id,
                    ts=summary["ts"],
                    market=self.market,
                    portfolio_id=self.portfolio_id,
                    source_kind="execution",
                    source_label="执行",
                    report_dir=str(report_path),
                    account_id=str(self.account_id or ""),
                    risk_overlay=risk_overlay,
                    details={
                        "submitted": bool(submit),
                        "order_count": int(len(order_rows)),
                        "blocked_order_count": int(len(blocked_rows)),
                        "blocked_market_rule_order_count": int(len(market_rule_blocked)),
                        "execution_session_bucket": str(summary.get("execution_session_bucket") or ""),
                        "execution_style": str(summary.get("execution_style") or ""),
                        "strategy_effective_controls_applied": bool(summary.get("strategy_effective_controls_applied", False)),
                    },
                )
            )
            plan_rows = list(order_rows) + list(blocked_rows)
            write_csv(str(report_path / "investment_execution_plan.csv"), plan_rows)
            write_json(str(report_path / "investment_execution_summary.json"), summary)
            self._write_md(report_path / "investment_execution_report.md", summary, plan_rows)
            log.info(
                "Investment execution complete: submitted=%s orders=%s gap_symbols=%s gap_notional=%.2f",
                submit,
                len(order_rows),
                gap_symbols,
                gap_notional,
            )
            return InvestmentExecutionResult(
                run_id=run_id,
                portfolio_id=self.portfolio_id,
                market=self.market,
                report_dir=str(report_path),
                submitted=bool(submit),
                broker_equity=float(broker_equity),
                broker_cash=float(broker_cash),
                target_equity=float(target_equity),
                order_count=int(len(order_rows)),
                order_value=float(sum(float(row.get("order_value") or 0.0) for row in order_rows)),
                gap_symbols=int(gap_symbols),
                gap_notional=float(gap_notional),
                account_profile_name=str(dict(account_profile or {}).get("name", "") or ""),
                account_profile_label=str(dict(account_profile or {}).get("label", "") or ""),
            )
        finally:
            self.execution_cfg = base_execution_cfg
