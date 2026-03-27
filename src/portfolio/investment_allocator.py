from __future__ import annotations

from dataclasses import dataclass, field
from math import floor
from pathlib import Path
from typing import Any, Dict, List


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


@dataclass
class InvestmentExecutionConfig:
    min_cash_buffer_pct: float = 0.05
    cash_buffer_floor: float = 1000.0
    min_trade_value: float = 500.0
    max_order_value_pct: float = 0.05
    weight_tolerance: float = 0.015
    max_orders_per_run: int = 6
    lot_size: int = 1
    lot_size_file: str = ""
    order_type: str = "MKT"
    limit_price_buffer_bps: float = 15.0
    wait_fill_sec: float = 20.0
    poll_interval_sec: float = 1.0
    allow_buys: bool = True
    allow_sells: bool = True
    account_allocation_pct: float = 0.30
    allow_min_lot_buy_override: bool = False
    min_lot_buy_override_value_pct: float = 0.0
    allow_min_lot_sell_override: bool = False
    min_lot_sell_override_value_pct: float = 0.0
    allow_fractional_qty: bool = False
    fractional_qty_decimals: int = 4
    tif: str = "DAY"
    outside_rth: bool = False
    route_exchange: str = ""
    include_overnight: bool = False
    allowed_opportunity_statuses: tuple[str, ...] = ("ENTRY_NOW", "ADD_ON_PULLBACK")
    account_snapshot_ttl_sec: int = 900
    min_model_recommendation_score: float = 0.15
    min_execution_score: float = 0.05
    require_execution_ready: bool = True
    manual_review_enabled: bool = True
    manual_review_order_value_pct: float = 0.10
    shadow_ml_review_enabled: bool = True
    shadow_ml_min_score_auto_submit: float = 0.0
    shadow_ml_min_positive_prob_auto_submit: float = 0.50
    shadow_ml_min_training_samples: int = 80
    adv_max_participation_pct: float = 0.05
    adv_split_trigger_pct: float = 0.02
    max_slices_per_symbol: int = 4
    split_order_pause_sec: float = 2.0
    prefer_limit_orders_for_sliced_execution: bool = True
    open_session_participation_scale: float = 0.70
    midday_session_participation_scale: float = 1.00
    close_session_participation_scale: float = 0.85
    open_session_limit_buffer_scale: float = 1.25
    midday_session_limit_buffer_scale: float = 0.85
    close_session_limit_buffer_scale: float = 1.10
    # 这些字段用于接 weekly review 产出的“执行热点惩罚”。
    # 目标不是永久拉黑标的，而是在热点周里让盘中执行更保守。
    execution_hotspot_penalties: List[Dict[str, Any]] = field(default_factory=list)
    execution_hotspot_defer_session_buckets: tuple[str, ...] = ("OPEN", "CLOSE")
    execution_hotspot_defer_min_execution_penalty: float = 0.04
    execution_hotspot_defer_min_expected_cost_bps_add: float = 8.0
    execution_hotspot_adv_participation_scale: float = 0.70
    execution_hotspot_split_trigger_scale: float = 0.70
    execution_hotspot_limit_buffer_scale: float = 1.25
    execution_hotspot_force_min_slices: int = 2
    execution_hotspot_force_limit_order: bool = True
    # 组合级风险轨迹告警：WATCH 先降速，ALERT 在开盘/尾盘可延后或转人工审核。
    risk_alert_guard_enabled: bool = True
    risk_alert_history_max_age_hours: int = 96
    risk_alert_scale_alert_threshold: float = 0.75
    risk_alert_corr_alert_threshold: float = 0.62
    risk_alert_stress_alert_threshold: float = 0.085
    risk_alert_scale_watch_delta: float = -0.05
    risk_alert_corr_watch_delta: float = 0.04
    risk_alert_stress_watch_delta: float = 0.01
    risk_alert_defer_session_buckets: tuple[str, ...] = ("OPEN", "CLOSE")
    risk_alert_manual_review_order_value_pct: float = 0.05
    risk_alert_watch_adv_participation_scale: float = 0.85
    risk_alert_watch_split_trigger_scale: float = 0.85
    risk_alert_watch_limit_buffer_scale: float = 1.10
    risk_alert_alert_adv_participation_scale: float = 0.65
    risk_alert_alert_split_trigger_scale: float = 0.65
    risk_alert_alert_limit_buffer_scale: float = 1.30
    risk_alert_force_min_slices_alert: int = 2
    risk_alert_force_limit_order: bool = True

    @classmethod
    def from_dict(cls, raw: Dict[str, Any] | None) -> "InvestmentExecutionConfig":
        raw = raw or {}
        return cls(**{k: raw[k] for k in cls.__dataclass_fields__ if k in raw})


def load_lot_size_map(path_str: str) -> Dict[str, int]:
    if not str(path_str or "").strip():
        return {}
    path = Path(path_str)
    if not path.exists():
        return {}
    try:
        import csv

        out: Dict[str, int] = {}
        with path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                symbol = str(row.get("symbol") or "").upper().strip()
                lot_size = int(float(row.get("lot_size") or 0))
                if symbol and lot_size > 0:
                    out[symbol] = lot_size
        return out
    except Exception:
        return {}


def _round_lot_qty(raw_qty: float, lot_size: int) -> int:
    lot = max(1, int(lot_size or 1))
    qty = int(floor(abs(raw_qty) / lot)) * lot
    return int(qty)


def _round_trade_qty(raw_qty: float, lot_size: int, cfg: InvestmentExecutionConfig) -> float:
    if bool(cfg.allow_fractional_qty) and int(lot_size or 1) <= 1:
        precision = max(0, int(cfg.fractional_qty_decimals or 0))
        scale = float(10**precision)
        qty = floor(max(0.0, float(raw_qty)) * scale) / scale
        return float(qty)
    return float(_round_lot_qty(raw_qty, lot_size))


def _target_signed_qty(
    target_weight: float,
    *,
    investable_equity: float,
    ref_price: float,
    lot_size: int,
    cfg: InvestmentExecutionConfig,
) -> float:
    if ref_price <= 0.0:
        return 0.0
    raw_qty = abs(float(investable_equity) * float(target_weight)) / float(ref_price)
    rounded = _round_trade_qty(raw_qty, lot_size, cfg)
    if rounded <= 0.0:
        return 0.0
    return float(rounded if float(target_weight) >= 0.0 else -rounded)


def _rebalance_reason(current_qty: float, target_qty: float, action: str) -> str:
    current_qty = float(current_qty)
    target_qty = float(target_qty)
    action = str(action or "").upper()
    if action == "SELL":
        if current_qty <= 0.0 and target_qty < current_qty:
            return "rebalance_add_short"
        if current_qty > 0.0 and target_qty < 0.0:
            return "rebalance_flip_to_short"
        return "rebalance_down" if target_qty > 0.0 else "rebalance_exit"
    if current_qty < 0.0 and target_qty >= 0.0:
        return "rebalance_flip_to_long" if target_qty > 0.0 else "rebalance_cover"
    if current_qty < 0.0 and target_qty > current_qty:
        return "rebalance_cover"
    return "rebalance_up"


def _priority_context(symbol: str, priority_context_map: Dict[str, Dict[str, Any]] | None) -> Dict[str, Any]:
    if not priority_context_map:
        return {}
    return dict(priority_context_map.get(str(symbol).upper(), {}) or {})


def _entry_priority_score(
    symbol: str,
    *,
    target_weight: float,
    priority_context_map: Dict[str, Dict[str, Any]] | None,
) -> float:
    context = _priority_context(symbol, priority_context_map)
    # 这里优先使用“成本后分数”，再叠加执行分数和流动性，避免高成本标的只靠目标权重抢占名额。
    score_net = _to_float(
        context.get("score"),
        _to_float(context.get("model_recommendation_score"), 0.0),
    )
    execution_score = _to_float(context.get("execution_score"), 0.0)
    liquidity_score = _to_float(context.get("liquidity_score"), 0.0)
    expected_cost_bps = max(0.0, _to_float(context.get("expected_cost_bps"), 0.0))
    return float(
        score_net
        + 0.30 * execution_score
        + 0.08 * liquidity_score
        + 0.05 * abs(float(target_weight))
        - min(expected_cost_bps, 150.0) / 1000.0
    )


def _sell_priority_key(
    symbol: str,
    *,
    current_qty: float,
    target_qty: float,
    ref_price: float,
    target_weight: float,
    priority_context_map: Dict[str, Dict[str, Any]] | None,
) -> tuple[float, float, str]:
    current_qty = float(current_qty)
    target_qty = float(target_qty)
    reducing_long = current_qty > 0.0 and target_qty >= 0.0
    exiting_long = current_qty > 0.0 and target_qty <= 0.0
    opening_or_adding_short = target_qty < min(0.0, current_qty)
    phase_rank = 0.0
    if exiting_long:
        phase_rank = 0.0
    elif reducing_long:
        phase_rank = 1.0
    elif opening_or_adding_short:
        phase_rank = 2.0
    order_value = abs(float(current_qty - target_qty) * max(0.0, float(ref_price)))
    priority = _entry_priority_score(symbol, target_weight=target_weight, priority_context_map=priority_context_map)
    return (phase_rank, -priority if opening_or_adding_short else -order_value, str(symbol))


def build_investment_rebalance_orders(
    current_positions: Dict[str, Dict[str, Any]],
    *,
    price_map: Dict[str, float],
    target_weights: Dict[str, float],
    broker_equity: float,
    broker_cash: float,
    cfg: InvestmentExecutionConfig,
    lot_size_map: Dict[str, int] | None = None,
    priority_context_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    lot_size_map = {str(k).upper(): int(v) for k, v in (lot_size_map or {}).items()}
    equity = max(0.0, float(broker_equity))
    cash = max(0.0, float(broker_cash))
    if equity <= 0:
        return []

    reserve_cash = max(float(cfg.cash_buffer_floor), equity * float(cfg.min_cash_buffer_pct))
    account_target_capital = equity * max(0.0, min(1.0, float(cfg.account_allocation_pct)))
    investable_equity = max(0.0, min(account_target_capital, equity - reserve_cash))
    max_order_value = max(0.0, equity * float(cfg.max_order_value_pct))
    working_positions = {str(sym).upper(): dict(pos) for sym, pos in (current_positions or {}).items()}
    target_qty_map: Dict[str, float] = {}
    for symbol, target_weight in target_weights.items():
        symbol_key = str(symbol).upper()
        current = working_positions.get(symbol_key, {})
        ref_price = _to_float(
            price_map.get(symbol_key),
            _to_float(current.get("market_price"), _to_float(current.get("last_price"), _to_float(current.get("avg_cost"), 0.0))),
        )
        lot_size = lot_size_map.get(symbol_key, max(1, int(cfg.lot_size)))
        target_qty_map[symbol_key] = _target_signed_qty(
            _to_float(target_weight),
            investable_equity=investable_equity,
            ref_price=ref_price,
            lot_size=lot_size,
            cfg=cfg,
        )

    orders: List[Dict[str, Any]] = []

    sell_symbols = list(sorted(set(working_positions) | set(target_qty_map)))
    sell_symbols.sort(
        key=lambda symbol: _sell_priority_key(
            symbol,
            current_qty=_to_float((working_positions.get(symbol) or {}).get("qty"), 0.0),
            target_qty=_to_float(target_qty_map.get(symbol), 0.0),
            ref_price=_to_float(
                price_map.get(symbol),
                _to_float(
                    (working_positions.get(symbol) or {}).get("market_price"),
                    _to_float((working_positions.get(symbol) or {}).get("last_price"), _to_float((working_positions.get(symbol) or {}).get("avg_cost"), 0.0)),
                ),
            ),
            target_weight=_to_float(target_weights.get(symbol), 0.0),
            priority_context_map=priority_context_map,
        )
    )
    for symbol in sell_symbols:
        if len(orders) >= int(cfg.max_orders_per_run):
            break
        current = working_positions.setdefault(symbol, {})
        ref_price = _to_float(
            price_map.get(symbol),
            _to_float(current.get("market_price"), _to_float(current.get("last_price"), _to_float(current.get("avg_cost"), 0.0))),
        )
        if ref_price <= 0.0 or not bool(cfg.allow_sells):
            continue
        lot_size = lot_size_map.get(symbol, max(1, int(cfg.lot_size)))
        min_lot_value = float(lot_size * ref_price)
        current_qty = _to_float(current.get("qty"), 0.0)
        target_qty = _to_float(target_qty_map.get(symbol), 0.0)
        sell_delta = current_qty - target_qty
        if sell_delta <= 0.0:
            continue
        desired_sell_value = float(sell_delta * ref_price)
        capped_sell_value = min(desired_sell_value, max_order_value or desired_sell_value)
        sell_qty = 0.0
        used_override = False
        if capped_sell_value >= float(cfg.min_trade_value):
            sell_qty = _round_trade_qty(capped_sell_value / ref_price, lot_size, cfg)
        if sell_qty <= 0.0 and bool(cfg.allow_min_lot_sell_override):
            override_cap = equity * max(0.0, float(cfg.min_lot_sell_override_value_pct))
            if (
                min_lot_value >= float(cfg.min_trade_value)
                and float(lot_size) <= sell_delta + 1e-9
                and min_lot_value <= desired_sell_value + 1e-9
                and min_lot_value <= max(max_order_value, override_cap) + 1e-9
            ):
                sell_qty = float(lot_size)
                used_override = True
        if sell_qty <= 0.0:
            continue
        next_qty = float(current_qty - sell_qty)
        order_value = float(sell_qty * ref_price)
        context = _priority_context(symbol, priority_context_map)
        orders.append(
            {
                "symbol": symbol,
                "action": "SELL",
                "current_qty": float(current_qty),
                "target_qty": float(next_qty),
                "delta_qty": float(sell_qty),
                "ref_price": float(ref_price),
                "target_weight": float(_to_float(target_weights.get(symbol), 0.0)),
                "order_value": order_value,
                "lot_size": int(lot_size),
                "priority_score": float(
                    _entry_priority_score(
                        symbol,
                        target_weight=_to_float(target_weights.get(symbol), 0.0),
                        priority_context_map=priority_context_map,
                    )
                ),
                "score": float(_to_float(context.get("score"), 0.0)),
                "score_before_cost": float(_to_float(context.get("score_before_cost"), _to_float(context.get("score"), 0.0))),
                "model_recommendation_score": float(_to_float(context.get("model_recommendation_score"), _to_float(context.get("score"), 0.0))),
                "execution_score": float(_to_float(context.get("execution_score"), 0.0)),
                "expected_cost_bps": float(_to_float(context.get("expected_cost_bps"), 0.0)),
                "spread_proxy_bps": float(_to_float(context.get("spread_proxy_bps"), 0.0)),
                "slippage_proxy_bps": float(_to_float(context.get("slippage_proxy_bps"), 0.0)),
                "commission_proxy_bps": float(_to_float(context.get("commission_proxy_bps"), 0.0)),
                "avg_daily_volume": float(_to_float(context.get("avg_daily_volume"), 0.0)),
                "avg_daily_dollar_volume": float(_to_float(context.get("avg_daily_dollar_volume"), 0.0)),
                "liquidity_score": float(_to_float(context.get("liquidity_score"), 0.0)),
                "reason": (
                    "rebalance_down_min_lot_override"
                    if used_override
                    else _rebalance_reason(current_qty, target_qty, "SELL")
                ),
            }
        )
        current["qty"] = float(next_qty)
        current["market_price"] = float(ref_price)
        cash += order_value

    buy_symbols = list(sorted(set(working_positions) | {str(sym).upper() for sym, weight in target_weights.items() if float(weight) > 0.0}))
    buy_symbols.sort(
        key=lambda symbol: (
            -_entry_priority_score(
                symbol,
                target_weight=_to_float(target_weights.get(symbol), 0.0),
                priority_context_map=priority_context_map,
            ),
            -abs(_to_float(target_weights.get(symbol), 0.0)),
            str(symbol),
        )
    )
    for symbol in buy_symbols:
        if len(orders) >= int(cfg.max_orders_per_run):
            break
        if not bool(cfg.allow_buys):
            break
        current = working_positions.setdefault(symbol, {})
        ref_price = _to_float(
            price_map.get(symbol),
            _to_float(current.get("market_price"), _to_float(current.get("last_price"), _to_float(current.get("avg_cost"), 0.0))),
        )
        if ref_price <= 0.0:
            continue
        lot_size = lot_size_map.get(symbol, max(1, int(cfg.lot_size)))
        min_lot_value = float(lot_size * ref_price)
        current_qty = _to_float(current.get("qty"), 0.0)
        target_qty = _to_float(target_qty_map.get(symbol), 0.0)
        buy_delta = target_qty - current_qty
        if buy_delta <= 0.0:
            continue
        desired_buy_value = float(buy_delta * ref_price)
        capped_buy_value = min(desired_buy_value, max_order_value or desired_buy_value, cash)
        buy_qty = 0.0
        used_override = False
        if capped_buy_value >= float(cfg.min_trade_value):
            buy_qty = _round_trade_qty(capped_buy_value / ref_price, lot_size, cfg)
        if buy_qty <= 0.0 and bool(cfg.allow_min_lot_buy_override):
            override_cap = equity * max(0.0, float(cfg.min_lot_buy_override_value_pct))
            target_weight = _to_float(target_weights.get(symbol), 0.0)
            if (
                current_qty <= 0.0
                and target_weight > float(cfg.weight_tolerance)
                and min_lot_value >= float(cfg.min_trade_value)
                and float(lot_size) <= buy_delta + 1e-9
                and min_lot_value <= desired_buy_value + 1e-9
                and min_lot_value <= cash + 1e-9
                and min_lot_value <= max(max_order_value, override_cap) + 1e-9
            ):
                buy_qty = float(lot_size)
                used_override = True
        if buy_qty <= 0.0:
            continue
        order_value = float(buy_qty * ref_price)
        if order_value > cash + 1e-9:
            continue
        next_qty = float(current_qty + buy_qty)
        context = _priority_context(symbol, priority_context_map)
        orders.append(
            {
                "symbol": symbol,
                "action": "BUY",
                "current_qty": float(current_qty),
                "target_qty": float(next_qty),
                "delta_qty": float(buy_qty),
                "ref_price": float(ref_price),
                "target_weight": float(_to_float(target_weights.get(symbol), 0.0)),
                "order_value": order_value,
                "lot_size": int(lot_size),
                "priority_score": float(
                    _entry_priority_score(
                        symbol,
                        target_weight=_to_float(target_weights.get(symbol), 0.0),
                        priority_context_map=priority_context_map,
                    )
                ),
                "score": float(_to_float(context.get("score"), 0.0)),
                "score_before_cost": float(_to_float(context.get("score_before_cost"), _to_float(context.get("score"), 0.0))),
                "model_recommendation_score": float(_to_float(context.get("model_recommendation_score"), _to_float(context.get("score"), 0.0))),
                "execution_score": float(_to_float(context.get("execution_score"), 0.0)),
                "expected_cost_bps": float(_to_float(context.get("expected_cost_bps"), 0.0)),
                "spread_proxy_bps": float(_to_float(context.get("spread_proxy_bps"), 0.0)),
                "slippage_proxy_bps": float(_to_float(context.get("slippage_proxy_bps"), 0.0)),
                "commission_proxy_bps": float(_to_float(context.get("commission_proxy_bps"), 0.0)),
                "avg_daily_volume": float(_to_float(context.get("avg_daily_volume"), 0.0)),
                "avg_daily_dollar_volume": float(_to_float(context.get("avg_daily_dollar_volume"), 0.0)),
                "liquidity_score": float(_to_float(context.get("liquidity_score"), 0.0)),
                "reason": (
                    "rebalance_up_min_lot_override"
                    if used_override
                    else _rebalance_reason(current_qty, target_qty, "BUY")
                ),
            }
        )
        current["qty"] = float(next_qty)
        current["market_price"] = float(ref_price)
        cash -= order_value

    orders.sort(
        key=lambda row: (
            0 if row["action"] == "SELL" else 1,
            -float(row.get("priority_score", 0.0) or 0.0),
            -float(row["order_value"]),
        )
    )
    return orders[: max(0, int(cfg.max_orders_per_run))]
