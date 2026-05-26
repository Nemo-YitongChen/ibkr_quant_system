from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from .account_profile import load_account_profiles
from .config_layers import load_layered_config
from .market_structure import load_market_structure, market_structure_summary
from .markets import market_config_path, resolve_market_code
from .runtime_paths import resolve_repo_path


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _load_json_dict(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_yaml_dict(path: Path) -> Dict[str, Any]:
    try:
        import yaml

        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_csv_rows(path: Path | None) -> List[Dict[str, Any]]:
    if path is None or not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _latest_existing_path(paths: Iterable[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda item: item.stat().st_mtime)


def _slugify_report_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


def _candidate_report_dirs(
    *,
    base_dir: Path,
    runtime_root: Path,
    market: str,
    out_dir: str,
    watchlist_yaml: str,
) -> List[Path]:
    stem = _slugify_report_name(Path(str(watchlist_yaml or "")).stem)
    raw_out = str(out_dir or "reports_investment").strip() or "reports_investment"
    market_root = f"reports_investment_{str(market or '').lower()}"
    candidates = [
        resolve_repo_path(base_dir, raw_out) / stem,
        runtime_root / raw_out / stem,
        resolve_repo_path(base_dir, market_root) / stem,
        runtime_root / market_root / stem,
    ]
    seen: set[str] = set()
    out: List[Path] = []
    for path in candidates:
        key = str(path.resolve() if path.exists() else path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _latest_artifact(
    *,
    base_dir: Path,
    runtime_root: Path,
    market: str,
    out_dir: str,
    watchlist_yaml: str,
    filename: str,
) -> tuple[Path | None, Dict[str, Any]]:
    path = _latest_existing_path(
        path / filename
        for path in _candidate_report_dirs(
            base_dir=base_dir,
            runtime_root=runtime_root,
            market=market,
            out_dir=out_dir,
            watchlist_yaml=watchlist_yaml,
        )
    )
    return path, (_load_json_dict(path) if path else {})


def _latest_artifact_path(
    *,
    base_dir: Path,
    runtime_root: Path,
    market: str,
    out_dir: str,
    watchlist_yaml: str,
    filename: str,
) -> Path | None:
    return _latest_existing_path(
        path / filename
        for path in _candidate_report_dirs(
            base_dir=base_dir,
            runtime_root=runtime_root,
            market=market,
            out_dir=out_dir,
            watchlist_yaml=watchlist_yaml,
        )
    )


def _artifact_age_hours(path: Path | None, *, now: datetime) -> float:
    if path is None or not path.exists():
        return 0.0
    try:
        modified_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        return max(0.0, (now - modified_at).total_seconds() / 3600.0)
    except Exception:
        return 0.0


def _artifact_source_health(
    *,
    path: Path | None,
    execution_summary: Mapping[str, Any],
    age_hours: float,
    max_age_hours: float,
) -> tuple[str, str, str]:
    if path is None or not execution_summary:
        return "MISSING", "missing_execution_artifact", "run investment report, paper, then execution dry-run"
    primary_reason = _execution_primary_reason(execution_summary).upper()
    if primary_reason == "IBKR_GATEWAY_UNAVAILABLE":
        return "DEGRADED_GATEWAY", "ibkr_gateway_unavailable_degraded_artifact", "start or unlock IB Gateway paper API, then rerun no-submit"
    if max_age_hours > 0.0 and age_hours > max_age_hours:
        return "STALE", f"execution_artifact_age_hours={age_hours:.1f}>max={max_age_hours:.1f}", "refresh report and no-submit before ranking this market"
    return "FRESH", f"execution_artifact_age_hours={age_hours:.1f}", "artifact fresh enough for readiness review"


def _effective_execution_payload(
    *,
    base_dir: Path,
    execution_config: str,
    ibkr_cfg: Mapping[str, Any],
    broker_equity: float,
) -> Dict[str, Any]:
    raw_payload = load_layered_config(
        base_dir,
        str(execution_config or "config/investment_execution.yaml"),
        default_paths=("config/investment_execution.yaml",),
    ).payload
    raw_execution = dict(raw_payload.get("execution") or {})
    account_cap = _float(raw_execution.get("account_equity_cap"), 0.0)
    effective_equity = _float(broker_equity, 0.0)
    if effective_equity <= 0.0 and account_cap > 0.0:
        effective_equity = float(account_cap)
    if account_cap > 0.0 and effective_equity > 0.0:
        effective_equity = min(float(effective_equity), float(account_cap))

    profiles = load_account_profiles(
        base_dir,
        str(ibkr_cfg.get("account_profile_config") or "config/account_profiles.yaml"),
    )
    profile = profiles.resolve(effective_equity)
    overrides = profile.execution.to_override_dict() if profile else {}
    effective_execution = dict(raw_execution)
    effective_execution.update(overrides)

    return {
        "raw_execution": raw_execution,
        "effective_execution": effective_execution,
        "account_equity_cap": float(account_cap),
        "effective_readiness_equity": float(effective_equity),
        "account_profile_name": str(profile.name if profile else ""),
        "account_profile_label": str(profile.display_label if profile else ""),
    }


def _small_account_feasibility(
    *,
    execution_payload: Mapping[str, Any],
    fee_floor_one_side_bps: float,
    odd_lot_discount_risk: bool,
) -> Dict[str, Any]:
    cfg = dict(execution_payload.get("effective_execution") or {})
    equity = _float(execution_payload.get("effective_readiness_equity"), 0.0)
    min_cash_buffer_pct = max(0.0, _float(cfg.get("min_cash_buffer_pct"), 0.0))
    cash_buffer_floor = max(0.0, _float(cfg.get("cash_buffer_floor"), 0.0))
    min_trade_value = max(0.0, _float(cfg.get("min_trade_value"), 0.0))
    max_order_value_pct = max(0.0, _float(cfg.get("max_order_value_pct"), 0.0))
    account_allocation_pct = max(0.0, min(1.0, _float(cfg.get("account_allocation_pct"), 0.0)))
    reserve_cash = max(cash_buffer_floor, equity * min_cash_buffer_pct)
    available_after_cash_buffer = max(0.0, equity - reserve_cash)
    account_target_capital = equity * account_allocation_pct
    effective_investable_equity = max(0.0, min(account_target_capital, available_after_cash_buffer))
    max_order_value = equity * max_order_value_pct

    issues: List[str] = []
    review_flags: List[str] = []
    if equity <= 0.0:
        issues.append("missing_effective_equity")
    if equity > 0.0 and reserve_cash + 1e-9 >= equity:
        issues.append("cash_buffer_exhausts_effective_equity")
    if min_trade_value > 0.0 and max_order_value > 0.0 and min_trade_value > max_order_value + 1e-9:
        issues.append("min_trade_value_exceeds_max_order_value")
    if min_trade_value > 0.0 and effective_investable_equity + 1e-9 < min_trade_value:
        issues.append("investable_equity_below_min_trade_value")
    if fee_floor_one_side_bps >= 10.0 and equity <= 5_000.0:
        review_flags.append("high_fee_drag_for_small_account")
    if odd_lot_discount_risk:
        review_flags.append("odd_lot_discount_risk")

    if issues:
        if "missing_effective_equity" in issues:
            status = "CONFIG_UNKNOWN_EQUITY"
            action = "refresh broker snapshot before deciding market readiness"
        elif "cash_buffer_exhausts_effective_equity" in issues:
            status = "CONFIG_BLOCKED_CASH_BUFFER"
            action = "apply small-account execution profile or reduce cash_buffer_floor before paper submit"
        elif "min_trade_value_exceeds_max_order_value" in issues:
            status = "CONFIG_BLOCKED_MIN_TRADE_GT_MAX_ORDER"
            action = "align min_trade_value with account cap and max_order_value_pct before paper submit"
        else:
            status = "CONFIG_BLOCKED_INVESTABLE_LT_MIN_TRADE"
            action = "reduce min_trade_value or allocation constraints through small-account profile before paper submit"
    elif review_flags:
        status = "CONFIG_REVIEW_FEE_LOT_FRICTION"
        action = "prefer ETF/board-lot-feasible names and require post-cost edge before submit"
    else:
        status = "CONFIG_TRADABLE"
        action = "configuration allows small-account paper planning; use artifact reasons for next blocker"

    return {
        "small_account_feasibility_status": status,
        "small_account_feasibility_reason": ",".join(issues + review_flags) or "PASS",
        "small_account_feasibility_action": action,
        "effective_readiness_equity": float(equity),
        "effective_cash_buffer_floor": float(cash_buffer_floor),
        "effective_min_cash_buffer_pct": float(min_cash_buffer_pct),
        "effective_reserve_cash": float(reserve_cash),
        "effective_available_after_cash_buffer": float(available_after_cash_buffer),
        "effective_account_allocation_pct": float(account_allocation_pct),
        "effective_investable_equity": float(effective_investable_equity),
        "effective_min_trade_value": float(min_trade_value),
        "effective_max_order_value_pct": float(max_order_value_pct),
        "effective_max_order_value": float(max_order_value),
        "effective_allow_whole_share_preferred_buy_override": bool(
            cfg.get("allow_whole_share_preferred_buy_override", False)
        ),
        "effective_allow_fractional_qty": bool(cfg.get("allow_fractional_qty", False)),
    }


def _submit_quality_thresholds(supervisor_config: Mapping[str, Any]) -> Dict[str, Any]:
    raw = dict(supervisor_config.get("auto_order_readiness") or {})
    return {
        "min_submit_net_edge_bps": _float(raw.get("min_submit_net_edge_bps"), 8.0),
        "min_submit_edge_margin_bps": _float(raw.get("min_submit_edge_margin_bps"), 3.0),
        "max_submit_expected_cost_bps": _float(raw.get("max_submit_expected_cost_bps"), 35.0),
        "require_limit_order_for_submit": _bool(raw.get("require_limit_order_for_submit", True)),
        "max_submit_order_adv_pct": _float(raw.get("max_submit_order_adv_pct"), 0.001),
        "high_quality_min_net_edge_bps": _float(raw.get("high_quality_min_net_edge_bps"), 16.0),
        "high_quality_min_edge_margin_bps": _float(raw.get("high_quality_min_edge_margin_bps"), 8.0),
        "high_quality_max_expected_cost_bps": _float(raw.get("high_quality_max_expected_cost_bps"), 25.0),
    }


def _mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values]
    return round(sum(clean) / len(clean), 6) if clean else 0.0


def _status_ok(value: Any, allowed: set[str]) -> bool:
    text = str(value or "").strip().upper()
    return text in allowed


def _submit_quality_tier(
    *,
    status: str,
    min_net_edge_bps: float,
    min_edge_margin_bps: float,
    max_expected_cost_bps: float,
    thresholds: Mapping[str, Any],
) -> str:
    if str(status or "").strip().upper() != "PASS":
        return "NONE"
    if (
        float(min_net_edge_bps) >= _float(thresholds.get("high_quality_min_net_edge_bps"), 16.0)
        and float(min_edge_margin_bps) >= _float(thresholds.get("high_quality_min_edge_margin_bps"), 8.0)
        and float(max_expected_cost_bps) <= _float(thresholds.get("high_quality_max_expected_cost_bps"), 25.0)
    ):
        return "HIGH"
    return "PASS"


def _submit_quality_summary(
    *,
    execution_plan_rows: Iterable[Mapping[str, Any]],
    order_count: int,
    thresholds: Mapping[str, Any],
) -> Dict[str, Any]:
    planned_rows = [
        dict(row)
        for row in list(execution_plan_rows or [])
        if str(row.get("status") or "").strip().upper() in {"PLANNED", "SUBMITTED", "FILLED"}
    ]
    if order_count <= 0:
        return {
            "submit_quality_status": "NO_ORDERS",
            "submit_quality_reason": "no_planned_orders",
            "submit_quality_tier": "NONE",
            "submit_quality_order_count": 0,
        }
    if not planned_rows:
        return {
            "submit_quality_status": "UNKNOWN",
            "submit_quality_reason": "missing_execution_plan_rows",
            "submit_quality_tier": "NONE",
            "submit_quality_order_count": int(order_count),
        }

    edge_values = [_float(row.get("expected_edge_bps"), 0.0) for row in planned_rows]
    cost_values = [_float(row.get("expected_cost_bps"), 0.0) for row in planned_rows]
    threshold_values = [_float(row.get("edge_gate_threshold_bps"), 0.0) for row in planned_rows]
    net_edge_values = [edge - cost for edge, cost in zip(edge_values, cost_values)]
    margin_values: List[float] = []
    for row, edge, threshold in zip(planned_rows, edge_values, threshold_values):
        explicit_margin = str(row.get("whole_share_edge_margin_bps") or "").strip()
        if explicit_margin:
            margin_values.append(_float(explicit_margin, edge - threshold))
        else:
            margin_values.append(edge - threshold)
    order_adv_values = [_float(row.get("dynamic_order_adv_pct"), 0.0) for row in planned_rows]
    order_types = sorted({str(row.get("execution_order_type") or "").strip().upper() or "UNKNOWN" for row in planned_rows})

    blocked_reasons: List[str] = []
    if min(net_edge_values or [0.0]) < _float(thresholds.get("min_submit_net_edge_bps"), 8.0):
        blocked_reasons.append("net_edge_below_min")
    if min(margin_values or [0.0]) < _float(thresholds.get("min_submit_edge_margin_bps"), 3.0):
        blocked_reasons.append("edge_margin_below_min")
    if max(cost_values or [0.0]) > _float(thresholds.get("max_submit_expected_cost_bps"), 35.0):
        blocked_reasons.append("expected_cost_above_max")
    if max(order_adv_values or [0.0]) > _float(thresholds.get("max_submit_order_adv_pct"), 0.001):
        blocked_reasons.append("order_adv_above_max")
    if _bool(thresholds.get("require_limit_order_for_submit", True)) and any(order_type != "LMT" for order_type in order_types):
        blocked_reasons.append("non_limit_order")

    bad_edge_gate = sum(1 for row in planned_rows if not _status_ok(row.get("edge_gate_status"), {"PASS"}))
    bad_quality = sum(1 for row in planned_rows if not _status_ok(row.get("quality_status"), {"QUALITY_OK"}))
    bad_market_rule = sum(1 for row in planned_rows if not _status_ok(row.get("market_rule_status"), {"RULES_OK"}))
    bad_shadow = sum(1 for row in planned_rows if not _status_ok(row.get("shadow_review_status"), {"AUTO_OK", ""}))
    bad_manual = sum(1 for row in planned_rows if not _status_ok(row.get("manual_review_status"), {"AUTO_OK", ""}))
    if bad_edge_gate:
        blocked_reasons.append("edge_gate_not_pass")
    if bad_quality:
        blocked_reasons.append("quality_not_ok")
    if bad_market_rule:
        blocked_reasons.append("market_rule_not_ok")
    if bad_shadow:
        blocked_reasons.append("shadow_review_not_ok")
    if bad_manual:
        blocked_reasons.append("manual_review_not_ok")

    status = "BLOCKED" if blocked_reasons else "PASS"
    min_expected_edge_bps = round(min(edge_values or [0.0]), 6)
    max_expected_cost_bps = round(max(cost_values or [0.0]), 6)
    min_net_edge_bps = round(min(net_edge_values or [0.0]), 6)
    min_edge_margin_bps = round(min(margin_values or [0.0]), 6)
    tier = _submit_quality_tier(
        status=status,
        min_net_edge_bps=min_net_edge_bps,
        min_edge_margin_bps=min_edge_margin_bps,
        max_expected_cost_bps=max_expected_cost_bps,
        thresholds=thresholds,
    )
    return {
        "submit_quality_status": status,
        "submit_quality_reason": ",".join(blocked_reasons) if blocked_reasons else "PASS",
        "submit_quality_tier": tier,
        "submit_quality_order_count": int(len(planned_rows)),
        "submit_quality_min_expected_edge_bps": min_expected_edge_bps,
        "submit_quality_avg_expected_edge_bps": _mean(edge_values),
        "submit_quality_max_expected_cost_bps": max_expected_cost_bps,
        "submit_quality_avg_expected_cost_bps": _mean(cost_values),
        "submit_quality_min_net_edge_bps": min_net_edge_bps,
        "submit_quality_avg_net_edge_bps": _mean(net_edge_values),
        "submit_quality_min_edge_margin_bps": min_edge_margin_bps,
        "submit_quality_avg_edge_margin_bps": _mean(margin_values),
        "submit_quality_max_order_adv_pct": round(max(order_adv_values or [0.0]), 8),
        "submit_quality_order_types": ",".join(order_types),
        "submit_quality_bad_edge_gate_count": int(bad_edge_gate),
        "submit_quality_bad_quality_count": int(bad_quality),
        "submit_quality_bad_market_rule_count": int(bad_market_rule),
        "submit_quality_bad_shadow_review_count": int(bad_shadow),
        "submit_quality_bad_manual_review_count": int(bad_manual),
        "submit_quality_min_net_edge_threshold_bps": _float(thresholds.get("min_submit_net_edge_bps"), 8.0),
        "submit_quality_min_edge_margin_threshold_bps": _float(thresholds.get("min_submit_edge_margin_bps"), 3.0),
        "submit_quality_max_expected_cost_threshold_bps": _float(thresholds.get("max_submit_expected_cost_bps"), 35.0),
        "submit_quality_max_order_adv_threshold_pct": _float(thresholds.get("max_submit_order_adv_pct"), 0.001),
        "submit_quality_require_limit_order": _bool(thresholds.get("require_limit_order_for_submit", True)),
        "submit_quality_high_min_net_edge_threshold_bps": _float(
            thresholds.get("high_quality_min_net_edge_bps"),
            16.0,
        ),
        "submit_quality_high_min_edge_margin_threshold_bps": _float(
            thresholds.get("high_quality_min_edge_margin_bps"),
            8.0,
        ),
        "submit_quality_high_max_expected_cost_threshold_bps": _float(
            thresholds.get("high_quality_max_expected_cost_bps"),
            25.0,
        ),
    }


def _readiness_status(
    *,
    run_execution: bool,
    submit_configured: bool,
    research_only: bool,
    execution_summary: Mapping[str, Any],
) -> tuple[str, str, str]:
    if research_only:
        return "RESEARCH_ONLY", "RESEARCH_ONLY", "keep research-only; do not submit orders"
    if not run_execution:
        return "DISABLED", "EXECUTION_DISABLED", "enable paper/execution only after market rules and data coverage pass"
    if not execution_summary:
        return "NEEDS_ARTIFACT", "MISSING_EXECUTION_ARTIFACT", "run investment report, paper, then execution dry-run"

    primary_reason = _execution_primary_reason(execution_summary)
    readiness = str(execution_summary.get("paper_submit_readiness_status") or "").strip().upper()
    submitted_orders = _int(execution_summary.get("submitted_order_count"), 0)
    order_count = _int(execution_summary.get("order_count"), 0)
    paper_ready = bool(execution_summary.get("paper_submit_ready", False))

    if submitted_orders > 0:
        return "PAPER_SUBMITTED", primary_reason, "monitor fills, slippage, broker ack, and post-cost edge"
    if primary_reason == "IBKR_GATEWAY_UNAVAILABLE":
        return "BLOCKED", primary_reason, "start or unlock IB Gateway paper API, then rerun no-submit"
    if readiness == "MARKET_CLOSED" or primary_reason == "MARKET_CLOSED_FOR_SUBMIT":
        return "PLANNED_MARKET_CLOSED", primary_reason, "rerun no-submit during local regular session before any submit"
    if paper_ready:
        action = "operator review, then paper submit one small order only" if submit_configured else "enable paper submit only after operator approval"
        return "READY_FOR_PAPER_REVIEW", primary_reason, action
    if order_count > 0:
        return "PAPER_PLANNED_NEEDS_REVIEW", primary_reason, "review submit readiness and hard blocking diagnostics"
    return "BLOCKED", primary_reason, str(execution_summary.get("no_order_primary_action") or _default_next_action(primary_reason))


def _execution_primary_reason(execution_summary: Mapping[str, Any]) -> str:
    direct = str(execution_summary.get("primary_no_order_reason") or "").strip()
    if direct:
        return direct
    if _int(execution_summary.get("order_count"), 0) > 0:
        return "ORDERS_PLANNED_NOT_SUBMITTED"
    blocked_count = _int(execution_summary.get("blocked_order_count"), 0)
    if blocked_count <= 0:
        return "UNKNOWN"
    blocked_reasons = [
        ("BLOCKED_QUALITY", _int(execution_summary.get("blocked_quality_order_count"), 0)),
        ("BLOCKED_OPPORTUNITY", _int(execution_summary.get("blocked_opportunity_order_count"), 0)),
        ("BLOCKED_EDGE", _int(execution_summary.get("blocked_edge_order_count"), 0)),
        ("BLOCKED_MARKET_RULE", _int(execution_summary.get("blocked_market_rule_order_count"), 0)),
        ("BLOCKED_LIQUIDITY", _int(execution_summary.get("blocked_liquidity_order_count"), 0)),
        ("BLOCKED_PENDING_BROKER_ORDER", _int(execution_summary.get("blocked_pending_broker_order_count"), 0)),
        ("REVIEW_MANUAL", _int(execution_summary.get("blocked_manual_review_order_count"), 0)),
        ("REVIEW_SHADOW_ML", _int(execution_summary.get("blocked_shadow_review_order_count"), 0)),
        ("REVIEW_MARKET_STRUCTURE", _int(execution_summary.get("blocked_market_structure_review_order_count"), 0)),
        ("REVIEW_SIZE", _int(execution_summary.get("blocked_size_review_order_count"), 0)),
        ("DEFERRED_RISK_ALERT", _int(execution_summary.get("blocked_risk_alert_order_count"), 0)),
    ]
    reason, count = max(blocked_reasons, key=lambda item: item[1])
    return reason if count > 0 else "BLOCKED_UNKNOWN"


def _default_next_action(primary_reason: str) -> str:
    reason = str(primary_reason or "").upper()
    if reason == "BLOCKED_QUALITY":
        return "refresh market report and inspect execution_ready/quality gates before submit"
    if reason == "BLOCKED_OPPORTUNITY":
        return "rerun opportunity scan after event/window clears; do not bypass event gate"
    if reason == "BLOCKED_EDGE":
        return "review expected edge versus post-cost threshold; do not lower edge gate without evidence"
    if reason == "BLOCKED_MARKET_RULE":
        return "inspect board lot, research-only, and market-rule constraints"
    if reason == "BLOCKED_LIQUIDITY":
        return "reduce order size or use more liquid ETF-first candidates"
    if reason == "BLOCKED_PENDING_BROKER_ORDER":
        return "wait for existing broker order to fill/cancel before submitting duplicate"
    if reason.startswith("REVIEW_"):
        return "complete manual/shadow/market-structure review before submit"
    if reason == "DEFERRED_RISK_ALERT":
        return "wait for risk alert to stabilize before adding exposure"
    return "review primary no-order reason"


def build_market_readiness_rows(
    *,
    base_dir: Path,
    supervisor_config: Mapping[str, Any],
    runtime_root: Path,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    readiness_cfg = dict(supervisor_config.get("market_readiness") or {})
    max_execution_artifact_age_hours = _float(readiness_cfg.get("max_execution_artifact_age_hours"), 24.0)
    submit_quality_thresholds = _submit_quality_thresholds(supervisor_config)
    for market_cfg_raw in list(supervisor_config.get("markets", []) or []):
        market_cfg = dict(market_cfg_raw or {})
        market = resolve_market_code(str(market_cfg.get("market") or market_cfg.get("name") or ""))
        if not market:
            continue
        market_enabled = _bool(market_cfg.get("enabled", True))
        for item_raw in list(market_cfg.get("reports", []) or []):
            item = dict(item_raw or {})
            if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                continue
            watchlist_yaml = str(item.get("watchlist_yaml") or "").strip()
            out_dir = str(item.get("out_dir") or "reports_investment")
            portfolio_id = str(item.get("portfolio_id") or f"{market}:{Path(watchlist_yaml).stem}").strip()
            run_execution = bool(item.get("run_investment_execution", False))
            submit_configured = bool(item.get("submit_investment_execution", False))
            ibkr_config = str(item.get("ibkr_config") or market_cfg.get("ibkr_config") or "").strip() or None
            ibkr_path = Path(market_config_path(base_dir, market, ibkr_config)).resolve()
            ibkr_cfg = _load_json_dict(ibkr_path)
            if not ibkr_cfg:
                ibkr_cfg = _load_yaml_dict(ibkr_path)

            structure = load_market_structure(
                base_dir,
                market,
                str(item.get("market_structure_config") or ibkr_cfg.get("market_structure_config") or f"config/market_structure_{market.lower()}.yaml"),
            )
            research_only = bool(item.get("research_only", False) or structure.research_only)
            execution_path, execution_summary = _latest_artifact(
                base_dir=base_dir,
                runtime_root=runtime_root,
                market=market,
                out_dir=out_dir,
                watchlist_yaml=watchlist_yaml,
                filename="investment_execution_summary.json",
            )
            execution_plan_path = _latest_artifact_path(
                base_dir=base_dir,
                runtime_root=runtime_root,
                market=market,
                out_dir=out_dir,
                watchlist_yaml=watchlist_yaml,
                filename="investment_execution_plan.csv",
            )
            snapshot_path, snapshot_summary = _latest_artifact(
                base_dir=base_dir,
                runtime_root=runtime_root,
                market=market,
                out_dir=out_dir,
                watchlist_yaml=watchlist_yaml,
                filename="investment_broker_snapshot_summary.json",
            )
            broker_equity = _float(execution_summary.get("broker_equity"), _float(snapshot_summary.get("broker_equity"), 0.0))
            broker_cash = _float(execution_summary.get("broker_cash"), _float(snapshot_summary.get("broker_cash"), 0.0))
            execution_artifact_age_hours = _artifact_age_hours(execution_path, now=now)
            artifact_health_status, artifact_health_reason, artifact_health_action = _artifact_source_health(
                path=execution_path,
                execution_summary=execution_summary,
                age_hours=execution_artifact_age_hours,
                max_age_hours=max_execution_artifact_age_hours,
            )
            structure_summary = market_structure_summary(structure, broker_equity=broker_equity)
            execution_payload = _effective_execution_payload(
                base_dir=base_dir,
                execution_config=str(item.get("execution_config") or f"config/investment_execution_{market.lower()}.yaml"),
                ibkr_cfg=ibkr_cfg,
                broker_equity=broker_equity,
            )
            feasibility = _small_account_feasibility(
                execution_payload=execution_payload,
                fee_floor_one_side_bps=_float(structure_summary.get("fee_floor_one_side_bps"), 0.0),
                odd_lot_discount_risk=bool(structure_summary.get("odd_lot_discount_risk", False)),
            )
            readiness_status, primary_reason, next_action = _readiness_status(
                run_execution=run_execution,
                submit_configured=submit_configured,
                research_only=research_only,
                execution_summary=execution_summary,
            )
            feasibility_status = str(feasibility.get("small_account_feasibility_status") or "")
            if feasibility_status.startswith("CONFIG_BLOCKED") and readiness_status not in {"RESEARCH_ONLY", "DISABLED"}:
                readiness_status = "CONFIG_BLOCKED"
                primary_reason = feasibility_status
                next_action = str(feasibility.get("small_account_feasibility_action") or next_action)
            elif feasibility_status == "CONFIG_REVIEW_FEE_LOT_FRICTION" and readiness_status in {"BLOCKED", "NEEDS_ARTIFACT", "READY_FOR_PAPER_REVIEW"}:
                next_action = f"{next_action}; {feasibility.get('small_account_feasibility_action')}"
            if artifact_health_status == "STALE" and readiness_status not in {"RESEARCH_ONLY", "DISABLED", "CONFIG_BLOCKED"}:
                readiness_status = "NEEDS_REFRESH"
                primary_reason = "STALE_EXECUTION_ARTIFACT"
                next_action = artifact_health_action
            order_count = _int(execution_summary.get("order_count"), 0)
            submit_quality = _submit_quality_summary(
                execution_plan_rows=_load_csv_rows(execution_plan_path),
                order_count=order_count,
                thresholds=submit_quality_thresholds,
            )
            account_equity_cap = _float(execution_payload.get("account_equity_cap"), 0.0)
            equity_cap_applied = bool(account_equity_cap > 0.0 and broker_equity > account_equity_cap + 1e-9)
            rows.append(
                {
                    "market": market,
                    "market_enabled": bool(market_enabled),
                    "watchlist": Path(watchlist_yaml).stem,
                    "portfolio_id": portfolio_id,
                    "report_dir": str(execution_path.parent if execution_path else (_candidate_report_dirs(base_dir=base_dir, runtime_root=runtime_root, market=market, out_dir=out_dir, watchlist_yaml=watchlist_yaml)[0])),
                    "account_mode": str(ibkr_cfg.get("mode") or "paper").strip().lower(),
                    "research_only": bool(research_only),
                    "run_investment_execution": bool(run_execution),
                    "submit_investment_execution": bool(submit_configured),
                    "account_profile_name": str(execution_payload.get("account_profile_name") or ""),
                    "account_profile_label": str(execution_payload.get("account_profile_label") or ""),
                    "account_equity_cap": account_equity_cap,
                    "equity_cap_applied": bool(equity_cap_applied),
                    "readiness_status": readiness_status,
                    "primary_reason": primary_reason,
                    "next_action": next_action,
                    "artifact_health_status": artifact_health_status,
                    "artifact_health_reason": artifact_health_reason,
                    "artifact_health_action": artifact_health_action,
                    "execution_artifact_age_hours": float(execution_artifact_age_hours),
                    "max_execution_artifact_age_hours": float(max_execution_artifact_age_hours),
                    **feasibility,
                    "paper_submit_ready": bool(execution_summary.get("paper_submit_ready", False)),
                    "paper_submit_readiness_status": str(execution_summary.get("paper_submit_readiness_status") or ""),
                    "order_count": order_count,
                    "submitted_order_count": _int(execution_summary.get("submitted_order_count"), 0),
                    "blocked_order_count": _int(execution_summary.get("blocked_order_count"), 0),
                    "planned_order_symbols": str(execution_summary.get("planned_order_symbols") or ""),
                    "planned_gross_order_value": _float(execution_summary.get("planned_gross_order_value"), _float(execution_summary.get("order_value"), 0.0)),
                    "planned_buy_order_value": _float(execution_summary.get("planned_buy_order_value"), 0.0),
                    "planned_sell_order_value": _float(execution_summary.get("planned_sell_order_value"), 0.0),
                    "planned_net_cash_order_value": _float(execution_summary.get("planned_net_cash_order_value"), 0.0),
                    "broker_equity": broker_equity,
                    "broker_cash": broker_cash,
                    "broker_position_count": _int(snapshot_summary.get("position_count"), 0),
                    "execution_summary_path": str(execution_path or ""),
                    "execution_plan_path": str(execution_plan_path or ""),
                    "broker_snapshot_path": str(snapshot_path or ""),
                    **submit_quality,
                    "market_scope": str(structure.market_scope or ""),
                    "benchmark_symbol": str(structure.benchmark_symbol or ""),
                    "buy_lot_multiple": _int(structure_summary.get("buy_lot_multiple"), 1),
                    "odd_lot_discount_risk": bool(structure_summary.get("odd_lot_discount_risk", False)),
                    "day_turnaround_allowed": bool(structure_summary.get("day_turnaround_allowed", True)),
                    "fee_floor_one_side_bps": _float(structure_summary.get("fee_floor_one_side_bps"), 0.0),
                    "recommended_rebalance_frequency": str(structure_summary.get("rebalance_frequency") or ""),
                    "max_rebalances_per_week": _int(structure_summary.get("max_rebalances_per_week"), 0),
                }
            )
    return rows


def build_market_readiness_summary(rows: List[Mapping[str, Any]]) -> Dict[str, Any]:
    status_counts: Dict[str, int] = {}
    market_counts: Dict[str, int] = {}
    ready_rows = 0
    blocked_rows = 0
    for row in rows:
        status = str(row.get("readiness_status") or "UNKNOWN")
        market = str(row.get("market") or "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
        market_counts[market] = market_counts.get(market, 0) + 1
        if status in {"READY_FOR_PAPER_REVIEW", "PAPER_SUBMITTED"}:
            ready_rows += 1
        if status in {"BLOCKED", "NEEDS_ARTIFACT", "CONFIG_BLOCKED", "NEEDS_REFRESH"}:
            blocked_rows += 1
    return {
        "portfolio_count": int(len(rows)),
        "market_count": int(len(market_counts)),
        "status_counts": status_counts,
        "market_counts": market_counts,
        "ready_or_submitted_count": int(ready_rows),
        "blocked_or_missing_count": int(blocked_rows),
        "summary_text": (
            f"markets={len(market_counts)} portfolios={len(rows)} ready_or_submitted={ready_rows} "
            f"blocked_or_missing={blocked_rows}"
        ),
    }


def _market_preparation_score(row: Mapping[str, Any]) -> float:
    status = str(row.get("readiness_status") or "").upper()
    artifact_status = str(row.get("artifact_health_status") or "").upper()
    feasibility_status = str(row.get("small_account_feasibility_status") or "").upper()
    score = 100.0
    if bool(row.get("research_only", False)):
        score -= 80.0
    if status in {"READY_FOR_PAPER_REVIEW", "PAPER_PLANNED_NEEDS_REVIEW", "PLANNED_MARKET_CLOSED"}:
        score += 25.0
    if _int(row.get("order_count"), 0) > 0:
        score += 15.0
    if artifact_status == "STALE":
        score -= 20.0
    if artifact_status == "DEGRADED_GATEWAY":
        score -= 25.0
    if feasibility_status.startswith("CONFIG_BLOCKED"):
        score -= 55.0
    elif feasibility_status == "CONFIG_REVIEW_FEE_LOT_FRICTION":
        score -= 20.0
    score -= min(30.0, max(0.0, _float(row.get("fee_floor_one_side_bps"), 0.0) * 2.0))
    if bool(row.get("odd_lot_discount_risk", False)):
        score -= 15.0
    if str(row.get("market") or "").upper() == "US":
        score += 5.0
    return round(float(score), 4)


def _market_preparation_tier(row: Mapping[str, Any]) -> str:
    status = str(row.get("readiness_status") or "").upper()
    artifact_status = str(row.get("artifact_health_status") or "").upper()
    feasibility_status = str(row.get("small_account_feasibility_status") or "").upper()
    if bool(row.get("research_only", False)):
        return "RESEARCH_ONLY"
    if feasibility_status.startswith("CONFIG_BLOCKED"):
        return "FIX_CONFIG_FIRST"
    if status in {"READY_FOR_PAPER_REVIEW", "PAPER_PLANNED_NEEDS_REVIEW"}:
        return "REVIEW_FOR_PAPER"
    if status == "PLANNED_MARKET_CLOSED":
        return "WAIT_MARKET_SESSION"
    if artifact_status in {"STALE", "DEGRADED_GATEWAY", "MISSING"} or status in {"NEEDS_REFRESH", "NEEDS_ARTIFACT"}:
        return "REFRESH_ARTIFACTS"
    if feasibility_status == "CONFIG_REVIEW_FEE_LOT_FRICTION":
        return "REVIEW_FRICTION"
    return "INVESTIGATE_BLOCKER"


def build_market_preparation_plan(rows: List[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    plan: List[Dict[str, Any]] = []
    for row in rows:
        score = _market_preparation_score(row)
        tier = _market_preparation_tier(row)
        plan.append(
            {
                "market": str(row.get("market") or ""),
                "portfolio_id": str(row.get("portfolio_id") or ""),
                "priority_score": float(score),
                "priority_tier": tier,
                "readiness_status": str(row.get("readiness_status") or ""),
                "primary_reason": str(row.get("primary_reason") or ""),
                "artifact_health_status": str(row.get("artifact_health_status") or ""),
                "small_account_feasibility_status": str(row.get("small_account_feasibility_status") or ""),
                "order_count": _int(row.get("order_count"), 0),
                "planned_gross_order_value": _float(row.get("planned_gross_order_value"), 0.0),
                "fee_floor_one_side_bps": _float(row.get("fee_floor_one_side_bps"), 0.0),
                "odd_lot_discount_risk": bool(row.get("odd_lot_discount_risk", False)),
                "next_action": str(row.get("next_action") or ""),
            }
        )
    plan.sort(
        key=lambda item: (
            -float(item.get("priority_score", 0.0) or 0.0),
            str(item.get("market") or ""),
            str(item.get("portfolio_id") or ""),
        )
    )
    for idx, item in enumerate(plan, start=1):
        item["preparation_rank"] = int(idx)
    return plan


def build_market_readiness_payload(
    *,
    base_dir: Path,
    supervisor_config: Mapping[str, Any],
    config_path: Path,
    runtime_root: Path,
) -> Dict[str, Any]:
    rows = build_market_readiness_rows(
        base_dir=base_dir,
        supervisor_config=supervisor_config,
        runtime_root=runtime_root,
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": "2026Q2.market_readiness.v1",
        "config_path": str(config_path),
        "runtime_root": str(runtime_root),
        "summary": build_market_readiness_summary(rows),
        "preparation_plan": build_market_preparation_plan(rows),
        "rows": rows,
    }
