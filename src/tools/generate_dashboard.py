from __future__ import annotations

import argparse
import csv
import html
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..analysis.tracking import STATUS_LABELS
from ..common.markets import market_config_path, resolve_market_code, symbol_matches_market
from ..common.runtime_paths import resolve_repo_path, resolve_scoped_runtime_path, scope_from_ibkr_config
from ..common.storage import Storage

BASE_DIR = Path(__file__).resolve().parents[2]
CONTROL_BUTTON_LABELS: Dict[str, str] = {
    "run_investment_paper": "跑 Dry Run",
    "force_local_paper_ledger": "保留本地账本",
    "run_investment_execution": "跑执行",
    "submit_investment_execution": "自动提交执行",
    "run_investment_guard": "跑 Guard",
    "submit_investment_guard": "自动提交 Guard",
    "run_investment_opportunity": "跑 Opportunity",
}
EXECUTION_MODE_LABELS: Dict[str, str] = {
    "AUTO": "自动执行",
    "REVIEW_ONLY": "只保留人工审核",
    "PAUSED": "暂停自动执行",
}


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate a static dashboard from supervisor and report outputs.")
    ap.add_argument("--config", default="config/supervisor.yaml", help="Path to supervisor config.")
    ap.add_argument("--out_dir", default="reports_supervisor", help="Output directory for dashboard html/json.")
    return ap.parse_args(argv)


def _resolve_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _load_yaml(path: str) -> Dict[str, Any]:
    resolved = _resolve_path(path)
    with resolved.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _infer_execution_control_mode(row: Dict[str, Any]) -> str:
    run_execution = bool(row.get("run_investment_execution", False))
    submit_execution = bool(row.get("submit_investment_execution", False))
    run_guard = bool(row.get("run_investment_guard", False))
    submit_guard = bool(row.get("submit_investment_guard", False))
    if not run_execution and not run_guard:
        return "PAUSED"
    if not submit_execution and not submit_guard:
        return "REVIEW_ONLY"
    return "AUTO"


def _read_csv_rows(path: Path, limit: int = 10) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return []
    return rows[:limit]


def _read_all_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _status_count_from_breakdown(text: str, status: str) -> int:
    wanted = str(status or "").strip().upper()
    if not wanted:
        return 0
    for part in str(text or "").split(","):
        name, _, value = part.partition(":")
        if str(name or "").strip().upper() != wanted:
            continue
        try:
            return int(float(value or 0))
        except Exception:
            return 0
    return 0


def _current_iso_week_label(now_dt: datetime) -> tuple[str, str]:
    iso_year, iso_week, iso_weekday = now_dt.isocalendar()
    week_start = (now_dt - timedelta(days=int(iso_weekday) - 1)).date().isoformat()
    return f"{iso_year}-W{iso_week:02d}", week_start


def _normalize_execution_weekly_row(
    raw: Dict[str, Any],
    *,
    default_week: str = "",
    default_week_start: str = "",
) -> Dict[str, Any]:
    if not raw:
        return {}
    submitted_order_rows = int(float(raw.get("submitted_order_rows", 0) or 0))
    execution_order_rows = int(float(raw.get("execution_order_rows", 0) or 0))
    planned_order_rows = int(float(raw.get("planned_order_rows", 0) or 0))
    fill_rows = int(float(raw.get("fill_rows", raw.get("fill_count", 0)) or 0))
    filled_order_rows = (
        int(float(raw.get("filled_order_rows", 0) or 0))
        if str(raw.get("filled_order_rows", "")).strip()
        else min(fill_rows, submitted_order_rows)
    )
    filled_with_audit_rows = (
        int(float(raw.get("filled_with_audit_rows", 0) or 0))
        if str(raw.get("filled_with_audit_rows", "")).strip()
        else min(fill_rows, submitted_order_rows)
    )
    blocked_opportunity_rows = (
        int(float(raw.get("blocked_opportunity_rows", 0) or 0))
        if str(raw.get("blocked_opportunity_rows", "")).strip()
        else _status_count_from_breakdown(str(raw.get("status_breakdown", "") or ""), "BLOCKED_OPPORTUNITY")
    )
    fill_rate_status = (
        float(raw.get("fill_rate_status", 0.0) or 0.0)
        if str(raw.get("fill_rate_status", "")).strip()
        else (float(filled_order_rows) / float(submitted_order_rows) if submitted_order_rows > 0 else None)
    )
    fill_rate_audit = (
        float(raw.get("fill_rate_audit", 0.0) or 0.0)
        if str(raw.get("fill_rate_audit", "")).strip()
        else (float(filled_with_audit_rows) / float(submitted_order_rows) if submitted_order_rows > 0 else None)
    )
    return {
        "week": str(raw.get("week", "") or default_week),
        "week_start": str(raw.get("week_start", "") or default_week_start),
        "market": str(raw.get("market", "") or ""),
        "portfolio_id": str(raw.get("portfolio_id", "") or ""),
        "execution_run_rows": int(float(raw.get("execution_run_rows", raw.get("execution_runs", 0)) or 0)),
        "submitted_runs": int(float(raw.get("submitted_runs", 0) or 0)),
        # weekly review 里既有 execution_order_rows，也会在成本口径下补一个 planned_order_rows。
        # dashboard 的“本周执行质量”更关心真实执行订单规模，因此优先显示 execution_order_rows。
        "planned_order_rows": execution_order_rows if execution_order_rows > 0 else planned_order_rows,
        "submitted_order_rows": submitted_order_rows,
        "filled_order_rows": filled_order_rows,
        "filled_with_audit_rows": filled_with_audit_rows,
        "blocked_opportunity_rows": blocked_opportunity_rows,
        "error_order_rows": int(float(raw.get("error_order_rows", 0) or 0)),
        "fill_rows": fill_rows,
        "commission_total": float(raw.get("commission_total", 0.0) or 0.0),
        "realized_net_pnl": float(raw.get("realized_net_pnl", 0.0) or 0.0),
        "fill_rate_status": fill_rate_status,
        "fill_rate_audit": fill_rate_audit,
        "fill_rate": fill_rate_audit,
        "avg_actual_slippage_bps": float(raw.get("avg_actual_slippage_bps", 0.0) or 0.0)
        if str(raw.get("avg_actual_slippage_bps", "")).strip()
        else None,
    }


def _load_weekly_shadow_review_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("shadow_review_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_shadow_review_summary.csv")


def _load_preflight_summary(preflight_dir: Path) -> Dict[str, Any]:
    return _load_json(preflight_dir / "supervisor_preflight_summary.json")


def _load_ibkr_history_probe_summary(preflight_dir: Path) -> Dict[str, Any]:
    return _load_json(preflight_dir / "ibkr_history_probe_summary.json")


def _load_weekly_attribution_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("attribution_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_attribution_summary.csv")


def _load_weekly_risk_review_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("risk_review_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_risk_review_summary.csv")


def _load_weekly_risk_feedback_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("risk_feedback_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_risk_feedback_summary.csv")


def _load_weekly_execution_feedback_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("execution_feedback_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_execution_feedback_summary.csv")


def _load_weekly_execution_session_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("execution_session_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_execution_session_summary.csv")


def _load_weekly_execution_hotspot_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("execution_hotspot_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_execution_hotspot_summary.csv")


def _load_weekly_feedback_calibration_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_calibration_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_calibration_summary.csv")


def _load_weekly_feedback_automation_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_automation_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_automation_summary.csv")


def _load_weekly_feedback_threshold_suggestion_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_suggestion_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_suggestion_summary.csv")


def _load_weekly_feedback_threshold_history_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_history_overview")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_history_overview.csv")


def _load_weekly_feedback_threshold_effect_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_effect_overview")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_effect_overview.csv")


def _load_weekly_feedback_threshold_cohort_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_cohort_overview")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_cohort_overview.csv")


def _load_weekly_feedback_threshold_trial_alert_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_trial_alerts")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_trial_alerts.csv")


def _load_weekly_feedback_threshold_tuning_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("feedback_threshold_tuning_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_feedback_threshold_tuning_summary.csv")


def _weekly_feedback_threshold_override_path(cfg: Dict[str, Any], review_dir: Path) -> Path:
    raw = str(cfg.get("weekly_feedback_thresholds_path", "") or "").strip()
    if raw:
        return _resolve_path(raw)
    return review_dir / "weekly_feedback_threshold_overrides.yaml"


def _load_weekly_feedback_threshold_override_rows(cfg: Dict[str, Any], review_dir: Path) -> List[Dict[str, Any]]:
    path = _weekly_feedback_threshold_override_path(cfg, review_dir)
    raw = _load_json(review_dir / "weekly_review_summary.json")
    tuning_rows = list(raw.get("feedback_threshold_tuning_summary") or []) if isinstance(raw, dict) else []
    suggestion_rows = list(raw.get("feedback_threshold_suggestion_summary") or []) if isinstance(raw, dict) else []
    threshold_cfg = _load_yaml(str(path.relative_to(BASE_DIR))) if path.exists() and str(path).startswith(str(BASE_DIR)) else (
        yaml.safe_load(path.read_text(encoding="utf-8")) or {} if path.exists() else {}
    )
    markets_cfg = dict(threshold_cfg.get("markets") or {}) if isinstance(threshold_cfg, dict) else {}
    tuning_map: Dict[tuple[str, str], Dict[str, Any]] = {}
    suggestion_map: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in tuning_rows:
        if not isinstance(row, dict):
            continue
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if market and feedback_kind:
            tuning_map[(market, feedback_kind)] = dict(row)
    for row in suggestion_rows:
        if not isinstance(row, dict):
            continue
        market = resolve_market_code(str(row.get("market") or ""))
        feedback_kind = str(row.get("feedback_kind") or "").strip().lower()
        if market and feedback_kind:
            suggestion_map[(market, feedback_kind)] = dict(row)

    keys: set[tuple[str, str]] = set()
    for market, kind_map in markets_cfg.items():
        market_code = resolve_market_code(str(market or ""))
        if not market_code or not isinstance(kind_map, dict):
            continue
        for feedback_kind in kind_map.keys():
            kind = str(feedback_kind or "").strip().lower()
            if kind:
                keys.add((market_code, kind))
    keys.update(tuning_map.keys())
    keys.update(suggestion_map.keys())

    rows: List[Dict[str, Any]] = []
    for market_code, feedback_kind in sorted(keys):
        market_cfg = dict(markets_cfg.get(market_code) or {})
        override_row = dict(market_cfg.get(feedback_kind) or {})
        tuning_row = dict(tuning_map.get((market_code, feedback_kind), {}) or {})
        suggestion_row = dict(suggestion_map.get((market_code, feedback_kind), {}) or {})
        tuning_action = str(tuning_row.get("suggestion_action") or "").strip().upper()
        state_label = "基线"
        if override_row:
            if tuning_action in {"KEEP_RELAX", "SOFT_RELAX", "RELAX_AUTO_APPLY"}:
                state_label = "继续放宽中"
            elif tuning_action in {"KEEP_TIGHTEN", "REVIEW_TIGHTEN", "TIGHTEN_AUTO_APPLY"}:
                state_label = "继续收紧中"
            else:
                state_label = "覆盖生效中"
        elif tuning_action == "REVERT_RELAX":
            state_label = "已收回到基线"
        elif tuning_action == "WATCH_COHORT":
            state_label = "观察中"
        rows.append(
            {
                "market": market_code,
                "feedback_kind": feedback_kind,
                "feedback_kind_label": str(
                    tuning_row.get("feedback_kind_label")
                    or suggestion_row.get("feedback_kind_label")
                    or feedback_kind
                ),
                "effective_state_label": state_label,
                "tuning_label": str(tuning_row.get("suggestion_label") or "-"),
                "tuning_action": tuning_action or "-",
                "auto_confidence": float(override_row.get("auto_confidence", 0.0) or 0.0),
                "auto_base_confidence": float(override_row.get("auto_base_confidence", 0.0) or 0.0),
                "auto_calibration_score": float(override_row.get("auto_calibration_score", 0.0) or 0.0),
                "auto_maturity_ratio": float(override_row.get("auto_maturity_ratio", 0.0) or 0.0),
                "reason": str(tuning_row.get("reason") or suggestion_row.get("reason") or "-"),
                "path": str(path),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("effective_state_label") or "") == "继续收紧中" else 1 if str(row.get("effective_state_label") or "") == "继续放宽中" else 2,
            str(row.get("market") or ""),
            str(row.get("feedback_kind_label") or ""),
        )
    )
    return rows


def _load_weekly_labeling_summary(review_dir: Path) -> Dict[str, Any]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary = summary_json.get("labeling_summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _load_weekly_labeling_skip_rows(review_dir: Path) -> List[Dict[str, Any]]:
    summary_json = _load_json(review_dir / "weekly_review_summary.json")
    summary_rows = summary_json.get("labeling_skip_summary")
    if isinstance(summary_rows, list) and summary_rows:
        return [dict(row) for row in summary_rows if isinstance(row, dict)]
    return _read_all_csv_rows(review_dir / "weekly_outcome_labeling_skip_summary.csv")


def _safe_load_yaml_path(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _dashboard_control_fallback(cfg: Dict[str, Any], cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not bool(cfg.get("dashboard_control_enabled", False)):
        return {}
    host = str(cfg.get("dashboard_control_host", "127.0.0.1") or "127.0.0.1")
    port = int(cfg.get("dashboard_control_port", 8765) or 8765)
    portfolios: Dict[str, Dict[str, Any]] = {}
    for card in cards:
        portfolio_id = str(card.get("portfolio_id", "") or "").strip()
        if not portfolio_id:
            continue
        portfolios[portfolio_id] = {
            "market": str(card.get("market", "") or ""),
            "watchlist": str(card.get("watchlist", "") or ""),
            "portfolio_id": portfolio_id,
            "run_investment_paper": bool(card.get("run_investment_paper", False)),
            "force_local_paper_ledger": bool(card.get("force_local_paper_ledger", False)),
            "run_investment_execution": bool(card.get("run_investment_execution", False)),
            "submit_investment_execution": bool(card.get("submit_investment_execution", False)),
            "run_investment_guard": bool(card.get("run_investment_guard", False)),
            "submit_investment_guard": bool(card.get("submit_investment_guard", False)),
            "run_investment_opportunity": bool(card.get("run_investment_opportunity", False)),
        }
        portfolios[portfolio_id]["execution_control_mode"] = _infer_execution_control_mode(portfolios[portfolio_id])
    return {
        "service": {
            "enabled": True,
            "status": "configured",
            "host": host,
            "port": port,
            "url": f"http://{host}:{port}",
        },
        "actions": {
            "run_once_in_progress": False,
            "last_action": "",
            "last_action_ts": "",
            "last_error": "",
        },
        "portfolios": portfolios,
    }


def _load_dashboard_control_payload(summary_dir: Path, cfg: Dict[str, Any], cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    fallback = _dashboard_control_fallback(cfg, cards)
    state_path = summary_dir / "dashboard_control_state.json"
    state = _load_json(state_path)
    if not state:
        return fallback
    merged = {
        "service": dict(fallback.get("service", {}) or {}),
        "actions": dict(fallback.get("actions", {}) or {}),
        "portfolios": dict(fallback.get("portfolios", {}) or {}),
    }
    for key in ("service", "actions"):
        merged[key].update(dict(state.get(key) or {}))
    merged["portfolios"].update(dict(state.get("portfolios") or {}))
    return merged


def _attach_dashboard_control(cards: List[Dict[str, Any]], control_payload: Dict[str, Any]) -> None:
    service = dict(control_payload.get("service", {}) or {})
    portfolios = dict(control_payload.get("portfolios", {}) or {})
    for card in cards:
        portfolio_id = str(card.get("portfolio_id", "") or "")
        card["dashboard_control"] = {
            "enabled": bool(service.get("enabled", False)),
            "status": str(service.get("status", "") or ""),
            "url": str(service.get("url", "") or ""),
            "portfolio": dict(portfolios.get(portfolio_id) or {}),
        }


def _slugify_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


def _ibkr_config_path(market_cfg: Dict[str, Any], item: Dict[str, Any]) -> str:
    market_code = resolve_market_code(str(market_cfg.get("market", market_cfg.get("name", "")) or ""))
    explicit_cfg = str(item.get("ibkr_config", market_cfg.get("ibkr_config", "")) or "").strip()
    return str(market_config_path(BASE_DIR, market_code, explicit_cfg or None))


def _runtime_scope(market_cfg: Dict[str, Any], item: Dict[str, Any]):
    cfg = _load_yaml(_ibkr_config_path(market_cfg, item))
    return scope_from_ibkr_config(cfg)


def _report_dir(market_cfg: Dict[str, Any], item: Dict[str, Any], market: str) -> Path:
    out_dir = resolve_scoped_runtime_path(
        BASE_DIR,
        str(item.get("out_dir", "reports_investment")),
        _runtime_scope(market_cfg, item),
    )
    watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
    slug = _slugify_name(Path(watchlist_yaml).stem) if watchlist_yaml else f"market_{market.lower()}"
    return out_dir / slug


def _base_paper_config_path(market_cfg: Dict[str, Any], item: Dict[str, Any], market: str) -> Path:
    explicit_cfg = str(item.get("paper_config", "") or "").strip()
    if explicit_cfg:
        return _resolve_path(explicit_cfg)
    ibkr_cfg = _load_yaml(_ibkr_config_path(market_cfg, item))
    return _resolve_path(str(ibkr_cfg.get("investment_paper_config", f"config/investment_paper_{market.lower()}.yaml")))


def _base_execution_config_path(market_cfg: Dict[str, Any], item: Dict[str, Any], market: str) -> Path:
    explicit_cfg = str(item.get("execution_config", "") or "").strip()
    if explicit_cfg:
        return _resolve_path(explicit_cfg)
    ibkr_cfg = _load_yaml(_ibkr_config_path(market_cfg, item))
    return _resolve_path(str(ibkr_cfg.get("investment_execution_config", f"config/investment_execution_{market.lower()}.yaml")))


def _mode_label(item: Dict[str, Any], runtime_scope) -> str:
    runtime_mode = str(getattr(runtime_scope, "mode", "") or "paper").strip().lower() or "paper"
    if bool(item.get("research_only", False)):
        return "research-only"
    if bool(item.get("submit_investment_execution", False)) or bool(item.get("submit_investment_guard", False)):
        return f"{runtime_mode}-auto-submit"
    if runtime_mode == "paper":
        return "paper-dry-run"
    return f"{runtime_mode}-read-only"


def _mode_detail(item: Dict[str, Any], runtime_scope) -> str:
    runtime_mode = str(getattr(runtime_scope, "mode", "") or "paper").strip().lower() or "paper"
    if bool(item.get("research_only", False)):
        return "只研究推荐，不执行 broker paper/live 下单"
    if bool(item.get("submit_investment_execution", False)) or bool(item.get("submit_investment_guard", False)):
        return f"{runtime_mode} 自动提交已开启"
    if runtime_mode == "paper":
        return "paper dry-run，仅生成计划与审计"
    return f"{runtime_mode} 只读模式，仅显示真实账户数据与分析"


def _trade_view_enabled(item: Dict[str, Any], runtime_scope) -> bool:
    if bool(item.get("research_only", False)):
        return True
    if bool(item.get("submit_investment_execution", False)) or bool(item.get("submit_investment_guard", False)):
        return True
    return str(getattr(runtime_scope, "mode", "") or "paper").strip().lower() != "paper"


def _dry_run_view_enabled(item: Dict[str, Any], runtime_scope) -> bool:
    if bool(item.get("research_only", False)):
        return False
    if not bool(item.get("run_investment_paper", False)):
        return False
    if bool(item.get("force_local_paper_ledger", False)):
        return True
    return _mode_label(item, runtime_scope) == "paper-dry-run"


def _portfolio_id(item: Dict[str, Any], market: str) -> str:
    watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
    slug = Path(watchlist_yaml).stem if watchlist_yaml else str(market or "").lower()
    return str(item.get("portfolio_id", f"{str(market or '').upper()}:{slug}") or f"{str(market or '').upper()}:{slug}")


def _fmt_money(value: Any) -> str:
    try:
        return f"{float(value):,.2f}"
    except Exception:
        return "-"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return "-"


def _fmt_signed_pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:+.1f}%"
    except Exception:
        return "-"


def _fmt_qty(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "-"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clamp_float(value: Any, lo: float, hi: float) -> float:
    number = _safe_float(value, lo)
    return max(float(lo), min(float(hi), number))


def _feedback_confidence_value(row: Dict[str, Any]) -> float:
    # dashboard 和 supervisor 共享同一套 confidence 口径。
    # 这里默认回退到 1.0，兼容旧周报还没带 feedback_confidence 的情况。
    raw = row.get("feedback_confidence")
    if raw in (None, ""):
        return 1.0
    return _clamp_float(raw, 0.0, 1.0)


def _scale_feedback_delta_preview(value: Any, row: Dict[str, Any], *, min_abs: float = 0.0) -> float:
    # 这里故意和 supervisor 的缩放策略对齐，避免页面上看到的“下一轮预计生效值”
    # 和真正落盘的 overlay 参数出现系统性偏差。
    scaled = _safe_float(value, 0.0) * _feedback_confidence_value(row)
    if scaled == 0.0:
        return 0.0
    if min_abs > 0.0 and abs(scaled) < float(min_abs):
        return float(min_abs) if scaled > 0 else -float(min_abs)
    return scaled


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except Exception:
        return str(path)


def _fmt_budget_change(base_value: Any, effective_value: Any, *, pct: bool = True) -> str:
    if pct:
        return f"{_fmt_pct(base_value)} -> {_fmt_pct(effective_value)}"
    return f"{_safe_float(base_value, 0.0):.2f} -> {_safe_float(effective_value, 0.0):.2f}"


def _iso_ts_sort_value(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return float("-inf")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return float("-inf")


def _action_priority(label: str) -> int:
    text = str(label or "").strip()
    if text == "防守动作":
        return 0
    if text == "可执行调仓":
        return 1
    if text == "可关注进场":
        return 2
    if text == "接近进场":
        return 3
    if text == "研究推荐":
        return 4
    if text == "等待机会":
        return 5
    if text == "持有观察":
        return 6
    return 6


def _candidate_label(row: Dict[str, Any]) -> str:
    symbol = str(row.get("symbol", "") or "").strip()
    action = str(row.get("action", "") or "").strip()
    if symbol and action:
        return f"{symbol}({action})"
    return symbol or action or "-"


def _top_candidates_summary(candidates: List[Dict[str, Any]], limit: int = 10) -> str:
    labels = [_candidate_label(row) for row in list(candidates or [])[: max(1, int(limit))]]
    labels = [label for label in labels if label and label != "-"]
    return " / ".join(labels)


def _load_market_summary_lines(report_dir: Path, limit: int = 4) -> List[str]:
    report_path = report_dir / "investment_report.md"
    if not report_path.exists():
        return []
    try:
        lines = report_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    in_section = False
    out: List[str] = []
    for raw in lines:
        line = str(raw or "").rstrip()
        if line.startswith("## "):
            if in_section:
                break
            in_section = line.strip() == "## Market Summary"
            continue
        if not in_section:
            continue
        stripped = line.strip()
        if stripped.startswith("- "):
            out.append(stripped[2:].strip())
        if len(out) >= max(1, int(limit)):
            break
    return out


def _load_report_data_warning(report_dir: Path) -> str:
    # 把报告里的“数据提醒”单独抽出来，方便 dashboard 区分“配置允许的 fallback”和“需要排查的历史权限问题”。
    for line in _load_market_summary_lines(report_dir, limit=8):
        text = str(line or "").strip()
        if text.startswith("数据提醒:"):
            return text.removeprefix("数据提醒:").strip()
    return ""


def _market_research_only_yfinance(market_code: str) -> bool:
    code = str(market_code or "").strip().upper()
    if not code:
        return False
    universe_cfg = _load_yaml(f"config/markets/{code.lower()}/universe.yaml")
    ibkr_cfg = _load_yaml(str(market_config_path(BASE_DIR, code)))
    investment_cfg_path = str(
        ibkr_cfg.get(
            "investment_config",
            f"config/investment_{code.lower()}.yaml",
        )
    )
    investment_cfg = _load_yaml(investment_cfg_path)
    return bool(
        universe_cfg.get("research_only_yfinance", False)
        or ibkr_cfg.get("research_only_yfinance", False)
        or investment_cfg.get("research_only_yfinance", False)
    )


def _action_distribution(candidates: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for row in list(candidates or [])[:10]:
        action = str(row.get("action", "") or "").strip().upper()
        if not action:
            continue
        counts[action] = counts.get(action, 0) + 1
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " / ".join(f"{name}:{count}" for name, count in ordered)


def _sector_theme_distribution(candidates: List[Dict[str, Any]], limit: int = 4) -> str:
    counts: Dict[str, int] = {}
    for row in list(candidates or [])[:10]:
        theme = str(row.get("asset_theme", "") or "").strip()
        sector = str(row.get("sector", "") or "").strip()
        industry = str(row.get("industry", "") or "").strip()
        label = theme or sector or industry
        if not label:
            continue
        counts[label] = counts.get(label, 0) + 1
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " / ".join(f"{name}:{count}" for name, count in ordered[: max(1, int(limit))])


def _recommended_action(
    *,
    mode: str,
    paper_summary: Dict[str, Any],
    execution_summary: Dict[str, Any],
    guard_summary: Dict[str, Any],
    opportunity_summary: Dict[str, Any],
    execution_plan: List[Dict[str, Any]],
    opportunity_scan: List[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
) -> tuple[str, str]:
    stop_count = int(guard_summary.get("stop_count", 0) or 0)
    take_profit_count = int(guard_summary.get("take_profit_count", 0) or 0)
    if stop_count > 0 or take_profit_count > 0:
        return "防守动作", f"guard stop={stop_count} tp={take_profit_count}"

    executable_rows = [
        row
        for row in execution_plan
        if str(row.get("status", "") or "").strip().upper() not in {"", "BLOCKED_OPPORTUNITY", "SKIP", "HOLD"}
    ]
    if executable_rows:
        first = executable_rows[0]
        return "可执行调仓", f"{first.get('action', '')} {first.get('symbol', '')}".strip()

    entry_now_count = int(opportunity_summary.get("entry_now_count", 0) or 0)
    near_entry_count = int(opportunity_summary.get("near_entry_count", 0) or 0)
    wait_count = int(opportunity_summary.get("wait_count", 0) or 0)
    if entry_now_count > 0:
        return "可关注进场", f"entry_now={entry_now_count}"
    if near_entry_count > 0:
        return "接近进场", f"near_entry={near_entry_count}"
    if wait_count > 0:
        first_wait = next((row for row in opportunity_scan if str(row.get("status", "") or "").strip()), {})
        detail = str(first_wait.get("status", "") or "WAIT").strip() or "WAIT"
        return "等待机会", detail

    if str(mode or "").strip() == "research-only" and candidates:
        return "研究推荐", _top_candidates_summary(candidates, limit=10)

    positions_count = int(paper_summary.get("positions_count", 0) or 0)
    if positions_count > 0:
        return "持有观察", f"positions={positions_count}"
    if candidates:
        return "观察推荐池", _top_candidates_summary(candidates, limit=10)
    return "观察推荐池", "no immediate action"


def _market_summary_map(summary_path: Path) -> Dict[str, Dict[str, Any]]:
    payload = _load_json(summary_path)
    markets = list(payload.get("markets", []) or [])
    out: Dict[str, Dict[str, Any]] = {}
    for row in markets:
        key = str(row.get("market", "") or "").strip().upper()
        if key:
            out[key] = dict(row)
    return out


def _load_broker_snapshot_rows(db_path: Path, *, market: str, portfolio_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT run_id
            FROM investment_execution_runs
            WHERE market=? AND portfolio_id=?
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (market_code, portfolio_id),
        ).fetchone()
        if not row:
            return []
        run_id = str(row["run_id"] or "")
        rows = conn.execute(
            """
            SELECT symbol, qty, market_value, weight, source
            FROM investment_broker_positions
            WHERE run_id=? AND market=? AND portfolio_id=? AND lower(source)='after'
            ORDER BY abs(weight) DESC, symbol ASC
            LIMIT ?
            """,
            (run_id, market_code, portfolio_id, int(limit)),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            item = dict(raw)
            symbol = str(item.get("symbol", "") or "")
            if market_code and symbol and not symbol_matches_market(symbol, market_code):
                continue
            out.append(item)
        return out
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _risk_overlay_from_run_details(details_value: Any) -> Dict[str, Any]:
    details = _parse_json_dict(details_value)
    if not details:
        return {}
    summary = _parse_json_dict(details.get("summary"))
    risk = dict(details.get("risk_overlay") or {})
    if not risk and summary:
        risk = {
            "dynamic_scale": summary.get("risk_dynamic_scale"),
            "dynamic_net_exposure": summary.get("risk_dynamic_net_exposure"),
            "dynamic_gross_exposure": summary.get("risk_dynamic_gross_exposure"),
            "dynamic_short_exposure": summary.get("risk_dynamic_short_exposure"),
            "avg_pair_correlation": summary.get("risk_avg_pair_correlation"),
            "max_pair_correlation": summary.get("risk_max_pair_correlation"),
            "stress_worst_loss": summary.get("risk_stress_worst_loss"),
            "stress_worst_scenario_label": summary.get("risk_stress_worst_scenario_label"),
            "top_sector_share": summary.get("risk_top_sector_share"),
            "notes": summary.get("risk_notes"),
            "correlation_reduced_symbols": summary.get("risk_correlation_reduced_symbols"),
        }
    if summary:
        risk.setdefault("dynamic_scale", summary.get("risk_dynamic_scale"))
        risk.setdefault("dynamic_net_exposure", summary.get("risk_dynamic_net_exposure"))
        risk.setdefault("dynamic_gross_exposure", summary.get("risk_dynamic_gross_exposure"))
        risk.setdefault("dynamic_short_exposure", summary.get("risk_dynamic_short_exposure"))
        risk.setdefault("top_sector_share", summary.get("risk_top_sector_share"))
        risk.setdefault("notes", summary.get("risk_notes"))
        risk.setdefault("correlation_reduced_symbols", summary.get("risk_correlation_reduced_symbols"))
        risk.setdefault("stress_worst_loss", summary.get("risk_stress_worst_loss"))
        risk.setdefault("stress_worst_scenario_label", summary.get("risk_stress_worst_scenario_label"))
    return risk


def _risk_overlay_from_history_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if str(row.get("source_kind") or "").strip():
        stress_scenarios = _parse_json_dict(row.get("stress_scenarios_json"))
        return {
            "dynamic_scale": row.get("dynamic_scale"),
            "dynamic_net_exposure": row.get("dynamic_net_exposure"),
            "dynamic_gross_exposure": row.get("dynamic_gross_exposure"),
            "dynamic_short_exposure": row.get("dynamic_short_exposure"),
            "avg_pair_correlation": row.get("avg_pair_correlation"),
            "final_avg_pair_correlation": row.get("avg_pair_correlation"),
            "max_pair_correlation": row.get("max_pair_correlation"),
            "final_max_pair_correlation": row.get("max_pair_correlation"),
            "stress_worst_loss": row.get("stress_worst_loss"),
            "final_stress_worst_loss": row.get("stress_worst_loss"),
            "stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "final_stress_worst_scenario_label": row.get("stress_worst_scenario_label"),
            "top_sector_share": row.get("top_sector_share"),
            "notes": _parse_json_list(row.get("notes_json")),
            "correlation_reduced_symbols": _parse_json_list(row.get("correlation_reduced_symbols_json")),
            "stress_scenarios": stress_scenarios,
            "final_stress_scenarios": stress_scenarios,
        }
    return _risk_overlay_from_run_details(row.get("details"))


def _risk_overlay_driver_and_diagnosis(risk: Dict[str, Any]) -> tuple[str, str]:
    avg_corr = float(risk.get("final_avg_pair_correlation", risk.get("avg_pair_correlation", 0.0)) or 0.0)
    worst_loss = float(risk.get("final_stress_worst_loss", risk.get("stress_worst_loss", 0.0)) or 0.0)
    dynamic_net = float(risk.get("dynamic_net_exposure", 0.0) or 0.0)
    dynamic_gross = float(risk.get("dynamic_gross_exposure", 0.0) or 0.0)
    top_sector_share = float(risk.get("top_sector_share", 0.0) or 0.0)
    if avg_corr >= 0.62 or top_sector_share >= 0.45:
        return "CORRELATION", "组合拥挤度偏高，风险预算偏向先分散、后扩仓。"
    if worst_loss >= 0.085:
        return "STRESS", "压力测试损失偏高，风险预算偏向先收敛净/总敞口。"
    if dynamic_net <= 0.70 or dynamic_gross <= 0.75:
        return "EXPOSURE_BUDGET", "当前风险预算较紧，系统主动放缓仓位扩张。"
    return "NORMAL", "当前风险覆盖整体平稳，可以继续观察信号与执行质量。"


def _load_recent_risk_history_rows(
    db_path: Path,
    *,
    market: str,
    portfolio_id: str,
    source_kind: str,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    source_kind_norm = str(source_kind or "").strip().lower()
    if source_kind_norm == "execution":
        table = "investment_execution_runs"
        source_label = "执行"
    else:
        table = "investment_runs"
        source_label = "Dry Run"
    # 这里直接读运行数据库，而不是只看 weekly summary，
    # 这样 dashboard 能展示“最近几次真实采用的风险预算轨迹”。
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        normalized_rows = conn.execute(
            """
            SELECT *
            FROM sqlite_master
            WHERE type='table' AND name='investment_risk_history'
            """
        ).fetchall()
        if normalized_rows:
            rows = conn.execute(
                """
                SELECT run_id, ts, source_kind, source_label, dynamic_scale, dynamic_net_exposure,
                       dynamic_gross_exposure, dynamic_short_exposure, avg_pair_correlation,
                       max_pair_correlation, stress_worst_loss, stress_worst_scenario_label,
                       top_sector_share, notes_json, correlation_reduced_symbols_json,
                       stress_scenarios_json
                FROM investment_risk_history
                WHERE market=? AND portfolio_id=? AND source_kind=?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (market_code, portfolio_id, source_kind_norm, max(1, int(limit))),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT run_id, ts, details
                FROM {table}
                WHERE market=? AND portfolio_id=?
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (market_code, portfolio_id, max(1, int(limit))),
            ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        risk = _risk_overlay_from_history_row(row)
        if not risk:
            continue
        driver, diagnosis = _risk_overlay_driver_and_diagnosis(risk)
        notes = [str(item).strip() for item in list(risk.get("notes", []) or []) if str(item).strip()]
        reduced = [str(item).strip() for item in list(risk.get("correlation_reduced_symbols", []) or []) if str(item).strip()]
        out.append(
            {
                "run_id": str(row.get("run_id", "") or ""),
                "ts": str(row.get("ts", "") or ""),
                "source_kind": str(row.get("source_kind", "") or source_kind_norm),
                "source_label": str(row.get("source_label", "") or source_label),
                "dynamic_scale": float(risk.get("dynamic_scale", 1.0) or 1.0),
                "dynamic_net_exposure": float(risk.get("dynamic_net_exposure", 0.0) or 0.0),
                "dynamic_gross_exposure": float(risk.get("dynamic_gross_exposure", 0.0) or 0.0),
                "dynamic_short_exposure": float(risk.get("dynamic_short_exposure", 0.0) or 0.0),
                "avg_pair_correlation": float(risk.get("final_avg_pair_correlation", risk.get("avg_pair_correlation", 0.0)) or 0.0),
                "max_pair_correlation": float(risk.get("final_max_pair_correlation", risk.get("max_pair_correlation", 0.0)) or 0.0),
                "stress_worst_loss": float(risk.get("final_stress_worst_loss", risk.get("stress_worst_loss", 0.0)) or 0.0),
                "stress_worst_scenario_label": str(
                    risk.get("final_stress_worst_scenario_label", risk.get("stress_worst_scenario_label", "")) or ""
                ),
                "top_sector_share": float(risk.get("top_sector_share", 0.0) or 0.0),
                "dominant_risk_driver": driver,
                "risk_diagnosis": diagnosis,
                "risk_notes": notes,
                "notes_preview": " / ".join(notes[:2]),
                "correlation_reduced_symbols": ",".join(reduced[:8]),
            }
        )
    return out


def _feedback_history_state_label(row: Dict[str, Any]) -> str:
    alert_bucket = str(row.get("alert_bucket", "") or "").strip().upper()
    if alert_bucket in {"ACTIVE", "READY", "SOON"}:
        return alert_bucket
    return str(row.get("calibration_apply_mode", "") or "HOLD").strip().upper() or "HOLD"


def _load_recent_feedback_automation_history_rows(
    db_path: Path,
    *,
    market: str,
    portfolio_id: str,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    try:
        rows = Storage(str(db_path)).get_recent_investment_feedback_automation_history(
            market_code,
            portfolio_id=portfolio_id,
            limit=max(1, int(limit)),
        )
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        row["state_label"] = _feedback_history_state_label(row)
        details_json = dict(row.get("details_json", {}) or {})
        if not row.get("automation_reason"):
            row["automation_reason"] = str(details_json.get("automation_reason", "") or "")
        if not row.get("feedback_reason"):
            row["feedback_reason"] = str(details_json.get("feedback_reason", "") or "")
        if not row.get("market_data_gate_label"):
            row["market_data_gate_label"] = str(details_json.get("market_data_gate_label", "") or "")
        if not row.get("market_data_gate_reason"):
            row["market_data_gate_reason"] = str(details_json.get("market_data_gate_reason", "") or "")
        out.append(row)
    return out


def _load_health_summary(db_path: Path, *, portfolio_id: str, hours: int = 24) -> Dict[str, Any]:
    if not db_path.exists():
        return {}
    cutoff_dt = datetime.utcnow() - timedelta(hours=max(1, int(hours)))
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts, kind, value, details
            FROM risk_events
            WHERE portfolio_id=?
              AND system_kind='investment_execution'
              AND kind IN ('IBKR_HEALTH_EVENT', 'ACCOUNT_SNAPSHOT_STALE_FALLBACK')
            ORDER BY ts DESC, id DESC
            LIMIT 500
            """,
            (portfolio_id,),
        ).fetchall()
    except sqlite3.Error:
        conn.close()
        return {}
    finally:
        conn.close()

    delayed_count = 0
    permission_count = 0
    connectivity_breaks = 0
    connectivity_restores = 0
    account_limit_count = 0
    snapshot_fallback_count = 0
    latest_event_ts = ""
    latest_event_label = ""
    latest_event_detail = ""
    for raw in rows:
        row = dict(raw)
        ts_raw = str(row.get("ts") or "")
        try:
            ts_obj = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if ts_obj.tzinfo is not None:
                ts_obj = ts_obj.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            ts_obj = None
        if ts_obj is not None and ts_obj < cutoff_dt:
            continue
        kind = str(row.get("kind") or "").upper().strip()
        code = int(float(row.get("value") or 0.0)) if str(row.get("value", "")).strip() else 0
        detail = str(row.get("details") or "")
        if not latest_event_ts:
            latest_event_ts = ts_raw
            latest_event_detail = detail
            if kind == "ACCOUNT_SNAPSHOT_STALE_FALLBACK":
                latest_event_label = "ACCOUNT_SNAPSHOT_STALE_FALLBACK"
            else:
                latest_event_label = f"IBKR_{code}" if code else kind
        if kind == "ACCOUNT_SNAPSHOT_STALE_FALLBACK":
            snapshot_fallback_count += 1
            continue
        if code == 10167:
            delayed_count += 1
        elif code in {162, 354}:
            permission_count += 1
        elif code in {1100, 165, 2103, 2105, 2157}:
            connectivity_breaks += 1
        elif code in {1102, 2104, 2106, 2119, 2158}:
            connectivity_restores += 1
        elif code == 322:
            account_limit_count += 1

    if not any(
        [
            delayed_count,
            permission_count,
            connectivity_breaks,
            connectivity_restores,
            account_limit_count,
            snapshot_fallback_count,
        ]
    ):
        return {
            "status": "OK",
            "status_detail": "no recent IBKR health events",
            "delayed_count": 0,
            "permission_count": 0,
            "connectivity_breaks": 0,
            "connectivity_restores": 0,
            "account_limit_count": 0,
            "snapshot_fallback_count": 0,
            "latest_event_ts": "",
            "latest_event_label": "",
            "latest_event_detail": "",
        }

    if connectivity_breaks > connectivity_restores or account_limit_count > 0 or snapshot_fallback_count > 0:
        status = "DEGRADED"
    elif permission_count > 0 or delayed_count > 0:
        status = "LIMITED"
    else:
        status = "OK"
    detail_parts = []
    if delayed_count:
        detail_parts.append(f"delayed={delayed_count}")
    if permission_count:
        detail_parts.append(f"perm={permission_count}")
    if connectivity_breaks:
        detail_parts.append(f"breaks={connectivity_breaks}")
    if connectivity_restores:
        detail_parts.append(f"restores={connectivity_restores}")
    if account_limit_count:
        detail_parts.append(f"acct_limit={account_limit_count}")
    if snapshot_fallback_count:
        detail_parts.append(f"acct_cache={snapshot_fallback_count}")
    return {
        "status": status,
        "status_detail": " | ".join(detail_parts) or "recent IBKR events",
        "delayed_count": delayed_count,
        "permission_count": permission_count,
        "connectivity_breaks": connectivity_breaks,
        "connectivity_restores": connectivity_restores,
        "account_limit_count": account_limit_count,
        "snapshot_fallback_count": snapshot_fallback_count,
        "latest_event_ts": latest_event_ts,
        "latest_event_label": latest_event_label,
        "latest_event_detail": latest_event_detail,
    }


def _load_candidate_outcome_summary_rows(
    db_path: Path,
    *,
    market: str,
    portfolio_id: str,
) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                horizon_days,
                COUNT(*) AS labeled_rows,
                AVG(future_return) AS avg_return,
                AVG(max_drawdown) AS avg_drawdown,
                AVG(CASE WHEN future_return > 0 THEN 1.0 ELSE 0.0 END) AS positive_rate,
                AVG(CASE WHEN outcome_label='BROKEN' THEN 1.0 ELSE 0.0 END) AS broken_rate,
                MAX(outcome_ts) AS latest_outcome_ts
            FROM investment_candidate_outcomes
            WHERE market=? AND portfolio_id=?
            GROUP BY horizon_days
            ORDER BY horizon_days ASC
            """
            ,
            (market_code, portfolio_id),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [dict(row) for row in rows]


def _analysis_status_label(status: str) -> str:
    return STATUS_LABELS.get(str(status or "").upper(), str(status or "").upper() or "-")


def _analysis_state_rank(status: str) -> int:
    norm = str(status or "").upper()
    order = {
        "ENTRY_READY": 0,
        "ADD_READY": 1,
        "REDUCE_READY": 2,
        "WATCH_NEAR_ENTRY": 3,
        "HOLDING": 4,
        "WATCHING": 5,
        "DEPRIORITIZED": 6,
        "REMOVED_FROM_WATCH": 7,
    }
    return order.get(norm, 9)


def _tracked_status_label(*, in_general_list: bool, action: str, entry_status: str, held_qty: float) -> str:
    action_norm = str(action or "").upper()
    entry_norm = str(entry_status or "").upper()
    held = float(held_qty or 0.0)
    if entry_norm == "ADD_ON_PULLBACK":
        return "可增持"
    if entry_norm == "ENTRY_NOW":
        return "可入场" if held <= 0 else "可增持"
    if action_norm == "REDUCE" and held > 0:
        return "可减持"
    if held > 0 and action_norm in {"ACCUMULATE", "HOLD"}:
        return "持有观察"
    if entry_norm == "NEAR_ENTRY":
        return "接近入场"
    if in_general_list:
        if action_norm == "REDUCE":
            return "取消观望"
        return "观望中"
    return "持仓补充"


def _tracked_status_rank(label: str) -> int:
    return {
        "可入场": 0,
        "可增持": 1,
        "可减持": 2,
        "接近入场": 3,
        "持有观察": 4,
        "观望中": 5,
        "取消观望": 6,
        "持仓补充": 7,
    }.get(str(label or ""), 9)


def _watchlist_slug(item: Dict[str, Any], market: str) -> str:
    watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
    return Path(watchlist_yaml).stem if watchlist_yaml else str(market or "").lower()


def _tracked_report_dirs(market_cfg: Dict[str, Any], item: Dict[str, Any], market: str) -> List[Dict[str, Any]]:
    watchlist = _watchlist_slug(item, market)
    runtime_scope = _runtime_scope(market_cfg, item)
    current_report_dir = _report_dir(market_cfg, item, market)
    raw_out_dir = str(item.get("out_dir", "reports_investment") or "reports_investment")
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []

    def add(path: Path, scope_label: str, *, is_current: bool) -> None:
        try:
            resolved = str(path.resolve())
        except Exception:
            resolved = str(path)
        if resolved in seen or not path.exists():
            return
        seen.add(resolved)
        out.append({"path": path, "scope": scope_label, "is_current": is_current})

    add(current_report_dir, runtime_scope.label, is_current=True)
    if not Path(raw_out_dir).is_absolute():
        add(resolve_repo_path(BASE_DIR, raw_out_dir) / watchlist, "repo", is_current=False)
        runtime_root = BASE_DIR / "runtime_data"
        if runtime_root.exists():
            for scope_dir in sorted(runtime_root.iterdir()):
                if not scope_dir.is_dir():
                    continue
                add(scope_dir / raw_out_dir / watchlist, scope_dir.name, is_current=(scope_dir.name == runtime_scope.label))
    return out


def _load_tracked_stock_rows(
    market_cfg: Dict[str, Any],
    item: Dict[str, Any],
    *,
    market: str,
    report_dir: Path,
    dashboard_db: Path,
    portfolio_id: str,
) -> List[Dict[str, Any]]:
    watchlist = _watchlist_slug(item, market)
    source_dirs = _tracked_report_dirs(market_cfg, item, market)
    rows_by_symbol: Dict[str, Dict[str, Any]] = {}

    def ensure_row(symbol: str) -> Dict[str, Any]:
        key = str(symbol or "").upper()
        row = rows_by_symbol.get(key)
        if row is None:
            row = {
                "market": str(market or "").upper(),
                "watchlist": watchlist,
                "symbol": key,
                "in_general_list": False,
                "action": "",
                "entry_status": "",
                "score": 0.0,
                "reason": "",
                "asset_label": "",
                "source_scopes": [],
                "primary_scope": "",
                "paper_qty": 0.0,
                "paper_weight": 0.0,
                "broker_qty": 0.0,
                "broker_weight": 0.0,
            }
            rows_by_symbol[key] = row
        return row

    for source in source_dirs:
        candidate_rows = _read_all_csv_rows(Path(source["path"]) / "investment_candidates.csv")
        opportunity_rows = _read_all_csv_rows(Path(source["path"]) / "investment_opportunity_scan.csv")
        opportunity_map = {
            str(row.get("symbol") or "").upper(): dict(row)
            for row in opportunity_rows
            if str(row.get("symbol") or "").strip()
        }
        for raw in candidate_rows:
            symbol = str(raw.get("symbol") or "").upper()
            if not symbol or (market and not symbol_matches_market(symbol, market)):
                continue
            row = ensure_row(symbol)
            row["in_general_list"] = True
            if source["scope"] not in row["source_scopes"]:
                row["source_scopes"].append(source["scope"])
            prefer = bool(source.get("is_current")) or not str(row.get("primary_scope") or "").strip()
            if prefer:
                row["primary_scope"] = str(source["scope"] or "")
                row["action"] = str(raw.get("action") or row.get("action") or "")
                row["score"] = _safe_float(raw.get("score"), row.get("score", 0.0))
                row["asset_label"] = (
                    str(raw.get("asset_class") or "").strip()
                    or str(raw.get("asset_theme") or "").strip()
                    or str(raw.get("sector") or "").strip()
                    or str(raw.get("industry") or "").strip()
                    or str(row.get("asset_label") or "").strip()
                )
            opp = dict(opportunity_map.get(symbol) or {})
            if opp and (prefer or not str(row.get("entry_status") or "").strip()):
                row["entry_status"] = str(opp.get("entry_status") or row.get("entry_status") or "")
                row["reason"] = str(opp.get("entry_reason") or row.get("reason") or "")
        for raw in opportunity_rows:
            symbol = str(raw.get("symbol") or "").upper()
            if not symbol or (market and not symbol_matches_market(symbol, market)):
                continue
            row = ensure_row(symbol)
            row["in_general_list"] = True
            if source["scope"] not in row["source_scopes"]:
                row["source_scopes"].append(source["scope"])
            prefer = bool(source.get("is_current")) or not str(row.get("primary_scope") or "").strip()
            if prefer:
                row["primary_scope"] = str(source["scope"] or "")
                row["action"] = str(raw.get("action") or row.get("action") or "")
                row["score"] = _safe_float(raw.get("score"), row.get("score", 0.0))
                row["entry_status"] = str(raw.get("entry_status") or row.get("entry_status") or "")
                row["reason"] = str(raw.get("entry_reason") or row.get("reason") or "")

    for raw in _read_all_csv_rows(report_dir / "investment_portfolio.csv"):
        symbol = str(raw.get("symbol") or "").upper()
        if not symbol or (market and not symbol_matches_market(symbol, market)):
            continue
        row = ensure_row(symbol)
        row["paper_qty"] = _safe_float(raw.get("qty"), row.get("paper_qty", 0.0))
        row["paper_weight"] = _safe_float(raw.get("weight"), row.get("paper_weight", 0.0))

    for raw in _load_broker_snapshot_rows(dashboard_db, market=market, portfolio_id=portfolio_id, limit=500):
        symbol = str(raw.get("symbol") or "").upper()
        if not symbol or (market and not symbol_matches_market(symbol, market)):
            continue
        row = ensure_row(symbol)
        row["broker_qty"] = _safe_float(raw.get("qty"), row.get("broker_qty", 0.0))
        row["broker_weight"] = _safe_float(raw.get("weight"), row.get("broker_weight", 0.0))

    final_rows: List[Dict[str, Any]] = []
    for row in rows_by_symbol.values():
        held_qty = row["broker_qty"] if abs(float(row.get("broker_qty", 0.0) or 0.0)) > 1e-9 else row["paper_qty"]
        row["held_qty"] = float(held_qty)
        row["source_scopes"] = ",".join(sorted(set(str(x) for x in row.get("source_scopes", []) if str(x).strip())))
        row["list_origin"] = (
            "GENERAL+HOLDING"
            if row["in_general_list"] and abs(float(row.get("held_qty", 0.0) or 0.0)) > 1e-9
            else "GENERAL"
            if row["in_general_list"]
            else "HOLDING_ONLY"
        )
        row["tracked_status"] = _tracked_status_label(
            in_general_list=bool(row.get("in_general_list", False)),
            action=str(row.get("action") or ""),
            entry_status=str(row.get("entry_status") or ""),
            held_qty=float(row.get("held_qty", 0.0) or 0.0),
        )
        final_rows.append(row)
    final_rows.sort(
        key=lambda row: (
            _tracked_status_rank(str(row.get("tracked_status") or "")),
            -_safe_float(row.get("score"), 0.0),
            str(row.get("watchlist") or ""),
            str(row.get("symbol") or ""),
        )
    )
    return final_rows


def _build_stock_list_groups(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for card in cards:
        market = str(card.get("market", "") or "")
        bucket = grouped.setdefault(market, {"market": market, "rows": {}})
        for raw in list(card.get("tracked_stocks", []) or []):
            row = dict(raw)
            key = (str(row.get("watchlist", "") or ""), str(row.get("symbol", "") or ""))
            existing = bucket["rows"].get(key)
            if existing is None:
                bucket["rows"][key] = row
                continue
            if str(row.get("list_origin") or "") == "GENERAL+HOLDING":
                existing["list_origin"] = "GENERAL+HOLDING"
            elif str(existing.get("list_origin") or "") != "GENERAL+HOLDING" and str(row.get("list_origin") or "") == "HOLDING_ONLY":
                existing["list_origin"] = str(existing.get("list_origin") or "HOLDING_ONLY")
            existing["paper_qty"] = max(_safe_float(existing.get("paper_qty"), 0.0), _safe_float(row.get("paper_qty"), 0.0))
            existing["paper_weight"] = max(_safe_float(existing.get("paper_weight"), 0.0), _safe_float(row.get("paper_weight"), 0.0))
            existing["broker_qty"] = max(_safe_float(existing.get("broker_qty"), 0.0), _safe_float(row.get("broker_qty"), 0.0))
            existing["broker_weight"] = max(_safe_float(existing.get("broker_weight"), 0.0), _safe_float(row.get("broker_weight"), 0.0))
            scopes = sorted(
                {
                    part
                    for part in (
                        str(existing.get("source_scopes") or "").split(",")
                        + str(row.get("source_scopes") or "").split(",")
                    )
                    if str(part).strip()
                }
            )
            existing["source_scopes"] = ",".join(scopes)
    out: List[Dict[str, Any]] = []
    for market, bucket in sorted(grouped.items()):
        rows = list(bucket["rows"].values())
        rows.sort(
            key=lambda row: (
                str(row.get("watchlist") or ""),
                _tracked_status_rank(str(row.get("tracked_status") or "")),
                -_safe_float(row.get("score"), 0.0),
                str(row.get("symbol") or ""),
            )
        )
        out.append({"market": market, "rows": rows})
    return out


def _load_analysis_state_rows(db_path: Path, *, market: str, portfolio_id: str, limit: int = 12) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts, symbol, status, lifecycle, action, entry_status, score, held_qty, reason
            FROM investment_analysis_states
            WHERE market=? AND portfolio_id=?
            ORDER BY ts DESC, id DESC
            """,
            (market_code, portfolio_id),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    scoped: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        symbol = str(row.get("symbol", "") or "")
        if market_code and symbol and not symbol_matches_market(symbol, market_code):
            continue
        row["status_label"] = _analysis_status_label(str(row.get("status") or ""))
        scoped.append(row)
    scoped.sort(
        key=lambda row: (
            _analysis_state_rank(str(row.get("status") or "")),
            -float(row.get("score", 0.0) or 0.0),
            str(row.get("symbol", "") or ""),
        )
    )
    return scoped[: max(1, int(limit))]


def _load_analysis_event_rows(db_path: Path, *, market: str, portfolio_id: str, limit: int = 12) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ts, symbol, event_kind, from_status, to_status, action, entry_status, summary
            FROM investment_analysis_events
            WHERE market=? AND portfolio_id=?
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (market_code, portfolio_id, max(1, int(limit))),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    scoped: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        symbol = str(row.get("symbol", "") or "")
        if market_code and symbol and not symbol_matches_market(symbol, market_code):
            continue
        row["from_status_label"] = _analysis_status_label(str(row.get("from_status") or ""))
        row["to_status_label"] = _analysis_status_label(str(row.get("to_status") or ""))
        scoped.append(row)
    return scoped


def _load_shadow_review_history_rows(
    db_path: Path,
    *,
    market: str,
    portfolio_id: str,
    limit: int = 24,
) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    market_code = resolve_market_code(market)
    try:
        rows = Storage(str(db_path)).get_recent_shadow_review_orders(
            market_code,
            portfolio_id=portfolio_id,
            limit=max(1, int(limit)),
        )
    except Exception:
        return []
    scoped: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        symbol = str(row.get("symbol", "") or "").upper()
        if market_code and symbol and not symbol_matches_market(symbol, market_code):
            continue
        details_json = dict(row.get("details_json", {}) or {})
        row["shadow_review_status"] = str(details_json.get("shadow_review_status") or "").strip()
        row["shadow_review_reason"] = (
            str(details_json.get("shadow_review_reason") or "").strip()
            or str(details_json.get("manual_review_reason") or "").strip()
            or str(row.get("reason") or "").strip()
        )
        scoped.append(row)
    return scoped


def _build_shadow_review_repeat_rows(
    history_rows: List[Dict[str, Any]],
    *,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for raw in history_rows:
        symbol = str(raw.get("symbol", "") or "").upper()
        if not symbol:
            continue
        row = grouped.setdefault(
            symbol,
            {
                "symbol": symbol,
                "repeat_count": 0,
                "latest_ts": "",
                "latest_action": "",
                "latest_order_value": 0.0,
                "latest_reason": "",
            },
        )
        row["repeat_count"] = int(row.get("repeat_count", 0) or 0) + 1
        ts = str(raw.get("ts", "") or "")
        if ts >= str(row.get("latest_ts", "") or ""):
            row["latest_ts"] = ts
            row["latest_action"] = str(raw.get("action", "") or "")
            row["latest_order_value"] = _safe_float(raw.get("order_value"), 0.0)
            row["latest_reason"] = str(raw.get("shadow_review_reason", "") or "")
    rows = list(grouped.values())
    rows.sort(
        key=lambda row: (
            -int(row.get("repeat_count", 0) or 0),
            -_iso_ts_sort_value(row.get("latest_ts")),
            str(row.get("symbol", "") or ""),
        ),
        reverse=False,
    )
    return rows[: max(1, int(limit))]


def _risk_feedback_auto_apply_enabled(cfg: Dict[str, Any], card: Dict[str, Any]) -> bool:
    account_mode = str(card.get("account_mode", "") or "paper").strip().lower() or "paper"
    if account_mode == "live":
        if bool(cfg.get("weekly_review_auto_apply_live", False)):
            return True
        control_portfolio = dict(card.get("dashboard_control", {}).get("portfolio", {}) or {})
        current_signature = str(control_portfolio.get("weekly_feedback_signature", "") or "").strip()
        confirmed_signature = str(control_portfolio.get("weekly_feedback_confirmed_signature", "") or "").strip()
        return bool(current_signature) and current_signature == confirmed_signature
    return bool(cfg.get("weekly_review_auto_apply_paper", True))


def _feedback_apply_status(
    cfg: Dict[str, Any],
    card: Dict[str, Any],
    *,
    feedback_present: bool,
    auto_apply_enabled: bool,
    overlay_exists: bool,
    feedback_kind_label: str,
    calibration_apply_mode: str = "",
    calibration_apply_mode_label: str = "",
    automation_reason: str = "",
) -> tuple[str, str]:
    account_mode = str(card.get("account_mode", "") or "paper").strip().lower() or "paper"
    mode_code = str(calibration_apply_mode or "").strip().upper()
    if feedback_present and auto_apply_enabled:
        if mode_code == "SUGGEST_ONLY":
            return "CALIBRATION_SUGGEST_ONLY", automation_reason or f"{feedback_kind_label}当前更适合先人工确认，再决定是否应用。"
        if mode_code == "HOLD":
            return "CALIBRATION_HOLD", automation_reason or f"{feedback_kind_label}当前仍建议继续观察，暂不自动应用。"
        if overlay_exists:
            return "AUTO_APPLY_OVERLAY", f"{feedback_kind_label}已自动落盘到 overlay。"
        return "AUTO_APPLY_PREDICTED", f"{feedback_kind_label}已生成，dashboard 先展示下一轮预计生效值。"
    if not feedback_present:
        weekly = dict(card.get("execution_weekly_row", {}) or {})
        weekly_attribution = dict(card.get("weekly_attribution", {}) or {})
        execution_summary = dict(card.get("execution_summary", {}) or {})
        execution_run_rows = int(weekly.get("execution_run_rows", 0) or 0)
        submitted_order_rows = int(weekly.get("submitted_order_rows", 0) or 0)
        fill_rows = int(weekly.get("fill_rows", 0) or 0)
        blocked_opportunity_rows = int(weekly.get("blocked_opportunity_rows", 0) or 0)
        blocked_opportunity_count = int(execution_summary.get("blocked_opportunity_order_count", 0) or 0)
        blocked_quality_count = int(execution_summary.get("blocked_quality_order_count", 0) or 0)
        blocked_risk_review_count = sum(
            int(execution_summary.get(key, 0) or 0)
            for key in (
                "blocked_risk_alert_order_count",
                "blocked_manual_review_order_count",
                "blocked_shadow_review_order_count",
                "blocked_size_review_order_count",
            )
        )
        blocked_liquidity_count = sum(
            int(execution_summary.get(key, 0) or 0)
            for key in ("blocked_liquidity_order_count", "blocked_hotspot_penalty_order_count")
        )
        detailed_block_total = blocked_opportunity_count + blocked_quality_count + blocked_risk_review_count + blocked_liquidity_count
        planned_execution_cost_total = _safe_float(weekly_attribution.get("planned_execution_cost_total"), 0.0)
        execution_cost_total = _safe_float(weekly_attribution.get("execution_cost_total"), 0.0)
        if not weekly and not weekly_attribution:
            return "NO_WEEKLY_DATA", f"本周还没有可用的{feedback_kind_label}周报数据，当前沿用基础配置。"
        if (
            execution_run_rows <= 0
            and submitted_order_rows <= 0
            and fill_rows <= 0
            and blocked_opportunity_rows <= 0
            and planned_execution_cost_total <= 0.0
            and execution_cost_total <= 0.0
        ):
            return "NO_EXECUTION_ACTIVITY", "本周还没有 execution run 或成交样本，暂时无法形成新的执行参数反馈。"
        if submitted_order_rows <= 0 and (blocked_opportunity_rows > 0 or detailed_block_total > 0):
            # 优先使用最新 execution summary 的细分阻断原因；只有这轮没有细分统计时才回退到 weekly blocked。
            if detailed_block_total > 0:
                if blocked_quality_count >= max(blocked_opportunity_count, blocked_risk_review_count, blocked_liquidity_count, 1):
                    return (
                        "NO_QUALITY_PASS",
                        f"本周没有实际提交订单，最近一轮主要被候选质量/执行质量门挡住（quality={blocked_quality_count}），因此没有新的执行参数反馈。",
                    )
                if blocked_risk_review_count >= max(blocked_opportunity_count, blocked_quality_count, blocked_liquidity_count, 1):
                    return (
                        "NO_GUARD_PASS",
                        f"本周没有实际提交订单，最近一轮主要被风险告警或人工审核门挡住（risk_review={blocked_risk_review_count}），因此没有新的执行参数反馈。",
                    )
                if blocked_liquidity_count >= max(blocked_opportunity_count, blocked_quality_count, blocked_risk_review_count, 1):
                    return (
                        "NO_LIQUIDITY_PASS",
                        f"本周没有实际提交订单，最近一轮主要被流动性/执行热点门挡住（liquidity={blocked_liquidity_count}），因此没有新的执行参数反馈。",
                    )
                return (
                    "NO_OPPORTUNITY_PASS",
                    f"本周没有实际提交订单，最近一轮主要被机会门挡住（opportunity={blocked_opportunity_count}），因此没有新的执行参数反馈。",
                )
            return (
                "NO_OPPORTUNITY_PASS",
                f"本周没有实际提交订单，主要被机会门挡住（opportunity={blocked_opportunity_rows}），因此没有新的执行参数反馈。",
            )
        if submitted_order_rows <= 0 and execution_run_rows > 0:
            return "NO_ACTIONABLE_ORDERS", "本周虽有 execution run，但没有形成可提交订单，当前沿用基础配置。"
        if submitted_order_rows > 0 and fill_rows <= 0:
            return "NO_FILL_SAMPLE", f"本周已有提交订单（submitted={submitted_order_rows}），但还没有成交样本，暂时无法生成新的执行参数反馈。"
        if submitted_order_rows > 0 and fill_rows > 0 and planned_execution_cost_total <= 0.0 and execution_cost_total <= 0.0:
            return "NO_COST_SAMPLE", "本周已有提交/成交，但成本样本仍不足，当前继续沿用基础配置。"
        return "NO_FEEDBACK", f"本周没有新的{feedback_kind_label}，当前沿用基础配置。"
    if account_mode == "live" and not bool(cfg.get("weekly_review_auto_apply_live", False)):
        control_portfolio = dict(card.get("dashboard_control", {}).get("portfolio", {}) or {})
        current_signature = str(control_portfolio.get("weekly_feedback_signature", "") or "").strip()
        confirmed_signature = str(control_portfolio.get("weekly_feedback_confirmed_signature", "") or "").strip()
        if mode_code == "HOLD":
            return "CALIBRATION_HOLD", automation_reason or f"{feedback_kind_label}当前仍建议继续观察，live 暂不需要确认。"
        if current_signature and current_signature != confirmed_signature:
            mode_hint = calibration_apply_mode_label or "建议确认"
            return "LIVE_CONFIRM_REQUIRED", automation_reason or f"live 模式下，这条{feedback_kind_label}属于“{mode_hint}”，需要先在 dashboard 手动确认。"
        return "LIVE_SUGGEST_ONLY", "live 模式默认只保留建议，不自动改真实执行参数。"
    if account_mode != "live" and not bool(cfg.get("weekly_review_auto_apply_paper", True)):
        return "PAPER_AUTO_APPLY_DISABLED", "paper 自动应用已关闭，当前只展示建议。"
    if mode_code == "SUGGEST_ONLY":
        return "CALIBRATION_SUGGEST_ONLY", automation_reason or f"{feedback_kind_label}当前更适合先人工确认，再决定是否应用。"
    if mode_code == "HOLD":
        return "CALIBRATION_HOLD", automation_reason or f"{feedback_kind_label}当前仍建议继续观察，暂不自动应用。"
    return "MANUAL_REVIEW", "当前组合未自动应用反馈，请先人工复核。"


def _risk_feedback_overlay_path(cfg: Dict[str, Any], card: Dict[str, Any]) -> Path:
    raw_dir = str(cfg.get("weekly_review_overlay_dir", "auto_feedback_configs") or "auto_feedback_configs")
    runtime_scope_label = str(card.get("runtime_scope", "") or "").strip()
    if Path(raw_dir).is_absolute():
        root = Path(raw_dir).resolve()
    elif runtime_scope_label:
        root = (BASE_DIR / "runtime_data" / runtime_scope_label / raw_dir).resolve()
    else:
        root = _resolve_path(raw_dir)
    watchlist = str(card.get("watchlist", "") or "").strip()
    slug = _slugify_name(watchlist) if watchlist else f"market_{str(card.get('market', '') or '').lower()}"
    return root / slug / "paper_auto_feedback.yaml"


def _build_paper_risk_feedback(card: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    # dashboard 侧也把基础预算和自动反馈串起来，减少协作者翻 overlay yaml 的成本。
    defaults = {
        "max_single_weight": 0.22,
        "max_sector_weight": 0.40,
        "max_net_exposure": 1.00,
        "max_gross_exposure": 1.00,
        "max_short_exposure": 0.35,
        "correlation_soft_limit": 0.62,
    }
    base_path_raw = str(card.get("paper_config_path", "") or "").strip()
    base_path = Path(base_path_raw).resolve() if base_path_raw else Path()
    base_cfg = _safe_load_yaml_path(base_path) if base_path_raw else {}
    base_paper = dict(base_cfg.get("paper", {}) or {})
    base_values = {
        key: _safe_float(base_paper.get(key), default)
        for key, default in defaults.items()
    }
    feedback_row = dict(card.get("weekly_risk_feedback", {}) or {})
    automation_row = dict(dict(card.get("weekly_feedback_automation_map", {}) or {}).get("risk", {}) or {})
    feedback_present = bool(feedback_row)
    auto_apply_enabled = _risk_feedback_auto_apply_enabled(cfg, card)
    overlay_path = _risk_feedback_overlay_path(cfg, card)
    feedback_confidence = _feedback_confidence_value(feedback_row)
    feedback_confidence_label = str(feedback_row.get("feedback_confidence_label", "") or ("HIGH" if feedback_confidence >= 0.75 else "MEDIUM" if feedback_confidence >= 0.45 else "LOW"))
    calibration_apply_mode = str(automation_row.get("calibration_apply_mode", "") or "")
    calibration_apply_mode_label = str(automation_row.get("calibration_apply_mode_label", "") or "")
    calibration_basis_label = str(automation_row.get("calibration_basis_label", "") or "")
    automation_reason = str(automation_row.get("automation_reason", "") or "")

    effective_values = dict(base_values)
    effective_source = "base"
    effective_source_label = "基础配置"
    apply_mode = "BASE_ONLY"
    apply_mode_label = "沿用基础配置"

    if feedback_present and calibration_apply_mode == "AUTO_APPLY" and auto_apply_enabled:
        overlay_cfg = _safe_load_yaml_path(overlay_path)
        overlay_paper = dict(overlay_cfg.get("paper", {}) or {})
        if overlay_paper:
            # 如果 supervisor 已经写出了 overlay，就直接展示真实生效值。
            effective_values = {
                key: _safe_float(overlay_paper.get(key), base_values[key])
                for key in defaults
            }
            effective_source = "overlay"
            effective_source_label = "overlay 已落盘"
        else:
            # 如果周报刚生成、下一轮报告还没跑到，这里先按同样规则预估下一轮会生效的预算。
            effective_values["max_single_weight"] = round(
                _clamp_float(
                    base_values["max_single_weight"] + _scale_feedback_delta_preview(feedback_row.get("paper_max_single_weight_delta"), feedback_row, min_abs=0.002),
                    0.05,
                    0.50,
                ),
                6,
            )
            effective_values["max_sector_weight"] = round(
                _clamp_float(
                    base_values["max_sector_weight"] + _scale_feedback_delta_preview(feedback_row.get("paper_max_sector_weight_delta"), feedback_row, min_abs=0.002),
                    0.10,
                    1.00,
                ),
                6,
            )
            effective_values["max_net_exposure"] = round(
                _clamp_float(
                    base_values["max_net_exposure"] + _scale_feedback_delta_preview(feedback_row.get("paper_max_net_exposure_delta"), feedback_row, min_abs=0.005),
                    0.20,
                    1.50,
                ),
                6,
            )
            effective_values["max_gross_exposure"] = round(
                _clamp_float(
                    base_values["max_gross_exposure"] + _scale_feedback_delta_preview(feedback_row.get("paper_max_gross_exposure_delta"), feedback_row, min_abs=0.005),
                    0.20,
                    2.00,
                ),
                6,
            )
            effective_values["max_short_exposure"] = round(
                _clamp_float(
                    base_values["max_short_exposure"] + _scale_feedback_delta_preview(feedback_row.get("paper_max_short_exposure_delta"), feedback_row, min_abs=0.002),
                    0.0,
                    min(effective_values["max_gross_exposure"], 1.00),
                ),
                6,
            )
            effective_values["correlation_soft_limit"] = round(
                _clamp_float(
                    base_values["correlation_soft_limit"] + _scale_feedback_delta_preview(feedback_row.get("paper_correlation_soft_limit_delta"), feedback_row, min_abs=0.005),
                    0.25,
                    0.95,
                ),
                6,
            )
            effective_source = "predicted"
            effective_source_label = "dashboard 预估"
        apply_mode = "AUTO_APPLY"
        apply_mode_label = "自动生效"
    elif feedback_present and calibration_apply_mode == "SUGGEST_ONLY":
        apply_mode = "SUGGEST_ONLY"
        apply_mode_label = "仅建议未自动生效"
        effective_source = "base"
        effective_source_label = "基础配置（未自动改）"
    elif feedback_present and calibration_apply_mode == "HOLD":
        apply_mode = "BASE_ONLY"
        apply_mode_label = "沿用基础配置"
        effective_source = "base"
        effective_source_label = "基础配置（继续观察）"
    elif feedback_present:
        if auto_apply_enabled:
            apply_mode = "AUTO_APPLY"
            apply_mode_label = "自动生效"
        else:
            apply_mode = "SUGGEST_ONLY"
            apply_mode_label = "仅建议未自动生效"

    return {
        "feedback_present": feedback_present,
        "auto_apply_enabled": auto_apply_enabled,
        "apply_mode": apply_mode,
        "apply_mode_label": apply_mode_label,
        "calibration_apply_mode": calibration_apply_mode,
        "calibration_apply_mode_label": calibration_apply_mode_label,
        "calibration_basis_label": calibration_basis_label,
        "automation_reason": automation_reason,
        "effective_source": effective_source,
        "effective_source_label": effective_source_label,
        "risk_feedback_action": str(feedback_row.get("risk_feedback_action", "") or ""),
        "feedback_base_confidence": _safe_float(feedback_row.get("feedback_base_confidence"), feedback_confidence),
        "feedback_base_confidence_label": str(feedback_row.get("feedback_base_confidence_label", "") or feedback_confidence_label),
        "feedback_calibration_score": _safe_float(feedback_row.get("feedback_calibration_score"), 0.5),
        "feedback_calibration_label": str(feedback_row.get("feedback_calibration_label", "") or "MEDIUM"),
        "feedback_calibration_sample_count": int(_safe_float(feedback_row.get("feedback_calibration_sample_count"), 0.0)),
        "feedback_calibration_horizon_days": str(feedback_row.get("feedback_calibration_horizon_days", "") or ""),
        "feedback_calibration_scope": str(feedback_row.get("feedback_calibration_scope", "") or ""),
        "feedback_calibration_reason": str(feedback_row.get("feedback_calibration_reason", "") or ""),
        "feedback_confidence": float(feedback_confidence),
        "feedback_confidence_label": feedback_confidence_label,
        "feedback_sample_count": int(_safe_float(feedback_row.get("feedback_sample_count"), 0.0)),
        "feedback_scope": str(feedback_row.get("feedback_scope", "") or ""),
        "feedback_reason": str(feedback_row.get("feedback_reason", "") or ""),
        "base_config_path": _display_path(base_path) if base_path_raw else "-",
        "effective_config_path": _display_path(overlay_path),
        "overlay_exists": overlay_path.exists(),
        "base_max_single_weight": base_values["max_single_weight"],
        "effective_max_single_weight": effective_values["max_single_weight"],
        "base_max_sector_weight": base_values["max_sector_weight"],
        "effective_max_sector_weight": effective_values["max_sector_weight"],
        "base_max_net_exposure": base_values["max_net_exposure"],
        "effective_max_net_exposure": effective_values["max_net_exposure"],
        "base_max_gross_exposure": base_values["max_gross_exposure"],
        "effective_max_gross_exposure": effective_values["max_gross_exposure"],
        "base_max_short_exposure": base_values["max_short_exposure"],
        "effective_max_short_exposure": effective_values["max_short_exposure"],
        "base_correlation_soft_limit": base_values["correlation_soft_limit"],
        "effective_correlation_soft_limit": effective_values["correlation_soft_limit"],
    }


def _execution_feedback_overlay_path(cfg: Dict[str, Any], card: Dict[str, Any]) -> Path:
    raw_dir = str(cfg.get("weekly_review_overlay_dir", "auto_feedback_configs") or "auto_feedback_configs")
    runtime_scope_label = str(card.get("runtime_scope", "") or "").strip()
    if Path(raw_dir).is_absolute():
        root = Path(raw_dir).resolve()
    elif runtime_scope_label:
        root = (BASE_DIR / "runtime_data" / runtime_scope_label / raw_dir).resolve()
    else:
        root = _resolve_path(raw_dir)
    watchlist = str(card.get("watchlist", "") or "").strip()
    slug = _slugify_name(watchlist) if watchlist else f"market_{str(card.get('market', '') or '').lower()}"
    return root / slug / "execution_auto_feedback.yaml"


def _build_execution_feedback(card: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {
        "adv_max_participation_pct": 0.05,
        "adv_split_trigger_pct": 0.02,
        "max_slices_per_symbol": 4.0,
        "open_session_participation_scale": 0.70,
        "midday_session_participation_scale": 1.00,
        "close_session_participation_scale": 0.85,
    }
    base_path_raw = str(card.get("execution_config_path", "") or "").strip()
    base_path = Path(base_path_raw).resolve() if base_path_raw else Path()
    base_cfg = _safe_load_yaml_path(base_path) if base_path_raw else {}
    base_execution = dict(base_cfg.get("execution", {}) or {})
    base_values = {
        key: _safe_float(base_execution.get(key), default)
        for key, default in defaults.items()
    }
    feedback_row = dict(card.get("weekly_execution_feedback", {}) or {})
    automation_row = dict(dict(card.get("weekly_feedback_automation_map", {}) or {}).get("execution", {}) or {})
    feedback_present = bool(feedback_row)
    auto_apply_enabled = _risk_feedback_auto_apply_enabled(cfg, card)
    overlay_path = _execution_feedback_overlay_path(cfg, card)
    feedback_confidence = _feedback_confidence_value(feedback_row)
    feedback_confidence_label = str(feedback_row.get("feedback_confidence_label", "") or ("HIGH" if feedback_confidence >= 0.75 else "MEDIUM" if feedback_confidence >= 0.45 else "LOW"))
    calibration_apply_mode = str(automation_row.get("calibration_apply_mode", "") or "")
    calibration_apply_mode_label = str(automation_row.get("calibration_apply_mode_label", "") or "")
    calibration_basis_label = str(automation_row.get("calibration_basis_label", "") or "")
    automation_reason = str(automation_row.get("automation_reason", "") or "")
    session_feedback_rows: List[Dict[str, Any]] = []
    raw_session_feedback = str(feedback_row.get("execution_session_feedback_json", "") or "").strip()
    if raw_session_feedback:
        try:
            parsed = json.loads(raw_session_feedback)
            if isinstance(parsed, list):
                session_feedback_rows = [dict(item) for item in parsed if isinstance(item, dict)]
        except Exception:
            session_feedback_rows = []
    hotspot_rows: List[Dict[str, Any]] = []
    raw_hotspots = str(feedback_row.get("execution_hotspots_json", "") or "").strip()
    if raw_hotspots:
        try:
            parsed = json.loads(raw_hotspots)
            if isinstance(parsed, list):
                hotspot_rows = [dict(item) for item in parsed if isinstance(item, dict)]
        except Exception:
            hotspot_rows = []
    execution_penalty_rows: List[Dict[str, Any]] = []
    raw_execution_penalties = str(feedback_row.get("execution_penalties_json", "") or "").strip()
    if raw_execution_penalties:
        try:
            parsed = json.loads(raw_execution_penalties)
            if isinstance(parsed, list):
                execution_penalty_rows = [dict(item) for item in parsed if isinstance(item, dict)]
        except Exception:
            execution_penalty_rows = []

    effective_values = dict(base_values)
    effective_source = "base"
    effective_source_label = "基础配置"
    apply_mode = "BASE_ONLY"
    apply_mode_label = "沿用基础配置"

    if feedback_present and calibration_apply_mode == "AUTO_APPLY" and auto_apply_enabled:
        overlay_cfg = _safe_load_yaml_path(overlay_path)
        overlay_execution = dict(overlay_cfg.get("execution", {}) or {})
        if overlay_execution:
            effective_values = {
                key: _safe_float(overlay_execution.get(key), base_values[key])
                for key in defaults
            }
            effective_source = "overlay"
            effective_source_label = "overlay 已落盘"
        else:
            # 这里和 supervisor 的 clamp 保持一致，便于 dashboard 在周报刚生成时先展示“下一轮预计生效值”。
            effective_values["adv_max_participation_pct"] = round(
                _clamp_float(
                    base_values["adv_max_participation_pct"] + _scale_feedback_delta_preview(feedback_row.get("execution_adv_max_participation_pct_delta"), feedback_row, min_abs=0.001),
                    0.01,
                    0.20,
                ),
                6,
            )
            effective_values["adv_split_trigger_pct"] = round(
                _clamp_float(
                    base_values["adv_split_trigger_pct"] + _scale_feedback_delta_preview(feedback_row.get("execution_adv_split_trigger_pct_delta"), feedback_row, min_abs=0.001),
                    0.005,
                    0.10,
                ),
                6,
            )
            effective_values["max_slices_per_symbol"] = round(
                _clamp_float(
                    base_values["max_slices_per_symbol"] + _scale_feedback_delta_preview(feedback_row.get("execution_max_slices_per_symbol_delta"), feedback_row, min_abs=1.0),
                    1.0,
                    8.0,
                ),
                6,
            )
            effective_values["open_session_participation_scale"] = round(
                _clamp_float(
                    base_values["open_session_participation_scale"] + _scale_feedback_delta_preview(feedback_row.get("execution_open_session_participation_scale_delta"), feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
            effective_values["midday_session_participation_scale"] = round(
                _clamp_float(
                    base_values["midday_session_participation_scale"] + _scale_feedback_delta_preview(feedback_row.get("execution_midday_session_participation_scale_delta"), feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
            effective_values["close_session_participation_scale"] = round(
                _clamp_float(
                    base_values["close_session_participation_scale"] + _scale_feedback_delta_preview(feedback_row.get("execution_close_session_participation_scale_delta"), feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
            effective_source = "predicted"
            effective_source_label = "dashboard 预估"
        apply_mode = "AUTO_APPLY"
        apply_mode_label = "自动生效"
    elif feedback_present and calibration_apply_mode == "SUGGEST_ONLY":
        apply_mode = "SUGGEST_ONLY"
        apply_mode_label = "仅建议未自动生效"
        effective_source = "base"
        effective_source_label = "基础配置（未自动改）"
    elif feedback_present and calibration_apply_mode == "HOLD":
        apply_mode = "BASE_ONLY"
        apply_mode_label = "沿用基础配置"
        effective_source = "base"
        effective_source_label = "基础配置（继续观察）"
    elif feedback_present:
        if auto_apply_enabled:
            apply_mode = "AUTO_APPLY"
            apply_mode_label = "自动生效"
        else:
            apply_mode = "SUGGEST_ONLY"
            apply_mode_label = "仅建议未自动生效"

    apply_status_code, apply_status_reason = _feedback_apply_status(
        cfg,
        card,
        feedback_present=feedback_present,
        auto_apply_enabled=auto_apply_enabled,
        overlay_exists=overlay_path.exists(),
        feedback_kind_label="执行参数反馈",
        calibration_apply_mode=calibration_apply_mode,
        calibration_apply_mode_label=calibration_apply_mode_label,
        automation_reason=automation_reason,
    )

    return {
        "feedback_present": feedback_present,
        "auto_apply_enabled": auto_apply_enabled,
        "apply_mode": apply_mode,
        "apply_mode_label": apply_mode_label,
        "calibration_apply_mode": calibration_apply_mode,
        "calibration_apply_mode_label": calibration_apply_mode_label,
        "calibration_basis_label": calibration_basis_label,
        "automation_reason": automation_reason,
        "apply_status_code": apply_status_code,
        "apply_status_reason": apply_status_reason,
        "effective_source": effective_source,
        "effective_source_label": effective_source_label,
        "execution_feedback_action": str(feedback_row.get("execution_feedback_action", "") or ""),
        "feedback_base_confidence": _safe_float(feedback_row.get("feedback_base_confidence"), feedback_confidence),
        "feedback_base_confidence_label": str(feedback_row.get("feedback_base_confidence_label", "") or feedback_confidence_label),
        "feedback_calibration_score": _safe_float(feedback_row.get("feedback_calibration_score"), 0.5),
        "feedback_calibration_label": str(feedback_row.get("feedback_calibration_label", "") or "MEDIUM"),
        "feedback_calibration_sample_count": int(_safe_float(feedback_row.get("feedback_calibration_sample_count"), 0.0)),
        "feedback_calibration_horizon_days": str(feedback_row.get("feedback_calibration_horizon_days", "") or ""),
        "feedback_calibration_scope": str(feedback_row.get("feedback_calibration_scope", "") or ""),
        "feedback_calibration_reason": str(feedback_row.get("feedback_calibration_reason", "") or ""),
        "feedback_confidence": float(feedback_confidence),
        "feedback_confidence_label": feedback_confidence_label,
        "feedback_sample_count": int(_safe_float(feedback_row.get("feedback_sample_count"), 0.0)),
        "feedback_scope": str(feedback_row.get("feedback_scope", "") or ""),
        "feedback_reason": str(feedback_row.get("feedback_reason", "") or ""),
        "base_config_path": _display_path(base_path) if base_path_raw else "-",
        "effective_config_path": _display_path(overlay_path),
        "overlay_exists": overlay_path.exists(),
        "base_adv_max_participation_pct": base_values["adv_max_participation_pct"],
        "effective_adv_max_participation_pct": effective_values["adv_max_participation_pct"],
        "base_adv_split_trigger_pct": base_values["adv_split_trigger_pct"],
        "effective_adv_split_trigger_pct": effective_values["adv_split_trigger_pct"],
        "base_max_slices_per_symbol": base_values["max_slices_per_symbol"],
        "effective_max_slices_per_symbol": effective_values["max_slices_per_symbol"],
        "base_open_session_participation_scale": base_values["open_session_participation_scale"],
        "effective_open_session_participation_scale": effective_values["open_session_participation_scale"],
        "base_midday_session_participation_scale": base_values["midday_session_participation_scale"],
        "effective_midday_session_participation_scale": effective_values["midday_session_participation_scale"],
        "base_close_session_participation_scale": base_values["close_session_participation_scale"],
        "effective_close_session_participation_scale": effective_values["close_session_participation_scale"],
        "planned_execution_cost_total": _safe_float(feedback_row.get("planned_execution_cost_total"), 0.0),
        "execution_cost_total": _safe_float(feedback_row.get("execution_cost_total"), 0.0),
        "execution_cost_gap": _safe_float(feedback_row.get("execution_cost_gap"), 0.0),
        "avg_expected_cost_bps": _safe_float(feedback_row.get("avg_expected_cost_bps"), 0.0),
        "avg_actual_slippage_bps": _safe_float(feedback_row.get("avg_actual_slippage_bps"), 0.0),
        "execution_style_breakdown": str(feedback_row.get("execution_style_breakdown", "") or ""),
        "dominant_execution_session_bucket": str(feedback_row.get("dominant_execution_session_bucket", "") or ""),
        "dominant_execution_session_label": str(feedback_row.get("dominant_execution_session_label", "") or ""),
        "dominant_execution_hotspot_symbol": str(feedback_row.get("dominant_execution_hotspot_symbol", "") or ""),
        "dominant_execution_hotspot_session_label": str(feedback_row.get("dominant_execution_hotspot_session_label", "") or ""),
        "execution_penalty_symbols": str(feedback_row.get("execution_penalty_symbols", "") or ""),
        "session_feedback_rows": session_feedback_rows,
        "hotspot_rows": hotspot_rows,
        "execution_penalty_rows": execution_penalty_rows,
    }


def _build_report_card(
    market_cfg: Dict[str, Any],
    item: Dict[str, Any],
    summary_map: Dict[str, Dict[str, Any]],
    *,
    dashboard_db_raw: str,
) -> Dict[str, Any]:
    market_code = str(market_cfg.get("market", market_cfg.get("name", "")) or "").strip().upper()
    watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
    watchlist = Path(watchlist_yaml).stem if watchlist_yaml else market_code.lower()
    runtime_scope = _runtime_scope(market_cfg, item)
    report_dir = _report_dir(market_cfg, item, market_code)
    paper_config_path = _base_paper_config_path(market_cfg, item, market_code)
    execution_config_path = _base_execution_config_path(market_cfg, item, market_code)
    portfolio_id = _portfolio_id(item, market_code)
    dashboard_db = (
        _resolve_path(dashboard_db_raw)
        if Path(str(dashboard_db_raw)).is_absolute()
        else resolve_scoped_runtime_path(
            BASE_DIR,
            str(dashboard_db_raw or item.get("db", "audit.db")),
            runtime_scope,
        )
    )
    paper_summary = _load_json(report_dir / "investment_paper_summary.json")
    exec_summary = _load_json(report_dir / "investment_execution_summary.json")
    guard_summary = _load_json(report_dir / "investment_guard_summary.json")
    opp_summary = _load_json(report_dir / "investment_opportunity_summary.json")
    data_quality_summary = _load_json(report_dir / "investment_data_quality_summary.json")
    cost_summary = _load_json(report_dir / "investment_cost_summary.json")
    shadow_model_summary = _load_json(report_dir / "investment_shadow_model_summary.json")
    candidates = _read_csv_rows(report_dir / "investment_candidates.csv", limit=10)
    plan_rows = _read_csv_rows(report_dir / "investment_plan.csv", limit=8)
    holdings = _read_csv_rows(report_dir / "investment_portfolio.csv", limit=10)
    broker_holdings = _load_broker_snapshot_rows(dashboard_db, market=market_code, portfolio_id=portfolio_id, limit=10)
    health_summary = _load_health_summary(dashboard_db, portfolio_id=portfolio_id, hours=24)
    outcome_summary_rows = _load_candidate_outcome_summary_rows(dashboard_db, market=market_code, portfolio_id=portfolio_id)
    analysis_states = _load_analysis_state_rows(dashboard_db, market=market_code, portfolio_id=portfolio_id, limit=12)
    analysis_events = _load_analysis_event_rows(dashboard_db, market=market_code, portfolio_id=portfolio_id, limit=12)
    shadow_review_recent_rows = _load_shadow_review_history_rows(dashboard_db, market=market_code, portfolio_id=portfolio_id, limit=24)
    shadow_review_repeat_rows = _build_shadow_review_repeat_rows(shadow_review_recent_rows, limit=8)
    feedback_automation_history_rows = _load_recent_feedback_automation_history_rows(
        dashboard_db,
        market=market_code,
        portfolio_id=portfolio_id,
        limit=12,
    )
    paper_risk_history_rows = _load_recent_risk_history_rows(
        dashboard_db,
        market=market_code,
        portfolio_id=portfolio_id,
        source_kind="paper",
        limit=8,
    )
    execution_risk_history_rows = _load_recent_risk_history_rows(
        dashboard_db,
        market=market_code,
        portfolio_id=portfolio_id,
        source_kind="execution",
        limit=8,
    )
    tracked_stocks = _load_tracked_stock_rows(
        market_cfg,
        item,
        market=market_code,
        report_dir=report_dir,
        dashboard_db=dashboard_db,
        portfolio_id=portfolio_id,
    )
    execution_plan = _read_csv_rows(report_dir / "investment_execution_plan.csv", limit=6)
    paper_trades = _read_csv_rows(report_dir / "investment_rebalance_trades.csv", limit=8)
    opportunity_scan = _read_csv_rows(report_dir / "investment_opportunity_scan.csv", limit=6)
    market_summary = dict(summary_map.get(market_code, {}) or {})

    report_statuses = list(market_summary.get("report_statuses", []) or [])
    report_status = next((row for row in report_statuses if str(row.get("watchlist", "")) == watchlist), {})
    market_summary_lines = _load_market_summary_lines(report_dir)
    report_data_warning = _load_report_data_warning(report_dir)
    research_only_yfinance = _market_research_only_yfinance(market_code)

    try:
        display_report_dir = str(report_dir.relative_to(BASE_DIR))
    except ValueError:
        display_report_dir = str(report_dir)

    recommended_action, recommended_detail = _recommended_action(
        mode=_mode_label(item, runtime_scope),
        paper_summary=paper_summary,
        execution_summary=exec_summary,
        guard_summary=guard_summary,
        opportunity_summary=opp_summary,
        execution_plan=execution_plan,
        opportunity_scan=opportunity_scan,
        candidates=candidates,
    )
    actionable = recommended_action in {"防守动作", "可执行调仓", "可关注进场", "接近进场"}

    return {
        "market": market_code,
        "watchlist": watchlist,
        "portfolio_id": portfolio_id,
        "runtime_scope": runtime_scope.label,
        "account_id": str(getattr(runtime_scope, "account_id", "") or ""),
        "account_mode": str(getattr(runtime_scope, "mode", "") or ""),
        "mode": _mode_label(item, runtime_scope),
        "mode_detail": _mode_detail(item, runtime_scope),
        "report_dir": display_report_dir,
        "paper_config_path": str(paper_config_path),
        "execution_config_path": str(execution_config_path),
        "dashboard_db_path": str(dashboard_db),
        "exchange_open": bool(market_summary.get("exchange_open", False)),
        "priority_order": int(market_summary.get("priority_order", 0) or 0),
        "priority_reason": str(market_summary.get("priority_reason", "") or ""),
        "report_status": report_status,
        "market_summary_lines": market_summary_lines,
        "report_data_warning": report_data_warning,
        "research_only_yfinance": research_only_yfinance,
        "action_distribution": _action_distribution(candidates),
        "sector_theme_distribution": _sector_theme_distribution(candidates),
        "paper_summary": paper_summary,
        "execution_summary": exec_summary,
        "guard_summary": guard_summary,
        "opportunity_summary": opp_summary,
        "data_quality_summary": data_quality_summary,
        "cost_summary": cost_summary,
        "shadow_model_summary": shadow_model_summary,
        "health_summary": health_summary,
        "analysis_states": analysis_states,
        "analysis_events": analysis_events,
        "shadow_review_recent_rows": shadow_review_recent_rows[:8],
        "shadow_review_repeat_rows": shadow_review_repeat_rows,
        "feedback_automation_history_rows": feedback_automation_history_rows,
        "paper_risk_history_rows": paper_risk_history_rows,
        "execution_risk_history_rows": execution_risk_history_rows,
        "outcome_summary_rows": outcome_summary_rows,
        "tracked_stocks": tracked_stocks,
        "recommended_action": recommended_action,
        "recommended_detail": recommended_detail,
        "action_priority": _action_priority(recommended_action),
        "actionable": actionable,
        "holdings": holdings,
        "broker_holdings": broker_holdings,
        "run_investment_paper": bool(item.get("run_investment_paper", False)),
        "force_local_paper_ledger": bool(item.get("force_local_paper_ledger", False)),
        "run_investment_execution": bool(item.get("run_investment_execution", False)),
        "submit_investment_execution": bool(item.get("submit_investment_execution", False)),
        "run_investment_guard": bool(item.get("run_investment_guard", False)),
        "submit_investment_guard": bool(item.get("submit_investment_guard", False)),
        "run_investment_opportunity": bool(item.get("run_investment_opportunity", False)),
        "trade_view_enabled": _trade_view_enabled(item, runtime_scope),
        "dry_run_view_enabled": _dry_run_view_enabled(item, runtime_scope),
        "execution_weekly_row": {},
        "weekly_execution_sessions": [],
        "weekly_execution_hotspots": [],
        "weekly_execution_feedback": {},
        "plan_rows": plan_rows,
        "candidates": candidates,
        "execution_plan": execution_plan,
        "paper_trades": paper_trades,
        "opportunity_scan": opportunity_scan,
    }


def _display_card_variant(card: Dict[str, Any], *, dashboard_view: str) -> Dict[str, Any]:
    variant = dict(card)
    variant["dashboard_view"] = dashboard_view
    if dashboard_view == "dry-run":
        variant["mode"] = "dry-run"
        variant["mode_detail"] = "本地模拟账本 + 快照回标，用于无下单闭环、阈值复盘与策略升级。"
        variant["risk_history_rows"] = list(card.get("paper_risk_history_rows", []) or [])
        variant["risk_history_source_label"] = "Dry Run 风险轨迹"
        variant["risk_history_fallback"] = False
    else:
        execution_rows = list(card.get("execution_risk_history_rows", []) or [])
        paper_rows = list(card.get("paper_risk_history_rows", []) or [])
        if execution_rows:
            variant["risk_history_rows"] = execution_rows
            variant["risk_history_source_label"] = "执行风险轨迹"
            variant["risk_history_fallback"] = False
        else:
            # trade 视图如果暂时还没有 execution run，就明确回退到 dry-run 风险历史，
            # 避免页面空白，也避免把两条链路静默混在一起。
            variant["risk_history_rows"] = paper_rows
            variant["risk_history_source_label"] = "Dry Run 风险轨迹（当前 trade 暂无执行历史）"
            variant["risk_history_fallback"] = bool(paper_rows)
    variant["risk_trend_summary"] = _build_risk_trend_summary(list(variant.get("risk_history_rows", []) or []))
    control_portfolio = dict(variant.get("dashboard_control", {}).get("portfolio", {}) or {})
    current_mode = str(control_portfolio.get("execution_control_mode", "") or _infer_execution_control_mode(control_portfolio if control_portfolio else variant))
    variant["execution_mode_recommendation"] = _build_execution_mode_recommendation(
        dict(variant.get("risk_trend_summary", {}) or {}),
        current_mode=current_mode,
    )
    return variant


def _expand_display_cards(cards: List[Dict[str, Any]], *, dashboard_view: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for card in cards:
        if dashboard_view == "trade" and bool(card.get("trade_view_enabled", False)):
            out.append(_display_card_variant(card, dashboard_view=dashboard_view))
        elif dashboard_view == "dry-run" and bool(card.get("dry_run_view_enabled", False)):
            out.append(_display_card_variant(card, dashboard_view=dashboard_view))
    return out


def _build_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        paper = dict(card.get("paper_summary", {}) or {})
        execution = dict(card.get("execution_summary", {}) or {})
        opp = dict(card.get("opportunity_summary", {}) or {})
        health = dict(card.get("health_summary", {}) or {})
        dashboard_view = str(card.get("dashboard_view", "trade") or "trade").strip().lower()
        if dashboard_view == "dry-run":
            display_equity = paper.get("equity_after")
            display_cash = paper.get("cash_after")
        else:
            display_equity = execution.get("broker_equity")
            display_cash = execution.get("broker_cash")
        rows.append(
            {
                "market": card["market"],
                "watchlist": card["watchlist"],
                "mode": card["mode"],
                "exchange_open": bool(card.get("exchange_open", False)),
                "priority_order": int(card.get("priority_order", 0) or 0),
                "recommended_action": str(card.get("recommended_action", "") or ""),
                "recommended_detail": str(card.get("recommended_detail", "") or ""),
                "paper_equity": display_equity,
                "paper_cash": display_cash,
                "ibkr_health": health.get("status", "OK"),
                "opp_entry_now": int(opp.get("entry_now_count", 0) or 0),
                "opp_wait": int(opp.get("wait_count", 0) or 0),
                "execution_orders": int(execution.get("order_count", 0) or 0),
            }
        )
    return rows


def _build_review_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        execution = dict(card.get("execution_summary", {}) or {})
        shadow_review = int(execution.get("blocked_shadow_review_order_count", 0) or 0)
        size_review = int(execution.get("blocked_size_review_order_count", 0) or 0)
        total_review = int(execution.get("blocked_manual_review_order_count", shadow_review + size_review) or 0)
        rows.append(
            {
                "market": card.get("market", ""),
                "watchlist": card.get("watchlist", ""),
                "portfolio_id": card.get("portfolio_id", ""),
                "shadow_review_count": shadow_review,
                "size_review_count": size_review,
                "total_review_count": total_review,
                "idle_capital_gap": execution.get("idle_capital_gap"),
                "recommended_action": str(card.get("recommended_action", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row.get("total_review_count", 0) or 0),
            -int(row.get("shadow_review_count", 0) or 0),
            str(row.get("market", "")),
            str(row.get("watchlist", "")),
        )
    )
    return rows


def _build_shadow_review_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        for raw in list(card.get("shadow_review_repeat_rows", []) or []):
            row = dict(raw)
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "symbol": str(row.get("symbol", "") or ""),
                    "repeat_count": int(row.get("repeat_count", 0) or 0),
                    "latest_ts": str(row.get("latest_ts", "") or ""),
                    "latest_action": str(row.get("latest_action", "") or ""),
                    "latest_order_value": _safe_float(row.get("latest_order_value"), 0.0),
                    "latest_reason": str(row.get("latest_reason", "") or ""),
                }
            )
    rows.sort(
        key=lambda row: (
            -int(row.get("repeat_count", 0) or 0),
            -_iso_ts_sort_value(row.get("latest_ts")),
            str(row.get("market", "") or ""),
            str(row.get("symbol", "") or ""),
        ),
        reverse=False,
    )
    return rows[:12]


def _build_shadow_strategy_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        weekly = dict(card.get("weekly_shadow_review", {}) or {})
        if not weekly:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "shadow_review_action": str(weekly.get("shadow_review_action", "") or ""),
                "shadow_review_reason": str(weekly.get("shadow_review_reason", "") or ""),
                "shadow_review_count": int(_safe_float(weekly.get("shadow_review_count"), 0.0)),
                "near_miss_count": int(_safe_float(weekly.get("near_miss_count"), 0.0)),
                "far_below_count": int(_safe_float(weekly.get("far_below_count"), 0.0)),
                "repeated_symbol_count": int(_safe_float(weekly.get("repeated_symbol_count"), 0.0)),
                "repeated_symbols": str(weekly.get("repeated_symbols", "") or ""),
                "latest_shadow_symbol": str(weekly.get("latest_shadow_symbol", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("shadow_review_action", "") or "") == "WEAK_SIGNAL" else 1 if str(row.get("shadow_review_action", "") or "") == "REVIEW_THRESHOLD" else 2,
            -int(row.get("shadow_review_count", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_risk_review_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        weekly = dict(card.get("weekly_risk_review", {}) or {})
        if not weekly:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "dominant_risk_driver": str(weekly.get("dominant_risk_driver", "") or ""),
                "risk_diagnosis": str(weekly.get("risk_diagnosis", "") or ""),
                "latest_dynamic_net_exposure": float(weekly.get("latest_dynamic_net_exposure", 0.0) or 0.0),
                "latest_dynamic_gross_exposure": float(weekly.get("latest_dynamic_gross_exposure", 0.0) or 0.0),
                "latest_avg_pair_correlation": float(weekly.get("latest_avg_pair_correlation", 0.0) or 0.0),
                "latest_stress_worst_scenario_label": str(weekly.get("latest_stress_worst_scenario_label", "") or ""),
                "latest_stress_worst_loss": float(weekly.get("latest_stress_worst_loss", 0.0) or 0.0),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("dominant_risk_driver", "") or "") == "STRESS" else 1 if str(row.get("dominant_risk_driver", "") or "") == "CORRELATION" else 2,
            -float(row.get("latest_stress_worst_loss", 0.0) or 0.0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_risk_history_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        for raw in list(card.get("risk_history_rows", []) or [])[:3]:
            row = dict(raw)
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "source_label": str(row.get("source_label", "") or ""),
                    "ts": str(row.get("ts", "") or ""),
                    "dynamic_scale": float(row.get("dynamic_scale", 1.0) or 1.0),
                    "dynamic_net_exposure": float(row.get("dynamic_net_exposure", 0.0) or 0.0),
                    "dynamic_gross_exposure": float(row.get("dynamic_gross_exposure", 0.0) or 0.0),
                    "avg_pair_correlation": float(row.get("avg_pair_correlation", 0.0) or 0.0),
                    "stress_worst_scenario_label": str(row.get("stress_worst_scenario_label", "") or ""),
                    "stress_worst_loss": float(row.get("stress_worst_loss", 0.0) or 0.0),
                    "dominant_risk_driver": str(row.get("dominant_risk_driver", "") or ""),
                    "notes_preview": str(row.get("notes_preview", "") or ""),
                }
            )
    rows.sort(key=lambda row: _iso_ts_sort_value(row.get("ts")), reverse=True)
    return rows[:18]


def _build_risk_trend_summary(risk_history_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = [dict(row) for row in list(risk_history_rows or []) if isinstance(row, dict)]
    if not rows:
        return {}
    def _row_level(row: Dict[str, Any]) -> str:
        scale = float(row.get("dynamic_scale", 1.0) or 1.0)
        corr = float(row.get("avg_pair_correlation", 0.0) or 0.0)
        stress = float(row.get("stress_worst_loss", 0.0) or 0.0)
        if stress >= 0.085 or corr >= 0.62 or scale <= 0.75:
            return "ALERT"
        if stress >= 0.075 or corr >= 0.58 or scale <= 0.82:
            return "WATCH"
        return "STABLE"

    latest = dict(rows[0])
    previous = dict(rows[1]) if len(rows) > 1 else {}
    latest_scale = float(latest.get("dynamic_scale", 1.0) or 1.0)
    latest_net = float(latest.get("dynamic_net_exposure", 0.0) or 0.0)
    latest_gross = float(latest.get("dynamic_gross_exposure", 0.0) or 0.0)
    latest_corr = float(latest.get("avg_pair_correlation", 0.0) or 0.0)
    latest_stress = float(latest.get("stress_worst_loss", 0.0) or 0.0)
    previous_scale = float(previous.get("dynamic_scale", latest_scale) or latest_scale)
    previous_net = float(previous.get("dynamic_net_exposure", latest_net) or latest_net)
    previous_gross = float(previous.get("dynamic_gross_exposure", latest_gross) or latest_gross)
    previous_corr = float(previous.get("avg_pair_correlation", latest_corr) or latest_corr)
    previous_stress = float(previous.get("stress_worst_loss", latest_stress) or latest_stress)
    scale_delta = float(latest_scale - previous_scale)
    net_delta = float(latest_net - previous_net)
    gross_delta = float(latest_gross - previous_gross)
    corr_delta = float(latest_corr - previous_corr)
    stress_delta = float(latest_stress - previous_stress)
    tightening = scale_delta <= -0.05 or net_delta <= -0.05 or gross_delta <= -0.05
    loosening = scale_delta >= 0.05 or net_delta >= 0.05 or gross_delta >= 0.05
    if latest_stress >= 0.085 or latest_corr >= 0.62 or latest_scale <= 0.75:
        alert_level = "ALERT"
    elif tightening or corr_delta >= 0.04 or stress_delta >= 0.01:
        alert_level = "WATCH"
    else:
        alert_level = "STABLE"
    trend_label = "收紧" if tightening else "放松" if loosening else "稳定"
    reason_parts: List[str] = []
    if latest_scale <= 0.75:
        reason_parts.append("动态 scale 偏低")
    if latest_corr >= 0.62:
        reason_parts.append("平均相关性偏高")
    if latest_stress >= 0.085:
        reason_parts.append("最差 stress 损失偏高")
    if corr_delta >= 0.04:
        reason_parts.append("相关性最近继续抬升")
    if stress_delta >= 0.01:
        reason_parts.append("stress 损失最近继续恶化")
    if tightening and not reason_parts:
        reason_parts.append("系统最近主动收紧风险预算")
    if not reason_parts:
        reason_parts.append("风险预算整体平稳")
    consecutive_alert_count = 0
    consecutive_watch_count = 0
    for raw in rows:
        row_level = _row_level(raw)
        if row_level == "ALERT":
            consecutive_alert_count += 1
            consecutive_watch_count += 1
            continue
        if row_level == "WATCH":
            consecutive_watch_count += 1
            break
        break
    return {
        "latest_ts": str(latest.get("ts", "") or ""),
        "previous_ts": str(previous.get("ts", "") or ""),
        "source_label": str(latest.get("source_label", "") or ""),
        "alert_level": alert_level,
        "trend_label": trend_label,
        "latest_dynamic_scale": latest_scale,
        "scale_delta": scale_delta if previous else 0.0,
        "latest_dynamic_net_exposure": latest_net,
        "net_delta": net_delta if previous else 0.0,
        "latest_dynamic_gross_exposure": latest_gross,
        "gross_delta": gross_delta if previous else 0.0,
        "latest_avg_pair_correlation": latest_corr,
        "corr_delta": corr_delta if previous else 0.0,
        "latest_stress_worst_scenario_label": str(latest.get("stress_worst_scenario_label", "") or ""),
        "latest_stress_worst_loss": latest_stress,
        "stress_delta": stress_delta if previous else 0.0,
        "dominant_risk_driver": str(latest.get("dominant_risk_driver", "") or ""),
        "consecutive_alert_count": int(consecutive_alert_count),
        "consecutive_watch_count": int(consecutive_watch_count),
        "diagnosis": "；".join(reason_parts),
    }


def _build_execution_mode_recommendation(
    risk_trend_summary: Dict[str, Any],
    *,
    current_mode: str,
) -> Dict[str, Any]:
    summary = dict(risk_trend_summary or {})
    if not summary:
        return {}
    alert_level = str(summary.get("alert_level", "") or "STABLE").upper()
    trend_label = str(summary.get("trend_label", "") or "稳定")
    current_mode_normalized = str(current_mode or "").strip().upper() or "AUTO"
    latest_scale = float(summary.get("latest_dynamic_scale", 1.0) or 1.0)
    latest_stress = float(summary.get("latest_stress_worst_loss", 0.0) or 0.0)
    latest_corr = float(summary.get("latest_avg_pair_correlation", 0.0) or 0.0)
    consecutive_alert_count = int(summary.get("consecutive_alert_count", 0) or 0)
    consecutive_watch_count = int(summary.get("consecutive_watch_count", 0) or 0)
    recommended_mode = "AUTO"
    reason_parts: List[str] = []

    if alert_level == "ALERT":
        if consecutive_alert_count >= 3 or latest_scale <= 0.65 or latest_stress >= 0.11:
            recommended_mode = "PAUSED"
            if consecutive_alert_count >= 3:
                reason_parts.append(f"已连续 {consecutive_alert_count} 次 ALERT")
            if latest_scale <= 0.65:
                reason_parts.append("动态 scale 过低")
            if latest_stress >= 0.11:
                reason_parts.append("最差 stress 损失过高")
        elif consecutive_alert_count >= 2 or trend_label == "收紧":
            recommended_mode = "REVIEW_ONLY"
            if consecutive_alert_count >= 2:
                reason_parts.append(f"已连续 {consecutive_alert_count} 次 ALERT")
            if trend_label == "收紧":
                reason_parts.append("组合仍在继续收紧")
    elif alert_level == "WATCH" and (consecutive_watch_count >= 2 or trend_label == "收紧"):
        recommended_mode = "REVIEW_ONLY"
        if consecutive_watch_count >= 2:
            reason_parts.append(f"已连续 {consecutive_watch_count} 次 WATCH/ALERT")
        if trend_label == "收紧":
            reason_parts.append("风险预算仍在收紧")

    if latest_corr >= 0.62 and recommended_mode != "AUTO":
        reason_parts.append("平均相关性偏高")
    if latest_stress >= 0.085 and recommended_mode != "AUTO":
        reason_parts.append("最差 stress 损失偏高")
    if not reason_parts:
        reason_parts.append("当前风险预算稳定，可继续按基线执行")

    return {
        "current_mode": current_mode_normalized,
        "recommended_mode": recommended_mode,
        "recommended_mode_label": str(EXECUTION_MODE_LABELS.get(recommended_mode, recommended_mode)),
        "current_mode_label": str(EXECUTION_MODE_LABELS.get(current_mode_normalized, current_mode_normalized)),
        "differs_from_current": bool(recommended_mode != current_mode_normalized),
        "consecutive_alert_count": consecutive_alert_count,
        "consecutive_watch_count": consecutive_watch_count,
        "reason": "；".join(reason_parts),
    }


def _build_risk_alert_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        summary = dict(card.get("risk_trend_summary", {}) or {})
        recommendation = dict(card.get("execution_mode_recommendation", {}) or {})
        if not summary:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "source_label": str(summary.get("source_label", "") or ""),
                "alert_level": str(summary.get("alert_level", "") or ""),
                "trend_label": str(summary.get("trend_label", "") or ""),
                "latest_ts": str(summary.get("latest_ts", "") or ""),
                "latest_dynamic_scale": float(summary.get("latest_dynamic_scale", 1.0) or 1.0),
                "scale_delta": float(summary.get("scale_delta", 0.0) or 0.0),
                "latest_dynamic_net_exposure": float(summary.get("latest_dynamic_net_exposure", 0.0) or 0.0),
                "latest_dynamic_gross_exposure": float(summary.get("latest_dynamic_gross_exposure", 0.0) or 0.0),
                "latest_avg_pair_correlation": float(summary.get("latest_avg_pair_correlation", 0.0) or 0.0),
                "latest_stress_worst_scenario_label": str(summary.get("latest_stress_worst_scenario_label", "") or ""),
                "latest_stress_worst_loss": float(summary.get("latest_stress_worst_loss", 0.0) or 0.0),
                "dominant_risk_driver": str(summary.get("dominant_risk_driver", "") or ""),
                "recommended_mode": str(recommendation.get("recommended_mode_label", "") or "-"),
                "current_mode": str(recommendation.get("current_mode_label", "") or "-"),
                "recommendation_reason": str(recommendation.get("reason", "") or ""),
                "diagnosis": str(summary.get("diagnosis", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("alert_level", "") or "") == "ALERT" else 1 if str(row.get("alert_level", "") or "") == "WATCH" else 2,
            -float(row.get("latest_stress_worst_loss", 0.0) or 0.0),
            float(row.get("scale_delta", 0.0) or 0.0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows[:12]


def _build_feedback_calibration_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        calibration = dict(card.get("weekly_feedback_calibration", {}) or {})
        if not calibration:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "selection_scope_label": str(calibration.get("selection_scope_label", "") or "-"),
                "selected_horizon_days": str(calibration.get("selected_horizon_days", "") or "-"),
                "outcome_sample_count": int(_safe_float(calibration.get("outcome_sample_count"), 0.0)),
                "outcome_positive_rate": _safe_float(calibration.get("outcome_positive_rate"), 0.0),
                "outcome_broken_rate": _safe_float(calibration.get("outcome_broken_rate"), 0.0),
                "avg_future_return": _safe_float(calibration.get("avg_future_return"), 0.0),
                "avg_max_drawdown": _safe_float(calibration.get("avg_max_drawdown"), 0.0),
                "score_alignment_score": _safe_float(calibration.get("score_alignment_score"), 0.5),
                "signal_quality_score": _safe_float(calibration.get("signal_quality_score"), 0.5),
                "shadow_threshold_relax_support": _safe_float(calibration.get("shadow_threshold_relax_support"), 0.5),
                "shadow_weak_signal_support": _safe_float(calibration.get("shadow_weak_signal_support"), 0.5),
                "risk_tighten_support": _safe_float(calibration.get("risk_tighten_support"), 0.5),
                "risk_relax_support": _safe_float(calibration.get("risk_relax_support"), 0.5),
                "execution_support": _safe_float(calibration.get("execution_support"), 0.5),
                "calibration_confidence": _safe_float(calibration.get("calibration_confidence"), 0.0),
                "calibration_confidence_label": str(calibration.get("calibration_confidence_label", "") or "-"),
                "calibration_reason": str(calibration.get("calibration_reason", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row.get("outcome_sample_count", 0) or 0),
            -float(row.get("signal_quality_score", 0.0) or 0.0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_feedback_automation_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        automation_map = dict(card.get("weekly_feedback_automation_map", {}) or {})
        for feedback_kind in ("shadow", "risk", "execution"):
            row = dict(automation_map.get(feedback_kind, {}) or {})
            if not row:
                continue
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "feedback_kind": str(row.get("feedback_kind", "") or feedback_kind),
                    "feedback_kind_label": str(row.get("feedback_kind_label", "") or feedback_kind),
                    "feedback_action": str(row.get("feedback_action", "") or "-"),
                    "calibration_apply_mode": str(row.get("calibration_apply_mode", "") or "HOLD"),
                    "calibration_apply_mode_label": str(row.get("calibration_apply_mode_label", "") or "继续观察"),
                    "calibration_basis_label": str(row.get("calibration_basis_label", "") or "-"),
                    "market_data_gate_label": str(row.get("market_data_gate_label", "") or ""),
                    "market_data_gate_reason": str(row.get("market_data_gate_reason", "") or ""),
                    "paper_auto_apply_enabled": int(_safe_float(row.get("paper_auto_apply_enabled"), 0.0)),
                    "live_confirmation_required": int(_safe_float(row.get("live_confirmation_required"), 0.0)),
                    "feedback_base_confidence": _safe_float(row.get("feedback_base_confidence"), 0.0),
                    "feedback_base_confidence_label": str(row.get("feedback_base_confidence_label", "") or "-"),
                    "feedback_calibration_score": _safe_float(row.get("feedback_calibration_score"), 0.5),
                    "feedback_calibration_label": str(row.get("feedback_calibration_label", "") or "-"),
                    "feedback_confidence": _safe_float(row.get("feedback_confidence"), 0.0),
                    "feedback_confidence_label": str(row.get("feedback_confidence_label", "") or "-"),
                    "feedback_sample_count": int(_safe_float(row.get("feedback_sample_count"), 0.0)),
                    "feedback_calibration_sample_count": int(_safe_float(row.get("feedback_calibration_sample_count"), 0.0)),
                    "outcome_maturity_ratio": _safe_float(row.get("outcome_maturity_ratio"), 0.0),
                    "outcome_maturity_label": str(row.get("outcome_maturity_label", "") or "UNKNOWN"),
                    "outcome_pending_sample_count": int(_safe_float(row.get("outcome_pending_sample_count"), 0.0)),
                    "outcome_ready_estimate_end_ts": str(row.get("outcome_ready_estimate_end_ts", "") or ""),
                    "automation_reason": str(row.get("automation_reason", "") or ""),
                }
            )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("calibration_apply_mode", "") or "") == "AUTO_APPLY" else 1 if str(row.get("calibration_apply_mode", "") or "") == "SUGGEST_ONLY" else 2,
            0 if str(row.get("feedback_kind", "") or "") == "execution" else 1 if str(row.get("feedback_kind", "") or "") == "risk" else 2,
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_feedback_maturity_alert_overview(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    out: List[Dict[str, Any]] = []
    for raw in list(rows or []):
        row = dict(raw)
        apply_mode = str(row.get("calibration_apply_mode", "") or "").strip().upper()
        maturity_label = str(row.get("outcome_maturity_label", "") or "UNKNOWN").strip().upper()
        pending_count = int(_safe_float(row.get("outcome_pending_sample_count"), 0.0))
        ready_end_text = str(row.get("outcome_ready_estimate_end_ts", "") or "").strip()
        days_until_ready = 999
        if ready_end_text:
            try:
                ready_end_ts = datetime.fromisoformat(ready_end_text.replace("Z", "+00:00"))
                if ready_end_ts.tzinfo is None:
                    ready_end_ts = ready_end_ts.replace(tzinfo=timezone.utc)
                days_until_ready = max(0, (ready_end_ts.date() - now_utc.date()).days)
            except Exception:
                days_until_ready = 999
        alert_bucket = ""
        suggestion = ""
        if apply_mode == "AUTO_APPLY" and maturity_label in {"MATURE", "LATE"}:
            alert_bucket = "ACTIVE"
            suggestion = "这组 feedback 已满足自动应用条件，当前可以重点跟踪自动应用后的实际表现。"
        elif apply_mode != "AUTO_APPLY" and maturity_label in {"MATURE", "LATE"}:
            alert_bucket = "READY"
            suggestion = "样本已经足够成熟，优先复核这组 feedback 是否该进入自动应用。"
        elif apply_mode != "AUTO_APPLY" and pending_count > 0 and days_until_ready <= 2:
            alert_bucket = "SOON"
            suggestion = "样本即将成熟，建议在 ready 时间后优先复核这组 feedback。"
        if not alert_bucket:
            continue
        out.append(
            {
                "market": str(row.get("market", "") or ""),
                "watchlist": str(row.get("watchlist", "") or ""),
                "portfolio_id": str(row.get("portfolio_id", "") or ""),
                "feedback_kind_label": str(row.get("feedback_kind_label", "") or "-"),
                "calibration_apply_mode_label": str(row.get("calibration_apply_mode_label", "") or "-"),
                "outcome_maturity_ratio": _safe_float(row.get("outcome_maturity_ratio"), 0.0),
                "outcome_maturity_label": maturity_label or "UNKNOWN",
                "outcome_pending_sample_count": pending_count,
                "days_until_ready": int(days_until_ready if days_until_ready < 999 else -1),
                "ready_estimate_end_ts": ready_end_text,
                "alert_bucket": alert_bucket,
                "suggestion": suggestion,
            }
        )
    out.sort(
        key=lambda row: (
            0 if str(row.get("alert_bucket", "") or "") == "ACTIVE" else 1 if str(row.get("alert_bucket", "") or "") == "READY" else 2,
            int(row.get("days_until_ready", -1) if int(row.get("days_until_ready", -1)) >= 0 else 999),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return out[:12]


def _build_feedback_automation_history_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for raw in list(card.get("feedback_automation_history_rows", []) or []):
            row = dict(raw)
            feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
            if not feedback_kind:
                continue
            grouped.setdefault(feedback_kind, []).append(row)
        for feedback_kind, history_rows in grouped.items():
            history_rows = sorted(
                history_rows,
                key=lambda row: (
                    str(row.get("week_start", "") or ""),
                    str(row.get("ts", "") or ""),
                ),
                reverse=True,
            )
            current = dict(history_rows[0])
            previous = dict(history_rows[1]) if len(history_rows) > 1 else {}
            state_chain = " -> ".join(
                f"{str(row.get('week_label', '') or '-')}"
                f":{str(row.get('state_label', '') or _feedback_history_state_label(row) or '-')}"
                for row in reversed(history_rows[:4])
            )
            same_state_weeks = 0
            current_state = str(current.get("state_label", "") or _feedback_history_state_label(current))
            for row in history_rows:
                if str(row.get("state_label", "") or _feedback_history_state_label(row)) != current_state:
                    break
                same_state_weeks += 1
            transition_label = "首次记录"
            if previous:
                previous_state = str(previous.get("state_label", "") or _feedback_history_state_label(previous))
                transition_label = "状态变化" if previous_state != current_state else "持续观察"
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "feedback_kind_label": str(current.get("feedback_kind_label", "") or feedback_kind),
                    "current_state": current_state,
                    "current_mode": str(current.get("calibration_apply_mode_label", "") or "-"),
                    "current_week": str(current.get("week_label", "") or "-"),
                    "transition": transition_label,
                    "same_state_weeks": int(same_state_weeks),
                    "weeks_tracked": int(len(history_rows)),
                    "maturity": (
                        f"{float(current.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                        f"{str(current.get('outcome_maturity_label', '') or 'UNKNOWN')}"
                    ),
                    "pending": int(_safe_float(current.get("outcome_pending_sample_count"), 0.0)),
                    "ready": str(current.get("outcome_ready_estimate_end_ts", "") or "")[:10] or "-",
                    "state_chain": state_chain,
                    "reason": str(current.get("automation_reason", "") or "-"),
                }
            )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("current_state", "") or "") == "ACTIVE" else 1 if str(row.get("current_state", "") or "") == "READY" else 2 if str(row.get("current_state", "") or "") == "SOON" else 3,
            -int(row.get("same_state_weeks", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return rows[:18]


def _feedback_stuck_bucket(current_state: str, current_mode: str, *, same_state_weeks: int) -> str:
    if current_state == "READY":
        return "已成熟仍未应用"
    if current_mode == "SUGGEST_ONLY":
        return "长期建议确认"
    if current_state == "SOON":
        return "长期等待成熟"
    if same_state_weeks >= 3:
        return "长期继续观察"
    return "继续观察"


def _build_feedback_automation_stuck_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for raw in list(card.get("feedback_automation_history_rows", []) or []):
            row = dict(raw)
            feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
            if not feedback_kind:
                continue
            grouped.setdefault(feedback_kind, []).append(row)
        for feedback_kind, history_rows in grouped.items():
            history_rows = sorted(
                history_rows,
                key=lambda row: (
                    str(row.get("week_start", "") or ""),
                    str(row.get("ts", "") or ""),
                ),
                reverse=True,
            )
            current = dict(history_rows[0])
            current_state = str(current.get("state_label", "") or _feedback_history_state_label(current))
            current_mode = str(current.get("calibration_apply_mode", "") or "HOLD").strip().upper() or "HOLD"
            same_state_weeks = 0
            for row in history_rows:
                if str(row.get("state_label", "") or _feedback_history_state_label(row)) != current_state:
                    break
                same_state_weeks += 1
            if current_state == "ACTIVE":
                continue
            if same_state_weeks < 2 and current_mode not in {"SUGGEST_ONLY", "HOLD"}:
                continue
            reason = str(current.get("automation_reason", "") or current.get("feedback_reason", "") or "-")
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "feedback_kind_label": str(current.get("feedback_kind_label", "") or feedback_kind),
                    "current_state": current_state,
                    "current_mode": str(current.get("calibration_apply_mode_label", "") or "-"),
                    "same_state_weeks": int(same_state_weeks),
                    "weeks_tracked": int(len(history_rows)),
                    "maturity": (
                        f"{float(current.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                        f"{str(current.get('outcome_maturity_label', '') or 'UNKNOWN')}"
                    ),
                    "pending": int(_safe_float(current.get("outcome_pending_sample_count"), 0.0)),
                    "ready": str(current.get("outcome_ready_estimate_end_ts", "") or "")[:10] or "-",
                    "stuck_bucket": _feedback_stuck_bucket(current_state, current_mode, same_state_weeks=int(same_state_weeks)),
                    "reason": reason,
                }
            )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("current_state", "") or "") == "READY" else 1 if str(row.get("current_mode", "") or "") == "建议确认" else 2,
            -int(row.get("same_state_weeks", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return rows[:12]


def _feedback_effect_snapshot_from_history(row: Dict[str, Any]) -> Dict[str, Any]:
    details = dict(row.get("details_json", {}) or {})
    return dict(details.get("effect_snapshot", {}) or {})


def _feedback_effect_compare_snapshot(
    feedback_kind: str,
    baseline: Dict[str, Any],
    current: Dict[str, Any],
) -> tuple[str, str]:
    kind = str(feedback_kind or "").strip().lower()
    if not baseline or not current:
        return "-", "-"
    if kind == "execution":
        gap_delta = _safe_float(current.get("execution_cost_gap"), 0.0) - _safe_float(baseline.get("execution_cost_gap"), 0.0)
        actual_delta = _safe_float(current.get("avg_actual_slippage_bps"), 0.0) - _safe_float(baseline.get("avg_actual_slippage_bps"), 0.0)
        if gap_delta <= -2.0 and actual_delta <= -3.0:
            label = "改善"
        elif gap_delta >= 2.0 and actual_delta >= 3.0:
            label = "恶化"
        else:
            label = "稳定"
        metric = f"gapΔ={_fmt_money(gap_delta)} / slipΔ={actual_delta:+.1f}bps"
        return label, metric
    if kind == "risk":
        scale_delta = _safe_float(current.get("latest_dynamic_scale"), 1.0) - _safe_float(baseline.get("latest_dynamic_scale"), 1.0)
        stress_delta = _safe_float(current.get("latest_stress_worst_loss"), 0.0) - _safe_float(baseline.get("latest_stress_worst_loss"), 0.0)
        corr_delta = _safe_float(current.get("latest_avg_pair_correlation"), 0.0) - _safe_float(baseline.get("latest_avg_pair_correlation"), 0.0)
        if scale_delta >= 0.03 and stress_delta <= -0.01 and corr_delta <= -0.03:
            label = "改善"
        elif scale_delta <= -0.03 and (stress_delta >= 0.01 or corr_delta >= 0.03):
            label = "恶化"
        else:
            label = "稳定"
        metric = f"scaleΔ={scale_delta:+.2f} / stressΔ={stress_delta:+.1%} / corrΔ={corr_delta:+.2f}"
        return label, metric
    positive_delta = _safe_float(current.get("outcome_positive_rate"), 0.0) - _safe_float(baseline.get("outcome_positive_rate"), 0.0)
    broken_delta = _safe_float(current.get("outcome_broken_rate"), 0.0) - _safe_float(baseline.get("outcome_broken_rate"), 0.0)
    align_delta = _safe_float(current.get("score_alignment_score"), 0.0) - _safe_float(baseline.get("score_alignment_score"), 0.0)
    if positive_delta >= 0.05 and broken_delta <= -0.03 and align_delta >= 0.04:
        label = "改善"
    elif positive_delta <= -0.05 and broken_delta >= 0.03 and align_delta <= -0.04:
        label = "恶化"
    else:
        label = "稳定"
    metric = f"posΔ={positive_delta:+.1%} / brokenΔ={broken_delta:+.1%} / alignΔ={align_delta:+.2f}"
    return label, metric


def _feedback_effect_milestone(active_rows_asc: List[Dict[str, Any]], feedback_kind: str, week_offset: int) -> str:
    if len(active_rows_asc) <= week_offset:
        return "-"
    baseline = _feedback_effect_snapshot_from_history(active_rows_asc[0])
    current = _feedback_effect_snapshot_from_history(active_rows_asc[week_offset])
    label, metric = _feedback_effect_compare_snapshot(feedback_kind, baseline, current)
    if label == "-" and metric == "-":
        return "-"
    return f"{label} ({metric})"


def _feedback_effect_bucket(text: Any) -> str:
    value = str(text or "").strip()
    if "恶化" in value:
        return "恶化"
    if "改善" in value:
        return "改善"
    if "稳定" in value:
        return "稳定"
    if value in {"-", ""}:
        return "无样本"
    return "观察中"


def _build_feedback_automation_effect_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        history_grouped: Dict[str, List[Dict[str, Any]]] = {}
        for raw in list(card.get("feedback_automation_history_rows", []) or []):
            row = dict(raw)
            feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
            if not feedback_kind:
                continue
            history_grouped.setdefault(feedback_kind, []).append(row)

        for feedback_kind, history_rows in history_grouped.items():
            history_rows = sorted(
                history_rows,
                key=lambda row: (
                    str(row.get("week_start", "") or ""),
                    str(row.get("ts", "") or ""),
                ),
                reverse=True,
            )
            current = dict(history_rows[0])
            current_state = str(current.get("state_label", "") or _feedback_history_state_label(current))
            current_mode = str(current.get("calibration_apply_mode", "") or "").strip().upper()
            if current_state != "ACTIVE" and current_mode != "AUTO_APPLY":
                continue
            active_weeks = 0
            for row in history_rows:
                row_state = str(row.get("state_label", "") or _feedback_history_state_label(row))
                row_mode = str(row.get("calibration_apply_mode", "") or "").strip().upper()
                if row_state != "ACTIVE" and row_mode != "AUTO_APPLY":
                    break
                active_weeks += 1
            active_rows_asc = list(reversed(history_rows[:active_weeks]))
            baseline_week = str(active_rows_asc[0].get("week_label", "") or "-") if active_rows_asc else "-"

            effect_label = "观察中"
            effect_metric = "-"
            reason = str(current.get("automation_reason", "") or current.get("feedback_reason", "") or "-")
            driver = str(current.get("feedback_action", "") or "-")

            if feedback_kind == "execution":
                feedback = dict(card.get("execution_feedback", {}) or {})
                planned_cost = float(feedback.get("planned_execution_cost_total", 0.0) or 0.0)
                cost_gap = float(feedback.get("execution_cost_gap", 0.0) or 0.0)
                expected_bps = float(feedback.get("avg_expected_cost_bps", 0.0) or 0.0)
                actual_bps = float(feedback.get("avg_actual_slippage_bps", 0.0) or 0.0)
                if planned_cost > 0.0 or actual_bps > 0.0:
                    if cost_gap <= 0.0 and actual_bps <= expected_bps + 2.0:
                        effect_label = "改善"
                    elif cost_gap <= max(2.0, planned_cost * 0.12) and actual_bps <= expected_bps + 6.0:
                        effect_label = "稳定"
                    else:
                        effect_label = "待观察"
                effect_metric = (
                    f"gap={_fmt_money(cost_gap)} / actual={actual_bps:.1f}bps / expected={expected_bps:.1f}bps"
                )
                reason = str(
                    feedback.get("feedback_reason", "")
                    or feedback.get("apply_status_reason", "")
                    or reason
                )
                driver = str(feedback.get("dominant_execution_session_label", "") or driver)
            elif feedback_kind == "risk":
                feedback = dict(card.get("paper_risk_feedback", {}) or {})
                risk_review = dict(card.get("weekly_risk_review", {}) or {})
                dynamic_scale = float(risk_review.get("latest_dynamic_scale", 1.0) or 1.0)
                stress = float(risk_review.get("latest_stress_worst_loss", 0.0) or 0.0)
                corr = float(risk_review.get("latest_avg_pair_correlation", 0.0) or 0.0)
                if dynamic_scale >= 0.90 and stress <= 0.06 and corr <= 0.50:
                    effect_label = "改善"
                elif dynamic_scale >= 0.78 and stress <= 0.085 and corr <= 0.62:
                    effect_label = "稳定"
                else:
                    effect_label = "仍偏紧"
                effect_metric = f"scale={dynamic_scale:.2f} / stress={stress:.1%} / corr={corr:.2f}"
                reason = str(feedback.get("feedback_reason", "") or risk_review.get("risk_diagnosis", "") or reason)
                driver = str(risk_review.get("dominant_risk_driver", "") or driver)
            else:
                calibration = dict(card.get("weekly_feedback_calibration", {}) or {})
                sample_count = int(_safe_float(calibration.get("outcome_sample_count"), 0.0))
                positive_rate = float(calibration.get("outcome_positive_rate", 0.0) or 0.0)
                broken_rate = float(calibration.get("outcome_broken_rate", 0.0) or 0.0)
                alignment = float(calibration.get("score_alignment_score", 0.0) or 0.0)
                if sample_count >= 12 and positive_rate >= 0.58 and broken_rate <= 0.15 and alignment >= 0.55:
                    effect_label = "支持"
                elif sample_count > 0:
                    effect_label = "观察中"
                effect_metric = (
                    f"samples={sample_count} / positive={positive_rate:.1%} / broken={broken_rate:.1%}"
                )
                reason = str(calibration.get("calibration_reason", "") or reason)
                driver = f"align={alignment:.2f}"

            if len(active_rows_asc) >= 2:
                baseline_snapshot = _feedback_effect_snapshot_from_history(active_rows_asc[0])
                latest_snapshot = _feedback_effect_snapshot_from_history(active_rows_asc[-1])
                compare_label, compare_metric = _feedback_effect_compare_snapshot(
                    feedback_kind,
                    baseline_snapshot,
                    latest_snapshot,
                )
                if compare_label != "-":
                    effect_label = compare_label
                    effect_metric = compare_metric

            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "feedback_kind_label": str(current.get("feedback_kind_label", "") or feedback_kind),
                    "current_state": current_state,
                    "current_mode": str(current.get("calibration_apply_mode_label", "") or "-"),
                    "baseline_week": baseline_week,
                    "active_weeks": int(active_weeks),
                    "effect_label": effect_label,
                    "effect_metric": effect_metric,
                    "effect_w1": _feedback_effect_milestone(active_rows_asc, feedback_kind, 1),
                    "effect_w2": _feedback_effect_milestone(active_rows_asc, feedback_kind, 2),
                    "effect_w4": _feedback_effect_milestone(active_rows_asc, feedback_kind, 4),
                    "driver": driver or "-",
                    "reason": reason,
                }
            )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("effect_label", "") or "") == "待观察" else 1 if str(row.get("effect_label", "") or "") == "仍偏紧" else 2,
            -int(row.get("active_weeks", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return rows[:12]


def _build_feedback_automation_effect_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for raw in list(rows or []):
        row = dict(raw)
        market = str(row.get("market", "") or "-")
        feedback_kind_label = str(row.get("feedback_kind_label", "") or "-")
        key = (market, feedback_kind_label)
        item = grouped.setdefault(
            key,
            {
                "market": market,
                "feedback_kind_label": feedback_kind_label,
                "tracked_count": 0,
                "latest_improved_count": 0,
                "latest_deteriorated_count": 0,
                "latest_stable_count": 0,
                "latest_observe_count": 0,
                "w1_improved_count": 0,
                "w2_improved_count": 0,
                "w4_improved_count": 0,
                "w1_deteriorated_count": 0,
                "w2_deteriorated_count": 0,
                "w4_deteriorated_count": 0,
                "active_weeks_total": 0,
                "top_portfolios": [],
            },
        )
        item["tracked_count"] = int(item.get("tracked_count", 0) or 0) + 1
        item["active_weeks_total"] = int(item.get("active_weeks_total", 0) or 0) + int(row.get("active_weeks", 0) or 0)
        latest_bucket = _feedback_effect_bucket(row.get("effect_label"))
        if latest_bucket == "改善":
            item["latest_improved_count"] = int(item.get("latest_improved_count", 0) or 0) + 1
        elif latest_bucket == "恶化":
            item["latest_deteriorated_count"] = int(item.get("latest_deteriorated_count", 0) or 0) + 1
        elif latest_bucket == "稳定":
            item["latest_stable_count"] = int(item.get("latest_stable_count", 0) or 0) + 1
        else:
            item["latest_observe_count"] = int(item.get("latest_observe_count", 0) or 0) + 1
        for horizon in ("w1", "w2", "w4"):
            bucket = _feedback_effect_bucket(row.get(f"effect_{horizon}"))
            if bucket == "改善":
                item[f"{horizon}_improved_count"] = int(item.get(f"{horizon}_improved_count", 0) or 0) + 1
            elif bucket == "恶化":
                item[f"{horizon}_deteriorated_count"] = int(item.get(f"{horizon}_deteriorated_count", 0) or 0) + 1
        top_portfolios = list(item.get("top_portfolios", []) or [])
        top_portfolios.append(
            f"{str(row.get('portfolio_id', '') or '-')}:"
            f"{_feedback_effect_bucket(row.get('effect_label'))}"
        )
        item["top_portfolios"] = top_portfolios[:5]

    out = list(grouped.values())
    for row in out:
        tracked = int(row.get("tracked_count", 0) or 0)
        row["avg_active_weeks"] = float(row.get("active_weeks_total", 0) or 0) / float(tracked or 1)
        if int(row.get("latest_deteriorated_count", 0) or 0) > 0:
            row["summary_signal"] = "需复核"
        elif int(row.get("latest_improved_count", 0) or 0) >= max(1, tracked // 2):
            row["summary_signal"] = "持续改善"
        elif int(row.get("latest_stable_count", 0) or 0) > 0:
            row["summary_signal"] = "稳定跟踪"
        else:
            row["summary_signal"] = "观察中"
        row["top_portfolios_text"] = " / ".join(list(row.get("top_portfolios", []) or [])[:3]) or "-"
    out.sort(
        key=lambda row: (
            0 if str(row.get("summary_signal", "") or "") == "需复核" else 1 if str(row.get("summary_signal", "") or "") == "持续改善" else 2,
            -int(row.get("latest_deteriorated_count", 0) or 0),
            -int(row.get("latest_improved_count", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("feedback_kind_label", "") or ""),
        )
    )
    return out[:12]


def _build_labeling_skip_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        # 这里展示的是“结果校准输入缺口”，不是新的风控结论。
        # 目的只是回答：为什么当前组合还没有形成足够的 outcome 样本来校准 weekly feedback。
        for raw in list(card.get("weekly_labeling_skips", []) or []):
            row = dict(raw)
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "horizon_days": str(row.get("horizon_days", "") or "-"),
                    "skip_reason": str(row.get("skip_reason", "") or ""),
                    "skip_reason_label": str(row.get("skip_reason_label", "") or str(row.get("skip_reason", "") or "-")),
                    "skip_count": int(_safe_float(row.get("skip_count"), 0.0)),
                    "symbol_count": int(_safe_float(row.get("symbol_count"), 0.0)),
                    "sample_symbols": str(row.get("sample_symbols", "") or ""),
                    "oldest_snapshot_ts": str(row.get("oldest_snapshot_ts", "") or ""),
                    "latest_snapshot_ts": str(row.get("latest_snapshot_ts", "") or ""),
                    "min_remaining_forward_bars": int(_safe_float(row.get("min_remaining_forward_bars"), 0.0)),
                    "max_remaining_forward_bars": int(_safe_float(row.get("max_remaining_forward_bars"), 0.0)),
                    "estimated_ready_start_ts": str(row.get("estimated_ready_start_ts", "") or ""),
                    "estimated_ready_end_ts": str(row.get("estimated_ready_end_ts", "") or ""),
                }
            )
    rows.sort(
        key=lambda row: (
            -int(row.get("skip_count", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
            str(row.get("skip_reason", "") or ""),
        )
    )
    return rows[:24]


def _build_labeling_ready_overview(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    tomorrow_utc = (now_utc + timedelta(days=1)).date()
    for raw in list(rows or []):
        if str(raw.get("skip_reason", "") or "").upper() != "INSUFFICIENT_FORWARD_BARS":
            continue
        ready_end_text = str(raw.get("estimated_ready_end_ts", "") or "")
        ready_start_text = str(raw.get("estimated_ready_start_ts", "") or "")
        if not ready_end_text:
            continue
        try:
            ready_end_ts = datetime.fromisoformat(ready_end_text)
        except Exception:
            continue
        if ready_end_ts.tzinfo is None:
            ready_end_ts = ready_end_ts.replace(tzinfo=timezone.utc)
        try:
            ready_start_ts = datetime.fromisoformat(ready_start_text) if ready_start_text else ready_end_ts
        except Exception:
            ready_start_ts = ready_end_ts
        if ready_start_ts.tzinfo is None:
            ready_start_ts = ready_start_ts.replace(tzinfo=timezone.utc)
        days_until_ready = max(0, (ready_end_ts.date() - now_utc.date()).days)
        out.append(
            {
                "market": str(raw.get("market", "") or ""),
                "watchlist": str(raw.get("watchlist", "") or ""),
                "portfolio_id": str(raw.get("portfolio_id", "") or ""),
                "horizon_days": str(raw.get("horizon_days", "") or "-"),
                "skip_count": int(raw.get("skip_count", 0) or 0),
                "symbol_count": int(raw.get("symbol_count", 0) or 0),
                "min_remaining_forward_bars": int(raw.get("min_remaining_forward_bars", 0) or 0),
                "max_remaining_forward_bars": int(raw.get("max_remaining_forward_bars", 0) or 0),
                "estimated_ready_start_ts": ready_start_ts.isoformat(),
                "estimated_ready_end_ts": ready_end_ts.isoformat(),
                "days_until_ready": int(days_until_ready),
                "ready_bucket": "TOMORROW" if ready_end_ts.date() <= tomorrow_utc else "LATER",
            }
        )
    out.sort(
        key=lambda row: (
            int(row.get("days_until_ready", 999) or 999),
            int(row.get("min_remaining_forward_bars", 999) or 999),
            -int(row.get("skip_count", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return out[:12]


def _build_risk_feedback_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        feedback = dict(card.get("paper_risk_feedback", {}) or {})
        if not bool(feedback.get("feedback_present", False)):
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "apply_mode_label": str(feedback.get("apply_mode_label", "") or ""),
                "risk_feedback_action": str(feedback.get("risk_feedback_action", "") or ""),
                "effective_source_label": str(feedback.get("effective_source_label", "") or ""),
                "base_max_single_weight": _safe_float(feedback.get("base_max_single_weight"), 0.0),
                "effective_max_single_weight": _safe_float(feedback.get("effective_max_single_weight"), 0.0),
                "base_max_net_exposure": _safe_float(feedback.get("base_max_net_exposure"), 0.0),
                "effective_max_net_exposure": _safe_float(feedback.get("effective_max_net_exposure"), 0.0),
                "base_max_gross_exposure": _safe_float(feedback.get("base_max_gross_exposure"), 0.0),
                "effective_max_gross_exposure": _safe_float(feedback.get("effective_max_gross_exposure"), 0.0),
                "base_correlation_soft_limit": _safe_float(feedback.get("base_correlation_soft_limit"), 0.0),
                "effective_correlation_soft_limit": _safe_float(feedback.get("effective_correlation_soft_limit"), 0.0),
                "feedback_base_confidence": _safe_float(feedback.get("feedback_base_confidence"), 1.0),
                "feedback_base_confidence_label": str(feedback.get("feedback_base_confidence_label", "") or "HIGH"),
                "feedback_calibration_score": _safe_float(feedback.get("feedback_calibration_score"), 0.5),
                "feedback_calibration_label": str(feedback.get("feedback_calibration_label", "") or "MEDIUM"),
                "feedback_confidence": _safe_float(feedback.get("feedback_confidence"), 1.0),
                "feedback_confidence_label": str(feedback.get("feedback_confidence_label", "") or "HIGH"),
                "feedback_sample_count": int(_safe_float(feedback.get("feedback_sample_count"), 0.0)),
                "feedback_calibration_sample_count": int(_safe_float(feedback.get("feedback_calibration_sample_count"), 0.0)),
                "feedback_calibration_reason": str(feedback.get("feedback_calibration_reason", "") or ""),
                "feedback_reason": str(feedback.get("feedback_reason", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("risk_feedback_action", "") or "") == "TIGHTEN" else 1 if str(row.get("risk_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_execution_mode_recommendation_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        recommendation = dict(card.get("execution_mode_recommendation", {}) or {})
        summary = dict(card.get("risk_trend_summary", {}) or {})
        if not recommendation:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "current_mode": str(recommendation.get("current_mode_label", "") or "-"),
                "recommended_mode": str(recommendation.get("recommended_mode_label", "") or "-"),
                "differs_from_current": bool(recommendation.get("differs_from_current", False)),
                "alert_level": str(summary.get("alert_level", "") or "-"),
                "trend_label": str(summary.get("trend_label", "") or "-"),
                "alert_streak": int(recommendation.get("consecutive_alert_count", 0) or 0),
                "watch_streak": int(recommendation.get("consecutive_watch_count", 0) or 0),
                "reason": str(recommendation.get("reason", "") or "-"),
            }
        )
    rows.sort(
        key=lambda row: (
            not bool(row.get("differs_from_current", False)),
            0 if str(row.get("recommended_mode", "") or "") == "暂停自动执行" else 1,
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_execution_mode_recommendation_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    recommendation_rows = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    pause_count = 0
    review_only_count = 0
    mismatch_count = 0
    market_stats: Dict[str, Dict[str, Any]] = {}
    for row in recommendation_rows:
        if not bool(row.get("differs_from_current", False)):
            continue
        mismatch_count += 1
        market = str(row.get("market", "") or "-")
        market_row = market_stats.setdefault(
            market,
            {
                "market": market,
                "mismatch_count": 0,
                "review_only_count": 0,
                "paused_count": 0,
            },
        )
        market_row["mismatch_count"] = int(market_row.get("mismatch_count", 0) or 0) + 1
        recommended_mode = str(row.get("recommended_mode", "") or "")
        if recommended_mode == str(EXECUTION_MODE_LABELS.get("PAUSED", "")):
            pause_count += 1
            market_row["paused_count"] = int(market_row.get("paused_count", 0) or 0) + 1
        elif recommended_mode == str(EXECUTION_MODE_LABELS.get("REVIEW_ONLY", "")):
            review_only_count += 1
            market_row["review_only_count"] = int(market_row.get("review_only_count", 0) or 0) + 1
    market_rows = list(market_stats.values())
    market_rows.sort(
        key=lambda row: (
            -int(row.get("paused_count", 0) or 0),
            -int(row.get("review_only_count", 0) or 0),
            str(row.get("market", "") or ""),
        )
    )
    return {
        "mismatch_count": int(mismatch_count),
        "review_only_count": int(review_only_count),
        "paused_count": int(pause_count),
        "market_rows": market_rows,
        "summary_text": (
            f"{int(mismatch_count)} 个组合建议切换："
            f"{int(review_only_count)} 个建议 {EXECUTION_MODE_LABELS['REVIEW_ONLY']}，"
            f"{int(pause_count)} 个建议 {EXECUTION_MODE_LABELS['PAUSED']}"
            if mismatch_count > 0
            else "当前执行模式与风险建议一致"
        ),
    }


def _build_weekly_attribution_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        attribution = dict(card.get("weekly_attribution", {}) or {})
        if not attribution:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "weekly_return": _safe_float(attribution.get("weekly_return"), 0.0),
                "selection_contribution": _safe_float(attribution.get("selection_contribution"), 0.0),
                "sizing_contribution": _safe_float(attribution.get("sizing_contribution"), 0.0),
                "sector_contribution": _safe_float(attribution.get("sector_contribution"), 0.0),
                "execution_contribution": _safe_float(attribution.get("execution_contribution"), 0.0),
                "market_contribution": _safe_float(attribution.get("market_contribution"), 0.0),
                "planned_execution_cost_total": _safe_float(attribution.get("planned_execution_cost_total"), 0.0),
                "execution_cost_total": _safe_float(attribution.get("execution_cost_total"), 0.0),
                "execution_cost_gap": _safe_float(attribution.get("execution_cost_gap"), 0.0),
                "avg_expected_cost_bps": _safe_float(attribution.get("avg_expected_cost_bps"), 0.0),
                "avg_actual_slippage_bps": _safe_float(attribution.get("avg_actual_slippage_bps"), 0.0),
                "dominant_driver": str(attribution.get("dominant_driver", "") or ""),
                "diagnosis": str(attribution.get("diagnosis", "") or ""),
            }
        )
    rows.sort(key=lambda row: abs(float(row.get("weekly_return", 0.0) or 0.0)), reverse=True)
    return rows


def _build_execution_cost_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        attribution = dict(card.get("weekly_attribution", {}) or {})
        if not attribution:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "planned_execution_cost_total": _safe_float(attribution.get("planned_execution_cost_total"), 0.0),
                "execution_cost_total": _safe_float(attribution.get("execution_cost_total"), 0.0),
                "execution_cost_gap": _safe_float(attribution.get("execution_cost_gap"), 0.0),
                "avg_expected_cost_bps": _safe_float(attribution.get("avg_expected_cost_bps"), 0.0),
                "avg_actual_slippage_bps": _safe_float(attribution.get("avg_actual_slippage_bps"), 0.0),
                "execution_style_breakdown": str(attribution.get("execution_style_breakdown", "") or ""),
                "diagnosis": str(attribution.get("diagnosis", "") or ""),
            }
        )
    rows.sort(key=lambda row: abs(float(row.get("execution_cost_gap", 0.0) or 0.0)), reverse=True)
    return rows


def _build_execution_feedback_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        feedback = dict(card.get("execution_feedback", {}) or {})
        if not feedback:
            continue
        rows.append(
            {
                "market": str(card.get("market", "") or ""),
                "watchlist": str(card.get("watchlist", "") or ""),
                "portfolio_id": str(card.get("portfolio_id", "") or ""),
                "apply_mode_label": str(feedback.get("apply_mode_label", "") or ""),
                "execution_feedback_action": str(feedback.get("execution_feedback_action", "") or ""),
                "apply_status_code": str(feedback.get("apply_status_code", "") or ""),
                "apply_status_reason": str(feedback.get("apply_status_reason", "") or ""),
                "effective_source_label": str(feedback.get("effective_source_label", "") or ""),
                "base_adv_max_participation_pct": _safe_float(feedback.get("base_adv_max_participation_pct"), 0.0),
                "effective_adv_max_participation_pct": _safe_float(feedback.get("effective_adv_max_participation_pct"), 0.0),
                "base_adv_split_trigger_pct": _safe_float(feedback.get("base_adv_split_trigger_pct"), 0.0),
                "effective_adv_split_trigger_pct": _safe_float(feedback.get("effective_adv_split_trigger_pct"), 0.0),
                "base_max_slices_per_symbol": _safe_float(feedback.get("base_max_slices_per_symbol"), 0.0),
                "effective_max_slices_per_symbol": _safe_float(feedback.get("effective_max_slices_per_symbol"), 0.0),
                "base_open_session_participation_scale": _safe_float(feedback.get("base_open_session_participation_scale"), 0.0),
                "effective_open_session_participation_scale": _safe_float(feedback.get("effective_open_session_participation_scale"), 0.0),
                "feedback_base_confidence": _safe_float(feedback.get("feedback_base_confidence"), 1.0),
                "feedback_base_confidence_label": str(feedback.get("feedback_base_confidence_label", "") or "HIGH"),
                "feedback_calibration_score": _safe_float(feedback.get("feedback_calibration_score"), 0.5),
                "feedback_calibration_label": str(feedback.get("feedback_calibration_label", "") or "MEDIUM"),
                "feedback_confidence": _safe_float(feedback.get("feedback_confidence"), 1.0),
                "feedback_confidence_label": str(feedback.get("feedback_confidence_label", "") or "HIGH"),
                "feedback_sample_count": int(_safe_float(feedback.get("feedback_sample_count"), 0.0)),
                "feedback_calibration_sample_count": int(_safe_float(feedback.get("feedback_calibration_sample_count"), 0.0)),
                "feedback_calibration_reason": str(feedback.get("feedback_calibration_reason", "") or ""),
                "feedback_reason": str(feedback.get("feedback_reason", "") or ""),
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("execution_feedback_action", "") or "") == "TIGHTEN" else 1 if str(row.get("execution_feedback_action", "") or "") == "RELAX" else 2,
            str(row.get("market", "") or ""),
            str(row.get("watchlist", "") or ""),
        )
    )
    return rows


def _build_execution_feedback_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # 这里把“执行参数自动校准”的覆盖范围压成一张总览卡，方便判断第三阶段是否已经真正跑起来。
    total = int(len(rows))
    auto_apply_count = sum(1 for row in rows if str(row.get("apply_mode_label", "") or "") == "自动生效")
    suggest_only_count = sum(1 for row in rows if str(row.get("apply_mode_label", "") or "") == "仅建议未自动生效")
    base_only_count = sum(1 for row in rows if str(row.get("apply_mode_label", "") or "") == "沿用基础配置")
    no_feedback_status_codes = {
        "NO_WEEKLY_DATA",
        "NO_EXECUTION_ACTIVITY",
        "NO_OPPORTUNITY_PASS",
        "NO_QUALITY_PASS",
        "NO_GUARD_PASS",
        "NO_LIQUIDITY_PASS",
        "NO_ACTIONABLE_ORDERS",
        "NO_FILL_SAMPLE",
        "NO_COST_SAMPLE",
        "NO_FEEDBACK",
    }
    no_feedback_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") in no_feedback_status_codes)
    no_data_count = sum(
        1
        for row in rows
        if str(row.get("apply_status_code", "") or "") in {"NO_WEEKLY_DATA", "NO_EXECUTION_ACTIVITY"}
    )
    no_opportunity_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") == "NO_OPPORTUNITY_PASS")
    no_quality_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") == "NO_QUALITY_PASS")
    no_guard_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") == "NO_GUARD_PASS")
    no_liquidity_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") == "NO_LIQUIDITY_PASS")
    no_order_count = sum(
        1
        for row in rows
        if str(row.get("apply_status_code", "") or "") in {
            "NO_OPPORTUNITY_PASS",
            "NO_QUALITY_PASS",
            "NO_GUARD_PASS",
            "NO_LIQUIDITY_PASS",
            "NO_ACTIONABLE_ORDERS",
        }
    )
    no_fill_count = sum(
        1
        for row in rows
        if str(row.get("apply_status_code", "") or "") in {"NO_FILL_SAMPLE", "NO_COST_SAMPLE"}
    )
    policy_block_count = sum(
        1
        for row in rows
        if str(row.get("apply_status_code", "") or "") in {"LIVE_SUGGEST_ONLY", "PAPER_AUTO_APPLY_DISABLED", "MANUAL_REVIEW"}
    )
    predicted_count = sum(1 for row in rows if str(row.get("apply_status_code", "") or "") == "AUTO_APPLY_PREDICTED")
    tighten_count = sum(1 for row in rows if str(row.get("execution_feedback_action", "") or "").upper() == "TIGHTEN")
    relax_count = sum(1 for row in rows if str(row.get("execution_feedback_action", "") or "").upper() == "RELAX")
    decay_count = sum(1 for row in rows if str(row.get("execution_feedback_action", "") or "").upper() == "DECAY")
    overlay_count = sum(1 for row in rows if str(row.get("effective_source_label", "") or "") == "overlay 已落盘")
    paper_scope_count = sum(1 for row in rows if str(row.get("feedback_scope", "") or "").strip().lower() == "paper_only")
    avg_base_confidence = sum(float(row.get("feedback_base_confidence", 1.0) or 1.0) for row in rows) / float(total or 1)
    avg_calibration_score = sum(float(row.get("feedback_calibration_score", 0.5) or 0.5) for row in rows) / float(total or 1)
    avg_confidence = sum(float(row.get("feedback_confidence", 1.0) or 1.0) for row in rows) / float(total or 1)
    summary_text = (
        f"第三阶段起步：execution 自动校准 total={total} | "
        f"auto_apply={auto_apply_count} | suggest_only={suggest_only_count} | "
        f"no_feedback={no_feedback_count} | tighten={tighten_count} | relax={relax_count} | decay={decay_count} | "
        f"avg_base={avg_base_confidence:.2f} | avg_calib={avg_calibration_score:.2f} | avg_final={avg_confidence:.2f}"
    )
    return {
        "total_count": total,
        "auto_apply_count": int(auto_apply_count),
        "suggest_only_count": int(suggest_only_count),
        "base_only_count": int(base_only_count),
        "no_feedback_count": int(no_feedback_count),
        "no_data_count": int(no_data_count),
        "no_order_count": int(no_order_count),
        "no_fill_count": int(no_fill_count),
        "no_opportunity_count": int(no_opportunity_count),
        "no_quality_count": int(no_quality_count),
        "no_guard_count": int(no_guard_count),
        "no_liquidity_count": int(no_liquidity_count),
        "policy_block_count": int(policy_block_count),
        "predicted_count": int(predicted_count),
        "tighten_count": int(tighten_count),
        "relax_count": int(relax_count),
        "decay_count": int(decay_count),
        "overlay_count": int(overlay_count),
        "paper_scope_count": int(paper_scope_count),
        "avg_base_confidence": float(avg_base_confidence),
        "avg_calibration_score": float(avg_calibration_score),
        "avg_confidence": float(avg_confidence),
        "summary_text": summary_text,
    }


def _build_execution_hotspot_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        raw_hotspots = list(card.get("weekly_execution_hotspots", []) or [])
        if not raw_hotspots:
            raw_hotspots = list(dict(card.get("execution_feedback", {}) or {}).get("hotspot_rows", []) or [])
        for row in raw_hotspots[:6]:
            rows.append(
                {
                    "market": str(card.get("market", "") or ""),
                    "watchlist": str(card.get("watchlist", "") or ""),
                    "portfolio_id": str(card.get("portfolio_id", "") or ""),
                    "symbol": str(row.get("symbol", "") or ""),
                    "session_label": str(row.get("session_label", "") or row.get("session_bucket", "") or ""),
                    "hotspot_action": str(row.get("hotspot_action", "") or "-"),
                    "planned_execution_cost_total": _safe_float(row.get("planned_execution_cost_total"), 0.0),
                    "execution_cost_total": _safe_float(row.get("execution_cost_total"), 0.0),
                    "execution_cost_gap": _safe_float(row.get("execution_cost_gap"), 0.0),
                    "avg_expected_cost_bps": _safe_float(row.get("avg_expected_cost_bps"), 0.0),
                    "avg_actual_slippage_bps": _safe_float(row.get("avg_actual_slippage_bps"), 0.0),
                    "pressure_score": _safe_float(row.get("pressure_score"), 0.0),
                    "reason": str(row.get("reason", row.get("hotspot_reason", "")) or ""),
                }
            )
    rows.sort(
        key=lambda row: (
            -float(row.get("pressure_score", 0.0) or 0.0),
            -float(row.get("execution_cost_gap", 0.0) or 0.0),
            str(row.get("market", "") or ""),
            str(row.get("symbol", "") or ""),
        )
    )
    return rows[:18]


def _build_health_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for card in cards:
        health = dict(card.get("health_summary", {}) or {})
        rows.append(
            {
                "market": card["market"],
                "watchlist": card["watchlist"],
                "status": str(health.get("status", "OK") or "OK"),
                "status_detail": str(health.get("status_detail", "") or "-"),
                "delayed_count": int(health.get("delayed_count", 0) or 0),
                "permission_count": int(health.get("permission_count", 0) or 0),
                "connectivity_breaks": int(health.get("connectivity_breaks", 0) or 0),
                "account_limit_count": int(health.get("account_limit_count", 0) or 0),
                "latest_event_label": str(health.get("latest_event_label", "") or "-"),
                "latest_event_ts": str(health.get("latest_event_ts", "") or "-"),
            }
        )
    rows.sort(key=lambda row: (0 if row["status"] == "DEGRADED" else 1 if row["status"] == "LIMITED" else 2, row["market"], row["watchlist"]))
    return rows


def _market_data_health_status(
    *,
    ibkr_count: int,
    yfinance_count: int,
    missing_count: int,
    avg_source_coverage: float,
    avg_missing_ratio: float,
    research_only_yfinance: bool,
) -> tuple[str, str]:
    # 这里优先给协作者一个“能不能放心继续用当前市场数据”的结论，而不是只抛原始计数。
    if ibkr_count <= 0 and yfinance_count <= 0 and missing_count <= 0:
        return "无数据", "当前还没有可用的数据质量摘要。"
    if research_only_yfinance and yfinance_count > 0 and ibkr_count <= 0:
        return "研究Fallback", "当前配置明确使用 yfinance 作为 research-only 日线，这更像研究设定而不是运行异常。"
    if ibkr_count > 0 and yfinance_count <= 0 and missing_count <= 0 and avg_source_coverage >= 0.95:
        return "IBKR正常", "当前历史数据主要来自 IBKR，覆盖稳定，可继续观察策略与执行质量。"
    if ibkr_count > 0 and yfinance_count <= 0:
        return "IBKR正常", "当前历史数据以 IBKR 为主，但仍建议关注覆盖率与少量缺口。"
    if ibkr_count > 0 and yfinance_count > 0:
        return "混合", "当前历史数据同时依赖 IBKR 与 fallback，建议关注权限、合约映射或时段覆盖。"
    if yfinance_count > 0 and missing_count > 0:
        return "待排查", "当前未配置 research-only，但主要依赖 yfinance 且仍有缺失，优先排查 IBKR 历史权限或合约覆盖。"
    if yfinance_count > 0:
        return "待排查", "当前未配置 research-only，但主要依赖 yfinance fallback，优先排查 IBKR 历史权限、订阅或合约覆盖。"
    return "有缺失", "当前仍有部分标的缺历史数据，调参与自动化应继续保守。"


def _build_market_data_health_overview(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for card in cards:
        market = str(card.get("market", "") or "").strip().upper()
        if not market:
            continue
        data_quality = dict(card.get("data_quality_summary", {}) or {})
        counts = dict(data_quality.get("history_source_counts", {}) or {})
        row = grouped.setdefault(
            market,
            {
                "market": market,
                "watchlists": set(),
                "portfolio_count": 0,
                "score_sum": 0.0,
                "coverage_sum": 0.0,
                "missing_sum": 0.0,
                "ibkr_count": 0,
                "yfinance_count": 0,
                "missing_count": 0,
                "research_only_yfinance": False,
                "warning_lines": set(),
            },
        )
        row["watchlists"].add(str(card.get("watchlist", "") or "").strip())
        row["portfolio_count"] += 1
        row["score_sum"] += float(data_quality.get("avg_data_quality_score", 0.0) or 0.0)
        row["coverage_sum"] += float(data_quality.get("avg_source_coverage", 0.0) or 0.0)
        row["missing_sum"] += float(data_quality.get("avg_missing_ratio", 0.0) or 0.0)
        row["ibkr_count"] += int(counts.get("ibkr", 0) or 0)
        row["yfinance_count"] += int(counts.get("yfinance", 0) or 0)
        row["missing_count"] += int(counts.get("missing", 0) or 0)
        row["research_only_yfinance"] = bool(row["research_only_yfinance"] or card.get("research_only_yfinance", False))
        warning_line = str(card.get("report_data_warning", "") or "").strip()
        if warning_line:
            row["warning_lines"].add(warning_line)

    rows: List[Dict[str, Any]] = []
    for market, raw in grouped.items():
        portfolio_count = max(int(raw.get("portfolio_count", 0) or 0), 1)
        avg_score = float(raw.get("score_sum", 0.0) or 0.0) / portfolio_count
        avg_coverage = float(raw.get("coverage_sum", 0.0) or 0.0) / portfolio_count
        avg_missing = float(raw.get("missing_sum", 0.0) or 0.0) / portfolio_count
        ibkr_count = int(raw.get("ibkr_count", 0) or 0)
        yfinance_count = int(raw.get("yfinance_count", 0) or 0)
        missing_count = int(raw.get("missing_count", 0) or 0)
        status_label, diagnosis = _market_data_health_status(
            ibkr_count=ibkr_count,
            yfinance_count=yfinance_count,
            missing_count=missing_count,
            avg_source_coverage=avg_coverage,
            avg_missing_ratio=avg_missing,
            research_only_yfinance=bool(raw.get("research_only_yfinance", False)),
        )
        warning_summary = " | ".join(sorted(str(x) for x in raw.get("warning_lines", set()) if str(x).strip()))
        rows.append(
            {
                "market": market,
                "portfolio_count": portfolio_count,
                "watchlists": ",".join(sorted(x for x in raw.get("watchlists", set()) if x)),
                "status_label": status_label,
                "research_only_yfinance": bool(raw.get("research_only_yfinance", False)),
                "avg_data_quality_score": avg_score,
                "avg_source_coverage": avg_coverage,
                "avg_missing_ratio": avg_missing,
                "ibkr_count": ibkr_count,
                "yfinance_count": yfinance_count,
                "missing_count": missing_count,
                "diagnosis": diagnosis,
                "warning_summary": warning_summary,
            }
        )

    def _rank(row: Dict[str, Any]) -> tuple[int, str]:
        status = str(row.get("status_label", "") or "")
        order = {
            "待排查": 0,
            "混合": 1,
            "研究Fallback": 2,
            "有缺失": 3,
            "无数据": 4,
            "IBKR正常": 5,
        }
        return order.get(status, 9), str(row.get("market", "") or "")

    rows.sort(key=_rank)
    return rows


def _build_focus_actions(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked = sorted(
        list(cards),
        key=lambda row: (
            0 if bool(row.get("exchange_open", False)) else 1,
            int(row.get("action_priority", 99) or 99),
            int(row.get("priority_order", 999) or 999),
            str(row.get("market", "")),
            str(row.get("watchlist", "")),
        ),
    )
    focus: List[Dict[str, Any]] = []
    for row in ranked:
        action = str(row.get("recommended_action", "") or "").strip()
        if not action:
            continue
        focus.append(
            {
                "market": row.get("market", ""),
                "watchlist": row.get("watchlist", ""),
                "exchange_open": bool(row.get("exchange_open", False)),
                "mode": row.get("mode", ""),
                "action": action,
                "detail": str(row.get("recommended_detail", "") or "-"),
                "priority_order": int(row.get("priority_order", 0) or 0),
            }
        )
        if len(focus) >= 3:
            break
    return focus


def _build_runtime_status(cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    account_ids = sorted({str(card.get("account_id", "") or "").strip() for card in cards if str(card.get("account_id", "") or "").strip()})
    account_modes = sorted({str(card.get("account_mode", "") or "").strip() for card in cards if str(card.get("account_mode", "") or "").strip()})
    scopes = sorted({str(card.get("runtime_scope", "") or "").strip() for card in cards if str(card.get("runtime_scope", "") or "").strip()})

    def _single_or_mixed(values: List[str], default: str = "-") -> str:
        if not values:
            return default
        if len(values) == 1:
            return values[0]
        return "mixed"

    account_id = _single_or_mixed(account_ids)
    account_mode = _single_or_mixed(account_modes)
    runtime_scope = _single_or_mixed(scopes)
    summary_text = (
        f"Current account: {account_id} | "
        f"account_mode: {account_mode} | "
        f"runtime_scope: {runtime_scope}"
    )
    market_mode_summary = [
        {
            "market": str(card.get("market", "") or "").strip(),
            "watchlist": str(card.get("watchlist", "") or "").strip(),
            "mode": str(card.get("mode", "") or "").strip() or "-",
        }
        for card in sorted(
            cards,
            key=lambda row: (
                str(row.get("market", "") or ""),
                str(row.get("watchlist", "") or ""),
            ),
        )
    ]
    market_mode_summary_text = " | ".join(
        f"{row['market']}:{row['watchlist']}={row['mode']}"
        for row in market_mode_summary
        if row["market"] and row["watchlist"]
    ) or "-"
    return {
        "account_id": account_id,
        "account_mode": account_mode,
        "runtime_scope": runtime_scope,
        "summary_text": summary_text,
        "market_mode_summary": market_mode_summary,
        "market_mode_summary_text": market_mode_summary_text,
    }


def _build_ops_overview(
    cards: List[Dict[str, Any]],
    *,
    preflight_summary: Dict[str, Any],
    control_payload: Dict[str, Any],
    execution_mode_summary: Dict[str, Any],
) -> Dict[str, Any]:
    # 运维总览只聚合“现在最值得先处理”的信号：preflight、报告新鲜度、组合健康度和执行模式偏差。
    checks = [dict(row) for row in list(preflight_summary.get("checks", []) or []) if isinstance(row, dict)]
    warning_rows = [row for row in checks if str(row.get("status", "") or "").upper() in {"WARN", "FAIL"}]
    port_warning_rows = [row for row in warning_rows if str(row.get("name", "") or "").startswith("ibkr_port:")]
    stale_rows = [
        card for card in cards
        if not bool(dict(card.get("report_status", {}) or {}).get("fresh", False))
    ]
    degraded_rows = [
        card for card in cards
        if str(dict(card.get("health_summary", {}) or {}).get("status", "OK") or "OK").upper() != "OK"
    ]
    action_state = dict(control_payload.get("actions", {}) or {})
    service_state = dict(control_payload.get("service", {}) or {})
    execution_mismatch_count = int(execution_mode_summary.get("mismatch_count", 0) or 0)
    alert_rows: List[Dict[str, Any]] = []
    for row in warning_rows[:8]:
        alert_rows.append(
            {
                "category": "PREFLIGHT",
                "name": str(row.get("name", "") or ""),
                "status": str(row.get("status", "") or ""),
                "detail": str(row.get("detail", "") or ""),
            }
        )
    for card in stale_rows[:4]:
        report_status = dict(card.get("report_status", {}) or {})
        alert_rows.append(
            {
                "category": "REPORT",
                "name": f"{card.get('market', '')}:{card.get('watchlist', '')}",
                "status": "WARN",
                "detail": str(report_status.get("fresh_reason", "") or "report_not_fresh"),
            }
        )
    for card in degraded_rows[:4]:
        health = dict(card.get("health_summary", {}) or {})
        alert_rows.append(
            {
                "category": "HEALTH",
                "name": f"{card.get('market', '')}:{card.get('watchlist', '')}",
                "status": str(health.get("status", "WARN") or "WARN"),
                "detail": str(health.get("status_detail", "") or "-"),
            }
        )
    preflight_banner_level = ""
    preflight_banner_title = ""
    preflight_banner_reason = ""
    preflight_banner_action = ""
    preflight_banner_rows: List[Dict[str, Any]] = []
    if warning_rows:
        preflight_banner_rows = [dict(row) for row in warning_rows[:3]]
        fail_count = int(preflight_summary.get("fail_count", 0) or 0)
        warn_count = int(preflight_summary.get("warn_count", 0) or 0)
        # 顶部提示条只强调“当前最影响自动执行”的 preflight 问题，避免用户先去读完整张运维表格。
        if fail_count > 0:
            preflight_banner_level = "FAIL"
            preflight_banner_title = "Preflight 存在失败项，当前不建议自动执行"
            preflight_banner_action = "先处理 FAIL 项，再恢复 AUTO。"
        elif port_warning_rows:
            preflight_banner_level = "WARN"
            preflight_banner_title = "IBKR 连接未就绪，当前不建议自动执行"
            preflight_banner_action = "先启动 TWS/Gateway，并确认目标端口处于监听状态。"
        else:
            preflight_banner_level = "WARN"
            preflight_banner_title = "Preflight 存在待确认项"
            preflight_banner_action = "先复核 warning，再决定是否继续自动执行。"
        reason_bits = [
            f"{str(row.get('name', '') or '').strip()}: {str(row.get('detail', '') or '').strip()}"
            for row in preflight_banner_rows
            if str(row.get("name", "") or "").strip()
        ]
        preflight_banner_reason = " | ".join(bit for bit in reason_bits if bit) or f"warn={warn_count} fail={fail_count}"
    summary_text = (
        f"preflight fail={int(preflight_summary.get('fail_count', 0) or 0)} warn={int(preflight_summary.get('warn_count', 0) or 0)} | "
        f"stale_reports={len(stale_rows)} | "
        f"degraded_health={len(degraded_rows)} | "
        f"mode_mismatch={execution_mismatch_count} | "
        f"service={str(service_state.get('status', 'disabled') or 'disabled')}"
    )
    return {
        "preflight_generated_at": str(preflight_summary.get("generated_at", "") or ""),
        "preflight_pass_count": int(preflight_summary.get("pass_count", 0) or 0),
        "preflight_warn_count": int(preflight_summary.get("warn_count", 0) or 0),
        "preflight_fail_count": int(preflight_summary.get("fail_count", 0) or 0),
        "ibkr_port_warning_count": int(len(port_warning_rows)),
        "stale_report_count": int(len(stale_rows)),
        "degraded_health_count": int(len(degraded_rows)),
        "execution_mode_mismatch_count": execution_mismatch_count,
        "control_service_status": str(service_state.get("status", "disabled") or "disabled"),
        "run_once_in_progress": bool(action_state.get("run_once_in_progress", False)),
        "preflight_in_progress": bool(action_state.get("preflight_in_progress", False)),
        "weekly_review_in_progress": bool(action_state.get("weekly_review_in_progress", False)),
        "summary_text": summary_text,
        "preflight_banner_level": preflight_banner_level,
        "preflight_banner_title": preflight_banner_title,
        "preflight_banner_reason": preflight_banner_reason,
        "preflight_banner_action": preflight_banner_action,
        "preflight_banner_rows": preflight_banner_rows,
        "alert_rows": alert_rows,
    }


def _build_execution_weekly(summary_csv: Path, *, default_week: str = "", default_week_start: str = "") -> Dict[str, Any]:
    rows = _read_all_csv_rows(summary_csv)
    if not rows:
        return {}
    return _normalize_execution_weekly_row(dict(rows[-1]), default_week=default_week, default_week_start=default_week_start)


def _portfolio_watchlist_slug(portfolio_id: str) -> str:
    text = str(portfolio_id or "").strip()
    if ":" in text:
        return text.split(":", 1)[1]
    return text


def _build_execution_weekly_groups(summary_csv: Path, *, default_week: str = "", default_week_start: str = "") -> List[Dict[str, Any]]:
    rows = _read_all_csv_rows(summary_csv)
    if not rows:
        return []
    latest_week = str(rows[-1].get("week", "") or default_week)
    scoped_rows: List[Dict[str, Any]] = []
    for raw in rows:
        row = _normalize_execution_weekly_row(dict(raw), default_week=default_week, default_week_start=default_week_start)
        if not row:
            continue
        if str(row.get("week", "") or default_week) != latest_week:
            continue
        scoped_rows.append(
            {
                **row,
                "week": latest_week,
                "watchlist": _portfolio_watchlist_slug(str(row.get("portfolio_id", "") or "")),
            }
        )
    scoped_rows.sort(
        key=lambda row: (
            str(row.get("market", "")),
            str(row.get("watchlist", "")),
            str(row.get("portfolio_id", "")),
        )
    )
    return scoped_rows


def _merge_execution_weekly_groups(
    cards: List[Dict[str, Any]],
    grouped_rows: List[Dict[str, Any]],
    *,
    week_label: str = "",
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    display_rows: List[Dict[str, Any]] = []
    orphan_rows: List[Dict[str, Any]] = []
    grouped_map: Dict[tuple[str, str], Dict[str, Any]] = {
        (str(row.get("market", "") or ""), str(row.get("portfolio_id", "") or "")): dict(row)
        for row in grouped_rows
    }
    seen: set[tuple[str, str]] = set()
    for card in cards:
        key = (str(card.get("market", "") or ""), str(card.get("portfolio_id", "") or ""))
        row = dict(grouped_map.get(key, {}))
        if not row:
            row = {
                "week": week_label,
                "week_start": "",
                "market": key[0],
                "portfolio_id": key[1],
                "watchlist": str(card.get("watchlist", "") or ""),
                "execution_run_rows": 0,
                "submitted_runs": 0,
                "planned_order_rows": 0,
                "submitted_order_rows": 0,
                "filled_order_rows": 0,
                "filled_with_audit_rows": 0,
                "blocked_opportunity_rows": 0,
                "error_order_rows": 0,
                "fill_rows": 0,
                "commission_total": 0.0,
                "realized_net_pnl": 0.0,
                "fill_rate_status": None,
                "fill_rate_audit": None,
            }
        row["market"] = key[0]
        row["portfolio_id"] = key[1]
        row["watchlist"] = str(card.get("watchlist", "") or row.get("watchlist", ""))
        display_rows.append(row)
        seen.add(key)
    for row in grouped_rows:
        key = (str(row.get("market", "") or ""), str(row.get("portfolio_id", "") or ""))
        if key in seen:
            continue
        orphan_rows.append(dict(row))
    orphan_rows.sort(
        key=lambda row: (
            str(row.get("market", "")),
            str(row.get("watchlist", "")),
            str(row.get("portfolio_id", "")),
        )
    )
    return display_rows, orphan_rows


def _render_table(headers: List[str], rows: List[List[str]]) -> str:
    if not rows:
        return '<div class="empty">无数据</div>'
    thead = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    tbody = []
    for row in rows:
        tbody.append("<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row) + "</tr>")
    return f"<table><thead><tr>{thead}</tr></thead><tbody>{''.join(tbody)}</tbody></table>"


def _render_card(card: Dict[str, Any]) -> str:
    paper = dict(card.get("paper_summary", {}) or {})
    execution = dict(card.get("execution_summary", {}) or {})
    guard = dict(card.get("guard_summary", {}) or {})
    opp = dict(card.get("opportunity_summary", {}) or {})
    health = dict(card.get("health_summary", {}) or {})
    report_status = dict(card.get("report_status", {}) or {})
    weekly = dict(card.get("execution_weekly_row", {}) or {})
    control = dict(card.get("dashboard_control", {}) or {})
    control_portfolio = dict(control.get("portfolio", {}) or {})

    holdings_rows = [
        [
            row.get("symbol", ""),
            row.get("qty", ""),
            _fmt_money(row.get("market_value")),
            _fmt_pct(row.get("weight")),
            row.get("status", ""),
        ]
        for row in list(card.get("holdings", []) or [])[:8]
    ]
    broker_rows = [
        [
            row.get("symbol", ""),
            row.get("qty", ""),
            _fmt_money(row.get("market_value")),
            _fmt_pct(row.get("weight")),
            row.get("source", "after"),
        ]
        for row in list(card.get("broker_holdings", []) or [])[:8]
    ]
    plan_rows = [
        [
            row.get("symbol", ""),
            row.get("action", ""),
            row.get("entry_style", ""),
            row.get("regime_state", ""),
            row.get("notes", "")[:80],
        ]
        for row in list(card.get("plan_rows", []) or [])[:6]
    ]
    candidate_rows = [
        [
            row.get("symbol", ""),
            row.get("action", ""),
            f"{float(row.get('score', 0.0)):.3f}" if str(row.get("score", "")).strip() else "-",
            f"{float(row.get('score_before_cost', row.get('score', 0.0)) or 0.0):.3f}" if str(row.get("score_before_cost", row.get("score", ""))).strip() else "-",
            f"{float(row.get('expected_cost_bps', 0.0) or 0.0):.1f}" if str(row.get("expected_cost_bps", "")).strip() else "-",
            f"{float(row.get('shadow_ml_score', 0.0) or 0.0):.2f}" if str(row.get("shadow_ml_score", "")).strip() else "-",
            f"{float(row.get('data_quality_score', 0.0) or 0.0):.2f}" if str(row.get("data_quality_score", "")).strip() else "-",
            f"{float(row.get('source_coverage', 0.0) or 0.0):.2f}" if str(row.get("source_coverage", "")).strip() else "-",
            f"{float(row.get('missing_ratio', 0.0) or 0.0):.2f}" if str(row.get("missing_ratio", "")).strip() else "-",
            row.get("history_source", "") or "-",
            row.get("asset_class", "") or row.get("asset_theme", "") or row.get("sector", "") or row.get("industry", "") or "-",
        ]
        for row in list(card.get("candidates", []) or [])[:10]
    ]
    exec_rows = [
        [
            row.get("symbol", ""),
            row.get("action", ""),
            row.get("status", ""),
            row.get("execution_style", "") or "-",
            f"{float(row.get('expected_cost_bps', 0.0) or 0.0):.1f}" if str(row.get("expected_cost_bps", "")).strip() else "-",
            row.get("reason", "")[:70],
        ]
        for row in list(card.get("execution_plan", []) or [])[:6]
    ]
    paper_trade_rows = [
        [
            row.get("symbol", ""),
            row.get("action", ""),
            _fmt_qty(row.get("qty")),
            _fmt_money(row.get("price")),
            _fmt_money(row.get("trade_value")),
            row.get("reason", "")[:70],
        ]
        for row in list(card.get("paper_trades", []) or [])[:8]
    ]
    opp_rows = [
        [
            row.get("symbol", ""),
            row.get("status", ""),
            row.get("action", ""),
            row.get("reason", "")[:70],
        ]
        for row in list(card.get("opportunity_scan", []) or [])[:6]
    ]

    report_day = report_status.get("report_day", "")
    report_slot = report_status.get("report_slot", "")
    report_fresh = "fresh" if report_status.get("fresh") else str(report_status.get("fresh_reason", "") or "-")
    report_schedule = ", ".join(f"{row.get('name')}@{row.get('time')}" for row in list(report_status.get("report_schedule", []) or []))
    action_label = str(card.get("recommended_action", "") or "")
    action_detail = str(card.get("recommended_detail", "") or "")
    mode = str(card.get("mode", "") or "")
    mode_detail = str(card.get("mode_detail", "") or "")
    candidate_summary = _top_candidates_summary(list(card.get("candidates", []) or []), limit=10)
    data_quality = dict(card.get("data_quality_summary", {}) or {})
    cost_summary = dict(card.get("cost_summary", {}) or {})
    shadow_payload = dict(card.get("shadow_model_summary", {}) or {})
    shadow_summary = dict(shadow_payload.get("summary", {}) or {})
    risk_summary = execution if execution else paper
    data_quality_label = (
        f"avg={float(data_quality.get('avg_data_quality_score', 0.0) or 0.0):.2f} / "
        f"low={int(data_quality.get('low_quality_count', 0) or 0)} / "
        f"src_cov={float(data_quality.get('avg_source_coverage', 0.0) or 0.0):.2f} / "
        f"miss={float(data_quality.get('avg_missing_ratio', 0.0) or 0.0):.2f}"
    )
    shadow_label = (
        f"enabled={'Y' if bool(shadow_summary.get('enabled', False)) else 'N'} / "
        f"ver={str(shadow_summary.get('model_version', '') or '-')} / "
        f"feat={int(shadow_summary.get('feature_count', 0) or 0)} / "
        f"samples={int(shadow_summary.get('training_samples', 0) or 0)} / "
        f"horizon={int(shadow_summary.get('horizon_days', 0) or 0)}d / "
        f"avg_ml={float(shadow_summary.get('avg_shadow_ml_score', 0.0) or 0.0):.2f}"
    )
    cost_label = (
        f"avg={float(cost_summary.get('avg_expected_cost_bps', 0.0) or 0.0):.1f}bps / "
        f"high={int(cost_summary.get('high_cost_count', 0) or 0)} / "
        f"low_liq={int(cost_summary.get('low_liquidity_count', 0) or 0)}"
    )
    review_label = (
        f"shadow={int(execution.get('blocked_shadow_review_order_count', 0) or 0)} / "
        f"size={int(execution.get('blocked_size_review_order_count', 0) or 0)} / "
        f"total={int(execution.get('blocked_manual_review_order_count', 0) or 0)}"
    )
    risk_label = (
        f"net={_fmt_pct(risk_summary.get('risk_dynamic_net_exposure'))} / "
        f"gross={_fmt_pct(risk_summary.get('risk_dynamic_gross_exposure'))} / "
        f"corr={float(risk_summary.get('risk_avg_pair_correlation', 0.0) or 0.0):.2f} / "
        f"stress={str(risk_summary.get('risk_stress_worst_scenario_label', '') or '-')}"
        f" {_fmt_pct(risk_summary.get('risk_stress_worst_loss'))}"
        f"{' / var95=' + _fmt_pct(risk_summary.get('risk_returns_based_var_95_1d')) if bool(risk_summary.get('risk_returns_based_enabled', False)) else ''}"
    )
    risk_notes = " | ".join(str(x).strip() for x in list(risk_summary.get("risk_notes", []) or []) if str(x).strip())
    market_summary_lines = list(card.get("market_summary_lines", []) or [])
    action_distribution = str(card.get("action_distribution", "") or "-")
    sector_theme_distribution = str(card.get("sector_theme_distribution", "") or "-")
    open_flag = bool(card.get("exchange_open", False))
    actionable = bool(card.get("actionable", False))
    weekly_submitted = int(weekly.get("submitted_order_rows", 0) or 0)
    weekly_filled_status = int(weekly.get("filled_order_rows", 0) or 0)
    weekly_filled_audit = int(weekly.get("filled_with_audit_rows", 0) or 0)
    weekly_blocked = int(weekly.get("blocked_opportunity_rows", 0) or 0)
    weekly_error = int(weekly.get("error_order_rows", 0) or 0)
    analysis_state_rows = [
        [
            row.get("symbol", ""),
            row.get("status_label", ""),
            row.get("action", "") or "-",
            row.get("entry_status", "") or "-",
            _fmt_qty(row.get("held_qty")),
            f"{float(row.get('score', 0.0) or 0.0):.3f}" if str(row.get("score", "")).strip() else "-",
        ]
        for row in list(card.get("analysis_states", []) or [])[:10]
    ]
    analysis_event_rows = [
        [
            str(row.get("ts", "") or "")[:19],
            row.get("symbol", ""),
            row.get("event_kind", ""),
            f"{row.get('from_status_label', '-')}" if row.get("from_status_label") else "-",
            row.get("to_status_label", ""),
        ]
        for row in list(card.get("analysis_events", []) or [])[:10]
    ]
    shadow_review_recent_rows = [
        [
            str(row.get("ts", "") or "")[:19],
            row.get("symbol", ""),
            row.get("action", ""),
            _fmt_money(row.get("order_value")),
            row.get("shadow_review_status", "") or "REVIEW_REQUIRED",
            str(row.get("shadow_review_reason", "") or "")[:90],
        ]
        for row in list(card.get("shadow_review_recent_rows", []) or [])[:8]
    ]
    shadow_review_repeat_rows = [
        [
            row.get("symbol", ""),
            str(int(row.get("repeat_count", 0) or 0)),
            row.get("latest_action", ""),
            _fmt_money(row.get("latest_order_value")),
            str(row.get("latest_ts", "") or "")[:19],
            str(row.get("latest_reason", "") or "")[:90],
        ]
        for row in list(card.get("shadow_review_repeat_rows", []) or [])[:8]
    ]
    analysis_active_count = int(len([row for row in list(card.get("analysis_states", []) or []) if str(row.get("status") or "").upper() != "REMOVED_FROM_WATCH"]))
    analysis_event_count = int(len(list(card.get("analysis_events", []) or [])))
    weekly_shadow_review = dict(card.get("weekly_shadow_review", {}) or {})
    weekly_attribution = dict(card.get("weekly_attribution", {}) or {})
    weekly_risk_review = dict(card.get("weekly_risk_review", {}) or {})
    weekly_feedback_calibration = dict(card.get("weekly_feedback_calibration", {}) or {})
    weekly_feedback_automation_map = dict(card.get("weekly_feedback_automation_map", {}) or {})
    weekly_labeling_skips = list(card.get("weekly_labeling_skips", []) or [])
    risk_history_rows_raw = list(card.get("risk_history_rows", []) or [])
    risk_history_source_label = str(card.get("risk_history_source_label", "") or "近期风险轨迹")
    risk_history_fallback = bool(card.get("risk_history_fallback", False))
    paper_risk_feedback = dict(card.get("paper_risk_feedback", {}) or {})
    execution_feedback = dict(card.get("execution_feedback", {}) or {})
    account_mode = str(card.get("account_mode", "") or "").strip().lower()
    dashboard_view = str(card.get("dashboard_view", "trade") or "trade").strip().lower()
    is_dry_run_view = dashboard_view == "dry-run"
    paper_stats_label = "Paper Ledger"
    broker_stats_label = "IBKR Paper" if account_mode == "paper" else "Broker"
    holdings_title = "当前持仓 (本地模拟账本)"
    broker_title = "当前持仓 (IBKR Paper 快照)" if account_mode == "paper" else "Broker 快照"
    portfolio_id = str(card.get("portfolio_id", "") or "")
    control_enabled = bool(control.get("enabled", False))
    control_status = str(control.get("status", "") or "")
    control_url = str(control.get("url", "") or "")
    execution_mode_recommendation = dict(card.get("execution_mode_recommendation", {}) or {})
    paper_trade_value = sum(_safe_float(row.get("trade_value"), 0.0) for row in list(card.get("paper_trades", []) or []))
    risk_feedback_label = (
        f"{str(paper_risk_feedback.get('apply_mode_label', '') or '沿用基础配置')} / "
        f"action={str(paper_risk_feedback.get('risk_feedback_action', '') or '-')} / "
        f"base={float(paper_risk_feedback.get('feedback_base_confidence', 1.0) or 1.0):.2f} / "
        f"calib={float(paper_risk_feedback.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
        f"({str(paper_risk_feedback.get('feedback_calibration_label', '') or '-')}) / "
        f"conf={float(paper_risk_feedback.get('feedback_confidence', 1.0) or 1.0):.2f}"
        f"({str(paper_risk_feedback.get('feedback_confidence_label', '') or '-')}) / "
        f"net={_fmt_budget_change(paper_risk_feedback.get('base_max_net_exposure'), paper_risk_feedback.get('effective_max_net_exposure'))} / "
        f"gross={_fmt_budget_change(paper_risk_feedback.get('base_max_gross_exposure'), paper_risk_feedback.get('effective_max_gross_exposure'))} / "
        f"corr={_fmt_budget_change(paper_risk_feedback.get('base_correlation_soft_limit'), paper_risk_feedback.get('effective_correlation_soft_limit'), pct=False)}"
    )
    risk_feedback_rows = [[
        str(paper_risk_feedback.get("apply_mode_label", "") or "沿用基础配置"),
        str(paper_risk_feedback.get("risk_feedback_action", "") or "-"),
        f"{float(paper_risk_feedback.get('feedback_base_confidence', 1.0) or 1.0):.2f}/{str(paper_risk_feedback.get('feedback_base_confidence_label', '') or '-')}",
        f"{float(paper_risk_feedback.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{str(paper_risk_feedback.get('feedback_calibration_label', '') or '-')}",
        f"{float(paper_risk_feedback.get('feedback_confidence', 1.0) or 1.0):.2f}/{str(paper_risk_feedback.get('feedback_confidence_label', '') or '-')}",
        _fmt_budget_change(paper_risk_feedback.get("base_max_single_weight"), paper_risk_feedback.get("effective_max_single_weight")),
        _fmt_budget_change(paper_risk_feedback.get("base_max_sector_weight"), paper_risk_feedback.get("effective_max_sector_weight")),
        _fmt_budget_change(paper_risk_feedback.get("base_max_net_exposure"), paper_risk_feedback.get("effective_max_net_exposure")),
        _fmt_budget_change(paper_risk_feedback.get("base_max_gross_exposure"), paper_risk_feedback.get("effective_max_gross_exposure")),
        _fmt_budget_change(paper_risk_feedback.get("base_max_short_exposure"), paper_risk_feedback.get("effective_max_short_exposure")),
        _fmt_budget_change(
            paper_risk_feedback.get("base_correlation_soft_limit"),
            paper_risk_feedback.get("effective_correlation_soft_limit"),
            pct=False,
        ),
        str(paper_risk_feedback.get("effective_source_label", "") or "-"),
        str(paper_risk_feedback.get("feedback_reason", "") or "本周没有新的风险预算反馈，当前沿用基础配置。"),
    ]]
    risk_feedback_meta = (
        f"base={str(paper_risk_feedback.get('base_config_path', '-') or '-')} | "
        f"effective={str(paper_risk_feedback.get('effective_config_path', '-') or '-')} | "
        f"overlay_exists={'Y' if bool(paper_risk_feedback.get('overlay_exists', False)) else 'N'}"
    )
    execution_feedback_label = (
        f"{str(execution_feedback.get('apply_mode_label', '') or '沿用基础配置')} / "
        f"action={str(execution_feedback.get('execution_feedback_action', '') or '-')} / "
        f"base={float(execution_feedback.get('feedback_base_confidence', 1.0) or 1.0):.2f} / "
        f"calib={float(execution_feedback.get('feedback_calibration_score', 0.5) or 0.5):.2f} "
        f"({str(execution_feedback.get('feedback_calibration_label', '') or '-')}) / "
        f"conf={float(execution_feedback.get('feedback_confidence', 1.0) or 1.0):.2f}"
        f"({str(execution_feedback.get('feedback_confidence_label', '') or '-')}) / "
        f"adv={_fmt_budget_change(execution_feedback.get('base_adv_max_participation_pct'), execution_feedback.get('effective_adv_max_participation_pct'))} / "
        f"split={_fmt_budget_change(execution_feedback.get('base_adv_split_trigger_pct'), execution_feedback.get('effective_adv_split_trigger_pct'))} / "
        f"slices={float(execution_feedback.get('base_max_slices_per_symbol', 0.0) or 0.0):.0f}->{float(execution_feedback.get('effective_max_slices_per_symbol', 0.0) or 0.0):.0f}"
        f"{' / dominant=' + str(execution_feedback.get('dominant_execution_session_label', '') or '') if str(execution_feedback.get('dominant_execution_session_label', '') or '').strip() else ''}"
        f"{' / hotspot=' + str(execution_feedback.get('dominant_execution_hotspot_symbol', '') or '') if str(execution_feedback.get('dominant_execution_hotspot_symbol', '') or '').strip() else ''}"
        f"{' / penalties=' + str(execution_feedback.get('execution_penalty_symbols', '') or '') if str(execution_feedback.get('execution_penalty_symbols', '') or '').strip() else ''}"
    )
    execution_feedback_rows = [[
        str(execution_feedback.get("apply_mode_label", "") or "沿用基础配置"),
        str(execution_feedback.get("execution_feedback_action", "") or "-"),
        f"{float(execution_feedback.get('feedback_base_confidence', 1.0) or 1.0):.2f}/{str(execution_feedback.get('feedback_base_confidence_label', '') or '-')}",
        f"{float(execution_feedback.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{str(execution_feedback.get('feedback_calibration_label', '') or '-')}",
        f"{float(execution_feedback.get('feedback_confidence', 1.0) or 1.0):.2f}/{str(execution_feedback.get('feedback_confidence_label', '') or '-')}",
        _fmt_budget_change(execution_feedback.get("base_adv_max_participation_pct"), execution_feedback.get("effective_adv_max_participation_pct")),
        _fmt_budget_change(execution_feedback.get("base_adv_split_trigger_pct"), execution_feedback.get("effective_adv_split_trigger_pct")),
        f"{float(execution_feedback.get('base_max_slices_per_symbol', 0.0) or 0.0):.0f}->{float(execution_feedback.get('effective_max_slices_per_symbol', 0.0) or 0.0):.0f}",
        _fmt_budget_change(execution_feedback.get("base_open_session_participation_scale"), execution_feedback.get("effective_open_session_participation_scale"), pct=False),
        _fmt_budget_change(execution_feedback.get("base_midday_session_participation_scale"), execution_feedback.get("effective_midday_session_participation_scale"), pct=False),
        _fmt_budget_change(execution_feedback.get("base_close_session_participation_scale"), execution_feedback.get("effective_close_session_participation_scale"), pct=False),
        str(execution_feedback.get("effective_source_label", "") or "-"),
        str(
            execution_feedback.get("apply_status_reason", "")
            or execution_feedback.get("feedback_reason", "")
            or "本周没有新的执行参数反馈，当前沿用基础配置。"
        ),
    ]]
    execution_feedback_meta = (
        f"base={str(execution_feedback.get('base_config_path', '-') or '-')} | "
        f"effective={str(execution_feedback.get('effective_config_path', '-') or '-')} | "
        f"overlay_exists={'Y' if bool(execution_feedback.get('overlay_exists', False)) else 'N'}"
    )
    raw_session_rows = list(execution_feedback.get("session_feedback_rows", []) or [])
    if not raw_session_rows:
        raw_session_rows = list(card.get("weekly_execution_sessions", []) or [])
    raw_hotspot_rows = list(execution_feedback.get("hotspot_rows", []) or [])
    if not raw_hotspot_rows:
        raw_hotspot_rows = list(card.get("weekly_execution_hotspots", []) or [])
    raw_execution_penalty_rows = list(execution_feedback.get("execution_penalty_rows", []) or [])
    feedback_automation_rows = [
        [
            str(row.get("feedback_kind_label", "") or "-"),
            str(row.get("feedback_action", "") or "-"),
            str(row.get("calibration_apply_mode_label", "") or "-"),
            str(row.get("calibration_basis_label", "") or "-"),
            str(row.get("market_data_gate_label", "") or "-"),
            f"{float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f}/{str(row.get('feedback_base_confidence_label', '') or '-')}",
            f"{float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{str(row.get('feedback_calibration_label', '') or '-')}",
            f"{float(row.get('feedback_confidence', 0.0) or 0.0):.2f}/{str(row.get('feedback_confidence_label', '') or '-')}",
            str(int(_safe_float(row.get("feedback_sample_count"), 0.0))),
            str(int(_safe_float(row.get("feedback_calibration_sample_count"), 0.0))),
            (
                f"{float(row.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                f"{str(row.get('outcome_maturity_label', '') or 'UNKNOWN')}"
            ),
            (
                f"{int(_safe_float(row.get('outcome_pending_sample_count'), 0.0))} | "
                f"{str(row.get('outcome_ready_estimate_end_ts', '') or '-')[:10]}"
            ),
            str(row.get("automation_reason", "") or "-"),
        ]
        for row in [
            dict(weekly_feedback_automation_map.get("shadow", {}) or {}),
            dict(weekly_feedback_automation_map.get("risk", {}) or {}),
            dict(weekly_feedback_automation_map.get("execution", {}) or {}),
        ]
        if row
    ]
    feedback_automation_history_rows = [
        [
            str(row.get("week_label", "") or "-"),
            str(row.get("feedback_kind_label", "") or row.get("feedback_kind", "") or "-"),
            str(row.get("state_label", "") or _feedback_history_state_label(row) or "-"),
            str(row.get("calibration_apply_mode_label", "") or "-"),
            str(row.get("market_data_gate_label", "") or "-"),
            (
                f"{float(row.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                f"{str(row.get('outcome_maturity_label', '') or 'UNKNOWN')}"
            ),
            str(int(_safe_float(row.get("outcome_pending_sample_count"), 0.0))),
            str(row.get("outcome_ready_estimate_end_ts", "") or "-")[:10],
            str(row.get("automation_reason", "") or "-"),
        ]
        for row in list(card.get("feedback_automation_history_rows", []) or [])[:8]
    ]
    feedback_automation_label = " / ".join(
        f"{str(row.get('feedback_kind_label', '') or row.get('feedback_kind', ''))}:{str(row.get('calibration_apply_mode_label', '') or '-')}"
        for row in [
            dict(weekly_feedback_automation_map.get("shadow", {}) or {}),
            dict(weekly_feedback_automation_map.get("risk", {}) or {}),
            dict(weekly_feedback_automation_map.get("execution", {}) or {}),
        ]
        if row
    ) or "当前还没有校准自动化结论"
    feedback_calibration_rows = [[
        str(weekly_feedback_calibration.get("selection_scope_label", "") or "-"),
        str(weekly_feedback_calibration.get("selected_horizon_days", "") or "-"),
        str(int(_safe_float(weekly_feedback_calibration.get("outcome_sample_count"), 0.0))),
        _fmt_pct(weekly_feedback_calibration.get("outcome_positive_rate")),
        _fmt_pct(weekly_feedback_calibration.get("outcome_broken_rate")),
        _fmt_signed_pct(weekly_feedback_calibration.get("avg_future_return")),
        _fmt_signed_pct(weekly_feedback_calibration.get("avg_max_drawdown")),
        f"{float(weekly_feedback_calibration.get('score_alignment_score', 0.0) or 0.0):.2f}",
        f"{float(weekly_feedback_calibration.get('signal_quality_score', 0.0) or 0.0):.2f}",
        f"{float(weekly_feedback_calibration.get('shadow_threshold_relax_support', 0.0) or 0.0):.2f}",
        f"{float(weekly_feedback_calibration.get('risk_tighten_support', 0.0) or 0.0):.2f}",
        f"{float(weekly_feedback_calibration.get('execution_support', 0.0) or 0.0):.2f}",
        str(weekly_feedback_calibration.get("calibration_reason", "") or "-"),
    ]] if weekly_feedback_calibration else []
    labeling_skip_rows = [
        [
            str(row.get("horizon_days", "") or "-"),
            str(row.get("skip_reason_label", "") or str(row.get("skip_reason", "") or "-")),
            str(int(_safe_float(row.get("skip_count"), 0.0))),
            str(int(_safe_float(row.get("symbol_count"), 0.0))),
            str(row.get("sample_symbols", "") or "-"),
            f"{str(row.get('oldest_snapshot_ts', '') or '-')[:19]} -> {str(row.get('latest_snapshot_ts', '') or '-')[:19]}",
            (
                f"{int(_safe_float(row.get('min_remaining_forward_bars'), 0.0))}"
                f"-{int(_safe_float(row.get('max_remaining_forward_bars'), 0.0))}"
                if int(_safe_float(row.get("max_remaining_forward_bars"), 0.0)) > 0
                else "-"
            ),
            (
                f"{str(row.get('estimated_ready_start_ts', '') or '-')[:10]} -> "
                f"{str(row.get('estimated_ready_end_ts', '') or '-')[:10]}"
                if str(row.get("estimated_ready_end_ts", "") or "")
                else "-"
            ),
        ]
        for row in weekly_labeling_skips[:8]
    ]
    execution_session_review_rows = [
        [
            str(row.get("session_label", "") or row.get("session_bucket", "") or "-"),
            str(row.get("session_action", "") or "-"),
            _fmt_money(row.get("planned_execution_cost_total")),
            _fmt_money(row.get("execution_cost_total")),
            _fmt_money(row.get("execution_cost_gap")),
            f"{float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('scale_delta', 0.0) or 0.0):+.3f}" if "scale_delta" in row else "-",
            str(row.get("execution_style_breakdown", "") or "-"),
            str(row.get("reason", "") or "-"),
        ]
        for row in raw_session_rows
    ]
    execution_hotspot_rows = [
        [
            str(row.get("symbol", "") or "-"),
            str(row.get("session_label", "") or row.get("session_bucket", "") or "-"),
            str(row.get("hotspot_action", "") or "-"),
            _fmt_money(row.get("planned_execution_cost_total")),
            _fmt_money(row.get("execution_cost_total")),
            _fmt_money(row.get("execution_cost_gap")),
            f"{float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('pressure_score', 0.0) or 0.0):.2f}",
            str(row.get("execution_style_breakdown", "") or "-"),
            str(row.get("reason", row.get("hotspot_reason", "")) or "-"),
        ]
        for row in raw_hotspot_rows[:8]
    ]
    execution_penalty_rows = [
        [
            str(row.get("symbol", "") or "-"),
            str(row.get("session_labels", "") or "-"),
            str(int(_safe_float(row.get("session_count"), 0.0))),
            f"{float(row.get('expected_cost_bps_add', 0.0) or 0.0):.1f}",
            f"{float(row.get('slippage_proxy_bps_add', 0.0) or 0.0):.1f}",
            f"{float(row.get('execution_penalty', 0.0) or 0.0):.3f}",
            str(row.get("reason", "") or "-"),
        ]
        for row in raw_execution_penalty_rows[:8]
    ]
    risk_history_rows = [
        [
            str(row.get("ts", "") or "")[:19] or "-",
            str(row.get("source_label", "") or "-"),
            f"{float(row.get('dynamic_scale', 1.0) or 1.0):.2f}",
            _fmt_pct(row.get("dynamic_net_exposure")),
            _fmt_pct(row.get("dynamic_gross_exposure")),
            f"{float(row.get('avg_pair_correlation', 0.0) or 0.0):.2f}",
            str(row.get("stress_worst_scenario_label", "") or "-"),
            _fmt_pct(row.get("stress_worst_loss")),
            str(row.get("dominant_risk_driver", "") or "-"),
            str(row.get("notes_preview", "") or row.get("risk_diagnosis", "") or "-"),
        ]
        for row in risk_history_rows_raw[:8]
    ]
    risk_history_meta = (
        "trade 视图当前没有 execution 风险历史，已回退显示 dry-run 风险历史。"
        if risk_history_fallback
        else "风险轨迹直接来自运行数据库，可用于解释最近几次缩仓/放仓。"
    )
    risk_trend_summary = dict(card.get("risk_trend_summary", {}) or {})
    execution_mode_recommendation = dict(card.get("execution_mode_recommendation", {}) or {})
    risk_trend_rows = [[
        str(risk_trend_summary.get("alert_level", "") or "-"),
        str(risk_trend_summary.get("trend_label", "") or "-"),
        str(risk_trend_summary.get("latest_ts", "") or "")[:19] or "-",
        str(risk_trend_summary.get("previous_ts", "") or "")[:19] or "-",
        f"{float(risk_trend_summary.get('latest_dynamic_scale', 1.0) or 1.0):.2f}",
        f"{float(risk_trend_summary.get('scale_delta', 0.0) or 0.0):+.2f}",
        _fmt_pct(risk_trend_summary.get("latest_dynamic_net_exposure")),
        _fmt_signed_pct(risk_trend_summary.get("net_delta")),
        _fmt_pct(risk_trend_summary.get("latest_dynamic_gross_exposure")),
        _fmt_signed_pct(risk_trend_summary.get("gross_delta")),
        f"{float(risk_trend_summary.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
        f"{float(risk_trend_summary.get('corr_delta', 0.0) or 0.0):+.2f}",
        str(risk_trend_summary.get("latest_stress_worst_scenario_label", "") or "-"),
        _fmt_pct(risk_trend_summary.get("latest_stress_worst_loss")),
        _fmt_signed_pct(risk_trend_summary.get("stress_delta")),
        str(risk_trend_summary.get("diagnosis", "") or "-"),
    ]] if risk_trend_summary else []
    execution_mode_recommendation_rows = [[
        str(execution_mode_recommendation.get("current_mode_label", "") or "-"),
        str(execution_mode_recommendation.get("recommended_mode_label", "") or "-"),
        "YES" if bool(execution_mode_recommendation.get("differs_from_current", False)) else "NO",
        str(int(execution_mode_recommendation.get("consecutive_alert_count", 0) or 0)),
        str(int(execution_mode_recommendation.get("consecutive_watch_count", 0) or 0)),
        str(execution_mode_recommendation.get("reason", "") or "-"),
    ]] if execution_mode_recommendation else []
    weekly_cost_compare_label = (
        f"plan={_fmt_money(weekly_attribution.get('planned_execution_cost_total'))} / "
        f"actual={_fmt_money(weekly_attribution.get('execution_cost_total'))} / "
        f"gap={_fmt_money(weekly_attribution.get('execution_cost_gap'))}"
    )
    dry_run_state_rows = [
        [
            str(paper.get("ts", "") or "")[:19] or "-",
            str(paper.get("run_id", "") or "-"),
            "YES" if bool(paper.get("executed", False)) else "NO",
            "DUE" if bool(paper.get("rebalance_due", False)) else "HOLD",
            str(len(list(card.get("paper_trades", []) or []))),
            _fmt_money(paper_trade_value),
            _fmt_pct(paper.get("target_invested_weight")),
            _fmt_pct(paper.get("target_net_weight")),
        ]
    ]
    outcome_rows = [
        [
            f"{int(row.get('horizon_days', 0) or 0)}d",
            str(int(row.get("labeled_rows", 0) or 0)),
            _fmt_pct(row.get("positive_rate")),
            _fmt_pct(row.get("broken_rate")),
            _fmt_pct(row.get("avg_return")),
            _fmt_pct(row.get("avg_drawdown")),
            str(row.get("latest_outcome_ts", "") or "")[:10] or "-",
        ]
        for row in list(card.get("outcome_summary_rows", []) or [])
    ]
    control_fields = (
        ["run_investment_paper", "force_local_paper_ledger", "run_investment_opportunity"]
        if is_dry_run_view
        else ["run_investment_execution", "submit_investment_execution", "run_investment_guard", "submit_investment_guard", "run_investment_opportunity"]
    )
    execution_control_mode = str(control_portfolio.get("execution_control_mode", "") or "")
    if not execution_control_mode:
        execution_control_mode = _infer_execution_control_mode(control_portfolio if control_portfolio else card)
    execution_control_mode_label = str(EXECUTION_MODE_LABELS.get(execution_control_mode, execution_control_mode))
    recommended_execution_mode = str(execution_mode_recommendation.get("recommended_mode", "") or "")
    recommended_execution_mode_label = str(execution_mode_recommendation.get("recommended_mode_label", "") or "-")
    recommendation_differs = bool(execution_mode_recommendation.get("differs_from_current", False))
    weekly_feedback_pending_live_confirm = bool(control_portfolio.get("weekly_feedback_pending_live_confirm", False))
    weekly_feedback_confirmed_ts = str(control_portfolio.get("weekly_feedback_confirmed_ts", "") or "")
    control_buttons: List[str] = []
    for field in control_fields:
        label = CONTROL_BUTTON_LABELS.get(field, field)
        value = bool(control_portfolio.get(field, card.get(field, False)))
        control_buttons.append(
            f'<button type="button" class="control-toggle{" active" if value else ""}" '
            f'data-portfolio-id="{html.escape(portfolio_id)}" '
            f'data-field="{html.escape(field)}" '
            f'data-label="{html.escape(label)}" '
            f'data-value="{str(value).lower()}">{html.escape(label)}: {"ON" if value else "OFF"}</button>'
        )
    mode_buttons: List[str] = []
    if control_enabled and (not is_dry_run_view):
        for mode_value, mode_label in EXECUTION_MODE_LABELS.items():
            mode_buttons.append(
                f'<button type="button" class="control-mode{" active" if execution_control_mode == mode_value else ""}{" recommended" if recommendation_differs and recommended_execution_mode == mode_value else ""}" '
                f'data-portfolio-id="{html.escape(portfolio_id)}" '
                f'data-mode-value="{html.escape(mode_value)}" '
                f'data-recommended-mode="{html.escape(recommended_execution_mode)}">{html.escape(mode_label)}</button>'
            )
    weekly_feedback_buttons: List[str] = []
    if control_enabled and (not is_dry_run_view) and str(account_mode or "").lower() == "live" and weekly_feedback_pending_live_confirm:
        # live 模式只在有新周报反馈、但尚未确认时显示确认按钮；
        # 这里确认的不只是本周 feedback overlay，也包含本市场下一轮 weekly review 的阈值建议。
        weekly_feedback_buttons.append(
            f'<button type="button" class="control-weekly-feedback" '
            f'data-portfolio-id="{html.escape(portfolio_id)}">确认应用 Weekly Feedback（含阈值建议）</button>'
        )
    control_panel = (
        f"""
  <div class="card-control">
    <div class="meta">dashboard_control={html.escape(control_status or 'configured')} | endpoint={html.escape(control_url or '-')}</div>
    <div class="meta">execution_control_mode=<span class="execution-mode-current" data-portfolio-id="{html.escape(portfolio_id)}">{html.escape(execution_control_mode_label)}</span></div>
    <div class="meta">execution_mode_recommendation=<span class="execution-mode-recommended" data-portfolio-id="{html.escape(portfolio_id)}" data-recommended-mode="{html.escape(recommended_execution_mode)}" data-recommended-label="{html.escape(recommended_execution_mode_label)}">{html.escape(recommended_execution_mode_label)}</span> | change=<span class="execution-mode-change" data-portfolio-id="{html.escape(portfolio_id)}" data-recommended-mode="{html.escape(recommended_execution_mode)}">{'YES' if recommendation_differs else 'NO'}</span></div>
    <div class="meta">{html.escape(str(execution_mode_recommendation.get('reason', '') or '当前没有需要切换执行模式的额外提示。'))}</div>
    <div class="meta">feedback_calibration={html.escape(feedback_automation_label)}</div>
    <div class="meta">weekly_feedback_confirm={'PENDING_CONFIRM' if weekly_feedback_pending_live_confirm else ('CONFIRMED @ ' + weekly_feedback_confirmed_ts[:19] if weekly_feedback_confirmed_ts else 'N/A')} | threshold_suggestion_sync={'PENDING' if weekly_feedback_pending_live_confirm else 'SYNCED'}</div>
    <div class="control-toolbar">
      {''.join(mode_buttons)}
    </div>
    <div class="control-toolbar">
      {''.join(weekly_feedback_buttons)}
    </div>
    <div class="control-toolbar">
      {''.join(control_buttons)}
    </div>
  </div>
"""
        if control_enabled and (control_buttons or mode_buttons)
        else ""
    )
    if is_dry_run_view:
        stats_rows = [
            f"<div><strong>{html.escape(paper_stats_label)} Equity</strong><span>{_fmt_money(paper.get('equity_after'))}</span></div>",
            f"<div><strong>{html.escape(paper_stats_label)} Cash</strong><span>{_fmt_money(paper.get('cash_after'))}</span></div>",
            f"<div><strong>Target Invested</strong><span>{_fmt_pct(paper.get('target_invested_weight'))}</span></div>",
            f"<div><strong>Rebalance</strong><span>{'DUE' if bool(paper.get('rebalance_due', False)) else 'HOLD'}</span></div>",
            f"<div><strong>Data Quality</strong><span>{html.escape(data_quality_label)}</span></div>",
            f"<div><strong>Cost Proxy</strong><span>{html.escape(cost_label)}</span></div>",
            f"<div><strong>Risk Overlay</strong><span>{html.escape(risk_label)}</span></div>",
        ]
        holdings_grid = f"""
  <div>
    <h3>{html.escape(holdings_title)}</h3>
    {_render_table(["symbol", "qty", "market_value", "weight", "status"], holdings_rows)}
  </div>
"""
        dry_run_rows = [
            ["Dry Run 与 trade 共用同一份候选股、计划与市场情绪数据，不会重复构建另一套 universe。"],
            ["本地模拟账本不向 IBKR 下单，只用于复盘目标仓位、调仓节奏、资金利用率与阈值设置。"],
            ["快照回标会把候选股后续 5/20/60 日表现写回数据库，用来判断哪些信号该提高或降低门槛。"],
            ["闭市后仍建议保留 post-report、baseline 与 snapshot labeling；盘中 opportunity/guard 则不需要持续 dry-run。"],
        ]
        dry_run_sections = f"""
  <div class="grid">
    <div>
      <h3>Dry Run 如何形成闭环</h3>
      {_render_table(["explanation"], dry_run_rows)}
    </div>
    <div>
      <h3>快照回标汇总</h3>
      {_render_table(["horizon", "labeled", "positive", "broken", "avg_return", "avg_drawdown", "latest"], outcome_rows)}
    </div>
  </div>
"""
        attribution_rows = [
            [
                _fmt_pct(weekly_attribution.get("weekly_return")),
                _fmt_pct(weekly_attribution.get("selection_contribution")),
                _fmt_pct(weekly_attribution.get("sizing_contribution")),
                _fmt_pct(weekly_attribution.get("sector_contribution")),
                _fmt_pct(weekly_attribution.get("execution_contribution")),
                _fmt_pct(weekly_attribution.get("market_contribution")),
                _fmt_money(weekly_attribution.get("planned_execution_cost_total")),
                _fmt_money(weekly_attribution.get("execution_cost_total")),
                _fmt_money(weekly_attribution.get("execution_cost_gap")),
                str(weekly_attribution.get("dominant_driver", "") or "-"),
                str(weekly_attribution.get("diagnosis", "") or "-"),
            ]
        ] if weekly_attribution else []
        performance_section = f"""
  <div>
    <h3>本地模拟账本状态</h3>
    {_render_table(["ts", "run_id", "rebalanced", "rebalance_due", "simulated_trades", "turnover", "target_invested", "target_net"], dry_run_state_rows)}
  </div>
"""
        execution_plan_section = f"""
    <div>
      <h3>本地模拟调仓</h3>
      {_render_table(["symbol", "action", "qty", "price", "trade_value", "reason"], paper_trade_rows)}
    </div>
"""
        shadow_review_history_section = ""
        strategy_upgrade_section = f"""
  <div>
    <h3>周度代理归因（策略复盘）</h3>
    {_render_table(["weekly_return", "selection", "sizing", "sector", "execution", "market", "plan_cost", "actual_cost", "cost_gap", "dominant", "diagnosis"], attribution_rows)}
  </div>
""" if attribution_rows else ""
        risk_review_rows = [
            [
                str(weekly_risk_review.get("dominant_risk_driver", "") or "-"),
                _fmt_pct(weekly_risk_review.get("latest_dynamic_net_exposure")),
                _fmt_pct(weekly_risk_review.get("latest_dynamic_gross_exposure")),
                f"{float(weekly_risk_review.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
                str(weekly_risk_review.get("latest_stress_worst_scenario_label", "") or "-"),
                _fmt_pct(weekly_risk_review.get("latest_stress_worst_loss")),
                str(weekly_risk_review.get("risk_diagnosis", "") or "-"),
            ]
        ] if weekly_risk_review else []
        strategy_upgrade_section += f"""
  <div>
    <h3>周度风险复盘</h3>
    {_render_table(["driver", "net", "gross", "corr", "stress", "stress_loss", "diagnosis"], risk_review_rows)}
  </div>
""" if risk_review_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>{html.escape(risk_history_source_label)}</h3>
    <div class="meta">{html.escape(risk_history_meta)}</div>
    {_render_table(["ts", "source", "scale", "net", "gross", "corr", "stress", "stress_loss", "driver", "notes"], risk_history_rows)}
  </div>
""" if risk_history_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>风险趋势与告警</h3>
    {_render_table(["alert", "trend", "latest_ts", "previous_ts", "scale", "scale_delta", "net", "net_delta", "gross", "gross_delta", "corr", "corr_delta", "stress", "stress_loss", "stress_delta", "diagnosis"], risk_trend_rows)}
  </div>
""" if risk_trend_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>执行模式建议</h3>
    {_render_table(["current", "recommended", "change", "alert_streak", "watch_streak", "reason"], execution_mode_recommendation_rows)}
  </div>
""" if execution_mode_recommendation_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>本周自动风险反馈</h3>
    {_render_table(["apply_mode", "action", "base_conf", "calib", "final_conf", "single", "sector", "net", "gross", "short", "corr_soft", "source", "reason"], risk_feedback_rows)}
    <div class="meta">{html.escape(risk_feedback_meta)}</div>
  </div>
"""
    else:
        stats_rows = [
            f"<div><strong>{html.escape(broker_stats_label)} Equity</strong><span>{_fmt_money(execution.get('broker_equity') or guard.get('broker_equity'))}</span></div>",
            f"<div><strong>{html.escape(broker_stats_label)} Cash</strong><span>{_fmt_money(execution.get('broker_cash') or guard.get('broker_cash'))}</span></div>",
            f"<div><strong>Target Capital</strong><span>{_fmt_money(execution.get('target_capital'))}</span></div>",
            f"<div><strong>Idle Gap</strong><span>{_fmt_money(execution.get('idle_capital_gap'))}</span></div>",
            f"<div><strong>Review Queue</strong><span>{html.escape(review_label)}</span></div>",
            f"<div><strong>Data Quality</strong><span>{html.escape(data_quality_label)}</span></div>",
            f"<div><strong>Risk Overlay</strong><span>{html.escape(risk_label)}</span></div>",
            f"<div><strong>计划 vs 实际成本</strong><span>{html.escape(weekly_cost_compare_label)}</span></div>",
            f"<div><strong>Execution Feedback</strong><span>{html.escape(execution_feedback_label)}</span></div>",
        ]
        holdings_grid = f"""
  <div>
    <h3>{html.escape(broker_title)}</h3>
    {_render_table(["symbol", "qty", "market_value", "weight", "source"], broker_rows)}
  </div>
"""
        dry_run_sections = ""
        performance_section = f"""
  <div>
    <h3>本周执行质量（当前市场）</h3>
    {_render_table(
        ["submitted", "filled(status/audit)", "blocked/error", "review(shadow/size/total)", "fill_rate(status/audit)", "plan_cost", "actual_cost", "cost_gap", "net_pnl", "commission"],
        [[
            str(weekly_submitted),
            f"{weekly_filled_status} / {weekly_filled_audit}",
            f"{weekly_blocked} / {weekly_error}",
            review_label,
            f"{_fmt_pct(weekly.get('fill_rate_status'))} / {_fmt_pct(weekly.get('fill_rate_audit'))}",
            _fmt_money(weekly_attribution.get("planned_execution_cost_total")),
            _fmt_money(weekly_attribution.get("execution_cost_total")),
            _fmt_money(weekly_attribution.get("execution_cost_gap")),
            _fmt_money(weekly.get("realized_net_pnl")),
            _fmt_money(weekly.get("commission_total")),
        ]],
    )}
  </div>
"""
        execution_plan_section = f"""
    <div>
      <h3>执行计划</h3>
      {_render_table(["symbol", "action", "status", "style", "expected_cost_bps", "reason"], exec_rows)}
    </div>
"""
        shadow_review_history_section = f"""
  <div class="grid">
    <div>
      <h3>Shadow Review 最近记录</h3>
      {_render_table(["ts", "symbol", "action", "order_value", "status", "reason"], shadow_review_recent_rows)}
    </div>
    <div>
      <h3>Shadow Review 重复拦截</h3>
      {_render_table(["symbol", "count", "latest_action", "latest_value", "latest_ts", "latest_reason"], shadow_review_repeat_rows)}
    </div>
  </div>
"""
        strategy_upgrade_rows = [
            [
                str(weekly_shadow_review.get("shadow_review_action", "") or "-"),
                str(int(_safe_float(weekly_shadow_review.get("shadow_review_count"), 0.0))),
                str(int(_safe_float(weekly_shadow_review.get("near_miss_count"), 0.0))),
                str(int(_safe_float(weekly_shadow_review.get("far_below_count"), 0.0))),
                str(int(_safe_float(weekly_shadow_review.get("repeated_symbol_count"), 0.0))),
                str(weekly_shadow_review.get("repeated_symbols", "") or "-"),
                str(weekly_shadow_review.get("shadow_review_reason", "") or "-"),
            ]
        ] if weekly_shadow_review else []
        strategy_upgrade_section = f"""
  <div>
    <h3>策略升级建议（Shadow Weekly）</h3>
    {_render_table(["action", "shadow_reviews", "near_miss", "far_below", "repeat_symbols", "symbols", "reason"], strategy_upgrade_rows)}
  </div>
"""
        risk_review_rows = [
            [
                str(weekly_risk_review.get("dominant_risk_driver", "") or "-"),
                _fmt_pct(weekly_risk_review.get("latest_dynamic_net_exposure")),
                _fmt_pct(weekly_risk_review.get("latest_dynamic_gross_exposure")),
                f"{float(weekly_risk_review.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
                str(weekly_risk_review.get("latest_stress_worst_scenario_label", "") or "-"),
                _fmt_pct(weekly_risk_review.get("latest_stress_worst_loss")),
                str(weekly_risk_review.get("risk_diagnosis", "") or "-"),
            ]
        ] if weekly_risk_review else []
        strategy_upgrade_section += f"""
  <div>
    <h3>周度风险复盘</h3>
    {_render_table(["driver", "net", "gross", "corr", "stress", "stress_loss", "diagnosis"], risk_review_rows)}
  </div>
""" if risk_review_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>{html.escape(risk_history_source_label)}</h3>
    <div class="meta">{html.escape(risk_history_meta)}</div>
    {_render_table(["ts", "source", "scale", "net", "gross", "corr", "stress", "stress_loss", "driver", "notes"], risk_history_rows)}
  </div>
""" if risk_history_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>风险趋势与告警</h3>
    {_render_table(["alert", "trend", "latest_ts", "previous_ts", "scale", "scale_delta", "net", "net_delta", "gross", "gross_delta", "corr", "corr_delta", "stress", "stress_loss", "stress_delta", "diagnosis"], risk_trend_rows)}
  </div>
""" if risk_trend_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>执行模式建议</h3>
    {_render_table(["current", "recommended", "change", "alert_streak", "watch_streak", "reason"], execution_mode_recommendation_rows)}
  </div>
""" if execution_mode_recommendation_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>计划成本 vs 实际执行成本</h3>
    {_render_table(
        ["plan_cost", "actual_cost", "cost_gap", "expected_bps", "actual_slippage_bps", "styles"],
        [[
            _fmt_money(weekly_attribution.get("planned_execution_cost_total")),
            _fmt_money(weekly_attribution.get("execution_cost_total")),
            _fmt_money(weekly_attribution.get("execution_cost_gap")),
            f"{float(weekly_attribution.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}",
            f"{float(weekly_attribution.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}",
            str(weekly_attribution.get("execution_style_breakdown", "") or "-"),
        ]],
    )}
  </div>
"""
        strategy_upgrade_section += f"""
  <div>
    <h3>反馈结果校准</h3>
    {_render_table(["scope", "horizon", "samples", "positive", "broken", "avg_return", "avg_drawdown", "score_align", "signal", "shadow_relax", "risk_tighten", "execution", "reason"], feedback_calibration_rows)}
  </div>
""" if feedback_calibration_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>校准自动化</h3>
    <div class="meta">paper 只会自动应用标成“自动应用”的 feedback；live 保持人工确认。这里直接说明每一类 feedback 当前处于哪一种模式。</div>
      {_render_table(["kind", "action", "mode", "basis", "data_gate", "base_conf", "calib", "final_conf", "weekly_samples", "outcome_samples", "maturity", "pending|ready", "reason"], feedback_automation_rows)}
  </div>
""" if feedback_automation_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>校准自动化历史</h3>
    <div class="meta">这里看最近几周的状态变化，方便判断当前是长期卡住，还是刚从 SOON/READY 往前推进。</div>
      {_render_table(["week", "kind", "state", "mode", "data_gate", "maturity", "pending", "ready", "reason"], feedback_automation_history_rows)}
  </div>
""" if feedback_automation_history_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>结果校准输入缺口</h3>
    <div class="meta">这里解释为什么当前组合还没有形成可用的 outcome 校准样本。若 `历史数据为空` 占多数，通常说明 labeling 阶段还没有拿到可用日线。</div>
    {_render_table(["horizon", "reason", "skipped", "symbols", "sample_symbols", "snapshot_window", "remaining_bars", "ready_estimate"], labeling_skip_rows)}
  </div>
""" if labeling_skip_rows else ""
        strategy_upgrade_section += f"""
  <div>
    <h3>本周自动风险反馈</h3>
    {_render_table(["apply_mode", "action", "base_conf", "calib", "final_conf", "single", "sector", "net", "gross", "short", "corr_soft", "source", "reason"], risk_feedback_rows)}
    <div class="meta">{html.escape(risk_feedback_meta)}</div>
  </div>
"""
        strategy_upgrade_section += f"""
  <div>
    <h3>本周自动执行反馈</h3>
    {_render_table(["apply_mode", "action", "base_conf", "calib", "final_conf", "adv", "split_trigger", "slices", "open_scale", "midday_scale", "close_scale", "source", "reason"], execution_feedback_rows)}
    <div class="meta">{html.escape(execution_feedback_meta)}</div>
  </div>
"""
        strategy_upgrade_section += f"""
  <div>
    <h3>执行时段复盘</h3>
    {_render_table(["session", "action", "plan_cost", "actual_cost", "cost_gap", "expected_bps", "actual_slippage_bps", "scale_delta", "styles", "reason"], execution_session_review_rows)}
  </div>
"""
        strategy_upgrade_section += f"""
  <div>
    <h3>执行热点（symbol + session）</h3>
    {_render_table(["symbol", "session", "action", "plan_cost", "actual_cost", "cost_gap", "expected_bps", "actual_slippage_bps", "pressure", "styles", "reason"], execution_hotspot_rows)}
  </div>
"""
        strategy_upgrade_section += f"""
  <div>
    <h3>执行热点惩罚（下轮候选）</h3>
    {_render_table(["symbol", "sessions", "session_count", "cost_add_bps", "slippage_add_bps", "execution_penalty", "reason"], execution_penalty_rows)}
  </div>
"""
    execution_badge = "NO EXECUTION" if mode == "research-only" else ("DRY RUN" if is_dry_run_view else "EXECUTION READY")

    return f"""
<section class="card" data-open="{str(open_flag).lower()}" data-mode="{html.escape(mode)}" data-actionable="{str(actionable).lower()}" data-dashboard-view="{html.escape(dashboard_view)}" data-market="{html.escape(str(card.get('market', '') or ''))}" data-portfolio-id="{html.escape(portfolio_id)}" data-recommended-mode="{html.escape(recommended_execution_mode or 'AUTO')}" data-execution-mode-change="{str(recommendation_differs).lower()}">
  <div class="card-head">
    <div>
      <h2>{html.escape(card['market'])} / {html.escape(card['watchlist'])}</h2>
      <div class="meta">mode={html.escape(mode)} | account_mode={html.escape(str(card.get('account_mode', '') or '-'))} | open={open_flag} | priority={card['priority_order']} | {html.escape(card['priority_reason'])}</div>
      <div class="meta"><span class="badge badge-mode">{html.escape(mode)}</span> <span class="badge badge-exec">{html.escape(execution_badge)}</span> <span>{html.escape(mode_detail)}</span></div>
      <div class="meta">portfolio_id={html.escape(str(card.get('portfolio_id', '') or '-'))}</div>
      <div class="meta">runtime_scope={html.escape(str(card.get('runtime_scope', '') or '-'))} | account_id={html.escape(str(card.get('account_id', '') or '-'))}</div>
      <div class="meta">report_day={html.escape(str(report_day))} | slot={html.escape(str(report_slot))} | freshness={html.escape(str(report_fresh))}</div>
      <div class="meta">report_schedule={html.escape(report_schedule or '-')} | dir={html.escape(card['report_dir'])}</div>
      <div class="meta"><span class="badge badge-action">{html.escape(action_label or '观察')}</span> <span>{html.escape(action_detail or '-')}</span></div>
      <div class="meta"><strong>推荐 Top10 摘要</strong> {html.escape(candidate_summary or '-')}</div>
      <div class="meta"><strong>Shadow ML</strong> {html.escape(shadow_label)}</div>
      <div class="meta"><strong>数据质量</strong> {html.escape(data_quality_label)}</div>
      <div class="meta"><strong>交易成本代理</strong> {html.escape(cost_label)}</div>
      <div class="meta"><strong>风险覆盖</strong> {html.escape(risk_label)}</div>
      <div class="meta"><strong>自动风险反馈</strong> {html.escape(risk_feedback_label)}</div>
      <div class="meta"><strong>自动执行反馈</strong> {html.escape(execution_feedback_label)}</div>
      <div class="meta"><strong>风险备注</strong> {html.escape(risk_notes or '-')}</div>
      <div class="meta"><strong>行业/主题分布</strong> {html.escape(sector_theme_distribution)}</div>
      <div class="meta"><strong>动作分布</strong> {html.escape(action_distribution)}</div>
    </div>
    <div class="stats">
      {''.join(stats_rows)}
      <div><strong>Opp</strong><span>entry={opp.get('entry_now_count', 0)} / near={opp.get('near_entry_count', 0)} / wait={opp.get('wait_count', 0)}</span></div>
      <div><strong>Guard</strong><span>stop={guard.get('stop_count', 0)} / tp={guard.get('take_profit_count', 0)}</span></div>
      <div><strong>Analysis</strong><span>active={analysis_active_count} / recent_events={analysis_event_count}</span></div>
      <div><strong>IBKR Health</strong><span>{html.escape(str(health.get('status', 'OK') or 'OK'))}</span></div>
      <div><strong>IBKR Health Detail</strong><span>{html.escape(str(health.get('status_detail', '-') or '-'))}</span></div>
    </div>
  </div>

  {control_panel}

  {performance_section}

  {holdings_grid}
  {dry_run_sections}

  <div class="grid">
    <div>
      <h3>当前建议</h3>
      {_render_table(["symbol", "action", "entry_style", "regime", "notes"], plan_rows)}
    </div>
    {execution_plan_section}
  </div>

  {shadow_review_history_section}
  {strategy_upgrade_section}

  <div>
    <h3>研究结论摘要</h3>
    {_render_table(["summary"], [[line] for line in market_summary_lines] if market_summary_lines else [])}
  </div>

  <div>
    <h3>推荐池 Top10</h3>
    {_render_table(["symbol", "action", "score_net", "score_raw", "cost_bps", "ml", "dq", "source_cov", "missing", "source", "class/theme"], candidate_rows)}
  </div>

  <div>
    <h3>盘中机会</h3>
    {_render_table(["symbol", "status", "action", "reason"], opp_rows)}
  </div>

  <div class="grid">
    <div>
      <h3>分析链路状态</h3>
      {_render_table(["symbol", "status", "action", "entry_status", "held_qty", "score"], analysis_state_rows)}
    </div>
    <div>
      <h3>最近分析变迁</h3>
      {_render_table(["ts", "symbol", "event", "from", "to"], analysis_event_rows)}
    </div>
  </div>
</section>
"""


def build_dashboard(config_path: str, out_dir: str) -> Dict[str, Any]:
    cfg = _load_yaml(config_path)
    summary_dir = _resolve_path(out_dir)
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_map = _market_summary_map(summary_dir / "supervisor_cycle_summary.json")
    execution_kpi_dir = _resolve_path(str(cfg.get("dashboard_execution_kpi_dir", "reports_investment_execution")))
    weekly_review_dir_raw = cfg.get("dashboard_weekly_review_dir")
    weekly_review_dir = _resolve_path(str(weekly_review_dir_raw or "reports_investment_weekly"))
    preflight_dir = _resolve_path(str(cfg.get("dashboard_preflight_dir", "reports_preflight") or "reports_preflight"))
    default_week_label, default_week_start = _current_iso_week_label(datetime.now())
    weekly_execution_summary_csv = weekly_review_dir / "weekly_execution_summary.csv"
    execution_weekly_source = (
        weekly_execution_summary_csv
        if weekly_review_dir_raw and _read_all_csv_rows(weekly_execution_summary_csv)
        else execution_kpi_dir / "investment_execution_weekly_summary.csv"
    )
    dashboard_db_raw = str(cfg.get("dashboard_db", "audit.db"))
    markets = list(cfg.get("markets", []) or [])
    cards: List[Dict[str, Any]] = []
    for market_cfg in markets:
        for item in list(dict(market_cfg).get("reports", []) or []):
            if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                continue
            cards.append(_build_report_card(dict(market_cfg), dict(item), summary_map, dashboard_db_raw=dashboard_db_raw))
    # 先吃 weekly review 的自动周报，再回退到旧 execution KPI 导出，避免 dashboard 长时间卡在旧数据。
    execution_weekly = _build_execution_weekly(
        execution_weekly_source,
        default_week=default_week_label,
        default_week_start=default_week_start,
    )
    execution_weekly_groups = _build_execution_weekly_groups(
        execution_weekly_source,
        default_week=default_week_label,
        default_week_start=default_week_start,
    )
    execution_weekly_display, execution_weekly_orphans = _merge_execution_weekly_groups(
        cards,
        execution_weekly_groups,
        week_label=str(execution_weekly.get("week", "") or ""),
    )
    execution_weekly_map: Dict[tuple[str, str], Dict[str, Any]] = {
        (str(row.get("market", "") or ""), str(row.get("portfolio_id", "") or "")): dict(row)
        for row in execution_weekly_display
    }
    for card in cards:
        key = (str(card.get("market", "") or ""), str(card.get("portfolio_id", "") or ""))
        card["execution_weekly_row"] = dict(execution_weekly_map.get(key, {}))
    weekly_shadow_review_rows = _load_weekly_shadow_review_rows(weekly_review_dir)
    weekly_shadow_review_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_shadow_review_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_attribution_rows = _load_weekly_attribution_rows(weekly_review_dir)
    weekly_attribution_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_attribution_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_risk_review_rows = _load_weekly_risk_review_rows(weekly_review_dir)
    weekly_risk_review_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_risk_review_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_risk_feedback_rows = _load_weekly_risk_feedback_rows(weekly_review_dir)
    weekly_risk_feedback_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_risk_feedback_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_execution_feedback_rows = _load_weekly_execution_feedback_rows(weekly_review_dir)
    weekly_execution_feedback_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_execution_feedback_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_execution_session_rows = _load_weekly_execution_session_rows(weekly_review_dir)
    weekly_execution_session_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in weekly_execution_session_rows:
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        if not portfolio_id:
            continue
        weekly_execution_session_map.setdefault(portfolio_id, []).append(dict(row))
    weekly_execution_hotspot_rows = _load_weekly_execution_hotspot_rows(weekly_review_dir)
    weekly_execution_hotspot_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in weekly_execution_hotspot_rows:
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        if not portfolio_id:
            continue
        weekly_execution_hotspot_map.setdefault(portfolio_id, []).append(dict(row))
    weekly_feedback_calibration_rows = _load_weekly_feedback_calibration_rows(weekly_review_dir)
    weekly_feedback_calibration_map: Dict[str, Dict[str, Any]] = {
        str(row.get("portfolio_id", "") or ""): dict(row)
        for row in weekly_feedback_calibration_rows
        if str(row.get("portfolio_id", "") or "").strip()
    }
    weekly_feedback_automation_rows = _load_weekly_feedback_automation_rows(weekly_review_dir)
    weekly_feedback_automation_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in weekly_feedback_automation_rows:
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
        if not portfolio_id or not feedback_kind:
            continue
        weekly_feedback_automation_map.setdefault(portfolio_id, {})[feedback_kind] = dict(row)
    weekly_feedback_threshold_suggestion_rows = _load_weekly_feedback_threshold_suggestion_rows(weekly_review_dir)
    weekly_feedback_threshold_history_rows = _load_weekly_feedback_threshold_history_rows(weekly_review_dir)
    weekly_feedback_threshold_effect_rows = _load_weekly_feedback_threshold_effect_rows(weekly_review_dir)
    weekly_feedback_threshold_cohort_rows = _load_weekly_feedback_threshold_cohort_rows(weekly_review_dir)
    weekly_feedback_threshold_trial_alert_rows = _load_weekly_feedback_threshold_trial_alert_rows(weekly_review_dir)
    weekly_feedback_threshold_tuning_rows = _load_weekly_feedback_threshold_tuning_rows(weekly_review_dir)
    weekly_feedback_threshold_override_rows = _load_weekly_feedback_threshold_override_rows(cfg, weekly_review_dir)
    weekly_labeling_summary = _load_weekly_labeling_summary(weekly_review_dir)
    weekly_labeling_skip_rows = _load_weekly_labeling_skip_rows(weekly_review_dir)
    weekly_labeling_skip_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in weekly_labeling_skip_rows:
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        if not portfolio_id:
            continue
        weekly_labeling_skip_map.setdefault(portfolio_id, []).append(dict(row))
    dashboard_control = _load_dashboard_control_payload(summary_dir, cfg, cards)
    _attach_dashboard_control(cards, dashboard_control)
    for card in cards:
        card["weekly_shadow_review"] = dict(weekly_shadow_review_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_attribution"] = dict(weekly_attribution_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_risk_review"] = dict(weekly_risk_review_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_risk_feedback"] = dict(weekly_risk_feedback_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_execution_sessions"] = list(weekly_execution_session_map.get(str(card.get("portfolio_id", "") or ""), []))
        card["weekly_execution_hotspots"] = list(weekly_execution_hotspot_map.get(str(card.get("portfolio_id", "") or ""), []))
        card["weekly_execution_feedback"] = dict(weekly_execution_feedback_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_feedback_calibration"] = dict(weekly_feedback_calibration_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_feedback_automation_map"] = dict(weekly_feedback_automation_map.get(str(card.get("portfolio_id", "") or ""), {}))
        card["weekly_labeling_skips"] = list(weekly_labeling_skip_map.get(str(card.get("portfolio_id", "") or ""), []))
        card["paper_risk_feedback"] = _build_paper_risk_feedback(card, cfg)
        card["execution_feedback"] = _build_execution_feedback(card, cfg)
    preflight_summary = _load_preflight_summary(preflight_dir)
    ibkr_history_probe_summary = _load_ibkr_history_probe_summary(preflight_dir)
    stock_list_groups = _build_stock_list_groups(cards)
    trade_cards = _expand_display_cards(cards, dashboard_view="trade")
    dry_run_cards = _expand_display_cards(cards, dashboard_view="dry-run")
    trade_execution_mode_recommendation_overview = _build_execution_mode_recommendation_overview(trade_cards)
    trade_execution_mode_recommendation_summary = _build_execution_mode_recommendation_summary(trade_execution_mode_recommendation_overview)
    execution_feedback_overview = _build_execution_feedback_overview(trade_cards)
    execution_feedback_summary = _build_execution_feedback_summary(execution_feedback_overview)
    feedback_automation_overview = _build_feedback_automation_overview(cards)
    feedback_automation_history_overview = _build_feedback_automation_history_overview(cards)
    feedback_automation_stuck_overview = _build_feedback_automation_stuck_overview(cards)
    feedback_automation_effect_overview = _build_feedback_automation_effect_overview(cards)
    feedback_automation_effect_summary = _build_feedback_automation_effect_summary(feedback_automation_effect_overview)
    feedback_maturity_alert_overview = _build_feedback_maturity_alert_overview(feedback_automation_overview)
    labeling_skip_overview = _build_labeling_skip_overview(cards)
    labeling_ready_overview = _build_labeling_ready_overview(labeling_skip_overview)
    market_data_health_overview = _build_market_data_health_overview(cards)
    ops_overview = _build_ops_overview(
        trade_cards,
        preflight_summary=preflight_summary,
        control_payload=dashboard_control,
        execution_mode_summary=trade_execution_mode_recommendation_summary,
    )
    payload = {
        "generated_at": datetime.now().isoformat(),
        "runtime_status": _build_runtime_status(cards),
        "preflight_summary": preflight_summary,
        "ibkr_history_probe_summary": ibkr_history_probe_summary,
        "ops_overview": ops_overview,
        "execution_weekly": execution_weekly,
        "execution_weekly_groups": execution_weekly_groups,
        "execution_weekly_display": execution_weekly_display,
        "execution_weekly_orphans": execution_weekly_orphans,
        "overview": _build_overview(trade_cards),
        "review_overview": _build_review_overview(trade_cards),
        "shadow_review_overview": _build_shadow_review_overview(trade_cards),
        "shadow_strategy_overview": _build_shadow_strategy_overview(trade_cards),
        "feedback_calibration_overview": _build_feedback_calibration_overview(cards),
        "feedback_automation_overview": feedback_automation_overview,
        "feedback_automation_history_overview": feedback_automation_history_overview,
        "feedback_automation_stuck_overview": feedback_automation_stuck_overview,
        "feedback_automation_effect_overview": feedback_automation_effect_overview,
        "feedback_automation_effect_summary": feedback_automation_effect_summary,
        "feedback_threshold_suggestion_summary": weekly_feedback_threshold_suggestion_rows,
        "feedback_threshold_history_overview": weekly_feedback_threshold_history_rows,
        "feedback_threshold_effect_overview": weekly_feedback_threshold_effect_rows,
        "feedback_threshold_cohort_overview": weekly_feedback_threshold_cohort_rows,
        "feedback_threshold_trial_alerts": weekly_feedback_threshold_trial_alert_rows,
        "feedback_threshold_tuning_summary": weekly_feedback_threshold_tuning_rows,
        "feedback_threshold_override_overview": weekly_feedback_threshold_override_rows,
        "feedback_maturity_alert_overview": feedback_maturity_alert_overview,
        "labeling_summary": weekly_labeling_summary,
        "labeling_skip_overview": labeling_skip_overview,
        "labeling_ready_overview": labeling_ready_overview,
        "market_data_health_overview": market_data_health_overview,
        "risk_review_overview": _build_risk_review_overview(cards),
        "trade_risk_history_overview": _build_risk_history_overview(trade_cards),
        "dry_run_risk_history_overview": _build_risk_history_overview(dry_run_cards),
        "trade_risk_alert_overview": _build_risk_alert_overview(trade_cards),
        "dry_run_risk_alert_overview": _build_risk_alert_overview(dry_run_cards),
        "trade_execution_mode_recommendation_overview": trade_execution_mode_recommendation_overview,
        "trade_execution_mode_recommendation_summary": trade_execution_mode_recommendation_summary,
        "risk_feedback_overview": _build_risk_feedback_overview(cards),
        "execution_feedback_overview": execution_feedback_overview,
        "execution_feedback_summary": execution_feedback_summary,
        "execution_hotspot_overview": _build_execution_hotspot_overview(trade_cards),
        "dry_run_attribution_overview": _build_weekly_attribution_overview(dry_run_cards),
        "execution_cost_overview": _build_execution_cost_overview(trade_cards),
        "health_overview": _build_health_overview(trade_cards),
        "stock_list_groups": stock_list_groups,
        "focus_actions": _build_focus_actions(trade_cards),
        "cards": cards,
        "trade_cards": trade_cards,
        "dry_run_cards": dry_run_cards,
        "dashboard_control": dashboard_control,
    }
    return payload


def write_dashboard(payload: Dict[str, Any], out_dir: str) -> None:
    out = _resolve_path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "dashboard.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    trade_cards_html = "\n".join(_render_card(card) for card in list(payload.get("trade_cards", []) or []))
    dry_run_cards = list(payload.get("dry_run_cards", []) or [])
    dry_run_cards_html = "\n".join(_render_card(card) for card in dry_run_cards)
    overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("mode", ""),
            "OPEN" if row.get("exchange_open") else "CLOSED",
            row.get("priority_order", ""),
            row.get("recommended_action", ""),
            row.get("recommended_detail", ""),
            _fmt_money(row.get("paper_equity")),
            _fmt_money(row.get("paper_cash")),
            row.get("ibkr_health", ""),
            str(row.get("opp_entry_now", 0)),
            str(row.get("opp_wait", 0)),
            str(row.get("execution_orders", 0)),
        ]
        for row in list(payload.get("overview", []) or [])
    ]
    health_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("status", ""),
            row.get("status_detail", ""),
            str(row.get("delayed_count", 0)),
            str(row.get("permission_count", 0)),
            str(row.get("connectivity_breaks", 0)),
            str(row.get("account_limit_count", 0)),
            row.get("latest_event_label", ""),
            row.get("latest_event_ts", ""),
        ]
        for row in list(payload.get("health_overview", []) or [])
    ]
    dry_run_overview_rows = []
    for card in dry_run_cards:
        paper = dict(card.get("paper_summary", {}) or {})
        horizon_labels = ",".join(
            f"{int(row.get('horizon_days', 0) or 0)}d"
            for row in list(card.get("outcome_summary_rows", []) or [])
        ) or "-"
        dry_run_overview_rows.append(
            [
                card.get("market", ""),
                card.get("watchlist", ""),
                _fmt_money(paper.get("equity_after")),
                _fmt_money(paper.get("cash_after")),
                _fmt_pct(paper.get("target_invested_weight")),
                horizon_labels,
                str(bool(paper.get("executed", False))),
            ]
        )
    dry_run_attribution_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            _fmt_pct(row.get("weekly_return")),
            _fmt_pct(row.get("selection_contribution")),
            _fmt_pct(row.get("sizing_contribution")),
            _fmt_pct(row.get("sector_contribution")),
            _fmt_pct(row.get("execution_contribution")),
            _fmt_pct(row.get("market_contribution")),
            _fmt_money(row.get("planned_execution_cost_total")),
            _fmt_money(row.get("execution_cost_total")),
            _fmt_money(row.get("execution_cost_gap")),
            row.get("dominant_driver", ""),
            row.get("diagnosis", "")[:96] or "-",
        ]
        for row in list(payload.get("dry_run_attribution_overview", []) or [])
    ]
    stock_list_sections: List[str] = []
    for group in list(payload.get("stock_list_groups", []) or []):
        rows = [
            [
                row.get("watchlist", ""),
                row.get("symbol", ""),
                row.get("tracked_status", ""),
                row.get("action", "") or "-",
                row.get("entry_status", "") or "-",
                f"{_safe_float(row.get('score'), 0.0):.3f}" if str(row.get("score", "")).strip() else "-",
                _fmt_qty(row.get("paper_qty")),
                _fmt_qty(row.get("broker_qty")),
                row.get("list_origin", ""),
                row.get("source_scopes", "") or "-",
                row.get("reason", "")[:72] or "-",
            ]
            for row in list(group.get("rows", []) or [])
        ]
        stock_list_sections.append(
            f"""
            <div style="margin-bottom:18px;">
              <h3>{html.escape(str(group.get('market', '') or '-'))}</h3>
              {_render_table(["watchlist", "symbol", "tracked_status", "action", "entry_status", "score", "paper_qty", "broker_qty", "origin", "general_scopes", "reason"], rows)}
            </div>
            """
        )
    focus_cards = []
    for row in list(payload.get("focus_actions", []) or []):
        market = html.escape(str(row.get("market", "") or ""))
        watchlist = html.escape(str(row.get("watchlist", "") or ""))
        action = html.escape(str(row.get("action", "") or ""))
        detail = html.escape(str(row.get("detail", "") or "-"))
        mode = html.escape(str(row.get("mode", "") or ""))
        state = "OPEN" if bool(row.get("exchange_open", False)) else "CLOSED"
        focus_cards.append(
            f"""
            <div class="focus-card">
              <div class="focus-top">
                <span class="badge badge-market">{market}</span>
                <span class="badge badge-mode">{mode}</span>
                <span class="badge badge-state">{state}</span>
              </div>
              <div class="focus-title">{market} / {watchlist}</div>
              <div class="focus-action">{action}</div>
              <div class="focus-detail">{detail}</div>
            </div>
            """
        )
    runtime_status = dict(payload.get("runtime_status", {}) or {})
    ops_overview = dict(payload.get("ops_overview", {}) or {})
    dashboard_control = dict(payload.get("dashboard_control", {}) or {})
    control_service = dict(dashboard_control.get("service", {}) or {})
    control_actions = dict(dashboard_control.get("actions", {}) or {})
    control_enabled = bool(control_service.get("enabled", False))
    control_url = str(control_service.get("url", "") or "")
    control_status_text = (
        f"service={str(control_service.get('status', 'disabled') or 'disabled')} | "
        f"endpoint={control_url or '-'} | "
        f"last_action={str(control_actions.get('last_action', '-') or '-')} | "
        f"last_error={str(control_actions.get('last_error', '-') or '-')}"
    )
    ops_alert_rows = [
        [
            row.get("category", ""),
            row.get("name", ""),
            row.get("status", ""),
            row.get("detail", ""),
        ]
        for row in list(ops_overview.get("alert_rows", []) or [])
    ]
    preflight_banner_rows = [
        [
            str(row.get("status", "") or ""),
            str(row.get("name", "") or ""),
            str(row.get("detail", "") or ""),
        ]
        for row in list(ops_overview.get("preflight_banner_rows", []) or [])
    ]
    preflight_banner = f"""
    <section class="card overview recommendation-banner ops-banner ops-banner-{html.escape(str(ops_overview.get('preflight_banner_level', 'WARN') or 'WARN').lower())}">
      <h2>Preflight 关键提示</h2>
      <div class="meta" style="font-size:18px; font-weight:700; color:var(--ink); margin-bottom:8px;">{html.escape(str(ops_overview.get('preflight_banner_title', '') or ''))}</div>
      <div class="meta">{html.escape(str(ops_overview.get('preflight_banner_reason', '') or ''))}</div>
      <div class="meta" style="margin-top:8px;">{html.escape(str(ops_overview.get('preflight_banner_action', '') or ''))}</div>
      {_render_table(["status", "name", "detail"], preflight_banner_rows) if preflight_banner_rows else ""}
    </section>
    """ if str(ops_overview.get("preflight_banner_title", "") or "").strip() else ""
    ops_card = f"""
    <section class="card overview">
      <h2>运维总览</h2>
      <div class="meta">{html.escape(str(ops_overview.get('summary_text', '尚无运维摘要') or '尚无运维摘要'))}</div>
      <div class="stats">
        <div><strong>Preflight</strong><span>P{int(ops_overview.get('preflight_pass_count', 0) or 0)} / W{int(ops_overview.get('preflight_warn_count', 0) or 0)} / F{int(ops_overview.get('preflight_fail_count', 0) or 0)}</span></div>
        <div><strong>IBKR Ports</strong><span>{int(ops_overview.get('ibkr_port_warning_count', 0) or 0)} warnings</span></div>
        <div><strong>Stale Reports</strong><span>{int(ops_overview.get('stale_report_count', 0) or 0)}</span></div>
        <div><strong>Degraded Health</strong><span>{int(ops_overview.get('degraded_health_count', 0) or 0)}</span></div>
        <div><strong>Mode Mismatch</strong><span>{int(ops_overview.get('execution_mode_mismatch_count', 0) or 0)}</span></div>
        <div><strong>Control</strong><span>{html.escape(str(ops_overview.get('control_service_status', '-') or '-'))}</span></div>
      </div>
      <div class="meta">preflight_generated_at={html.escape(str(ops_overview.get('preflight_generated_at', '-') or '-'))}</div>
      {_render_table(["category", "name", "status", "detail"], ops_alert_rows) if ops_alert_rows else '<div class="empty">当前没有需要优先处理的运维告警。</div>'}
    </section>
    """
    control_panel = (
        f"""
    <section class="card overview" id="dashboard-control" data-control-url="{html.escape(control_url)}">
      <h2>Dashboard 控制</h2>
      <div class="meta" id="control-status">{html.escape(control_status_text)}</div>
      <div class="control-toolbar">
        <button type="button" class="control-action" data-api-action="run_once">立即跑一轮</button>
        <button type="button" class="control-action" data-api-action="run_preflight">立即跑 Preflight</button>
        <button type="button" class="control-action" data-api-action="run_weekly_review">立即跑 Weekly Review</button>
        <button type="button" class="control-action" data-api-action="refresh_dashboard">刷新 Dashboard</button>
      </div>
      <div class="meta">这些按钮调用本机 supervisor control service；组合级开关会写入当前 summary 目录的 `dashboard_control_state.json`，并在下次启动 `python -m src.app.supervisor` 时自动恢复。</div>
    </section>
    """
        if control_enabled
        else ""
    )
    execution_mode_summary = dict(payload.get("trade_execution_mode_recommendation_summary", {}) or {})
    execution_mode_summary_market_rows = [
        [
            str(row.get("market", "") or "-"),
            str(int(row.get("mismatch_count", 0) or 0)),
            str(int(row.get("review_only_count", 0) or 0)),
            str(int(row.get("paused_count", 0) or 0)),
        ]
        for row in list(execution_mode_summary.get("market_rows", []) or [])
    ]
    execution_mode_summary_market_buttons = "".join(
        f'<button type="button" class="execution-mode-market-filter" data-market-filter="{html.escape(str(row[0]))}">{html.escape(str(row[0]))} ({html.escape(str(row[2]))}/{html.escape(str(row[3]))})</button>'
        for row in execution_mode_summary_market_rows
    )
    trade_banner = f"""
    <section class="card overview">
      <h2>交易运行状态</h2>
      <div class="meta" style="font-size:18px; font-weight:700; color:var(--ink); margin-bottom:10px;">{html.escape(str(runtime_status.get('summary_text', '-') or '-'))}</div>
      <div class="meta">market_modes={html.escape(str(runtime_status.get('market_mode_summary_text', '-') or '-'))}</div>
    </section>
    """
    execution_mode_summary_card = f"""
    <section class="card overview recommendation-banner" id="execution-mode-summary">
      <h2>执行模式告警计数</h2>
      <div class="meta" id="execution-mode-summary-text">{html.escape(str(execution_mode_summary.get("summary_text", "") or ""))}</div>
      <div class="stats">
        <div><strong>Need Change</strong><span id="execution-mode-summary-mismatch">{int(execution_mode_summary.get("mismatch_count", 0) or 0)}</span></div>
        <div><strong>Review Only</strong><span id="execution-mode-summary-review-only">{int(execution_mode_summary.get("review_only_count", 0) or 0)}</span></div>
        <div><strong>Paused</strong><span id="execution-mode-summary-paused">{int(execution_mode_summary.get("paused_count", 0) or 0)}</span></div>
      </div>
      <div class="control-toolbar">
        <button type="button" class="execution-mode-market-filter active" data-market-filter="">全部</button>
        {execution_mode_summary_market_buttons}
      </div>
      <div class="meta">
        <span id="execution-mode-market-filter-label">当前告警市场筛选：全部</span>
        <button type="button" id="execution-mode-market-filter-clear" style="display:none; margin-left:8px;">× 清除</button>
      </div>
      <table>
        <thead>
          <tr>
            <th>market</th>
            <th>need_change</th>
            <th>review_only</th>
            <th>paused</th>
          </tr>
        </thead>
        <tbody id="execution-mode-summary-market-body">
          {''.join(f"<tr><td>{html.escape(str(row[0]))}</td><td>{html.escape(str(row[1]))}</td><td>{html.escape(str(row[2]))}</td><td>{html.escape(str(row[3]))}</td></tr>" for row in execution_mode_summary_market_rows)}
        </tbody>
      </table>
    </section>
    """ if int(execution_mode_summary.get("mismatch_count", 0) or 0) > 0 else ""
    dry_run_banner = """
    <section class="card overview">
      <h2>Dry Run 页面说明</h2>
      <div class="meta">这里展示的是本地模拟账本与快照回标，不会向 IBKR 提交订单。它和 trade 共用同一份股票池、候选股与计划数据，目的是验证资金利用率、调仓节奏、阈值和打分是否需要升级。</div>
      <div class="meta">如果这里有 5/20/60 日回标数据，就能直接判断哪些信号长期有效、哪些执行门太松或太紧；闭市后更适合跑 post-report、baseline 和 snapshot labeling，而不是反复做盘中机会扫描。</div>
    </section>
    """
    dry_run_overview_card = f"""
    <section class="card overview">
      <h2>Dry Run 总览</h2>
      {_render_table(["market", "watchlist", "ledger_equity", "ledger_cash", "target_invested", "labeled_horizons", "rebalanced"], dry_run_overview_rows)}
    </section>
    """ if dry_run_overview_rows else """
    <section class="card overview">
      <h2>Dry Run 总览</h2>
      <div class="empty">当前没有启用中的 dry-run 页面数据；如果要和 trade 同时跑，请在对应 report 打开 `force_local_paper_ledger`。</div>
    </section>
    """
    dry_run_attribution_card = f"""
    <section class="card overview">
      <h2>Dry Run 周度代理归因</h2>
      <div class="meta">这是策略复盘用的代理归因 v1，用来指导调阈值、调信号和调仓位；它会尽量回收到周收益，但不是严格的学术因子归因。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "weekly_return", "selection", "sizing", "sector", "execution", "market", "plan_cost", "actual_cost", "cost_gap", "dominant", "diagnosis"], dry_run_attribution_rows)}
    </section>
    """ if dry_run_attribution_rows else """
    <section class="card overview">
      <h2>Dry Run 周度代理归因</h2>
      <div class="empty">当前还没有可展示的周度代理归因数据。</div>
    </section>
    """
    stock_list_card = f"""
    <section class="card overview" id="stock-list" data-view="stock-list">
      <h2>股票列表</h2>
      <div class="meta">通用分析列表会跨 repo 与各个 runtime scope 合并，不会因切换账号或 live/paper 而缩减；当前账户的 paper/broker holding 只会作为补充信息加入。</div>
      {''.join(stock_list_sections) or '<div class="empty">当前没有可展示的股票列表。</div>'}
    </section>
    """
    execution_weekly = dict(payload.get("execution_weekly", {}) or {})
    execution_weekly_groups = list(payload.get("execution_weekly_display", []) or [])
    execution_weekly_orphans = list(payload.get("execution_weekly_orphans", []) or [])
    if execution_weekly:
        weekly_card = f"""
        <section class="card overview">
          <h2>本周执行质量</h2>
          <div class="stats weekly-stats">
            <div><strong>Week</strong><span>{html.escape(str(execution_weekly.get('week', '') or '-'))}</span></div>
            <div><strong>Submitted</strong><span>{int(execution_weekly.get('submitted_order_rows', 0) or 0)}</span></div>
            <div><strong>Filled (status/audit)</strong><span>{int(execution_weekly.get('filled_order_rows', 0) or 0)} / {int(execution_weekly.get('filled_with_audit_rows', 0) or 0)}</span></div>
            <div><strong>Blocked/Error</strong><span>{int(execution_weekly.get('blocked_opportunity_rows', 0) or 0)} / {int(execution_weekly.get('error_order_rows', 0) or 0)}</span></div>
            <div><strong>Fill Rate (status/audit)</strong><span>{_fmt_pct(execution_weekly.get('fill_rate_status'))} / {_fmt_pct(execution_weekly.get('fill_rate_audit'))}</span></div>
            <div><strong>Net PnL</strong><span>{_fmt_money(execution_weekly.get('realized_net_pnl'))}</span></div>
            <div><strong>Commission</strong><span>{_fmt_money(execution_weekly.get('commission_total'))}</span></div>
          </div>
        </section>
        """
    else:
        weekly_card = """
        <section class="card overview">
          <h2>本周执行质量</h2>
          <div class="empty">当前还没有可展示的 execution 周度数据。</div>
        </section>
        """
    weekly_group_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            str(int(row.get("submitted_order_rows", 0) or 0)),
            f"{int(row.get('filled_order_rows', 0) or 0)} / {int(row.get('filled_with_audit_rows', 0) or 0)}",
            f"{int(row.get('blocked_opportunity_rows', 0) or 0)} / {int(row.get('error_order_rows', 0) or 0)}",
            f"{_fmt_pct(row.get('fill_rate_status'))} / {_fmt_pct(row.get('fill_rate_audit'))}",
            _fmt_money(row.get("realized_net_pnl")),
            _fmt_money(row.get("commission_total")),
        ]
        for row in execution_weekly_groups
    ]
    weekly_group_card = f"""
    <section class="card overview">
      <h2>本周执行质量（分市场）</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "submitted", "filled(status/audit)", "blocked/error", "fill_rate(status/audit)", "net_pnl", "commission"], weekly_group_rows)}
    </section>
    """
    orphan_group_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            str(int(row.get("submitted_order_rows", 0) or 0)),
            f"{int(row.get('filled_order_rows', 0) or 0)} / {int(row.get('filled_with_audit_rows', 0) or 0)}",
            f"{int(row.get('blocked_opportunity_rows', 0) or 0)} / {int(row.get('error_order_rows', 0) or 0)}",
            f"{_fmt_pct(row.get('fill_rate_status'))} / {_fmt_pct(row.get('fill_rate_audit'))}",
            _fmt_money(row.get("realized_net_pnl")),
            _fmt_money(row.get("commission_total")),
        ]
        for row in execution_weekly_orphans
    ]
    orphan_group_card = (
        f"""
    <section class="card overview">
      <h2>历史执行残留（当前未纳入市场卡片）</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "submitted", "filled(status/audit)", "blocked/error", "fill_rate(status/audit)", "net_pnl", "commission"], orphan_group_rows)}
    </section>
    """
        if orphan_group_rows
        else ""
    )
    review_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            str(int(row.get("shadow_review_count", 0) or 0)),
            str(int(row.get("size_review_count", 0) or 0)),
            str(int(row.get("total_review_count", 0) or 0)),
            _fmt_money(row.get("idle_capital_gap")),
            row.get("recommended_action", ""),
        ]
        for row in list(payload.get("review_overview", []) or [])
    ]
    review_overview_card = f"""
    <section class="card overview">
      <h2>人工审核队列</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "shadow_review", "size_review", "total_review", "idle_gap", "recommended_action"], review_overview_rows)}
    </section>
    """ if review_overview_rows else """
    <section class="card overview">
      <h2>人工审核队列</h2>
      <div class="empty">当前没有可展示的人工审核数据。</div>
    </section>
    """
    shadow_review_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("symbol", ""),
            str(int(row.get("repeat_count", 0) or 0)),
            row.get("latest_action", ""),
            _fmt_money(row.get("latest_order_value")),
            str(row.get("latest_ts", "") or "")[:19],
            str(row.get("latest_reason", "") or "")[:90],
        ]
        for row in list(payload.get("shadow_review_overview", []) or [])
    ]
    shadow_review_overview_card = f"""
    <section class="card overview">
      <h2>Shadow Review 历史重点</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "symbol", "repeat_count", "latest_action", "latest_value", "latest_ts", "latest_reason"], shadow_review_overview_rows)}
    </section>
    """ if shadow_review_overview_rows else """
    <section class="card overview">
      <h2>Shadow Review 历史重点</h2>
      <div class="empty">当前没有 shadow review 历史记录。</div>
    </section>
    """
    shadow_strategy_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("shadow_review_action", ""),
            str(int(row.get("shadow_review_count", 0) or 0)),
            str(int(row.get("near_miss_count", 0) or 0)),
            str(int(row.get("far_below_count", 0) or 0)),
            str(int(row.get("repeated_symbol_count", 0) or 0)),
            row.get("repeated_symbols", "") or "-",
            row.get("shadow_review_reason", "")[:96] or "-",
        ]
        for row in list(payload.get("shadow_strategy_overview", []) or [])
    ]
    risk_review_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("dominant_risk_driver", ""),
            _fmt_pct(row.get("latest_dynamic_net_exposure")),
            _fmt_pct(row.get("latest_dynamic_gross_exposure")),
            f"{float(row.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
            row.get("latest_stress_worst_scenario_label", "") or "-",
            _fmt_pct(row.get("latest_stress_worst_loss")),
            row.get("risk_diagnosis", "")[:96] or "-",
        ]
        for row in list(payload.get("risk_review_overview", []) or [])
    ]
    trade_risk_history_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            str(row.get("ts", "") or "")[:19] or "-",
            row.get("source_label", "") or "-",
            f"{float(row.get('dynamic_scale', 1.0) or 1.0):.2f}",
            _fmt_pct(row.get("dynamic_net_exposure")),
            _fmt_pct(row.get("dynamic_gross_exposure")),
            f"{float(row.get('avg_pair_correlation', 0.0) or 0.0):.2f}",
            row.get("stress_worst_scenario_label", "") or "-",
            _fmt_pct(row.get("stress_worst_loss")),
            row.get("dominant_risk_driver", "") or "-",
            row.get("notes_preview", "")[:96] or "-",
        ]
        for row in list(payload.get("trade_risk_history_overview", []) or [])
    ]
    dry_run_risk_history_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            str(row.get("ts", "") or "")[:19] or "-",
            row.get("source_label", "") or "-",
            f"{float(row.get('dynamic_scale', 1.0) or 1.0):.2f}",
            _fmt_pct(row.get("dynamic_net_exposure")),
            _fmt_pct(row.get("dynamic_gross_exposure")),
            f"{float(row.get('avg_pair_correlation', 0.0) or 0.0):.2f}",
            row.get("stress_worst_scenario_label", "") or "-",
            _fmt_pct(row.get("stress_worst_loss")),
            row.get("dominant_risk_driver", "") or "-",
            row.get("notes_preview", "")[:96] or "-",
        ]
        for row in list(payload.get("dry_run_risk_history_overview", []) or [])
    ]
    trade_risk_alert_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("source_label", "") or "-",
            row.get("alert_level", "") or "-",
            row.get("trend_label", "") or "-",
            str(row.get("latest_ts", "") or "")[:19] or "-",
            f"{float(row.get('latest_dynamic_scale', 1.0) or 1.0):.2f}",
            f"{float(row.get('scale_delta', 0.0) or 0.0):+.2f}",
            _fmt_pct(row.get("latest_dynamic_net_exposure")),
            _fmt_pct(row.get("latest_dynamic_gross_exposure")),
            f"{float(row.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
            row.get("latest_stress_worst_scenario_label", "") or "-",
            _fmt_pct(row.get("latest_stress_worst_loss")),
            row.get("diagnosis", "")[:96] or "-",
        ]
        for row in list(payload.get("trade_risk_alert_overview", []) or [])
    ]
    execution_mode_recommendation_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("current_mode", "") or "-",
            row.get("recommended_mode", "") or "-",
            "YES" if bool(row.get("differs_from_current", False)) else "NO",
            row.get("alert_level", "") or "-",
            row.get("trend_label", "") or "-",
            str(int(row.get("alert_streak", 0) or 0)),
            str(int(row.get("watch_streak", 0) or 0)),
            row.get("reason", "")[:96] or "-",
        ]
        for row in list(payload.get("trade_execution_mode_recommendation_overview", []) or [])
    ]
    execution_mode_banner_rows = [
        {
            "market": row.get("market", ""),
            "watchlist": row.get("watchlist", ""),
            "portfolio_id": row.get("portfolio_id", ""),
            "current_mode": row.get("current_mode", "") or "-",
            "recommended_mode": row.get("recommended_mode", "") or "-",
            "recommended_mode_code": (
                "PAUSED"
                if str(row.get("recommended_mode", "") or "") == str(EXECUTION_MODE_LABELS.get("PAUSED", ""))
                else "REVIEW_ONLY"
                if str(row.get("recommended_mode", "") or "") == str(EXECUTION_MODE_LABELS.get("REVIEW_ONLY", ""))
                else "AUTO"
            ),
            "alert_level": row.get("alert_level", "") or "-",
            "trend_label": row.get("trend_label", "") or "-",
            "reason": row.get("reason", "")[:120] or "-",
        }
        for row in list(payload.get("trade_execution_mode_recommendation_overview", []) or [])
        if bool(row.get("differs_from_current", False))
    ][:8]
    dry_run_risk_alert_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("source_label", "") or "-",
            row.get("alert_level", "") or "-",
            row.get("trend_label", "") or "-",
            str(row.get("latest_ts", "") or "")[:19] or "-",
            f"{float(row.get('latest_dynamic_scale', 1.0) or 1.0):.2f}",
            f"{float(row.get('scale_delta', 0.0) or 0.0):+.2f}",
            _fmt_pct(row.get("latest_dynamic_net_exposure")),
            _fmt_pct(row.get("latest_dynamic_gross_exposure")),
            f"{float(row.get('latest_avg_pair_correlation', 0.0) or 0.0):.2f}",
            row.get("latest_stress_worst_scenario_label", "") or "-",
            _fmt_pct(row.get("latest_stress_worst_loss")),
            row.get("diagnosis", "")[:96] or "-",
        ]
        for row in list(payload.get("dry_run_risk_alert_overview", []) or [])
    ]
    feedback_calibration_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("selection_scope_label", "") or "-",
            row.get("selected_horizon_days", "") or "-",
            str(int(row.get("outcome_sample_count", 0) or 0)),
            _fmt_pct(row.get("outcome_positive_rate")),
            _fmt_pct(row.get("outcome_broken_rate")),
            _fmt_signed_pct(row.get("avg_future_return")),
            _fmt_signed_pct(row.get("avg_max_drawdown")),
            f"{float(row.get('score_alignment_score', 0.0) or 0.0):.2f}",
            f"{float(row.get('signal_quality_score', 0.0) or 0.0):.2f}",
            f"{float(row.get('execution_support', 0.0) or 0.0):.2f}",
            f"{float(row.get('calibration_confidence', 0.0) or 0.0):.2f}/{row.get('calibration_confidence_label', '') or '-'}",
            row.get("calibration_reason", "")[:96] or "-",
        ]
        for row in list(payload.get("feedback_calibration_overview", []) or [])
    ]
    feedback_automation_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("feedback_action", "") or "-",
            row.get("calibration_apply_mode_label", "") or "-",
            row.get("calibration_basis_label", "") or "-",
            row.get("market_data_gate_label", "") or "-",
            f"{float(row.get('feedback_base_confidence', 0.0) or 0.0):.2f}/{row.get('feedback_base_confidence_label', '') or '-'}",
            f"{float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{row.get('feedback_calibration_label', '') or '-'}",
            f"{float(row.get('feedback_confidence', 0.0) or 0.0):.2f}/{row.get('feedback_confidence_label', '') or '-'}",
            str(int(row.get("feedback_sample_count", 0) or 0)),
            str(int(row.get("feedback_calibration_sample_count", 0) or 0)),
            (
                f"{float(row.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                f"{row.get('outcome_maturity_label', '') or 'UNKNOWN'}"
            ),
            (
                f"{int(row.get('outcome_pending_sample_count', 0) or 0)} | "
                f"{str(row.get('outcome_ready_estimate_end_ts', '') or '-')[:10]}"
            ),
            row.get("automation_reason", "")[:96] or "-",
        ]
        for row in list(payload.get("feedback_automation_overview", []) or [])
    ]
    feedback_maturity_alert_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("calibration_apply_mode_label", "") or "-",
            row.get("alert_bucket", "") or "-",
            (
                f"{float(row.get('outcome_maturity_ratio', 0.0) or 0.0):.2f}/"
                f"{row.get('outcome_maturity_label', '') or 'UNKNOWN'}"
            ),
            str(int(row.get("outcome_pending_sample_count", 0) or 0)),
            (
                str(int(row.get("days_until_ready", 0) or 0))
                if int(row.get("days_until_ready", -1) or -1) >= 0
                else "-"
            ),
            str(row.get("ready_estimate_end_ts", "") or "-")[:10],
            row.get("suggestion", "")[:96] or "-",
        ]
        for row in list(payload.get("feedback_maturity_alert_overview", []) or [])
    ]
    feedback_automation_history_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("current_state", "") or "-",
            row.get("current_mode", "") or "-",
            row.get("transition", "") or "-",
            str(int(row.get("same_state_weeks", 0) or 0)),
            str(int(row.get("weeks_tracked", 0) or 0)),
            row.get("maturity", "") or "-",
            str(int(row.get("pending", 0) or 0)),
            row.get("ready", "") or "-",
            row.get("state_chain", "") or "-",
        ]
        for row in list(payload.get("feedback_automation_history_overview", []) or [])
    ]
    feedback_automation_stuck_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("stuck_bucket", "") or "-",
            row.get("current_state", "") or "-",
            row.get("current_mode", "") or "-",
            str(int(row.get("same_state_weeks", 0) or 0)),
            str(int(row.get("weeks_tracked", 0) or 0)),
            row.get("maturity", "") or "-",
            str(int(row.get("pending", 0) or 0)),
            row.get("ready", "") or "-",
            row.get("reason", "")[:96] or "-",
        ]
        for row in list(payload.get("feedback_automation_stuck_overview", []) or [])
    ]
    feedback_automation_effect_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("baseline_week", "") or "-",
            row.get("current_state", "") or "-",
            row.get("current_mode", "") or "-",
            str(int(row.get("active_weeks", 0) or 0)),
            row.get("effect_w1", "") or "-",
            row.get("effect_w2", "") or "-",
            row.get("effect_w4", "") or "-",
            row.get("effect_label", "") or "-",
            row.get("effect_metric", "") or "-",
            row.get("driver", "") or "-",
            row.get("reason", "")[:96] or "-",
        ]
        for row in list(payload.get("feedback_automation_effect_overview", []) or [])
    ]
    feedback_automation_effect_summary_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("summary_signal", "") or "-",
            str(int(row.get("tracked_count", 0) or 0)),
            str(int(row.get("latest_improved_count", 0) or 0)),
            str(int(row.get("latest_deteriorated_count", 0) or 0)),
            str(int(row.get("latest_stable_count", 0) or 0)),
            str(int(row.get("latest_observe_count", 0) or 0)),
            str(int(row.get("w1_improved_count", 0) or 0)),
            str(int(row.get("w2_improved_count", 0) or 0)),
            str(int(row.get("w4_improved_count", 0) or 0)),
            str(int(row.get("w1_deteriorated_count", 0) or 0)),
            str(int(row.get("w2_deteriorated_count", 0) or 0)),
            str(int(row.get("w4_deteriorated_count", 0) or 0)),
            f"{float(row.get('avg_active_weeks', 0.0) or 0.0):.1f}",
            row.get("top_portfolios_text", "") or "-",
        ]
        for row in list(payload.get("feedback_automation_effect_summary", []) or [])
    ]
    feedback_threshold_suggestion_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("suggestion_label", "") or "-",
            row.get("summary_signal", "") or "-",
            str(int(row.get("tracked_count", 0) or 0)),
            f"{float(row.get('avg_active_weeks', 0.0) or 0.0):.1f}",
            f"{float(row.get('base_auto_confidence', 0.0) or 0.0):.2f}->{float(row.get('suggested_auto_confidence', 0.0) or 0.0):.2f}",
            f"{float(row.get('base_auto_base_confidence', 0.0) or 0.0):.2f}->{float(row.get('suggested_auto_base_confidence', 0.0) or 0.0):.2f}",
            f"{float(row.get('base_auto_calibration_score', 0.0) or 0.0):.2f}->{float(row.get('suggested_auto_calibration_score', 0.0) or 0.0):.2f}",
            f"{float(row.get('base_auto_maturity_ratio', 0.0) or 0.0):.2f}->{float(row.get('suggested_auto_maturity_ratio', 0.0) or 0.0):.2f}",
            row.get("examples", "") or "-",
            row.get("reason", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_suggestion_summary", []) or [])
    ]
    feedback_threshold_history_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("current_label", "") or "-",
            row.get("summary_signal", "") or "-",
            row.get("transition", "") or "-",
            row.get("trend_bucket", "") or "-",
            str(int(row.get("same_action_weeks", 0) or 0)),
            str(int(row.get("weeks_tracked", 0) or 0)),
            row.get("threshold_snapshot", "") or "-",
            row.get("action_chain", "") or "-",
            row.get("reason", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_history_overview", []) or [])
    ]
    feedback_threshold_effect_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("current_label", "") or "-",
            row.get("summary_signal", "") or "-",
            row.get("effect_label", "") or "-",
            str(int(row.get("same_action_weeks", 0) or 0)),
            str(int(row.get("weeks_tracked", 0) or 0)),
            f"{float(row.get('avg_active_weeks', 0.0) or 0.0):.1f}",
            row.get("threshold_snapshot", "") or "-",
            row.get("action_chain", "") or "-",
            row.get("effect_reason", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_effect_overview", []) or [])
    ]
    feedback_threshold_cohort_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("cohort_label", "") or "-",
            row.get("baseline_week", "") or "-",
            str(int(row.get("cohort_weeks", 0) or 0)),
            row.get("latest_effect", "") or "-",
            row.get("effect_w1", "") or "-",
            row.get("effect_w2", "") or "-",
            row.get("effect_w4", "") or "-",
            row.get("action_chain", "") or "-",
            row.get("diagnosis", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_cohort_overview", []) or [])
    ]
    feedback_threshold_trial_alert_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("stage_label", "") or "-",
            row.get("action_label", "") or "-",
            row.get("baseline_week", "") or "-",
            str(int(row.get("cohort_weeks", 0) or 0)),
            row.get("latest_effect", "") or "-",
            row.get("effect_w1", "") or "-",
            row.get("next_check", "") or "-",
            row.get("diagnosis", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_trial_alerts", []) or [])
    ]
    feedback_threshold_tuning_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("suggestion_label", "") or "-",
            row.get("cohort_label", "") or "-",
            row.get("baseline_week", "") or "-",
            str(int(row.get("cohort_weeks", 0) or 0)),
            row.get("latest_effect", "") or "-",
            row.get("effect_w1", "") or "-",
            row.get("effect_w2", "") or "-",
            row.get("effect_w4", "") or "-",
            row.get("diagnosis", "") or "-",
            row.get("reason", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_tuning_summary", []) or [])
    ]
    feedback_threshold_override_rows = [
        [
            row.get("market", ""),
            row.get("feedback_kind_label", "") or "-",
            row.get("effective_state_label", "") or "-",
            row.get("tuning_label", "") or "-",
            (
                f"{float(row.get('auto_confidence', 0.0) or 0.0):.2f}/"
                f"{float(row.get('auto_base_confidence', 0.0) or 0.0):.2f}/"
                f"{float(row.get('auto_calibration_score', 0.0) or 0.0):.2f}/"
                f"{float(row.get('auto_maturity_ratio', 0.0) or 0.0):.2f}"
                if any(float(row.get(key, 0.0) or 0.0) > 0.0 for key in ("auto_confidence", "auto_base_confidence", "auto_calibration_score", "auto_maturity_ratio"))
                else "-"
            ),
            row.get("reason", "") or "-",
        ]
        for row in list(payload.get("feedback_threshold_override_overview", []) or [])
    ]
    market_data_health_rows = [
        [
            row.get("market", ""),
            str(int(row.get("portfolio_count", 0) or 0)),
            row.get("watchlists", "") or "-",
            row.get("status_label", "") or "-",
            f"{float(row.get('avg_data_quality_score', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_source_coverage', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_missing_ratio', 0.0) or 0.0):.2f}",
            (
                f"{int(row.get('ibkr_count', 0) or 0)}/"
                f"{int(row.get('yfinance_count', 0) or 0)}/"
                f"{int(row.get('missing_count', 0) or 0)}"
            ),
            (
                f"{row.get('diagnosis', '') or '-'}"
                + (
                    f" | {row.get('warning_summary', '')}"
                    if str(row.get("warning_summary", "") or "").strip()
                    else ""
                )
            ),
        ]
        for row in list(payload.get("market_data_health_overview", []) or [])
    ]
    ibkr_history_probe_rows = [
        [
            row.get("market", ""),
            row.get("status_label", "") or "-",
            str(int(row.get("sample_count", 0) or 0)),
            str(int(row.get("ok_count", 0) or 0)),
            str(int(row.get("permission_count", 0) or 0)),
            str(int(row.get("contract_count", 0) or 0)),
            str(int(row.get("empty_count", 0) or 0)),
            row.get("symbols", "") or "-",
            row.get("diagnosis", "") or "-",
        ]
        for row in list(dict(payload.get("ibkr_history_probe_summary", {}) or {}).get("market_summary", []) or [])
    ]
    labeling_skip_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("horizon_days", "") or "-",
            row.get("skip_reason_label", "") or row.get("skip_reason", "") or "-",
            str(int(row.get("skip_count", 0) or 0)),
            str(int(row.get("symbol_count", 0) or 0)),
            row.get("sample_symbols", "") or "-",
            f"{str(row.get('oldest_snapshot_ts', '') or '-')[:19]} -> {str(row.get('latest_snapshot_ts', '') or '-')[:19]}",
            (
                f"{int(_safe_float(row.get('min_remaining_forward_bars'), 0.0))}"
                f"-{int(_safe_float(row.get('max_remaining_forward_bars'), 0.0))}"
                if int(_safe_float(row.get("max_remaining_forward_bars"), 0.0)) > 0
                else "-"
            ),
            (
                f"{str(row.get('estimated_ready_start_ts', '') or '-')[:10]} -> "
                f"{str(row.get('estimated_ready_end_ts', '') or '-')[:10]}"
                if str(row.get("estimated_ready_end_ts", "") or "")
                else "-"
            ),
        ]
        for row in list(payload.get("labeling_skip_overview", []) or [])
    ]
    labeling_ready_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("horizon_days", "") or "-",
            str(int(row.get("skip_count", 0) or 0)),
            str(int(row.get("symbol_count", 0) or 0)),
            (
                f"{int(_safe_float(row.get('min_remaining_forward_bars'), 0.0))}"
                f"-{int(_safe_float(row.get('max_remaining_forward_bars'), 0.0))}"
                if int(_safe_float(row.get("max_remaining_forward_bars"), 0.0)) > 0
                else "-"
            ),
            str(int(row.get("days_until_ready", 0) or 0)),
            row.get("ready_bucket", "") or "-",
            f"{str(row.get('estimated_ready_start_ts', '') or '-')[:10]} -> {str(row.get('estimated_ready_end_ts', '') or '-')[:10]}",
        ]
        for row in list(payload.get("labeling_ready_overview", []) or [])
    ]
    risk_feedback_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("apply_mode_label", ""),
            row.get("risk_feedback_action", "") or "-",
            f"{float(row.get('feedback_base_confidence', 1.0) or 1.0):.2f}/{row.get('feedback_base_confidence_label', '') or '-'}",
            f"{float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{row.get('feedback_calibration_label', '') or '-'}",
            f"{float(row.get('feedback_confidence', 1.0) or 1.0):.2f}/{row.get('feedback_confidence_label', '') or '-'}",
            _fmt_budget_change(row.get("base_max_single_weight"), row.get("effective_max_single_weight")),
            _fmt_budget_change(row.get("base_max_net_exposure"), row.get("effective_max_net_exposure")),
            _fmt_budget_change(row.get("base_max_gross_exposure"), row.get("effective_max_gross_exposure")),
            _fmt_budget_change(row.get("base_correlation_soft_limit"), row.get("effective_correlation_soft_limit"), pct=False),
            row.get("effective_source_label", "") or "-",
            row.get("feedback_reason", "")[:96] or "-",
        ]
        for row in list(payload.get("risk_feedback_overview", []) or [])
    ]
    execution_feedback_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("apply_mode_label", ""),
            row.get("execution_feedback_action", "") or "-",
            f"{float(row.get('feedback_base_confidence', 1.0) or 1.0):.2f}/{row.get('feedback_base_confidence_label', '') or '-'}",
            f"{float(row.get('feedback_calibration_score', 0.5) or 0.5):.2f}/{row.get('feedback_calibration_label', '') or '-'}",
            f"{float(row.get('feedback_confidence', 1.0) or 1.0):.2f}/{row.get('feedback_confidence_label', '') or '-'}",
            _fmt_budget_change(row.get("base_adv_max_participation_pct"), row.get("effective_adv_max_participation_pct")),
            _fmt_budget_change(row.get("base_adv_split_trigger_pct"), row.get("effective_adv_split_trigger_pct")),
            f"{float(row.get('base_max_slices_per_symbol', 0.0) or 0.0):.0f}->{float(row.get('effective_max_slices_per_symbol', 0.0) or 0.0):.0f}",
            _fmt_budget_change(row.get("base_open_session_participation_scale"), row.get("effective_open_session_participation_scale"), pct=False),
            row.get("effective_source_label", "") or "-",
            row.get("apply_status_reason", "")[:96] or "-",
            row.get("feedback_reason", "")[:96] or "-",
        ]
        for row in list(payload.get("execution_feedback_overview", []) or [])
    ]
    execution_feedback_summary = dict(payload.get("execution_feedback_summary", {}) or {})
    execution_hotspot_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            row.get("symbol", ""),
            row.get("session_label", "") or "-",
            row.get("hotspot_action", "") or "-",
            _fmt_money(row.get("planned_execution_cost_total")),
            _fmt_money(row.get("execution_cost_total")),
            _fmt_money(row.get("execution_cost_gap")),
            f"{float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('pressure_score', 0.0) or 0.0):.2f}",
            row.get("reason", "")[:96] or "-",
        ]
        for row in list(payload.get("execution_hotspot_overview", []) or [])
    ]
    execution_cost_overview_rows = [
        [
            row.get("market", ""),
            row.get("watchlist", ""),
            row.get("portfolio_id", ""),
            _fmt_money(row.get("planned_execution_cost_total")),
            _fmt_money(row.get("execution_cost_total")),
            _fmt_money(row.get("execution_cost_gap")),
            f"{float(row.get('avg_expected_cost_bps', 0.0) or 0.0):.2f}",
            f"{float(row.get('avg_actual_slippage_bps', 0.0) or 0.0):.2f}",
            row.get("execution_style_breakdown", "") or "-",
            row.get("diagnosis", "")[:96] or "-",
        ]
        for row in list(payload.get("execution_cost_overview", []) or [])
    ]
    shadow_strategy_overview_card = f"""
    <section class="card overview">
      <h2>策略升级建议</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "action", "shadow_reviews", "near_miss", "far_below", "repeat_symbols", "symbols", "reason"], shadow_strategy_overview_rows)}
    </section>
    """ if shadow_strategy_overview_rows else """
    <section class="card overview">
      <h2>策略升级建议</h2>
      <div class="empty">当前没有周度 shadow review 建议。</div>
    </section>
    """
    risk_review_overview_card = f"""
    <section class="card overview">
      <h2>周度风险复盘</h2>
      {_render_table(["market", "watchlist", "portfolio_id", "driver", "net", "gross", "corr", "stress", "stress_loss", "diagnosis"], risk_review_overview_rows)}
    </section>
    """ if risk_review_overview_rows else """
    <section class="card overview">
      <h2>周度风险复盘</h2>
      <div class="empty">当前没有组合风险复盘数据。</div>
    </section>
    """
    trade_risk_alert_overview_card = f"""
    <section class="card overview">
      <h2>风险轨迹告警</h2>
      <div class="meta">这里聚合最近一段时间风险预算的变化方向，优先把持续收紧、stress 恶化或相关性抬升的组合提到最前面。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "source", "alert", "trend", "latest_ts", "scale", "scale_delta", "net", "gross", "corr", "stress", "stress_loss", "diagnosis"], trade_risk_alert_overview_rows)}
    </section>
    """ if trade_risk_alert_overview_rows else """
    <section class="card overview">
      <h2>风险轨迹告警</h2>
      <div class="empty">当前没有可展示的 trade 风险趋势告警。</div>
    </section>
    """
    execution_mode_recommendation_overview_card = f"""
    <section class="card overview">
      <h2>执行模式建议</h2>
      <div class="meta">这里根据最近风险轨迹给出保守建议，只做提示，不会自动替你切换执行模式。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "current", "recommended", "change", "alert", "trend", "alert_streak", "watch_streak", "reason"], execution_mode_recommendation_overview_rows)}
    </section>
    """ if execution_mode_recommendation_overview_rows else """
    <section class="card overview">
      <h2>执行模式建议</h2>
      <div class="empty">当前没有需要提示的执行模式建议。</div>
    </section>
    """
    feedback_automation_history_overview_card = f"""
    <section class="card overview">
      <h2>校准自动化历史趋势</h2>
      <div class="meta">这里看的是每周状态转移，便于识别哪些组合长期卡住、哪些组合已经从 SOON 进入 READY/ACTIVE。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "kind", "state", "mode", "transition", "same_weeks", "tracked", "maturity", "pending", "ready", "history"], feedback_automation_history_overview_rows)}
    </section>
    """ if feedback_automation_history_overview_rows else """
    <section class="card overview">
      <h2>校准自动化历史趋势</h2>
      <div class="empty">当前还没有可展示的校准自动化历史。</div>
    </section>
    """
    feedback_automation_stuck_overview_card = f"""
    <section class="card overview">
      <h2>长期卡住的校准</h2>
      <div class="meta">这里优先提示连续多周停在 `建议确认/继续观察` 的组合，帮助判断该继续等样本成熟，还是该回头复核阈值与门控。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "kind", "bucket", "state", "mode", "same_weeks", "tracked", "maturity", "pending", "ready", "reason"], feedback_automation_stuck_overview_rows)}
    </section>
    """ if feedback_automation_stuck_overview_rows else """
    <section class="card overview">
      <h2>长期卡住的校准</h2>
      <div class="empty">当前没有连续多周卡住的校准项。</div>
    </section>
    """
    feedback_automation_effect_overview_card = f"""
    <section class="card overview">
      <h2>自动应用后效果</h2>
      <div class="meta">这里只看已经进入 `ACTIVE/AUTO_APPLY` 的组合。`W+1 / W+2 / W+4` 都是相对自动应用起点周的事后效果，用每周保存下来的执行/风险/校准快照来比较。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "kind", "apply_week", "state", "mode", "active_weeks", "W+1", "W+2", "W+4", "latest", "metric", "driver", "reason"], feedback_automation_effect_overview_rows)}
    </section>
    """ if feedback_automation_effect_overview_rows else """
    <section class="card overview">
      <h2>自动应用后效果</h2>
      <div class="empty">当前还没有进入自动应用且可复盘效果的组合。</div>
    </section>
    """
    feedback_automation_effect_summary_card = f"""
    <section class="card overview">
      <h2>自动应用效果汇总</h2>
      <div class="meta">这里按市场和反馈类型汇总自动应用后的阶段效果，优先看哪些市场已经持续改善，哪些市场已经出现恶化，适合回头调 AUTO_APPLY 阈值。</div>
      {_render_table(["market", "kind", "signal", "tracked", "latest_up", "latest_down", "latest_flat", "latest_watch", "W+1 up", "W+2 up", "W+4 up", "W+1 down", "W+2 down", "W+4 down", "avg_weeks", "examples"], feedback_automation_effect_summary_rows)}
    </section>
    """ if feedback_automation_effect_summary_rows else """
    <section class="card overview">
      <h2>自动应用效果汇总</h2>
      <div class="empty">当前还没有足够的自动应用历史来汇总阶段效果。</div>
    </section>
    """
    feedback_threshold_suggestion_card = f"""
    <section class="card overview">
      <h2>分市场 AUTO_APPLY 阈值建议</h2>
      <div class="meta">这里先给出周报建议，不直接自动改 live 阈值。优先看哪些市场适合继续保守，哪些市场已经可以适度放宽 paper 的 AUTO_APPLY 门槛。</div>
      {_render_table(["market", "kind", "action", "signal", "tracked", "avg_weeks", "conf", "base_conf", "calib", "maturity", "examples", "reason"], feedback_threshold_suggestion_rows)}
    </section>
    """ if feedback_threshold_suggestion_rows else """
    <section class="card overview">
      <h2>分市场 AUTO_APPLY 阈值建议</h2>
      <div class="empty">当前还没有足够的自动应用效果历史来给出分市场阈值建议。</div>
    </section>
    """
    feedback_threshold_history_card = f"""
    <section class="card overview">
      <h2>阈值建议历史趋势</h2>
      <div class="meta">这里看的是分市场阈值建议最近几周的动作变化，帮助判断某个市场是在连续放宽、连续收紧，还是因为样本不稳定而反复切换。</div>
      {_render_table(["market", "kind", "current", "signal", "transition", "trend", "same_weeks", "tracked", "thresholds", "history", "reason"], feedback_threshold_history_rows)}
    </section>
    """ if feedback_threshold_history_rows else """
    <section class="card overview">
      <h2>阈值建议历史趋势</h2>
      <div class="empty">当前还没有可展示的分市场阈值建议历史。</div>
    </section>
    """
    feedback_threshold_effect_card = f"""
    <section class="card overview">
      <h2>阈值试运行效果</h2>
      <div class="meta">这里把当前阈值动作和市场级效果信号放在一起看，直接回答“放宽后是否变好、收紧后是否趋稳”。</div>
      {_render_table(["market", "kind", "current", "signal", "effect", "same_weeks", "tracked", "avg_weeks", "thresholds", "history", "next"], feedback_threshold_effect_rows)}
    </section>
    """ if feedback_threshold_effect_rows else """
    <section class="card overview">
      <h2>阈值试运行效果</h2>
      <div class="empty">当前还没有足够的分市场阈值试运行效果可复盘。</div>
    </section>
    """
    feedback_threshold_cohort_card = f"""
    <section class="card overview">
      <h2>阈值试运行 Cohort</h2>
      <div class="meta">这里按“同一阈值动作连续周数”追踪里程碑，直接看某个市场从开始放宽/收紧后的 W+1、W+2、W+4 表现。</div>
      {_render_table(["market", "kind", "cohort", "baseline", "weeks", "latest", "W+1", "W+2", "W+4", "history", "diagnosis"], feedback_threshold_cohort_rows)}
    </section>
    """ if feedback_threshold_cohort_rows else """
    <section class="card overview">
      <h2>阈值试运行 Cohort</h2>
      <div class="empty">当前还没有足够的阈值试运行 cohort 可跟踪。</div>
    </section>
    """
    execution_mode_banner_table_rows = "\n".join(
        f"""
        <tr class="execution-mode-banner-row" data-portfolio-id="{html.escape(str(row.get('portfolio_id', '') or ''))}" data-market="{html.escape(str(row.get('market', '') or ''))}" data-recommended-mode="{html.escape(str(row.get('recommended_mode_code', 'AUTO') or 'AUTO'))}">
          <td>{html.escape(str(row.get('market', '') or ''))}</td>
          <td>{html.escape(str(row.get('watchlist', '') or ''))}</td>
          <td>{html.escape(str(row.get('portfolio_id', '') or ''))}</td>
          <td data-cell="current">{html.escape(str(row.get('current_mode', '') or '-'))}</td>
          <td>{html.escape(str(row.get('recommended_mode', '') or '-'))}</td>
          <td>{html.escape(str(row.get('alert_level', '') or '-'))}</td>
          <td>{html.escape(str(row.get('trend_label', '') or '-'))}</td>
          <td>{html.escape(str(row.get('reason', '') or '-'))}</td>
        </tr>
        """
        for row in execution_mode_banner_rows
    )
    execution_mode_banner = f"""
    <section class="card overview recommendation-banner" id="execution-mode-banner">
      <h2>建议切换执行模式</h2>
      <div class="meta">只有当前模式和建议模式不一致的组合才会出现在这里，方便你优先处理真实交易风险。</div>
      <table>
        <thead>
          <tr>
            <th>market</th>
            <th>watchlist</th>
            <th>portfolio_id</th>
            <th>current</th>
            <th>recommended</th>
            <th>alert</th>
            <th>trend</th>
            <th>reason</th>
          </tr>
        </thead>
        <tbody>
          {execution_mode_banner_table_rows}
        </tbody>
      </table>
    </section>
    """ if execution_mode_banner_rows else ""
    feedback_threshold_trial_alert_card = f"""
    <section class="card overview recommendation-banner">
      <h2>分市场阈值试运行观察期</h2>
      <div class="meta">这里优先提示刚进入或仍处于早期观察期的阈值试运行市场，闭市周报刷新后会自动更新。</div>
      {_render_table(["market", "kind", "stage", "action", "baseline", "weeks", "latest", "W+1", "next_check", "diagnosis"], feedback_threshold_trial_alert_rows)}
    </section>
    """ if feedback_threshold_trial_alert_rows else ""
    feedback_threshold_tuning_card = f"""
    <section class="card overview">
      <h2>分市场阈值调参建议</h2>
      <div class="meta">这里把 cohort 试运行结果进一步翻译成更明确的调参方向，方便后续决定是继续放宽、收回放宽，还是继续保持收紧。</div>
      {_render_table(["market", "kind", "suggestion", "cohort", "baseline", "weeks", "latest", "W+1", "W+2", "W+4", "diagnosis", "reason"], feedback_threshold_tuning_rows)}
    </section>
    """ if feedback_threshold_tuning_rows else """
    <section class="card overview">
      <h2>分市场阈值调参建议</h2>
      <div class="empty">当前还没有足够的阈值 cohort 历史来给出更明确的调参建议。</div>
    </section>
    """
    feedback_threshold_override_card = f"""
    <section class="card overview">
      <h2>当前生效中的分市场阈值 Override</h2>
      <div class="meta">这里展示当前真正写入 weekly feedback override 文件的阈值状态，帮助区分“只是建议”还是“已经在 paper 生效”。数值依次为 conf/base/calib/maturity。</div>
      {_render_table(["market", "kind", "state", "tuning", "override", "reason"], feedback_threshold_override_rows)}
    </section>
    """ if feedback_threshold_override_rows else """
    <section class="card overview">
      <h2>当前生效中的分市场阈值 Override</h2>
      <div class="empty">当前还没有可展示的分市场阈值 override。</div>
    </section>
    """
    market_data_health_card = f"""
    <section class="card overview">
      <h2>市场数据健康总览</h2>
      <div class="meta">这里按市场聚合当前投资报告的数据质量与历史来源，帮助区分“IBKR 正常”与“主要依赖 fallback”，避免在数据底座不稳时过度调参。</div>
      {_render_table(["market", "portfolios", "watchlists", "status", "avg_score", "src_cov", "miss", "ibkr/yf/missing", "diagnosis"], market_data_health_rows)}
    </section>
    """ if market_data_health_rows else """
    <section class="card overview">
      <h2>市场数据健康总览</h2>
      <div class="empty">当前没有可展示的市场数据健康摘要。</div>
    </section>
    """
    ibkr_history_probe_card = f"""
    <section class="card overview">
      <h2>IBKR 历史接入诊断</h2>
      <div class="meta">这里读取只读历史探针的最新结果，帮助快速区分“权限问题”“合约问题”与“空历史”。</div>
      {_render_table(["market", "status", "sampled", "ok", "permission", "contract", "empty", "symbols", "diagnosis"], ibkr_history_probe_rows)}
    </section>
    """ if ibkr_history_probe_rows else """
    <section class="card overview">
      <h2>IBKR 历史接入诊断</h2>
      <div class="empty">当前还没有历史接入诊断结果；运行 probe 后这里会显示权限/合约/空历史的抽样结论。</div>
    </section>
    """
    trade_risk_history_overview_card = f"""
    <section class="card overview">
      <h2>近期风险轨迹</h2>
      <div class="meta">这里直接读取运行数据库里的风险预算历史，帮助解释最近几次为什么主动缩仓、放仓或降低集中度。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "ts", "source", "scale", "net", "gross", "corr", "stress", "stress_loss", "driver", "notes"], trade_risk_history_overview_rows)}
    </section>
    """ if trade_risk_history_overview_rows else """
    <section class="card overview">
      <h2>近期风险轨迹</h2>
      <div class="empty">当前没有可展示的 trade 风险轨迹。</div>
    </section>
    """
    dry_run_risk_history_overview_card = f"""
    <section class="card overview">
      <h2>近期风险轨迹</h2>
      <div class="meta">这里显示 dry-run 本地账本最近几次实际采用的风险预算，方便复盘策略升级前后的变化。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "ts", "source", "scale", "net", "gross", "corr", "stress", "stress_loss", "driver", "notes"], dry_run_risk_history_overview_rows)}
    </section>
    """ if dry_run_risk_history_overview_rows else """
    <section class="card overview">
      <h2>近期风险轨迹</h2>
      <div class="empty">当前没有可展示的 dry-run 风险轨迹。</div>
    </section>
    """
    dry_run_risk_alert_overview_card = f"""
    <section class="card overview">
      <h2>风险轨迹告警</h2>
      <div class="meta">这里帮助 dry-run 侧快速识别哪些组合最近在持续收紧风险预算，适合优先复盘阈值、分散度和资金利用率。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "source", "alert", "trend", "latest_ts", "scale", "scale_delta", "net", "gross", "corr", "stress", "stress_loss", "diagnosis"], dry_run_risk_alert_overview_rows)}
    </section>
    """ if dry_run_risk_alert_overview_rows else """
    <section class="card overview">
      <h2>风险轨迹告警</h2>
      <div class="empty">当前没有可展示的 dry-run 风险趋势告警。</div>
    </section>
    """
    feedback_calibration_overview_card = f"""
    <section class="card overview">
      <h2>结果校准</h2>
      <div class="meta">这里使用最近已回标的 candidate outcomes，校准 weekly feedback 的自动应用强度。`base_conf` 仍来自周报样本，`calib` 表示 outcome 对这类调参的支持度。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "scope", "horizon", "samples", "positive", "broken", "avg_return", "avg_drawdown", "score_align", "signal", "execution", "calib_conf", "reason"], feedback_calibration_overview_rows)}
    </section>
    """ if feedback_calibration_overview_rows else """
    <section class="card overview">
      <h2>结果校准</h2>
      <div class="empty">当前还没有足够的 outcome 回标样本来校准 weekly feedback。</div>
    </section>
    """
    feedback_automation_overview_card = f"""
    <section class="card overview">
      <h2>校准自动化</h2>
      <div class="meta">这里把 shadow ML、风险反馈和执行反馈统一成自动化模式。paper 只自动应用 `自动应用`；live 保留人工确认。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "kind", "action", "mode", "basis", "data_gate", "base_conf", "calib", "final_conf", "weekly_samples", "outcome_samples", "maturity", "pending|ready", "reason"], feedback_automation_overview_rows)}
    </section>
    """ if feedback_automation_overview_rows else """
    <section class="card overview">
      <h2>校准自动化</h2>
      <div class="empty">当前还没有可展示的校准自动化结论。</div>
    </section>
    """
    feedback_maturity_alert_overview_card = f"""
    <section class="card overview">
      <h2>接近自动应用的校准</h2>
      <div class="meta">这里优先提示“样本已较成熟”或“1-2 天内会成熟”的 feedback，方便你优先复核哪些组合最接近进入 AUTO_APPLY。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "kind", "mode", "bucket", "maturity", "pending", "days_until", "ready", "suggestion"], feedback_maturity_alert_rows)}
    </section>
    """ if feedback_maturity_alert_rows else """
    <section class="card overview">
      <h2>接近自动应用的校准</h2>
      <div class="empty">当前没有接近进入自动应用的校准项。</div>
    </section>
    """
    labeling_ready_overview_card = f"""
    <section class="card overview">
      <h2>即将成熟的 Outcome 样本</h2>
      <div class="meta">这里优先列出最接近形成 outcome 回标样本的组合，方便你判断明天最可能新增哪几组第三阶段校准输入。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "horizon", "skipped", "symbols", "remaining_bars", "days_until", "bucket", "ready_estimate"], labeling_ready_overview_rows)}
    </section>
    """ if labeling_ready_overview_rows else """
    <section class="card overview">
      <h2>即将成熟的 Outcome 样本</h2>
      <div class="empty">当前没有可预测成熟时间的 labeling 缺口。</div>
    </section>
    """
    labeling_summary = dict(payload.get("labeling_summary", {}) or {})
    labeling_skip_overview_card = f"""
    <section class="card overview">
      <h2>结果校准输入缺口</h2>
      <div class="meta">
        当前 snapshot labeling 汇总: labeled={int(labeling_summary.get("labeled_rows", 0) or 0)}
        / skipped={int(labeling_summary.get("skipped_rows", 0) or 0)}。
        这里帮助判断“为什么还没有 outcome 校准样本”，例如历史数据为空、前向样本不足，或快照时间落在历史覆盖之外。
      </div>
      {_render_table(["market", "watchlist", "portfolio_id", "horizon", "reason", "skipped", "symbols", "sample_symbols", "snapshot_window", "remaining_bars", "ready_estimate"], labeling_skip_overview_rows)}
    </section>
    """ if labeling_skip_overview_rows else f"""
    <section class="card overview">
      <h2>结果校准输入缺口</h2>
      <div class="empty">当前没有额外的 labeling 缺口记录。labeled={int(labeling_summary.get("labeled_rows", 0) or 0)} / skipped={int(labeling_summary.get("skipped_rows", 0) or 0)}。</div>
    </section>
    """
    risk_feedback_overview_card = f"""
    <section class="card overview">
      <h2>本周自动风险反馈</h2>
      <div class="meta">这里同时展示周报给出的风险预算建议，以及当前 paper 侧实际会采用的预算值；协作者不需要再单独打开 auto feedback yaml。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "apply_mode", "action", "base_conf", "calib", "final_conf", "single", "net", "gross", "corr_soft", "source", "reason"], risk_feedback_overview_rows)}
    </section>
    """ if risk_feedback_overview_rows else """
    <section class="card overview">
      <h2>本周自动风险反馈</h2>
      <div class="empty">当前没有新的自动风险反馈；本周沿用基础 paper 风险预算。</div>
    </section>
    """
    execution_feedback_overview_card = f"""
    <section class="card overview">
      <h2>本周自动执行反馈</h2>
      <div class="meta">这里展示周报给出的 execution 参数建议，以及当前 execution overlay 的实际或预估生效值。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "apply_mode", "action", "base_conf", "calib", "final_conf", "adv", "split_trigger", "slices", "open_scale", "source", "status", "reason"], execution_feedback_overview_rows)}
    </section>
    """ if execution_feedback_overview_rows else """
    <section class="card overview">
      <h2>本周自动执行反馈</h2>
      <div class="empty">当前没有新的执行参数反馈；本周沿用基础 execution 配置。</div>
    </section>
    """
    execution_feedback_summary_card = f"""
    <section class="card overview">
      <h2>第三阶段：自动执行校准进度</h2>
      <div class="meta">{html.escape(str(execution_feedback_summary.get("summary_text", "") or ""))}</div>
      <div class="stats">
        <div><strong>Total</strong><span>{int(execution_feedback_summary.get("total_count", 0) or 0)}</span></div>
        <div><strong>Auto Apply</strong><span>{int(execution_feedback_summary.get("auto_apply_count", 0) or 0)}</span></div>
        <div><strong>Suggest Only</strong><span>{int(execution_feedback_summary.get("suggest_only_count", 0) or 0)}</span></div>
        <div><strong>No Feedback</strong><span>{int(execution_feedback_summary.get("no_feedback_count", 0) or 0)}</span></div>
        <div><strong>No Data</strong><span>{int(execution_feedback_summary.get("no_data_count", 0) or 0)}</span></div>
        <div><strong>No Orders</strong><span>{int(execution_feedback_summary.get("no_order_count", 0) or 0)}</span></div>
        <div><strong>No Fills</strong><span>{int(execution_feedback_summary.get("no_fill_count", 0) or 0)}</span></div>
        <div><strong>Opp Gate</strong><span>{int(execution_feedback_summary.get("no_opportunity_count", 0) or 0)}</span></div>
        <div><strong>Quality Gate</strong><span>{int(execution_feedback_summary.get("no_quality_count", 0) or 0)}</span></div>
        <div><strong>Risk/Review</strong><span>{int(execution_feedback_summary.get("no_guard_count", 0) or 0)}</span></div>
        <div><strong>Liquidity</strong><span>{int(execution_feedback_summary.get("no_liquidity_count", 0) or 0)}</span></div>
        <div><strong>Policy Block</strong><span>{int(execution_feedback_summary.get("policy_block_count", 0) or 0)}</span></div>
        <div><strong>Predicted</strong><span>{int(execution_feedback_summary.get("predicted_count", 0) or 0)}</span></div>
        <div><strong>Overlay</strong><span>{int(execution_feedback_summary.get("overlay_count", 0) or 0)}</span></div>
        <div><strong>Tighten</strong><span>{int(execution_feedback_summary.get("tighten_count", 0) or 0)}</span></div>
        <div><strong>Relax/Decay</strong><span>{int(execution_feedback_summary.get("relax_count", 0) or 0) + int(execution_feedback_summary.get("decay_count", 0) or 0)}</span></div>
        <div><strong>Avg Base</strong><span>{float(execution_feedback_summary.get("avg_base_confidence", 0.0) or 0.0):.2f}</span></div>
        <div><strong>Avg Calib</strong><span>{float(execution_feedback_summary.get("avg_calibration_score", 0.0) or 0.0):.2f}</span></div>
        <div><strong>Avg Final</strong><span>{float(execution_feedback_summary.get("avg_confidence", 0.0) or 0.0):.2f}</span></div>
      </div>
      <div class="meta">这里优先看“paper 自动生效”和“为什么仍停在 base_only/suggest_only”；`Opp Gate / Quality Gate / Risk/Review / Liquidity` 会进一步告诉你主要卡在机会门、质量门、风险审核，还是流动性约束。</div>
    </section>
    """ if int(execution_feedback_summary.get("total_count", 0) or 0) > 0 else ""
    execution_hotspot_overview_card = f"""
    <section class="card overview">
      <h2>执行热点（symbol + session）</h2>
      <div class="meta">这里列出本周最值得优先排查的执行热点，帮助区分“是全局执行参数过激”，还是“少数标的在特定时段异常拖成本”。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "symbol", "session", "action", "plan_cost", "actual_cost", "cost_gap", "expected_bps", "actual_slippage_bps", "pressure", "reason"], execution_hotspot_overview_rows)}
    </section>
    """ if execution_hotspot_overview_rows else """
    <section class="card overview">
      <h2>执行热点（symbol + session）</h2>
      <div class="empty">当前没有明显的执行热点。</div>
    </section>
    """
    execution_cost_overview_card = f"""
    <section class="card overview">
      <h2>计划成本 vs 实际执行成本</h2>
      <div class="meta">plan_cost 来自执行计划；actual_cost 来自 fills + commission。两者差值越大，越值得优先复盘拆单、时段风格和成交质量。</div>
      {_render_table(["market", "watchlist", "portfolio_id", "plan_cost", "actual_cost", "cost_gap", "expected_bps", "actual_slippage_bps", "styles", "diagnosis"], execution_cost_overview_rows)}
    </section>
    """ if execution_cost_overview_rows else """
    <section class="card overview">
      <h2>计划成本 vs 实际执行成本</h2>
      <div class="empty">当前没有可展示的计划/实际执行成本对比数据。</div>
    </section>
    """
    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="60">
  <title>Trading Dashboard</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffdf8;
      --ink: #1f2a2d;
      --muted: #6b7477;
      --line: #d7d2c8;
      --accent: #1f5f5b;
      --warn: #8a5a00;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Georgia, "Times New Roman", serif; background: linear-gradient(180deg, #f7f2e8 0%, #ebe4d8 100%); color: var(--ink); }}
    .wrap {{ max-width: 1500px; margin: 0 auto; padding: 28px 20px 40px; }}
    h1 {{ margin: 0 0 8px; font-size: 32px; }}
    .sub {{ color: var(--muted); margin-bottom: 20px; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 18px; }}
    .toolbar button {{ border: 1px solid var(--line); background: #fffaf2; color: var(--ink); border-radius: 999px; padding: 8px 14px; cursor: pointer; }}
    .toolbar button.active {{ background: var(--accent); color: white; border-color: var(--accent); }}
    .control-toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 10px 0 4px; }}
    .control-toolbar button {{ border: 1px solid var(--line); background: #fffaf2; color: var(--ink); border-radius: 999px; padding: 8px 14px; cursor: pointer; }}
    .control-toolbar button.active {{ background: var(--accent); color: white; border-color: var(--accent); }}
    .control-toolbar button.recommended {{ box-shadow: inset 0 0 0 2px #b46a00; border-color: #b46a00; }}
    .control-toolbar button:disabled {{ opacity: 0.55; cursor: wait; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 18px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(63, 48, 23, 0.06); }}
    .overview {{ margin-bottom: 18px; }}
    .recommendation-banner {{ border-color: #b46a00; background: linear-gradient(180deg, #fff8ec 0%, #fffdf8 100%); }}
    .ops-banner-fail {{ border-color: #a63b2d; background: linear-gradient(180deg, #fff0ec 0%, #fffaf8 100%); }}
    .ops-banner-warn {{ border-color: #b46a00; background: linear-gradient(180deg, #fff8ec 0%, #fffdf8 100%); }}
    .card-control {{ margin-bottom: 16px; }}
    .focus-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-bottom: 18px; }}
    .focus-card {{ background: #fff9ef; border: 1px solid var(--line); border-radius: 16px; padding: 14px; }}
    .focus-top {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
    .focus-title {{ font-size: 15px; color: var(--muted); margin-bottom: 8px; }}
    .focus-action {{ font-size: 20px; font-weight: 700; margin-bottom: 6px; }}
    .focus-detail {{ font-size: 14px; color: var(--ink); }}
    .card-head {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; margin-bottom: 14px; }}
    .card h2 {{ margin: 0 0 6px; font-size: 24px; }}
    .meta {{ color: var(--muted); font-size: 14px; margin-bottom: 4px; }}
    .badge {{ display: inline-block; border-radius: 999px; padding: 3px 8px; font-size: 12px; }}
    .badge-action {{ background: #e7f2ef; color: var(--accent); }}
    .badge-market {{ background: #efe6d1; color: #6e4b00; }}
    .badge-mode {{ background: #e7ebf7; color: #334d8f; }}
    .badge-exec {{ background: #f3ebe2; color: #7a4f14; }}
    .badge-state {{ background: #e8f3de; color: #3d6b00; }}
    .stats {{ min-width: 320px; display: grid; grid-template-columns: repeat(2, minmax(140px, 1fr)); gap: 10px; }}
    .stats div {{ border: 1px solid var(--line); border-radius: 12px; padding: 10px 12px; background: #fcfaf4; }}
    .stats strong {{ display: block; font-size: 12px; letter-spacing: 0.04em; text-transform: uppercase; color: var(--muted); margin-bottom: 4px; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }}
    h3 {{ margin: 0 0 8px; font-size: 18px; }}
    table {{ width: 100%; border-collapse: collapse; border: 1px solid var(--line); background: white; }}
    th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 13px; }}
    th {{ background: #f6f1e8; color: var(--muted); font-weight: 600; }}
    .empty {{ border: 1px dashed var(--line); border-radius: 12px; padding: 12px; color: var(--warn); background: #fffaf0; }}
    @media (max-width: 980px) {{
      .card-head, .grid {{ grid-template-columns: 1fr; display: block; }}
      .stats {{ margin-top: 12px; }}
      .focus-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Market Dashboard</h1>
    <div class="sub">生成时间：{html.escape(str(payload.get("generated_at", "")))} | 60 秒自动刷新</div>
    <div class="toolbar">
      <button class="active" data-filter="trade">交易</button>
      <button data-filter="dry-run">Dry Run</button>
      <button data-filter="all">全部</button>
      <button data-filter="open">只看开市</button>
      <button data-filter="auto-submit">只看自动提交</button>
      <button data-filter="actionable">只看有动作</button>
      <button data-filter="stock-list">股票列表</button>
    </div>
    {control_panel}
    <div data-view="trade">
    {preflight_banner}
    {trade_banner}
    {ops_card}
    {market_data_health_card}
    {ibkr_history_probe_card}
    {execution_mode_summary_card}
    {execution_mode_banner}
    {feedback_threshold_trial_alert_card}
    <section class="card overview">
      <h2>今日最该关注的动作 / 研究</h2>
      <div class="focus-grid">
        {''.join(focus_cards) or '<div class="empty">当前没有高优先级动作。</div>'}
      </div>
    </section>
    {weekly_card}
    {weekly_group_card}
    {orphan_group_card}
    {review_overview_card}
    {shadow_review_overview_card}
    {shadow_strategy_overview_card}
    {labeling_ready_overview_card}
    {feedback_calibration_overview_card}
    {feedback_maturity_alert_overview_card}
    {feedback_automation_overview_card}
    {feedback_automation_history_overview_card}
    {feedback_automation_stuck_overview_card}
    {feedback_automation_effect_summary_card}
    {feedback_threshold_suggestion_card}
    {feedback_threshold_override_card}
    {feedback_threshold_history_card}
    {feedback_threshold_effect_card}
    {feedback_threshold_cohort_card}
    {feedback_threshold_tuning_card}
    {feedback_automation_effect_overview_card}
    {labeling_skip_overview_card}
    {risk_review_overview_card}
    {trade_risk_alert_overview_card}
    {execution_mode_recommendation_overview_card}
    {trade_risk_history_overview_card}
    {risk_feedback_overview_card}
    {execution_feedback_summary_card}
    {execution_feedback_overview_card}
    {execution_hotspot_overview_card}
    {execution_cost_overview_card}
    <section class="card overview">
      <h2>IBKR 健康状态</h2>
      {_render_table(["market", "watchlist", "status", "detail", "delayed", "perm", "breaks", "acct_limit", "latest_event", "latest_ts"], health_rows)}
    </section>
    <section class="card overview">
      <h2>市场总览</h2>
      {_render_table(["market", "watchlist", "mode", "open", "priority", "recommended_action", "detail", "account_equity", "account_cash", "ibkr_health", "entry_now", "wait", "exec_orders"], overview_rows)}
    </section>
    {trade_cards_html or '<div class="empty">当前没有可展示的交易页面报告。</div>'}
    </div>
    <div data-view="dry-run" style="display:none;">
    {dry_run_banner}
    {market_data_health_card}
    {ibkr_history_probe_card}
    {dry_run_overview_card}
    {dry_run_attribution_card}
    {labeling_ready_overview_card}
    {feedback_calibration_overview_card}
    {feedback_maturity_alert_overview_card}
    {feedback_automation_overview_card}
    {feedback_automation_history_overview_card}
    {feedback_automation_stuck_overview_card}
    {feedback_automation_effect_summary_card}
    {feedback_threshold_suggestion_card}
    {feedback_threshold_override_card}
    {feedback_threshold_history_card}
    {feedback_threshold_effect_card}
    {feedback_threshold_cohort_card}
    {feedback_threshold_tuning_card}
    {feedback_automation_effect_overview_card}
    {labeling_skip_overview_card}
    {dry_run_risk_alert_overview_card}
    {dry_run_risk_history_overview_card}
    {risk_feedback_overview_card}
    {dry_run_cards_html or '<div class="empty">当前没有可展示的 dry-run 页面数据。</div>'}
    </div>
    {stock_list_card}
  </div>
  <script>
    const filterButtons = Array.from(document.querySelectorAll('.toolbar button'));
    const tradeCards = Array.from(document.querySelectorAll('.card[data-open][data-dashboard-view="trade"]'));
    const tradeSections = Array.from(document.querySelectorAll('[data-view="trade"]'));
    const dryRunSections = Array.from(document.querySelectorAll('[data-view="dry-run"]'));
    const stockListSections = Array.from(document.querySelectorAll('[data-view="stock-list"]'));
    const controlRoot = document.getElementById('dashboard-control');
    const controlStatus = document.getElementById('control-status');
    const controlActions = Array.from(document.querySelectorAll('.control-action'));
    const controlWeeklyFeedbackButtons = Array.from(document.querySelectorAll('.control-weekly-feedback'));
    const controlToggles = Array.from(document.querySelectorAll('.control-toggle'));
    const controlModes = Array.from(document.querySelectorAll('.control-mode'));
    const executionModeMarketButtons = Array.from(document.querySelectorAll('.execution-mode-market-filter'));
    const executionModeCurrentLabels = Array.from(document.querySelectorAll('.execution-mode-current'));
    const executionModeRecommendedLabels = Array.from(document.querySelectorAll('.execution-mode-recommended'));
    const executionModeChangeLabels = Array.from(document.querySelectorAll('.execution-mode-change'));
    const executionModeSummaryCard = document.getElementById('execution-mode-summary');
    const executionModeSummaryText = document.getElementById('execution-mode-summary-text');
    const executionModeSummaryMismatch = document.getElementById('execution-mode-summary-mismatch');
    const executionModeSummaryReviewOnly = document.getElementById('execution-mode-summary-review-only');
    const executionModeSummaryPaused = document.getElementById('execution-mode-summary-paused');
    const executionModeSummaryMarketBody = document.getElementById('execution-mode-summary-market-body');
    const executionModeMarketFilterLabel = document.getElementById('execution-mode-market-filter-label');
    const executionModeMarketFilterClear = document.getElementById('execution-mode-market-filter-clear');
    const executionModeBanner = document.getElementById('execution-mode-banner');
    const executionModeBannerRows = Array.from(document.querySelectorAll('.execution-mode-banner-row'));
    const controlUrl = controlRoot ? (controlRoot.dataset.controlUrl || '') : '';
    const executionModeMarketFilterStorageKey = 'dashboard.executionModeMarketFilter';
    const executionModeHashViewKey = 'view';
    const executionModeHashMarketKey = 'alert_market';
    const executionModeLabelMap = {{
      AUTO: '自动执行',
      REVIEW_ONLY: '只保留人工审核',
      PAUSED: '暂停自动执行',
    }};
    let currentFilterKind = 'trade';
    let executionModeMarketFilter = '';
    const persistExecutionModeMarketFilter = () => {{
      try {{
        if (!window.localStorage) return;
        if (executionModeMarketFilter) {{
          window.localStorage.setItem(executionModeMarketFilterStorageKey, executionModeMarketFilter);
        }} else {{
          window.localStorage.removeItem(executionModeMarketFilterStorageKey);
        }}
      }} catch (error) {{
        // 本地存储失败时只跳过记忆能力，不影响 dashboard 正常使用。
      }}
    }};
    const loadExecutionModeMarketFilter = () => {{
      try {{
        if (!window.localStorage) return '';
        return window.localStorage.getItem(executionModeMarketFilterStorageKey) || '';
      }} catch (error) {{
        // 某些浏览器环境可能禁用 localStorage，这里直接回退到默认筛选。
        return '';
      }}
    }};
    const loadDashboardHashState = () => {{
      try {{
        const hashText = String(window.location.hash || '').replace(/^#/, '');
        if (!hashText) return {{}};
        const params = new URLSearchParams(hashText);
        return {{
          filterKind: params.get(executionModeHashViewKey) || '',
          marketFilter: params.get(executionModeHashMarketKey) || '',
        }};
      }} catch (error) {{
        // hash 解析失败时直接忽略，避免影响 dashboard 主流程。
        return {{}};
      }}
    }};
    const syncDashboardHashState = () => {{
      try {{
        const params = new URLSearchParams();
        if (currentFilterKind && currentFilterKind !== 'trade') {{
          params.set(executionModeHashViewKey, currentFilterKind);
        }}
        if (executionModeMarketFilter) {{
          params.set(executionModeHashMarketKey, executionModeMarketFilter);
        }}
        const nextHash = params.toString();
        const baseUrl = `${{window.location.pathname}}${{window.location.search}}`;
        const nextUrl = nextHash ? `${{baseUrl}}#${{nextHash}}` : baseUrl;
        window.history.replaceState(null, '', nextUrl);
      }} catch (error) {{
        // URL 状态同步失败时只丢失可分享视角，不影响页面使用。
      }}
    }};
    const updateExecutionModeMarketButtons = () => {{
      executionModeMarketButtons.forEach((btn) => {{
        btn.classList.toggle('active', (btn.dataset.marketFilter || '') === executionModeMarketFilter);
      }});
      if (executionModeMarketFilterLabel) {{
        // 这里直接展示当前告警筛选，避免刷新后看不出页面为什么只剩某个市场。
        executionModeMarketFilterLabel.textContent = `当前告警市场筛选：${{executionModeMarketFilter || '全部'}}`;
      }}
      if (executionModeMarketFilterClear) {{
        executionModeMarketFilterClear.style.display = executionModeMarketFilter ? '' : 'none';
      }}
    }};
    const applyFilter = (kind) => {{
      currentFilterKind = kind;
      updateExecutionModeMarketButtons();
      filterButtons.forEach((btn) => btn.classList.toggle('active', btn.dataset.filter === kind));
      const tradeMode = kind === 'trade' || kind === 'open' || kind === 'auto-submit' || kind === 'actionable';
      const dryRunMode = kind === 'dry-run';
      const stockListMode = kind === 'stock-list';
      const allMode = kind === 'all';
      tradeSections.forEach((section) => {{
        section.style.display = tradeMode || allMode ? '' : 'none';
      }});
      dryRunSections.forEach((section) => {{
        section.style.display = dryRunMode || allMode ? '' : 'none';
      }});
      stockListSections.forEach((section) => {{
        section.style.display = stockListMode || allMode ? '' : 'none';
      }});
      if (dryRunMode || stockListMode) {{
        tradeCards.forEach((card) => {{
          card.style.display = '';
        }});
        return;
      }}
      tradeCards.forEach((card) => {{
        let visible = true;
        if (kind === 'open') visible = card.dataset.open === 'true';
        if (kind === 'auto-submit') visible = card.dataset.mode.endsWith('-auto-submit');
        if (kind === 'actionable') visible = card.dataset.actionable === 'true';
        if (visible && executionModeMarketFilter) {{
          visible = card.dataset.market === executionModeMarketFilter && card.dataset.executionModeChange === 'true';
        }}
        card.style.display = visible ? '' : 'none';
      }});
      syncDashboardHashState();
    }};
    const updateControlUi = (state) => {{
      if (!state) return;
      const service = state.service || {{}};
      const actions = state.actions || {{}};
      const portfolios = state.portfolios || {{}};
      if (controlStatus) {{
        controlStatus.textContent = `service=${{service.status || 'unknown'}} | endpoint=${{service.url || controlUrl || '-'}} | last_action=${{actions.last_action || '-'}} | last_error=${{actions.last_error || '-'}}`;
      }}
      const busy = !!actions.run_once_in_progress || !!actions.preflight_in_progress || !!actions.weekly_review_in_progress;
      controlActions.forEach((btn) => {{
        btn.disabled = busy && (
          btn.dataset.apiAction === 'run_once'
          || btn.dataset.apiAction === 'run_preflight'
          || btn.dataset.apiAction === 'run_weekly_review'
        );
      }});
      controlToggles.forEach((btn) => {{
        const row = portfolios[btn.dataset.portfolioId] || {{}};
        if (!Object.prototype.hasOwnProperty.call(row, btn.dataset.field)) return;
        const value = !!row[btn.dataset.field];
        btn.dataset.value = value ? 'true' : 'false';
        btn.classList.toggle('active', value);
        btn.textContent = `${{btn.dataset.label}}: ${{value ? 'ON' : 'OFF'}}`;
      }});
      controlModes.forEach((btn) => {{
        const row = portfolios[btn.dataset.portfolioId] || {{}};
        const mode = row.execution_control_mode || 'AUTO';
        const recommendedMode = btn.dataset.recommendedMode || 'AUTO';
        btn.disabled = busy;
        btn.classList.toggle('active', mode === btn.dataset.modeValue);
        btn.classList.toggle('recommended', recommendedMode === btn.dataset.modeValue && recommendedMode !== mode);
      }});
      controlWeeklyFeedbackButtons.forEach((btn) => {{
        const row = portfolios[btn.dataset.portfolioId] || {{}};
        const pending = !!row.weekly_feedback_pending_live_confirm;
        btn.disabled = busy || !pending;
        btn.style.display = pending ? '' : 'none';
      }});
      executionModeCurrentLabels.forEach((node) => {{
        const row = portfolios[node.dataset.portfolioId] || {{}};
        const mode = row.execution_control_mode || 'AUTO';
        node.textContent = executionModeLabelMap[mode] || mode;
      }});
      executionModeRecommendedLabels.forEach((node) => {{
        node.textContent = node.dataset.recommendedLabel || '自动执行';
      }});
      executionModeChangeLabels.forEach((node) => {{
        const row = portfolios[node.dataset.portfolioId] || {{}};
        const mode = row.execution_control_mode || 'AUTO';
        const recommendedMode = node.dataset.recommendedMode || 'AUTO';
        node.textContent = recommendedMode !== mode ? 'YES' : 'NO';
      }});
      executionModeBannerRows.forEach((rowNode) => {{
        const row = portfolios[rowNode.dataset.portfolioId] || {{}};
        const mode = row.execution_control_mode || 'AUTO';
        const recommendedMode = rowNode.dataset.recommendedMode || 'AUTO';
        const currentCell = rowNode.querySelector('[data-cell="current"]');
        if (currentCell) {{
          currentCell.textContent = executionModeLabelMap[mode] || mode;
        }}
        rowNode.style.display = recommendedMode !== mode ? '' : 'none';
      }});
      if (executionModeBanner) {{
        const visibleBannerRows = executionModeBannerRows.filter((rowNode) => rowNode.style.display !== 'none');
        executionModeBanner.style.display = visibleBannerRows.length ? '' : 'none';
        const visibleMarkets = new Set(visibleBannerRows.map((rowNode) => rowNode.dataset.market || '-'));
        executionModeMarketButtons.forEach((btn) => {{
          const market = btn.dataset.marketFilter || '';
          btn.style.display = (!market || visibleMarkets.has(market)) ? '' : 'none';
        }});
        if (executionModeMarketFilter && !visibleMarkets.has(executionModeMarketFilter)) {{
          executionModeMarketFilter = '';
          persistExecutionModeMarketFilter();
        }}
        if (executionModeSummaryCard) {{
          let reviewOnlyCount = 0;
          let pausedCount = 0;
          const marketStats = {{}};
          visibleBannerRows.forEach((rowNode) => {{
            const recommendedMode = rowNode.dataset.recommendedMode || 'AUTO';
            const market = rowNode.dataset.market || '-';
            if (!marketStats[market]) {{
              marketStats[market] = {{ mismatch: 0, reviewOnly: 0, paused: 0 }};
            }}
            marketStats[market].mismatch += 1;
            if (recommendedMode === 'REVIEW_ONLY') reviewOnlyCount += 1;
            if (recommendedMode === 'PAUSED') pausedCount += 1;
            if (recommendedMode === 'REVIEW_ONLY') marketStats[market].reviewOnly += 1;
            if (recommendedMode === 'PAUSED') marketStats[market].paused += 1;
          }});
          const mismatchCount = visibleBannerRows.length;
          executionModeSummaryCard.style.display = mismatchCount ? '' : 'none';
          if (executionModeSummaryMismatch) executionModeSummaryMismatch.textContent = String(mismatchCount);
          if (executionModeSummaryReviewOnly) executionModeSummaryReviewOnly.textContent = String(reviewOnlyCount);
          if (executionModeSummaryPaused) executionModeSummaryPaused.textContent = String(pausedCount);
          if (executionModeSummaryMarketBody) {{
            const marketRows = Object.entries(marketStats)
              .sort((a, b) => {{
                const pausedDiff = (b[1].paused || 0) - (a[1].paused || 0);
                if (pausedDiff !== 0) return pausedDiff;
                const reviewDiff = (b[1].reviewOnly || 0) - (a[1].reviewOnly || 0);
                if (reviewDiff !== 0) return reviewDiff;
                return String(a[0]).localeCompare(String(b[0]));
              }})
              .map(([market, stats]) => `<tr><td>${{market}}</td><td>${{stats.mismatch}}</td><td>${{stats.reviewOnly}}</td><td>${{stats.paused}}</td></tr>`)
              .join('');
            executionModeSummaryMarketBody.innerHTML = marketRows;
          }}
          if (executionModeSummaryText) {{
            executionModeSummaryText.textContent = mismatchCount
              ? `${{mismatchCount}} 个组合建议切换：${{reviewOnlyCount}} 个建议 只保留人工审核，${{pausedCount}} 个建议 暂停自动执行`
              : '当前执行模式与风险建议一致';
          }}
        }}
      }}
      applyFilter(currentFilterKind);
    }};
    const fetchControlState = async () => {{
      if (!controlUrl) return null;
      try {{
        const response = await fetch(`${{controlUrl}}/state`);
        const data = await response.json();
        updateControlUi(data);
        return data;
      }} catch (error) {{
        if (controlStatus) {{
          controlStatus.textContent = `service=unreachable | endpoint=${{controlUrl}} | error=${{error.message}}`;
        }}
        return null;
      }}
    }};
    const postControl = async (path, payload) => {{
      if (!controlUrl) return null;
      const response = await fetch(`${{controlUrl}}${{path}}`, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload || {{}}),
      }});
      const data = await response.json();
      if (data && (data.service || data.portfolios || data.actions)) {{
        updateControlUi(data);
      }}
      return data;
    }};
    filterButtons.forEach((btn) => btn.addEventListener('click', () => applyFilter(btn.dataset.filter)));
    controlActions.forEach((btn) => btn.addEventListener('click', async () => {{
      btn.disabled = true;
      try {{
        if (btn.dataset.apiAction === 'run_once') {{
          await postControl('/run_once', {{}});
          window.setTimeout(fetchControlState, 1200);
          window.setTimeout(fetchControlState, 4000);
        }} else if (btn.dataset.apiAction === 'run_preflight') {{
          await postControl('/run_preflight', {{}});
          window.setTimeout(fetchControlState, 1200);
          window.setTimeout(fetchControlState, 4000);
        }} else if (btn.dataset.apiAction === 'run_weekly_review') {{
          await postControl('/run_weekly_review', {{}});
          window.setTimeout(fetchControlState, 1200);
          window.setTimeout(fetchControlState, 4000);
        }} else if (btn.dataset.apiAction === 'refresh_dashboard') {{
          await postControl('/refresh_dashboard', {{}});
          window.setTimeout(fetchControlState, 800);
        }}
      }} catch (error) {{
        if (controlStatus) {{
          controlStatus.textContent = `service=unreachable | endpoint=${{controlUrl}} | error=${{error.message}}`;
        }}
      }} finally {{
        btn.disabled = false;
      }}
    }}));
    controlToggles.forEach((btn) => btn.addEventListener('click', async () => {{
      const nextValue = !(btn.dataset.value === 'true');
      btn.disabled = true;
      try {{
        await postControl('/toggle_flag', {{
          portfolio_id: btn.dataset.portfolioId,
          field: btn.dataset.field,
          value: nextValue,
        }});
        window.setTimeout(fetchControlState, 400);
      }} catch (error) {{
        if (controlStatus) {{
          controlStatus.textContent = `service=unreachable | endpoint=${{controlUrl}} | error=${{error.message}}`;
        }}
      }} finally {{
        btn.disabled = false;
      }}
    }}));
    controlModes.forEach((btn) => btn.addEventListener('click', async () => {{
      btn.disabled = true;
      try {{
        await postControl('/set_execution_mode', {{
          portfolio_id: btn.dataset.portfolioId,
          mode: btn.dataset.modeValue,
        }});
        window.setTimeout(fetchControlState, 400);
      }} catch (error) {{
        if (controlStatus) {{
          controlStatus.textContent = `service=unreachable | endpoint=${{controlUrl}} | error=${{error.message}}`;
        }}
      }} finally {{
        btn.disabled = false;
      }}
    }}));
    controlWeeklyFeedbackButtons.forEach((btn) => btn.addEventListener('click', async () => {{
      btn.disabled = true;
      try {{
        await postControl('/apply_weekly_feedback', {{
          portfolio_id: btn.dataset.portfolioId,
        }});
        window.setTimeout(fetchControlState, 400);
      }} catch (error) {{
        if (controlStatus) {{
          controlStatus.textContent = `service=unreachable | endpoint=${{controlUrl}} | error=${{error.message}}`;
        }}
      }} finally {{
        btn.disabled = false;
      }}
    }}));
    executionModeMarketButtons.forEach((btn) => btn.addEventListener('click', () => {{
      executionModeMarketFilter = btn.dataset.marketFilter || '';
      persistExecutionModeMarketFilter();
      applyFilter('trade');
    }}));
    if (executionModeMarketFilterClear) {{
      executionModeMarketFilterClear.addEventListener('click', () => {{
        executionModeMarketFilter = '';
        persistExecutionModeMarketFilter();
        applyFilter('trade');
      }});
    }}
    const availableFilterKinds = new Set(filterButtons.map((btn) => btn.dataset.filter || ''));
    const availableExecutionModeMarkets = new Set(executionModeMarketButtons.map((btn) => btn.dataset.marketFilter || ''));
    const restoreDashboardState = (useLocalStorageFallback) => {{
      const restoredHashState = loadDashboardHashState();
      if (restoredHashState.filterKind && availableFilterKinds.has(restoredHashState.filterKind)) {{
        currentFilterKind = restoredHashState.filterKind;
      }} else {{
        currentFilterKind = 'trade';
      }}
      let nextMarketFilter = '';
      if (restoredHashState.marketFilter) {{
        nextMarketFilter = restoredHashState.marketFilter;
      }} else if (useLocalStorageFallback) {{
        nextMarketFilter = loadExecutionModeMarketFilter();
      }}
      executionModeMarketFilter = availableExecutionModeMarkets.has(nextMarketFilter) ? nextMarketFilter : '';
      // 让本地记忆与当前页面真实视角保持一致，避免刷新后回到旧筛选。
      persistExecutionModeMarketFilter();
    }};
    window.addEventListener('hashchange', () => {{
      restoreDashboardState(false);
      applyFilter(currentFilterKind);
    }});
    restoreDashboardState(true);
    applyFilter(currentFilterKind);
    if (controlRoot) {{
      fetchControlState();
      window.setInterval(fetchControlState, 5000);
    }}
  </script>
</body>
</html>
"""
    (out / "dashboard.html").write_text(html_text, encoding="utf-8")


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    payload = build_dashboard(args.config, args.out_dir)
    write_dashboard(payload, args.out_dir)
    out = _resolve_path(args.out_dir)
    print(f"dashboard_json={out / 'dashboard.json'}")
    print(f"dashboard_html={out / 'dashboard.html'}")


if __name__ == "__main__":
    main()
