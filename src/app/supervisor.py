from __future__ import annotations

import argparse
import copy
import signal
import subprocess
import sys
import time
import json
import hashlib
import threading
import webbrowser
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import yaml

from ..common.logger import get_logger
from ..common.markets import market_config_path, market_timezone_name, resolve_market_code
from ..common.runtime_paths import resolve_repo_path, resolve_scoped_runtime_path, scope_from_ibkr_config
from ..enrichment.providers import EnrichmentProviders
from .dashboard_control import DashboardControlService
from .supervisor_support import (
    clamp_float as _clamp_float,
    feedback_confidence_value as _feedback_confidence_value,
    in_window as _in_window,
    merge_execution_feedback_penalties as _merge_execution_feedback_penalties,
    parse_feedback_penalty_rows as _parse_feedback_penalty_rows,
    past_time as _past_time,
    scale_feedback_delta as _scale_feedback_delta,
    scale_feedback_penalty_rows as _scale_feedback_penalty_rows,
)
from ..tools.preflight_supervisor import run_preflight

log = get_logger("app.supervisor")
BASE_DIR = Path(__file__).resolve().parents[2]
CONTROL_FLAG_FIELDS = {
    "run_investment_paper",
    "force_local_paper_ledger",
    "run_investment_execution",
    "submit_investment_execution",
    "run_investment_guard",
    "submit_investment_guard",
    "run_investment_opportunity",
}
EXECUTION_CONTROL_FIELDS = {
    "run_investment_execution",
    "submit_investment_execution",
    "run_investment_guard",
    "submit_investment_guard",
}
EXECUTION_CONTROL_MODE_VALUES = {"AUTO", "REVIEW_ONLY", "PAUSED"}


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the project supervisor for scheduled report and paper tasks.")
    ap.add_argument("--config", default="config/supervisor.yaml", help="Path to supervisor yaml config.")
    ap.add_argument("--once", action="store_true", default=False, help="Run exactly one scheduler cycle at the current time and exit.")
    return ap.parse_args(argv)


def _resolve_path(path_str: str) -> Path:
    return resolve_repo_path(BASE_DIR, path_str)


def _load_yaml(path: str) -> Dict[str, Any]:
    resolved = _resolve_path(path)
    with resolved.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _slugify_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "default"


@dataclass
class ManagedProcess:
    name: str
    cmd: List[str]
    process: Optional[subprocess.Popen[str]] = None

    def start(self) -> None:
        if self.process and self.process.poll() is None:
            return
        log.info(f"Starting process {self.name}: {' '.join(self.cmd)}")
        self.process = subprocess.Popen(self.cmd, cwd=str(BASE_DIR), text=True)

    def stop(self) -> None:
        proc = self.process
        if proc is None:
            return
        if proc.poll() is not None:
            self.process = None
            return
        log.info(f"Stopping process {self.name}")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        finally:
            self.process = None

    def ensure_running(self) -> None:
        proc = self.process
        if proc is None or proc.poll() is not None:
            if proc is not None:
                log.warning(f"Process {self.name} exited with code {proc.returncode}; restarting")
                self.process = None
            self.start()


@dataclass
class MarketRuntime:
    name: str
    market_code: str
    ibkr_config: str
    local_timezone: str
    watchlists: List[Dict[str, Any]]
    reports: List[Dict[str, Any]]
    trading: Dict[str, Any]
    short_safety_sync: Dict[str, Any]
    watchlist_refresh_time: str
    report_time: str
    enabled: bool = True
    last_watchlist_day: str = ""
    last_report_day: str = ""
    last_short_safety_sync_day: str = ""
    last_short_safety_sync_attempt_ts: float = 0.0


class Supervisor:
    def __init__(self, config_path: str = "config/supervisor.yaml"):
        self.config_path = config_path
        self.cfg = _load_yaml(config_path)
        self.tz = ZoneInfo(str(self.cfg.get("timezone", "Australia/Sydney")))
        self.poll_sec = int(self.cfg.get("poll_sec", 30))
        self._stopping = False
        self.markets = self._load_markets()
        self.holiday_cfg = _load_yaml(str(self.cfg.get("market_holidays_config", "config/market_holidays.yaml")))
        self.trade_proc = ManagedProcess("trade-engine", [sys.executable, "-m", "src.main"])
        self._active_market: Optional[str] = None
        self._macro_signature_cache: Dict[str, tuple[float, str]] = {}
        self._last_cycle_summary_signature: str = ""
        self._last_market_summary_signatures: Dict[str, str] = {}
        self._dashboard_opened_once = False
        self._runtime_scope_cache: Dict[str, Any] = {}
        self._cycle_running = False
        self._dashboard_control_lock = threading.Lock()
        self._dashboard_control_service: Optional[DashboardControlService] = None
        self._dashboard_control_run_once_in_progress = False
        self._dashboard_control_preflight_in_progress = False
        self._dashboard_control_weekly_review_in_progress = False
        self._dashboard_control_last_action = ""
        self._dashboard_control_last_action_ts = ""
        self._dashboard_control_last_error = ""
        self._capture_dashboard_control_baselines()
        self._apply_dashboard_control_overrides()

    def _primary_runtime_scope(self):
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                return self._runtime_scope_for(item, market.market_code)
        return None

    def _summary_output_dir(self) -> Path:
        raw_dir = str(self.cfg.get("summary_out_dir", "reports_supervisor"))
        if not bool(self.cfg.get("scope_summary_out_dir", False)):
            return (BASE_DIR / raw_dir).resolve()
        scope = self._primary_runtime_scope()
        if scope is None:
            return (BASE_DIR / raw_dir).resolve()
        return resolve_scoped_runtime_path(BASE_DIR, raw_dir, scope)

    def _dashboard_db_path(self) -> Path:
        raw_path = str(self.cfg.get("dashboard_db", "audit.db") or "audit.db")
        if Path(raw_path).is_absolute():
            return Path(raw_path).resolve()
        scope = self._primary_runtime_scope()
        if scope is None:
            return (BASE_DIR / raw_path).resolve()
        return resolve_scoped_runtime_path(BASE_DIR, raw_path, scope)

    def _weekly_review_output_dir(self) -> Path:
        raw_dir = str(self.cfg.get("dashboard_weekly_review_dir", "reports_investment_weekly") or "reports_investment_weekly")
        return _resolve_path(raw_dir)

    def _labeling_output_dir(self) -> Path:
        raw_dir = str(
            self.cfg.get("dashboard_labeling_dir", self.cfg.get("labeling_out_dir", "reports_investment_labeling"))
            or "reports_investment_labeling"
        )
        return _resolve_path(raw_dir)

    def _preflight_output_dir(self) -> Path:
        raw_dir = str(self.cfg.get("dashboard_preflight_dir", "reports_preflight") or "reports_preflight")
        return _resolve_path(raw_dir)

    def _labeling_enabled(self) -> bool:
        return bool(self.cfg.get("run_investment_labeling", True))

    def _labeling_due(self, now: datetime) -> tuple[bool, str]:
        if not self._labeling_enabled():
            return False, "disabled"
        if bool(self.cfg.get("labeling_only_when_all_markets_closed", True)):
            open_markets = [market.market_code for market in self.markets if market.enabled and self._market_exchange_open(market, now)]
            if open_markets:
                return False, f"markets_open:{','.join(open_markets)}"
        run_time = str(self.cfg.get("labeling_time", "") or "").strip()
        local_now = now.astimezone(self.tz)
        if run_time and not _past_time(local_now, run_time):
            return False, "before_labeling_time"
        interval_min = max(1, int(self.cfg.get("labeling_interval_min", 180) or 180))
        marker = self._labeling_output_dir() / "all" / "investment_candidate_outcomes_summary.json"
        if marker.exists():
            age_sec = local_now.timestamp() - marker.stat().st_mtime
            if age_sec >= 0 and age_sec < (interval_min * 60):
                return False, "labeling_interval_not_elapsed"
        return True, "due"

    def _run_investment_labeling(self, now: datetime, *, force: bool = False) -> bool:
        due, reason = self._labeling_due(now)
        if force:
            due = True
            reason = "forced"
        if not due:
            return False
        cmd = [
            sys.executable,
            "-m",
            "src.tools.label_investment_snapshots",
            "--db",
            str(self._dashboard_db_path()),
            "--out_dir",
            str(self._labeling_output_dir()),
            "--horizons",
            str(self.cfg.get("labeling_horizons", "5,20,60") or "5,20,60"),
            "--limit",
            str(int(self.cfg.get("labeling_limit", 400) or 400)),
        ]
        market_filter = str(self.cfg.get("labeling_market", "") or "").strip()
        portfolio_filter = str(self.cfg.get("labeling_portfolio_id", "") or "").strip()
        stage = str(self.cfg.get("labeling_stage", "final") or "").strip()
        if market_filter:
            cmd.extend(["--market", market_filter])
        if portfolio_filter:
            cmd.extend(["--portfolio_id", portfolio_filter])
        if stage:
            cmd.extend(["--stage", stage])
        return self._run_cmd(
            f"label_investment_snapshots:{reason}",
            cmd,
            timeout_sec=float(self.cfg.get("labeling_timeout_sec", self.cfg.get("dashboard_timeout_sec", 300))),
        )

    def _weekly_review_enabled(self) -> bool:
        return bool(self.cfg.get("run_investment_weekly_review", True))

    def _weekly_review_due(self, now: datetime) -> tuple[bool, str]:
        if not self._weekly_review_enabled():
            return False, "disabled"
        if bool(self.cfg.get("weekly_review_only_when_all_markets_closed", True)):
            open_markets = [market.market_code for market in self.markets if market.enabled and self._market_exchange_open(market, now)]
            if open_markets:
                return False, f"markets_open:{','.join(open_markets)}"
        run_time = str(self.cfg.get("weekly_review_time", "") or "").strip()
        local_now = now.astimezone(self.tz)
        if run_time and not _past_time(local_now, run_time):
            return False, "before_weekly_review_time"
        interval_min = max(1, int(self.cfg.get("weekly_review_interval_min", 180) or 180))
        marker = self._weekly_review_output_dir() / "weekly_review_summary.json"
        if marker.exists():
            age_sec = local_now.timestamp() - marker.stat().st_mtime
            if age_sec >= 0 and age_sec < (interval_min * 60):
                return False, "weekly_review_interval_not_elapsed"
        return True, "due"

    def _run_investment_weekly_review(self, now: datetime, *, force: bool = False) -> bool:
        due, reason = self._weekly_review_due(now)
        if force:
            due = True
            reason = "forced"
        if not due:
            return False
        cmd = [
            sys.executable,
            "-m",
            "src.tools.review_investment_weekly",
            "--db",
            str(self._dashboard_db_path()),
            "--out_dir",
            str(self._weekly_review_output_dir()),
            "--labeling_dir",
            str(self._labeling_output_dir()),
            "--preflight_dir",
            str(self._preflight_output_dir()),
            "--feedback_thresholds_config",
            str(self._weekly_feedback_threshold_override_path()),
            "--days",
            str(int(self.cfg.get("weekly_review_days", 7) or 7)),
        ]
        market_filter = str(self.cfg.get("weekly_review_market", "") or "").strip()
        portfolio_filter = str(self.cfg.get("weekly_review_portfolio_id", "") or "").strip()
        if market_filter:
            cmd.extend(["--market", market_filter])
        if portfolio_filter:
            cmd.extend(["--portfolio_id", portfolio_filter])
        if bool(self.cfg.get("weekly_review_include_legacy", False)):
            cmd.append("--include_legacy")
        ok = self._run_cmd(
            f"review_investment_weekly:{reason}",
            cmd,
            timeout_sec=float(self.cfg.get("weekly_review_timeout_sec", self.cfg.get("dashboard_timeout_sec", 300))),
        )
        if ok:
            self._refresh_weekly_feedback_threshold_overrides()
        return ok

    def _dashboard_control_enabled(self) -> bool:
        return bool(self.cfg.get("dashboard_control_enabled", False))

    def _dashboard_control_host(self) -> str:
        return str(self.cfg.get("dashboard_control_host", "127.0.0.1") or "127.0.0.1")

    def _dashboard_control_port(self) -> int:
        return int(self.cfg.get("dashboard_control_port", 8765) or 8765)

    def _dashboard_control_url(self) -> str:
        host = self._dashboard_control_host()
        port = self._dashboard_control_port()
        if self._dashboard_control_service is not None:
            return self._dashboard_control_service.base_url
        return f"http://{host}:{port}"

    def _dashboard_control_state_path(self) -> Path:
        return self._summary_output_dir() / "dashboard_control_state.json"

    def _dashboard_control_base_flags(self, item: Dict[str, Any]) -> Dict[str, bool]:
        base = item.get("_dashboard_control_base_flags")
        if isinstance(base, dict) and base:
            return {str(k): bool(v) for k, v in base.items()}
        base_flags = {
            field: bool(item.get(field, False))
            for field in CONTROL_FLAG_FIELDS
        }
        item["_dashboard_control_base_flags"] = dict(base_flags)
        return base_flags

    def _capture_dashboard_control_baselines(self) -> None:
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                self._dashboard_control_base_flags(item)

    def _execution_control_mode_for_item(self, item: Dict[str, Any]) -> str:
        run_execution = bool(item.get("run_investment_execution", False))
        submit_execution = bool(item.get("submit_investment_execution", False))
        run_guard = bool(item.get("run_investment_guard", False))
        submit_guard = bool(item.get("submit_investment_guard", False))
        if not run_execution and not run_guard:
            return "PAUSED"
        if not submit_execution and not submit_guard:
            return "REVIEW_ONLY"
        return "AUTO"

    def _apply_execution_control_mode_to_item(self, item: Dict[str, Any], mode: str) -> None:
        normalized = str(mode or "").strip().upper() or "AUTO"
        if normalized not in EXECUTION_CONTROL_MODE_VALUES:
            raise ValueError(f"unsupported_execution_mode:{normalized}")
        base = self._dashboard_control_base_flags(item)
        if normalized == "AUTO":
            # AUTO 模式恢复到配置文件里的原始执行开关，避免控制面多次切换后“回不去”。
            for field in EXECUTION_CONTROL_FIELDS:
                item[field] = bool(base.get(field, False))
            return
        if normalized == "REVIEW_ONLY":
            # REVIEW_ONLY 继续跑执行/guard 生成计划，但禁止自动提交，适合风险恶化后的人工观察期。
            item["run_investment_execution"] = bool(base.get("run_investment_execution", False))
            item["submit_investment_execution"] = False
            item["run_investment_guard"] = bool(base.get("run_investment_guard", False))
            item["submit_investment_guard"] = False
            return
        # PAUSED 直接暂停执行与 guard，保留 report/opportunity 等分析链路继续运行。
        item["run_investment_execution"] = False
        item["submit_investment_execution"] = False
        item["run_investment_guard"] = False
        item["submit_investment_guard"] = False

    def _portfolio_id_for_item(self, item: Dict[str, Any], report_market: str) -> str:
        watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
        slug = Path(watchlist_yaml).stem if watchlist_yaml else str(report_market or "").lower()
        return str(item.get("portfolio_id", f"{str(report_market or '').upper()}:{slug}") or f"{str(report_market or '').upper()}:{slug}")

    def _weekly_feedback_payload_for_item(self, item: Dict[str, Any], report_market: str) -> Dict[str, Any]:
        shadow_row = self._weekly_feedback_row_for_item(item, report_market)
        risk_row = self._weekly_risk_feedback_row_for_item(item, report_market)
        execution_row = self._weekly_execution_feedback_row_for_item(item, report_market)
        automation_rows = self._weekly_feedback_automation_rows_for_item(item, report_market)
        threshold_rows = self._weekly_feedback_threshold_suggestion_rows_for_market(report_market)
        tuning_rows = self._weekly_feedback_threshold_tuning_rows_for_market(report_market)
        payload: Dict[str, Any] = {
            "portfolio_id": self._portfolio_id_for_item(item, report_market),
            "market": str(report_market or "").upper(),
        }
        if shadow_row:
            payload["shadow_feedback"] = dict(shadow_row)
        if risk_row:
            payload["risk_feedback"] = dict(risk_row)
        if execution_row:
            payload["execution_feedback"] = dict(execution_row)
        if automation_rows:
            payload["feedback_automation"] = dict(automation_rows)
        if threshold_rows:
            payload["feedback_threshold_suggestions"] = list(threshold_rows)
        if tuning_rows:
            # live 人工确认时把 tuning 结论一并纳入签名，避免确认的还是旧版 suggestion。
            payload["feedback_threshold_tuning"] = list(tuning_rows)
        return payload

    def _weekly_feedback_signature_for_item(self, item: Dict[str, Any], report_market: str) -> str:
        payload = self._weekly_feedback_payload_for_item(item, report_market)
        if len(payload) <= 2:
            return ""
        # 用当前周报反馈内容生成稳定签名，便于 live 侧做“这一次确认的是哪一版反馈”。
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(serialized.encode("utf-8")).hexdigest()

    def _weekly_feedback_confirmed_signature_for_item(self, item: Dict[str, Any]) -> str:
        return str(item.get("_dashboard_control_weekly_feedback_confirmed_signature", "") or "").strip()

    def _weekly_feedback_confirmed_ts_for_item(self, item: Dict[str, Any]) -> str:
        return str(item.get("_dashboard_control_weekly_feedback_confirmed_ts", "") or "").strip()

    def _dashboard_control_portfolios(self) -> Dict[str, Dict[str, Any]]:
        rows: Dict[str, Dict[str, Any]] = {}
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                report_market = resolve_market_code(str(item.get("market", market.market_code)))
                runtime_scope = self._runtime_scope_for(item, report_market)
                account_mode = str(getattr(runtime_scope, "mode", "") or "paper").strip().lower() or "paper"
                portfolio_id = self._portfolio_id_for_item(item, report_market)
                feedback_signature = self._weekly_feedback_signature_for_item(item, report_market)
                confirmed_signature = self._weekly_feedback_confirmed_signature_for_item(item)
                live_auto_apply_enabled = bool(self.cfg.get("weekly_review_auto_apply_live", False))
                automation_rows = self._weekly_feedback_automation_rows_for_item(item, report_market)
                confirmable_feedback_present = (
                    any(
                        str(row.get("calibration_apply_mode") or "").strip().upper() in {"AUTO_APPLY", "SUGGEST_ONLY"}
                        for row in automation_rows.values()
                    )
                    if automation_rows
                    else bool(feedback_signature)
                )
                rows[portfolio_id] = {
                    "market": report_market,
                    "watchlist": Path(str(item.get("watchlist_yaml", "") or report_market)).stem,
                    "portfolio_id": portfolio_id,
                    "account_mode": account_mode,
                    "execution_control_mode": self._execution_control_mode_for_item(item),
                    "run_investment_paper": bool(item.get("run_investment_paper", False)),
                    "force_local_paper_ledger": bool(item.get("force_local_paper_ledger", False)),
                    "run_investment_execution": bool(item.get("run_investment_execution", False)),
                    "submit_investment_execution": bool(item.get("submit_investment_execution", False)),
                    "run_investment_guard": bool(item.get("run_investment_guard", False)),
                    "submit_investment_guard": bool(item.get("submit_investment_guard", False)),
                    "run_investment_opportunity": bool(item.get("run_investment_opportunity", False)),
                    "weekly_feedback_present": bool(feedback_signature),
                    "weekly_feedback_signature": feedback_signature,
                    "weekly_feedback_confirmed_signature": confirmed_signature,
                    "weekly_feedback_confirmed_ts": self._weekly_feedback_confirmed_ts_for_item(item),
                    "weekly_feedback_automation_modes": {
                        kind: str(dict(row).get("calibration_apply_mode") or "")
                        for kind, row in automation_rows.items()
                    },
                    "weekly_feedback_pending_live_confirm": bool(
                        account_mode == "live"
                        and feedback_signature
                        and not live_auto_apply_enabled
                        and feedback_signature != confirmed_signature
                        and confirmable_feedback_present
                    ),
                    "weekly_feedback_auto_apply_enabled": self._weekly_feedback_auto_apply_enabled(item, report_market),
                }
        return rows

    def _dashboard_control_state_payload(self, *, service_status: str = "running") -> Dict[str, Any]:
        return {
            "ts": datetime.now(self.tz).isoformat(),
            "service": {
                "enabled": self._dashboard_control_enabled(),
                "status": service_status,
                "host": self._dashboard_control_host(),
                "port": self._dashboard_control_port(),
                "url": self._dashboard_control_url(),
            },
            "actions": {
                "run_once_in_progress": bool(self._dashboard_control_run_once_in_progress),
                "preflight_in_progress": bool(self._dashboard_control_preflight_in_progress),
                "weekly_review_in_progress": bool(self._dashboard_control_weekly_review_in_progress),
                "last_action": str(self._dashboard_control_last_action or ""),
                "last_action_ts": str(self._dashboard_control_last_action_ts or ""),
                "last_error": str(self._dashboard_control_last_error or ""),
                "preflight_summary_path": str(self._preflight_output_dir() / "supervisor_preflight_summary.json"),
            },
            "portfolios": self._dashboard_control_portfolios(),
        }

    def _write_dashboard_control_state(self, *, service_status: str = "running") -> None:
        if not self._dashboard_control_enabled():
            return
        path = self._dashboard_control_state_path()
        payload = self._dashboard_control_state_payload(service_status=service_status)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as e:
            log.warning("Failed to write dashboard control state: path=%s error=%s", path, e)

    def _apply_dashboard_control_overrides(self) -> None:
        if not self._dashboard_control_enabled():
            return
        path = self._dashboard_control_state_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        portfolios = dict(payload.get("portfolios") or {})
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                report_market = resolve_market_code(str(item.get("market", market.market_code)))
                portfolio_id = self._portfolio_id_for_item(item, report_market)
                row = dict(portfolios.get(portfolio_id) or {})
                if not row:
                    continue
                for field in CONTROL_FLAG_FIELDS:
                    if field in row:
                        item[field] = bool(row.get(field, False))
                if "weekly_feedback_confirmed_signature" in row:
                    item["_dashboard_control_weekly_feedback_confirmed_signature"] = str(row.get("weekly_feedback_confirmed_signature") or "")
                if "weekly_feedback_confirmed_ts" in row:
                    item["_dashboard_control_weekly_feedback_confirmed_ts"] = str(row.get("weekly_feedback_confirmed_ts") or "")

    def _set_dashboard_control_flag(self, *, portfolio_id: str, field: str, value: bool) -> Dict[str, Any]:
        target_portfolio = str(portfolio_id or "").strip()
        target_field = str(field or "").strip()
        if not target_portfolio:
            return {"ok": False, "error": "missing_portfolio_id"}
        if target_field not in CONTROL_FLAG_FIELDS:
            return {"ok": False, "error": "unsupported_field", "field": target_field}
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                report_market = resolve_market_code(str(item.get("market", market.market_code)))
                item_portfolio_id = self._portfolio_id_for_item(item, report_market)
                if item_portfolio_id != target_portfolio:
                    continue
                item[target_field] = bool(value)
                self._dashboard_control_last_action = f"toggle:{target_portfolio}:{target_field}={int(bool(value))}"
                self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
                self._dashboard_control_last_error = ""
                self._write_dashboard_control_state()
                return {
                    "ok": True,
                    "portfolio_id": target_portfolio,
                    "field": target_field,
                    "value": bool(value),
                }
        return {"ok": False, "error": "portfolio_not_found", "portfolio_id": target_portfolio}

    def _set_dashboard_execution_mode(self, *, portfolio_id: str, mode: str) -> Dict[str, Any]:
        target_portfolio = str(portfolio_id or "").strip()
        target_mode = str(mode or "").strip().upper() or "AUTO"
        if not target_portfolio:
            return {"ok": False, "error": "missing_portfolio_id"}
        if target_mode not in EXECUTION_CONTROL_MODE_VALUES:
            return {"ok": False, "error": "unsupported_execution_mode", "mode": target_mode}
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                report_market = resolve_market_code(str(item.get("market", market.market_code)))
                item_portfolio_id = self._portfolio_id_for_item(item, report_market)
                if item_portfolio_id != target_portfolio:
                    continue
                self._apply_execution_control_mode_to_item(item, target_mode)
                self._dashboard_control_last_action = f"execution_mode:{target_portfolio}:{target_mode}"
                self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
                self._dashboard_control_last_error = ""
                self._write_dashboard_control_state()
                return {
                    "ok": True,
                    "portfolio_id": target_portfolio,
                    "execution_control_mode": self._execution_control_mode_for_item(item),
                }
        return {"ok": False, "error": "portfolio_not_found", "portfolio_id": target_portfolio}

    def _dashboard_control_run_once(self, _: Dict[str, Any] | None = None) -> Dict[str, Any]:
        with self._dashboard_control_lock:
            if (
                self._dashboard_control_run_once_in_progress
                or self._dashboard_control_preflight_in_progress
                or self._dashboard_control_weekly_review_in_progress
                or bool(self._cycle_running)
            ):
                return {"ok": False, "status": "busy"}
            self._dashboard_control_run_once_in_progress = True
            self._dashboard_control_last_action = "run_once"
            self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
            self._dashboard_control_last_error = ""
            self._write_dashboard_control_state()

        def _worker() -> None:
            try:
                self.run_cycle()
                self._refresh_dashboard()
            except Exception as e:
                self._dashboard_control_last_error = f"{type(e).__name__}: {e}"
            finally:
                with self._dashboard_control_lock:
                    self._dashboard_control_run_once_in_progress = False
                    self._write_dashboard_control_state()

        threading.Thread(target=_worker, name="dashboard-control-run-once", daemon=True).start()
        return {"ok": True, "status": "accepted"}

    def _dashboard_control_run_preflight(self, _: Dict[str, Any] | None = None) -> Dict[str, Any]:
        with self._dashboard_control_lock:
            if (
                self._dashboard_control_run_once_in_progress
                or self._dashboard_control_preflight_in_progress
                or self._dashboard_control_weekly_review_in_progress
                or bool(self._cycle_running)
            ):
                return {"ok": False, "status": "busy"}
            self._dashboard_control_preflight_in_progress = True
            self._dashboard_control_last_action = "run_preflight"
            self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
            self._dashboard_control_last_error = ""
            self._write_dashboard_control_state()

        def _worker() -> None:
            try:
                # 这里优先对当前主 runtime scope 做轻量体检，保证 dashboard 上看到的是“当前实际运行环境”的结果。
                scope = self._primary_runtime_scope()
                runtime_root = str(scope.root(BASE_DIR)) if scope is not None else ""
                run_preflight(
                    self.config_path,
                    runtime_root=runtime_root,
                    out_dir=str(self._preflight_output_dir()),
                )
                self._refresh_dashboard()
            except Exception as e:
                self._dashboard_control_last_error = f"{type(e).__name__}: {e}"
            finally:
                with self._dashboard_control_lock:
                    self._dashboard_control_preflight_in_progress = False
                    self._write_dashboard_control_state()

        threading.Thread(target=_worker, name="dashboard-control-preflight", daemon=True).start()
        return {"ok": True, "status": "accepted"}

    def _dashboard_control_run_weekly_review(self, _: Dict[str, Any] | None = None) -> Dict[str, Any]:
        with self._dashboard_control_lock:
            if (
                self._dashboard_control_run_once_in_progress
                or self._dashboard_control_preflight_in_progress
                or self._dashboard_control_weekly_review_in_progress
                or bool(self._cycle_running)
            ):
                return {"ok": False, "status": "busy"}
            self._dashboard_control_weekly_review_in_progress = True
            self._dashboard_control_last_action = "run_weekly_review"
            self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
            self._dashboard_control_last_error = ""
            self._write_dashboard_control_state()

        def _worker() -> None:
            try:
                ok = self._run_investment_weekly_review(datetime.now(self.tz), force=True)
                if ok:
                    self._refresh_dashboard()
                else:
                    self._dashboard_control_last_error = "weekly_review_failed"
            except Exception as e:
                self._dashboard_control_last_error = f"{type(e).__name__}: {e}"
            finally:
                with self._dashboard_control_lock:
                    self._dashboard_control_weekly_review_in_progress = False
                    self._write_dashboard_control_state()

        threading.Thread(target=_worker, name="dashboard-control-weekly-review", daemon=True).start()
        return {"ok": True, "status": "accepted"}

    def _dashboard_control_refresh_dashboard(self, _: Dict[str, Any] | None = None) -> Dict[str, Any]:
        ok = self._refresh_dashboard()
        self._dashboard_control_last_action = "refresh_dashboard"
        self._dashboard_control_last_action_ts = datetime.now(self.tz).isoformat()
        self._dashboard_control_last_error = "" if ok else "dashboard_refresh_failed"
        self._write_dashboard_control_state()
        return {"ok": bool(ok)}

    def _dashboard_control_toggle_flag(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = dict(payload or {})
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        field = str(row.get("field", "") or "").strip()
        if "value" in row:
            value = bool(row.get("value"))
        else:
            state = self._dashboard_control_portfolios().get(portfolio_id, {})
            value = not bool(state.get(field, False))
        result = self._set_dashboard_control_flag(portfolio_id=portfolio_id, field=field, value=value)
        if bool(result.get("ok", False)):
            self._refresh_dashboard()
        return result

    def _dashboard_control_set_execution_mode(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = dict(payload or {})
        portfolio_id = str(row.get("portfolio_id", "") or "").strip()
        mode = str(row.get("mode", "") or "").strip().upper()
        result = self._set_dashboard_execution_mode(portfolio_id=portfolio_id, mode=mode)
        if bool(result.get("ok", False)):
            self._refresh_dashboard()
        return result

    def _dashboard_control_apply_weekly_feedback(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        row = dict(payload or {})
        target_portfolio = str(row.get("portfolio_id", "") or "").strip()
        if not target_portfolio:
            return {"ok": False, "error": "missing_portfolio_id"}
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                report_market = resolve_market_code(str(item.get("market", market.market_code)))
                item_portfolio_id = self._portfolio_id_for_item(item, report_market)
                if item_portfolio_id != target_portfolio:
                    continue
                feedback_signature = self._weekly_feedback_signature_for_item(item, report_market)
                if not feedback_signature:
                    return {"ok": False, "error": "weekly_feedback_not_available", "portfolio_id": target_portfolio}
                confirmed_ts = datetime.now(self.tz).isoformat()
                item["_dashboard_control_weekly_feedback_confirmed_signature"] = feedback_signature
                item["_dashboard_control_weekly_feedback_confirmed_ts"] = confirmed_ts
                investment_cfg = str(self._effective_investment_config_path(item, report_market))
                execution_cfg = str(self._effective_execution_config_path(item, report_market))
                paper_cfg = str(self._effective_paper_config_path(item, report_market))
                threshold_cfg = str(self._refresh_weekly_feedback_threshold_overrides(target_markets={report_market}))
                self._dashboard_control_last_action = f"apply_weekly_feedback:{target_portfolio}"
                self._dashboard_control_last_action_ts = confirmed_ts
                self._dashboard_control_last_error = ""
                self._write_dashboard_control_state()
                self._refresh_dashboard()
                return {
                    "ok": True,
                    "portfolio_id": target_portfolio,
                    "weekly_feedback_signature": feedback_signature,
                    "weekly_feedback_confirmed_ts": confirmed_ts,
                    "investment_config_path": investment_cfg,
                    "execution_config_path": execution_cfg,
                    "paper_config_path": paper_cfg,
                    "feedback_thresholds_config_path": threshold_cfg,
                }
        return {"ok": False, "error": "portfolio_not_found", "portfolio_id": target_portfolio}

    def _start_dashboard_control_service(self) -> None:
        if not self._dashboard_control_enabled() or self._dashboard_control_service is not None:
            return
        service = DashboardControlService(
            self._dashboard_control_host(),
            self._dashboard_control_port(),
            get_state=lambda: self._dashboard_control_state_payload(service_status="running"),
            run_once=self._dashboard_control_run_once,
            run_preflight=self._dashboard_control_run_preflight,
            run_weekly_review=self._dashboard_control_run_weekly_review,
            apply_weekly_feedback=self._dashboard_control_apply_weekly_feedback,
            refresh_dashboard=self._dashboard_control_refresh_dashboard,
            toggle_flag=self._dashboard_control_toggle_flag,
            set_execution_mode=self._dashboard_control_set_execution_mode,
        )
        try:
            service.start()
        except Exception as e:
            self._dashboard_control_last_error = f"{type(e).__name__}: {e}"
            self._write_dashboard_control_state(service_status="error")
            log.warning("Failed to start dashboard control service: %s %s", type(e).__name__, e)
            return
        self._dashboard_control_service = service
        self._write_dashboard_control_state(service_status="running")
        log.info("Dashboard control service started -> %s", service.base_url)

    def _stop_dashboard_control_service(self) -> None:
        if self._dashboard_control_service is None:
            return
        try:
            self._write_dashboard_control_state(service_status="stopped")
            self._dashboard_control_service.stop()
        finally:
            self._dashboard_control_service = None

    @staticmethod
    def _add_reason(counter: Dict[str, int], reason: str) -> None:
        key = str(reason or "unspecified").strip() or "unspecified"
        counter[key] = int(counter.get(key, 0) or 0) + 1

    def _new_market_summary(self, market: MarketRuntime, market_now: datetime, *, priority_order: int) -> Dict[str, Any]:
        is_open = bool(self._market_exchange_open(market, market_now))
        return {
            "market": market.market_code,
            "market_name": market.name,
            "local_time": market_now.isoformat(),
            "exchange_open": is_open,
            "priority_order": int(priority_order),
            "priority_reason": "market_open_first" if is_open else "market_closed_after_open_markets",
            "execution_submit_enabled": False,
            "guard_submit_enabled": False,
            "watchlists_refreshed": 0,
            "reports_run": 0,
            "report_skip_reasons": {},
            "papers_run": 0,
            "paper_skip_reasons": {},
            "baselines_run": 0,
            "broker_snapshot_runs": 0,
            "broker_snapshot_skip_reasons": {},
            "execution_run": 0,
            "execution_skip_reasons": {},
            "guard_run": 0,
            "guard_skip_reasons": {},
            "opportunity_run": 0,
            "opportunity_skip_reasons": {},
            "report_statuses": [],
            "notable_actions": [],
        }

    @staticmethod
    def _summary_signature_payload(market_summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in market_summaries:
            row_copy = dict(row)
            row_copy.pop("local_time", None)
            normalized.append(row_copy)
        return normalized

    def _market_summary_signature(self, market_summary: Dict[str, Any]) -> str:
        payload = self._summary_signature_payload([market_summary])[0]
        return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _report_status_snapshot(
        self,
        market: MarketRuntime,
        item: Dict[str, Any],
        *,
        report_market: str,
        market_now: datetime,
    ) -> Dict[str, Any]:
        watchlist = Path(str(item.get("watchlist_yaml", "") or report_market)).stem
        report_time = str(item.get("report_time", market.report_time) or market.report_time).strip()
        report_day = str(item.get("_last_successful_report_day", "") or "").strip()
        marker = self._report_marker_path(item, report_market)
        fresh, fresh_reason = self._report_fresh_enough(
            market,
            item,
            report_market=report_market,
            market_now=market_now,
        )
        return {
            "watchlist": watchlist,
            "report_time": report_time,
            "report_schedule": self._report_schedule_entries(market, item),
            "report_day": report_day,
            "report_slot": str(item.get("_last_successful_report_slot_name", "") or "").strip(),
            "marker_exists": marker.exists(),
            "fresh": bool(fresh),
            "fresh_reason": fresh_reason,
        }

    def _write_cycle_summary(self, now: datetime, market_summaries: List[Dict[str, Any]]) -> bool:
        signature = hashlib.sha1(
            json.dumps(self._summary_signature_payload(market_summaries), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        if signature == self._last_cycle_summary_signature:
            return False

        out_dir = self._summary_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": now.isoformat(),
            "markets": market_summaries,
        }
        json_path = out_dir / "supervisor_cycle_summary.json"
        md_path = out_dir / "supervisor_cycle_summary.md"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            "# Supervisor Cycle Summary",
            "",
            f"- Generated: {now.isoformat()}",
            "",
        ]
        for row in market_summaries:
            lines.append(f"## {row['market_name']} ({row['market']})")
            lines.append(f"- Priority order: {int(row.get('priority_order', 0) or 0)}")
            lines.append(f"- Priority reason: {row.get('priority_reason', '')}")
            lines.append(f"- Local time: {row['local_time']}")
            lines.append(f"- Exchange open: {row['exchange_open']}")
            lines.append(f"- Execution submit enabled: {bool(row.get('execution_submit_enabled', False))}")
            lines.append(f"- Guard submit enabled: {bool(row.get('guard_submit_enabled', False))}")
            lines.append(f"- Watchlists refreshed: {int(row['watchlists_refreshed'] or 0)}")
            lines.append(f"- Reports run: {int(row['reports_run'] or 0)}")
            if row.get("report_skip_reasons"):
                lines.append(f"- Report skips: {json.dumps(row['report_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            lines.append(f"- Paper runs: {int(row['papers_run'] or 0)}")
            if row.get("paper_skip_reasons"):
                lines.append(f"- Paper skips: {json.dumps(row['paper_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            lines.append(f"- Baseline runs: {int(row['baselines_run'] or 0)}")
            lines.append(f"- Broker snapshot runs: {int(row.get('broker_snapshot_runs', 0) or 0)}")
            if row.get("broker_snapshot_skip_reasons"):
                lines.append(f"- Broker snapshot skips: {json.dumps(row['broker_snapshot_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            lines.append(f"- Execution runs: {int(row['execution_run'] or 0)}")
            if row.get("execution_skip_reasons"):
                lines.append(f"- Execution skips: {json.dumps(row['execution_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            lines.append(f"- Guard runs: {int(row['guard_run'] or 0)}")
            if row.get("guard_skip_reasons"):
                lines.append(f"- Guard skips: {json.dumps(row['guard_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            lines.append(f"- Opportunity runs: {int(row['opportunity_run'] or 0)}")
            if row.get("opportunity_skip_reasons"):
                lines.append(f"- Opportunity skips: {json.dumps(row['opportunity_skip_reasons'], ensure_ascii=False, sort_keys=True)}")
            if row.get("report_statuses"):
                lines.append(f"- Report statuses: {json.dumps(row['report_statuses'], ensure_ascii=False, sort_keys=True)}")
            if row.get("notable_actions"):
                lines.append(f"- Notable actions: {json.dumps(row['notable_actions'], ensure_ascii=False)}")
            lines.append("")
        md_path.write_text("\n".join(lines), encoding="utf-8")
        self._last_cycle_summary_signature = signature
        log.info("Wrote supervisor cycle summary -> %s", md_path)
        return True

    def _open_dashboard_once(self, out_dir: str) -> None:
        if self._dashboard_opened_once:
            return
        if not bool(self.cfg.get("dashboard_auto_open", False)):
            return
        dashboard_path = _resolve_path(out_dir) / "dashboard.html"
        if not dashboard_path.exists():
            return
        try:
            opened = bool(webbrowser.open(dashboard_path.resolve().as_uri(), new=0, autoraise=False))
            if opened:
                self._dashboard_opened_once = True
                log.info("Opened dashboard -> %s", dashboard_path)
            else:
                log.warning("Dashboard open request was not accepted by the system browser: %s", dashboard_path)
        except Exception as e:
            log.warning("Failed to open dashboard automatically: %s %s", type(e).__name__, e)

    def _refresh_dashboard(self) -> bool:
        if not bool(self.cfg.get("dashboard_enabled", True)):
            return False
        out_dir = str(self._summary_output_dir())
        ok = self._run_cmd(
            "generate_dashboard",
            [
                sys.executable,
                "-m",
                "src.tools.generate_dashboard",
                "--config",
                self.config_path,
                "--out_dir",
                out_dir,
            ],
            timeout_sec=float(self.cfg.get("dashboard_timeout_sec", 120)),
        )
        if ok:
            self._open_dashboard_once(out_dir)
        return ok

    def _load_markets(self) -> List[MarketRuntime]:
        market_cfgs = self.cfg.get("markets", [])
        if market_cfgs:
            markets: List[MarketRuntime] = []
            for item in market_cfgs:
                name = str(item.get("name", "")).strip()
                if not name:
                    raise ValueError("Each market entry in config/supervisor.yaml must define a non-empty 'name'")
                markets.append(
                    MarketRuntime(
                        name=name,
                        market_code=resolve_market_code(str(item.get("market", name))),
                        ibkr_config=str(
                            market_config_path(
                                BASE_DIR,
                                resolve_market_code(str(item.get("market", name))),
                                str(item.get("ibkr_config", "")) or None,
                            )
                        ),
                        local_timezone=str(
                            item.get(
                                "local_timezone",
                                market_timezone_name(str(item.get("market", name)), str(self.cfg.get("timezone", "Australia/Sydney"))),
                            )
                        ),
                        watchlists=list(item.get("watchlists", [])),
                        reports=list(item.get("reports", [])),
                        trading=dict(item.get("trading", {})),
                        short_safety_sync=dict(item.get("short_safety_sync", self.cfg.get("short_safety_sync", {}))),
                        watchlist_refresh_time=str(item.get("watchlist_refresh_time", self.cfg.get("watchlist_refresh_time", "19:00"))),
                        report_time=str(item.get("report_time", self.cfg.get("report_time", "20:00"))),
                        enabled=bool(item.get("enabled", True)),
                    )
                )
            return markets

        # Backward-compatible single-market config.
        return [
            MarketRuntime(
                name=str(self.cfg.get("market_name", "default")),
                market_code=resolve_market_code(str(self.cfg.get("market", self.cfg.get("market_name", "default")))),
                ibkr_config=str(
                    market_config_path(
                        BASE_DIR,
                        resolve_market_code(str(self.cfg.get("market", self.cfg.get("market_name", "default")))),
                        str(self.cfg.get("ibkr_config", "")) or None,
                    )
                ),
                local_timezone=str(self.cfg.get("timezone", "Australia/Sydney")),
                watchlists=list(self.cfg.get("watchlists", [])),
                reports=list(self.cfg.get("reports", [])),
                trading=dict(self.cfg.get("trading", {})),
                short_safety_sync=dict(self.cfg.get("short_safety_sync", {})),
                watchlist_refresh_time=str(self.cfg.get("watchlist_refresh_time", "19:00")),
                report_time=str(self.cfg.get("report_time", "20:00")),
                enabled=True,
            )
        ]

    @staticmethod
    def _market_now(now: datetime, market: MarketRuntime) -> datetime:
        return now.astimezone(ZoneInfo(str(market.local_timezone or "UTC")))

    def _market_day_key(self, market: MarketRuntime, now: datetime) -> str:
        return self._market_now(now, market).strftime("%Y-%m-%d")

    def _market_holiday_config(self, market_code: str) -> Dict[str, Any]:
        markets = dict(self.holiday_cfg.get("markets") or {})
        return dict(markets.get(resolve_market_code(market_code), {}) or {})

    def _market_holiday_key(self, market: MarketRuntime, market_now: datetime) -> str:
        return self._market_now(market_now, market).date().isoformat()

    def _market_holiday_name(self, market: MarketRuntime, market_now: datetime) -> str:
        cfg = self._market_holiday_config(market.market_code)
        holiday_key = self._market_holiday_key(market, market_now)
        names = dict(cfg.get("holiday_names") or {})
        return str(names.get(holiday_key, "") or "").strip()

    def _market_is_holiday(self, market: MarketRuntime, market_now: datetime) -> bool:
        cfg = self._market_holiday_config(market.market_code)
        holiday_key = self._market_holiday_key(market, market_now)
        holidays = {str(x).strip() for x in list(cfg.get("holidays") or []) if str(x).strip()}
        return holiday_key in holidays

    def _market_early_close_time(self, market: MarketRuntime, market_now: datetime) -> str:
        cfg = self._market_holiday_config(market.market_code)
        holiday_key = self._market_holiday_key(market, market_now)
        early_close = dict(cfg.get("early_close") or {})
        return str(early_close.get(holiday_key, "") or "").strip()

    def _market_is_trading_day(self, market: MarketRuntime, market_day: date) -> bool:
        trading = dict(market.trading or {})
        weekdays = {int(x) for x in list(trading.get("weekdays", [0, 1, 2, 3, 4]))}
        if market_day.weekday() not in weekdays:
            return False
        cfg = self._market_holiday_config(market.market_code)
        holidays = {str(x).strip() for x in list(cfg.get("holidays") or []) if str(x).strip()}
        return market_day.isoformat() not in holidays

    def _trading_days_old(self, market: MarketRuntime, report_day: date, current_day: date) -> int:
        if current_day <= report_day:
            return 0
        steps = 0
        cursor = report_day + timedelta(days=1)
        while cursor <= current_day:
            if self._market_is_trading_day(market, cursor):
                steps += 1
            cursor += timedelta(days=1)
        return int(steps)

    def _market_exchange_open(self, market: MarketRuntime, now: datetime) -> bool:
        trading = dict(market.trading or {})
        weekdays = list(trading.get("weekdays", [0, 1, 2, 3, 4]))
        start_hhmm = str(trading.get("start", "") or "").strip()
        end_hhmm = str(trading.get("end", "") or "").strip()
        market_now = self._market_now(now, market)
        if self._market_is_holiday(market, market_now):
            return False
        if start_hhmm and end_hhmm:
            early_close = self._market_early_close_time(market, market_now)
            effective_end = early_close or end_hhmm
            if _in_window(market_now, start_hhmm, effective_end, weekdays):
                return True
        for window in list(trading.get("additional_windows", []) or []):
            extra_start = str(dict(window).get("start", "") or "").strip()
            extra_end = str(dict(window).get("end", "") or "").strip()
            extra_weekdays = list(dict(window).get("weekdays", weekdays))
            if extra_start and extra_end and _in_window(market_now, extra_start, extra_end, extra_weekdays):
                return True
        return False

    def _ordered_markets(self, now: datetime) -> List[MarketRuntime]:
        def _priority(market: MarketRuntime) -> tuple[int, str]:
            is_open = self._market_exchange_open(market, now)
            return (0 if is_open else 1, str(market.name or ""))

        return sorted([market for market in self.markets if market.enabled], key=_priority)

    def _run_cmd(self, name: str, cmd: List[str], *, timeout_sec: float | int | None = None) -> bool:
        log.info(f"Running task {name}: {' '.join(cmd)}")
        timeout = None
        if timeout_sec is not None and float(timeout_sec) > 0:
            timeout = float(timeout_sec)
        try:
            res = subprocess.run(cmd, cwd=str(BASE_DIR), text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            log.warning(f"Task {name} timed out after {timeout:.0f}s")
            return False
        if res.returncode != 0:
            log.warning(f"Task {name} exited with code {res.returncode}")
            return False
        return True

    def _ibkr_config_path_for(self, item: Dict[str, Any], report_market: str) -> str:
        explicit_cfg = str(item.get("ibkr_config", "") or "").strip()
        return str(market_config_path(BASE_DIR, report_market, explicit_cfg or None))

    def _runtime_scope_for(self, item: Dict[str, Any], report_market: str):
        cfg_path = self._ibkr_config_path_for(item, report_market)
        cached = self._runtime_scope_cache.get(cfg_path)
        if cached is not None:
            return cached
        cfg = _load_yaml(cfg_path)
        scope = scope_from_ibkr_config(cfg)
        self._runtime_scope_cache[cfg_path] = scope
        return scope

    def _should_run_local_paper_after_report(self, item: Dict[str, Any], report_market: str) -> bool:
        if not bool(item.get("run_investment_paper", False)):
            return False
        if bool(item.get("force_local_paper_ledger", False)):
            return True
        cfg = _load_yaml(self._ibkr_config_path_for(item, report_market))
        mode = str(cfg.get("mode", "paper") or "paper").strip().lower()
        if mode == "paper" and bool(item.get("run_investment_execution", False)) and bool(item.get("submit_investment_execution", False)):
            return False
        return True

    def _scoped_runtime_path(self, item: Dict[str, Any], report_market: str, raw_path: str) -> Path:
        scope = self._runtime_scope_for(item, report_market)
        return resolve_scoped_runtime_path(BASE_DIR, raw_path, scope)

    def _report_output_dir(self, item: Dict[str, Any], report_market: str) -> Path:
        out_dir = self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))
        watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
        if watchlist_yaml:
            return out_dir / _slugify_name(Path(watchlist_yaml).stem)
        return out_dir / f"market_{report_market.lower()}"

    def _report_marker_path(self, item: Dict[str, Any], report_market: str) -> Path:
        report_kind = str(item.get("kind", "trade") or "trade").strip().lower()
        filename = "investment_report.md" if report_kind == "investment" else "report.md"
        return self._report_output_dir(item, report_market) / filename

    def _report_enrichment_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "enrichment.json"

    def _report_state_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "supervisor_report_state.json"

    def _execution_marker_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "investment_execution_summary.json"

    def _guard_marker_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "investment_guard_summary.json"

    def _broker_snapshot_marker_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "investment_broker_snapshot_summary.json"

    def _baseline_output_dir(self, item: Dict[str, Any], report_market: str, day_key: str) -> Path:
        root = self._scoped_runtime_path(item, report_market, str(item.get("baseline_out_dir", "reports_baseline")))
        watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
        slug = _slugify_name(Path(watchlist_yaml).stem) if watchlist_yaml else f"market_{report_market.lower()}"
        return root / slug / day_key

    def _db_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._scoped_runtime_path(item, report_market, str(item.get("db", "audit.db")))

    def _portfolio_id_for_item(self, item: Dict[str, Any], report_market: str) -> str:
        watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
        watchlist_stem = Path(watchlist_yaml).stem if watchlist_yaml else self._report_output_dir(item, report_market).name
        return f"{report_market}:{watchlist_stem}"

    def _weekly_feedback_auto_apply_enabled(self, item: Dict[str, Any], report_market: str) -> bool:
        scope = self._runtime_scope_for(item, report_market)
        mode = str(getattr(scope, "mode", "") or "paper").strip().lower() or "paper"
        if mode == "live":
            if bool(self.cfg.get("weekly_review_auto_apply_live", False)):
                return True
            current_signature = self._weekly_feedback_signature_for_item(item, report_market)
            confirmed_signature = self._weekly_feedback_confirmed_signature_for_item(item)
            return bool(current_signature) and current_signature == confirmed_signature
        return bool(self.cfg.get("weekly_review_auto_apply_paper", True))

    def _weekly_feedback_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("shadow_feedback_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_risk_feedback_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("risk_feedback_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_execution_feedback_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("execution_feedback_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_feedback_automation_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("feedback_automation_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_feedback_threshold_suggestion_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("feedback_threshold_suggestion_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_feedback_threshold_tuning_rows(self) -> List[Dict[str, Any]]:
        payload = _load_json_file(self._weekly_review_output_dir() / "weekly_review_summary.json")
        rows = payload.get("feedback_threshold_tuning_summary")
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, dict)]
        return []

    def _weekly_feedback_threshold_suggestion_rows_for_market(self, report_market: str) -> List[Dict[str, Any]]:
        market_code = resolve_market_code(str(report_market or ""))
        rows: List[Dict[str, Any]] = []
        for row in self._weekly_feedback_threshold_suggestion_rows():
            market = resolve_market_code(str(row.get("market") or ""))
            if market != market_code:
                continue
            rows.append(dict(row))
        rows.sort(
            key=lambda row: (
                0 if str(row.get("suggestion_action", "") or "") == "TIGHTEN_AUTO_APPLY" else 1,
                str(row.get("feedback_kind_label", "") or ""),
            )
        )
        return rows

    def _weekly_feedback_threshold_tuning_rows_for_market(self, report_market: str) -> List[Dict[str, Any]]:
        market_code = resolve_market_code(str(report_market or ""))
        rows: List[Dict[str, Any]] = []
        for row in self._weekly_feedback_threshold_tuning_rows():
            market = resolve_market_code(str(row.get("market") or ""))
            if market != market_code:
                continue
            rows.append(dict(row))
        rows.sort(
            key=lambda row: (
                0 if str(row.get("suggestion_action", "") or "") in {"REVERT_RELAX", "REVIEW_TIGHTEN"} else 1,
                str(row.get("feedback_kind_label", "") or ""),
            )
        )
        return rows

    def _weekly_feedback_row_for_item(self, item: Dict[str, Any], report_market: str) -> Dict[str, Any]:
        portfolio_id = self._portfolio_id_for_item(item, report_market)
        for row in self._weekly_feedback_rows():
            if str(row.get("portfolio_id") or "") != portfolio_id:
                continue
            market = str(row.get("market") or "").upper().strip()
            if market and market != str(report_market or "").upper():
                continue
            return dict(row)
        return {}

    def _weekly_risk_feedback_row_for_item(self, item: Dict[str, Any], report_market: str) -> Dict[str, Any]:
        portfolio_id = self._portfolio_id_for_item(item, report_market)
        for row in self._weekly_risk_feedback_rows():
            if str(row.get("portfolio_id") or "") != portfolio_id:
                continue
            market = str(row.get("market") or "").upper().strip()
            if market and market != str(report_market or "").upper():
                continue
            return dict(row)
        return {}

    def _weekly_execution_feedback_row_for_item(self, item: Dict[str, Any], report_market: str) -> Dict[str, Any]:
        portfolio_id = self._portfolio_id_for_item(item, report_market)
        for row in self._weekly_execution_feedback_rows():
            if str(row.get("portfolio_id") or "") != portfolio_id:
                continue
            market = str(row.get("market") or "").upper().strip()
            if market and market != str(report_market or "").upper():
                continue
            return dict(row)
        return {}

    def _weekly_feedback_automation_rows_for_item(self, item: Dict[str, Any], report_market: str) -> Dict[str, Dict[str, Any]]:
        portfolio_id = self._portfolio_id_for_item(item, report_market)
        out: Dict[str, Dict[str, Any]] = {}
        for row in self._weekly_feedback_automation_rows():
            if str(row.get("portfolio_id") or "") != portfolio_id:
                continue
            market = str(row.get("market") or "").upper().strip()
            if market and market != str(report_market or "").upper():
                continue
            kind = str(row.get("feedback_kind") or "").strip().lower()
            if not kind:
                continue
            out[kind] = dict(row)
        return out

    def _weekly_feedback_automation_row_for_item(
        self,
        item: Dict[str, Any],
        report_market: str,
        feedback_kind: str,
    ) -> Dict[str, Any]:
        rows = self._weekly_feedback_automation_rows_for_item(item, report_market)
        return dict(rows.get(str(feedback_kind or "").strip().lower(), {}) or {})

    def _weekly_feedback_kind_auto_apply_enabled(self, item: Dict[str, Any], report_market: str, feedback_kind: str) -> bool:
        if not self._weekly_feedback_auto_apply_enabled(item, report_market):
            return False
        automation_row = self._weekly_feedback_automation_row_for_item(item, report_market, feedback_kind)
        if not automation_row:
            return True
        apply_mode = str(automation_row.get("calibration_apply_mode") or "").strip().upper()
        if not apply_mode:
            return True
        scope = self._runtime_scope_for(item, report_market)
        mode = str(getattr(scope, "mode", "") or "paper").strip().lower() or "paper"
        if mode == "live":
            return apply_mode in {"AUTO_APPLY", "SUGGEST_ONLY"}
        return apply_mode == "AUTO_APPLY"

    def _weekly_feedback_kind_confirmable(self, item: Dict[str, Any], report_market: str, feedback_kind: str) -> bool:
        automation_row = self._weekly_feedback_automation_row_for_item(item, report_market, feedback_kind)
        if not automation_row:
            return False
        return str(automation_row.get("calibration_apply_mode") or "").strip().upper() in {"AUTO_APPLY", "SUGGEST_ONLY"}

    def _weekly_feedback_overlay_dir(self, item: Dict[str, Any], report_market: str) -> Path:
        root = self._scoped_runtime_path(
            item,
            report_market,
            str(self.cfg.get("weekly_review_overlay_dir", "auto_feedback_configs") or "auto_feedback_configs"),
        )
        return root / _slugify_name(Path(str(item.get("watchlist_yaml", "") or "")).stem or f"market_{report_market.lower()}")

    def _weekly_feedback_threshold_override_path(self) -> Path:
        raw = str(self.cfg.get("weekly_feedback_thresholds_path", "") or "").strip()
        if raw:
            return _resolve_path(raw)
        return self._weekly_review_output_dir() / "weekly_feedback_threshold_overrides.yaml"

    def _market_has_auto_apply_enabled_item(self, report_market: str) -> bool:
        market_code = resolve_market_code(str(report_market or ""))
        for market in self.markets:
            for item in list(market.reports or []):
                if str(item.get("kind", "investment") or "investment").strip().lower() != "investment":
                    continue
                item_market = resolve_market_code(str(item.get("market", market.market_code)))
                if item_market != market_code:
                    continue
                if self._weekly_feedback_auto_apply_enabled(item, item_market):
                    return True
        return False

    def _weekly_feedback_threshold_override_rows_for_market(self, report_market: str) -> Dict[str, Dict[str, float]]:
        suggestion_rows = self._weekly_feedback_threshold_suggestion_rows_for_market(report_market)
        tuning_map: Dict[str, Dict[str, Any]] = {}
        for row in self._weekly_feedback_threshold_tuning_rows_for_market(report_market):
            feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
            if feedback_kind:
                tuning_map[feedback_kind] = dict(row)
        out: Dict[str, Dict[str, float]] = {}
        for row in suggestion_rows:
            feedback_kind = str(row.get("feedback_kind", "") or "").strip().lower()
            if not feedback_kind:
                continue
            tuning_row = dict(tuning_map.get(feedback_kind, {}) or {})
            # 这里优先按 tuning summary 决定“继续保留 / 收回 / 继续收紧”，
            # 只有 tuning 还没形成时，才回退到原始 threshold suggestion。
            action = str(
                tuning_row.get("suggestion_action")
                or row.get("suggestion_action")
                or ""
            ).strip().upper()
            if action not in {
                "RELAX_AUTO_APPLY",
                "TIGHTEN_AUTO_APPLY",
                "KEEP_RELAX",
                "SOFT_RELAX",
                "KEEP_TIGHTEN",
                "REVIEW_TIGHTEN",
            }:
                continue
            out[feedback_kind] = {
                "auto_confidence": float(row.get("suggested_auto_confidence", 0.0) or 0.0),
                "auto_base_confidence": float(row.get("suggested_auto_base_confidence", 0.0) or 0.0),
                "auto_calibration_score": float(row.get("suggested_auto_calibration_score", 0.0) or 0.0),
                "auto_maturity_ratio": float(row.get("suggested_auto_maturity_ratio", 0.0) or 0.0),
            }
        return out

    def _refresh_weekly_feedback_threshold_overrides(self, *, target_markets: set[str] | None = None) -> Path:
        path = self._weekly_feedback_threshold_override_path()
        existing = _load_yaml(str(path)) if path.exists() else {}
        markets_cfg = dict(existing.get("markets") or {}) if isinstance(existing, dict) else {}
        candidate_markets = {
            resolve_market_code(str(row.get("market") or ""))
            for row in self._weekly_feedback_threshold_suggestion_rows()
            if resolve_market_code(str(row.get("market") or ""))
        }
        if target_markets is not None:
            candidate_markets = {resolve_market_code(str(x or "")) for x in target_markets if resolve_market_code(str(x or ""))}
        for market_code in sorted(candidate_markets):
            if self._market_has_auto_apply_enabled_item(market_code):
                market_overrides = self._weekly_feedback_threshold_override_rows_for_market(market_code)
                if market_overrides:
                    markets_cfg[market_code] = market_overrides
                else:
                    markets_cfg.pop(market_code, None)
            else:
                markets_cfg.pop(market_code, None)
        payload = {
            "metadata": {
                "updated_at": datetime.now(self.tz).isoformat(),
                "source": "weekly_review_threshold_suggestions",
            },
            "markets": markets_cfg,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return path

    def _base_investment_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        if str(item.get("investment_config", "") or "").strip():
            return _resolve_path(str(item["investment_config"]))
        ibkr_cfg = _load_yaml(self._ibkr_config_path_for(item, report_market))
        return _resolve_path(str(ibkr_cfg.get("investment_config", f"config/investment_{report_market.lower()}.yaml")))

    def _base_paper_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        if str(item.get("paper_config", "") or "").strip():
            return _resolve_path(str(item["paper_config"]))
        ibkr_cfg = _load_yaml(self._ibkr_config_path_for(item, report_market))
        return _resolve_path(str(ibkr_cfg.get("investment_paper_config", f"config/investment_paper_{report_market.lower()}.yaml")))

    def _base_execution_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        if str(item.get("execution_config", "") or "").strip():
            return _resolve_path(str(item["execution_config"]))
        ibkr_cfg = _load_yaml(self._ibkr_config_path_for(item, report_market))
        return _resolve_path(str(ibkr_cfg.get("investment_execution_config", f"config/investment_execution_{report_market.lower()}.yaml")))

    def _write_yaml_file(self, path: Path, payload: Dict[str, Any]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return path

    def _effective_investment_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        base_path = self._base_investment_config_path(item, report_market)
        feedback_row = self._weekly_feedback_row_for_item(item, report_market)
        execution_feedback_row = self._weekly_execution_feedback_row_for_item(item, report_market)
        shadow_auto_apply_enabled = self._weekly_feedback_kind_auto_apply_enabled(item, report_market, "shadow")
        execution_auto_apply_enabled = self._weekly_feedback_kind_auto_apply_enabled(item, report_market, "execution")
        overlay_path = self._weekly_feedback_overlay_dir(item, report_market) / "investment_auto_feedback.yaml"
        existing_overlay_cfg = _load_yaml(str(overlay_path)) if overlay_path.exists() else {}
        existing_weekly_feedback = dict(existing_overlay_cfg.get("weekly_feedback") or {})
        previous_execution_penalties = _parse_feedback_penalty_rows(existing_weekly_feedback.get("execution_penalties"))
        if not execution_auto_apply_enabled and self._weekly_feedback_automation_row_for_item(item, report_market, "execution"):
            previous_execution_penalties = []
        if not shadow_auto_apply_enabled:
            feedback_row = {}
        if not execution_auto_apply_enabled:
            execution_feedback_row = {}
        if (not feedback_row and not execution_feedback_row and not previous_execution_penalties) or not self._weekly_feedback_auto_apply_enabled(item, report_market):
            return base_path
        base_cfg = copy.deepcopy(_load_yaml(str(base_path)))
        scoring = dict(base_cfg.get("scoring") or {})
        plan = dict(base_cfg.get("plan") or {})
        signal_penalties = _scale_feedback_penalty_rows(
            _parse_feedback_penalty_rows(feedback_row.get("signal_penalties_json")),
            feedback_row,
        )
        current_execution_penalties = _scale_feedback_penalty_rows(
            _parse_feedback_penalty_rows(execution_feedback_row.get("execution_penalties_json")),
            execution_feedback_row,
        )
        execution_penalties = _merge_execution_feedback_penalties(current_execution_penalties, previous_execution_penalties)
        if feedback_row:
            scoring["accumulate_threshold"] = round(
                _clamp_float(
                    float(scoring.get("accumulate_threshold", 0.35) or 0.35)
                    + _scale_feedback_delta(feedback_row.get("scoring_accumulate_threshold_delta", 0.0), feedback_row, min_abs=0.002),
                    -1.0,
                    1.0,
                ),
                6,
            )
            scoring["execution_ready_threshold"] = round(
                _clamp_float(
                    float(scoring.get("execution_ready_threshold", 0.08) or 0.08)
                    + _scale_feedback_delta(feedback_row.get("scoring_execution_ready_threshold_delta", 0.0), feedback_row, min_abs=0.002),
                    0.0,
                    1.0,
                ),
                6,
            )
            plan["review_window_days"] = int(
                round(
                    _clamp_float(
                        int(plan.get("review_window_days", 90) or 90)
                        + _scale_feedback_delta(feedback_row.get("plan_review_window_days_delta", 0), feedback_row),
                        7.0,
                        365.0,
                    )
                )
            )
        base_cfg["scoring"] = scoring
        base_cfg["plan"] = plan
        feedback_reason_parts = [
            str(feedback_row.get("feedback_reason") or "").strip(),
            str(execution_feedback_row.get("feedback_reason") or "").strip(),
        ]
        if previous_execution_penalties and not current_execution_penalties:
            feedback_reason_parts.append("沿用并衰减上一轮执行热点惩罚。")
        base_cfg["weekly_feedback"] = {
            "enabled": True,
            "portfolio_id": str(
                feedback_row.get("portfolio_id") or execution_feedback_row.get("portfolio_id") or existing_weekly_feedback.get("portfolio_id") or ""
            ),
            "market": str(feedback_row.get("market") or execution_feedback_row.get("market") or existing_weekly_feedback.get("market") or ""),
            "shadow_calibration_apply_mode": str(
                self._weekly_feedback_automation_row_for_item(item, report_market, "shadow").get("calibration_apply_mode") or ""
            ),
            "execution_calibration_apply_mode": str(
                self._weekly_feedback_automation_row_for_item(item, report_market, "execution").get("calibration_apply_mode") or ""
            ),
            "shadow_review_action": str(feedback_row.get("shadow_review_action") or ""),
            "execution_feedback_action": (
                str(execution_feedback_row.get("execution_feedback_action") or "")
                or ("DECAY" if execution_penalties and not current_execution_penalties else "")
            ),
            "shadow_feedback_base_confidence": float(
                _clamp_float(feedback_row.get("feedback_base_confidence", _feedback_confidence_value(feedback_row)), 0.0, 1.0)
            ),
            "shadow_feedback_calibration_score": float(_clamp_float(feedback_row.get("feedback_calibration_score", 0.5), 0.0, 1.0)),
            "shadow_feedback_confidence": float(_feedback_confidence_value(feedback_row)),
            "execution_feedback_base_confidence": float(
                _clamp_float(execution_feedback_row.get("feedback_base_confidence", _feedback_confidence_value(execution_feedback_row)), 0.0, 1.0)
            ),
            "execution_feedback_calibration_score": float(
                _clamp_float(execution_feedback_row.get("feedback_calibration_score", 0.5), 0.0, 1.0)
            ),
            "execution_feedback_confidence": float(_feedback_confidence_value(execution_feedback_row)),
            "feedback_scope": str(
                feedback_row.get("feedback_scope")
                or execution_feedback_row.get("feedback_scope")
                or existing_weekly_feedback.get("feedback_scope")
                or "paper_only"
            ),
            "feedback_reason": " ".join(part for part in feedback_reason_parts if part),
            "signal_penalties": signal_penalties,
            "execution_penalties": execution_penalties,
        }
        return self._write_yaml_file(overlay_path, base_cfg)

    def _effective_execution_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        base_path = self._base_execution_config_path(item, report_market)
        shadow_feedback_row = self._weekly_feedback_row_for_item(item, report_market)
        execution_feedback_row = self._weekly_execution_feedback_row_for_item(item, report_market)
        shadow_auto_apply_enabled = self._weekly_feedback_kind_auto_apply_enabled(item, report_market, "shadow")
        execution_auto_apply_enabled = self._weekly_feedback_kind_auto_apply_enabled(item, report_market, "execution")
        overlay_path = self._weekly_feedback_overlay_dir(item, report_market) / "execution_auto_feedback.yaml"
        existing_overlay_cfg = _load_yaml(str(overlay_path)) if overlay_path.exists() else {}
        existing_weekly_feedback = dict(existing_overlay_cfg.get("weekly_feedback") or {})
        previous_execution_penalties = _parse_feedback_penalty_rows(
            existing_weekly_feedback.get("execution_hotspot_penalties") or existing_weekly_feedback.get("execution_penalties")
        )
        if not previous_execution_penalties:
            legacy_overlay_path = self._weekly_feedback_overlay_dir(item, report_market) / "investment_auto_feedback.yaml"
            legacy_overlay_cfg = _load_yaml(str(legacy_overlay_path)) if legacy_overlay_path.exists() else {}
            legacy_weekly_feedback = dict(legacy_overlay_cfg.get("weekly_feedback") or {})
            # 兼容旧版本：之前 execution hotspot 只写进 investment feedback。
            previous_execution_penalties = _parse_feedback_penalty_rows(
                legacy_weekly_feedback.get("execution_penalties")
            )
        if not execution_auto_apply_enabled and self._weekly_feedback_automation_row_for_item(item, report_market, "execution"):
            previous_execution_penalties = []
        if not shadow_auto_apply_enabled:
            shadow_feedback_row = {}
        if not execution_auto_apply_enabled:
            execution_feedback_row = {}
        if (
            not shadow_feedback_row
            and not execution_feedback_row
            and not previous_execution_penalties
        ) or not self._weekly_feedback_auto_apply_enabled(item, report_market):
            return base_path
        base_cfg = copy.deepcopy(_load_yaml(str(base_path)))
        execution = dict(base_cfg.get("execution") or {})
        if shadow_feedback_row:
            execution["shadow_ml_min_score_auto_submit"] = round(
                _clamp_float(
                    float(execution.get("shadow_ml_min_score_auto_submit", 0.0) or 0.0)
                    + _scale_feedback_delta(shadow_feedback_row.get("execution_shadow_score_delta", 0.0), shadow_feedback_row, min_abs=0.002),
                    -0.25,
                    1.0,
                ),
                6,
            )
            execution["shadow_ml_min_positive_prob_auto_submit"] = round(
                _clamp_float(
                    float(execution.get("shadow_ml_min_positive_prob_auto_submit", 0.50) or 0.50)
                    + _scale_feedback_delta(shadow_feedback_row.get("execution_shadow_prob_delta", 0.0), shadow_feedback_row, min_abs=0.002),
                    0.0,
                    1.0,
                ),
                6,
            )
        current_execution_penalties = _scale_feedback_penalty_rows(
            _parse_feedback_penalty_rows(execution_feedback_row.get("execution_penalties_json")),
            execution_feedback_row,
        )
        if execution_feedback_row:
            # 这里把周报里的执行成本反馈收敛成“下一轮 execution config 的安全增量”。
            execution["adv_max_participation_pct"] = round(
                _clamp_float(
                    float(execution.get("adv_max_participation_pct", 0.05) or 0.05)
                    + _scale_feedback_delta(execution_feedback_row.get("execution_adv_max_participation_pct_delta", 0.0), execution_feedback_row, min_abs=0.001),
                    0.01,
                    0.20,
                ),
                6,
            )
            execution["adv_split_trigger_pct"] = round(
                _clamp_float(
                    float(execution.get("adv_split_trigger_pct", 0.02) or 0.02)
                    + _scale_feedback_delta(execution_feedback_row.get("execution_adv_split_trigger_pct_delta", 0.0), execution_feedback_row, min_abs=0.001),
                    0.005,
                    0.10,
                ),
                6,
            )
            execution["max_slices_per_symbol"] = int(
                round(
                    _clamp_float(
                        float(execution.get("max_slices_per_symbol", 4) or 4)
                        + _scale_feedback_delta(execution_feedback_row.get("execution_max_slices_per_symbol_delta", 0.0), execution_feedback_row, min_abs=1.0),
                        1.0,
                        8.0,
                    )
                )
            )
            execution["open_session_participation_scale"] = round(
                _clamp_float(
                    float(execution.get("open_session_participation_scale", 0.70) or 0.70)
                    + _scale_feedback_delta(execution_feedback_row.get("execution_open_session_participation_scale_delta", 0.0), execution_feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
            execution["midday_session_participation_scale"] = round(
                _clamp_float(
                    float(execution.get("midday_session_participation_scale", 1.00) or 1.00)
                    + _scale_feedback_delta(execution_feedback_row.get("execution_midday_session_participation_scale_delta", 0.0), execution_feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
            execution["close_session_participation_scale"] = round(
                _clamp_float(
                    float(execution.get("close_session_participation_scale", 0.85) or 0.85)
                    + _scale_feedback_delta(execution_feedback_row.get("execution_close_session_participation_scale_delta", 0.0), execution_feedback_row, min_abs=0.01),
                    0.30,
                    1.50,
                ),
                6,
            )
        execution_hotspot_penalties = _merge_execution_feedback_penalties(current_execution_penalties, previous_execution_penalties)
        if execution_hotspot_penalties:
            # 这里把 symbol 级热点带入 execution config，让本周盘中执行也能主动降速或延后。
            execution["execution_hotspot_penalties"] = execution_hotspot_penalties
        else:
            execution.pop("execution_hotspot_penalties", None)
        base_cfg["execution"] = execution
        feedback_reason_parts = [
            str(shadow_feedback_row.get("feedback_reason") or "").strip(),
            str(execution_feedback_row.get("feedback_reason") or "").strip(),
        ]
        if previous_execution_penalties and not current_execution_penalties:
            feedback_reason_parts.append("沿用并衰减上一轮执行热点惩罚。")
        base_cfg["weekly_feedback"] = {
            "enabled": True,
            "portfolio_id": str(
                shadow_feedback_row.get("portfolio_id")
                or execution_feedback_row.get("portfolio_id")
                or existing_weekly_feedback.get("portfolio_id")
                or ""
            ),
            "market": str(
                shadow_feedback_row.get("market")
                or execution_feedback_row.get("market")
                or existing_weekly_feedback.get("market")
                or ""
            ),
            "shadow_calibration_apply_mode": str(
                self._weekly_feedback_automation_row_for_item(item, report_market, "shadow").get("calibration_apply_mode") or ""
            ),
            "execution_calibration_apply_mode": str(
                self._weekly_feedback_automation_row_for_item(item, report_market, "execution").get("calibration_apply_mode") or ""
            ),
            "feedback_scope": str(
                shadow_feedback_row.get("feedback_scope")
                or execution_feedback_row.get("feedback_scope")
                or existing_weekly_feedback.get("feedback_scope")
                or "paper_only"
            ),
            "shadow_review_action": str(shadow_feedback_row.get("shadow_review_action") or ""),
            "shadow_feedback_base_confidence": float(
                _clamp_float(shadow_feedback_row.get("feedback_base_confidence", _feedback_confidence_value(shadow_feedback_row)), 0.0, 1.0)
            ),
            "shadow_feedback_calibration_score": float(
                _clamp_float(shadow_feedback_row.get("feedback_calibration_score", 0.5), 0.0, 1.0)
            ),
            "shadow_feedback_confidence": float(_feedback_confidence_value(shadow_feedback_row)),
            "shadow_feedback_reason": str(shadow_feedback_row.get("feedback_reason") or existing_weekly_feedback.get("shadow_feedback_reason") or ""),
            "execution_feedback_action": (
                str(execution_feedback_row.get("execution_feedback_action") or "")
                or ("DECAY" if execution_hotspot_penalties and not current_execution_penalties else "")
                or str(existing_weekly_feedback.get("execution_feedback_action") or "")
            ),
            "execution_feedback_base_confidence": float(
                _clamp_float(execution_feedback_row.get("feedback_base_confidence", _feedback_confidence_value(execution_feedback_row)), 0.0, 1.0)
            ),
            "execution_feedback_calibration_score": float(
                _clamp_float(execution_feedback_row.get("feedback_calibration_score", 0.5), 0.0, 1.0)
            ),
            "execution_feedback_confidence": float(_feedback_confidence_value(execution_feedback_row)),
            "execution_feedback_reason": " ".join(part for part in feedback_reason_parts if part),
            "execution_dominant_session_bucket": str(
                execution_feedback_row.get("dominant_execution_session_bucket")
                or existing_weekly_feedback.get("execution_dominant_session_bucket")
                or ""
            ),
            "execution_dominant_session_label": str(
                execution_feedback_row.get("dominant_execution_session_label")
                or existing_weekly_feedback.get("execution_dominant_session_label")
                or ""
            ),
            "execution_dominant_hotspot_symbol": str(
                execution_feedback_row.get("dominant_execution_hotspot_symbol")
                or existing_weekly_feedback.get("execution_dominant_hotspot_symbol")
                or ""
            ),
            "execution_dominant_hotspot_session_label": str(
                execution_feedback_row.get("dominant_execution_hotspot_session_label")
                or existing_weekly_feedback.get("execution_dominant_hotspot_session_label")
                or ""
            ),
            "execution_session_feedback_json": str(
                execution_feedback_row.get("execution_session_feedback_json")
                or existing_weekly_feedback.get("execution_session_feedback_json")
                or ""
            ),
            "execution_hotspots_json": str(
                execution_feedback_row.get("execution_hotspots_json")
                or existing_weekly_feedback.get("execution_hotspots_json")
                or ""
            ),
            "execution_hotspot_penalties": execution_hotspot_penalties,
        }
        return self._write_yaml_file(overlay_path, base_cfg)

    def _effective_paper_config_path(self, item: Dict[str, Any], report_market: str) -> Path:
        base_path = self._base_paper_config_path(item, report_market)
        feedback_row = self._weekly_risk_feedback_row_for_item(item, report_market)
        if not self._weekly_feedback_kind_auto_apply_enabled(item, report_market, "risk"):
            feedback_row = {}
        if not feedback_row or not self._weekly_feedback_auto_apply_enabled(item, report_market):
            return base_path
        base_cfg = copy.deepcopy(_load_yaml(str(base_path)))
        paper = dict(base_cfg.get("paper") or {})
        next_max_single = round(
            _clamp_float(
                float(paper.get("max_single_weight", 0.22) or 0.22)
                + _scale_feedback_delta(feedback_row.get("paper_max_single_weight_delta", 0.0), feedback_row, min_abs=0.002),
                0.05,
                0.50,
            ),
            6,
        )
        next_max_sector = round(
            _clamp_float(
                float(paper.get("max_sector_weight", 0.40) or 0.40)
                + _scale_feedback_delta(feedback_row.get("paper_max_sector_weight_delta", 0.0), feedback_row, min_abs=0.002),
                0.10,
                1.00,
            ),
            6,
        )
        next_max_net = round(
            _clamp_float(
                float(paper.get("max_net_exposure", 1.00) or 1.00)
                + _scale_feedback_delta(feedback_row.get("paper_max_net_exposure_delta", 0.0), feedback_row, min_abs=0.005),
                0.20,
                1.50,
            ),
            6,
        )
        next_max_gross = round(
            _clamp_float(
                float(paper.get("max_gross_exposure", 1.00) or 1.00)
                + _scale_feedback_delta(feedback_row.get("paper_max_gross_exposure_delta", 0.0), feedback_row, min_abs=0.005),
                0.20,
                2.00,
            ),
            6,
        )
        next_max_short = round(
            _clamp_float(
                float(paper.get("max_short_exposure", 0.35) or 0.35)
                + _scale_feedback_delta(feedback_row.get("paper_max_short_exposure_delta", 0.0), feedback_row, min_abs=0.002),
                0.0,
                min(next_max_gross, 1.00),
            ),
            6,
        )
        next_corr_soft = round(
            _clamp_float(
                float(paper.get("correlation_soft_limit", 0.62) or 0.62)
                + _scale_feedback_delta(feedback_row.get("paper_correlation_soft_limit_delta", 0.0), feedback_row, min_abs=0.005),
                0.25,
                0.95,
            ),
            6,
        )
        paper["max_single_weight"] = next_max_single
        paper["max_sector_weight"] = next_max_sector
        paper["max_net_exposure"] = next_max_net
        paper["max_gross_exposure"] = next_max_gross
        paper["max_short_exposure"] = next_max_short
        paper["correlation_soft_limit"] = next_corr_soft
        base_cfg["paper"] = paper
        base_cfg["risk_feedback"] = {
            "enabled": True,
            "portfolio_id": str(feedback_row.get("portfolio_id") or ""),
            "market": str(feedback_row.get("market") or ""),
            "risk_calibration_apply_mode": str(
                self._weekly_feedback_automation_row_for_item(item, report_market, "risk").get("calibration_apply_mode") or ""
            ),
            "risk_feedback_action": str(feedback_row.get("risk_feedback_action") or ""),
            "risk_feedback_base_confidence": float(
                _clamp_float(feedback_row.get("feedback_base_confidence", _feedback_confidence_value(feedback_row)), 0.0, 1.0)
            ),
            "risk_feedback_calibration_score": float(_clamp_float(feedback_row.get("feedback_calibration_score", 0.5), 0.0, 1.0)),
            "risk_feedback_confidence": float(_feedback_confidence_value(feedback_row)),
            "feedback_scope": str(feedback_row.get("feedback_scope") or "paper_only"),
            "feedback_reason": str(feedback_row.get("feedback_reason") or ""),
        }
        return self._write_yaml_file(
            self._weekly_feedback_overlay_dir(item, report_market) / "paper_auto_feedback.yaml",
            base_cfg,
        )

    def _report_schedule_entries(self, market: MarketRuntime, item: Dict[str, Any]) -> List[Dict[str, str]]:
        entries: List[Dict[str, str]] = []
        raw_entries = list(item.get("report_schedule", []) or [])
        if raw_entries:
            for idx, raw in enumerate(raw_entries, start=1):
                row = dict(raw or {})
                hhmm = str(row.get("time", "") or "").strip()
                if not hhmm:
                    continue
                name = str(row.get("name", f"slot_{idx}") or f"slot_{idx}").strip() or f"slot_{idx}"
                entries.append({"name": _slugify_name(name), "time": hhmm})
        if not entries:
            report_time = str(item.get("report_time", market.report_time) or market.report_time).strip()
            if report_time:
                entries.append({"name": "default", "time": report_time})
        return entries

    @staticmethod
    def _has_explicit_report_schedule(item: Dict[str, Any]) -> bool:
        return bool(list(item.get("report_schedule", []) or []))

    def _file_day_key(self, path: Path, timezone_name: str) -> str:
        ts = datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo(str(timezone_name or self.tz.key)))
        return ts.strftime("%Y-%m-%d")

    def _restore_report_state(self, item: Dict[str, Any], report_market: str) -> None:
        if str(item.get("_last_successful_report_day", "") or "").strip():
            return
        state_path = self._report_state_path(item, report_market)
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except Exception:
                state = {}
            if state:
                report_day = str(state.get("last_successful_report_day", "") or "").strip()
                if report_day:
                    item["_last_successful_report_day"] = report_day
                slot_keys = [str(x).strip() for x in list(state.get("last_successful_report_slot_keys", []) or []) if str(x).strip()]
                if slot_keys:
                    item["_last_successful_report_slot_keys"] = slot_keys
                slot_name = str(state.get("last_successful_report_slot_name", "") or "").strip()
                if slot_name:
                    item["_last_successful_report_slot_name"] = slot_name
                last_signature = str(state.get("last_macro_signature", "") or "").strip()
                if last_signature:
                    item["_last_macro_signature"] = last_signature
        marker = self._report_marker_path(item, report_market)
        if not marker.exists():
            return
        timezone_name = str(item.get("_local_timezone", self.tz.key) or self.tz.key)
        report_day = self._file_day_key(marker, timezone_name)
        item["_last_successful_report_day"] = report_day
        report_signature = self._report_macro_signature(item, report_market)
        if report_signature:
            item["_last_macro_signature"] = report_signature
        exec_marker = self._execution_marker_path(item, report_market)
        if exec_marker.exists() and exec_marker.stat().st_mtime >= marker.stat().st_mtime:
            item["_last_execution_for_report_day"] = report_day
        guard_marker = self._guard_marker_path(item, report_market)
        if guard_marker.exists():
            item["_last_guard_run_ts"] = float(guard_marker.stat().st_mtime)
        opportunity_marker = self._opportunity_marker_path(item, report_market)
        if opportunity_marker.exists():
            item["_last_opportunity_run_ts"] = float(opportunity_marker.stat().st_mtime)
        broker_snapshot_marker = self._broker_snapshot_marker_path(item, report_market)
        if broker_snapshot_marker.exists():
            item["_last_broker_snapshot_run_ts"] = float(broker_snapshot_marker.stat().st_mtime)

    def _persist_report_state(self, item: Dict[str, Any], report_market: str) -> None:
        state_path = self._report_state_path(item, report_market)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_successful_report_day": str(item.get("_last_successful_report_day", "") or ""),
            "last_successful_report_slot_keys": list(item.get("_last_successful_report_slot_keys", []) or []),
            "last_successful_report_slot_name": str(item.get("_last_successful_report_slot_name", "") or ""),
            "last_macro_signature": str(item.get("_last_macro_signature", "") or ""),
        }
        state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _report_macro_signature(self, item: Dict[str, Any], report_market: str) -> str:
        path = self._report_enrichment_path(item, report_market)
        if not path.exists():
            return ""
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return ""
        include_market_snapshot = bool(item.get("macro_signature_include_market_snapshot", False))
        signature_payload = {
            "market": report_market,
            "macro_indicators": dict(payload.get("macro_indicators", {}) or {}),
            "macro_events": list(payload.get("macro_events", []) or [])[:10],
        }
        if include_market_snapshot:
            signature_payload["markets"] = dict(payload.get("markets", {}) or {})
        raw = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _report_action_reason(
        self,
        market: MarketRuntime,
        item: Dict[str, Any],
        *,
        report_market: str,
        day_key: str,
        market_now: datetime,
    ) -> tuple[bool, str]:
        if self._has_explicit_report_schedule(item):
            schedule_entries = self._report_schedule_entries(market, item)
            completed_slot_keys = {
                str(x).strip() for x in list(item.get("_last_successful_report_slot_keys", []) or []) if str(x).strip()
            }
            due_entries = [entry for entry in schedule_entries if _past_time(market_now, str(entry.get("time", "") or ""))]
            pending_slot_keys = [
                f"{day_key}:{str(entry['name'])}" for entry in due_entries if f"{day_key}:{str(entry['name'])}" not in completed_slot_keys
            ]
            if pending_slot_keys:
                pending_slot_name = str(due_entries[-1]["name"])
                item["_pending_report_slot_keys"] = pending_slot_keys
                item["_pending_report_slot_name"] = pending_slot_name
                return True, f"scheduled_slot_due:{pending_slot_name}"
            if not due_entries:
                return False, "before_report_time"
        else:
            report_time = str(item.get("report_time", market.report_time) or market.report_time).strip()
            if report_time and not _past_time(market_now, report_time):
                return False, "before_report_time"
        market_day = self._market_now(market_now, market).date()
        if not self._market_is_trading_day(market, market_day):
            report_day = str(item.get("_last_successful_report_day", "") or "").strip()
            if not bool(item.get("rerun_report_on_macro_change", True)):
                return False, "market_holiday"
            if not report_day:
                return False, "market_holiday"
            last_signature = str(item.get("_last_macro_signature", "") or "").strip()
            if not last_signature:
                last_signature = self._report_macro_signature(item, report_market)
                if last_signature:
                    item["_last_macro_signature"] = last_signature
            current_signature = self._current_macro_signature(report_market, item)
            if current_signature and last_signature and current_signature != last_signature:
                return True, "macro_signature_changed_non_trading_day"
            return False, "market_holiday"
        report_day = str(item.get("_last_successful_report_day", "") or "").strip()
        if report_day != day_key:
            return True, "missing_report_for_day"
        if not bool(item.get("rerun_report_on_macro_change", True)):
            return False, "already_generated_today"
        if self._market_exchange_open(market, market_now):
            return False, "market_open_use_existing_report"
        last_signature = str(item.get("_last_macro_signature", "") or "").strip()
        if not last_signature:
            last_signature = self._report_macro_signature(item, report_market)
            if last_signature:
                item["_last_macro_signature"] = last_signature
        current_signature = self._current_macro_signature(report_market, item)
        if not current_signature or not last_signature:
            return False, "macro_signature_unavailable"
        if current_signature != last_signature:
            return True, "macro_signature_changed"
        return False, "already_generated_today_macro_unchanged"

    def _current_macro_signature(self, report_market: str, item: Dict[str, Any]) -> str:
        now_ts = time.time()
        cache_ttl_sec = max(60, int(item.get("macro_signature_ttl_sec", 1800) or 1800))
        include_market_snapshot = bool(item.get("macro_signature_include_market_snapshot", False))
        cache_key = f"{report_market}:{'with_market' if include_market_snapshot else 'macro_only'}"
        cached = self._macro_signature_cache.get(cache_key)
        if cached and (now_ts - float(cached[0])) < cache_ttl_sec:
            return str(cached[1] or "")
        try:
            providers = EnrichmentProviders()
            payload = {
                "market": report_market,
                "macro_indicators": providers.fetch_macro_indicators(),
                "macro_events": providers.fetch_macro_calendar(days_ahead=7)[:10],
            }
            if include_market_snapshot:
                payload["markets"] = providers.fetch_market_snapshot(market=report_market)
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
            self._macro_signature_cache[cache_key] = (now_ts, digest)
            return digest
        except Exception as e:
            log.warning("macro signature refresh failed: market=%s error=%s %s", report_market, type(e).__name__, e)
            return ""

    def _should_rerun_report_for_macro_change(
        self,
        market: MarketRuntime,
        item: Dict[str, Any],
        *,
        report_market: str,
        day_key: str,
        market_now: datetime,
    ) -> bool:
        should_run, _ = self._report_action_reason(
            market,
            item,
            report_market=report_market,
            day_key=day_key,
            market_now=market_now,
        )
        return should_run

    def _run_baseline_regression(self, market: MarketRuntime, item: Dict[str, Any], *, day_key: str) -> bool:
        report_market = resolve_market_code(str(item.get("market", market.market_code)))
        report_dir = self._report_output_dir(item, report_market)
        out_dir = self._baseline_output_dir(item, report_market, day_key)
        watchlist_yaml = str(item.get("watchlist_yaml", "") or "").strip()
        slug = _slugify_name(Path(watchlist_yaml).stem) if watchlist_yaml else f"market_{report_market.lower()}"
        report_slot = str(item.get("_last_successful_report_slot_name", "") or "").strip()
        slot_suffix = f"_{report_slot}" if report_slot and report_slot != "default" else ""
        baseline_name = str(item.get("baseline_name", f"{report_market.lower()}_{slug}_{day_key}{slot_suffix}") or "")
        compare_to = str(item.get("_last_baseline_snapshot", "") or "").strip()
        cmd = [
            sys.executable,
            "-m",
            "src.tools.review_baseline_regression",
            "--market",
            report_market,
            "--portfolio_id",
            str(item.get("baseline_portfolio_id", f"{report_market}:{Path(watchlist_yaml).stem}" if watchlist_yaml else report_market)),
            "--report_dir",
            str(report_dir),
            "--out_dir",
            str(out_dir),
            "--baseline_name",
            baseline_name,
        ]
        if compare_to:
            cmd.extend(["--compare_to", compare_to])
        ok = self._run_cmd(
            f"review_baseline_regression:{market.name}:{slug}",
            cmd,
            timeout_sec=float(item.get("baseline_timeout_sec", self.cfg.get("report_timeout_sec", 1200))),
        )
        if ok:
            item["_last_baseline_snapshot"] = str(out_dir / "baseline_snapshot.json")
        return ok

    def _opportunity_marker_path(self, item: Dict[str, Any], report_market: str) -> Path:
        return self._report_output_dir(item, report_market) / "investment_opportunity_summary.json"

    def _short_safety_output_paths(self, market: MarketRuntime) -> List[Path]:
        try:
            ibkr_cfg = _load_yaml(market.ibkr_config)
            risk_cfg = _load_yaml(str(ibkr_cfg.get("risk_config", "config/risk.yaml")))
        except Exception as e:
            log.warning("load short safety output paths failed: market=%s error=%s %s", market.name, type(e).__name__, e)
            return []

        paths: List[Path] = []
        risk_context = dict(risk_cfg.get("risk_context") or {})
        short_safety = dict(risk_cfg.get("short_safety") or {})
        for raw in (
            str(risk_context.get("short_borrow_fee_file", "") or "").strip(),
            str(short_safety.get("short_safety_file", "") or "").strip(),
        ):
            if raw:
                paths.append(_resolve_path(raw))
        return paths

    def _files_fresh(self, paths: List[Path], now: datetime, max_age_sec: int) -> bool:
        if not paths:
            return False
        now_ts = now.timestamp()
        for path in paths:
            try:
                age = now_ts - path.stat().st_mtime
            except FileNotFoundError:
                return False
            if age < 0 or age > int(max_age_sec):
                return False
        return True

    def _sync_short_safety(self, market: MarketRuntime, now: datetime, *, reason: str) -> bool:
        cfg = dict(market.short_safety_sync or {})
        if not bool(cfg.get("enabled", False)):
            return True

        max_age_sec = int(cfg.get("max_age_sec", 6 * 3600))
        retry_sec = int(cfg.get("retry_sec", 15 * 60))
        day_key = now.strftime("%Y-%m-%d")
        output_paths = self._short_safety_output_paths(market)
        if market.last_short_safety_sync_day == day_key and self._files_fresh(output_paths, now, max_age_sec):
            return True

        now_ts = now.timestamp()
        if market.last_short_safety_sync_attempt_ts and (now_ts - market.last_short_safety_sync_attempt_ts) < retry_sec:
            return False
        market.last_short_safety_sync_attempt_ts = now_ts

        cmd = [
            sys.executable,
            "-m",
            "src.tools.sync_short_safety_from_ibkr",
            "--market",
            market.market_code,
            "--ibkr_config",
            market.ibkr_config,
            "--max_symbols",
            str(cfg.get("max_symbols", 200)),
            "--snapshot_wait_sec",
            str(cfg.get("snapshot_wait_sec", 2.5)),
            "--batch_size",
            str(cfg.get("batch_size", 40)),
            "--market_data_type",
            str(cfg.get("market_data_type", 1)),
            "--fallback_market_data_type",
            str(cfg.get("fallback_market_data_type", 4)),
        ]
        if bool(cfg.get("no_delayed_fallback", False)):
            cmd.append("--no_delayed_fallback")
        if str(cfg.get("watchlist_yaml", "") or "").strip():
            cmd.extend(["--watchlist_yaml", str(cfg["watchlist_yaml"])])
        if str(cfg.get("symbols", "") or "").strip():
            cmd.extend(["--symbols", str(cfg["symbols"])])
        if str(cfg.get("generic_tick_list", "") or "").strip():
            cmd.extend(["--generic_tick_list", str(cfg["generic_tick_list"])])

        ok = self._run_cmd(
            f"short_safety_sync:{market.name}:{reason}",
            cmd,
            timeout_sec=float(cfg.get("timeout_sec", self.cfg.get("short_safety_timeout_sec", 300))),
        )
        if ok:
            market.last_short_safety_sync_day = day_key
        return ok

    def _refresh_watchlists(self, market: MarketRuntime) -> None:
        for item in market.watchlists:
            config_path = str(item["config"])
            out_path = str(item["out"])
            self._run_cmd(
                f"refresh_watchlist:{market.name}:{Path(out_path).stem}",
                [sys.executable, "-m", "src.tools.refresh_watchlist", "--config", config_path, "--out", out_path],
                timeout_sec=float(item.get("timeout_sec", self.cfg.get("watchlist_timeout_sec", 180))),
            )

    def _generate_reports(self, market: MarketRuntime, *, day_key: str, market_now: datetime) -> None:
        for item in market.reports:
            report_market = resolve_market_code(str(item.get("market", market.market_code)))
            item["_local_timezone"] = market.local_timezone
            self._restore_report_state(item, report_market)
            if not self._should_rerun_report_for_macro_change(
                market,
                item,
                report_market=report_market,
                day_key=day_key,
                market_now=market_now,
            ):
                continue
            report_kind = str(item.get("kind", "trade") or "trade").strip().lower()
            if report_kind == "investment":
                report_timeout_sec = float(item.get("timeout_sec", self.cfg.get("report_timeout_sec", 1200)))
                effective_investment_config = self._effective_investment_config_path(item, report_market)
                cmd = [
                    sys.executable,
                    "-m",
                    "src.tools.generate_investment_report",
                    "--out_dir",
                    str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
                    "--watchlist_yaml",
                    str(item["watchlist_yaml"]),
                    "--market",
                    report_market,
                    "--max_universe",
                    str(item.get("max_universe", 1000)),
                    "--top_n",
                    str(item.get("top_n", 15)),
                    "--db",
                    str(self._db_path(item, report_market)),
                    "--audit_limit",
                    str(item.get("audit_limit", 500)),
                ]
                if item.get("request_timeout_sec") is not None:
                    cmd.extend(["--request_timeout_sec", str(item["request_timeout_sec"])])
                if item.get("backtest_top_k") is not None:
                    cmd.extend(["--backtest_top_k", str(item["backtest_top_k"])])
                if item.get("fundamentals_top_k") is not None:
                    cmd.extend(["--fundamentals_top_k", str(item["fundamentals_top_k"])])
                if bool(item.get("use_audit_recent", False)):
                    cmd.append("--use_audit_recent")
                cmd.extend(["--investment_config", str(effective_investment_config)])
                if item.get("ibkr_config"):
                    cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
                ok = self._run_cmd(
                    f"generate_investment_report:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
                    cmd,
                    timeout_sec=report_timeout_sec,
                )
                if ok and self._should_run_local_paper_after_report(item, report_market):
                    paper_cmd = [
                        sys.executable,
                        "-m",
                        "src.tools.run_investment_paper",
                        "--market",
                        report_market,
                        "--reports_root",
                        str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
                        "--watchlist_yaml",
                        str(item["watchlist_yaml"]),
                        "--portfolio_id",
                        f"{report_market}:{Path(str(item['watchlist_yaml'])).stem}",
                        "--db",
                        str(self._db_path(item, report_market)),
                    ]
                    effective_paper_config = self._effective_paper_config_path(item, report_market)
                    if effective_paper_config:
                        paper_cmd.extend(["--paper_config", str(effective_paper_config)])
                    paper_ok = self._run_cmd(
                        f"run_investment_paper:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
                        paper_cmd,
                        timeout_sec=float(item.get("paper_timeout_sec", self.cfg.get("paper_timeout_sec", 300))),
                    )
                    if paper_ok:
                        item["_last_local_paper_run_day"] = day_key
                if ok and bool(item.get("run_baseline_regression", False)):
                    self._run_baseline_regression(market, item, day_key=day_key)
                if ok:
                    item["_last_successful_report_day"] = day_key
                    pending_slot_keys = [str(x).strip() for x in list(item.get("_pending_report_slot_keys", []) or []) if str(x).strip()]
                    existing_slot_keys = [
                        str(x).strip()
                        for x in list(item.get("_last_successful_report_slot_keys", []) or [])
                        if str(x).strip() and str(x).strip().startswith(f"{day_key}:")
                    ]
                    merged_slot_keys = []
                    for key in existing_slot_keys + pending_slot_keys:
                        if key and key not in merged_slot_keys:
                            merged_slot_keys.append(key)
                    item["_last_successful_report_slot_keys"] = merged_slot_keys
                    if str(item.get("_pending_report_slot_name", "") or "").strip():
                        item["_last_successful_report_slot_name"] = str(item.get("_pending_report_slot_name", "") or "").strip()
                    macro_signature = self._report_macro_signature(item, report_market)
                    if macro_signature:
                        item["_last_macro_signature"] = macro_signature
                    self._persist_report_state(item, report_market)
                item.pop("_pending_report_slot_keys", None)
                item.pop("_pending_report_slot_name", None)
                continue

            cmd = [
                sys.executable,
                "-m",
                "src.tools.generate_trade_report",
                "--out_dir",
                str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports")))),
                "--watchlist_yaml",
                str(item["watchlist_yaml"]),
                "--market",
                report_market,
                "--max_universe",
                str(item.get("max_universe", 1000)),
                "--top_n",
                str(item.get("top_n", 10)),
                "--db",
                str(self._db_path(item, report_market)),
                "--audit_limit",
                str(item.get("audit_limit", 500)),
            ]
            if bool(item.get("no_seed", True)):
                cmd.append("--no_seed")
            if bool(item.get("use_audit_recent", True)):
                cmd.append("--use_audit_recent")
            if bool(item.get("use_scanner", False)):
                cmd.append("--use_scanner")
            if item.get("ibkr_config"):
                cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
            ok = self._run_cmd(
                f"generate_trade_report:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
                cmd,
                timeout_sec=float(item.get("timeout_sec", self.cfg.get("report_timeout_sec", 1200))),
            )
            if ok:
                item["_last_successful_report_day"] = day_key
                pending_slot_keys = [str(x).strip() for x in list(item.get("_pending_report_slot_keys", []) or []) if str(x).strip()]
                existing_slot_keys = [
                    str(x).strip()
                    for x in list(item.get("_last_successful_report_slot_keys", []) or [])
                    if str(x).strip() and str(x).strip().startswith(f"{day_key}:")
                ]
                merged_slot_keys = []
                for key in existing_slot_keys + pending_slot_keys:
                    if key and key not in merged_slot_keys:
                        merged_slot_keys.append(key)
                item["_last_successful_report_slot_keys"] = merged_slot_keys
                if str(item.get("_pending_report_slot_name", "") or "").strip():
                    item["_last_successful_report_slot_name"] = str(item.get("_pending_report_slot_name", "") or "").strip()
                macro_signature = self._report_macro_signature(item, report_market)
                if macro_signature:
                    item["_last_macro_signature"] = macro_signature
                self._persist_report_state(item, report_market)
            item.pop("_pending_report_slot_keys", None)
            item.pop("_pending_report_slot_name", None)

    def _run_investment_execution(self, market: MarketRuntime, item: Dict[str, Any]) -> bool:
        report_market = resolve_market_code(str(item.get("market", market.market_code)))
        effective_execution_config = self._effective_execution_config_path(item, report_market)
        effective_paper_config = self._effective_paper_config_path(item, report_market)
        cmd = [
            sys.executable,
            "-m",
            "src.tools.run_investment_execution",
            "--market",
            report_market,
            "--reports_root",
            str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
            "--watchlist_yaml",
            str(item["watchlist_yaml"]),
            "--portfolio_id",
            f"{report_market}:{Path(str(item['watchlist_yaml'])).stem}",
            "--db",
            str(self._db_path(item, report_market)),
            "--execution_config",
            str(effective_execution_config),
        ]
        if effective_paper_config:
            cmd.extend(["--paper_config", str(effective_paper_config)])
        if item.get("ibkr_config"):
            cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
        if bool(item.get("submit_investment_execution", False)):
            cmd.append("--submit")
        return self._run_cmd(
            f"run_investment_execution:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
            cmd,
            timeout_sec=float(item.get("execution_timeout_sec", self.cfg.get("execution_timeout_sec", 300))),
        )

    def _run_investment_broker_snapshot_sync(self, market: MarketRuntime, item: Dict[str, Any]) -> bool:
        report_market = resolve_market_code(str(item.get("market", market.market_code)))
        effective_execution_config = self._effective_execution_config_path(item, report_market)
        effective_paper_config = self._effective_paper_config_path(item, report_market)
        cmd = [
            sys.executable,
            "-m",
            "src.tools.sync_investment_broker_snapshot",
            "--market",
            report_market,
            "--reports_root",
            str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
            "--watchlist_yaml",
            str(item["watchlist_yaml"]),
            "--portfolio_id",
            f"{report_market}:{Path(str(item['watchlist_yaml'])).stem}",
            "--db",
            str(self._db_path(item, report_market)),
            "--request_timeout_sec",
            str(item.get("broker_snapshot_request_timeout_sec", item.get("request_timeout_sec", 10))),
            "--execution_config",
            str(effective_execution_config),
        ]
        if effective_paper_config:
            cmd.extend(["--paper_config", str(effective_paper_config)])
        if item.get("ibkr_config"):
            cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
        return self._run_cmd(
            f"sync_investment_broker_snapshot:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
            cmd,
            timeout_sec=float(item.get("broker_snapshot_timeout_sec", self.cfg.get("execution_timeout_sec", 300))),
        )

    def _run_investment_guard(self, market: MarketRuntime, item: Dict[str, Any]) -> bool:
        report_market = resolve_market_code(str(item.get("market", market.market_code)))
        effective_execution_config = self._effective_execution_config_path(item, report_market)
        cmd = [
            sys.executable,
            "-m",
            "src.tools.run_investment_guard",
            "--market",
            report_market,
            "--reports_root",
            str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
            "--watchlist_yaml",
            str(item["watchlist_yaml"]),
            "--portfolio_id",
            f"{report_market}:{Path(str(item['watchlist_yaml'])).stem}",
            "--db",
            str(self._db_path(item, report_market)),
            "--request_timeout_sec",
            str(item.get("guard_request_timeout_sec", item.get("request_timeout_sec", 10))),
            "--execution_config",
            str(effective_execution_config),
        ]
        if item.get("guard_config"):
            cmd.extend(["--guard_config", str(item["guard_config"])])
        if item.get("ibkr_config"):
            cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
        if bool(item.get("submit_investment_guard", False)):
            cmd.append("--submit")
        return self._run_cmd(
            f"run_investment_guard:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
            cmd,
            timeout_sec=float(item.get("guard_timeout_sec", self.cfg.get("guard_timeout_sec", 240))),
        )

    def _run_investment_opportunity(self, market: MarketRuntime, item: Dict[str, Any]) -> bool:
        report_market = resolve_market_code(str(item.get("market", market.market_code)))
        effective_execution_config = self._effective_execution_config_path(item, report_market)
        cmd = [
            sys.executable,
            "-m",
            "src.tools.run_investment_opportunity",
            "--market",
            report_market,
            "--reports_root",
            str(self._scoped_runtime_path(item, report_market, str(item.get("out_dir", "reports_investment")))),
            "--watchlist_yaml",
            str(item["watchlist_yaml"]),
            "--portfolio_id",
            f"{report_market}:{Path(str(item['watchlist_yaml'])).stem}",
            "--request_timeout_sec",
            str(item.get("opportunity_request_timeout_sec", item.get("request_timeout_sec", 10))),
            "--execution_config",
            str(effective_execution_config),
        ]
        if item.get("opportunity_config"):
            cmd.extend(["--opportunity_config", str(item["opportunity_config"])])
        if item.get("ibkr_config"):
            cmd.extend(["--ibkr_config", str(item["ibkr_config"])])
        return self._run_cmd(
            f"run_investment_opportunity:{market.name}:{Path(str(item['watchlist_yaml'])).stem}",
            cmd,
            timeout_sec=float(item.get("opportunity_timeout_sec", self.cfg.get("guard_timeout_sec", 240))),
        )

    def _market_has_due_reports(self, market: MarketRuntime, *, day_key: str, market_now: datetime) -> bool:
        for item in market.reports:
            report_market = resolve_market_code(str(item.get("market", market.market_code)))
            should_run, _ = self._report_action_reason(
                market,
                item,
                report_market=report_market,
                day_key=day_key,
                market_now=market_now,
            )
            if should_run:
                return True
        return False

    def _report_files_ready(
        self,
        item: Dict[str, Any],
        report_market: str,
        required_files: List[str],
    ) -> tuple[bool, str]:
        report_dir = self._report_output_dir(item, report_market)
        missing: List[str] = []
        for name in required_files:
            path = report_dir / name
            if not path.exists() or (path.is_file() and path.stat().st_size <= 0):
                missing.append(name)
        if missing:
            return False, f"missing_report_files:{','.join(missing)}"
        return True, "ready"

    def _report_fresh_enough(
        self,
        market: MarketRuntime,
        item: Dict[str, Any],
        *,
        report_market: str,
        market_now: datetime,
    ) -> tuple[bool, str]:
        marker = self._report_marker_path(item, report_market)
        if not marker.exists():
            return False, "missing_report_marker"
        report_day = str(item.get("_last_successful_report_day", "") or "").strip()
        if not report_day:
            return False, "missing_report_day"
        try:
            report_date = datetime.strptime(report_day, "%Y-%m-%d").date()
        except ValueError:
            return False, "invalid_report_day"
        local_now = self._market_now(market_now, market)
        trading_days_old = self._trading_days_old(market, report_date, local_now.date())
        max_trading_days_old = int(item.get("report_max_trading_days_old", self.cfg.get("report_max_trading_days_old", 1)) or 1)
        if trading_days_old > max_trading_days_old:
            return False, f"stale_report_trading_days_old:{trading_days_old}"
        max_age_hours = float(item.get("report_max_age_hours", self.cfg.get("report_max_age_hours", 168)) or 168)
        age_hours = (local_now.timestamp() - marker.stat().st_mtime) / 3600.0
        if max_age_hours > 0 and age_hours > max_age_hours:
            return False, f"stale_report_age_hours:{age_hours:.1f}"
        return True, "fresh"

    def _market_live(self, market: MarketRuntime, now: datetime) -> bool:
        trading = market.trading
        if not bool(trading.get("enabled", True)):
            return False
        weekdays = list(trading.get("weekdays", [0, 1, 2, 3, 4]))
        start_hhmm = str(trading.get("start", "23:20"))
        end_hhmm = str(trading.get("end", "06:10"))
        market_now = self._market_now(now, market)
        if self._market_is_holiday(market, market_now):
            return False
        early_close = self._market_early_close_time(market, market_now)
        if early_close:
            end_hhmm = early_close
        return _in_window(market_now, start_hhmm, end_hhmm, weekdays)

    def _active_live_market(self, now: datetime) -> Optional[MarketRuntime]:
        active = [market for market in self.markets if market.enabled and self._market_live(market, now)]
        if not active:
            return None
        if len(active) > 1:
            log.warning(
                "Multiple market windows are active at the same time; running only the first configured market: %s",
                [market.name for market in active],
            )
        return active[0]

    def _setup_signal_handlers(self) -> None:
        def _handler(signum, frame):
            self._stopping = True
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def run_cycle(self, now: Optional[datetime] = None) -> None:
        if self._cycle_running:
            log.info("Skip overlapping supervisor cycle")
            return
        self._cycle_running = True
        try:
            now = now or datetime.now(self.tz)
            cycle_summary: List[Dict[str, Any]] = []

            for idx, market in enumerate(self._ordered_markets(now), start=1):
                market_now = self._market_now(now, market)
                day_key = self._market_day_key(market, now)
                market_summary = self._new_market_summary(market, market_now, priority_order=idx)
                for item in market.reports:
                    market_summary["execution_submit_enabled"] = bool(market_summary["execution_submit_enabled"]) or bool(item.get("submit_investment_execution", False))
                    market_summary["guard_submit_enabled"] = bool(market_summary["guard_submit_enabled"]) or bool(item.get("submit_investment_guard", False))
                    item["_local_timezone"] = market.local_timezone
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    self._restore_report_state(item, report_market)
                if market.reports:
                    all_done_for_day = all(str(item.get("_last_successful_report_day", "") or "") == day_key for item in market.reports)
                    if all_done_for_day:
                        market.last_report_day = day_key
                if market.last_watchlist_day != day_key and _past_time(market_now, market.watchlist_refresh_time):
                    self._refresh_watchlists(market)
                    market.last_watchlist_day = day_key
                    market_summary["watchlists_refreshed"] = int(len(market.watchlists))

                sync_cfg = dict(market.short_safety_sync or {})
                sync_time = str(sync_cfg.get("time", "") or "").strip()
                if sync_time and _past_time(market_now, sync_time):
                    self._sync_short_safety(market, market_now, reason="scheduled")

                has_due_reports = False
                for item in market.reports:
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    should_run, reason = self._report_action_reason(
                        market,
                        item,
                        report_market=report_market,
                        day_key=day_key,
                        market_now=market_now,
                    )
                    slug = Path(str(item.get("watchlist_yaml", "") or report_market)).stem
                    if should_run:
                        has_due_reports = True
                        state = f"due:{reason}"
                        if str(item.get("_last_logged_report_state", "") or "") != state:
                            log.info(
                                "Report due: market=%s watchlist=%s reason=%s",
                                market.market_code,
                                slug,
                                reason,
                            )
                            item["_last_logged_report_state"] = state
                    else:
                        self._add_reason(market_summary["report_skip_reasons"], reason)
                        state = f"skip:{reason}"
                        if str(item.get("_last_logged_report_state", "") or "") != state:
                            log.info(
                                "Skip report: market=%s watchlist=%s reason=%s",
                                market.market_code,
                                slug,
                                reason,
                            )
                            item["_last_logged_report_state"] = state

                if has_due_reports:
                    if bool(sync_cfg.get("run_before_report", True)):
                        self._sync_short_safety(market, market_now, reason="pre_report")
                    report_days_before = {
                        id(item): str(item.get("_last_successful_report_day", "") or "")
                        for item in market.reports
                    }
                    self._generate_reports(market, day_key=day_key, market_now=market_now)
                    for item in market.reports:
                        if str(item.get("_last_successful_report_day", "") or "") == day_key and report_days_before.get(id(item), "") != day_key:
                            market_summary["reports_run"] = int(market_summary["reports_run"]) + 1
                            market_summary["notable_actions"].append(f"report:{Path(str(item['watchlist_yaml'])).stem}")
                            if str(item.get("_last_local_paper_run_day", "") or "") == day_key:
                                market_summary["papers_run"] = int(market_summary["papers_run"]) + 1
                                market_summary["notable_actions"].append(f"paper:{Path(str(item['watchlist_yaml'])).stem}")
                            elif bool(item.get("run_investment_paper", False)) and not self._should_run_local_paper_after_report(item, resolve_market_code(str(item.get("market", market.market_code)))):
                                self._add_reason(market_summary["paper_skip_reasons"], "broker_paper_is_primary")
                            if bool(item.get("run_baseline_regression", False)):
                                market_summary["baselines_run"] = int(market_summary["baselines_run"]) + 1
                                market_summary["notable_actions"].append(f"baseline:{Path(str(item['watchlist_yaml'])).stem}")
                    all_done_for_day = all(str(item.get("_last_successful_report_day", "") or "") == day_key for item in market.reports)
                    market.last_report_day = day_key if all_done_for_day else ""

                for item in market.reports:
                    if str(item.get("kind", "trade") or "trade").strip().lower() != "investment":
                        continue
                    if bool(item.get("research_only", False)):
                        self._add_reason(market_summary["broker_snapshot_skip_reasons"], "research_only")
                        continue
                    if not bool(item.get("run_broker_snapshot_sync", True)):
                        self._add_reason(market_summary["broker_snapshot_skip_reasons"], "disabled")
                        continue
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    item["_local_timezone"] = market.local_timezone
                    self._restore_report_state(item, report_market)
                    interval_min = max(1, int(item.get("broker_snapshot_interval_min", self.cfg.get("broker_snapshot_interval_min", 60)) or 60))
                    last_snapshot_ts = float(item.get("_last_broker_snapshot_run_ts", 0.0) or 0.0)
                    if last_snapshot_ts > 0 and (now.timestamp() - last_snapshot_ts) < (interval_min * 60):
                        self._add_reason(market_summary["broker_snapshot_skip_reasons"], "snapshot_interval_not_elapsed")
                        continue
                    if self._run_investment_broker_snapshot_sync(market, item):
                        item["_last_broker_snapshot_run_ts"] = now.timestamp()
                        market_summary["broker_snapshot_runs"] = int(market_summary["broker_snapshot_runs"]) + 1
                        market_summary["notable_actions"].append(f"broker_snapshot:{Path(str(item['watchlist_yaml'])).stem}")

                for item in market.reports:
                    if str(item.get("kind", "trade") or "trade").strip().lower() != "investment":
                        continue
                    if not bool(item.get("run_investment_execution", False)):
                        continue
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    item["_local_timezone"] = market.local_timezone
                    self._restore_report_state(item, report_market)
                    report_day = str(item.get("_last_successful_report_day", "") or "").strip()
                    if not report_day:
                        continue
                    execution_time = str(item.get("execution_time", "") or "").strip()
                    if not execution_time or not _past_time(market_now, execution_time):
                        continue
                    execution_day_offset = int(item.get("execution_day_offset", 0) or 0)
                    try:
                        report_date = datetime.strptime(report_day, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    if market_now.date() < (report_date + timedelta(days=execution_day_offset)):
                        self._add_reason(market_summary["execution_skip_reasons"], "before_execution_day")
                        continue
                    if str(item.get("_last_execution_for_report_day", "")) == report_day:
                        self._add_reason(market_summary["execution_skip_reasons"], "already_executed_for_report_day")
                        continue
                    report_ready, report_reason = self._report_files_ready(
                        item,
                        report_market,
                        ["investment_candidates.csv", "investment_plan.csv", "investment_report.md"],
                    )
                    if not report_ready:
                        self._add_reason(market_summary["execution_skip_reasons"], report_reason)
                        log.info(
                            "Skip execution: market=%s watchlist=%s reason=%s",
                            market.market_code,
                            Path(str(item["watchlist_yaml"])).stem,
                            report_reason,
                        )
                        continue
                    report_fresh, fresh_reason = self._report_fresh_enough(
                        market,
                        item,
                        report_market=report_market,
                        market_now=now,
                    )
                    if not report_fresh:
                        self._add_reason(market_summary["execution_skip_reasons"], fresh_reason)
                        log.info(
                            "Skip execution: market=%s watchlist=%s reason=%s",
                            market.market_code,
                            Path(str(item["watchlist_yaml"])).stem,
                            fresh_reason,
                        )
                        continue
                    if bool(item.get("run_investment_opportunity", False)):
                        opp_ready, opp_reason = self._report_files_ready(item, report_market, ["investment_candidates.csv"])
                        if not opp_ready:
                            self._add_reason(market_summary["opportunity_skip_reasons"], opp_reason)
                            log.info(
                                "Skip opportunity before execution: market=%s watchlist=%s reason=%s",
                                market.market_code,
                                Path(str(item["watchlist_yaml"])).stem,
                                opp_reason,
                            )
                        else:
                            if self._run_investment_opportunity(market, item):
                                item["_last_opportunity_run_ts"] = now.timestamp()
                                market_summary["opportunity_run"] = int(market_summary["opportunity_run"]) + 1
                    if self._run_investment_execution(market, item):
                        item["_last_execution_for_report_day"] = report_day
                        market_summary["execution_run"] = int(market_summary["execution_run"]) + 1
                        market_summary["notable_actions"].append(
                            f"execution:{Path(str(item['watchlist_yaml'])).stem}:{'submit' if bool(item.get('submit_investment_execution', False)) else 'dry_run'}"
                        )

                    # Continue to guard checks in the same cycle if configured; no early continue.

                for item in market.reports:
                    if str(item.get("kind", "trade") or "trade").strip().lower() != "investment":
                        continue
                    if not bool(item.get("run_investment_guard", False)):
                        continue
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    item["_local_timezone"] = market.local_timezone
                    self._restore_report_state(item, report_market)
                    guard_start = str(item.get("guard_start", "") or "").strip()
                    guard_end = str(item.get("guard_end", "") or "").strip()
                    if not guard_start or not guard_end:
                        self._add_reason(market_summary["guard_skip_reasons"], "guard_window_not_configured")
                        continue
                    guard_weekdays = list(item.get("guard_weekdays", [0, 1, 2, 3, 4]))
                    if not _in_window(market_now, guard_start, guard_end, guard_weekdays):
                        self._add_reason(market_summary["guard_skip_reasons"], "outside_guard_window")
                        continue
                    interval_min = max(1, int(item.get("guard_interval_min", 30) or 30))
                    last_guard_ts = float(item.get("_last_guard_run_ts", 0.0) or 0.0)
                    if last_guard_ts > 0 and (now.timestamp() - last_guard_ts) < (interval_min * 60):
                        self._add_reason(market_summary["guard_skip_reasons"], "guard_interval_not_elapsed")
                        continue
                    report_fresh, fresh_reason = self._report_fresh_enough(
                        market,
                        item,
                        report_market=report_market,
                        market_now=now,
                    )
                    if not report_fresh:
                        self._add_reason(market_summary["guard_skip_reasons"], fresh_reason)
                        log.info(
                            "Skip guard: market=%s watchlist=%s reason=%s",
                            market.market_code,
                            Path(str(item["watchlist_yaml"])).stem,
                            fresh_reason,
                        )
                        continue
                    if self._run_investment_guard(market, item):
                        item["_last_guard_run_ts"] = now.timestamp()
                        market_summary["guard_run"] = int(market_summary["guard_run"]) + 1
                        market_summary["notable_actions"].append(
                            f"guard:{Path(str(item['watchlist_yaml'])).stem}:{'submit' if bool(item.get('submit_investment_guard', False)) else 'dry_run'}"
                        )

                for item in market.reports:
                    if str(item.get("kind", "trade") or "trade").strip().lower() != "investment":
                        continue
                    if not bool(item.get("run_investment_opportunity", False)):
                        continue
                    report_market = resolve_market_code(str(item.get("market", market.market_code)))
                    item["_local_timezone"] = market.local_timezone
                    self._restore_report_state(item, report_market)
                    opportunity_start = str(item.get("opportunity_start", "") or "").strip()
                    opportunity_end = str(item.get("opportunity_end", "") or "").strip()
                    if not opportunity_start or not opportunity_end:
                        self._add_reason(market_summary["opportunity_skip_reasons"], "opportunity_window_not_configured")
                        continue
                    opportunity_weekdays = list(item.get("opportunity_weekdays", [0, 1, 2, 3, 4]))
                    if not _in_window(market_now, opportunity_start, opportunity_end, opportunity_weekdays):
                        self._add_reason(market_summary["opportunity_skip_reasons"], "outside_opportunity_window")
                        continue
                    interval_min = max(1, int(item.get("opportunity_interval_min", 30) or 30))
                    last_opportunity_ts = float(item.get("_last_opportunity_run_ts", 0.0) or 0.0)
                    if last_opportunity_ts > 0 and (now.timestamp() - last_opportunity_ts) < (interval_min * 60):
                        self._add_reason(market_summary["opportunity_skip_reasons"], "opportunity_interval_not_elapsed")
                        continue
                    ready, reason = self._report_files_ready(item, report_market, ["investment_candidates.csv"])
                    if not ready:
                        self._add_reason(market_summary["opportunity_skip_reasons"], reason)
                        log.info(
                            "Skip opportunity: market=%s watchlist=%s reason=%s",
                            market.market_code,
                            Path(str(item["watchlist_yaml"])).stem,
                            reason,
                        )
                        continue
                    report_fresh, fresh_reason = self._report_fresh_enough(
                        market,
                        item,
                        report_market=report_market,
                        market_now=now,
                    )
                    if not report_fresh:
                        self._add_reason(market_summary["opportunity_skip_reasons"], fresh_reason)
                        log.info(
                            "Skip opportunity: market=%s watchlist=%s reason=%s",
                            market.market_code,
                            Path(str(item["watchlist_yaml"])).stem,
                            fresh_reason,
                        )
                        continue
                    if self._run_investment_opportunity(market, item):
                        item["_last_opportunity_run_ts"] = now.timestamp()
                        market_summary["opportunity_run"] = int(market_summary["opportunity_run"]) + 1
                        market_summary["notable_actions"].append(
                            f"opportunity:{Path(str(item['watchlist_yaml'])).stem}"
                        )
                market_summary["report_statuses"] = [
                    self._report_status_snapshot(
                        market,
                        item,
                        report_market=resolve_market_code(str(item.get("market", market.market_code))),
                        market_now=now,
                    )
                    for item in market.reports
                ]
                cycle_summary.append(market_summary)
                market_signature = self._market_summary_signature(market_summary)
                if self._last_market_summary_signatures.get(market.market_code, "") != market_signature:
                    log.info(
                        "Market cycle summary: market=%s open=%s reports_run=%s broker_snapshot_runs=%s executions_run=%s guards_run=%s opportunities_run=%s",
                        market.market_code,
                        market_summary["exchange_open"],
                        market_summary["reports_run"],
                        market_summary["broker_snapshot_runs"],
                        market_summary["execution_run"],
                        market_summary["guard_run"],
                        market_summary["opportunity_run"],
                    )
                    self._last_market_summary_signatures[market.market_code] = market_signature

            live_market = self._active_live_market(now)
            if live_market:
                if bool(dict(live_market.short_safety_sync or {}).get("run_before_live", True)):
                    self._sync_short_safety(live_market, now, reason="pre_live")
                if self._active_market != live_market.name:
                    self.trade_proc.stop()
                    self.trade_proc = ManagedProcess(
                        f"trade-engine:{live_market.name}",
                        [sys.executable, "-m", "src.main", "--market", live_market.market_code],
                    )
                    self._active_market = live_market.name
                    log.info(f"Switched live market -> {live_market.name} market={live_market.market_code} config={live_market.ibkr_config}")
                self.trade_proc.ensure_running()
            else:
                self.trade_proc.stop()
                self._active_market = None
            labeling_ran = self._run_investment_labeling(now)
            weekly_review_ran = self._run_investment_weekly_review(
                now,
                force=bool(labeling_ran and self._weekly_review_enabled()),
            )
            summary_changed = self._write_cycle_summary(now, cycle_summary)
            if summary_changed or labeling_ran or weekly_review_ran:
                self._refresh_dashboard()
            self._write_dashboard_control_state()
        finally:
            self._cycle_running = False

    def run_once(self) -> None:
        self.run_cycle()
        self.trade_proc.stop()

    def run_forever(self) -> None:
        self._setup_signal_handlers()
        self._start_dashboard_control_service()
        try:
            while not self._stopping:
                self.run_cycle()
                time.sleep(self.poll_sec)
        finally:
            self._stop_dashboard_control_service()
            self.trade_proc.stop()


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    supervisor = Supervisor(args.config)
    if bool(args.once):
        supervisor.run_once()
        return
    supervisor.run_forever()


if __name__ == "__main__":
    main()
