from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import subprocess
import json
import re
import shutil
import time
import yaml

from src.app.supervisor import BASE_DIR as SUPERVISOR_BASE_DIR
from src.app.supervisor import ManagedProcess, Supervisor, parse_args
from src.common.runtime_paths import resolve_scoped_runtime_path, scope_from_ibkr_config
from src.tools.generate_dashboard import build_dashboard, write_dashboard
from src.common.storage import Storage, build_investment_risk_history_row


class SupervisorCliTests(unittest.TestCase):
    def test_managed_process_stop_clears_handle_after_graceful_shutdown(self):
        proc = unittest.mock.Mock()
        proc.poll.side_effect = [None]
        managed = ManagedProcess(name="trade-engine", cmd=["python", "-m", "src.main"], process=proc)

        managed.stop()

        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=10)
        self.assertIsNone(managed.process)

    def test_managed_process_stop_clears_handle_for_already_exited_process(self):
        proc = unittest.mock.Mock(returncode=0)
        proc.poll.return_value = 0
        managed = ManagedProcess(name="trade-engine", cmd=["python", "-m", "src.main"], process=proc)

        managed.stop()

        proc.terminate.assert_not_called()
        self.assertIsNone(managed.process)

    def test_parse_args_accepts_once_and_config(self):
        args = parse_args(["--config", "config/supervisor.yaml", "--once"])
        self.assertEqual(args.config, "config/supervisor.yaml")
        self.assertTrue(args.once)

    def test_first_version_rollout_keeps_cn_recommendation_only_and_enables_hk_submit(self):
        supervisor_cfg = yaml.safe_load((SUPERVISOR_BASE_DIR / "config" / "supervisor.yaml").read_text(encoding="utf-8"))
        markets = {str(item.get("market") or ""): dict(item) for item in list(supervisor_cfg.get("markets") or [])}
        hk_reports = [dict(item) for item in list(markets["HK"].get("reports") or []) if str(item.get("kind") or "") == "investment"]
        cn_reports = [dict(item) for item in list(markets["CN"].get("reports") or []) if str(item.get("kind") or "") == "investment"]

        self.assertTrue(hk_reports)
        self.assertTrue(all(bool(item.get("submit_investment_execution", False)) for item in hk_reports))

        self.assertTrue(cn_reports)
        self.assertTrue(all(bool(item.get("research_only", False)) for item in cn_reports))
        self.assertTrue(all(not bool(item.get("run_investment_execution", False)) for item in cn_reports))

        hk_ibkr_cfg = yaml.safe_load((SUPERVISOR_BASE_DIR / "config" / "ibkr_hk.yaml").read_text(encoding="utf-8"))
        self.assertTrue(bool(hk_ibkr_cfg.get("scanner_enabled", False)))

    def test_supervisor_core_markets_use_same_day_timely_execution(self):
        supervisor_cfg = yaml.safe_load((SUPERVISOR_BASE_DIR / "config" / "supervisor.yaml").read_text(encoding="utf-8"))
        markets = {str(item.get("market") or ""): dict(item) for item in list(supervisor_cfg.get("markets") or [])}
        for market_code in ("HK", "US", "ASX", "XETRA"):
            reports = [dict(item) for item in list(markets[market_code].get("reports") or []) if str(item.get("kind") or "") == "investment"]
            self.assertTrue(reports)
            for report in reports:
                watchlist_yaml = str(report.get("watchlist_yaml", "") or "")
                if "overnight" in watchlist_yaml.lower():
                    continue
                with self.subTest(market=market_code, watchlist=report.get("watchlist_yaml", "")):
                    self.assertNotIn("execution_day_offset", report)
                    self.assertTrue(bool(report.get("run_investment_execution", False)))
                    self.assertTrue(bool(report.get("submit_investment_execution", False)))

    def test_supervisor_core_markets_keep_shadow_dry_run_enabled(self):
        supervisor_cfg = yaml.safe_load((SUPERVISOR_BASE_DIR / "config" / "supervisor.yaml").read_text(encoding="utf-8"))
        markets = {str(item.get("market") or ""): dict(item) for item in list(supervisor_cfg.get("markets") or [])}
        for market_code in ("HK", "US", "ASX", "XETRA"):
            reports = [dict(item) for item in list(markets[market_code].get("reports") or []) if str(item.get("kind") or "") == "investment"]
            self.assertTrue(reports)
            for report in reports:
                watchlist_yaml = str(report.get("watchlist_yaml", "") or "")
                if "overnight" in watchlist_yaml.lower():
                    continue
                with self.subTest(market=market_code, watchlist=watchlist_yaml):
                    self.assertTrue(bool(report.get("run_investment_paper", False)))
                    self.assertTrue(bool(report.get("force_local_paper_ledger", False)))

    def test_supervisor_live_config_points_core_markets_to_live_ibkr_configs(self):
        supervisor_cfg = yaml.safe_load((SUPERVISOR_BASE_DIR / "config" / "supervisor_live.yaml").read_text(encoding="utf-8"))
        markets = {str(item.get("market") or ""): dict(item) for item in list(supervisor_cfg.get("markets") or [])}
        self.assertEqual(set(markets.keys()), {"HK", "US", "ASX", "XETRA"})
        self.assertTrue(bool(supervisor_cfg.get("dashboard_control_enabled", False)))
        self.assertEqual(str(supervisor_cfg.get("dashboard_control_host") or ""), "127.0.0.1")
        self.assertEqual(int(supervisor_cfg.get("dashboard_control_port") or 0), 8766)
        self.assertTrue(bool(supervisor_cfg.get("run_investment_weekly_review", False)))
        self.assertTrue(bool(supervisor_cfg.get("weekly_review_only_when_all_markets_closed", False)))
        self.assertEqual(str(supervisor_cfg.get("dashboard_weekly_review_dir") or ""), "reports_investment_weekly_live")
        self.assertTrue(bool(supervisor_cfg.get("weekly_review_auto_apply_paper", False)))
        self.assertFalse(bool(supervisor_cfg.get("weekly_review_auto_apply_live", False)))
        expected_cfgs = {
            "HK": "config/ibkr_hk_live.yaml",
            "US": "config/ibkr_us_live.yaml",
            "ASX": "config/ibkr_asx_live.yaml",
            "XETRA": "config/ibkr_xetra_live.yaml",
        }
        for market_code, ibkr_cfg in expected_cfgs.items():
            reports = [dict(item) for item in list(markets[market_code].get("reports") or []) if str(item.get("kind") or "") == "investment"]
            self.assertTrue(reports)
            for report in reports:
                with self.subTest(market=market_code, watchlist=report.get("watchlist_yaml", "")):
                    self.assertEqual(str(report.get("ibkr_config") or ""), ibkr_cfg)
                    self.assertTrue(bool(report.get("submit_investment_execution", False)))
                    self.assertTrue(bool(report.get("run_investment_paper", False)))
                    self.assertTrue(bool(report.get("force_local_paper_ledger", False)))

    def test_supervisor_paper_weekly_feedback_builds_effective_overlay_configs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            investment_cfg_path = base / "investment_us.yaml"
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            investment_cfg_path.write_text(
                "\n".join(
                    [
                        "scoring:",
                        "  accumulate_threshold: 0.38",
                        "  execution_ready_threshold: 0.08",
                        "plan:",
                        "  review_window_days: 90",
                    ]
                ),
                encoding="utf-8",
            )
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  shadow_ml_min_score_auto_submit: 0.00",
                        "  shadow_ml_min_positive_prob_auto_submit: 0.50",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU1234567"',
                        f'investment_config: "{investment_cfg_path}"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "shadow_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "shadow_review_action": "WEAK_SIGNAL",
                                "feedback_scope": "paper_only",
                                "execution_shadow_score_delta": 0.0,
                                "execution_shadow_prob_delta": 0.0,
                                "scoring_accumulate_threshold_delta": 0.02,
                                "scoring_execution_ready_threshold_delta": 0.02,
                                "plan_review_window_days_delta": 7,
                                "signal_penalties_json": json.dumps(
                                    [
                                        {
                                            "symbol": "AAPL",
                                            "score_penalty": 0.08,
                                            "execution_penalty": 0.05,
                                            "repeat_count": 2,
                                            "cooldown_days": 14,
                                            "reason": "repeat_shadow_weak_signal",
                                        }
                                    ]
                                ),
                                "feedback_reason": "weak signal",
                            }
                        ],
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_scope": "paper_only",
                                "execution_penalties_json": json.dumps(
                                    [
                                        {
                                            "symbol": "AAPL",
                                            "expected_cost_bps_add": 8.0,
                                            "slippage_proxy_bps_add": 6.0,
                                            "execution_penalty": 0.03,
                                            "score_penalty": 0.01,
                                            "session_count": 2,
                                            "reason": "repeat_execution_hotspot",
                                        }
                                    ]
                                ),
                                "feedback_reason": "execution hotspot",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        f'weekly_review_overlay_dir: "{base / "auto_feedback_configs"}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            effective_investment = supervisor._effective_investment_config_path(item, "US")
            effective_execution = supervisor._effective_execution_config_path(item, "US")
            self.assertNotEqual(effective_investment, investment_cfg_path.resolve())
            self.assertNotEqual(effective_execution, execution_cfg_path.resolve())
            effective_investment_cfg = yaml.safe_load(effective_investment.read_text(encoding="utf-8"))
            effective_execution_cfg = yaml.safe_load(effective_execution.read_text(encoding="utf-8"))
            self.assertAlmostEqual(float(effective_investment_cfg["scoring"]["accumulate_threshold"]), 0.40, places=6)
            self.assertAlmostEqual(float(effective_investment_cfg["scoring"]["execution_ready_threshold"]), 0.10, places=6)
            self.assertEqual(int(effective_investment_cfg["plan"]["review_window_days"]), 97)
            self.assertEqual(effective_investment_cfg["weekly_feedback"]["signal_penalties"][0]["symbol"], "AAPL")
            self.assertEqual(effective_investment_cfg["weekly_feedback"]["execution_penalties"][0]["symbol"], "AAPL")
            self.assertAlmostEqual(float(effective_execution_cfg["execution"]["shadow_ml_min_score_auto_submit"]), 0.00, places=6)
            self.assertAlmostEqual(float(effective_execution_cfg["execution"]["shadow_ml_min_positive_prob_auto_submit"]), 0.50, places=6)
            self.assertEqual(effective_execution_cfg["execution"]["execution_hotspot_penalties"][0]["symbol"], "AAPL")
            self.assertEqual(effective_execution_cfg["weekly_feedback"]["execution_hotspot_penalties"][0]["symbol"], "AAPL")

    def test_supervisor_paper_weekly_feedback_decays_previous_execution_penalties(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            investment_cfg_path = base / "investment_us.yaml"
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            investment_cfg_path.write_text(
                "\n".join(
                    [
                        "scoring:",
                        "  accumulate_threshold: 0.38",
                        "  execution_ready_threshold: 0.08",
                        "plan:",
                        "  review_window_days: 90",
                    ]
                ),
                encoding="utf-8",
            )
            execution_cfg_path.write_text("execution: {}\n", encoding="utf-8")
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU1234567"',
                        f'investment_config: "{investment_cfg_path}"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps({"shadow_feedback_summary": [], "execution_feedback_summary": []}),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        f'weekly_review_overlay_dir: "{base / "auto_feedback_configs"}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            overlay_dir = supervisor._weekly_feedback_overlay_dir(item, "US")
            overlay_dir.mkdir(parents=True, exist_ok=True)
            (overlay_dir / "investment_auto_feedback.yaml").write_text(
                yaml.safe_dump(
                    {
                        "weekly_feedback": {
                            "portfolio_id": "US:watchlist",
                            "market": "US",
                            "feedback_scope": "paper_only",
                            "execution_penalties": [
                                {
                                    "symbol": "MSFT",
                                    "score_penalty": 0.02,
                                    "execution_penalty": 0.04,
                                    "expected_cost_bps_add": 10.0,
                                    "slippage_proxy_bps_add": 8.0,
                                    "session_count": 2,
                                    "reason": "repeat_execution_hotspot",
                                }
                            ],
                        }
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )
            effective_investment = supervisor._effective_investment_config_path(item, "US")
            effective_investment_cfg = yaml.safe_load(effective_investment.read_text(encoding="utf-8"))
            penalty = effective_investment_cfg["weekly_feedback"]["execution_penalties"][0]
            self.assertEqual(str(effective_investment_cfg["weekly_feedback"]["execution_feedback_action"]), "DECAY")
            self.assertEqual(str(effective_investment_cfg["weekly_feedback"]["portfolio_id"]), "US:watchlist")
            self.assertEqual(str(penalty["symbol"]), "MSFT")
            self.assertLess(float(penalty["expected_cost_bps_add"]), 10.0)
            self.assertLess(float(penalty["slippage_proxy_bps_add"]), 8.0)
            self.assertLess(float(penalty["execution_penalty"]), 0.04)
            self.assertEqual(str(penalty["reason"]), "execution_hotspot_decay")
            effective_execution = supervisor._effective_execution_config_path(item, "US")
            effective_execution_cfg = yaml.safe_load(effective_execution.read_text(encoding="utf-8"))
            execution_penalty = effective_execution_cfg["execution"]["execution_hotspot_penalties"][0]
            self.assertEqual(str(effective_execution_cfg["weekly_feedback"]["execution_feedback_action"]), "DECAY")
            self.assertEqual(str(execution_penalty["symbol"]), "MSFT")
            self.assertLess(float(execution_penalty["expected_cost_bps_add"]), 10.0)
            self.assertEqual(str(execution_penalty["reason"]), "execution_hotspot_decay")

    def test_supervisor_live_weekly_feedback_keeps_base_configs_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly_live"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            investment_cfg_path = base / "investment_us.yaml"
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us_live.yaml"
            investment_cfg_path.write_text("scoring:\n  accumulate_threshold: 0.38\n", encoding="utf-8")
            execution_cfg_path.write_text("execution:\n  shadow_ml_min_positive_prob_auto_submit: 0.50\n", encoding="utf-8")
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "live"',
                        'execution_mode: "investment_only"',
                        'account_id: "U1234567"',
                        f'investment_config: "{investment_cfg_path}"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "shadow_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "shadow_review_action": "REVIEW_THRESHOLD",
                                "execution_shadow_score_delta": -0.02,
                                "execution_shadow_prob_delta": -0.02,
                                "feedback_reason": "threshold review",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor_live.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            self.assertEqual(supervisor._effective_investment_config_path(item, "US"), investment_cfg_path.resolve())
            self.assertEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())

    def test_supervisor_live_weekly_feedback_can_apply_after_dashboard_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly_live"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            summary_dir = base / "reports_supervisor"
            investment_cfg_path = base / "investment_us.yaml"
            execution_cfg_path = base / "investment_execution_us.yaml"
            paper_cfg_path = base / "investment_paper_us.yaml"
            ibkr_cfg_path = base / "ibkr_us_live.yaml"
            investment_cfg_path.write_text("scoring:\n  accumulate_threshold: 0.38\n  execution_ready_threshold: 0.08\n", encoding="utf-8")
            execution_cfg_path.write_text("execution:\n  shadow_ml_min_positive_prob_auto_submit: 0.50\n", encoding="utf-8")
            paper_cfg_path.write_text("paper:\n  max_net_exposure: 1.0\n", encoding="utf-8")
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "live"',
                        'execution_mode: "investment_only"',
                        'account_id: "U1234567"',
                        f'investment_config: "{investment_cfg_path}"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                        f'investment_paper_config: "{paper_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "shadow_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "shadow_review_action": "REVIEW_THRESHOLD",
                                "execution_shadow_score_delta": -0.02,
                                "execution_shadow_prob_delta": -0.02,
                                "feedback_reason": "threshold review",
                            }
                        ],
                        "risk_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "risk_feedback_action": "TIGHTEN",
                                "paper_max_net_exposure_delta": -0.10,
                                "feedback_reason": "risk tighten",
                            }
                        ],
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "live_manual_confirm",
                                "execution_feedback_action": "TIGHTEN",
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                                "execution_max_slices_per_symbol_delta": 1,
                                "feedback_reason": "execution tighten",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor_live.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_control_enabled: true",
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = supervisor.markets[0].reports[0]
            self.assertFalse(supervisor._weekly_feedback_auto_apply_enabled(item, "US"))
            self.assertEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())
            result = supervisor._dashboard_control_apply_weekly_feedback({"portfolio_id": "US:watchlist"})
            self.assertTrue(bool(result.get("ok", False)))
            self.assertTrue(str(result.get("weekly_feedback_signature", "") or "").strip())
            self.assertTrue(supervisor._weekly_feedback_auto_apply_enabled(item, "US"))
            self.assertNotEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())
            self.assertNotEqual(supervisor._effective_investment_config_path(item, "US"), investment_cfg_path.resolve())
            self.assertNotEqual(supervisor._effective_paper_config_path(item, "US"), paper_cfg_path.resolve())

    def test_supervisor_paper_weekly_execution_feedback_builds_effective_execution_overlay(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU7654321"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                                "execution_max_slices_per_symbol_delta": 1,
                                "execution_open_session_participation_scale_delta": -0.05,
                                "execution_midday_session_participation_scale_delta": -0.03,
                                "execution_close_session_participation_scale_delta": -0.04,
                                "feedback_reason": "actual execution cost above plan",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            effective_execution = supervisor._effective_execution_config_path(item, "US")
            self.assertNotEqual(effective_execution, execution_cfg_path.resolve())
            effective_execution_cfg = yaml.safe_load(effective_execution.read_text(encoding="utf-8"))
            execution = dict(effective_execution_cfg.get("execution") or {})
            self.assertAlmostEqual(float(execution["adv_max_participation_pct"]), 0.04, places=6)
            self.assertAlmostEqual(float(execution["adv_split_trigger_pct"]), 0.017, places=6)
            self.assertEqual(int(execution["max_slices_per_symbol"]), 5)
            self.assertAlmostEqual(float(execution["open_session_participation_scale"]), 0.65, places=6)
            self.assertEqual(str(effective_execution_cfg["weekly_feedback"]["execution_feedback_action"]), "TIGHTEN")

    def test_supervisor_paper_weekly_execution_feedback_scales_delta_by_confidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU7654321"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_base_confidence": 0.8,
                                "feedback_base_confidence_label": "HIGH",
                                "feedback_calibration_score": 0.4,
                                "feedback_calibration_label": "LOW",
                                "feedback_confidence": 0.5,
                                "feedback_confidence_label": "MEDIUM",
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.004,
                                "execution_max_slices_per_symbol_delta": 2,
                                "execution_open_session_participation_scale_delta": -0.06,
                                "execution_midday_session_participation_scale_delta": -0.02,
                                "execution_close_session_participation_scale_delta": -0.04,
                                "feedback_reason": "sample not yet full",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            effective_execution_cfg = yaml.safe_load(
                supervisor._effective_execution_config_path(item, "US").read_text(encoding="utf-8")
            )
            execution = dict(effective_execution_cfg.get("execution") or {})
            self.assertAlmostEqual(float(execution["adv_max_participation_pct"]), 0.045, places=6)
            self.assertAlmostEqual(float(execution["adv_split_trigger_pct"]), 0.018, places=6)
            self.assertEqual(int(execution["max_slices_per_symbol"]), 5)
            self.assertAlmostEqual(float(effective_execution_cfg["weekly_feedback"]["execution_feedback_base_confidence"]), 0.8, places=6)
            self.assertAlmostEqual(float(effective_execution_cfg["weekly_feedback"]["execution_feedback_calibration_score"]), 0.4, places=6)
            self.assertAlmostEqual(float(effective_execution_cfg["weekly_feedback"]["execution_feedback_confidence"]), 0.5, places=6)

    def test_supervisor_paper_weekly_feedback_respects_suggest_only_automation_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU7654321"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_base_confidence": 0.54,
                                "feedback_confidence": 0.52,
                                "feedback_calibration_score": 0.50,
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                            }
                        ],
                        "feedback_automation_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_kind": "execution",
                                "calibration_apply_mode": "SUGGEST_ONLY",
                                "calibration_apply_mode_label": "建议确认",
                                "automation_reason": "周报样本已有依据，但 outcome 校准样本还不够稳。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            self.assertEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())

    def test_supervisor_live_weekly_feedback_can_apply_suggest_only_after_dashboard_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly_live"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            summary_dir = base / "reports_supervisor"
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us_live.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "live"',
                        'execution_mode: "investment_only"',
                        'account_id: "U7654321"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "live_manual_confirm",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_base_confidence": 0.54,
                                "feedback_confidence": 0.52,
                                "feedback_calibration_score": 0.50,
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                            }
                        ],
                        "feedback_automation_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_kind": "execution",
                                "calibration_apply_mode": "SUGGEST_ONLY",
                                "calibration_apply_mode_label": "建议确认",
                                "automation_reason": "live 先人工确认。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor_live.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_control_enabled: true",
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = supervisor.markets[0].reports[0]
            self.assertEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())
            result = supervisor._dashboard_control_apply_weekly_feedback({"portfolio_id": "US:watchlist"})
            self.assertTrue(bool(result.get("ok", False)))
            self.assertNotEqual(supervisor._effective_execution_config_path(item, "US"), execution_cfg_path.resolve())

    def test_supervisor_paper_weekly_execution_feedback_can_target_open_session_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU7654321"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "execution_adv_max_participation_pct_delta": -0.005,
                                "execution_adv_split_trigger_pct_delta": 0.0,
                                "execution_max_slices_per_symbol_delta": 0,
                                "execution_open_session_participation_scale_delta": -0.07,
                                "execution_midday_session_participation_scale_delta": 0.0,
                                "execution_close_session_participation_scale_delta": 0.0,
                                "dominant_execution_session_bucket": "OPEN",
                                "dominant_execution_session_label": "开盘",
                                "execution_session_feedback_json": json.dumps(
                                    [
                                        {
                                            "session_bucket": "OPEN",
                                            "session_label": "开盘",
                                            "session_action": "TIGHTEN",
                                            "scale_delta": -0.07,
                                            "reason": "开盘实际成本高于计划。",
                                        }
                                    ],
                                    ensure_ascii=False,
                                ),
                                "feedback_reason": "总执行成本之外，开盘是本周最需要关注的执行时段。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            effective_execution = supervisor._effective_execution_config_path(item, "US")
            effective_execution_cfg = yaml.safe_load(effective_execution.read_text(encoding="utf-8"))
            execution = dict(effective_execution_cfg.get("execution") or {})
            self.assertAlmostEqual(float(execution["adv_max_participation_pct"]), 0.045, places=6)
            self.assertAlmostEqual(float(execution["open_session_participation_scale"]), 0.63, places=6)
            self.assertAlmostEqual(float(execution["midday_session_participation_scale"]), 1.00, places=6)
            self.assertAlmostEqual(float(execution["close_session_participation_scale"]), 0.85, places=6)
            self.assertEqual(str(effective_execution_cfg["weekly_feedback"]["execution_dominant_session_label"]), "开盘")

    def test_supervisor_paper_weekly_risk_feedback_builds_effective_paper_overlay(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            paper_cfg_path = base / "investment_paper_us.yaml"
            execution_cfg_path = base / "investment_execution_us.yaml"
            investment_cfg_path = base / "investment_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            paper_cfg_path.write_text(
                "\n".join(
                    [
                        "paper:",
                        "  max_single_weight: 0.22",
                        "  max_sector_weight: 0.40",
                        "  max_net_exposure: 0.88",
                        "  max_gross_exposure: 0.95",
                        "  max_short_exposure: 0.35",
                        "  correlation_soft_limit: 0.62",
                    ]
                ),
                encoding="utf-8",
            )
            execution_cfg_path.write_text("execution:\n  max_orders_per_run: 6\n", encoding="utf-8")
            investment_cfg_path.write_text("scoring:\n  accumulate_threshold: 0.38\n", encoding="utf-8")
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU1234567"',
                        f'investment_config: "{investment_cfg_path}"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                        f'investment_paper_config: "{paper_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "risk_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "risk_feedback_action": "TIGHTEN",
                                "paper_max_single_weight_delta": -0.02,
                                "paper_max_sector_weight_delta": -0.04,
                                "paper_max_net_exposure_delta": -0.05,
                                "paper_max_gross_exposure_delta": -0.06,
                                "paper_max_short_exposure_delta": -0.02,
                                "paper_correlation_soft_limit_delta": -0.03,
                                "feedback_reason": "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = dict(supervisor.markets[0].reports[0])
            effective_paper = supervisor._effective_paper_config_path(item, "US")
            self.assertNotEqual(effective_paper, paper_cfg_path.resolve())
            effective_paper_cfg = yaml.safe_load(effective_paper.read_text(encoding="utf-8"))
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["max_single_weight"]), 0.20, places=6)
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["max_sector_weight"]), 0.36, places=6)
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["max_net_exposure"]), 0.83, places=6)
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["max_gross_exposure"]), 0.89, places=6)
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["max_short_exposure"]), 0.33, places=6)
            self.assertAlmostEqual(float(effective_paper_cfg["paper"]["correlation_soft_limit"]), 0.59, places=6)
            self.assertEqual(effective_paper_cfg["risk_feedback"]["risk_feedback_action"], "TIGHTEN")

    def test_supervisor_paper_auto_writes_feedback_threshold_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            ibkr_cfg_path = base / "ibkr_us.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU1234567"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "feedback_threshold_suggestion_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "suggestion_action": "RELAX_AUTO_APPLY",
                                "suggested_auto_confidence": 0.57,
                                "suggested_auto_base_confidence": 0.73,
                                "suggested_auto_calibration_score": 0.54,
                                "suggested_auto_maturity_ratio": 0.55,
                            }
                        ],
                        "feedback_threshold_tuning_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "suggestion_action": "KEEP_RELAX",
                                "suggestion_label": "继续放宽试运行",
                                "reason": "放宽后的 cohort 已连续出现改善，可继续保留当前放宽。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            override_path = supervisor._refresh_weekly_feedback_threshold_overrides()
            override_cfg = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
            self.assertAlmostEqual(
                float(override_cfg["markets"]["US"]["execution"]["auto_confidence"]),
                0.57,
                places=6,
            )

    def test_supervisor_threshold_tuning_can_revert_relax_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            ibkr_cfg_path = base / "ibkr_us.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU1234567"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "feedback_threshold_suggestion_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "suggestion_action": "RELAX_AUTO_APPLY",
                                "suggested_auto_confidence": 0.57,
                                "suggested_auto_base_confidence": 0.73,
                                "suggested_auto_calibration_score": 0.54,
                                "suggested_auto_maturity_ratio": 0.55,
                            }
                        ],
                        "feedback_threshold_tuning_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "suggestion_action": "REVERT_RELAX",
                                "suggestion_label": "收回放宽",
                                "reason": "放宽后的 cohort 已出现恶化，优先考虑收回这轮放宽。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            override_path = supervisor._refresh_weekly_feedback_threshold_overrides()
            override_cfg = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
            self.assertEqual(dict(override_cfg.get("markets") or {}), {})

    def test_supervisor_live_feedback_threshold_override_requires_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            weekly_dir = base / "reports_investment_weekly"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            ibkr_cfg_path = base / "ibkr_us_live.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "live"',
                        'execution_mode: "investment_only"',
                        'account_id: "U1234567"',
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "feedback_threshold_suggestion_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "suggestion_action": "TIGHTEN_AUTO_APPLY",
                                "suggested_auto_confidence": 0.64,
                                "suggested_auto_base_confidence": 0.80,
                                "suggested_auto_calibration_score": 0.59,
                                "suggested_auto_maturity_ratio": 0.65,
                                "reason": "自动应用后出现恶化样本。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "weekly_review_auto_apply_live: false",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            override_path = supervisor._refresh_weekly_feedback_threshold_overrides()
            override_cfg = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
            self.assertEqual(dict(override_cfg.get("markets") or {}), {})

            item = supervisor.markets[0].reports[0]
            signature = supervisor._weekly_feedback_signature_for_item(item, "US")
            self.assertTrue(signature)
            item["_dashboard_control_weekly_feedback_confirmed_signature"] = signature
            item["_dashboard_control_weekly_feedback_confirmed_ts"] = "2026-03-25T10:00:00+11:00"
            override_path = supervisor._refresh_weekly_feedback_threshold_overrides(target_markets={"US"})
            override_cfg = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
            self.assertAlmostEqual(
                float(override_cfg["markets"]["US"]["execution"]["auto_confidence"]),
                0.64,
                places=6,
            )

    def test_supervisor_runs_weekly_review_when_all_markets_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            weekly_dir = base / "reports_investment_weekly"
            labeling_dir = base / "reports_investment_labeling"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        f'dashboard_labeling_dir: "{labeling_dir}"',
                        "run_investment_labeling: true",
                        "labeling_interval_min: 180",
                        "labeling_only_when_all_markets_closed: true",
                        "run_investment_weekly_review: true",
                        "weekly_review_interval_min: 180",
                        "weekly_review_only_when_all_markets_closed: true",
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 19, 22, 0, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_run_cmd", return_value=True) as mock_run_cmd, patch.object(
                supervisor, "_refresh_dashboard", return_value=True
            ):
                supervisor.run_cycle(now)
            labeling_calls = [call for call in mock_run_cmd.call_args_list if str(call.args[0]).startswith("label_investment_snapshots")]
            review_calls = [call for call in mock_run_cmd.call_args_list if str(call.args[0]).startswith("review_investment_weekly")]
            self.assertEqual(len(labeling_calls), 1)
            self.assertEqual(len(review_calls), 1)
            labeling_cmd = list(labeling_calls[0].args[1])
            review_cmd = list(review_calls[0].args[1])
            self.assertIn("--out_dir", labeling_cmd)
            self.assertIn(str(labeling_dir.resolve()), labeling_cmd)
            self.assertIn("--out_dir", review_cmd)
            self.assertIn(str(weekly_dir.resolve()), review_cmd)
            self.assertIn("--labeling_dir", review_cmd)
            self.assertIn(str(labeling_dir.resolve()), review_cmd)

    def test_supervisor_skips_weekly_review_while_any_market_is_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            weekly_dir = base / "reports_investment_weekly"
            labeling_dir = base / "reports_investment_labeling"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        f'dashboard_labeling_dir: "{labeling_dir}"',
                        "run_investment_labeling: true",
                        "labeling_only_when_all_markets_closed: true",
                        "run_investment_weekly_review: true",
                        "weekly_review_only_when_all_markets_closed: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports: []",
                        "    trading:",
                        "      enabled: true",
                        '      start: "00:00"',
                        '      end: "23:59"',
                        "      weekdays: [0,1,2,3,4,5,6]",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 19, 22, 0, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_run_cmd", return_value=True) as mock_run_cmd, patch.object(
                supervisor, "_refresh_dashboard", return_value=True
            ), patch.object(supervisor, "_active_live_market", return_value=None):
                supervisor.run_cycle(now)
            labeling_calls = [call for call in mock_run_cmd.call_args_list if str(call.args[0]).startswith("label_investment_snapshots")]
            review_calls = [call for call in mock_run_cmd.call_args_list if str(call.args[0]).startswith("review_investment_weekly")]
            self.assertEqual(labeling_calls, [])
            self.assertEqual(review_calls, [])

    def test_supervisor_forces_weekly_review_after_labeling_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        "run_investment_labeling: true",
                        "run_investment_weekly_review: true",
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 19, 22, 0, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_run_investment_labeling", return_value=True) as mock_labeling, patch.object(
                supervisor, "_run_investment_weekly_review", return_value=True
            ) as mock_weekly, patch.object(supervisor, "_refresh_dashboard", return_value=True):
                supervisor.run_cycle(now)
            mock_labeling.assert_called_once()
            mock_weekly.assert_called_once()
            self.assertTrue(bool(mock_weekly.call_args.kwargs.get("force", False)))

    def test_dashboard_control_run_weekly_review_forces_review_and_refreshes_dashboard(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_control_enabled: true",
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with patch.object(supervisor, "_run_investment_weekly_review", return_value=True) as mock_weekly, patch.object(
                supervisor, "_refresh_dashboard", return_value=True
            ) as mock_refresh:
                result = supervisor._dashboard_control_run_weekly_review()
                self.assertTrue(result["ok"])
                for _ in range(40):
                    if mock_weekly.called:
                        break
                    time.sleep(0.01)
            mock_weekly.assert_called()
            self.assertTrue(bool(mock_weekly.call_args.kwargs.get("force", False)))
            mock_refresh.assert_called()

    def test_dashboard_control_state_write_ignores_oserror(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_control_enabled: true",
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with self.assertLogs("app.supervisor", level="WARNING") as logs, patch.object(
                Path,
                "write_text",
                side_effect=OSError(22, "Invalid argument"),
            ):
                supervisor._write_dashboard_control_state()
            self.assertIn("Failed to write dashboard control state", "\n".join(logs.output))

    def test_dashboard_control_run_preflight_generates_report_and_refreshes_dashboard(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            preflight_dir = base / "reports_preflight"
            runtime_root = base / "runtime_data" / "paper_test"
            runtime_root.mkdir(parents=True, exist_ok=True)
            (runtime_root / "audit.db").write_text("", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_preflight_dir: "{preflight_dir}"',
                        "scope_summary_out_dir: true",
                        "dashboard_control_enabled: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "cn"',
                        '    market: "CN"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        ibkr_config: "{base / "ibkr_cn.yaml"}"',
                        '        watchlist_yaml: "config/watchlists/cn_top_quality.yaml"',
                        '        out_dir: "reports_investment_cn"',
                    ]
                ),
                encoding="utf-8",
            )
            (base / "ibkr_cn.yaml").write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "paper_test"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with patch.object(supervisor, "_refresh_dashboard", return_value=True) as mock_refresh:
                result = supervisor._dashboard_control_run_preflight()
                self.assertTrue(result["ok"])
                for _ in range(40):
                    if (preflight_dir / "supervisor_preflight_summary.json").exists():
                        break
                    time.sleep(0.01)
            self.assertTrue((preflight_dir / "supervisor_preflight_summary.json").exists())
            mock_refresh.assert_called()

    def test_dashboard_control_execution_mode_switch_restores_base_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_control_enabled: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        '        out_dir: "reports_investment"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                        "        run_investment_guard: true",
                        "        submit_investment_guard: true",
                        "        run_investment_opportunity: true",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            portfolio_id = "US:watchlist"

            review_only = supervisor._set_dashboard_execution_mode(portfolio_id=portfolio_id, mode="REVIEW_ONLY")
            self.assertTrue(review_only["ok"])
            row = supervisor._dashboard_control_portfolios()[portfolio_id]
            self.assertEqual(row["execution_control_mode"], "REVIEW_ONLY")
            self.assertTrue(bool(row["run_investment_execution"]))
            self.assertFalse(bool(row["submit_investment_execution"]))
            self.assertTrue(bool(row["run_investment_guard"]))
            self.assertFalse(bool(row["submit_investment_guard"]))

            paused = supervisor._set_dashboard_execution_mode(portfolio_id=portfolio_id, mode="PAUSED")
            self.assertTrue(paused["ok"])
            row = supervisor._dashboard_control_portfolios()[portfolio_id]
            self.assertEqual(row["execution_control_mode"], "PAUSED")
            self.assertFalse(bool(row["run_investment_execution"]))
            self.assertFalse(bool(row["submit_investment_execution"]))
            self.assertFalse(bool(row["run_investment_guard"]))
            self.assertFalse(bool(row["submit_investment_guard"]))

            auto = supervisor._set_dashboard_execution_mode(portfolio_id=portfolio_id, mode="AUTO")
            self.assertTrue(auto["ok"])
            row = supervisor._dashboard_control_portfolios()[portfolio_id]
            self.assertEqual(row["execution_control_mode"], "AUTO")
            self.assertTrue(bool(row["run_investment_execution"]))
            self.assertTrue(bool(row["submit_investment_execution"]))
            self.assertTrue(bool(row["run_investment_guard"]))
            self.assertTrue(bool(row["submit_investment_guard"]))

    def test_dashboard_loads_execution_weekly_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            execution_dir = base / "reports_investment_execution"
            execution_dir.mkdir(parents=True, exist_ok=True)
            (execution_dir / "investment_execution_weekly_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit,fill_rate,avg_actual_slippage_bps",
                        "2026-W11,2026-03-09,5,2,7,4,3,2,1,0,3,12.5,45.7,0.75,0.50,0.50,-3.2",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_execution_kpi_dir: "{execution_dir}"',
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["execution_weekly"]["week"], "2026-W11")
            self.assertEqual(payload["execution_weekly"]["filled_order_rows"], 3)
            self.assertEqual(payload["execution_weekly"]["filled_with_audit_rows"], 2)
            self.assertAlmostEqual(payload["execution_weekly"]["fill_rate_status"], 0.75, places=6)
            self.assertAlmostEqual(payload["execution_weekly"]["fill_rate_audit"], 0.50, places=6)
            self.assertAlmostEqual(payload["execution_weekly"]["fill_rate"], 0.50, places=6)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-simple-section="weekly-execution"', html_text)
            self.assertIn("2026-W11", html_text)

    def test_dashboard_loads_execution_weekly_groups(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            execution_dir = base / "reports_investment_execution"
            execution_dir.mkdir(parents=True, exist_ok=True)
            (execution_dir / "investment_execution_weekly_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W11,2026-03-09,US,US:watchlist,3,1,4,2,1,1,1,0,1,12.5,45.7,0.50,0.50",
                        "2026-W11,2026-03-09,ASX,ASX:asx_top_quality,2,1,1,1,0,0,1,0,0,0.0,0.0,0.00,0.00",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_execution_kpi_dir: "{execution_dir}"',
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            groups = payload["execution_weekly_groups"]
            self.assertEqual(len(groups), 2)
            self.assertEqual(groups[0]["market"], "ASX")
            self.assertEqual(groups[0]["watchlist"], "asx_top_quality")
            self.assertEqual(groups[1]["market"], "US")
            self.assertEqual(groups[1]["watchlist"], "watchlist")

    def test_dashboard_prefers_weekly_review_execution_summary_over_stale_execution_kpi(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            execution_dir = base / "reports_investment_execution"
            weekly_review_dir = base / "reports_investment_weekly"
            execution_dir.mkdir(parents=True, exist_ok=True)
            weekly_review_dir.mkdir(parents=True, exist_ok=True)
            (execution_dir / "investment_execution_weekly_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W11,2026-03-09,HK,HK:resolved_hk_top100_bluechip,7,2,7,7,0,2,0,7,2,108.22,-108.22,0.0,0.286",
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_review_dir / "weekly_execution_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,portfolio_id,market,execution_run_rows,execution_runs,submitted_runs,planned_order_rows,execution_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W13,2026-03-23,HK:resolved_hk_top100_bluechip,HK,72,72,6,36,36,0,0,0,7,0,0,0.0,0.0,0.0,0.0",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_execution_kpi_dir: "{execution_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_review_dir}"',
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["execution_weekly"]["week"], "2026-W13")
            self.assertEqual(payload["execution_weekly"]["planned_order_rows"], 36)
            self.assertEqual(payload["execution_weekly"]["submitted_order_rows"], 0)
            self.assertEqual(payload["execution_weekly"]["blocked_opportunity_rows"], 7)
            self.assertAlmostEqual(payload["execution_weekly"]["realized_net_pnl"], 0.0, places=6)

    def test_dashboard_execution_weekly_display_includes_card_without_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            execution_dir = base / "reports_investment_execution"
            report_root = base / "reports_investment_asx"
            execution_dir.mkdir(parents=True, exist_ok=True)
            report_dir = report_root / "asx_top_quality"
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (execution_dir / "investment_execution_weekly_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W11,2026-03-09,US,US:watchlist,3,1,4,2,1,1,1,0,1,12.5,45.7,0.50,0.50",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_execution_kpi_dir: "{execution_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "asx"',
                        '    market: "ASX"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlists/asx_top_quality.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            rows = payload["execution_weekly_display"]
            asx_row = next((row for row in rows if row["market"] == "ASX"), None)
            self.assertIsNotNone(asx_row)
            self.assertEqual(asx_row["watchlist"], "asx_top_quality")
            self.assertEqual(asx_row["submitted_order_rows"], 0)
            self.assertEqual(payload["cards"][0]["execution_weekly_row"]["watchlist"], "asx_top_quality")

    def test_dashboard_surfaces_shadow_and_size_review_breakdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_paper_summary.json").write_text("{}", encoding="utf-8")
            (report_dir / "investment_guard_summary.json").write_text("{}", encoding="utf-8")
            (report_dir / "investment_opportunity_summary.json").write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "broker_equity": 100000.0,
                        "broker_cash": 65000.0,
                        "target_capital": 30000.0,
                        "idle_capital_gap": 9000.0,
                        "blocked_manual_review_order_count": 3,
                        "blocked_shadow_review_order_count": 2,
                        "blocked_size_review_order_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["review_overview"][0]["shadow_review_count"], 2)
            self.assertEqual(payload["review_overview"][0]["size_review_count"], 1)
            self.assertEqual(payload["review_overview"][0]["total_review_count"], 3)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("人工审核队列", html_text)
            self.assertIn("shadow_review", html_text)
            self.assertIn("size_review", html_text)
            self.assertIn("shadow=2 / size=1 / total=3", html_text)

    def test_dashboard_surfaces_shadow_review_history_and_repeat_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "broker_equity": 100000.0,
                        "broker_cash": 70000.0,
                        "blocked_manual_review_order_count": 2,
                        "blocked_shadow_review_order_count": 2,
                        "blocked_size_review_order_count": 0,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            for idx, (ts_value, symbol, order_value) in enumerate(
                [
                    ("2026-03-18T10:05:00+00:00", "AAPL", 5400.0),
                    ("2026-03-17T10:05:00+00:00", "AAPL", 5100.0),
                    ("2026-03-16T10:05:00+00:00", "MSFT", 4300.0),
                ],
                start=1,
            ):
                storage.insert_investment_execution_order(
                    {
                        "run_id": f"US-exec-{idx}",
                        "ts": ts_value,
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "symbol": symbol,
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 10.0,
                        "delta_qty": 10.0,
                        "ref_price": 100.0,
                        "target_weight": 0.05,
                        "order_value": order_value,
                        "order_type": "LMT",
                        "broker_order_id": 0,
                        "status": "REVIEW_REQUIRED",
                        "reason": "manual_review|shadow_ml_review",
                        "details": json.dumps(
                            {
                                "submitted": False,
                                "manual_review_status": "REVIEW_REQUIRED",
                                "manual_review_reason": "shadow ML burn-in requires review",
                                "shadow_review_status": "REVIEW_REQUIRED",
                                "shadow_review_reason": f"{symbol} shadow score below burn-in threshold",
                            },
                            ensure_ascii=False,
                        ),
                    }
                )
            storage.insert_investment_execution_order(
                {
                    "run_id": "US-exec-size-only",
                    "ts": "2026-03-18T11:05:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "NVDA",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 10.0,
                    "delta_qty": 10.0,
                    "ref_price": 100.0,
                    "target_weight": 0.05,
                    "order_value": 9000.0,
                    "order_type": "LMT",
                    "broker_order_id": 0,
                    "status": "REVIEW_REQUIRED",
                    "reason": "manual_review|max_order_value_pct",
                    "details": json.dumps(
                        {
                            "submitted": False,
                            "manual_review_status": "REVIEW_REQUIRED",
                            "manual_review_reason": "size review only",
                            "shadow_review_status": "",
                            "shadow_review_reason": "",
                        },
                        ensure_ascii=False,
                    ),
                }
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["trade_cards"][0]
            self.assertEqual([row["symbol"] for row in card["shadow_review_recent_rows"]], ["AAPL", "AAPL", "MSFT"])
            self.assertEqual(card["shadow_review_repeat_rows"][0]["symbol"], "AAPL")
            self.assertEqual(card["shadow_review_repeat_rows"][0]["repeat_count"], 2)
            self.assertEqual(payload["shadow_review_overview"][0]["symbol"], "AAPL")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Shadow Review 历史重点", html_text)
            self.assertIn("Shadow Review 最近记录", html_text)
            self.assertIn("Shadow Review 重复拦截", html_text)
            self.assertIn("AAPL shadow score below burn-in threshold", html_text)

    def test_dashboard_surfaces_weekly_shadow_strategy_upgrade_suggestions(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            db_path = base / "audit.db"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            threshold_override_path = weekly_dir / "weekly_feedback_threshold_overrides.yaml"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            storage = Storage(str(db_path))
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W11",
                    "week_start": "2026-03-09",
                    "window_start": "2026-03-03T00:00:00+00:00",
                    "window_end": "2026-03-10T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "SUGGEST_ONLY",
                    "calibration_apply_mode_label": "建议确认",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.70,
                    "feedback_calibration_score": 0.54,
                    "feedback_confidence": 0.61,
                    "feedback_sample_count": 2,
                    "feedback_calibration_sample_count": 12,
                    "outcome_maturity_ratio": 0.45,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 9,
                    "outcome_ready_estimate_end_ts": "2026-03-19T00:00:00+00:00",
                    "alert_bucket": "SOON",
                    "details": {"automation_reason": "上一周样本仍在成熟。"},
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W12",
                    "week_start": "2026-03-16",
                    "window_start": "2026-03-10T00:00:00+00:00",
                    "window_end": "2026-03-17T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.78,
                    "feedback_calibration_score": 0.58,
                    "feedback_confidence": 0.70,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 16,
                    "outcome_maturity_ratio": 0.66,
                    "outcome_maturity_label": "LATE",
                    "outcome_pending_sample_count": 8,
                    "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数上周已开始自动应用。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "planned_execution_cost_total": 21.4,
                            "execution_cost_total": 41.4,
                            "execution_cost_gap": 20.0,
                            "avg_expected_cost_bps": 18.4,
                            "avg_actual_slippage_bps": 35.2,
                            "dominant_execution_session_label": "开盘",
                            "execution_feedback_action": "TIGHTEN",
                        },
                        "effect_snapshot_week_label": "2026-W12",
                    },
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W13",
                    "week_start": "2026-03-23",
                    "window_start": "2026-03-17T00:00:00+00:00",
                    "window_end": "2026-03-24T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.80,
                    "feedback_calibration_score": 0.60,
                    "feedback_confidence": 0.72,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 18,
                    "outcome_maturity_ratio": 0.72,
                    "outcome_maturity_label": "LATE",
                    "outcome_pending_sample_count": 7,
                    "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数本周已满足 paper 自动应用条件。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "planned_execution_cost_total": 21.4,
                            "execution_cost_total": 34.9,
                            "execution_cost_gap": 13.5,
                            "avg_expected_cost_bps": 18.4,
                            "avg_actual_slippage_bps": 29.7,
                            "dominant_execution_session_label": "开盘",
                            "execution_feedback_action": "TIGHTEN",
                        },
                        "effect_snapshot_week_label": "2026-W13",
                    },
                }
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 100000.0, "broker_cash": 70000.0}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "shadow_review_order_count": 4,
                        "shadow_review_portfolio_count": 1,
                        "shadow_review_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "shadow_review_count": 4,
                                "near_miss_count": 3,
                                "far_below_count": 0,
                                "repeated_symbol_count": 1,
                                "repeated_symbols": "AAPL",
                                "latest_shadow_symbol": "AAPL",
                                "shadow_review_action": "REVIEW_THRESHOLD",
                                "shadow_review_reason": "多数 shadow review 拦单接近阈值，建议复核 burn-in 阈值是否偏严。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["shadow_strategy_overview"][0]["shadow_review_action"], "REVIEW_THRESHOLD")
            self.assertEqual(payload["trade_cards"][0]["weekly_shadow_review"]["repeated_symbols"], "AAPL")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("策略升级建议", html_text)
            self.assertIn("策略升级建议（Shadow Weekly）", html_text)
            self.assertIn("REVIEW_THRESHOLD", html_text)
            self.assertIn("AAPL", html_text)

    def test_dashboard_surfaces_dry_run_weekly_proxy_attribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            threshold_override_path = weekly_dir / "weekly_feedback_threshold_overrides.yaml"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_paper_summary.json").write_text(
                json.dumps(
                    {
                        "equity_after": 120000.0,
                        "cash_after": 30000.0,
                        "target_invested_weight": 0.75,
                        "rebalance_due": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "attribution_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "weekly_return": 0.052,
                                "selection_contribution": 0.028,
                                "sizing_contribution": 0.009,
                                "sector_contribution": 0.006,
                                "execution_contribution": -0.004,
                                "market_contribution": 0.013,
                                "dominant_driver": "SELECTION",
                                "diagnosis": "收益主要由选股质量驱动，优先复盘信号与候选排序。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        force_local_paper_ledger: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["dry_run_attribution_overview"][0]["dominant_driver"], "SELECTION")
            self.assertEqual(payload["dry_run_cards"][0]["weekly_attribution"]["market"], "US")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Dry Run 周度代理归因", html_text)
            self.assertIn('data-simple-section="dry-run-attribution"', html_text)
            self.assertIn("周度代理归因（策略复盘）", html_text)
            self.assertIn("SELECTION", html_text)
            self.assertIn("收益主要由选股质量驱动", html_text)

    def test_dashboard_surfaces_planned_vs_actual_execution_costs(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            threshold_override_path = weekly_dir / "weekly_feedback_threshold_overrides.yaml"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "broker_equity": 100000.0,
                        "broker_cash": 65000.0,
                        "planned_execution_cost_total": 21.4,
                        "execution_style": "VWAP_LITE_MIDDAY",
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_execution_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,status,execution_style,expected_cost_bps,reason",
                        "AAPL,BUY,PLANNED,VWAP_LITE_MIDDAY,18.4,rebalance_up|vwap_lite_midday",
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "attribution_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "weekly_return": 0.031,
                                "selection_contribution": 0.017,
                                "sizing_contribution": 0.005,
                                "sector_contribution": 0.004,
                                "execution_contribution": -0.003,
                                "market_contribution": 0.008,
                                "planned_execution_cost_total": 21.4,
                                "execution_cost_total": 34.9,
                                "execution_cost_gap": 13.5,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 29.7,
                                "execution_style_breakdown": "VWAP_LITE_MIDDAY:2",
                                "dominant_driver": "EXECUTION",
                                "diagnosis": "实际执行成本高于计划，优先复盘拆单和执行时段。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertAlmostEqual(payload["execution_cost_overview"][0]["planned_execution_cost_total"], 21.4, places=6)
            self.assertAlmostEqual(payload["trade_cards"][0]["weekly_attribution"]["execution_cost_gap"], 13.5, places=6)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("计划成本 vs 实际执行成本", html_text)
            self.assertIn("VWAP_LITE_MIDDAY", html_text)
            self.assertIn("13.50", html_text)
            self.assertIn("实际执行成本高于计划", html_text)

    def test_dashboard_surfaces_weekly_execution_feedback(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            db_path = base / "audit.db"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            threshold_override_path = weekly_dir / "weekly_feedback_threshold_overrides.yaml"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            storage = Storage(str(db_path))
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W12",
                    "week_start": "2026-03-16",
                    "window_start": "2026-03-10T00:00:00+00:00",
                    "window_end": "2026-03-17T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "SUGGEST_ONLY",
                    "calibration_apply_mode_label": "建议确认",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.70,
                    "feedback_calibration_score": 0.54,
                    "feedback_confidence": 0.61,
                    "feedback_sample_count": 2,
                    "feedback_calibration_sample_count": 12,
                    "outcome_maturity_ratio": 0.45,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 9,
                    "outcome_ready_estimate_end_ts": "2026-03-19T00:00:00+00:00",
                    "alert_bucket": "SOON",
                    "details": {"automation_reason": "上一周样本仍在成熟。"},
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W13",
                    "week_start": "2026-03-23",
                    "window_start": "2026-03-17T00:00:00+00:00",
                    "window_end": "2026-03-24T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.80,
                    "feedback_calibration_score": 0.60,
                    "feedback_confidence": 0.72,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 18,
                    "outcome_maturity_ratio": 0.72,
                    "outcome_maturity_label": "LATE",
                    "outcome_pending_sample_count": 7,
                    "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                    "alert_bucket": "ACTIVE",
                    "details": {"automation_reason": "执行参数本周已满足 paper 自动应用条件。"},
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W12",
                    "week_start": "2026-03-16",
                    "window_start": "2026-03-10T00:00:00+00:00",
                    "window_end": "2026-03-17T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "risk",
                    "feedback_kind_label": "风险预算",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "HOLD",
                    "calibration_apply_mode_label": "继续观察",
                    "calibration_basis": "BASE_WEEKLY",
                    "calibration_basis_label": "周报信号",
                    "feedback_base_confidence": 0.42,
                    "feedback_calibration_score": 0.28,
                    "feedback_confidence": 0.35,
                    "feedback_sample_count": 2,
                    "feedback_calibration_sample_count": 6,
                    "outcome_maturity_ratio": 0.40,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 11,
                    "outcome_ready_estimate_end_ts": "2026-03-26T00:00:00+00:00",
                    "alert_bucket": "SOON",
                    "details": {"automation_reason": "风险预算样本还偏少，先继续观察。"},
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W13",
                    "week_start": "2026-03-23",
                    "window_start": "2026-03-17T00:00:00+00:00",
                    "window_end": "2026-03-24T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "risk",
                    "feedback_kind_label": "风险预算",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "HOLD",
                    "calibration_apply_mode_label": "继续观察",
                    "calibration_basis": "BASE_WEEKLY",
                    "calibration_basis_label": "周报信号",
                    "feedback_base_confidence": 0.44,
                    "feedback_calibration_score": 0.30,
                    "feedback_confidence": 0.36,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 7,
                    "outcome_maturity_ratio": 0.43,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 10,
                    "outcome_ready_estimate_end_ts": "2026-03-26T00:00:00+00:00",
                    "alert_bucket": "SOON",
                    "details": {"automation_reason": "风险预算 outcome 仍在成熟，暂不自动应用。"},
                }
            )
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_EXEC_DASH"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 100000.0, "broker_cash": 70000.0}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_hotspot_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "symbol": "AAPL",
                                "session_bucket": "OPEN",
                                "session_label": "开盘",
                                "hotspot_action": "INVESTIGATE_EXECUTION",
                                "planned_execution_cost_total": 12.0,
                                "execution_cost_total": 20.5,
                                "execution_cost_gap": 8.5,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 32.1,
                                "pressure_score": 16.72,
                                "execution_style_breakdown": "TWAP_LITE_OPEN:2",
                                "reason": "AAPL 在开盘阶段的实际执行成本高于计划。",
                            }
                        ],
                        "execution_session_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "session_bucket": "OPEN",
                                "session_label": "开盘",
                                "planned_execution_cost_total": 12.0,
                                "execution_cost_total": 20.5,
                                "execution_cost_gap": 8.5,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 32.1,
                                "execution_style_breakdown": "TWAP_LITE_OPEN:2",
                            },
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "session_bucket": "MIDDAY",
                                "session_label": "午盘",
                                "planned_execution_cost_total": 9.4,
                                "execution_cost_total": 8.8,
                                "execution_cost_gap": -0.6,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 15.2,
                                "execution_style_breakdown": "VWAP_LITE_MIDDAY:1",
                            },
                        ],
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_base_confidence": 0.80,
                                "feedback_base_confidence_label": "HIGH",
                                "feedback_calibration_score": 0.60,
                                "feedback_calibration_label": "MEDIUM",
                                "feedback_calibration_sample_count": 18,
                                "feedback_calibration_horizon_days": "20",
                                "feedback_calibration_scope": "final 可执行候选",
                                "feedback_calibration_reason": "近期 candidate outcome 仍显示 alpha 存在，执行调参有继续自动应用的依据。",
                                "feedback_confidence": 0.72,
                                "feedback_confidence_label": "MEDIUM",
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                                "execution_max_slices_per_symbol_delta": 1,
                                "execution_open_session_participation_scale_delta": -0.05,
                                "execution_midday_session_participation_scale_delta": -0.03,
                                "execution_close_session_participation_scale_delta": -0.04,
                                "planned_execution_cost_total": 21.4,
                                "execution_cost_total": 34.9,
                                "execution_cost_gap": 13.5,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 29.7,
                                "execution_style_breakdown": "VWAP_LITE_MIDDAY:2",
                                "dominant_execution_session_bucket": "OPEN",
                                "dominant_execution_session_label": "开盘",
                                "execution_session_feedback_json": json.dumps(
                                    [
                                        {
                                            "session_bucket": "OPEN",
                                            "session_label": "开盘",
                                            "session_action": "TIGHTEN",
                                            "planned_execution_cost_total": 12.0,
                                            "execution_cost_total": 20.5,
                                            "execution_cost_gap": 8.5,
                                            "avg_expected_cost_bps": 18.4,
                                            "avg_actual_slippage_bps": 32.1,
                                            "scale_delta": -0.07,
                                            "execution_style_breakdown": "TWAP_LITE_OPEN:2",
                                            "reason": "开盘的实际执行成本高于计划，下一轮应降低该时段参与率。",
                                        },
                                        {
                                            "session_bucket": "MIDDAY",
                                            "session_label": "午盘",
                                            "session_action": "HOLD",
                                            "planned_execution_cost_total": 9.4,
                                            "execution_cost_total": 8.8,
                                            "execution_cost_gap": -0.6,
                                            "avg_expected_cost_bps": 18.4,
                                            "avg_actual_slippage_bps": 15.2,
                                            "scale_delta": 0.0,
                                            "execution_style_breakdown": "VWAP_LITE_MIDDAY:1",
                                            "reason": "午盘成本与滑点大致稳定，暂不单独调整该时段参与率。",
                                        },
                                    ],
                                    ensure_ascii=False,
                                ),
                                "execution_penalty_symbols": "AAPL",
                                "execution_penalties_json": json.dumps(
                                    [
                                        {
                                            "symbol": "AAPL",
                                            "session_labels": "开盘",
                                            "session_count": 1,
                                            "expected_cost_bps_add": 8.0,
                                            "slippage_proxy_bps_add": 6.0,
                                            "execution_penalty": 0.03,
                                            "reason": "repeat_execution_hotspot",
                                        }
                                    ],
                                    ensure_ascii=False,
                                ),
                                "feedback_reason": "实际执行成本高于计划，下一轮收紧参与率并增加拆单。",
                            }
                        ],
                        "feedback_calibration_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "selection_scope_label": "final 可执行候选",
                                "selected_horizon_days": "20",
                                "outcome_sample_count": 18,
                                "outcome_positive_rate": 0.61,
                                "outcome_broken_rate": 0.11,
                                "avg_future_return": 0.072,
                                "avg_max_drawdown": -0.048,
                                "score_alignment_score": 0.68,
                                "signal_quality_score": 0.66,
                                "shadow_threshold_relax_support": 0.64,
                                "risk_tighten_support": 0.29,
                                "execution_support": 0.60,
                                "calibration_confidence": 0.63,
                                "calibration_confidence_label": "MEDIUM",
                                "calibration_reason": "近期 candidate outcome 整体偏强，说明 alpha 仍有一定稳定性。",
                            }
                        ],
                        "feedback_automation_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "feedback_action": "TIGHTEN",
                                "calibration_apply_mode": "AUTO_APPLY",
                                "calibration_apply_mode_label": "自动应用",
                                "calibration_basis_label": "已有 outcome 校准",
                                "feedback_base_confidence": 0.80,
                                "feedback_base_confidence_label": "HIGH",
                                "feedback_calibration_score": 0.60,
                                "feedback_calibration_label": "MEDIUM",
                                "feedback_confidence": 0.72,
                                "feedback_confidence_label": "MEDIUM",
                                "feedback_sample_count": 3,
                                "feedback_calibration_sample_count": 18,
                                "outcome_maturity_ratio": 0.72,
                                "outcome_maturity_label": "LATE",
                                "outcome_pending_sample_count": 7,
                                "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                                "automation_reason": "执行参数本周已满足 paper 自动应用条件。",
                            }
                        ],
                        "labeling_summary": {
                            "labeled_rows": 18,
                            "skipped_rows": 7,
                            "skip_reason_counts": {"INSUFFICIENT_FORWARD_BARS": 7},
                        },
                        "labeling_skip_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "horizon_days": 5,
                                "skip_reason": "INSUFFICIENT_FORWARD_BARS",
                                "skip_reason_label": "前向样本不足",
                                "skip_count": 7,
                                "symbol_count": 4,
                                "sample_symbols": "AAPL,MSFT,NVDA,AMD",
                                "oldest_snapshot_ts": "2026-03-10T00:00:00+00:00",
                                "latest_snapshot_ts": "2026-03-14T00:00:00+00:00",
                                "min_remaining_forward_bars": 2,
                                "max_remaining_forward_bars": 4,
                                "estimated_ready_start_ts": "2026-03-18T00:00:00+00:00",
                                "estimated_ready_end_ts": "2026-03-20T00:00:00+00:00",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["execution_feedback_overview"][0]["execution_feedback_action"], "TIGHTEN")
            self.assertEqual(int(payload["execution_feedback_summary"]["total_count"]), 1)
            self.assertEqual(int(payload["execution_feedback_summary"]["auto_apply_count"]), 1)
            self.assertEqual(int(payload["execution_feedback_summary"]["tighten_count"]), 1)
            self.assertAlmostEqual(float(payload["execution_feedback_summary"]["avg_base_confidence"]), 0.80, places=6)
            self.assertAlmostEqual(float(payload["execution_feedback_summary"]["avg_calibration_score"]), 0.60, places=6)
            self.assertAlmostEqual(float(payload["execution_feedback_summary"]["avg_confidence"]), 0.72, places=6)
            self.assertEqual(int(payload["labeling_summary"]["skipped_rows"]), 7)
            self.assertEqual(payload["labeling_skip_overview"][0]["skip_reason_label"], "前向样本不足")
            self.assertEqual(int(payload["labeling_skip_overview"][0]["min_remaining_forward_bars"]), 2)
            self.assertEqual(str(payload["labeling_skip_overview"][0]["estimated_ready_end_ts"]), "2026-03-20T00:00:00+00:00")
            self.assertEqual(payload["labeling_ready_overview"][0]["portfolio_id"], "US:watchlist")
            self.assertEqual(int(payload["labeling_ready_overview"][0]["days_until_ready"]), 0)
            self.assertEqual(payload["trade_cards"][0]["execution_feedback"]["effective_source_label"], "dashboard 预估")
            self.assertEqual(payload["trade_cards"][0]["execution_feedback"]["dominant_execution_session_label"], "开盘")
            self.assertEqual(payload["trade_cards"][0]["execution_feedback"]["execution_penalty_symbols"], "AAPL")
            self.assertEqual(payload["trade_cards"][0]["weekly_labeling_skips"][0]["skip_reason_label"], "前向样本不足")
            self.assertEqual(int(payload["trade_cards"][0]["weekly_labeling_skips"][0]["max_remaining_forward_bars"]), 4)
            self.assertAlmostEqual(float(payload["trade_cards"][0]["execution_feedback"]["feedback_confidence"]), 0.72, places=6)
            self.assertAlmostEqual(float(payload["trade_cards"][0]["execution_feedback"]["feedback_base_confidence"]), 0.80, places=6)
            self.assertAlmostEqual(float(payload["trade_cards"][0]["execution_feedback"]["feedback_calibration_score"]), 0.60, places=6)
            self.assertEqual(int(payload["feedback_calibration_overview"][0]["outcome_sample_count"]), 18)
            self.assertEqual(payload["feedback_automation_overview"][0]["feedback_kind"], "execution")
            self.assertEqual(payload["feedback_automation_overview"][0]["calibration_apply_mode"], "AUTO_APPLY")
            self.assertAlmostEqual(float(payload["feedback_automation_overview"][0]["outcome_maturity_ratio"]), 0.72, places=6)
            self.assertEqual(payload["feedback_automation_overview"][0]["outcome_maturity_label"], "LATE")
            self.assertEqual(payload["feedback_automation_history_overview"][0]["current_state"], "ACTIVE")
            self.assertIn(payload["feedback_automation_history_overview"][0]["transition"], {"状态变化", "持续观察"})
            self.assertEqual(payload["feedback_automation_stuck_overview"][0]["feedback_kind_label"], "风险预算")
            self.assertEqual(payload["feedback_automation_stuck_overview"][0]["stuck_bucket"], "长期等待成熟")
            self.assertEqual(int(payload["feedback_automation_stuck_overview"][0]["same_state_weeks"]), 2)
            self.assertEqual(payload["feedback_automation_effect_overview"][0]["feedback_kind_label"], "执行参数")
            self.assertEqual(payload["feedback_maturity_alert_overview"][0]["alert_bucket"], "ACTIVE")
            self.assertEqual(payload["feedback_maturity_alert_overview"][0]["portfolio_id"], "US:watchlist")
            self.assertEqual(payload["execution_hotspot_overview"][0]["symbol"], "AAPL")
            self.assertEqual(len(payload["trade_cards"][0]["execution_feedback"]["session_feedback_rows"]), 2)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("第三阶段：自动执行校准进度", html_text)
            self.assertIn("本周自动执行反馈", html_text)
            self.assertIn("即将成熟的 Outcome 样本", html_text)
            self.assertIn("ready_estimate", html_text)
            self.assertIn("结果校准", html_text)
            self.assertIn("校准自动化", html_text)
            self.assertIn("校准自动化历史", html_text)
            self.assertIn("校准自动化历史趋势", html_text)
            self.assertIn("长期卡住的校准", html_text)
            self.assertIn("自动应用后效果", html_text)
            self.assertIn("W+1", html_text)
            self.assertIn("apply_week", html_text)
            self.assertIn("接近自动应用的校准", html_text)
            self.assertIn("pending|ready", html_text)
            self.assertIn("LATE", html_text)
            self.assertIn("结果校准输入缺口", html_text)
            self.assertIn("历史数据为空", html_text)
            self.assertIn("执行时段复盘", html_text)
            self.assertIn("执行热点（symbol + session）", html_text)
            self.assertIn("执行热点惩罚（下轮候选）", html_text)
            self.assertIn("Avg Base", html_text)
            self.assertIn("Avg Calib", html_text)
            self.assertIn("Avg Final", html_text)
            self.assertIn("AAPL", html_text)
            self.assertIn("开盘", html_text)
            self.assertIn("5.0% -&gt; 4.3%", html_text)
            self.assertIn("4-&gt;5", html_text)
            self.assertIn("实际执行成本高于计划", html_text)

    def test_dashboard_feedback_automation_effect_overview_tracks_w1_milestone(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            db_path = base / "audit.db"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            threshold_override_path = weekly_dir / "weekly_feedback_threshold_overrides.yaml"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            storage = Storage(str(db_path))
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W11",
                    "week_start": "2026-03-09",
                    "window_start": "2026-03-03T00:00:00+00:00",
                    "window_end": "2026-03-10T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "SUGGEST_ONLY",
                    "calibration_apply_mode_label": "建议确认",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.70,
                    "feedback_calibration_score": 0.54,
                    "feedback_confidence": 0.61,
                    "feedback_sample_count": 2,
                    "feedback_calibration_sample_count": 12,
                    "outcome_maturity_ratio": 0.45,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 9,
                    "outcome_ready_estimate_end_ts": "2026-03-19T00:00:00+00:00",
                    "alert_bucket": "SOON",
                    "details": {"automation_reason": "上一周样本仍在成熟。"},
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W12",
                    "week_start": "2026-03-16",
                    "window_start": "2026-03-10T00:00:00+00:00",
                    "window_end": "2026-03-17T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.78,
                    "feedback_calibration_score": 0.58,
                    "feedback_confidence": 0.70,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 16,
                    "outcome_maturity_ratio": 0.66,
                    "outcome_maturity_label": "LATE",
                    "outcome_pending_sample_count": 8,
                    "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数上周已开始自动应用。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "planned_execution_cost_total": 21.4,
                            "execution_cost_total": 41.4,
                            "execution_cost_gap": 20.0,
                            "avg_expected_cost_bps": 18.4,
                            "avg_actual_slippage_bps": 35.2,
                            "dominant_execution_session_label": "开盘",
                            "execution_feedback_action": "TIGHTEN",
                        },
                        "effect_snapshot_week_label": "2026-W12",
                    },
                }
            )
            storage.upsert_investment_feedback_automation_history(
                {
                    "week_label": "2026-W13",
                    "week_start": "2026-03-23",
                    "window_start": "2026-03-17T00:00:00+00:00",
                    "window_end": "2026-03-24T00:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "feedback_kind_label": "执行参数",
                    "feedback_action": "TIGHTEN",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "calibration_basis": "OUTCOME_CALIBRATED",
                    "calibration_basis_label": "已有 outcome 校准",
                    "feedback_base_confidence": 0.80,
                    "feedback_calibration_score": 0.60,
                    "feedback_confidence": 0.72,
                    "feedback_sample_count": 3,
                    "feedback_calibration_sample_count": 18,
                    "outcome_maturity_ratio": 0.72,
                    "outcome_maturity_label": "LATE",
                    "outcome_pending_sample_count": 7,
                    "outcome_ready_estimate_end_ts": "2026-03-20T00:00:00+00:00",
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数本周已满足 paper 自动应用条件。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "planned_execution_cost_total": 21.4,
                            "execution_cost_total": 34.9,
                            "execution_cost_gap": 13.5,
                            "avg_expected_cost_bps": 18.4,
                            "avg_actual_slippage_bps": 29.7,
                            "dominant_execution_session_label": "开盘",
                            "execution_feedback_action": "TIGHTEN",
                        },
                        "effect_snapshot_week_label": "2026-W13",
                    },
                }
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 100000.0, "broker_cash": 70000.0}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_base_confidence": 0.80,
                                "feedback_base_confidence_label": "HIGH",
                                "feedback_calibration_score": 0.60,
                                "feedback_calibration_label": "MEDIUM",
                                "feedback_calibration_sample_count": 18,
                                "feedback_calibration_horizon_days": "20",
                                "feedback_calibration_scope": "final 可执行候选",
                                "feedback_confidence": 0.72,
                                "feedback_confidence_label": "MEDIUM",
                                "planned_execution_cost_total": 21.4,
                                "execution_cost_total": 34.9,
                                "execution_cost_gap": 13.5,
                                "avg_expected_cost_bps": 18.4,
                                "avg_actual_slippage_bps": 29.7,
                                "dominant_execution_session_label": "开盘",
                                "feedback_reason": "实际执行成本高于计划，下一轮收紧参与率并增加拆单。",
                            }
                        ],
                        "feedback_calibration_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "selection_scope_label": "final 可执行候选",
                                "selected_horizon_days": "20",
                                "outcome_sample_count": 18,
                                "outcome_positive_rate": 0.61,
                                "outcome_broken_rate": 0.11,
                                "avg_future_return": 0.072,
                                "avg_max_drawdown": -0.048,
                                "score_alignment_score": 0.68,
                                "signal_quality_score": 0.66,
                                "execution_support": 0.60,
                                "calibration_confidence": 0.63,
                                "calibration_confidence_label": "MEDIUM",
                            }
                        ],
                        "feedback_threshold_suggestion_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "summary_signal": "持续改善",
                                "suggestion_action": "RELAX_AUTO_APPLY",
                                "suggestion_label": "可适度放宽",
                                "tracked_count": 1,
                                "avg_active_weeks": 2.0,
                                "base_auto_confidence": 0.60,
                                "suggested_auto_confidence": 0.57,
                                "base_auto_base_confidence": 0.76,
                                "suggested_auto_base_confidence": 0.73,
                                "base_auto_calibration_score": 0.56,
                                "suggested_auto_calibration_score": 0.54,
                                "base_auto_maturity_ratio": 0.60,
                                "suggested_auto_maturity_ratio": 0.55,
                                "examples": "US:watchlist: 改善",
                                "reason": "自动应用后已出现连续改善，可适度放宽 AUTO_APPLY 门槛。",
                            }
                        ],
                        "feedback_threshold_history_overview": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "current_action": "RELAX_AUTO_APPLY",
                                "current_label": "可适度放宽",
                                "summary_signal": "持续改善",
                                "transition": "持续试运行",
                                "same_action_weeks": 2,
                                "weeks_tracked": 3,
                                "trend_bucket": "连续放宽",
                                "threshold_snapshot": "conf 0.60->0.57 | base 0.76->0.73",
                                "action_chain": "2026-W12:KEEP_BASE -> 2026-W13:RELAX_AUTO_APPLY -> 2026-W14:RELAX_AUTO_APPLY",
                                "reason": "自动应用后连续两周改善。",
                            }
                        ],
                        "feedback_threshold_effect_overview": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "current_action": "RELAX_AUTO_APPLY",
                                "current_label": "可适度放宽",
                                "summary_signal": "持续改善",
                                "effect_label": "放宽后改善",
                                "effect_reason": "这条市场阈值放宽后，自动应用效果仍在继续改善。",
                                "same_action_weeks": 2,
                                "weeks_tracked": 3,
                                "tracked_count": 2,
                                "avg_active_weeks": 3.0,
                                "threshold_snapshot": "conf 0.60->0.57 | calib 0.56->0.54",
                                "action_chain": "2026-W12:KEEP_BASE -> 2026-W13:RELAX_AUTO_APPLY -> 2026-W14:RELAX_AUTO_APPLY",
                                "reason": "自动应用后连续两周改善。",
                            }
                        ],
                        "feedback_threshold_cohort_overview": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "cohort_action": "RELAX_AUTO_APPLY",
                                "cohort_label": "可适度放宽",
                                "baseline_week": "2026-W13",
                                "cohort_weeks": 2,
                                "tracked_count": 2,
                                "avg_active_weeks": 3.0,
                                "latest_effect": "放宽后改善",
                                "effect_w1": "放宽后改善",
                                "effect_w2": "-",
                                "effect_w4": "-",
                                "diagnosis": "放宽后已看到改善，可继续试运行并跟踪。",
                                "action_chain": "2026-W13:RELAX_AUTO_APPLY -> 2026-W14:RELAX_AUTO_APPLY",
                            }
                        ],
                        "feedback_threshold_trial_alerts": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "cohort_label": "可适度放宽",
                                "baseline_week": "2026-W13",
                                "cohort_weeks": 2,
                                "stage_label": "持续观察期",
                                "action_label": "放宽试运行",
                                "latest_effect": "放宽后改善",
                                "effect_w1": "放宽后改善",
                                "effect_w2": "-",
                                "diagnosis": "放宽后已看到改善，可继续试运行并跟踪。",
                                "next_check": "优先确认是否恶化",
                            }
                        ],
                        "feedback_threshold_tuning_summary": [
                            {
                                "market": "US",
                                "feedback_kind": "execution",
                                "feedback_kind_label": "执行参数",
                                "cohort_label": "可适度放宽",
                                "baseline_week": "2026-W13",
                                "cohort_weeks": 2,
                                "latest_effect": "放宽后改善",
                                "effect_w1": "放宽后改善",
                                "effect_w2": "-",
                                "effect_w4": "-",
                                "suggestion_action": "KEEP_RELAX",
                                "suggestion_label": "继续放宽试运行",
                                "diagnosis": "放宽后已看到改善，可继续试运行并跟踪。",
                                "reason": "放宽后的 cohort 已连续出现改善，可继续保留当前放宽。",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            threshold_override_path.write_text(
                yaml.safe_dump(
                    {
                        "metadata": {"source": "weekly_review_threshold_suggestions"},
                        "markets": {
                            "US": {
                                "execution": {
                                    "auto_confidence": 0.57,
                                    "auto_base_confidence": 0.73,
                                    "auto_calibration_score": 0.54,
                                    "auto_maturity_ratio": 0.55,
                                }
                            }
                        },
                    },
                    sort_keys=False,
                    allow_unicode=True,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        f'weekly_feedback_thresholds_path: "{threshold_override_path}"',
                        "weekly_review_auto_apply_paper: true",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            row = payload["feedback_automation_effect_overview"][0]
            self.assertEqual(row["feedback_kind_label"], "执行参数")
            self.assertEqual(row["baseline_week"], "2026-W12")
            self.assertEqual(int(row["active_weeks"]), 2)
            self.assertIn("改善", str(row["effect_w1"]))
            summary_row = payload["feedback_automation_effect_summary"][0]
            self.assertEqual(summary_row["market"], "US")
            self.assertEqual(summary_row["feedback_kind_label"], "执行参数")
            self.assertEqual(summary_row["summary_signal"], "持续改善")
            self.assertEqual(int(summary_row["tracked_count"]), 1)
            self.assertEqual(int(summary_row["w1_improved_count"]), 1)
            self.assertEqual(payload["feedback_threshold_suggestion_summary"][0]["suggestion_action"], "RELAX_AUTO_APPLY")
            self.assertEqual(payload["feedback_threshold_history_overview"][0]["trend_bucket"], "连续放宽")
            self.assertEqual(payload["feedback_threshold_effect_overview"][0]["effect_label"], "放宽后改善")
            self.assertEqual(payload["feedback_threshold_cohort_overview"][0]["effect_w1"], "放宽后改善")
            self.assertEqual(payload["feedback_threshold_trial_alerts"][0]["stage_label"], "持续观察期")
            self.assertEqual(payload["feedback_threshold_tuning_summary"][0]["suggestion_action"], "KEEP_RELAX")
            self.assertEqual(payload["feedback_threshold_override_overview"][0]["effective_state_label"], "继续放宽中")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("自动应用效果汇总", html_text)
            self.assertIn("分市场 AUTO_APPLY 阈值建议", html_text)
            self.assertIn("当前生效中的分市场阈值 Override", html_text)
            self.assertIn("阈值建议历史趋势", html_text)
            self.assertIn("阈值试运行效果", html_text)
            self.assertIn("阈值试运行 Cohort", html_text)
            self.assertIn("分市场阈值试运行观察期", html_text)
            self.assertIn("分市场阈值调参建议", html_text)
            self.assertIn("继续放宽试运行", html_text)
            self.assertIn("继续放宽中", html_text)
            self.assertIn("可适度放宽", html_text)
            self.assertIn("持续改善", html_text)

    def test_dashboard_execution_feedback_explains_why_not_auto_applied(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_EXEC_DASH"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 100000.0, "broker_cash": 70000.0}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "execution_feedback_action": "TIGHTEN",
                                "execution_adv_max_participation_pct_delta": -0.01,
                                "execution_adv_split_trigger_pct_delta": -0.003,
                                "execution_max_slices_per_symbol_delta": 1,
                                "execution_open_session_participation_scale_delta": -0.05,
                                "feedback_reason": "本周给出执行收紧建议，但当前不自动应用。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: false",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            feedback = payload["trade_cards"][0]["execution_feedback"]
            self.assertEqual(feedback["apply_mode_label"], "仅建议未自动生效")
            self.assertEqual(feedback["apply_status_code"], "PAPER_AUTO_APPLY_DISABLED")
            self.assertIn("paper 自动应用已关闭", feedback["apply_status_reason"])
            self.assertEqual(int(payload["execution_feedback_summary"]["suggest_only_count"]), 1)
            self.assertEqual(int(payload["execution_feedback_summary"]["policy_block_count"]), 1)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("仅建议未自动生效", html_text)
            self.assertIn("paper 自动应用已关闭", html_text)

    def test_dashboard_execution_feedback_explains_no_feedback_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_EXEC_DASH"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 100000.0, "broker_cash": 70000.0}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_execution_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W13,2026-03-23,US,US:watchlist,12,2,8,0,0,0,5,0,0,0,0,0,0",
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps({"execution_feedback_summary": []}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            feedback = payload["trade_cards"][0]["execution_feedback"]
            self.assertEqual(feedback["apply_mode_label"], "沿用基础配置")
            self.assertEqual(feedback["apply_status_code"], "NO_OPPORTUNITY_PASS")
            self.assertIn("opportunity=5", feedback["apply_status_reason"])
            self.assertEqual(int(payload["execution_feedback_summary"]["no_feedback_count"]), 1)
            self.assertEqual(int(payload["execution_feedback_summary"]["no_order_count"]), 1)
            self.assertEqual(int(payload["execution_feedback_summary"]["no_opportunity_count"]), 1)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Opp Gate", html_text)
            self.assertIn("No Orders", html_text)
            self.assertIn("opportunity=5", html_text)

    def test_dashboard_execution_feedback_explains_quality_gate_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            execution_cfg_path = base / "investment_execution_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            execution_cfg_path.write_text(
                "\n".join(
                    [
                        "execution:",
                        "  adv_max_participation_pct: 0.05",
                        "  adv_split_trigger_pct: 0.02",
                        "  max_slices_per_symbol: 4",
                        "  open_session_participation_scale: 0.70",
                        "  midday_session_participation_scale: 1.00",
                        "  close_session_participation_scale: 0.85",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_EXEC_DASH"',
                        f'investment_execution_config: "{execution_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "broker_equity": 100000.0,
                        "broker_cash": 70000.0,
                        "blocked_order_count": 3,
                        "blocked_quality_order_count": 3,
                        "blocked_opportunity_order_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_execution_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W13,2026-03-23,US,US:watchlist,12,2,8,0,0,0,1,0,0,0,0,0,0",
                    ]
                ),
                encoding="utf-8",
            )
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps({"execution_feedback_summary": []}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            feedback = payload["trade_cards"][0]["execution_feedback"]
            self.assertEqual(feedback["apply_status_code"], "NO_QUALITY_PASS")
            self.assertIn("quality=3", feedback["apply_status_reason"])
            self.assertEqual(int(payload["execution_feedback_summary"]["no_quality_count"]), 1)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Quality Gate", html_text)
            self.assertIn("quality=3", html_text)

    def test_dashboard_surfaces_weekly_risk_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "risk_review_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "dominant_risk_driver": "CORRELATION",
                                "latest_dynamic_net_exposure": 0.69,
                                "latest_dynamic_gross_exposure": 0.77,
                                "latest_avg_pair_correlation": 0.64,
                                "latest_stress_worst_scenario_label": "流动性恶化",
                                "latest_stress_worst_loss": 0.089,
                                "risk_diagnosis": "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["risk_review_overview"][0]["dominant_risk_driver"], "CORRELATION")
            self.assertEqual(payload["cards"][0]["weekly_risk_review"]["latest_stress_worst_scenario_label"], "流动性恶化")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("周度风险复盘", html_text)
            self.assertIn('data-simple-section="risk-review-overview"', html_text)
            self.assertIn("CORRELATION", html_text)
            self.assertIn("流动性恶化", html_text)
            self.assertIn("组合拥挤度偏高", html_text)

    def test_dashboard_surfaces_recent_risk_history_for_trade_and_dry_run_views(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            storage.insert_investment_run(
                {
                    "run_id": "paper-risk-1",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "report_dir": str(report_dir),
                    "rebalance_due": 1,
                    "executed": 1,
                    "cash_before": 100000.0,
                    "cash_after": 82000.0,
                    "equity_before": 100000.0,
                    "equity_after": 100500.0,
                    "details": json.dumps(
                        {
                            "risk_overlay": {
                                "dynamic_scale": 0.78,
                                "dynamic_net_exposure": 0.66,
                                "dynamic_gross_exposure": 0.74,
                                "avg_pair_correlation": 0.64,
                                "stress_worst_loss": 0.091,
                                "stress_worst_scenario_label": "波动抬升",
                                "notes": ["相关性偏高，降低组合总敞口。"],
                                "correlation_reduced_symbols": ["AAPL", "MSFT"],
                            }
                        },
                        ensure_ascii=False,
                    ),
                }
            )
            storage.insert_investment_execution_run(
                {
                    "run_id": "exec-risk-1",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "account_id": "DU1234567",
                    "report_dir": str(report_dir),
                    "submitted": 1,
                    "order_count": 2,
                    "order_value": 15000.0,
                    "broker_equity": 120000.0,
                    "broker_cash": 60000.0,
                    "target_equity": 30000.0,
                    "details": json.dumps(
                        {
                            "risk_overlay": {
                                "dynamic_scale": 0.84,
                                "dynamic_net_exposure": 0.74,
                                "dynamic_gross_exposure": 0.82,
                                "avg_pair_correlation": 0.58,
                                "stress_worst_loss": 0.073,
                                "stress_worst_scenario_label": "流动性恶化",
                                "notes": ["执行链路当前仍保持温和收敛。"],
                            }
                        },
                        ensure_ascii=False,
                    ),
                }
            )
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="paper-risk-1",
                    ts="2026-03-19T01:00:00+00:00",
                    market="US",
                    portfolio_id="US:watchlist",
                    source_kind="paper",
                    source_label="Dry Run",
                    report_dir=str(report_dir),
                    risk_overlay={
                        "dynamic_scale": 0.77,
                        "dynamic_net_exposure": 0.63,
                        "dynamic_gross_exposure": 0.72,
                        "avg_pair_correlation": 0.67,
                        "stress_worst_loss": 0.093,
                        "stress_worst_scenario_label": "波动抬升",
                        "notes": ["规范化风险历史优先覆盖 dry run 视图。"],
                        "correlation_reduced_symbols": ["AAPL", "MSFT"],
                    },
                )
            )
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-risk-1",
                    ts="2026-03-19T02:00:00+00:00",
                    market="US",
                    portfolio_id="US:watchlist",
                    source_kind="execution",
                    source_label="执行",
                    report_dir=str(report_dir),
                    account_id="DU1234567",
                    risk_overlay={
                        "dynamic_scale": 0.82,
                        "dynamic_net_exposure": 0.71,
                        "dynamic_gross_exposure": 0.80,
                        "avg_pair_correlation": 0.59,
                        "stress_worst_loss": 0.071,
                        "stress_worst_scenario_label": "流动性恶化",
                        "notes": ["规范化风险历史优先覆盖 trade 视图。"],
                    },
                )
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        force_local_paper_ledger: true",
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["trade_risk_history_overview"][0]["source_label"], "执行")
            self.assertEqual(payload["dry_run_risk_history_overview"][0]["source_label"], "Dry Run")
            self.assertAlmostEqual(float(payload["trade_cards"][0]["risk_history_rows"][0]["dynamic_net_exposure"]), 0.71, places=6)
            self.assertAlmostEqual(float(payload["dry_run_cards"][0]["risk_history_rows"][0]["dynamic_net_exposure"]), 0.63, places=6)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("近期风险轨迹", html_text)
            self.assertIn('data-simple-section="trade-risk-history"', html_text)
            self.assertIn('data-simple-section="dry-run-risk-history"', html_text)
            self.assertIn("执行风险轨迹", html_text)
            self.assertIn("Dry Run 风险轨迹", html_text)
            self.assertIn("波动抬升", html_text)
            self.assertIn("流动性恶化", html_text)

    def test_dashboard_surfaces_risk_history_alerts_and_trends(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-risk-older",
                    ts="2026-03-18T01:00:00+00:00",
                    market="US",
                    portfolio_id="US:watchlist",
                    source_kind="execution",
                    source_label="执行",
                    report_dir=str(report_dir),
                    risk_overlay={
                        "dynamic_scale": 0.90,
                        "dynamic_net_exposure": 0.84,
                        "dynamic_gross_exposure": 0.92,
                        "avg_pair_correlation": 0.51,
                        "stress_worst_loss": 0.061,
                        "stress_worst_scenario_label": "指数下跌",
                    },
                )
            )
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-risk-latest",
                    ts="2026-03-19T01:00:00+00:00",
                    market="US",
                    portfolio_id="US:watchlist",
                    source_kind="execution",
                    source_label="执行",
                    report_dir=str(report_dir),
                    risk_overlay={
                        "dynamic_scale": 0.74,
                        "dynamic_net_exposure": 0.68,
                        "dynamic_gross_exposure": 0.76,
                        "avg_pair_correlation": 0.66,
                        "stress_worst_loss": 0.089,
                        "stress_worst_scenario_label": "流动性恶化",
                        "notes": ["相关性和 stress 同时抬升，组合继续缩仓。"],
                    },
                )
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["trade_risk_alert_overview"][0]["alert_level"], "ALERT")
            self.assertEqual(payload["trade_cards"][0]["risk_trend_summary"]["trend_label"], "收紧")
            self.assertIn("平均相关性偏高", payload["trade_cards"][0]["risk_trend_summary"]["diagnosis"])
            self.assertEqual(payload["trade_cards"][0]["execution_mode_recommendation"]["recommended_mode"], "REVIEW_ONLY")
            self.assertEqual(payload["trade_execution_mode_recommendation_overview"][0]["recommended_mode"], "只保留人工审核")
            self.assertEqual(payload["trade_execution_mode_recommendation_summary"]["mismatch_count"], 1)
            self.assertEqual(payload["trade_execution_mode_recommendation_summary"]["review_only_count"], 1)
            self.assertEqual(payload["trade_execution_mode_recommendation_summary"]["paused_count"], 0)
            self.assertEqual(payload["trade_execution_mode_recommendation_summary"]["market_rows"][0]["market"], "US")
            self.assertEqual(payload["trade_execution_mode_recommendation_summary"]["market_rows"][0]["review_only_count"], 1)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("风险轨迹告警", html_text)
            self.assertIn('data-simple-section="trade-risk-alert"', html_text)
            self.assertIn("风险趋势与告警", html_text)
            self.assertIn("执行模式建议", html_text)
            self.assertIn("执行模式告警计数", html_text)
            self.assertIn("建议切换", html_text)
            self.assertIn("建议人工审核", html_text)
            self.assertIn("建议暂停", html_text)
            self.assertIn("US", html_text)
            self.assertIn('class="execution-mode-market-filter active"', html_text)
            self.assertIn('data-market-filter="US"', html_text)
            self.assertIn("dashboard.executionModeMarketFilter", html_text)
            self.assertIn("alert_market", html_text)
            self.assertIn("window.location.hash", html_text)
            self.assertIn("window.addEventListener('hashchange'", html_text)
            self.assertIn('id="execution-mode-market-filter-label"', html_text)
            self.assertIn('id="execution-mode-market-filter-clear"', html_text)
            self.assertIn("当前告警市场筛选：全部", html_text)
            self.assertIn("当前有 1 个组合建议切换：1 个建议人工审核，0 个建议暂停自动执行", html_text)
            self.assertIn("建议切换执行模式", html_text)
            self.assertIn('id="execution-mode-banner"', html_text)
            self.assertIn('id="execution-mode-summary"', html_text)
            self.assertIn('class="execution-mode-banner-row"', html_text)
            self.assertIn('data-market="US"', html_text)
            self.assertIn("只保留人工审核", html_text)
            self.assertIn("组合仍在继续收紧", html_text)
            self.assertIn("ALERT", html_text)

    def test_dashboard_surfaces_weekly_risk_feedback_and_effective_paper_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            paper_cfg_path = base / "investment_paper_us.yaml"
            ibkr_cfg_path = base / "ibkr_us.yaml"
            paper_cfg_path.write_text(
                "\n".join(
                    [
                        "paper:",
                        "  max_single_weight: 0.22",
                        "  max_sector_weight: 0.40",
                        "  max_net_exposure: 0.88",
                        "  max_gross_exposure: 0.95",
                        "  max_short_exposure: 0.35",
                        "  correlation_soft_limit: 0.62",
                    ]
                ),
                encoding="utf-8",
            )
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_DASHBOARD_RISK"',
                        f'investment_paper_config: "{paper_cfg_path}"',
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "risk_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "feedback_scope": "paper_only",
                                "risk_feedback_action": "TIGHTEN",
                                "paper_max_single_weight_delta": -0.02,
                                "paper_max_sector_weight_delta": -0.04,
                                "paper_max_net_exposure_delta": -0.05,
                                "paper_max_gross_exposure_delta": -0.06,
                                "paper_max_short_exposure_delta": -0.02,
                                "paper_correlation_soft_limit_delta": -0.03,
                                "feedback_reason": "组合拥挤度偏高，下一轮自动收紧风险预算。",
                            }
                        ]
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "weekly_review_auto_apply_paper: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["risk_feedback_overview"][0]["risk_feedback_action"], "TIGHTEN")
            card_feedback = payload["cards"][0]["paper_risk_feedback"]
            self.assertTrue(card_feedback["feedback_present"])
            self.assertTrue(card_feedback["auto_apply_enabled"])
            self.assertEqual(card_feedback["apply_mode"], "AUTO_APPLY")
            self.assertEqual(card_feedback["effective_source"], "predicted")
            self.assertAlmostEqual(float(card_feedback["effective_max_single_weight"]), 0.20, places=6)
            self.assertAlmostEqual(float(card_feedback["effective_max_net_exposure"]), 0.83, places=6)
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("本周自动风险反馈", html_text)
            self.assertIn("自动生效", html_text)
            self.assertIn("22.0% -&gt; 20.0%", html_text)
            self.assertIn("组合拥挤度偏高", html_text)

    def test_dashboard_runtime_status_uses_account_scope_and_market_mode_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            runtime_status = payload["runtime_status"]
            self.assertIn("Current account:", runtime_status["summary_text"])
            self.assertIn("account_mode:", runtime_status["summary_text"])
            self.assertIn("runtime_scope:", runtime_status["summary_text"])
            self.assertNotIn("runtime:", runtime_status["summary_text"])
            self.assertIn("US:watchlist=paper-dry-run", runtime_status["market_mode_summary_text"])
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-simple-section="runtime-status"', html_text)
            self.assertIn("连接账户", html_text)
            self.assertIn("账户模式", html_text)
            self.assertIn("Paper 账户", html_text)
            self.assertIn("Paper 模拟运行", html_text)
            self.assertNotIn("market_modes=", html_text)

    def test_dashboard_simple_mode_shows_market_structure_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            (watchlist_dir / "investment_paper_summary.json").write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_execution_summary.json").write_text(
                json.dumps({"broker_equity": 10000.0}),
                encoding="utf-8",
            )
            (watchlist_dir / "investment_guard_summary.json").write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_opportunity_summary.json").write_text(
                json.dumps({"adaptive_strategy_wait_count": 2}),
                encoding="utf-8",
            )
            (watchlist_dir / "investment_adaptive_strategy_summary.json").write_text(
                json.dumps(
                    {
                        "adaptive_strategy": {
                            "name": "ACM-RS",
                            "execution": {"rebalance_frequency": "weekly"},
                            "defensive": {"raise_entry_threshold_pct": 0.2},
                        },
                        "summary": {"defensive_cap_count": 2},
                    }
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["cards"][0]
            self.assertTrue(card["market_structure_summary"]["small_account_rule_active"])
            self.assertEqual(card["account_profile_summary"]["name"], "small")
            self.assertEqual(card["adaptive_strategy_summary"]["name"], "ACM-RS")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-simple-section="market-structure"', html_text)
            self.assertIn("市场约束", html_text)
            self.assertIn("账户档位", html_text)
            self.assertIn("策略框架", html_text)
            self.assertIn("策略提醒", html_text)
            self.assertIn("小资金规则", html_text)
            self.assertIn("当前权益处于小资金档，先优先 ETF。", html_text)
            self.assertIn("当前防守环境已把 2 个新开仓机会降级为观察。", html_text)

    def test_dashboard_simple_mode_shows_weekly_strategy_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            weekly_dir = base / "reports_investment_weekly"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "portfolio_strategy_context": [
                            {
                                "portfolio_id": "US:watchlist",
                                "account_profile_label": "小资金",
                                "market_rules_summary": "settlement=T+1 / no same-day round trip",
                                "adaptive_strategy_name": "ACM-RS",
                                "adaptive_strategy_summary": "上涨做相对强弱，高波动看回撤，下跌先防守；周调仓。",
                                "weekly_strategy_note": "本周有 2 个新开仓机会因防守环境被降级为观察，先不把回撤信号直接转成加仓动作。",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["cards"][0]
            self.assertEqual(card["weekly_strategy_context"]["account_profile_label"], "小资金")
            self.assertIn("防守环境被降级为观察", card["weekly_strategy_context"]["weekly_strategy_note"])
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-simple-section="weekly-strategy-context"', html_text)
            self.assertIn("本周策略解释", html_text)
            self.assertIn("周度解释", html_text)
            self.assertIn("settlement=T+1 / no same-day round trip", html_text)
            self.assertIn("本周有 2 个新开仓机会因防守环境被降级为观察", html_text)

    def test_dashboard_execution_plan_prefers_user_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_execution_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,status,execution_style,expected_cost_bps,reason,user_reason_label,user_reason",
                        "AAPL,BUY,PLANNED,VWAP_LITE_MIDDAY,18.4,rebalance_up|manual_review,大额订单待人工确认,单笔订单超出自动提交阈值，先人工确认。",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("单笔订单超出自动提交阈值，先人工确认。", html_text)
            self.assertNotIn("rebalance_up|manual_review", html_text)

    def test_dashboard_prefers_ibkr_paper_snapshot_before_local_ledger_in_paper_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_candidates.csv").write_text("symbol,score,action\nAAPL,0.8,HOLD\n", encoding="utf-8")
            (watchlist_dir / "investment_plan.csv").write_text("symbol,action,entry_style,notes\nAAPL,HOLD,HOLD_CORE,test\n", encoding="utf-8")
            (watchlist_dir / "investment_portfolio.csv").write_text("symbol,qty,market_value,weight,status\nAAPL,10,1000,0.1,OPEN\n", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("当前持仓 (IBKR Paper 快照)", html_text)
            self.assertNotIn("当前持仓 (本地模拟账本)", html_text)
            self.assertNotIn("Paper Ledger Equity", html_text)

    def test_dashboard_separates_trade_and_dry_run_views_when_local_ledger_runs_with_broker_submit(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            ibkr_cfg_path = base / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_TEST_DASHBOARD"',
                    ]
                ),
                encoding="utf-8",
            )
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            (watchlist_dir / "investment_paper_summary.json").write_text(
                json.dumps(
                    {
                        "equity_after": 101000,
                        "cash_after": 12000,
                        "target_invested_weight": 0.55,
                        "executed": True,
                    }
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_candidates.csv").write_text("symbol,score,action\nAAPL,0.8,HOLD\n", encoding="utf-8")
            (watchlist_dir / "investment_plan.csv").write_text("symbol,action,entry_style,notes\nAAPL,HOLD,HOLD_CORE,test\n", encoding="utf-8")
            (watchlist_dir / "investment_portfolio.csv").write_text("symbol,qty,market_value,weight,status\nAAPL,10,1000,0.1,OPEN\n", encoding="utf-8")
            (watchlist_dir / "investment_rebalance_trades.csv").write_text(
                "symbol,action,qty,price,trade_value,reason\nAAPL,BUY,10,100.0,1000.0,target_add\n",
                encoding="utf-8",
            )
            (watchlist_dir / "investment_execution_plan.csv").write_text(
                "symbol,action,status,reason\nAAPL,BUY,READY,trade\n",
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                        "        force_local_paper_ledger: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(len(payload["trade_cards"]), 1)
            self.assertEqual(len(payload["dry_run_cards"]), 1)
            self.assertEqual(payload["trade_cards"][0]["mode"], "paper-auto-submit")
            self.assertEqual(payload["dry_run_cards"][0]["mode"], "dry-run")

            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-filter="trade"', html_text)
            self.assertIn('data-filter="dry-run"', html_text)
            self.assertIn('data-view="trade"', html_text)
            self.assertIn('data-view="dry-run"', html_text)
            self.assertIn("Dry Run 页面说明", html_text)
            self.assertIn('data-simple-section="dry-run-banner"', html_text)
            self.assertIn("这里只做本地模拟，不会向 IBKR 下单。", html_text)
            self.assertIn("当前持仓 (IBKR Paper 快照)", html_text)
            self.assertIn("当前持仓 (本地模拟账本)", html_text)
            self.assertIn("Paper 自动执行", html_text)
            self.assertIn("本地模拟运行", html_text)
            dry_run_match = re.search(r'<section class="card"[^>]*data-dashboard-view="dry-run".*?</section>', html_text, re.S)
            self.assertIsNotNone(dry_run_match)
            dry_run_html = dry_run_match.group(0)
            self.assertIn("本地模拟账本状态", dry_run_html)
            self.assertIn("本地模拟调仓", dry_run_html)
            self.assertIn('data-simple-section="paper-plan"', dry_run_html)
            self.assertIn('data-simple-section="dry-run-overview"', html_text)
            self.assertNotIn("filled(status/audit)", dry_run_html)
            self.assertNotIn(">执行计划<", dry_run_html)

    def test_dashboard_renders_control_panel_and_card_toggles_when_control_service_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            preflight_dir = base / "reports_preflight"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            preflight_dir.mkdir(parents=True, exist_ok=True)
            (preflight_dir / "supervisor_preflight_summary.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-03-23T21:50:00",
                        "pass_count": 8,
                        "warn_count": 2,
                        "fail_count": 1,
                        "checks": [
                            {"name": "ibkr_port:4002", "status": "WARN", "detail": "127.0.0.1:4002 not_listening"},
                            {"name": "dashboard_db", "status": "FAIL", "detail": "audit db missing"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_candidates.csv").write_text("symbol,score,action\nAAPL,0.8,HOLD\n", encoding="utf-8")
            (watchlist_dir / "investment_plan.csv").write_text("symbol,action,entry_style,notes\nAAPL,HOLD,HOLD_CORE,test\n", encoding="utf-8")
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / "dashboard_control_state.json").write_text(
                json.dumps(
                    {
                        "service": {
                            "enabled": True,
                            "status": "running",
                            "host": "127.0.0.1",
                            "port": 8877,
                            "url": "http://127.0.0.1:8877",
                        },
                        "actions": {
                            "run_once_in_progress": False,
                            "weekly_review_in_progress": False,
                            "last_action": "refresh_dashboard",
                            "last_action_ts": "2026-03-13T12:00:00",
                            "last_error": "",
                        },
                        "portfolios": {
                            "US:watchlist": {
                                "market": "US",
                                "watchlist": "watchlist",
                                "portfolio_id": "US:watchlist",
                                "execution_control_mode": "AUTO",
                                "run_investment_paper": True,
                                "force_local_paper_ledger": True,
                                "run_investment_execution": True,
                                "submit_investment_execution": True,
                                "run_investment_guard": True,
                                "submit_investment_guard": False,
                                "run_investment_opportunity": True,
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_preflight_dir: "{preflight_dir}"',
                        "dashboard_control_enabled: true",
                        'dashboard_control_host: "127.0.0.1"',
                        "dashboard_control_port: 8877",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        force_local_paper_ledger: true",
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                        "        run_investment_guard: true",
                        "        run_investment_opportunity: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["dashboard_control"]["service"]["url"], "http://127.0.0.1:8877")
            self.assertEqual(payload["ops_overview"]["preflight_fail_count"], 1)
            self.assertEqual(payload["ops_overview"]["ibkr_port_warning_count"], 1)
            self.assertEqual(payload["ops_overview"]["preflight_banner_level"], "FAIL")
            self.assertIn("当前不建议自动执行", payload["ops_overview"]["preflight_banner_title"])
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Preflight 关键提示", html_text)
            self.assertIn("运维总览", html_text)
            self.assertIn("Dashboard 控制", html_text)
            self.assertIn('data-api-action="run_once"', html_text)
            self.assertIn('data-api-action="run_preflight"', html_text)
            self.assertIn('data-api-action="run_weekly_review"', html_text)
            self.assertIn('data-api-action="refresh_dashboard"', html_text)
            self.assertIn('data-mode-value="AUTO"', html_text)
            self.assertIn('data-mode-value="REVIEW_ONLY"', html_text)
            self.assertIn('data-mode-value="PAUSED"', html_text)
            self.assertIn('data-detail-mode="simple"', html_text)
            self.assertIn('data-detail-mode-button="simple"', html_text)
            self.assertIn('data-detail-mode-button="advanced"', html_text)
            self.assertIn('data-language="zh"', html_text)
            self.assertIn('data-language-button="zh"', html_text)
            self.assertIn('data-language-button="en"', html_text)
            self.assertIn('dashboard.language', html_text)
            self.assertIn("一眼看懂", html_text)
            self.assertIn('class="execution-mode-current"', html_text)
            self.assertIn('class="execution-mode-change"', html_text)
            self.assertIn("只保留人工审核", html_text)
            self.assertIn("暂停自动执行", html_text)
            self.assertIn('data-field="run_investment_execution"', html_text)
            self.assertIn('data-field="submit_investment_execution"', html_text)
            self.assertIn('data-simple-section="preflight-banner"', html_text)
            self.assertIn('data-simple-section="ops-overview"', html_text)
            self.assertIn('data-simple-section="focus-actions"', html_text)
            self.assertIn('data-simple-section="current-actions"', html_text)
            self.assertIn('data-simple-section="execution-plan"', html_text)
            self.assertIn('data-simple-section="market-overview"', html_text)
            self.assertIn("IB Gateway 端口", html_text)

    def test_dashboard_loads_ibkr_history_probe_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            preflight_dir = base / "reports_preflight"
            report_root = base / "reports_investment_xetra"
            report_dir = report_root / "xetra_top_quality"
            report_dir.mkdir(parents=True, exist_ok=True)
            preflight_dir.mkdir(parents=True, exist_ok=True)
            (preflight_dir / "ibkr_history_probe_summary.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-03-26T12:00:00",
                        "market_summary": [
                            {
                                "market": "XETRA",
                                "sample_count": 2,
                                "ok_count": 0,
                                "permission_count": 2,
                                "contract_count": 0,
                                "empty_count": 0,
                                "status_label": "权限待补",
                                "diagnosis": "至少一个样本合约能解析，但历史权限不足，优先检查该市场订阅/权限。",
                                "symbols": "SAP.DE,SIE.DE",
                            }
                        ],
                        "symbol_rows": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_preflight_dir: "{preflight_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "xetra"',
                        '    market: "XETRA"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlists/xetra_top_quality.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["ibkr_history_probe_summary"]["market_summary"][0]["status_label"], "权限待补")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("IBKR 历史接入诊断", html_text)
            self.assertIn("权限待补", html_text)
            self.assertIn("SAP.DE,SIE.DE", html_text)

    def test_dashboard_renders_live_weekly_feedback_confirm_button(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor_live.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            weekly_dir = base / "reports_investment_weekly_live"
            weekly_dir.mkdir(parents=True, exist_ok=True)
            ibkr_cfg_path = base / "ibkr_us_live.yaml"
            ibkr_cfg_path.write_text('mode: "live"\nexecution_mode: "investment_only"\naccount_id: "U1234567"\n', encoding="utf-8")
            (weekly_dir / "weekly_review_summary.json").write_text(
                json.dumps(
                    {
                        "execution_feedback_summary": [
                            {
                                "portfolio_id": "US:watchlist",
                                "market": "US",
                                "execution_feedback_action": "TIGHTEN",
                                "feedback_reason": "live confirm required",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_candidates.csv").write_text("symbol,score,action\nAAPL,0.8,HOLD\n", encoding="utf-8")
            (report_dir / "investment_plan.csv").write_text("symbol,action,entry_style,notes\nAAPL,HOLD,HOLD_CORE,test\n", encoding="utf-8")
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / "dashboard_control_state.json").write_text(
                json.dumps(
                    {
                        "service": {
                            "enabled": True,
                            "status": "running",
                            "host": "127.0.0.1",
                            "port": 8877,
                            "url": "http://127.0.0.1:8877",
                        },
                        "actions": {
                            "run_once_in_progress": False,
                            "weekly_review_in_progress": False,
                            "last_action": "refresh_dashboard",
                            "last_action_ts": "2026-03-23T12:00:00",
                            "last_error": "",
                        },
                        "portfolios": {
                            "US:watchlist": {
                                "market": "US",
                                "watchlist": "watchlist",
                                "portfolio_id": "US:watchlist",
                                "account_mode": "live",
                                "execution_control_mode": "AUTO",
                                "run_investment_paper": True,
                                "force_local_paper_ledger": True,
                                "run_investment_execution": True,
                                "submit_investment_execution": True,
                                "run_investment_guard": True,
                                "submit_investment_guard": True,
                                "run_investment_opportunity": True,
                                "weekly_feedback_present": True,
                                "weekly_feedback_signature": "sig-1",
                                "weekly_feedback_confirmed_signature": "",
                                "weekly_feedback_confirmed_ts": "",
                                "weekly_feedback_pending_live_confirm": True,
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_weekly_review_dir: "{weekly_dir}"',
                        "dashboard_control_enabled: true",
                        'dashboard_control_host: "127.0.0.1"',
                        "dashboard_control_port: 8877",
                        "weekly_review_auto_apply_live: false",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        force_local_paper_ledger: true",
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                        "        run_investment_guard: true",
                        "        submit_investment_guard: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("确认应用 Weekly Feedback", html_text)
            self.assertIn("待确认", html_text)
            self.assertIn("LIVE_CONFIRM_REQUIRED", json.dumps(payload, ensure_ascii=False))

    def test_dashboard_execution_weekly_orphans_are_separated(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            execution_dir = base / "reports_investment_execution"
            report_root = base / "reports_investment"
            execution_dir.mkdir(parents=True, exist_ok=True)
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (execution_dir / "investment_execution_weekly_summary.csv").write_text(
                "\n".join(
                    [
                        "week,week_start,market,portfolio_id,execution_run_rows,submitted_runs,planned_order_rows,submitted_order_rows,filled_order_rows,filled_with_audit_rows,blocked_opportunity_rows,error_order_rows,fill_rows,commission_total,realized_net_pnl,fill_rate_status,fill_rate_audit",
                        "2026-W11,2026-03-09,US,US:watchlist,3,1,4,2,1,1,1,0,1,12.5,45.7,0.50,0.50",
                        "2026-W11,2026-03-09,US,US:us_exec_watchlist,2,1,2,1,0,0,1,1,0,4.0,-3.0,0.00,0.00",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_execution_kpi_dir: "{execution_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            display_keys = {(row["market"], row["portfolio_id"]) for row in payload["execution_weekly_display"]}
            orphan_keys = {(row["market"], row["portfolio_id"]) for row in payload["execution_weekly_orphans"]}
            self.assertIn(("US", "US:watchlist"), display_keys)
            self.assertNotIn(("US", "US:us_exec_watchlist"), display_keys)
            self.assertIn(("US", "US:us_exec_watchlist"), orphan_keys)

    def test_dashboard_loads_broker_snapshot_per_market_portfolio(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_investment_execution_run(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "account_id": "DUQ152001",
                    "report_dir": str(report_root / "watchlist"),
                    "submitted": 0,
                    "order_count": 0,
                    "order_value": 0.0,
                    "broker_equity": 100000.0,
                    "broker_cash": 90000.0,
                    "target_equity": 85000.0,
                    "details": "{}",
                }
            )
            storage.insert_investment_broker_position(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:01+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "SPY",
                    "qty": 10.0,
                    "avg_cost": 510.0,
                    "market_price": 512.0,
                    "market_value": 5120.0,
                    "weight": 0.0512,
                    "source": "after",
                    "details": "{}",
                }
            )
            storage.insert_investment_broker_position(
                {
                    "run_id": "US-exec-0",
                    "ts": "2026-03-12T08:00:02+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "0883.HK",
                    "qty": 1000.0,
                    "avg_cost": 29.17,
                    "market_price": 28.88,
                    "market_value": 28880.0,
                    "weight": 0.2888,
                    "source": "after",
                    "details": "{}",
                }
            )
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            (watchlist_dir / "investment_paper_summary.json").write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_execution_summary.json").write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_guard_summary.json").write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_opportunity_summary.json").write_text("{}", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["cards"][0]
            self.assertEqual(card["portfolio_id"], "US:watchlist")
            self.assertEqual([row["symbol"] for row in card["broker_holdings"]], ["SPY"])

    def test_dashboard_loads_ibkr_health_summary_per_portfolio(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            storage.insert_risk_event(
                "IBKR_HEALTH_EVENT",
                10167.0,
                "delayed data",
                ts="2026-03-12T08:00:00+00:00",
                portfolio_id="US:watchlist",
                system_kind="investment_execution",
            )
            storage.insert_risk_event(
                "IBKR_HEALTH_EVENT",
                322.0,
                "account summary limit",
                ts="2026-03-12T08:05:00+00:00",
                portfolio_id="US:watchlist",
                system_kind="investment_execution",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["cards"][0]
            self.assertEqual(card["health_summary"]["status"], "DEGRADED")
            self.assertEqual(card["health_summary"]["delayed_count"], 1)
            self.assertEqual(card["health_summary"]["account_limit_count"], 1)
            self.assertEqual(payload["health_overview"][0]["status"], "DEGRADED")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("当前 1 个组合里，0 个连接正常，1 个降级，0 个受限。 异常计数：延迟 1 / 权限 0 / 中断 0 / 额度 1。", html_text)

    def test_dashboard_loads_analysis_chain_per_portfolio(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            db_path = base / "audit.db"
            storage = Storage(str(db_path))
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            storage.upsert_investment_analysis_state(
                {
                    "ts": "2026-03-12T08:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "AAPL",
                    "analysis_run_id": "run-1",
                    "status": "ENTRY_READY",
                    "lifecycle": "ENTRY",
                    "action": "ACCUMULATE",
                    "entry_status": "ENTRY_NOW",
                    "score": 0.91,
                    "held_qty": 0.0,
                    "report_dir": str(watchlist_dir),
                    "run_kind": "opportunity",
                    "reason": "pullback reached",
                    "details": "{}",
                }
            )
            storage.upsert_investment_analysis_state(
                {
                    "ts": "2026-03-12T08:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "MSFT",
                    "analysis_run_id": "run-1",
                    "status": "WATCHING",
                    "lifecycle": "WATCH",
                    "action": "WATCH",
                    "entry_status": "",
                    "score": 0.20,
                    "held_qty": 0.0,
                    "report_dir": str(watchlist_dir),
                    "run_kind": "opportunity",
                    "reason": "keep watching",
                    "details": "{}",
                }
            )
            storage.insert_investment_analysis_event(
                {
                    "ts": "2026-03-12T08:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "AAPL",
                    "analysis_run_id": "run-1",
                    "event_kind": "WATCH_TO_ENTRY",
                    "from_status": "WATCHING",
                    "to_status": "ENTRY_READY",
                    "from_lifecycle": "WATCH",
                    "to_lifecycle": "ENTRY",
                    "action": "ACCUMULATE",
                    "entry_status": "ENTRY_NOW",
                    "score": 0.91,
                    "held_qty": 0.0,
                    "report_dir": str(watchlist_dir),
                    "run_kind": "opportunity",
                    "summary": "AAPL 观望中 -> 可入场",
                    "details": "{}",
                }
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_db: "{db_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            card = payload["cards"][0]
            self.assertEqual([row["symbol"] for row in card["analysis_states"][:2]], ["AAPL", "MSFT"])
            self.assertEqual(card["analysis_states"][0]["status_label"], "可入场")
            self.assertEqual(card["analysis_events"][0]["event_kind"], "WATCH_TO_ENTRY")
            self.assertEqual(card["analysis_events"][0]["to_status_label"], "可入场")

    def test_dashboard_stock_list_keeps_general_symbols_across_runtime_scopes_and_adds_holdings(self):
        runtime_scope_roots = []
        try:
            with tempfile.TemporaryDirectory() as tmp:
                base = Path(tmp)
                cfg_path = base / "supervisor.yaml"
                summary_dir = base / "reports_supervisor"
                current_ibkr_cfg = base / "ibkr_current.yaml"
                other_ibkr_cfg = base / "ibkr_other.yaml"
                current_ibkr = {
                    "mode": "paper",
                    "execution_mode": "investment_only",
                    "account_id": "DU_SCOPE_STOCKLIST_A",
                }
                other_ibkr = {
                    "mode": "live",
                    "execution_mode": "investment_only",
                    "account_id": "U_SCOPE_STOCKLIST_B",
                }
                current_ibkr_cfg.write_text(
                    "\n".join(
                        [
                            'mode: "paper"',
                            'execution_mode: "investment_only"',
                            'account_id: "DU_SCOPE_STOCKLIST_A"',
                        ]
                    ),
                    encoding="utf-8",
                )
                other_ibkr_cfg.write_text(
                    "\n".join(
                        [
                            'mode: "live"',
                            'execution_mode: "investment_only"',
                            'account_id: "U_SCOPE_STOCKLIST_B"',
                        ]
                    ),
                    encoding="utf-8",
                )

                current_scope = scope_from_ibkr_config(current_ibkr)
                other_scope = scope_from_ibkr_config(other_ibkr)
                current_root = (SUPERVISOR_BASE_DIR / "runtime_data" / current_scope.label).resolve()
                other_root = (SUPERVISOR_BASE_DIR / "runtime_data" / other_scope.label).resolve()
                runtime_scope_roots.extend([current_root, other_root])

                current_report_dir = current_root / "reports_investment" / "watchlist"
                other_report_dir = other_root / "reports_investment" / "watchlist"
                current_db = current_root / "audit.db"
                current_report_dir.mkdir(parents=True, exist_ok=True)
                other_report_dir.mkdir(parents=True, exist_ok=True)
                current_db.parent.mkdir(parents=True, exist_ok=True)

                for report_dir in (current_report_dir, other_report_dir):
                    for name in (
                        "investment_paper_summary.json",
                        "investment_execution_summary.json",
                        "investment_guard_summary.json",
                        "investment_opportunity_summary.json",
                    ):
                        (report_dir / name).write_text("{}", encoding="utf-8")

                (current_report_dir / "investment_candidates.csv").write_text(
                    "\n".join(
                        [
                            "symbol,score,action,sector",
                            "AAPL,0.91,ACCUMULATE,Technology",
                        ]
                    ),
                    encoding="utf-8",
                )
                (current_report_dir / "investment_opportunity_scan.csv").write_text(
                    "\n".join(
                        [
                            "symbol,entry_status,entry_reason,action,score",
                            "AAPL,ENTRY_NOW,pullback reached,ACCUMULATE,0.91",
                        ]
                    ),
                    encoding="utf-8",
                )
                (other_report_dir / "investment_candidates.csv").write_text(
                    "\n".join(
                        [
                            "symbol,score,action,sector",
                            "MSFT,0.72,WATCH,Technology",
                        ]
                    ),
                    encoding="utf-8",
                )
                (other_report_dir / "investment_opportunity_scan.csv").write_text(
                    "\n".join(
                        [
                            "symbol,entry_status,entry_reason,action,score",
                            "MSFT,NEAR_ENTRY,watch the next pullback,WATCH,0.72",
                        ]
                    ),
                    encoding="utf-8",
                )

                storage = Storage(str(current_db))
                storage.insert_investment_execution_run(
                    {
                        "run_id": "US-stocklist-0",
                        "ts": "2026-03-12T08:00:00+00:00",
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "account_id": "DU_SCOPE_STOCKLIST_A",
                        "report_dir": str(current_report_dir),
                        "submitted": 0,
                        "order_count": 0,
                        "order_value": 0.0,
                        "broker_equity": 100000.0,
                        "broker_cash": 90000.0,
                        "target_equity": 85000.0,
                        "details": "{}",
                    }
                )
                storage.insert_investment_broker_position(
                    {
                        "run_id": "US-stocklist-0",
                        "ts": "2026-03-12T08:00:01+00:00",
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "symbol": "NVDA",
                        "qty": 3.0,
                        "avg_cost": 880.0,
                        "market_price": 905.0,
                        "market_value": 2715.0,
                        "weight": 0.02715,
                        "source": "after",
                        "details": "{}",
                    }
                )

                cfg_path.write_text(
                    "\n".join(
                        [
                            'timezone: "Australia/Sydney"',
                            f'summary_out_dir: "{summary_dir}"',
                            'dashboard_db: "audit.db"',
                            "poll_sec: 30",
                            "markets:",
                            '  - name: "us"',
                            '    market: "US"',
                            "    enabled: true",
                            '    report_time: "16:30"',
                            "    reports:",
                            '      - kind: "investment"',
                            '        out_dir: "reports_investment"',
                            f'        ibkr_config: "{current_ibkr_cfg}"',
                            '        watchlist_yaml: "config/watchlist.yaml"',
                        ]
                    ),
                    encoding="utf-8",
                )

                payload = build_dashboard(str(cfg_path), str(summary_dir))
                groups = payload["stock_list_groups"]
                self.assertEqual(len(groups), 1)
                rows = groups[0]["rows"]
                symbols = [row["symbol"] for row in rows]
                self.assertIn("AAPL", symbols)
                self.assertIn("MSFT", symbols)
                self.assertIn("NVDA", symbols)
                nvda_row = next(row for row in rows if row["symbol"] == "NVDA")
                self.assertEqual(nvda_row["list_origin"], "HOLDING_ONLY")
                self.assertEqual(nvda_row["tracked_status"], "持仓补充")
                msft_row = next(row for row in rows if row["symbol"] == "MSFT")
                self.assertIn(other_scope.label, msft_row["source_scopes"])
        finally:
            for runtime_scope_root in runtime_scope_roots:
                if runtime_scope_root.exists():
                    shutil.rmtree(runtime_scope_root, ignore_errors=True)

    def test_dashboard_renders_stock_list_as_last_section_and_tag(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,score,action,sector",
                        "AAPL,0.91,ACCUMULATE,Technology",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_opportunity_scan.csv").write_text(
                "\n".join(
                    [
                        "symbol,entry_status,entry_reason,action,score",
                        "AAPL,ENTRY_NOW,pullback reached,ACCUMULATE,0.91",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn('data-filter="stock-list"', html_text)
            self.assertIn('id="stock-list"', html_text)
            self.assertIn('data-simple-section="stock-list-intro"', html_text)
            self.assertIn("这里汇总当前需要跟踪的股票；基础观察池不会因切换账号或 live/paper 而消失。", html_text)
            self.assertGreater(html_text.rfind('id="stock-list"'), html_text.find('<h2>市场总览</h2>'))

    def test_supervisor_scopes_relative_report_and_db_paths_by_mode_and_account(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "live"',
                        'execution_mode: "investment_only"',
                        'account_id: "U_SCOPE_TEST"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    reports:",
                        '      - kind: "investment"',
                        '        out_dir: "reports_investment"',
                        '        db: "audit.db"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = supervisor.markets[0].reports[0]
            scope = scope_from_ibkr_config({"mode": "live", "execution_mode": "investment_only", "account_id": "U_SCOPE_TEST"})
            expected_root = SUPERVISOR_BASE_DIR / "runtime_data" / scope.label
            self.assertEqual(
                supervisor._report_output_dir(item, "US"),
                (expected_root / "reports_investment" / "watchlist").resolve(),
            )
            self.assertEqual(
                supervisor._db_path(item, "US"),
                (expected_root / "audit.db").resolve(),
            )

    def test_supervisor_scopes_summary_output_dir_by_mode_and_account(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_SCOPE_SUMMARY"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        'summary_out_dir: "reports_supervisor"',
                        "scope_summary_out_dir: true",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    reports:",
                        '      - kind: "investment"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        out_dir: "reports_investment"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            scope = scope_from_ibkr_config(
                {"mode": "paper", "execution_mode": "investment_only", "account_id": "DU_SCOPE_SUMMARY"}
            )
            expected = (SUPERVISOR_BASE_DIR / "runtime_data" / scope.label / "reports_supervisor").resolve()
            self.assertEqual(supervisor._summary_output_dir(), expected)

    def test_dashboard_uses_scoped_relative_db_for_matching_runtime_scope(self):
        runtime_scope_root = None
        try:
            with tempfile.TemporaryDirectory() as tmp:
                base = Path(tmp)
                cfg_path = base / "supervisor.yaml"
                summary_dir = base / "reports_supervisor"
                ibkr_cfg_path = base / "ibkr_test.yaml"
                ibkr_cfg = {
                    "mode": "paper",
                    "execution_mode": "investment_only",
                    "account_id": "DU_SCOPE_TEST",
                }
                ibkr_cfg_path.write_text(
                    "\n".join(
                        [
                            'mode: "paper"',
                            'execution_mode: "investment_only"',
                            'account_id: "DU_SCOPE_TEST"',
                        ]
                    ),
                    encoding="utf-8",
                )
                scope = scope_from_ibkr_config(ibkr_cfg)
                runtime_scope_root = (SUPERVISOR_BASE_DIR / "runtime_data" / scope.label).resolve()
                report_dir = resolve_scoped_runtime_path(SUPERVISOR_BASE_DIR, "reports_investment", scope) / "watchlist"
                db_path = resolve_scoped_runtime_path(SUPERVISOR_BASE_DIR, "audit.db", scope)
                report_dir.mkdir(parents=True, exist_ok=True)
                db_path.parent.mkdir(parents=True, exist_ok=True)

                storage = Storage(str(db_path))
                storage.insert_investment_execution_run(
                    {
                        "run_id": "US-scope-0",
                        "ts": "2026-03-12T08:00:00+00:00",
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "account_id": "DU_SCOPE_TEST",
                        "report_dir": str(report_dir),
                        "submitted": 0,
                        "order_count": 0,
                        "order_value": 0.0,
                        "broker_equity": 100000.0,
                        "broker_cash": 90000.0,
                        "target_equity": 85000.0,
                        "details": "{}",
                    }
                )
                storage.insert_investment_broker_position(
                    {
                        "run_id": "US-scope-0",
                        "ts": "2026-03-12T08:00:01+00:00",
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "symbol": "GLD",
                        "qty": 2.5,
                        "avg_cost": 210.0,
                        "market_price": 211.0,
                        "market_value": 527.5,
                        "weight": 0.0053,
                        "source": "after",
                        "details": "{}",
                    }
                )
                (report_dir / "investment_paper_summary.json").write_text("{}", encoding="utf-8")
                (report_dir / "investment_execution_summary.json").write_text("{}", encoding="utf-8")
                (report_dir / "investment_guard_summary.json").write_text("{}", encoding="utf-8")
                (report_dir / "investment_opportunity_summary.json").write_text("{}", encoding="utf-8")

                cfg_path.write_text(
                    "\n".join(
                        [
                            'timezone: "Australia/Sydney"',
                            f'summary_out_dir: "{summary_dir}"',
                            'dashboard_db: "audit.db"',
                            "poll_sec: 30",
                            "markets:",
                            '  - name: "us"',
                            '    market: "US"',
                            "    enabled: true",
                            '    report_time: "16:30"',
                            "    reports:",
                            '      - kind: "investment"',
                            '        out_dir: "reports_investment"',
                            '        db: "audit.db"',
                            f'        ibkr_config: "{ibkr_cfg_path}"',
                            '        watchlist_yaml: "config/watchlist.yaml"',
                        ]
                    ),
                    encoding="utf-8",
                )
                payload = build_dashboard(str(cfg_path), str(summary_dir))
                card = payload["cards"][0]
                self.assertEqual(card["runtime_scope"], scope.label)
                self.assertEqual([row["symbol"] for row in card["broker_holdings"]], ["GLD"])
        finally:
            if runtime_scope_root and runtime_scope_root.exists():
                shutil.rmtree(runtime_scope_root, ignore_errors=True)

    def test_run_cycle_marks_report_day_when_due(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            reports_root = Path(tmp) / "reports_investment"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_TEST_REPORT_DUE"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Australia/Sydney"',
                        "    enabled: true",
                        '    report_time: "08:30"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 12, 8, 45, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_generate_reports") as mock_reports:
                supervisor.run_cycle(now)
            mock_reports.assert_called_once()

    def test_run_cmd_returns_false_on_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with patch("src.app.supervisor.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=["x"], timeout=1)):
                ok = supervisor._run_cmd("slow-task", ["python", "-m", "slow"], timeout_sec=1)
            self.assertFalse(ok)

    def test_investment_execution_waits_for_offset_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            reports_root = Path(tmp) / "reports_investment"
            report_dir = reports_root / "resolved_hk_top100_bluechip"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_TEST_EXEC_WAIT"',
                    ]
                ),
                encoding="utf-8",
            )
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_candidates.csv").write_text("symbol,action\n0883.HK,BUY\n", encoding="utf-8")
            (report_dir / "investment_plan.csv").write_text("symbol,action\n0883.HK,BUY\n", encoding="utf-8")
            (report_dir / "investment_report.md").write_text("# report\n", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Australia/Sydney"',
                        "    enabled: true",
                        '    report_time: "20:00"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        run_investment_execution: true",
                        '        execution_time: "12:35"',
                        "        execution_day_offset: 1",
                        "        submit_investment_execution: false",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = supervisor.markets[0].reports[0]
            item["_last_successful_report_day"] = "2026-03-12"

            too_early = datetime(2026, 3, 12, 13, 0, 0, tzinfo=supervisor.tz)
            next_day = datetime(2026, 3, 13, 13, 0, 0, tzinfo=supervisor.tz)

            with patch.object(supervisor, "_run_investment_execution") as mock_exec:
                supervisor.run_cycle(too_early)
                mock_exec.assert_not_called()
                supervisor.run_cycle(next_day)
                mock_exec.assert_called_once()
            self.assertEqual(item["_last_execution_for_report_day"], "2026-03-12")

    def test_restore_report_state_from_report_and_execution_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            reports_root = Path(tmp) / "reports_investment"
            report_dir = reports_root / "resolved_hk_top100_bluechip"
            report_dir.mkdir(parents=True, exist_ok=True)
            report_file = report_dir / "investment_report.md"
            exec_file = report_dir / "investment_execution_summary.json"
            report_file.write_text("report", encoding="utf-8")
            exec_file.write_text("{}", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        "    enabled: true",
                        '    report_time: "20:00"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        run_investment_execution: true",
                        '        execution_time: "12:35"',
                        "        execution_day_offset: 1",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            item = supervisor.markets[0].reports[0]
            supervisor._restore_report_state(item, "HK")
            expected_day = datetime.fromtimestamp(
                report_file.stat().st_mtime,
                tz=supervisor.tz,
            ).strftime("%Y-%m-%d")
            self.assertEqual(item["_last_successful_report_day"], expected_day)
            self.assertEqual(item["_last_execution_for_report_day"], item["_last_successful_report_day"])

    def test_generate_reports_skips_local_paper_ledger_when_ibkr_paper_submit_is_primary(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            reports_root = Path(tmp) / "reports_investment"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_TEST_PAPER"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "America/New_York"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            with patch.object(supervisor, "_run_cmd", return_value=True) as mock_run_cmd:
                supervisor._generate_reports(
                    market,
                    day_key="2026-03-12",
                    market_now=datetime(2026, 3, 12, 17, 0, 0, tzinfo=supervisor.tz),
                )
            call_names = [str(call.args[0]) for call in mock_run_cmd.call_args_list]
            self.assertTrue(any(name.startswith("generate_investment_report:") for name in call_names))
            self.assertFalse(any(name.startswith("run_investment_paper:") for name in call_names))

    def test_market_holiday_blocks_exchange_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            holiday_path = Path(tmp) / "holidays.yaml"
            holiday_path.write_text(
                "\n".join(
                    [
                        "markets:",
                        "  XETRA:",
                        "    holidays:",
                        '      - "2026-04-03"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'market_holidays_config: "{holiday_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "xetra"',
                        '    market: "XETRA"',
                        '    local_timezone: "Europe/Berlin"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        "    watchlists: []",
                        "    reports: []",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: true",
                        "      weekdays: [0, 1, 2, 3, 4]",
                        '      start: "09:00"',
                        '      end: "17:30"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            now = datetime(2026, 4, 3, 12, 0, 0, tzinfo=supervisor.tz)
            self.assertFalse(supervisor._market_exchange_open(market, now))

    def test_cn_market_holiday_blocks_exchange_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            holiday_path = Path(tmp) / "holidays.yaml"
            holiday_path.write_text(
                "\n".join(
                    [
                        "markets:",
                        "  CN:",
                        "    holidays:",
                        '      - "2026-10-01"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'market_holidays_config: "{holiday_path}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "cn"',
                        '    market: "CN"',
                        '    local_timezone: "Asia/Shanghai"',
                        "    enabled: true",
                        '    report_time: "15:30"',
                        "    watchlists: []",
                        "    reports: []",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: true",
                        "      weekdays: [0, 1, 2, 3, 4]",
                        '      start: "09:30"',
                        '      end: "15:00"',
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            now = datetime(2026, 10, 1, 11, 0, 0, tzinfo=supervisor.tz)
            self.assertFalse(supervisor._market_exchange_open(market, now))

    def test_additional_trading_window_marks_us_overnight_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "America/New_York"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    watchlists: []",
                        "    reports: []",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: true",
                        "      weekdays: [0, 1, 2, 3, 4]",
                        '      start: "09:30"',
                        '      end: "16:00"',
                        "      additional_windows:",
                        '        - start: "20:00"',
                        '          end: "03:50"',
                        "          weekdays: [6, 0, 1, 2, 3]",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            now = datetime(2026, 3, 16, 12, 30, 0, tzinfo=supervisor.tz)  # 21:30 America/New_York on Sunday
            self.assertTrue(supervisor._market_exchange_open(market, now))

    def test_report_freshness_blocks_stale_trading_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            report_root = Path(tmp) / "reports"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            marker = report_dir / "investment_report.md"
            marker.write_text("report", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "report_max_trading_days_old: 1",
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "America/New_York"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            item = supervisor.markets[0].reports[0]
            item["_last_successful_report_day"] = "2026-03-10"
            ok, reason = supervisor._report_fresh_enough(
                market,
                item,
                report_market="US",
                market_now=datetime(2026, 3, 13, 8, 0, 0, tzinfo=supervisor.tz),
            )
            self.assertFalse(ok)
            self.assertTrue(str(reason).startswith("stale_report_trading_days_old:"))

    def test_report_time_can_be_overridden_per_report_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "America/New_York"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        '        out_dir: "reports_investment_overnight"',
                        '        watchlist_yaml: "config/watchlists/us_overnight_core.yaml"',
                        '        report_time: "19:30"',
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            item = market.reports[0]
            market_now = supervisor._market_now(
                datetime(2026, 3, 12, 22, 0, 0, tzinfo=supervisor.tz),
                market,
            )
            should_run, reason = supervisor._report_action_reason(
                market,
                item,
                report_market="US",
                day_key="2026-03-12",
                market_now=market_now,
            )
            self.assertFalse(should_run)
            self.assertEqual(reason, "before_report_time")

    def test_investment_guard_runs_inside_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_root = Path(tmp) / "reports_investment"
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "Australia/Sydney"',
                        "    enabled: true",
                        '    report_time: "08:30"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_guard: true",
                        '        guard_start: "00:40"',
                        '        guard_end: "06:40"',
                        "        guard_interval_min: 30",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 13, 1, 15, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_report_fresh_enough", return_value=(True, "fresh")), patch.object(
                supervisor, "_run_investment_guard"
            ) as mock_guard:
                supervisor.run_cycle(now)
                mock_guard.assert_called_once()

    def test_generate_reports_runs_baseline_when_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            reports_root = Path(tmp) / "reports_investment"
            ibkr_cfg_path = Path(tmp) / "ibkr_test.yaml"
            ibkr_cfg_path.write_text(
                "\n".join(
                    [
                        'mode: "paper"',
                        'execution_mode: "investment_only"',
                        'account_id: "DU_TEST_BASELINE"',
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        '    local_timezone: "America/New_York"',
                        "    enabled: true",
                        '    report_time: "16:30"',
                        '    watchlist_refresh_time: "19:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        f'        ibkr_config: "{ibkr_cfg_path}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_paper: true",
                        "        run_baseline_regression: true",
                        '        baseline_out_dir: "reports_baseline"',
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            market = supervisor.markets[0]
            with patch.object(supervisor, "_run_cmd", return_value=True) as mock_run_cmd, patch.object(
                supervisor,
                "_run_baseline_regression",
                return_value=True,
            ) as mock_baseline:
                supervisor._generate_reports(
                    market,
                    day_key="2026-03-12",
                    market_now=datetime(2026, 3, 12, 17, 0, 0, tzinfo=supervisor.tz),
                )
            self.assertTrue(mock_run_cmd.called)
            mock_baseline.assert_called_once()

    def test_market_local_timezone_controls_due_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "supervisor.yaml"
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "xetra"',
                        '    market: "XETRA"',
                        '    local_timezone: "Europe/Berlin"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        '        out_dir: "reports_investment"',
                        '        watchlist_yaml: "config/watchlists/xetra_top_quality.yaml"',
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 13, 4, 5, 0, tzinfo=supervisor.tz)  # 18:05 Europe/Berlin on 2026-03-12
            with patch.object(supervisor, "_generate_reports") as mock_reports:
                supervisor.run_cycle(now)
            mock_reports.assert_called_once()

    def test_closed_market_report_is_not_repeated_when_marker_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_root = Path(tmp) / "reports_investment"
            summary_dir = Path(tmp) / "reports_supervisor"
            report_dir = reports_root / "resolved_hk_top100_bluechip"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_report.md").write_text("ok", encoding="utf-8")
            (report_dir / "enrichment.json").write_text(
                '{"macro_indicators":{"fed_funds":3.5},"markets":{"source":"test"},"macro_events":[]}',
                encoding="utf-8",
            )
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Asia/Hong_Kong"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:10"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        rerun_report_on_macro_change: false",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            current = datetime.now(supervisor.tz)
            now = current.replace(hour=23, minute=9, second=0, microsecond=0)
            with patch.object(supervisor, "_generate_reports") as mock_reports:
                supervisor.run_cycle(now)
            mock_reports.assert_not_called()

    def test_closed_market_can_rerun_when_macro_signature_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_root = Path(tmp) / "reports_investment"
            summary_dir = Path(tmp) / "reports_supervisor"
            report_dir = reports_root / "resolved_hk_top100_bluechip"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_report.md").write_text("ok", encoding="utf-8")
            (report_dir / "enrichment.json").write_text(
                '{"macro_indicators":{"fed_funds":3.5},"markets":{"source":"test"},"macro_events":[]}',
                encoding="utf-8",
            )
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Asia/Hong_Kong"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:10"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        rerun_report_on_macro_change: true",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            current = datetime.now(supervisor.tz)
            now = current.replace(hour=23, minute=9, second=0, microsecond=0)
            with patch.object(supervisor, "_current_macro_signature", return_value="changed"), patch.object(
                supervisor,
                "_generate_reports",
            ) as mock_reports:
                supervisor.run_cycle(now)
            mock_reports.assert_called_once()

    def test_closed_market_market_snapshot_change_does_not_rerun_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports_root = Path(tmp) / "reports_investment"
            summary_dir = Path(tmp) / "reports_supervisor"
            report_dir = reports_root / "resolved_hk_top100_bluechip"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_report.md").write_text("ok", encoding="utf-8")
            (report_dir / "enrichment.json").write_text(
                json.dumps(
                    {
                        "macro_indicators": {"fed_funds": 3.5},
                        "markets": {"source": "snapshot_a", "tickers": {"2800.HK": {"ret1d": 0.01}}},
                        "macro_events": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Asia/Hong_Kong"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:10"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{reports_root}"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        rerun_report_on_macro_change: true",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            current = datetime.now(supervisor.tz)
            now = current.replace(hour=23, minute=9, second=0, microsecond=0)
            with patch.object(
                supervisor,
                "_current_macro_signature",
                return_value=supervisor._report_macro_signature(supervisor.markets[0].reports[0], "HK"),
            ), patch.object(supervisor, "_generate_reports") as mock_reports:
                supervisor.run_cycle(now)
            mock_reports.assert_not_called()

    def test_opportunity_is_skipped_when_report_candidates_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "xetra"',
                        '    market: "XETRA"',
                        '    local_timezone: "Europe/Berlin"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:00"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        '        out_dir: "reports_investment"',
                        '        watchlist_yaml: "config/watchlists/xetra_top_quality.yaml"',
                        "        run_investment_opportunity: true",
                        '        opportunity_start: "09:25"',
                        '        opportunity_end: "17:20"',
                        "        opportunity_interval_min: 30",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            now = datetime(2026, 3, 12, 23, 24, 0, tzinfo=supervisor.tz)
            with patch.object(supervisor, "_run_investment_opportunity") as mock_opportunity:
                supervisor.run_cycle(now)
            mock_opportunity.assert_not_called()
            payload = json.loads((summary_dir / "supervisor_cycle_summary.json").read_text(encoding="utf-8"))
            xetra = payload["markets"][0]
            self.assertIn("missing_report_files:investment_candidates.csv", xetra["opportunity_skip_reasons"])

    def test_cycle_summary_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_dir = Path(tmp) / "reports_supervisor"
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "hk"',
                        '    market: "HK"',
                        '    local_timezone: "Asia/Hong_Kong"',
                        "    enabled: true",
                        '    report_time: "18:00"',
                        '    watchlist_refresh_time: "17:10"',
                        "    watchlists: []",
                        "    reports:",
                        '      - kind: "investment"',
                        '        out_dir: "reports_investment"',
                        '        watchlist_yaml: "config/watchlists/resolved_hk_top100_bluechip.yaml"',
                        "        rerun_report_on_macro_change: false",
                        "    short_safety_sync:",
                        "      enabled: false",
                        "    trading:",
                        "      enabled: false",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with patch.object(supervisor, "_generate_reports"):
                supervisor.run_cycle(datetime(2026, 3, 12, 12, 0, 0, tzinfo=supervisor.tz))
            self.assertTrue((summary_dir / "supervisor_cycle_summary.json").exists())
            self.assertTrue((summary_dir / "supervisor_cycle_summary.md").exists())
            payload = json.loads((summary_dir / "supervisor_cycle_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["markets"][0]["priority_order"], 1)
            self.assertIn("priority_reason", payload["markets"][0])

    def test_refresh_dashboard_can_open_browser_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_dir = Path(tmp) / "reports_supervisor"
            summary_dir.mkdir(parents=True, exist_ok=True)
            (summary_dir / "dashboard.html").write_text("<html></html>", encoding="utf-8")
            cfg_path = Path(tmp) / "supervisor.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "dashboard_enabled: true",
                        "dashboard_auto_open: true",
                        "poll_sec: 30",
                        "markets: []",
                    ]
                ),
                encoding="utf-8",
            )
            supervisor = Supervisor(str(cfg_path))
            with patch.object(supervisor, "_run_cmd", return_value=True) as mock_run_cmd, patch(
                "src.app.supervisor.webbrowser.open", return_value=True
            ) as mock_browser:
                self.assertTrue(supervisor._refresh_dashboard())
                self.assertTrue(supervisor._dashboard_opened_once)
                self.assertTrue(supervisor._refresh_dashboard())
            mock_run_cmd.assert_called()
            mock_browser.assert_called_once()

    def test_dashboard_marks_research_only_card_as_no_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment_cn"
            report_dir = report_root / "cn_top_quality"
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "investment_report.md").write_text(
                "\n".join(
                    [
                        "# Investment Candidate Report",
                        "",
                        "## Market Summary",
                        "- 市场画像: name=cn_a_share_research；benchmark=510300.SS；timezone=Asia/Shanghai；style_bias=quality_and_policy_resilience",
                        "- 市场备注: 当前为 research-only 中国A股推荐池，不启用 execution、guard 或 broker submit。",
                    ]
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,score,asset_class,asset_theme,sector,industry",
                        "600519.SS,HOLD,0.81,equity,consumer,Consumer Defensive,Beverages",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "cn"',
                        '    market: "CN"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlists/cn_top_quality.yaml"',
                        "        research_only: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("research-only", html_text)
            self.assertIn("只做研究", html_text)
            self.assertIn("只研究推荐，不执行 broker paper/live 下单", html_text)
            self.assertIn("研究推荐", html_text)
            self.assertIn("推荐 Top10 摘要", html_text)
            self.assertIn("600519.SS(HOLD)", html_text)
            self.assertIn("行业/主题分布", html_text)
            self.assertIn("consumer:1", html_text)
            self.assertIn("研究结论摘要", html_text)
            self.assertIn("当前建议:", html_text)
            self.assertIn("重点标的:", html_text)
            self.assertIn("执行方式:", html_text)
            self.assertIn("补充说明:", html_text)
            self.assertIn("Recommendation: ", html_text)
            self.assertIn("Focus symbols: ", html_text)
            self.assertIn("市场画像:", html_text)

    def test_dashboard_loads_data_quality_summary_and_candidate_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment_us"
            report_dir = report_root / "watchlist"
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_paper_summary.json").write_text(
                json.dumps(
                    {
                        "risk_dynamic_net_exposure": 0.62,
                        "risk_dynamic_gross_exposure": 0.70,
                        "risk_avg_pair_correlation": 0.54,
                        "risk_stress_worst_scenario_label": "流动性恶化",
                        "risk_stress_worst_loss": 0.073,
                        "risk_notes": ["组合平均流动性偏弱，降低总敞口并保留现金。"],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "risk_dynamic_net_exposure": 0.62,
                        "risk_dynamic_gross_exposure": 0.70,
                        "risk_avg_pair_correlation": 0.54,
                        "risk_stress_worst_scenario_label": "流动性恶化",
                        "risk_stress_worst_loss": 0.073,
                        "risk_notes": ["组合平均流动性偏弱，降低总敞口并保留现金。"],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_data_quality_summary.json").write_text(
                json.dumps(
                    {
                        "avg_data_quality_score": 0.83,
                        "avg_source_coverage": 0.91,
                        "avg_missing_ratio": 0.11,
                        "low_quality_count": 2,
                        "ranked_low_quality_count": 1,
                        "history_source_counts": {
                            "ibkr": 12,
                            "yfinance": 0,
                            "missing": 0,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_cost_summary.json").write_text(
                json.dumps(
                    {
                        "avg_expected_cost_bps": 18.4,
                        "high_cost_count": 1,
                        "low_liquidity_count": 0,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_shadow_model_summary.json").write_text(
                json.dumps(
                    {
                        "summary": {
                            "enabled": True,
                            "training_samples": 128,
                            "horizon_days": 20,
                            "avg_shadow_ml_score": 0.19,
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,score,score_before_cost,expected_cost_bps,shadow_ml_score,data_quality_score,source_coverage,missing_ratio,history_source,asset_class,asset_theme,sector,industry",
                        "AAPL,ACCUMULATE,0.81,0.84,18.4,0.44,0.93,1.00,0.04,ibkr,equity,quality,Technology,Consumer Electronics",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_report.md").write_text(
                "\n".join(
                    [
                        "# Investment Candidate Report",
                        "",
                        "## Market Summary",
                        "- 数据质量: avg_score=0.83；source_cov=0.91；missing_ratio=0.11；low_quality=2；top_low_quality=1",
                    ]
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                        "        run_investment_execution: true",
                        "        submit_investment_execution: true",
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertAlmostEqual(payload["cards"][0]["data_quality_summary"]["avg_data_quality_score"], 0.83, places=6)
            self.assertEqual(payload["market_data_health_overview"][0]["status_label"], "IBKR正常")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("Shadow ML", html_text)
            self.assertIn("avg_ml=0.19", html_text)
            self.assertIn("数据质量", html_text)
            self.assertIn("avg=0.83 / low=2 / src_cov=0.91 / miss=0.11", html_text)
            self.assertIn("市场数据健康总览", html_text)
            self.assertIn('data-simple-section="market-data-health"', html_text)
            self.assertIn("当前 1 个市场里，1 个 IBKR 正常，0 个研究 fallback，0 个混合，0 个需要排查。", html_text)
            self.assertIn("IBKR正常", html_text)
            self.assertIn("交易成本代理", html_text)
            self.assertIn("avg=18.4bps / high=1 / low_liq=0", html_text)
            self.assertIn("score_net", html_text)
            self.assertIn("score_raw", html_text)
            self.assertIn("cost_bps", html_text)
            self.assertIn("风险覆盖", html_text)
            self.assertIn("net=62.0%", html_text)

    def test_dashboard_market_data_health_overview_marks_research_fallback_market(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment_cn"
            report_dir = report_root / "cn_top_quality"
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
                "investment_paper_summary.json",
                "investment_execution_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_data_quality_summary.json").write_text(
                json.dumps(
                    {
                        "avg_data_quality_score": 0.89,
                        "avg_source_coverage": 0.72,
                        "avg_missing_ratio": 0.04,
                        "history_source_counts": {
                            "ibkr": 0,
                            "yfinance": 94,
                            "missing": 0,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "cn"',
                        '    market: "CN"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlists/cn_top_quality.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["market_data_health_overview"][0]["status_label"], "研究Fallback")
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("研究Fallback", html_text)
            self.assertIn("0/94/0", html_text)

    def test_dashboard_market_data_health_overview_marks_nonresearch_fallback_market_as_attention(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            report_root = base / "reports_investment_xetra"
            report_dir = report_root / "xetra_top_quality"
            report_dir.mkdir(parents=True, exist_ok=True)
            for name in (
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
                "investment_paper_summary.json",
                "investment_execution_summary.json",
            ):
                (report_dir / name).write_text("{}", encoding="utf-8")
            (report_dir / "investment_data_quality_summary.json").write_text(
                json.dumps(
                    {
                        "avg_data_quality_score": 0.87,
                        "avg_source_coverage": 0.70,
                        "avg_missing_ratio": 0.05,
                        "history_source_counts": {
                            "ibkr": 0,
                            "yfinance": 42,
                            "missing": 1,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_report.md").write_text(
                "# Investment Candidate Report\n\n## Market Summary\n- 数据提醒: XETRA 的 IBKR 历史行情不可用，当前已回退到 yfinance 免费日线。\n",
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "xetra"',
                        '    market: "XETRA"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlists/xetra_top_quality.yaml"',
                    ]
                ),
                encoding="utf-8",
            )
            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertEqual(payload["market_data_health_overview"][0]["status_label"], "待排查")
            self.assertIn("IBKR 历史行情不可用", payload["market_data_health_overview"][0]["warning_summary"])
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("待排查", html_text)
            self.assertIn("IBKR 历史行情不可用", html_text)

    def test_dashboard_preflight_warn_banner_uses_ib_gateway_wording(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            cfg_path = base / "supervisor.yaml"
            summary_dir = base / "reports_supervisor"
            preflight_dir = base / "reports_preflight"
            report_root = base / "reports_investment"
            watchlist_dir = report_root / "watchlist"
            watchlist_dir.mkdir(parents=True, exist_ok=True)
            preflight_dir.mkdir(parents=True, exist_ok=True)
            summary_dir.mkdir(parents=True, exist_ok=True)
            (preflight_dir / "supervisor_preflight_summary.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-03-30T10:00:00",
                        "pass_count": 6,
                        "warn_count": 1,
                        "fail_count": 0,
                        "checks": [
                            {"name": "ibkr_port:4002", "status": "WARN", "detail": "127.0.0.1:4002 not_listening"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            for name in (
                "investment_paper_summary.json",
                "investment_execution_summary.json",
                "investment_guard_summary.json",
                "investment_opportunity_summary.json",
            ):
                (watchlist_dir / name).write_text("{}", encoding="utf-8")
            (watchlist_dir / "investment_candidates.csv").write_text("symbol,score,action\nAAPL,0.8,HOLD\n", encoding="utf-8")
            (watchlist_dir / "investment_plan.csv").write_text("symbol,action,entry_style,notes\nAAPL,HOLD,HOLD_CORE,test\n", encoding="utf-8")
            cfg_path.write_text(
                "\n".join(
                    [
                        'timezone: "Australia/Sydney"',
                        f'summary_out_dir: "{summary_dir}"',
                        f'dashboard_preflight_dir: "{preflight_dir}"',
                        "poll_sec: 30",
                        "markets:",
                        '  - name: "us"',
                        '    market: "US"',
                        "    enabled: true",
                        "    reports:",
                        '      - kind: "investment"',
                        f'        out_dir: "{report_root}"',
                        '        watchlist_yaml: "config/watchlist.yaml"',
                    ]
                ),
                encoding="utf-8",
            )

            payload = build_dashboard(str(cfg_path), str(summary_dir))
            self.assertIn("IB Gateway", payload["ops_overview"]["preflight_banner_action"])
            write_dashboard(payload, str(summary_dir))
            html_text = (summary_dir / "dashboard.html").read_text(encoding="utf-8")
            self.assertIn("IB Gateway", html_text)


if __name__ == "__main__":
    unittest.main()
