from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.tools.review_investment_weekly import (
    _build_attribution_rows,
    _build_broker_local_diff_rows,
    _build_feedback_threshold_cohort_overview,
    _build_feedback_threshold_trial_alert_overview,
    _build_feedback_threshold_tuning_summary,
    _build_feedback_automation_effect_overview,
    _build_feedback_threshold_effect_overview,
    _build_feedback_threshold_history_overview,
    _build_feedback_threshold_suggestion_rows,
    _build_feedback_effect_market_summary,
    _build_feedback_automation_rows,
    _build_execution_feedback_rows,
    _build_feedback_calibration_rows,
    _build_execution_hotspot_penalties,
    _build_execution_hotspot_rows,
    _build_execution_session_rows,
    _build_execution_gate_rows,
    _build_risk_feedback_rows,
    _build_risk_review_rows,
    _build_broker_summary_rows,
    _build_execution_effect_rows,
    _build_planned_execution_cost_rows,
    _build_execution_summary_rows,
    _build_shadow_feedback_rows,
    _build_holdings_change_rows,
    _build_shadow_review_order_rows,
    _build_shadow_review_summary_rows,
    _build_position_snapshots,
    _persist_feedback_automation_history,
    _persist_feedback_threshold_history,
    _run_source,
    _build_sector_rows,
    _max_drawdown,
)
from src.common.storage import Storage


class ReviewInvestmentWeeklyTests(unittest.TestCase):
    def test_max_drawdown(self):
        self.assertAlmostEqual(_max_drawdown([100.0, 120.0, 90.0, 110.0]), -0.25, places=6)

    def test_position_snapshot_uses_latest_ts_before_cutoff(self):
        rows = [
            {"portfolio_id": "P1", "market": "HK", "symbol": "AAA", "ts": "2026-03-01T00:00:00+00:00", "qty": 1},
            {"portfolio_id": "P1", "market": "HK", "symbol": "AAA", "ts": "2026-03-08T00:00:00+00:00", "qty": 2},
            {"portfolio_id": "P1", "market": "HK", "symbol": "BBB", "ts": "2026-03-08T00:00:00+00:00", "qty": 3},
        ]
        snap = _build_position_snapshots(rows, asof_ts="2026-03-07T00:00:00+00:00", strict_before=True)
        self.assertEqual(len(snap["P1"]), 1)
        self.assertEqual(snap["P1"][0]["symbol"], "AAA")
        self.assertEqual(snap["P1"][0]["qty"], 1)

    def test_holdings_change_rows_classify_symbols(self):
        baseline = {
            "P1": [
                {"portfolio_id": "P1", "market": "HK", "symbol": "AAA", "qty": 10.0, "weight": 0.5},
                {"portfolio_id": "P1", "market": "HK", "symbol": "BBB", "qty": 5.0, "weight": 0.5},
            ]
        }
        latest = {
            "P1": [
                {"portfolio_id": "P1", "market": "HK", "symbol": "AAA", "qty": 15.0, "weight": 0.6},
                {"portfolio_id": "P1", "market": "HK", "symbol": "CCC", "qty": 3.0, "weight": 0.4},
            ]
        }
        rows = _build_holdings_change_rows(latest, baseline)
        by_symbol = {row["symbol"]: row for row in rows}
        self.assertEqual(by_symbol["AAA"]["change_type"], "INCREASED")
        self.assertEqual(by_symbol["BBB"]["change_type"], "REMOVED")
        self.assertEqual(by_symbol["CCC"]["change_type"], "ADDED")

    def test_sector_rows_use_fundamentals_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp)
            (report_dir / "fundamentals.json").write_text(
                json.dumps(
                    {
                        "AAA": {"sector": "Technology"},
                        "BBB": {"sector": "Finance"},
                    }
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_candidates.csv").write_text(
                "symbol,score,action,sector,industry,source\nAAA,0.8,ACCUMULATE,Technology,Software,yfinance\nBBB,0.6,HOLD,Finance,Banks,yfinance\n",
                encoding="utf-8",
            )
            latest = {
                "P1": [
                    {
                        "portfolio_id": "P1",
                        "market": "HK",
                        "symbol": "AAA",
                        "weight": 0.7,
                        "market_value": 70000.0,
                    },
                    {
                        "portfolio_id": "P1",
                        "market": "HK",
                        "symbol": "BBB",
                        "weight": 0.3,
                        "market_value": 30000.0,
                    },
                ]
            }
            runs = {
                "P1": [
                    {
                        "portfolio_id": "P1",
                        "market": "HK",
                        "details": json.dumps({"report_dir": str(report_dir)}),
                    }
                ]
            }
            rows = _build_sector_rows(latest, runs)
            by_sector = {row["sector"]: row for row in rows}
            self.assertAlmostEqual(by_sector["Technology"]["weight"], 0.7, places=6)
            self.assertAlmostEqual(by_sector["Finance"]["weight"], 0.3, places=6)

    def test_execution_summary_rows_use_latest_gap(self):
        rows = _build_execution_summary_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "HK",
                    "ts": "2026-03-10T00:00:00+00:00",
                    "submitted": 0,
                    "details": json.dumps({"summary": {"gap_symbols": 2, "gap_notional": 1000.0}}),
                },
                {
                    "portfolio_id": "P1",
                    "market": "HK",
                    "ts": "2026-03-11T00:00:00+00:00",
                    "submitted": 1,
                    "broker_equity": 100000.0,
                    "broker_cash": 5000.0,
                    "details": json.dumps({"summary": {"gap_symbols": 1, "gap_notional": 250.0}}),
                },
            ],
            [
                {
                    "portfolio_id": "P1",
                    "run_id": "RUN-1",
                    "broker_order_id": 123,
                    "order_value": 1000.0,
                    "status": "FILLED",
                },
                {"portfolio_id": "P1", "broker_order_id": 0, "order_value": 500.0, "status": "ERROR_2139"},
            ],
            [
                {
                    "portfolio_id": "P1",
                    "execution_run_id": "RUN-1",
                    "order_id": 123,
                    "exec_id": "EXEC-1",
                    "symbol": "0005.HK",
                    "qty": 10.0,
                    "price": 50.0,
                    "pnl": 25.0,
                    "actual_slippage_bps": -3.0,
                }
            ],
            [
                {
                    "exec_id": "EXEC-1",
                    "value": 2.5,
                }
            ],
            week_label="2026-W11",
            week_start="2026-03-09",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["week"], "2026-W11")
        self.assertEqual(rows[0]["week_start"], "2026-03-09")
        self.assertEqual(rows[0]["submitted_runs"], 1)
        self.assertEqual(rows[0]["planned_order_rows"], 2)
        self.assertEqual(rows[0]["submitted_order_rows"], 1)
        self.assertEqual(rows[0]["filled_order_rows"], 1)
        self.assertEqual(rows[0]["filled_with_audit_rows"], 1)
        self.assertEqual(rows[0]["fill_rows"], 1)
        self.assertEqual(rows[0]["error_order_rows"], 1)
        self.assertEqual(rows[0]["latest_gap_symbols"], 1)
        self.assertAlmostEqual(rows[0]["latest_gap_notional"], 250.0, places=6)
        self.assertAlmostEqual(rows[0]["commission_total"], 2.5, places=6)
        self.assertAlmostEqual(rows[0]["realized_net_pnl"], 22.5, places=6)
        self.assertAlmostEqual(rows[0]["fill_rate_status"], 1.0, places=6)
        self.assertAlmostEqual(rows[0]["fill_rate_audit"], 1.0, places=6)
        self.assertEqual(rows[0]["error_statuses"], "ERROR_2139")

    def test_broker_summary_rows_include_broker_holdings(self):
        rows = _build_broker_summary_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "HK",
                    "ts": "2026-03-11T00:00:00+00:00",
                    "submitted": 1,
                    "broker_equity": 100000.0,
                    "broker_cash": 5000.0,
                    "details": json.dumps({"summary": {"gap_symbols": 0, "gap_notional": 0.0}}),
                },
            ],
            [
                {"portfolio_id": "P1", "broker_order_id": 123, "order_value": 1000.0, "status": "Submitted"},
            ],
            {
                "P1": [
                    {"portfolio_id": "P1", "market": "HK", "symbol": "AAA", "weight": 0.6, "market_value": 60000.0},
                    {"portfolio_id": "P1", "market": "HK", "symbol": "BBB", "weight": 0.4, "market_value": 40000.0},
                ]
            },
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["broker_holdings_count"], 2)
        self.assertAlmostEqual(rows[0]["broker_holdings_value"], 100000.0, places=6)
        self.assertIn("AAA:0.60", rows[0]["broker_top_holdings"])

    def test_planned_execution_cost_rows_use_submitted_basis_and_submitted_styles(self):
        rows = _build_planned_execution_cost_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "status": "SUBMITTED",
                    "order_value": 1000.0,
                    "details": json.dumps(
                        {
                            "expected_cost_bps": 18.0,
                            "expected_spread_cost": 0.4,
                            "expected_slippage_cost": 1.2,
                            "expected_commission_cost": 0.2,
                            "expected_cost_value": 1.8,
                            "execution_style": "VWAP_LITE_MIDDAY",
                        }
                    ),
                },
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "status": "PLANNED",
                    "order_value": 600.0,
                    "details": json.dumps(
                        {
                            "expected_cost_bps": 24.0,
                            "expected_spread_cost": 0.5,
                            "expected_slippage_cost": 0.8,
                            "expected_commission_cost": 0.1,
                            "expected_cost_value": 1.44,
                            "execution_style": "TWAP_LITE_OPEN",
                        }
                    ),
                },
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "status": "BLOCKED_LIQUIDITY",
                    "order_value": 900.0,
                    "details": json.dumps({"expected_cost_bps": 99.0, "execution_style": "VWAP_LITE_CLOSE"}),
                },
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["planned_cost_basis"], "submitted_orders")
        self.assertEqual(int(rows[0]["planned_order_rows"]), 1)
        self.assertAlmostEqual(float(rows[0]["planned_order_value"]), 1000.0, places=6)
        self.assertAlmostEqual(float(rows[0]["planned_execution_cost_total"]), 1.8, places=6)
        self.assertAlmostEqual(float(rows[0]["avg_expected_cost_bps"]), 18.0, places=6)
        self.assertEqual(rows[0]["execution_style_breakdown"], "VWAP_LITE_MIDDAY:1")

    def test_execution_feedback_rows_tighten_when_actual_cost_exceeds_plan(self):
        rows = _build_execution_feedback_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "planned_execution_cost_total": 20.0,
                    "execution_cost_total": 38.0,
                    "execution_cost_gap": 18.0,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 31.0,
                    "execution_style_breakdown": "VWAP_LITE_MIDDAY:3",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "submitted_order_rows": 3,
                    "error_order_rows": 1,
                    "latest_gap_symbols": 0,
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["execution_feedback_action"], "TIGHTEN")
        self.assertLess(float(rows[0]["execution_adv_max_participation_pct_delta"]), 0.0)
        self.assertLess(float(rows[0]["execution_adv_split_trigger_pct_delta"]), 0.0)
        self.assertGreater(int(rows[0]["execution_max_slices_per_symbol_delta"]), 0)
        self.assertGreater(float(rows[0]["feedback_confidence"]), 0.0)
        self.assertIn(str(rows[0]["feedback_confidence_label"]), {"LOW", "MEDIUM", "HIGH"})

    def test_execution_feedback_rows_hold_when_gate_blocking_is_primary_driver(self):
        rows = _build_execution_feedback_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "planned_execution_cost_total": 20.0,
                    "execution_cost_total": 20.0,
                    "execution_cost_gap": 0.0,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 18.0,
                    "execution_style_breakdown": "VWAP_LITE_MIDDAY:2",
                    "strategy_control_weight_delta": 0.0,
                    "risk_overlay_weight_delta": 0.01,
                    "execution_gate_blocked_weight": 0.08,
                    "execution_gate_blocked_order_ratio": 0.60,
                    "execution_gate_blocked_order_value": 8000.0,
                    "control_split_text": "策略 0.0% | 风险 1.0% | 执行 8.0%（blocked 8000.00 / 60%）",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "submitted_order_rows": 4,
                    "error_order_rows": 0,
                    "latest_gap_symbols": 0,
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["execution_feedback_action"], "HOLD")
        self.assertEqual(float(row["execution_adv_max_participation_pct_delta"]), 0.0)
        self.assertEqual(float(row["execution_adv_split_trigger_pct_delta"]), 0.0)
        self.assertEqual(int(row["execution_max_slices_per_symbol_delta"]), 0)
        self.assertEqual(str(row["feedback_control_driver"]), "EXECUTION")
        self.assertIn("执行 gate 阻断", str(row["feedback_reason"]))

    def test_feedback_calibration_rows_build_support_scores_from_recent_outcomes(self):
        outcome_rows = []
        for idx, future_return in enumerate((0.08, 0.11, 0.06, 0.09, 0.12, 0.05), start=1):
            outcome_rows.append(
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "symbol": f"SYM{idx}",
                    "horizon_days": 20,
                    "outcome_ts": f"2026-03-{10+idx:02d}T00:00:00+00:00",
                    "future_return": future_return,
                    "max_drawdown": -0.04,
                    "outcome_label": "POSITIVE",
                    "details": json.dumps(
                        {
                            "stage": "final",
                            "action": "ACCUMULATE",
                            "model_recommendation_score": 0.40 + idx * 0.05,
                            "execution_score": 0.55 + idx * 0.03,
                        }
                    ),
                }
            )
        rows = _build_feedback_calibration_rows(outcome_rows)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["selection_scope"]), "FINAL_ACTIONABLE")
        self.assertEqual(str(row["selected_horizon_days"]), "20")
        self.assertEqual(int(row["outcome_sample_count"]), 6)
        self.assertGreater(float(row["signal_quality_score"]), 0.5)
        self.assertGreater(float(row["shadow_threshold_relax_support"]), float(row["shadow_weak_signal_support"]))
        self.assertGreater(float(row["execution_support"]), 0.5)

    def test_execution_feedback_rows_apply_outcome_calibration_to_final_confidence(self):
        rows = _build_execution_feedback_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "planned_execution_cost_total": 20.0,
                    "execution_cost_total": 38.0,
                    "execution_cost_gap": 18.0,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 31.0,
                    "execution_style_breakdown": "VWAP_LITE_MIDDAY:3",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "submitted_order_rows": 3,
                    "error_order_rows": 1,
                    "latest_gap_symbols": 0,
                }
            ],
            feedback_calibration_map={
                "P1": {
                    "execution_support": 0.20,
                    "outcome_sample_count": 12,
                    "selected_horizon_days": "20",
                    "selection_scope_label": "final 可执行候选",
                    "calibration_reason": "近期 outcome 本身偏弱。",
                }
            },
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertLess(float(row["feedback_confidence"]), float(row["feedback_base_confidence"]))
        self.assertAlmostEqual(float(row["feedback_calibration_score"]), 0.20, places=6)
        self.assertEqual(str(row["feedback_calibration_horizon_days"]), "20")
        self.assertEqual(int(row["feedback_calibration_sample_count"]), 12)

    def test_feedback_automation_rows_auto_apply_when_outcome_support_is_strong(self):
        rows = _build_feedback_automation_rows(
            [],
            [],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "execution_feedback_action": "TIGHTEN",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 4,
                    "feedback_base_confidence": 0.82,
                    "feedback_base_confidence_label": "HIGH",
                    "feedback_calibration_score": 0.67,
                    "feedback_calibration_label": "MEDIUM",
                    "feedback_calibration_sample_count": 18,
                    "feedback_confidence": 0.74,
                    "feedback_confidence_label": "MEDIUM",
                    "feedback_reason": "actual execution cost above plan",
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["feedback_kind"]), "execution")
        self.assertEqual(str(row["calibration_apply_mode"]), "AUTO_APPLY")
        self.assertEqual(int(row["paper_auto_apply_enabled"]), 1)
        self.assertEqual(int(row["live_confirmation_required"]), 1)
        self.assertEqual(str(row["calibration_basis"]), "OUTCOME_CALIBRATED")

    def test_feedback_automation_rows_suggest_only_when_outcome_not_ready(self):
        rows = _build_feedback_automation_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "shadow_review_action": "REVIEW_THRESHOLD",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 2,
                    "feedback_base_confidence": 0.52,
                    "feedback_base_confidence_label": "MEDIUM",
                    "feedback_calibration_score": 0.50,
                    "feedback_calibration_label": "MEDIUM",
                    "feedback_calibration_sample_count": 0,
                    "feedback_confidence": 0.52,
                    "feedback_confidence_label": "MEDIUM",
                    "feedback_reason": "threshold review",
                }
            ],
            [],
            [],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["feedback_kind"]), "shadow")
        self.assertEqual(str(row["calibration_apply_mode"]), "SUGGEST_ONLY")
        self.assertEqual(int(row["paper_auto_apply_enabled"]), 0)
        self.assertEqual(str(row["calibration_basis"]), "BASE_WEEKLY")

    def test_feedback_automation_rows_hold_when_confidence_is_too_weak(self):
        rows = _build_feedback_automation_rows(
            [],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "risk_feedback_action": "TIGHTEN",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 1,
                    "feedback_base_confidence": 0.18,
                    "feedback_base_confidence_label": "LOW",
                    "feedback_calibration_score": 0.33,
                    "feedback_calibration_label": "LOW",
                    "feedback_calibration_sample_count": 2,
                    "feedback_confidence": 0.14,
                    "feedback_confidence_label": "LOW",
                    "feedback_reason": "risk tighten",
                }
            ],
            [],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["feedback_kind"]), "risk")
        self.assertEqual(str(row["calibration_apply_mode"]), "HOLD")
        self.assertEqual(int(row["paper_auto_apply_enabled"]), 0)

    def test_feedback_automation_rows_downgrade_auto_apply_when_outcomes_are_still_maturing(self):
        rows = _build_feedback_automation_rows(
            [],
            [],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "execution_feedback_action": "TIGHTEN",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 4,
                    "feedback_base_confidence": 0.84,
                    "feedback_base_confidence_label": "HIGH",
                    "feedback_calibration_score": 0.72,
                    "feedback_calibration_label": "HIGH",
                    "feedback_calibration_sample_count": 18,
                    "feedback_calibration_horizon_days": "5",
                    "feedback_confidence": 0.78,
                    "feedback_confidence_label": "HIGH",
                    "feedback_reason": "actual execution cost above plan",
                }
            ],
            labeling_skip_rows=[
                {
                    "market": "US",
                    "portfolio_id": "P1",
                    "horizon_days": 5,
                    "skip_reason": "INSUFFICIENT_FORWARD_BARS",
                    "skip_count": 30,
                    "min_remaining_forward_bars": 1,
                    "max_remaining_forward_bars": 3,
                    "estimated_ready_start_ts": "2026-03-25T00:00:00+00:00",
                    "estimated_ready_end_ts": "2026-03-27T00:00:00+00:00",
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["calibration_apply_mode"]), "SUGGEST_ONLY")
        self.assertEqual(str(row["outcome_maturity_label"]), "BUILDING")
        self.assertEqual(int(row["outcome_pending_sample_count"]), 30)
        self.assertIn("待成熟", str(row["automation_reason"]))

    def test_feedback_automation_rows_apply_market_threshold_override(self):
        rows = _build_feedback_automation_rows(
            [],
            [],
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "execution_feedback_action": "TIGHTEN",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 3,
                    "feedback_base_confidence": 0.80,
                    "feedback_base_confidence_label": "HIGH",
                    "feedback_calibration_score": 0.60,
                    "feedback_calibration_label": "MEDIUM",
                    "feedback_calibration_sample_count": 18,
                    "feedback_calibration_horizon_days": "5",
                    "feedback_confidence": 0.58,
                    "feedback_confidence_label": "MEDIUM",
                    "feedback_reason": "actual execution cost above plan",
                }
            ],
            threshold_overrides={
                "US": {
                    "execution": {
                        "auto_confidence": 0.57,
                        "auto_base_confidence": 0.76,
                        "auto_calibration_score": 0.56,
                        "auto_maturity_ratio": 0.60,
                    }
                }
            },
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["calibration_apply_mode"]), "AUTO_APPLY")
        self.assertIn('"auto_confidence": 0.57', str(row["auto_threshold_snapshot_json"]))

    def test_feedback_automation_rows_downgrade_auto_apply_when_market_data_needs_attention(self):
        rows = _build_feedback_automation_rows(
            [],
            [],
            [
                {
                    "portfolio_id": "XETRA:xetra_top_quality",
                    "market": "XETRA",
                    "execution_feedback_action": "TIGHTEN",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 4,
                    "feedback_base_confidence": 0.84,
                    "feedback_base_confidence_label": "HIGH",
                    "feedback_calibration_score": 0.72,
                    "feedback_calibration_label": "HIGH",
                    "feedback_calibration_sample_count": 18,
                    "feedback_confidence": 0.78,
                    "feedback_confidence_label": "HIGH",
                    "feedback_reason": "actual execution cost above plan",
                }
            ],
            market_data_gate_map={
                "XETRA:xetra_top_quality": {
                    "status_code": "ATTENTION",
                    "status_label": "待排查",
                    "reason": "IBKR 历史权限待补，当前不适合直接自动放大 weekly feedback。",
                    "probe_status_label": "权限待补",
                }
            },
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["calibration_apply_mode"]), "SUGGEST_ONLY")
        self.assertEqual(str(row["calibration_basis"]), "DATA_HEALTH_GATED")
        self.assertEqual(str(row["market_data_gate_label"]), "待排查")
        self.assertIn("不适合直接自动放大", str(row["automation_reason"]))

    def test_feedback_automation_rows_keep_research_fallback_as_suggest_only(self):
        rows = _build_feedback_automation_rows(
            [
                {
                    "portfolio_id": "CN:cn_top_quality",
                    "market": "CN",
                    "shadow_review_action": "REVIEW_THRESHOLD",
                    "feedback_scope": "paper_only",
                    "feedback_sample_count": 3,
                    "feedback_base_confidence": 0.74,
                    "feedback_base_confidence_label": "HIGH",
                    "feedback_calibration_score": 0.65,
                    "feedback_calibration_label": "MEDIUM",
                    "feedback_calibration_sample_count": 16,
                    "feedback_confidence": 0.71,
                    "feedback_confidence_label": "HIGH",
                    "feedback_reason": "threshold review",
                }
            ],
            [],
            [],
            market_data_gate_map={
                "CN:cn_top_quality": {
                    "status_code": "RESEARCH_FALLBACK",
                    "status_label": "研究Fallback",
                    "reason": "当前市场配置为 research-only fallback，周报反馈更适合作为研究建议，不直接自动应用。",
                }
            },
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["calibration_apply_mode"]), "SUGGEST_ONLY")
        self.assertEqual(str(row["calibration_basis"]), "DATA_HEALTH_GATED")
        self.assertEqual(str(row["market_data_gate_label"]), "研究Fallback")
        self.assertIn("研究建议", str(row["automation_reason"]))

    def test_persist_feedback_automation_history_writes_recent_week_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            rows = _build_feedback_automation_rows(
                [],
                [],
                [
                    {
                        "portfolio_id": "US:watchlist",
                        "market": "US",
                        "execution_feedback_action": "TIGHTEN",
                        "feedback_scope": "paper_only",
                        "feedback_sample_count": 4,
                        "feedback_base_confidence": 0.82,
                        "feedback_base_confidence_label": "HIGH",
                        "feedback_calibration_score": 0.67,
                        "feedback_calibration_label": "MEDIUM",
                        "feedback_calibration_sample_count": 18,
                        "feedback_calibration_horizon_days": "5",
                        "feedback_confidence": 0.74,
                        "feedback_confidence_label": "MEDIUM",
                        "feedback_reason": "actual execution cost above plan",
                    }
                ],
                labeling_skip_rows=[
                    {
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "horizon_days": 5,
                        "skip_reason": "INSUFFICIENT_FORWARD_BARS",
                        "skip_count": 30,
                        "min_remaining_forward_bars": 1,
                        "max_remaining_forward_bars": 3,
                        "estimated_ready_start_ts": "2026-03-25T00:00:00+00:00",
                        "estimated_ready_end_ts": "2026-03-27T00:00:00+00:00",
                    }
                ],
            )
            _persist_feedback_automation_history(
                db_path,
                rows,
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
                execution_feedback_map={
                    "US:watchlist": {
                        "planned_execution_cost_total": 14.0,
                        "execution_cost_total": 21.5,
                        "execution_cost_gap": 7.5,
                        "avg_expected_cost_bps": 18.0,
                        "avg_actual_slippage_bps": 28.0,
                        "dominant_execution_session_label": "开盘",
                        "execution_feedback_action": "TIGHTEN",
                    }
                },
            )
            history_rows = Storage(str(db_path)).get_recent_investment_feedback_automation_history(
                "US",
                portfolio_id="US:watchlist",
                feedback_kind="execution",
                limit=5,
            )
            self.assertEqual(len(history_rows), 1)
            self.assertEqual(str(history_rows[0]["week_label"]), "2026-W13")
            self.assertEqual(str(history_rows[0]["calibration_apply_mode"]), "SUGGEST_ONLY")
            self.assertEqual(str(history_rows[0]["outcome_maturity_label"]), "BUILDING")
            self.assertEqual(int(history_rows[0]["outcome_pending_sample_count"]), 30)
            effect_snapshot = dict(history_rows[0]["details_json"].get("effect_snapshot", {}) or {})
            self.assertEqual(str(effect_snapshot.get("snapshot_kind")), "execution")
            self.assertAlmostEqual(float(effect_snapshot.get("execution_cost_gap", 0.0) or 0.0), 7.5, places=6)
            self.assertEqual(str(history_rows[0]["details_json"].get("effect_snapshot_week_label") or ""), "2026-W13")

    def test_feedback_effect_market_summary_marks_execution_market_as_improving(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
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
                    "feedback_base_confidence": 0.72,
                    "feedback_calibration_score": 0.58,
                    "feedback_confidence": 0.62,
                    "feedback_sample_count": 2,
                    "feedback_calibration_sample_count": 11,
                    "outcome_maturity_ratio": 0.45,
                    "outcome_maturity_label": "BUILDING",
                    "outcome_pending_sample_count": 8,
                    "alert_bucket": "SOON",
                    "details": {
                        "automation_reason": "前一周仍在观察。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "execution_cost_gap": 18.0,
                            "avg_actual_slippage_bps": 32.0,
                        },
                    },
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
                    "feedback_base_confidence": 0.81,
                    "feedback_calibration_score": 0.71,
                    "feedback_confidence": 0.77,
                    "feedback_sample_count": 4,
                    "feedback_calibration_sample_count": 18,
                    "outcome_maturity_ratio": 0.78,
                    "outcome_maturity_label": "READY",
                    "outcome_pending_sample_count": 0,
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数开始自动应用。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "execution_cost_gap": 16.0,
                            "avg_actual_slippage_bps": 30.0,
                        },
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
                    "feedback_base_confidence": 0.83,
                    "feedback_calibration_score": 0.74,
                    "feedback_confidence": 0.79,
                    "feedback_sample_count": 5,
                    "feedback_calibration_sample_count": 22,
                    "outcome_maturity_ratio": 0.84,
                    "outcome_maturity_label": "ACTIVE",
                    "outcome_pending_sample_count": 0,
                    "alert_bucket": "ACTIVE",
                    "details": {
                        "automation_reason": "执行参数自动应用后成本改善。",
                        "effect_snapshot": {
                            "snapshot_kind": "execution",
                            "execution_cost_gap": 8.0,
                            "avg_actual_slippage_bps": 20.0,
                        },
                    },
                }
            )
            overview_rows = _build_feedback_automation_effect_overview(
                db_path,
                [
                    {
                        "market": "US",
                        "portfolio_id": "US:watchlist",
                        "feedback_kind": "execution",
                    }
                ],
            )
            self.assertEqual(len(overview_rows), 1)
            self.assertEqual(str(overview_rows[0]["baseline_week"]), "2026-W12")
            self.assertEqual(int(overview_rows[0]["active_weeks"]), 2)
            self.assertIn("改善", str(overview_rows[0]["effect_w1"]))

            summary_rows = _build_feedback_effect_market_summary(overview_rows)
            self.assertEqual(len(summary_rows), 1)
            self.assertEqual(str(summary_rows[0]["market"]), "US")
            self.assertEqual(str(summary_rows[0]["feedback_kind_label"]), "执行参数")
            self.assertEqual(str(summary_rows[0]["summary_signal"]), "持续改善")
            self.assertEqual(int(summary_rows[0]["tracked_count"]), 1)
            self.assertEqual(int(summary_rows[0]["w1_improved_count"]), 1)

            suggestion_rows = _build_feedback_threshold_suggestion_rows(summary_rows)
            self.assertEqual(len(suggestion_rows), 1)
            self.assertEqual(str(suggestion_rows[0]["suggestion_action"]), "RELAX_AUTO_APPLY")
            self.assertLess(
                float(suggestion_rows[0]["suggested_auto_confidence"]),
                float(suggestion_rows[0]["base_auto_confidence"]),
            )

    def test_feedback_threshold_history_overview_tracks_relax_trend(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            _persist_feedback_threshold_history(
                db_path,
                [
                    {
                        "market": "US",
                        "feedback_kind": "execution",
                        "feedback_kind_label": "执行参数",
                        "suggestion_action": "KEEP_BASE",
                        "suggestion_label": "维持基线",
                        "summary_signal": "观察中",
                        "tracked_count": 1,
                        "avg_active_weeks": 1.0,
                        "base_auto_confidence": 0.60,
                        "suggested_auto_confidence": 0.60,
                        "base_auto_base_confidence": 0.76,
                        "suggested_auto_base_confidence": 0.76,
                        "base_auto_calibration_score": 0.56,
                        "suggested_auto_calibration_score": 0.56,
                        "base_auto_maturity_ratio": 0.60,
                        "suggested_auto_maturity_ratio": 0.60,
                        "reason": "样本还不够。",
                    }
                ],
                week_label="2026-W12",
                week_start="2026-03-16",
                window_start="2026-03-10T00:00:00+00:00",
                window_end="2026-03-17T00:00:00+00:00",
            )
            _persist_feedback_threshold_history(
                db_path,
                [
                    {
                        "market": "US",
                        "feedback_kind": "execution",
                        "feedback_kind_label": "执行参数",
                        "suggestion_action": "RELAX_AUTO_APPLY",
                        "suggestion_label": "可适度放宽",
                        "summary_signal": "持续改善",
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
                        "reason": "自动应用后已出现连续改善。",
                    }
                ],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_feedback_threshold_history(
                db_path,
                [
                    {
                        "market": "US",
                        "feedback_kind": "execution",
                        "feedback_kind_label": "执行参数",
                        "suggestion_action": "RELAX_AUTO_APPLY",
                        "suggestion_label": "可适度放宽",
                        "summary_signal": "持续改善",
                        "tracked_count": 2,
                        "avg_active_weeks": 3.0,
                        "base_auto_confidence": 0.57,
                        "suggested_auto_confidence": 0.54,
                        "base_auto_base_confidence": 0.73,
                        "suggested_auto_base_confidence": 0.70,
                        "base_auto_calibration_score": 0.54,
                        "suggested_auto_calibration_score": 0.52,
                        "base_auto_maturity_ratio": 0.55,
                        "suggested_auto_maturity_ratio": 0.50,
                        "reason": "连续第二周改善。",
                    }
                ],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )
            rows = _build_feedback_threshold_history_overview(
                db_path,
                [{"market": "US", "feedback_kind": "execution"}],
            )
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["market"], "US")
            self.assertEqual(row["feedback_kind_label"], "执行参数")
            self.assertEqual(row["current_action"], "RELAX_AUTO_APPLY")
            self.assertEqual(row["trend_bucket"], "连续放宽")
            self.assertEqual(row["transition"], "持续试运行")
            self.assertEqual(int(row["same_action_weeks"]), 2)
            self.assertEqual(int(row["weeks_tracked"]), 3)
            self.assertIn("2026-W13:RELAX_AUTO_APPLY", str(row["action_chain"]))

            effect_rows = _build_feedback_threshold_effect_overview(
                db_path,
                [{"market": "US", "feedback_kind": "execution"}],
            )
            self.assertEqual(len(effect_rows), 1)
            effect_row = effect_rows[0]
            self.assertEqual(effect_row["effect_label"], "放宽后改善")
            self.assertEqual(effect_row["summary_signal"], "持续改善")
            self.assertEqual(int(effect_row["same_action_weeks"]), 2)
            self.assertIn("自动应用效果仍在继续改善", str(effect_row["effect_reason"]))

            cohort_rows = _build_feedback_threshold_cohort_overview(
                db_path,
                [{"market": "US", "feedback_kind": "execution"}],
            )
            self.assertEqual(len(cohort_rows), 1)
            cohort_row = cohort_rows[0]
            self.assertEqual(cohort_row["cohort_label"], "可适度放宽")
            self.assertEqual(cohort_row["baseline_week"], "2026-W13")
            self.assertEqual(int(cohort_row["cohort_weeks"]), 2)
            self.assertEqual(cohort_row["latest_effect"], "放宽后改善")
            self.assertEqual(cohort_row["effect_w1"], "放宽后改善")
            self.assertEqual(cohort_row["effect_w2"], "-")
            self.assertIn("继续试运行", str(cohort_row["diagnosis"]))

            alert_rows = _build_feedback_threshold_trial_alert_overview(cohort_rows)
            self.assertEqual(len(alert_rows), 1)
            alert_row = alert_rows[0]
            self.assertEqual(alert_row["stage_label"], "持续观察期")
            self.assertEqual(alert_row["action_label"], "放宽试运行")
            self.assertEqual(alert_row["latest_effect"], "放宽后改善")
            self.assertIn("优先确认是否恶化", str(alert_row["next_check"]))

            tuning_rows = _build_feedback_threshold_tuning_summary(cohort_rows)
            self.assertEqual(len(tuning_rows), 1)
            tuning_row = tuning_rows[0]
            self.assertEqual(tuning_row["suggestion_action"], "KEEP_RELAX")
            self.assertEqual(tuning_row["suggestion_label"], "继续放宽试运行")
            self.assertIn("连续出现改善", str(tuning_row["reason"]))

    def test_execution_session_rows_and_feedback_track_open_session_hotspot(self):
        execution_orders = [
            {
                "portfolio_id": "P1",
                "market": "US",
                "symbol": "AAPL",
                "broker_order_id": 101,
                "status": "SUBMITTED",
                "order_value": 5000.0,
                "details": json.dumps(
                    {
                        "session_bucket": "OPEN",
                        "session_label": "开盘",
                        "execution_style": "TWAP_LITE_OPEN",
                        "expected_cost_bps": 18.0,
                        "expected_spread_cost": 2.0,
                        "expected_slippage_cost": 5.0,
                        "expected_commission_cost": 1.0,
                        "expected_cost_value": 8.0,
                    }
                ),
            },
            {
                "portfolio_id": "P1",
                "market": "US",
                "symbol": "MSFT",
                "broker_order_id": 102,
                "status": "SUBMITTED",
                "order_value": 4000.0,
                "details": json.dumps(
                    {
                        "session_bucket": "MIDDAY",
                        "session_label": "午盘",
                        "execution_style": "VWAP_LITE_MIDDAY",
                        "expected_cost_bps": 18.0,
                        "expected_spread_cost": 1.0,
                        "expected_slippage_cost": 4.0,
                        "expected_commission_cost": 1.0,
                        "expected_cost_value": 6.0,
                    }
                ),
            },
        ]
        fill_rows = [
            {
                "order_id": 101,
                "exec_id": "exec-open",
                "portfolio_id": "P1",
                "symbol": "AAPL",
                "qty": 50.0,
                "price": 100.0,
                "actual_slippage_bps": 31.0,
                "slippage_bps_deviation": 12.0,
            },
            {
                "order_id": 102,
                "exec_id": "exec-mid",
                "portfolio_id": "P1",
                "symbol": "MSFT",
                "qty": 20.0,
                "price": 200.0,
                "actual_slippage_bps": 9.0,
                "slippage_bps_deviation": -7.0,
            },
        ]
        commission_rows = [
            {"exec_id": "exec-open", "value": 1.5},
            {"exec_id": "exec-mid", "value": 1.0},
        ]
        session_rows = _build_execution_session_rows(execution_orders, fill_rows, commission_rows)
        by_session = {str(row["session_bucket"]): row for row in session_rows}
        self.assertAlmostEqual(float(by_session["OPEN"]["planned_execution_cost_total"]), 8.0, places=6)
        self.assertGreater(float(by_session["OPEN"]["execution_cost_gap"]), 8.0)
        self.assertLess(float(by_session["MIDDAY"]["execution_cost_gap"]), 0.0)

        feedback_rows = _build_execution_feedback_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "planned_execution_cost_total": 14.0,
                    "execution_cost_total": 21.0,
                    "execution_cost_gap": 7.0,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 22.0,
                    "execution_style_breakdown": "TWAP_LITE_OPEN:1,VWAP_LITE_MIDDAY:1",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "submitted_order_rows": 2,
                    "error_order_rows": 0,
                    "latest_gap_symbols": 0,
                }
            ],
            execution_session_rows=session_rows,
        )
        self.assertEqual(feedback_rows[0]["execution_feedback_action"], "TIGHTEN")
        self.assertLess(float(feedback_rows[0]["execution_open_session_participation_scale_delta"]), 0.0)
        self.assertEqual(float(feedback_rows[0]["execution_midday_session_participation_scale_delta"]), 0.0)
        self.assertEqual(str(feedback_rows[0]["dominant_execution_session_bucket"]), "OPEN")
        self.assertIn("开盘", str(feedback_rows[0]["execution_session_feedback_json"]))

    def test_execution_hotspot_rows_rank_symbol_session_pressure(self):
        execution_orders = [
            {
                "portfolio_id": "P1",
                "market": "US",
                "symbol": "AAPL",
                "broker_order_id": 201,
                "status": "SUBMITTED",
                "order_value": 6000.0,
                "details": json.dumps(
                    {
                        "session_bucket": "OPEN",
                        "session_label": "开盘",
                        "execution_style": "TWAP_LITE_OPEN",
                        "expected_cost_bps": 18.0,
                        "expected_cost_value": 10.8,
                    }
                ),
            },
            {
                "portfolio_id": "P1",
                "market": "US",
                "symbol": "MSFT",
                "broker_order_id": 202,
                "status": "SUBMITTED",
                "order_value": 5000.0,
                "details": json.dumps(
                    {
                        "session_bucket": "MIDDAY",
                        "session_label": "午盘",
                        "execution_style": "VWAP_LITE_MIDDAY",
                        "expected_cost_bps": 18.0,
                        "expected_cost_value": 9.0,
                    }
                ),
            },
        ]
        fill_rows = [
            {
                "order_id": 201,
                "exec_id": "exec-aapl",
                "portfolio_id": "P1",
                "symbol": "AAPL",
                "qty": 60.0,
                "price": 100.0,
                "actual_slippage_bps": 34.0,
                "slippage_bps_deviation": 16.0,
            },
            {
                "order_id": 202,
                "exec_id": "exec-msft",
                "portfolio_id": "P1",
                "symbol": "MSFT",
                "qty": 25.0,
                "price": 200.0,
                "actual_slippage_bps": 16.0,
                "slippage_bps_deviation": -2.0,
            },
        ]
        commission_rows = [
            {"exec_id": "exec-aapl", "value": 1.8},
            {"exec_id": "exec-msft", "value": 1.0},
        ]
        rows = _build_execution_hotspot_rows(execution_orders, fill_rows, commission_rows)
        self.assertEqual(rows[0]["symbol"], "AAPL")
        self.assertEqual(rows[0]["session_bucket"], "OPEN")
        self.assertEqual(rows[0]["hotspot_action"], "INVESTIGATE_EXECUTION")
        self.assertGreater(float(rows[0]["pressure_score"]), float(rows[1]["pressure_score"]))

    def test_execution_hotspot_penalties_promote_symbol_level_cost_guard(self):
        penalties = _build_execution_hotspot_penalties(
            [
                {
                    "symbol": "AAPL",
                    "session_label": "开盘",
                    "hotspot_action": "INVESTIGATE_EXECUTION",
                    "pressure_score": 2.2,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 35.0,
                },
                {
                    "symbol": "AAPL",
                    "session_label": "尾盘",
                    "hotspot_action": "INVESTIGATE_EXECUTION",
                    "pressure_score": 1.6,
                    "avg_expected_cost_bps": 20.0,
                    "avg_actual_slippage_bps": 31.0,
                },
                {
                    "symbol": "MSFT",
                    "session_label": "午盘",
                    "hotspot_action": "OBSERVE",
                    "pressure_score": 0.2,
                    "avg_expected_cost_bps": 18.0,
                    "avg_actual_slippage_bps": 19.0,
                },
            ]
        )
        self.assertEqual(len(penalties), 1)
        self.assertEqual(penalties[0]["symbol"], "AAPL")
        self.assertGreater(float(penalties[0]["expected_cost_bps_add"]), 0.0)
        self.assertGreater(float(penalties[0]["execution_penalty"]), 0.0)
        self.assertIn("开盘", str(penalties[0]["session_labels"]))

    def test_broker_local_diff_rows_show_symbol_mismatch(self):
        rows = _build_broker_local_diff_rows(
            {
                "P1": [
                    {"portfolio_id": "P1", "market": "HK", "symbol": "AAA"},
                    {"portfolio_id": "P1", "market": "HK", "symbol": "BBB"},
                ]
            },
            {
                "P1": [
                    {"portfolio_id": "P1", "market": "HK", "symbol": "BBB"},
                    {"portfolio_id": "P1", "market": "HK", "symbol": "CCC"},
                ]
            },
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["local_only_count"], 1)
        self.assertEqual(rows[0]["broker_only_count"], 1)
        self.assertEqual(rows[0]["local_only_symbols"], "AAA")
        self.assertEqual(rows[0]["broker_only_symbols"], "CCC")

    def test_run_source_reads_broker_sync_marker(self):
        self.assertEqual(_run_source({"details": json.dumps({"source": "broker_sync"})}), "broker_sync")
        self.assertEqual(_run_source({"details": json.dumps({"source": "manual"})}), "manual")
        self.assertEqual(_run_source({"details": ""}), "")

    def test_shadow_review_summary_flags_threshold_review_for_near_miss_blocks(self):
        rows = _build_shadow_review_order_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "ts": "2026-03-18T10:00:00+00:00",
                    "symbol": "AAPL",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5000.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.470 prob=0.540 samples=150; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.470 prob=0.540 samples=150",
                        }
                    ),
                },
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "ts": "2026-03-18T11:00:00+00:00",
                    "symbol": "MSFT",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 4200.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.490 prob=0.570 samples=180; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.490 prob=0.570 samples=180",
                        }
                    ),
                },
            ]
        )
        summary = _build_shadow_review_summary_rows(rows)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["shadow_review_action"], "REVIEW_THRESHOLD")
        self.assertEqual(summary[0]["near_miss_count"], 2)
        self.assertAlmostEqual(summary[0]["avg_shadow_score"], 0.48, places=6)

    def test_shadow_review_summary_flags_weak_signal_for_repeat_far_below_blocks(self):
        rows = _build_shadow_review_order_rows(
            [
                {
                    "portfolio_id": "P2",
                    "market": "HK",
                    "ts": "2026-03-18T10:00:00+00:00",
                    "symbol": "0700.HK",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5000.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.180 prob=0.220 samples=220; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.180 prob=0.220 samples=220",
                        }
                    ),
                },
                {
                    "portfolio_id": "P2",
                    "market": "HK",
                    "ts": "2026-03-18T11:00:00+00:00",
                    "symbol": "0700.HK",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5200.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.150 prob=0.250 samples=220; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.150 prob=0.250 samples=220",
                        }
                    ),
                },
            ]
        )
        summary = _build_shadow_review_summary_rows(rows)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["shadow_review_action"], "WEAK_SIGNAL")
        self.assertEqual(summary[0]["repeated_symbol_count"], 1)
        self.assertEqual(summary[0]["repeated_symbols"], "0700.HK")
        self.assertGreaterEqual(summary[0]["far_below_count"], 2)

    def test_shadow_feedback_rows_lower_execution_thresholds_for_near_miss(self):
        rows = _build_shadow_review_order_rows(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "ts": "2026-03-18T10:00:00+00:00",
                    "symbol": "AAPL",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5000.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.470 prob=0.540 samples=150; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.470 prob=0.540 samples=150",
                        }
                    ),
                },
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "ts": "2026-03-18T11:00:00+00:00",
                    "symbol": "MSFT",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 4200.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.490 prob=0.570 samples=180; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.490 prob=0.570 samples=180",
                        }
                    ),
                },
            ]
        )
        summary = _build_shadow_review_summary_rows(rows)
        feedback = _build_shadow_feedback_rows(rows, summary)
        self.assertEqual(len(feedback), 1)
        self.assertEqual(feedback[0]["shadow_review_action"], "REVIEW_THRESHOLD")
        self.assertLess(float(feedback[0]["execution_shadow_score_delta"]), 0.0)
        self.assertLess(float(feedback[0]["execution_shadow_prob_delta"]), 0.0)
        self.assertEqual(int(feedback[0]["signal_penalty_symbol_count"]), 0)

    def test_shadow_feedback_rows_raise_signal_selectivity_for_weak_signal(self):
        rows = _build_shadow_review_order_rows(
            [
                {
                    "portfolio_id": "P2",
                    "market": "HK",
                    "ts": "2026-03-18T10:00:00+00:00",
                    "symbol": "0700.HK",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5000.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.180 prob=0.220 samples=220; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.180 prob=0.220 samples=220",
                        }
                    ),
                },
                {
                    "portfolio_id": "P2",
                    "market": "HK",
                    "ts": "2026-03-18T11:00:00+00:00",
                    "symbol": "0700.HK",
                    "action": "BUY",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 5200.0,
                    "reason": "signal|shadow_ml_review",
                    "details": json.dumps(
                        {
                            "shadow_review_status": "REVIEW_REQUIRED",
                            "shadow_review_reason": "score=0.150 prob=0.250 samples=220; shadow_score<0.50, shadow_prob<0.60",
                            "manual_review_reason": "shadow ML burn-in requires review: score=0.150 prob=0.250 samples=220",
                        }
                    ),
                },
            ]
        )
        summary = _build_shadow_review_summary_rows(rows)
        feedback = _build_shadow_feedback_rows(rows, summary)
        self.assertEqual(len(feedback), 1)
        self.assertEqual(feedback[0]["shadow_review_action"], "WEAK_SIGNAL")
        self.assertGreater(float(feedback[0]["scoring_accumulate_threshold_delta"]), 0.0)
        self.assertGreater(float(feedback[0]["scoring_execution_ready_threshold_delta"]), 0.0)
        self.assertGreaterEqual(int(feedback[0]["plan_review_window_days_delta"]), 7)
        penalties = json.loads(str(feedback[0]["signal_penalties_json"]))
        self.assertEqual(penalties[0]["symbol"], "0700.HK")
        self.assertGreater(float(penalties[0]["score_penalty"]), 0.0)

    def test_attribution_rows_split_weekly_return_into_proxy_components(self):
        summary_rows = [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "weekly_return": 0.06,
                "latest_equity": 100000.0,
                "cash_after": 25000.0,
            }
        ]
        sector_rows = [
            {"portfolio_id": "US:watchlist", "sector": "Technology", "weight": 0.42},
            {"portfolio_id": "US:watchlist", "sector": "Finance", "weight": 0.18},
        ]
        latest_rows = {
            "US:watchlist": [
                {"portfolio_id": "US:watchlist", "symbol": "AAPL", "market_value": 42000.0},
                {"portfolio_id": "US:watchlist", "symbol": "MSFT", "market_value": 33000.0},
            ]
        }
        execution_effect_rows = _build_execution_effect_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "symbol": "AAPL",
                    "qty": 10.0,
                    "price": 200.0,
                    "actual_slippage_bps": 12.0,
                    "exec_id": "E1",
                }
            ],
            [
                {
                    "portfolio_id": "US:watchlist",
                    "exec_id": "E1",
                    "value": 2.5,
                }
            ],
        )
        execution_gate_rows = _build_execution_gate_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "symbol": "AAPL",
                    "status": "BLOCKED_LIQUIDITY",
                    "order_value": 1500.0,
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "symbol": "MSFT",
                    "status": "PLANNED",
                    "order_value": 2000.0,
                },
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            report_dir = Path(tmp)
            (report_dir / "market_sentiment.json").write_text(
                json.dumps({"benchmark_ret5d": 0.03}),
                encoding="utf-8",
            )
            (report_dir / "investment_execution_summary.json").write_text(
                json.dumps(
                    {
                        "strategy_effective_controls": {
                            "base_effective_target_invested_weight": 0.70,
                            "effective_target_invested_weight": 0.55,
                        },
                        "risk_base_gross_exposure": 0.90,
                        "risk_dynamic_gross_exposure": 0.72,
                    }
                ),
                encoding="utf-8",
            )
            runs_by_portfolio = {
                "US:watchlist": [
                    {
                        "portfolio_id": "US:watchlist",
                        "report_dir": str(report_dir),
                        "details": json.dumps({"report_dir": str(report_dir)}),
                    }
                ]
            }
            rows = _build_attribution_rows(
                summary_rows,
                sector_rows=sector_rows,
                latest_rows_by_portfolio=latest_rows,
                execution_effect_rows=execution_effect_rows,
                execution_gate_rows=execution_gate_rows,
                runs_by_portfolio=runs_by_portfolio,
            )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        total = (
            float(row["selection_contribution"])
            + float(row["sizing_contribution"])
            + float(row["sector_contribution"])
            + float(row["execution_contribution"])
            + float(row["market_contribution"])
        )
        self.assertAlmostEqual(total, float(row["weekly_return"]), places=6)
        self.assertEqual(row["top_sector"], "Technology")
        self.assertEqual(row["attribution_mode"], "proxy_v1")
        self.assertAlmostEqual(float(row["strategy_control_weight_delta"]), 0.15, places=6)
        self.assertAlmostEqual(float(row["risk_overlay_weight_delta"]), 0.18, places=6)
        self.assertAlmostEqual(float(row["execution_gate_blocked_order_value"]), 1500.0, places=6)
        self.assertAlmostEqual(float(row["execution_gate_blocked_order_ratio"]), 0.5, places=6)
        self.assertAlmostEqual(float(row["execution_gate_blocked_weight"]), 0.015, places=6)
        self.assertIn("策略 15.0%", str(row["control_split_text"]))
        self.assertTrue(str(row["diagnosis"]))

    def test_execution_gate_rows_summarize_blocked_counts_and_values(self):
        rows = _build_execution_gate_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "symbol": "AAPL",
                    "status": "BLOCKED_OPPORTUNITY",
                    "order_value": 1200.0,
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "symbol": "MSFT",
                    "status": "REVIEW_REQUIRED",
                    "order_value": 1800.0,
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "symbol": "NVDA",
                    "status": "PLANNED",
                    "order_value": 3000.0,
                },
            ]
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["blocked_order_count"], 2)
        self.assertAlmostEqual(float(row["blocked_order_value"]), 3000.0, places=6)
        self.assertAlmostEqual(float(row["blocked_order_ratio"]), 2.0 / 3.0, places=6)
        self.assertAlmostEqual(float(row["blocked_order_value_ratio"]), 0.5, places=6)

    def test_risk_review_rows_summarize_dynamic_exposure_and_stress(self):
        runs_by_portfolio = {
            "US:watchlist": [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "ts": "2026-03-17T10:00:00+00:00",
                    "details": json.dumps(
                        {
                            "risk_overlay": {
                                "dynamic_net_exposure": 0.72,
                                "dynamic_gross_exposure": 0.80,
                                "avg_pair_correlation": 0.66,
                                "stress_worst_loss": 0.094,
                                "stress_worst_scenario_label": "指数下跌",
                                "top_sector_share": 0.47,
                                "notes": ["相关性偏高，降低组合总敞口。"],
                                "stress_scenarios": {
                                    "index_drop": {"loss": 0.094},
                                    "volatility_spike": {"loss": 0.071},
                                    "liquidity_shock": {"loss": 0.053},
                                },
                            }
                        }
                    ),
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "ts": "2026-03-18T10:00:00+00:00",
                    "details": json.dumps(
                        {
                            "risk_overlay": {
                                "dynamic_scale": 0.78,
                                "dynamic_net_exposure": 0.69,
                                "dynamic_gross_exposure": 0.77,
                                "dynamic_short_exposure": 0.20,
                                "final_avg_pair_correlation": 0.64,
                                "final_max_pair_correlation": 0.81,
                                "final_stress_worst_loss": 0.089,
                                "final_stress_worst_scenario_label": "流动性恶化",
                                "top_sector_share": 0.44,
                                "notes": ["组合平均流动性偏弱，降低总敞口并保留现金。"],
                                "correlation_reduced_symbols": ["AAPL", "MSFT"],
                                "final_stress_scenarios": {
                                    "index_drop": {"loss": 0.081},
                                    "volatility_spike": {"loss": 0.074},
                                    "liquidity_shock": {"loss": 0.089},
                                },
                            }
                        }
                    ),
                },
            ]
        }
        rows = _build_risk_review_rows(runs_by_portfolio)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["dominant_risk_driver"], "CORRELATION")
        self.assertAlmostEqual(row["latest_dynamic_net_exposure"], 0.69, places=6)
        self.assertAlmostEqual(row["latest_stress_worst_loss"], 0.089, places=6)
        self.assertEqual(row["latest_stress_worst_scenario_label"], "流动性恶化")
        self.assertIn("AAPL", row["correlation_reduced_symbols"])
        self.assertTrue(str(row["risk_diagnosis"]))

    def test_risk_review_rows_prefer_normalized_risk_history_when_available(self):
        runs_by_portfolio = {
            "US:watchlist": [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "ts": "2026-03-18T09:00:00+00:00",
                    "details": json.dumps(
                        {
                            "risk_overlay": {
                                "dynamic_net_exposure": 0.91,
                                "dynamic_gross_exposure": 0.96,
                                "avg_pair_correlation": 0.22,
                                "stress_worst_loss": 0.021,
                                "stress_worst_scenario_label": "指数下跌",
                            }
                        }
                    ),
                }
            ]
        }
        risk_history_by_portfolio = {
            "US:watchlist": [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "ts": "2026-03-19T09:00:00+00:00",
                    "source_kind": "execution",
                    "dynamic_scale": 0.79,
                    "dynamic_net_exposure": 0.68,
                    "dynamic_gross_exposure": 0.75,
                    "dynamic_short_exposure": 0.18,
                    "avg_pair_correlation": 0.65,
                    "max_pair_correlation": 0.82,
                    "top_sector_share": 0.46,
                    "stress_worst_loss": 0.088,
                    "stress_worst_scenario": "liquidity_shock",
                    "stress_worst_scenario_label": "流动性恶化",
                    "notes_json": json.dumps(["执行侧风险预算继续收紧。"], ensure_ascii=False),
                    "correlation_reduced_symbols_json": json.dumps(["AAPL", "MSFT"], ensure_ascii=False),
                    "stress_scenarios_json": json.dumps(
                        {
                            "index_drop": {"loss": 0.081},
                            "volatility_spike": {"loss": 0.074},
                            "liquidity_shock": {"loss": 0.088},
                        },
                        ensure_ascii=False,
                    ),
                }
            ]
        }
        rows = _build_risk_review_rows(runs_by_portfolio, risk_history_by_portfolio)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["risk_history_source"], "normalized_table")
        self.assertEqual(row["risk_history_sources"], "execution")
        self.assertAlmostEqual(row["latest_dynamic_net_exposure"], 0.68, places=6)
        self.assertEqual(row["latest_stress_worst_scenario_label"], "流动性恶化")
        self.assertEqual(row["dominant_risk_driver"], "CORRELATION")
        self.assertIn("AAPL", row["correlation_reduced_symbols"])

    def test_risk_feedback_rows_translate_review_into_budget_deltas(self):
        rows = _build_risk_feedback_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "dominant_risk_driver": "CORRELATION",
                    "latest_avg_pair_correlation": 0.66,
                    "latest_stress_worst_loss": 0.071,
                    "latest_top_sector_share": 0.47,
                    "latest_dynamic_net_exposure": 0.70,
                    "latest_dynamic_gross_exposure": 0.78,
                    "risk_diagnosis": "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。",
                },
                {
                    "portfolio_id": "HK:watchlist",
                    "market": "HK",
                    "dominant_risk_driver": "EXPOSURE_BUDGET",
                    "latest_avg_pair_correlation": 0.38,
                    "latest_stress_worst_loss": 0.048,
                    "latest_top_sector_share": 0.28,
                    "latest_dynamic_net_exposure": 0.63,
                    "latest_dynamic_gross_exposure": 0.70,
                    "risk_diagnosis": "组合风险预算偏紧，优先提升流动性与数据质量，再争取释放仓位。",
                },
            ]
        )
        self.assertEqual(len(rows), 2)
        us_row = rows[0]
        hk_row = rows[1]
        self.assertEqual(us_row["risk_feedback_action"], "TIGHTEN")
        self.assertLess(float(us_row["paper_max_net_exposure_delta"]), 0.0)
        self.assertLess(float(us_row["paper_max_sector_weight_delta"]), 0.0)
        self.assertLess(float(us_row["paper_correlation_soft_limit_delta"]), 0.0)
        self.assertEqual(hk_row["risk_feedback_action"], "RELAX")
        self.assertGreater(float(hk_row["paper_max_net_exposure_delta"]), 0.0)
        self.assertGreater(float(hk_row["paper_max_gross_exposure_delta"]), 0.0)

    def test_risk_feedback_rows_hold_when_strategy_de_risking_is_primary_driver(self):
        rows = _build_risk_feedback_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "dominant_risk_driver": "CORRELATION",
                    "latest_avg_pair_correlation": 0.66,
                    "latest_stress_worst_loss": 0.071,
                    "latest_top_sector_share": 0.47,
                    "latest_dynamic_net_exposure": 0.70,
                    "latest_dynamic_gross_exposure": 0.78,
                    "risk_diagnosis": "组合拥挤度偏高，优先增加跨行业/跨市场分散度，再考虑放宽仓位。",
                }
            ],
            attribution_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "strategy_control_weight_delta": 0.18,
                    "risk_overlay_weight_delta": 0.03,
                    "execution_gate_blocked_weight": 0.0,
                    "execution_gate_blocked_order_ratio": 0.0,
                    "execution_gate_blocked_order_value": 0.0,
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["risk_feedback_action"], "HOLD")
        self.assertEqual(float(row["paper_max_net_exposure_delta"]), 0.0)
        self.assertEqual(float(row["paper_max_gross_exposure_delta"]), 0.0)
        self.assertEqual(str(row["feedback_control_driver"]), "STRATEGY")
        self.assertIn("策略主动控仓", str(row["feedback_reason"]))


if __name__ == "__main__":
    unittest.main()
