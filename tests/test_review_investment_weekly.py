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
    _build_execution_parent_rows,
    _build_execution_feedback_rows,
    _build_feedback_calibration_rows,
    _build_execution_hotspot_penalties,
    _build_execution_hotspot_rows,
    _build_execution_session_rows,
    _build_execution_gate_rows,
    _build_weekly_blocked_edge_attribution_rows,
    _build_weekly_control_timeseries_rows,
    _build_weekly_decision_evidence_rows,
    _build_weekly_decision_evidence_summary_rows,
    _build_weekly_decision_evidence_history_overview,
    _build_trading_quality_evidence_rows,
    _build_unified_evidence_rows,
    _build_blocked_vs_allowed_expost_rows,
    _build_weekly_calibration_patch_suggestion_rows,
    _build_weekly_edge_calibration_rows,
    _build_weekly_edge_realization_rows,
    _build_weekly_patch_governance_summary_rows,
    _build_market_profile_patch_readiness,
    _build_weekly_outcome_spread_rows,
    _build_weekly_risk_calibration_rows,
    _build_weekly_slicing_calibration_rows,
    _build_weekly_tuning_dataset_rows,
    _build_weekly_tuning_dataset_summary,
    _link_execution_orders_to_candidate_snapshots,
    _build_market_profile_tuning_summary,
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
    _persist_market_profile_patch_history,
    _persist_weekly_decision_evidence_history,
    _persist_trading_quality_evidence,
    _persist_weekly_tuning_history,
    _persist_feedback_threshold_history,
    _run_source,
    _build_sector_rows,
    _build_weekly_tuning_history_overview,
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

    def test_market_profile_tuning_summary_flags_execution_gate_as_too_tight(self):
        rows = _build_market_profile_tuning_summary(
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "adaptive_strategy_active_market_profile": "US",
                    "adaptive_strategy_active_market_execution_summary": "min_edge=16.0bps | edge_buffer=5.0bps",
                    "adaptive_strategy_market_profile_note": "当前使用 US trend-first 市场档案。",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "strategy_control_weight_delta": 0.01,
                    "risk_overlay_weight_delta": 0.00,
                    "execution_gate_blocked_weight": 0.08,
                    "execution_gate_blocked_order_ratio": 0.60,
                    "execution_gate_blocked_order_count": 2,
                    "control_split_text": "策略 1.0% | 风险 0.0% | 执行 8.0%",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "risk_feedback_action": "HOLD",
                }
            ],
            [
                {
                    "portfolio_id": "P1",
                    "market": "US",
                    "execution_feedback_action": "HOLD",
                }
            ],
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["market_profile_tuning_target"]), "EXECUTION_GATE")
        self.assertEqual(str(row["market_profile_tuning_bias"]), "TOO_TIGHT")
        self.assertEqual(
            str(row["adaptive_strategy_active_market_execution_summary"]),
            "min_edge=16.0bps | edge_buffer=5.0bps",
        )
        self.assertIn("min_expected_edge_bps", str(row["market_profile_tuning_note"]))
        self.assertIn("counterfactual", str(row["market_profile_tuning_note"]))
        self.assertIn("5/20/60d", str(row["no_trade_optimization_note"]))
        self.assertEqual(int(row["counterfactual_optimization_available"]), 1)

    def test_weekly_tuning_dataset_rows_merge_strategy_outcome_and_execution_views(self):
        rows = _build_weekly_tuning_dataset_rows(
            [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "weekly_return": 0.021,
                    "max_drawdown": -0.015,
                    "turnover": 0.31,
                    "latest_equity": 102000.0,
                    "strategy_effective_controls_applied": True,
                    "market_profile_ready_for_manual_apply": 1,
                    "market_profile_readiness_label": "READY_FOR_MANUAL_APPLY",
                    "market_profile_readiness_summary": "连续 2 周一致，建议人工先改 1 项。",
                    "market_profile_cohort_weeks": 2,
                }
            ],
            decision_evidence_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "parent_order_key": "AAA-parent",
                    "symbol": "AAA",
                    "decision_status": "FILLED",
                    "order_value": 4200.0,
                    "fill_notional": 4200.0,
                    "signal_score": 0.88,
                    "expected_edge_bps": 34.0,
                    "expected_cost_bps": 14.0,
                    "edge_gate_threshold_bps": 20.0,
                    "blocked_market_rule_order_count": 0,
                    "blocked_edge_order_count": 1,
                    "dynamic_liquidity_bucket": "CORE",
                    "dynamic_order_adv_pct": 0.012,
                    "slice_count": 3,
                    "realized_slippage_bps": 25.0,
                    "realized_edge_bps": 118.0,
                    "outcome_5d_bps": 60.0,
                    "outcome_20d_bps": 180.0,
                    "outcome_60d_bps": 320.0,
                }
            ],
            strategy_context_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "adaptive_strategy_active_market_profile": "US",
                    "adaptive_strategy_market_profile_note": "当前使用 US trend-first 市场档案。",
                    "strategy_effective_controls_note": "策略主动转入防守。",
                    "execution_gate_summary": "边际收益 gate 阻断 1 单。",
                }
            ],
            attribution_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "planned_execution_cost_total": 18.0,
                    "execution_cost_total": 29.5,
                    "execution_cost_gap": 11.5,
                    "avg_expected_cost_bps": 14.0,
                    "avg_actual_slippage_bps": 25.0,
                    "strategy_control_weight_delta": 0.06,
                    "risk_overlay_weight_delta": 0.01,
                    "risk_market_profile_budget_weight_delta": 0.03,
                    "risk_throttle_weight_delta": 0.02,
                    "risk_recovery_weight_credit": 0.01,
                    "risk_layered_split_text": "budget 3.0% | throttle 2.0%(相关性) | recovery +1.0%",
                    "risk_dominant_throttle_layer": "CORRELATION",
                    "risk_dominant_throttle_layer_label": "相关性",
                    "execution_gate_blocked_order_count": 1,
                    "execution_gate_blocked_order_value": 4200.0,
                    "execution_gate_blocked_order_ratio": 0.25,
                    "execution_gate_blocked_weight": 0.04,
                    "control_split_text": "策略 6.0% | 风险 1.0% | 执行 4.0%",
                    "dominant_driver": "EXECUTION",
                }
            ],
            risk_review_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "dominant_risk_driver": "CORRELATION",
                    "latest_market_profile_budget_tightening": 0.03,
                    "latest_throttle_tightening": 0.02,
                    "latest_recovery_credit": 0.01,
                    "latest_dominant_throttle_layer": "CORRELATION",
                    "latest_dominant_throttle_layer_label": "相关性",
                    "risk_diagnosis": "组合拥挤度偏高。",
                }
            ],
            risk_feedback_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "risk_feedback_action": "HOLD",
                    "feedback_confidence": 0.22,
                    "feedback_confidence_label": "LOW",
                    "feedback_reason": "当前先保持风险预算不变。",
                }
            ],
            execution_feedback_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "execution_feedback_action": "TIGHTEN",
                    "feedback_confidence": 0.74,
                    "feedback_confidence_label": "MEDIUM",
                    "feedback_reason": "实际执行成本高于计划。",
                    "dominant_execution_session_label": "开盘",
                    "dominant_execution_hotspot_symbol": "AAPL",
                    "execution_penalty_symbol_count": 2,
                    "feedback_control_driver": "EXECUTION",
                    "feedback_control_driver_label": "执行 gate",
                }
            ],
            market_profile_tuning_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "market_profile_tuning_target": "EXECUTION_GATE",
                    "market_profile_tuning_bias": "TOO_TIGHT",
                    "market_profile_tuning_action": "REVIEW_EXECUTION_GATE",
                    "market_profile_tuning_note": "优先复核 min_expected_edge_bps / edge_cost_buffer_bps。",
                }
            ],
            feedback_calibration_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "outcome_sample_count": 18,
                    "outcome_positive_rate": 0.61,
                    "outcome_broken_rate": 0.11,
                    "signal_quality_score": 0.67,
                    "calibration_confidence": 0.71,
                    "calibration_confidence_label": "MEDIUM",
                    "latest_outcome_ts": "2026-03-20T00:00:00+00:00",
                    "selection_scope_label": "final 可执行候选",
                    "selected_horizon_days": "20",
                }
            ],
            feedback_automation_rows=[
                {
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "shadow",
                    "calibration_apply_mode": "SUGGEST_ONLY",
                    "calibration_apply_mode_label": "仅建议",
                    "outcome_maturity_label": "BUILDING",
                },
                {
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "risk",
                    "calibration_apply_mode": "HOLD",
                    "calibration_apply_mode_label": "继续观察",
                    "outcome_maturity_label": "LATE",
                },
                {
                    "portfolio_id": "US:watchlist",
                    "feedback_kind": "execution",
                    "calibration_apply_mode": "AUTO_APPLY",
                    "calibration_apply_mode_label": "自动应用",
                    "outcome_maturity_label": "LATE",
                    "market_data_gate_status": "OK",
                    "market_data_gate_label": "IBKR正常",
                },
            ],
            week_label="2026-W12",
            window_start="2026-03-15T00:00:00+00:00",
            window_end="2026-03-22T00:00:00+00:00",
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["portfolio_id"], "US:watchlist")
        self.assertEqual(row["adaptive_strategy_active_market_profile"], "US")
        self.assertEqual(row["outcome_sample_count"], 18)
        self.assertEqual(row["execution_feedback_action"], "TIGHTEN")
        self.assertEqual(row["execution_apply_mode"], "AUTO_APPLY")
        self.assertEqual(row["market_profile_tuning_action"], "REVIEW_EXECUTION_GATE")
        self.assertEqual(row["market_profile_ready_for_manual_apply"], 1)
        self.assertEqual(row["dominant_driver"], "EXECUTION")
        self.assertEqual(row["execution_gate_blocked_order_count"], 1)
        self.assertEqual(row["decision_evidence_row_count"], 1)
        self.assertEqual(row["decision_primary_liquidity_bucket"], "CORE")
        self.assertAlmostEqual(float(row["decision_avg_dynamic_order_adv_pct"]), 0.012, places=6)
        self.assertAlmostEqual(float(row["decision_avg_slice_count"]), 3.0, places=6)
        self.assertAlmostEqual(float(row["decision_avg_realized_edge_bps"]), 118.0, places=6)
        self.assertAlmostEqual(float(row["decision_avg_outcome_20d_bps"]), 180.0, places=6)
        self.assertAlmostEqual(float(row["risk_market_profile_budget_weight_delta"]), 0.03, places=6)
        self.assertAlmostEqual(float(row["risk_throttle_weight_delta"]), 0.02, places=6)
        self.assertAlmostEqual(float(row["risk_recovery_weight_credit"]), 0.01, places=6)
        self.assertEqual(row["risk_dominant_throttle_layer"], "CORRELATION")
        self.assertIn("budget 3.0%", row["risk_layered_split_text"])
        self.assertAlmostEqual(float(row["risk_latest_market_profile_budget_tightening"]), 0.03, places=6)
        self.assertAlmostEqual(float(row["risk_latest_throttle_tightening"]), 0.02, places=6)
        self.assertAlmostEqual(float(row["risk_latest_recovery_credit"]), 0.01, places=6)
        self.assertIn("策略 6.0%", row["control_split_text"])

        summary = _build_weekly_tuning_dataset_summary(rows)
        self.assertEqual(int(summary["portfolio_count"]), 1)
        self.assertEqual(int(summary["execution_driver_count"]), 1)
        self.assertEqual(int(summary["execution_tighten_count"]), 1)
        self.assertEqual(int(summary["ready_for_manual_apply_count"]), 1)
        self.assertAlmostEqual(float(summary["avg_signal_quality_score"]), 0.67, places=6)

    def test_weekly_patch_governance_summary_rows_aggregate_review_cycles(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            storage = Storage(str(db_path))
            storage.insert_investment_patch_review_history(
                {
                    "week_label": "2026-W12",
                    "week_start": "2026-03-16",
                    "ts": "2026-03-16T09:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "patch_kind": "calibration",
                    "feedback_signature": "sig-cal-1",
                    "review_status": "APPROVED",
                    "review_status_label": "已批准",
                    "scope": "SLICING_RELAX",
                    "details": {
                        "primary_item": {
                            "field": "adv_split_trigger_pct",
                            "config_path": "execution.adv_split_trigger_pct",
                            "scope_label": "执行切片",
                        }
                    },
                }
            )
            storage.insert_investment_patch_review_history(
                {
                    "week_label": "2026-W13",
                    "week_start": "2026-03-23",
                    "ts": "2026-03-23T09:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "patch_kind": "calibration",
                    "feedback_signature": "sig-cal-1",
                    "review_status": "APPLIED",
                    "review_status_label": "已应用",
                    "scope": "SLICING_RELAX",
                    "details": {
                        "primary_item": {
                            "field": "adv_split_trigger_pct",
                            "config_path": "execution.adv_split_trigger_pct",
                            "scope_label": "执行切片",
                        }
                    },
                }
            )
            storage.insert_investment_patch_review_history(
                {
                    "week_label": "2026-W14",
                    "week_start": "2026-03-30",
                    "ts": "2026-03-30T09:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "patch_kind": "calibration",
                    "feedback_signature": "sig-cal-2",
                    "review_status": "REJECTED",
                    "review_status_label": "已驳回",
                    "scope": "SLICING_RELAX",
                    "details": {
                        "primary_item": {
                            "field": "adv_split_trigger_pct",
                            "config_path": "execution.adv_split_trigger_pct",
                            "scope_label": "执行切片",
                        }
                    },
                }
            )
            storage.insert_investment_patch_review_history(
                {
                    "week_label": "2026-W14",
                    "week_start": "2026-03-30",
                    "ts": "2026-03-30T10:00:00+00:00",
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "patch_kind": "market_profile",
                    "feedback_signature": "sig-mp-1",
                    "review_status": "APPROVED",
                    "review_status_label": "已批准",
                    "scope": "REGIME_PLAN",
                    "details": {
                        "primary_item": {
                            "field": "no_trade_band_pct",
                            "config_path": "market_profiles.US.no_trade_band_pct",
                            "scope_label": "Regime / 计划参数",
                        }
                    },
                }
            )
            rows = _build_weekly_patch_governance_summary_rows(
                db_path,
                [{"portfolio_id": "US:watchlist", "market": "US"}],
            )
            self.assertEqual(len(rows), 2)
            row_map = {str(row["field"]): dict(row) for row in rows}
            calibration_row = row_map["adv_split_trigger_pct"]
            self.assertEqual(str(calibration_row["patch_kind_label"]), "校准补丁")
            self.assertEqual(str(calibration_row["field"]), "adv_split_trigger_pct")
            self.assertEqual(str(calibration_row["scope_label"]), "执行切片")
            self.assertEqual(int(calibration_row["review_cycle_count"]), 2)
            self.assertEqual(int(calibration_row["approved_count"]), 1)
            self.assertEqual(int(calibration_row["rejected_count"]), 1)
            self.assertEqual(int(calibration_row["applied_count"]), 1)
            self.assertEqual(int(calibration_row["approved_not_applied_count"]), 0)
            self.assertEqual(int(calibration_row["open_cycle_count"]), 0)
            self.assertAlmostEqual(float(calibration_row["approval_rate"]), 0.5, places=6)
            self.assertAlmostEqual(float(calibration_row["apply_rate"]), 0.5, places=6)
            self.assertAlmostEqual(float(calibration_row["avg_review_to_apply_weeks"]), 1.0, places=6)
            self.assertEqual(str(calibration_row["review_latency_basis"]), "review_to_apply")
            market_profile_row = row_map["no_trade_band_pct"]
            self.assertEqual(str(market_profile_row["patch_kind_label"]), "市场档案")
            self.assertEqual(str(market_profile_row["field"]), "no_trade_band_pct")
            self.assertEqual(int(market_profile_row["approved_not_applied_count"]), 1)
            self.assertEqual(int(market_profile_row["open_cycle_count"]), 1)

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

    def test_market_profile_patch_readiness_requires_two_consistent_weeks(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            row_w13 = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "adaptive_strategy_active_market_profile": "US",
                "market_profile_tuning_target": "EXECUTION_GATE",
                "market_profile_tuning_action": "REVIEW_EXECUTION_GATE",
                "market_profile_tuning_bias": "TOO_TIGHT",
                "market_profile_tuning_note": "优先复核 edge gate。",
                "adaptive_strategy_active_market_execution_summary": "min_edge=16.0bps | edge_buffer=5.0bps",
                "execution_feedback_action": "HOLD",
                "risk_feedback_action": "HOLD",
                "strategy_control_weight_delta": 0.01,
                "risk_overlay_weight_delta": 0.00,
                "execution_gate_blocked_weight": 0.08,
            }
            row_w14 = dict(row_w13)
            _persist_market_profile_patch_history(
                db_path,
                [row_w13],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_market_profile_patch_history(
                db_path,
                [row_w14],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )
            rows = _build_market_profile_patch_readiness(db_path, [row_w14])
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(str(row["market_profile_readiness_label"]), "READY_FOR_MANUAL_APPLY")
            self.assertEqual(int(row["market_profile_cohort_weeks"]), 2)
            self.assertEqual(str(row["market_profile_baseline_week"]), "2026-W13")
            self.assertEqual(int(row["market_profile_ready_for_manual_apply"]), 1)
            self.assertIn("可升级为人工应用候选", str(row["market_profile_readiness_summary"]))
            self.assertIn("2026-W13:REVIEW_EXECUTION_GATE", str(row["market_profile_action_chain"]))

    def test_weekly_tuning_history_persists_and_builds_overview(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            row_w13 = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "adaptive_strategy_active_market_profile": "US",
                "dominant_driver": "EXECUTION",
                "market_profile_tuning_action": "REVIEW_EXECUTION_GATE",
                "weekly_return": 0.011,
                "max_drawdown": -0.021,
                "turnover": 0.28,
                "outcome_sample_count": 12,
                "signal_quality_score": 0.54,
                "execution_cost_gap": 18.0,
                "execution_gate_blocked_weight": 0.06,
                "strategy_control_weight_delta": 0.03,
                "risk_overlay_weight_delta": 0.01,
                "risk_feedback_action": "HOLD",
                "execution_feedback_action": "TIGHTEN",
                "shadow_apply_mode": "SUGGEST_ONLY",
                "risk_apply_mode": "HOLD",
                "execution_apply_mode": "SUGGEST_ONLY",
                "market_profile_ready_for_manual_apply": 0,
            }
            row_w14 = dict(row_w13)
            row_w14.update(
                {
                    "weekly_return": 0.019,
                    "outcome_sample_count": 18,
                    "signal_quality_score": 0.66,
                    "execution_cost_gap": 7.5,
                    "execution_gate_blocked_weight": 0.03,
                    "execution_apply_mode": "AUTO_APPLY",
                    "market_profile_ready_for_manual_apply": 1,
                }
            )

            _persist_weekly_tuning_history(
                db_path,
                [row_w13],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_weekly_tuning_history(
                db_path,
                [row_w14],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )

            history_rows = Storage(str(db_path)).get_recent_investment_weekly_tuning_history(
                "US",
                portfolio_id="US:watchlist",
                limit=5,
            )
            self.assertEqual(len(history_rows), 2)
            self.assertEqual(str(history_rows[0]["week_label"]), "2026-W14")
            self.assertEqual(str(history_rows[0]["dominant_driver"]), "EXECUTION")
            self.assertEqual(int(history_rows[0]["market_profile_ready_for_manual_apply"]), 1)
            self.assertAlmostEqual(float(history_rows[0]["signal_quality_score"]), 0.66, places=6)
            self.assertAlmostEqual(float(history_rows[0]["execution_cost_gap"]), 7.5, places=6)
            self.assertEqual(str(history_rows[0]["details_json"]["execution_apply_mode"]), "AUTO_APPLY")

            overview_rows = _build_weekly_tuning_history_overview(
                db_path,
                [row_w14],
            )
            self.assertEqual(len(overview_rows), 1)
            row = overview_rows[0]
            self.assertEqual(str(row["latest_week_label"]), "2026-W14")
            self.assertEqual(str(row["baseline_week_label"]), "2026-W13")
            self.assertEqual(int(row["weeks_tracked"]), 2)
            self.assertEqual(str(row["latest_market_profile_tuning_action"]), "REVIEW_EXECUTION_GATE")
            self.assertAlmostEqual(float(row["signal_quality_delta"]), 0.12, places=6)
            self.assertEqual(str(row["signal_quality_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["execution_cost_gap_delta"]), -10.5, places=6)
            self.assertEqual(str(row["execution_cost_gap_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["execution_gate_blocked_weight_delta"]), -0.03, places=6)
            self.assertEqual(str(row["execution_gate_pressure_trend"]), "IMPROVING")
            self.assertIn("2026-W13:EXECUTION", str(row["driver_chain"]))
            self.assertIn("2026-W14:REVIEW_EXECUTION_GATE", str(row["tuning_action_chain"]))

    def test_weekly_decision_evidence_history_persists_and_builds_overview(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            row_w13 = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "run_id": "EX1",
                "parent_order_key": "AAA-parent",
                "symbol": "AAA",
                "action": "BUY",
                "decision_status": "BLOCKED_EDGE",
                "candidate_snapshot_id": "RUN1|final|AAA",
                "candidate_stage": "final",
                "order_value": 3200.0,
                "fill_notional": 0.0,
                "signal_score": 0.62,
                "expected_edge_bps": 32.0,
                "expected_cost_bps": 12.0,
                "edge_gate_threshold_bps": 20.0,
                "blocked_market_rule_order_count": 0,
                "blocked_edge_order_count": 1,
                "blocked_gate_order_count": 1,
                "dynamic_liquidity_bucket": "TAIL",
                "dynamic_order_adv_pct": 0.004,
                "slice_count": 4,
                "strategy_control_weight_delta": 0.04,
                "risk_overlay_weight_delta": 0.02,
                "risk_market_profile_budget_weight_delta": 0.01,
                "risk_throttle_weight_delta": 0.01,
                "risk_recovery_weight_credit": 0.0,
                "execution_gate_blocked_weight": 0.05,
                "realized_slippage_bps": 15.0,
                "realized_edge_bps": 40.0,
                "execution_capture_bps": 12.0,
                "first_fill_delay_seconds": 120.0,
                "outcome_5d_bps": 40.0,
                "outcome_20d_bps": 80.0,
                "outcome_60d_bps": 120.0,
            }
            row_w14 = dict(row_w13)
            row_w14.update(
                {
                    "decision_status": "FILLED",
                    "blocked_edge_order_count": 0,
                    "blocked_gate_order_count": 0,
                    "dynamic_liquidity_bucket": "CORE",
                    "dynamic_order_adv_pct": 0.010,
                    "slice_count": 2,
                    "fill_notional": 3200.0,
                    "realized_slippage_bps": 8.0,
                    "realized_edge_bps": 92.0,
                    "execution_capture_bps": 24.0,
                    "first_fill_delay_seconds": 60.0,
                    "outcome_5d_bps": 70.0,
                    "outcome_20d_bps": 170.0,
                    "outcome_60d_bps": 260.0,
                }
            )

            _persist_weekly_decision_evidence_history(
                db_path,
                [row_w13],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_weekly_decision_evidence_history(
                db_path,
                [row_w14],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )

            history_rows = Storage(str(db_path)).get_recent_investment_weekly_decision_evidence_history(
                "US",
                portfolio_id="US:watchlist",
                limit=10,
            )
            self.assertEqual(len(history_rows), 2)
            self.assertEqual(str(history_rows[0]["week_label"]), "2026-W14")
            self.assertEqual(str(history_rows[0]["decision_status"]), "FILLED")
            self.assertAlmostEqual(float(history_rows[0]["realized_edge_bps"]), 92.0, places=6)
            self.assertEqual(str(history_rows[0]["details_json"]["dynamic_liquidity_bucket"]), "CORE")

            overview_rows = _build_weekly_decision_evidence_history_overview(
                db_path,
                [row_w14],
                limit=6,
            )
            self.assertEqual(len(overview_rows), 1)
            row = overview_rows[0]
            self.assertEqual(str(row["latest_week_label"]), "2026-W14")
            self.assertEqual(str(row["baseline_week_label"]), "2026-W13")
            self.assertEqual(int(row["weeks_tracked"]), 2)
            self.assertEqual(str(row["latest_primary_liquidity_bucket"]), "CORE")
            self.assertIn("2026-W13:TAIL", str(row["liquidity_bucket_chain"]))
            self.assertIn("2026-W14:CORE", str(row["liquidity_bucket_chain"]))
            self.assertAlmostEqual(float(row["decision_avg_realized_slippage_bps_delta"]), -7.0, places=6)
            self.assertEqual(str(row["decision_slippage_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["decision_avg_realized_edge_bps_delta"]), 52.0, places=6)
            self.assertEqual(str(row["decision_realized_edge_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["decision_avg_outcome_20d_bps_delta"]), 90.0, places=6)
            self.assertEqual(str(row["decision_outcome_20d_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["decision_blocked_edge_order_count_delta"]), -1.0, places=6)
            self.assertEqual(str(row["decision_blocked_edge_trend"]), "IMPROVING")
            self.assertAlmostEqual(float(row["decision_avg_dynamic_order_adv_pct_delta"]), 0.006, places=6)
            self.assertAlmostEqual(float(row["decision_avg_slice_count_delta"]), -2.0, places=6)

    def test_trading_quality_evidence_rows_persist_gate_and_execution_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            filled = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "run_id": "EX1",
                "parent_order_key": "AAA-parent",
                "symbol": "AAA",
                "decision_status": "FILLED",
                "order_value": 5000.0,
                "fill_notional": 5000.0,
                "expected_edge_bps": 90.0,
                "expected_cost_bps": 20.0,
                "edge_gate_threshold_bps": 45.0,
                "blocked_market_rule_order_count": 0,
                "blocked_edge_order_count": 0,
                "dynamic_liquidity_bucket": "CORE",
                "dynamic_order_adv_pct": 0.01,
                "slice_count": 2,
                "realized_slippage_bps": 16.0,
                "realized_edge_bps": 78.0,
                "outcome_20d_bps": 120.0,
            }
            blocked = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "run_id": "EX1",
                "parent_order_key": "BBB-parent",
                "symbol": "BBB",
                "decision_status": "BLOCKED_EDGE",
                "order_value": 4000.0,
                "fill_notional": 0.0,
                "expected_edge_bps": 25.0,
                "expected_cost_bps": 20.0,
                "edge_gate_threshold_bps": 45.0,
                "blocked_market_rule_order_count": 0,
                "blocked_edge_order_count": 1,
                "dynamic_liquidity_bucket": "TAIL",
                "dynamic_order_adv_pct": 0.003,
                "slice_count": 5,
                "realized_slippage_bps": None,
                "realized_edge_bps": None,
                "outcome_20d_bps": -20.0,
            }
            rows = _build_trading_quality_evidence_rows([filled, blocked])

            edge_row = next(row for row in rows if row["evidence_layer"] == "EDGE_GATE")
            execution_row = next(row for row in rows if row["evidence_layer"] == "EXECUTION_QUALITY")
            self.assertEqual(edge_row["portfolio_id"], "US:watchlist")
            self.assertEqual(edge_row["sample_count"], 2)
            self.assertEqual(edge_row["blocked_count"], 1)
            self.assertAlmostEqual(float(edge_row["post_cost_edge_delta_bps"]), 140.0, places=6)
            self.assertEqual(edge_row["rule_quality"], "HELPING_POST_COST_EDGE")
            self.assertEqual(execution_row["evidence_key"], "CORE")
            self.assertEqual(execution_row["rule_quality"], "EXECUTION_DISCIPLINE_OK")

            _persist_trading_quality_evidence(
                db_path,
                rows,
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )
            stored = Storage(str(db_path)).get_recent_investment_trading_quality_evidence(
                "US",
                portfolio_id="US:watchlist",
                limit=10,
            )
            self.assertEqual(len(stored), 3)
            stored_edge = next(row for row in stored if row["evidence_layer"] == "EDGE_GATE")
            self.assertEqual(stored_edge["details_json"]["filled_symbols"], ["AAA"])
            self.assertEqual(stored_edge["details_json"]["blocked_symbols"], ["BBB"])

    def test_unified_evidence_and_blocked_expost_review(self):
        filled = {
            "portfolio_id": "US:watchlist",
            "market": "US",
            "run_id": "EX1",
            "parent_order_key": "AAA-parent",
            "symbol": "AAA",
            "decision_status": "FILLED",
            "fill_notional": 5000.0,
            "signal_score": 0.82,
            "expected_edge_bps": 90.0,
            "expected_cost_bps": 20.0,
            "edge_gate_threshold_bps": 45.0,
            "blocked_edge_order_count": 0,
            "blocked_market_rule_order_count": 0,
            "dynamic_liquidity_bucket": "CORE",
            "dynamic_order_adv_pct": 0.01,
            "slice_count": 2,
            "realized_slippage_bps": 16.0,
            "realized_edge_bps": 78.0,
            "outcome_5d_bps": 35.0,
            "outcome_20d_bps": 120.0,
            "outcome_60d_bps": 180.0,
        }
        blocked = {
            "portfolio_id": "US:watchlist",
            "market": "US",
            "run_id": "EX1",
            "parent_order_key": "BBB-parent",
            "symbol": "BBB",
            "decision_status": "BLOCKED_EDGE",
            "fill_notional": 0.0,
            "signal_score": 0.31,
            "expected_edge_bps": 25.0,
            "expected_cost_bps": 20.0,
            "edge_gate_threshold_bps": 45.0,
            "blocked_edge_order_count": 1,
            "blocked_market_rule_order_count": 0,
            "dynamic_liquidity_bucket": "TAIL",
            "dynamic_order_adv_pct": 0.003,
            "slice_count": 5,
            "outcome_5d_bps": -12.0,
            "outcome_20d_bps": -20.0,
            "outcome_60d_bps": -55.0,
        }

        unified_rows = _build_unified_evidence_rows([filled, blocked])
        self.assertEqual(len(unified_rows), 2)
        allowed_row = next(row for row in unified_rows if row["symbol"] == "AAA")
        blocked_row = next(row for row in unified_rows if row["symbol"] == "BBB")
        self.assertEqual(allowed_row["allowed_flag"], 1)
        self.assertEqual(blocked_row["blocked_flag"], 1)
        self.assertEqual(blocked_row["block_reason"], "EDGE_GATE")
        self.assertAlmostEqual(float(allowed_row["realized_edge_delta_bps"]), 8.0, places=6)

        review_rows = _build_blocked_vs_allowed_expost_rows([filled, blocked])
        self.assertEqual(len(review_rows), 1)
        review_row = review_rows[0]
        self.assertEqual(review_row["block_reason"], "EDGE_GATE")
        self.assertEqual(review_row["allowed_count"], 1)
        self.assertEqual(review_row["blocked_count"], 1)
        self.assertAlmostEqual(float(review_row["allowed_minus_blocked_outcome_5d_bps"]), 47.0, places=6)
        self.assertAlmostEqual(float(review_row["allowed_minus_blocked_outcome_20d_bps"]), 140.0, places=6)
        self.assertAlmostEqual(float(review_row["allowed_minus_blocked_outcome_60d_bps"]), 235.0, places=6)
        self.assertEqual(review_row["positive_outcome_horizon_count"], 3)
        self.assertEqual(review_row["review_basis"], "5/20/60d_multi_horizon")
        self.assertEqual(review_row["review_label"], "BLOCKING_HELPED")

    def test_decision_evidence_keeps_candidate_outcomes_without_orders(self):
        portfolio_id = "US:watchlist"
        snapshot_rows = [
            {
                "snapshot_id": "RUN1|final|AAA",
                "market": "US",
                "portfolio_id": portfolio_id,
                "analysis_run_id": "RUN1",
                "stage": "final",
                "symbol": "AAA",
                "action": "ACCUMULATE",
                "direction": "LONG",
                "score": 0.72,
                "score_before_cost": 0.81,
                "expected_cost_bps": 12.0,
                "expected_edge_bps": 55.0,
                "details": json.dumps({"stage_rank": 1}),
            },
            {
                "snapshot_id": "RUN1|deep|BBB",
                "market": "US",
                "portfolio_id": portfolio_id,
                "analysis_run_id": "RUN1",
                "stage": "deep",
                "symbol": "BBB",
                "action": "WATCH",
                "direction": "LONG",
                "score": 0.44,
                "score_before_cost": 0.50,
                "expected_cost_bps": 9.0,
                "expected_edge_bps": 18.0,
                "details": json.dumps({"stage_rank": 2}),
            },
        ]
        outcome_rows = [
            {
                "snapshot_id": "RUN1|final|AAA",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "AAA",
                "horizon_days": 5,
                "future_return": 0.02,
            },
            {
                "snapshot_id": "RUN1|final|AAA",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "AAA",
                "horizon_days": 20,
                "future_return": 0.07,
            },
            {
                "snapshot_id": "RUN1|deep|BBB",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "BBB",
                "horizon_days": 20,
                "future_return": -0.01,
            },
        ]

        decision_rows = _build_weekly_decision_evidence_rows(
            [],
            strategy_context_rows=[
                {
                    "portfolio_id": portfolio_id,
                    "strategy_effective_controls_note": "no order week",
                }
            ],
            attribution_rows=[
                {
                    "portfolio_id": portfolio_id,
                    "strategy_control_weight_delta": 0.05,
                }
            ],
            snapshot_rows=snapshot_rows,
            outcome_rows=outcome_rows,
        )
        self.assertEqual(len(decision_rows), 2)
        selected = next(row for row in decision_rows if row["symbol"] == "AAA")
        self.assertEqual(str(selected["decision_source"]), "candidate_snapshot")
        self.assertEqual(str(selected["decision_status"]), "CANDIDATE_SELECTED")
        self.assertEqual(str(selected["join_quality"]), "candidate_outcome_only")
        self.assertEqual(int(selected["candidate_only_flag"]), 1)
        self.assertAlmostEqual(float(selected["realized_edge_bps"]), 688.0, places=6)
        self.assertAlmostEqual(float(selected["outcome_20d_bps"]), 700.0, places=6)

        unified_rows = _build_unified_evidence_rows(decision_rows)
        unified_selected = next(row for row in unified_rows if row["symbol"] == "AAA")
        self.assertEqual(int(unified_selected["candidate_only_flag"]), 1)
        self.assertEqual(str(unified_selected["join_quality"]), "candidate_outcome_only")
        self.assertEqual(int(unified_selected["allowed_flag"]), 0)
        self.assertEqual(int(unified_selected["blocked_flag"]), 0)
        self.assertAlmostEqual(float(unified_selected["realized_edge_delta_bps"]), 645.0, places=6)

    def test_weekly_edge_slicing_and_risk_calibration_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            decision_w13 = [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "run_id": "EX13-A",
                    "parent_order_key": "AAA-parent",
                    "symbol": "AAA",
                    "action": "BUY",
                    "decision_status": "FILLED",
                    "candidate_snapshot_id": "RUN13|final|AAA",
                    "candidate_stage": "final",
                    "order_value": 4000.0,
                    "fill_notional": 4000.0,
                    "signal_score": 0.72,
                    "expected_edge_bps": 36.0,
                    "expected_cost_bps": 12.0,
                    "edge_gate_threshold_bps": 20.0,
                    "blocked_market_rule_order_count": 0,
                    "blocked_edge_order_count": 0,
                    "blocked_gate_order_count": 0,
                    "dynamic_liquidity_bucket": "CORE",
                    "dynamic_order_adv_pct": 0.010,
                    "slice_count": 4,
                    "strategy_control_weight_delta": 0.03,
                    "risk_overlay_weight_delta": 0.02,
                    "risk_market_profile_budget_weight_delta": 0.01,
                    "risk_throttle_weight_delta": 0.02,
                    "risk_recovery_weight_credit": 0.00,
                    "execution_gate_blocked_weight": 0.04,
                    "realized_slippage_bps": 6.0,
                    "realized_edge_bps": 90.0,
                    "execution_capture_bps": 24.0,
                    "first_fill_delay_seconds": 100.0,
                    "outcome_5d_bps": 60.0,
                    "outcome_20d_bps": 180.0,
                    "outcome_60d_bps": 260.0,
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "run_id": "EX13-B",
                    "parent_order_key": "BBB-parent",
                    "symbol": "BBB",
                    "action": "BUY",
                    "decision_status": "BLOCKED_EDGE",
                    "candidate_snapshot_id": "RUN13|final|BBB",
                    "candidate_stage": "final",
                    "order_value": 1800.0,
                    "fill_notional": 0.0,
                    "signal_score": 0.41,
                    "expected_edge_bps": 14.0,
                    "expected_cost_bps": 10.0,
                    "edge_gate_threshold_bps": 20.0,
                    "blocked_market_rule_order_count": 0,
                    "blocked_edge_order_count": 1,
                    "blocked_gate_order_count": 1,
                    "dynamic_liquidity_bucket": "TAIL",
                    "dynamic_order_adv_pct": 0.004,
                    "slice_count": 1,
                    "strategy_control_weight_delta": 0.03,
                    "risk_overlay_weight_delta": 0.02,
                    "risk_market_profile_budget_weight_delta": 0.01,
                    "risk_throttle_weight_delta": 0.02,
                    "risk_recovery_weight_credit": 0.00,
                    "execution_gate_blocked_weight": 0.04,
                    "realized_slippage_bps": None,
                    "realized_edge_bps": None,
                    "execution_capture_bps": None,
                    "first_fill_delay_seconds": None,
                    "outcome_5d_bps": 20.0,
                    "outcome_20d_bps": 60.0,
                    "outcome_60d_bps": 80.0,
                },
            ]
            decision_w14 = [
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "run_id": "EX14-A",
                    "parent_order_key": "CCC-parent",
                    "symbol": "CCC",
                    "action": "BUY",
                    "decision_status": "FILLED",
                    "candidate_snapshot_id": "RUN14|final|CCC",
                    "candidate_stage": "final",
                    "order_value": 4200.0,
                    "fill_notional": 4200.0,
                    "signal_score": 0.69,
                    "expected_edge_bps": 34.0,
                    "expected_cost_bps": 12.0,
                    "edge_gate_threshold_bps": 20.0,
                    "blocked_market_rule_order_count": 0,
                    "blocked_edge_order_count": 0,
                    "blocked_gate_order_count": 0,
                    "dynamic_liquidity_bucket": "CORE",
                    "dynamic_order_adv_pct": 0.011,
                    "slice_count": 4,
                    "strategy_control_weight_delta": 0.03,
                    "risk_overlay_weight_delta": 0.05,
                    "risk_market_profile_budget_weight_delta": 0.01,
                    "risk_throttle_weight_delta": 0.06,
                    "risk_recovery_weight_credit": 0.00,
                    "execution_gate_blocked_weight": 0.02,
                    "realized_slippage_bps": 7.0,
                    "realized_edge_bps": 48.0,
                    "execution_capture_bps": 14.0,
                    "first_fill_delay_seconds": 110.0,
                    "outcome_5d_bps": 30.0,
                    "outcome_20d_bps": 100.0,
                    "outcome_60d_bps": 150.0,
                },
                {
                    "portfolio_id": "US:watchlist",
                    "market": "US",
                    "run_id": "EX14-B",
                    "parent_order_key": "DDD-parent",
                    "symbol": "DDD",
                    "action": "BUY",
                    "decision_status": "BLOCKED_MARKET_RULE",
                    "candidate_snapshot_id": "RUN14|final|DDD",
                    "candidate_stage": "final",
                    "order_value": 1200.0,
                    "fill_notional": 0.0,
                    "signal_score": 0.38,
                    "expected_edge_bps": 10.0,
                    "expected_cost_bps": 9.0,
                    "edge_gate_threshold_bps": 18.0,
                    "blocked_market_rule_order_count": 1,
                    "blocked_edge_order_count": 0,
                    "blocked_gate_order_count": 1,
                    "dynamic_liquidity_bucket": "TAIL",
                    "dynamic_order_adv_pct": 0.003,
                    "slice_count": 1,
                    "strategy_control_weight_delta": 0.03,
                    "risk_overlay_weight_delta": 0.05,
                    "risk_market_profile_budget_weight_delta": 0.01,
                    "risk_throttle_weight_delta": 0.06,
                    "risk_recovery_weight_credit": 0.00,
                    "execution_gate_blocked_weight": 0.02,
                    "realized_slippage_bps": None,
                    "realized_edge_bps": None,
                    "execution_capture_bps": None,
                    "first_fill_delay_seconds": None,
                    "outcome_5d_bps": 15.0,
                    "outcome_20d_bps": 50.0,
                    "outcome_60d_bps": 70.0,
                },
            ]
            tuning_w13 = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "adaptive_strategy_active_market_profile": "US",
                "dominant_driver": "RISK",
                "market_profile_tuning_action": "OBSERVE",
                "weekly_return": 0.012,
                "max_drawdown": -0.020,
                "turnover": 0.22,
                "outcome_sample_count": 12,
                "signal_quality_score": 0.62,
                "execution_cost_gap": 8.0,
                "execution_gate_blocked_weight": 0.04,
                "strategy_control_weight_delta": 0.03,
                "risk_overlay_weight_delta": 0.02,
                "risk_market_profile_budget_weight_delta": 0.01,
                "risk_throttle_weight_delta": 0.02,
                "risk_recovery_weight_credit": 0.00,
                "risk_dominant_throttle_layer": "CORRELATION",
                "risk_dominant_throttle_layer_label": "相关性",
                "risk_feedback_action": "HOLD",
                "execution_feedback_action": "HOLD",
                "shadow_apply_mode": "SUGGEST_ONLY",
                "risk_apply_mode": "HOLD",
                "execution_apply_mode": "SUGGEST_ONLY",
                "market_profile_ready_for_manual_apply": 0,
            }
            tuning_w14 = dict(tuning_w13)
            tuning_w14.update(
                {
                    "weekly_return": 0.004,
                    "outcome_sample_count": 18,
                    "signal_quality_score": 0.58,
                    "execution_cost_gap": 5.0,
                    "execution_gate_blocked_weight": 0.02,
                    "risk_overlay_weight_delta": 0.05,
                    "risk_throttle_weight_delta": 0.06,
                }
            )

            _persist_weekly_decision_evidence_history(
                db_path,
                decision_w13,
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_weekly_decision_evidence_history(
                db_path,
                decision_w14,
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )
            _persist_weekly_tuning_history(
                db_path,
                [tuning_w13],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_weekly_tuning_history(
                db_path,
                [tuning_w14],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )

            edge_rows = _build_weekly_edge_calibration_rows(db_path, decision_w14, limit=6)
            self.assertEqual(len(edge_rows), 1)
            self.assertEqual(str(edge_rows[0]["edge_gate_quality"]), "GATE_DISCIPLINE_GOOD")
            self.assertEqual(str(edge_rows[0]["market_rule_quality"]), "RULE_FILTER_GOOD")
            self.assertAlmostEqual(float(edge_rows[0]["blocked_edge_vs_filled_outcome_20d_bps"]), -80.0, places=6)

            slicing_rows = _build_weekly_slicing_calibration_rows(db_path, decision_w14, limit=6)
            core_row = next(row for row in slicing_rows if str(row["dynamic_liquidity_bucket"]) == "CORE")
            self.assertEqual(str(core_row["slicing_assessment"]), "POSSIBLY_TOO_CONSERVATIVE")
            self.assertAlmostEqual(float(core_row["avg_realized_slippage_bps"]), 6.5, places=6)

            risk_rows = _build_weekly_risk_calibration_rows(db_path, [tuning_w14], limit=6)
            self.assertEqual(len(risk_rows), 1)
            self.assertEqual(str(risk_rows[0]["risk_calibration_target"]), "THROTTLE_TOO_TIGHT")
            self.assertEqual(str(risk_rows[0]["latest_dominant_throttle_layer_label"]), "相关性")
            self.assertAlmostEqual(float(risk_rows[0]["decision_avg_outcome_20d_bps_delta"]), -53.8697318, places=6)

    def test_weekly_calibration_patch_suggestions_cover_config_scopes(self):
        strategy_context_rows = [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "adaptive_strategy_active_market_profile": "US",
                "adaptive_strategy_active_market_plan_summary": "staged=3x | no_trade_band=3.0%",
                "adaptive_strategy_active_market_regime_summary": "risk_on=0.50 | hard_off=0.25",
                "adaptive_strategy_active_market_execution_summary": "min_edge=16.0bps | edge_buffer=5.0bps",
                "adaptive_strategy_market_profile_note": "当前使用 US trend-first 市场档案；risk budget net=0.88 gross=0.95。",
            }
        ]
        edge_rows = [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "edge_gate_quality": "GATE_TOO_TIGHT",
                "edge_calibration_note": "被 edge gate 挡掉的单事后并不差，当前 edge floor/buffer 可能偏紧。",
            }
        ]
        slicing_rows = [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "dynamic_liquidity_bucket": "CORE",
                "sample_count": 8,
                "filled_sample_count": 6,
                "slicing_assessment": "POSSIBLY_TOO_CONSERVATIVE",
                "slicing_calibration_note": "切片次数偏多但滑点仍低，当前 bucket 可能过度保守。",
            }
        ]
        risk_rows = [
            {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "risk_calibration_target": "THROTTLE_TOO_TIGHT",
                "latest_dominant_throttle_layer": "CORRELATION",
                "latest_dominant_throttle_layer_label": "相关性",
                "risk_calibration_note": "最近收益拖累更像来自 throttle 层，优先复核相关性/流动性/集中度 throttle。",
            }
        ]

        rows = _build_weekly_calibration_patch_suggestion_rows(
            strategy_context_rows,
            edge_calibration_rows=edge_rows,
            slicing_calibration_rows=slicing_rows,
            risk_calibration_rows=risk_rows,
        )
        self.assertGreaterEqual(len(rows), 5)

        edge_item = next(row for row in rows if str(row["field"]) == "edge_cost_buffer_bps")
        self.assertEqual(str(edge_item["config_scope"]), "ADAPTIVE_STRATEGY")
        self.assertTrue(str(edge_item["config_file"]).endswith("config/adaptive_strategy_framework.yaml"))
        self.assertEqual(str(edge_item["config_path"]), "market_profiles.US.edge_cost_buffer_bps")
        self.assertAlmostEqual(float(edge_item["current_value"]), 5.0, places=6)
        self.assertAlmostEqual(float(edge_item["suggested_value"]), 4.0, places=6)

        slicing_item = next(row for row in rows if str(row["field"]) == "adv_split_trigger_pct")
        self.assertEqual(str(slicing_item["config_scope"]), "EXECUTION")
        self.assertTrue(str(slicing_item["config_file"]).endswith("config/investment_execution_us.yaml"))
        self.assertEqual(str(slicing_item["config_path"]), "execution.adv_split_trigger_pct")
        self.assertAlmostEqual(float(slicing_item["current_value"]), 0.02, places=6)
        self.assertAlmostEqual(float(slicing_item["suggested_value"]), 0.025, places=6)

        risk_item = next(row for row in rows if str(row["field"]) == "correlation_soft_limit")
        self.assertEqual(str(risk_item["config_scope"]), "PAPER")
        self.assertTrue(str(risk_item["config_file"]).endswith("config/investment_paper_us.yaml"))
        self.assertEqual(str(risk_item["config_path"]), "paper.correlation_soft_limit")
        self.assertAlmostEqual(float(risk_item["current_value"]), 0.62, places=6)
        self.assertAlmostEqual(float(risk_item["suggested_value"]), 0.65, places=6)

    def test_outcome_spread_edge_realization_and_blocked_edge_attribution(self):
        portfolio_id = "US:watchlist"
        snapshot_rows = [
            {
                "snapshot_id": "RUN1|final|AAA",
                "market": "US",
                "portfolio_id": portfolio_id,
                "report_dir": "/tmp/report-run-1",
                "analysis_run_id": "RUN1",
                "stage": "final",
                "symbol": "AAA",
                "direction": "LONG",
                "score": 0.82,
                "score_before_cost": 0.90,
                "expected_cost_bps": 10.0,
                "expected_edge_threshold": 0.20,
                "expected_edge_score": 0.30,
                "expected_edge_bps": 50.0,
                "details": json.dumps({"stage_rank": 1}),
            },
            {
                "snapshot_id": "RUN2|final|BBB",
                "market": "US",
                "portfolio_id": portfolio_id,
                "report_dir": "/tmp/report-run-2",
                "analysis_run_id": "RUN2",
                "stage": "final",
                "symbol": "BBB",
                "direction": "LONG",
                "score": 0.48,
                "score_before_cost": 0.40,
                "expected_cost_bps": 8.0,
                "expected_edge_threshold": 0.18,
                "expected_edge_score": 0.07,
                "expected_edge_bps": 12.0,
                "details": json.dumps({"stage_rank": 2}),
            },
            {
                "snapshot_id": "RUN1|deep|CCC",
                "market": "US",
                "portfolio_id": portfolio_id,
                "report_dir": "/tmp/report-run-1",
                "analysis_run_id": "RUN1",
                "stage": "deep",
                "symbol": "CCC",
                "direction": "LONG",
                "score": 0.31,
                "score_before_cost": 0.28,
                "expected_cost_bps": 9.0,
                "expected_edge_threshold": 0.15,
                "expected_edge_score": 0.04,
                "expected_edge_bps": 9.0,
                "details": json.dumps({"stage_rank": 1}),
            },
        ]
        outcome_rows = []
        for horizon_days, aaa_ret, bbb_ret, ccc_ret in (
            (5, 0.03, 0.01, 0.005),
            (20, 0.08, 0.03, 0.01),
            (60, 0.15, 0.05, 0.02),
        ):
            outcome_rows.extend(
                [
                    {
                        "snapshot_id": "RUN1|final|AAA",
                        "market": "US",
                        "portfolio_id": portfolio_id,
                        "symbol": "AAA",
                        "direction": "LONG",
                        "horizon_days": horizon_days,
                        "future_return": aaa_ret,
                    },
                    {
                        "snapshot_id": "RUN2|final|BBB",
                        "market": "US",
                        "portfolio_id": portfolio_id,
                        "symbol": "BBB",
                        "direction": "LONG",
                        "horizon_days": horizon_days,
                        "future_return": bbb_ret,
                    },
                    {
                        "snapshot_id": "RUN1|deep|CCC",
                        "market": "US",
                        "portfolio_id": portfolio_id,
                        "symbol": "CCC",
                        "direction": "LONG",
                        "horizon_days": horizon_days,
                        "future_return": ccc_ret,
                    },
                ]
            )
        execution_run_rows = [
            {
                "run_id": "EX1",
                "market": "US",
                "portfolio_id": portfolio_id,
                "report_dir": "/tmp/report-run-1",
            },
            {
                "run_id": "EX2",
                "market": "US",
                "portfolio_id": portfolio_id,
                "report_dir": "/tmp/report-run-2",
            },
        ]
        execution_order_rows = [
            {
                "run_id": "EX1",
                "ts": "2026-03-01T14:30:00+00:00",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "AAA",
                "action": "BUY",
                "current_qty": 0.0,
                "target_qty": 10.0,
                "delta_qty": 10.0,
                "target_weight": 0.10,
                "order_value": 1000.0,
                "broker_order_id": 101,
                "status": "FILLED",
                "score_before_cost": 0.90,
                "expected_cost_bps": 10.0,
                "expected_edge_threshold": 0.20,
                "expected_edge_score": 0.30,
                "expected_edge_bps": 50.0,
                "edge_gate_threshold_bps": 18.0,
                "details": json.dumps(
                    {
                        "parent_order_key": "AAA-parent",
                        "dynamic_liquidity_bucket": "CORE",
                        "dynamic_order_adv_pct": 0.015,
                        "slice_count": 3,
                    }
                ),
            },
            {
                "run_id": "EX2",
                "ts": "2026-03-02T14:30:00+00:00",
                "market": "US",
                "portfolio_id": portfolio_id,
                "symbol": "BBB",
                "action": "BUY",
                "current_qty": 0.0,
                "target_qty": 12.0,
                "delta_qty": 12.0,
                "target_weight": 0.12,
                "order_value": 1200.0,
                "broker_order_id": 0,
                "status": "BLOCKED_EDGE",
                "score_before_cost": 0.40,
                "expected_cost_bps": 8.0,
                "expected_edge_threshold": 0.18,
                "expected_edge_score": 0.07,
                "expected_edge_bps": 12.0,
                "edge_gate_threshold_bps": 18.0,
                "details": json.dumps(
                    {
                        "parent_order_key": "BBB-parent",
                        "dynamic_liquidity_bucket": "TAIL",
                        "dynamic_order_adv_pct": 0.004,
                        "slice_count": 1,
                        "market_rule_status": "RULES_OK",
                    }
                ),
            },
        ]
        fill_rows = [
            {
                "ts": "2026-03-01T14:31:30+00:00",
                "order_id": 101,
                "exec_id": "EXEC-AAA-1",
                "symbol": "AAA",
                "qty": 10.0,
                "price": 100.0,
                "actual_slippage_bps": 6.0,
                "portfolio_id": portfolio_id,
                "fill_delay_seconds": 90.0,
            }
        ]
        commission_rows = [
            {
                "exec_id": "EXEC-AAA-1",
                "value": 2.0,
                "portfolio_id": portfolio_id,
            }
        ]

        linked_orders = _link_execution_orders_to_candidate_snapshots(
            execution_order_rows,
            execution_run_rows,
            snapshot_rows,
        )
        self.assertEqual(str(linked_orders[0]["linked_snapshot_id"]), "RUN1|final|AAA")
        self.assertEqual(str(linked_orders[1]["linked_snapshot_id"]), "RUN2|final|BBB")

        parent_rows = _build_execution_parent_rows(
            linked_orders,
            fill_rows,
            commission_rows,
            outcome_rows,
        )
        self.assertEqual(str(parent_rows[0]["dynamic_liquidity_bucket"]), "CORE")
        self.assertAlmostEqual(float(parent_rows[0]["avg_dynamic_order_adv_pct"]), 0.015, places=6)
        self.assertEqual(int(parent_rows[0]["slice_count"]), 3)
        self.assertEqual(int(parent_rows[1]["blocked_edge_order_count"]), 1)
        self.assertEqual(int(parent_rows[1]["blocked_market_rule_order_count"]), 0)
        outcome_spread_rows = _build_weekly_outcome_spread_rows(
            snapshot_rows,
            outcome_rows,
            parent_rows,
        )
        edge_rows = _build_weekly_edge_realization_rows(parent_rows)
        blocked_rows = _build_weekly_blocked_edge_attribution_rows(parent_rows)
        decision_rows = _build_weekly_decision_evidence_rows(
            parent_rows,
            strategy_context_rows=[{"portfolio_id": portfolio_id, "strategy_effective_controls_note": "策略主动控仓。"}],
            attribution_rows=[
                {
                    "portfolio_id": portfolio_id,
                    "strategy_control_weight_delta": 0.06,
                    "risk_overlay_weight_delta": 0.03,
                    "risk_market_profile_budget_weight_delta": 0.01,
                    "risk_throttle_weight_delta": 0.02,
                    "risk_recovery_weight_credit": 0.0,
                    "execution_gate_blocked_weight": 0.04,
                }
            ],
        )
        decision_summary_rows = _build_weekly_decision_evidence_summary_rows(decision_rows)

        outcome_20d = next(row for row in outcome_spread_rows if int(row["horizon_days"]) == 20)
        self.assertEqual(int(outcome_20d["selected_sample_count"]), 2)
        self.assertEqual(int(outcome_20d["blocked_edge_sample_count"]), 1)
        self.assertAlmostEqual(float(outcome_20d["selected_spread_vs_unselected_bps"]), 450.0, places=6)
        self.assertAlmostEqual(float(outcome_20d["executed_spread_vs_blocked_edge_bps"]), 500.0, places=6)

        self.assertEqual(len(edge_rows), 1)
        edge_row = edge_rows[0]
        self.assertAlmostEqual(float(edge_row["avg_expected_edge_bps"]), 29.272727, places=5)
        self.assertAlmostEqual(float(edge_row["avg_realized_total_cost_bps"]), 26.0, places=6)
        self.assertAlmostEqual(float(edge_row["avg_execution_capture_bps"]), 24.0, places=6)
        self.assertAlmostEqual(float(edge_row["avg_fill_delay_seconds"]), 90.0, places=6)
        self.assertAlmostEqual(float(edge_row["matured_20d_avg_realized_edge_bps"]), 774.0, places=6)

        self.assertEqual(len(blocked_rows), 1)
        blocked_row = blocked_rows[0]
        self.assertEqual(int(blocked_row["blocked_edge_parent_count"]), 1)
        self.assertAlmostEqual(float(blocked_row["avg_required_gap_bps"]), 6.0, places=6)
        self.assertAlmostEqual(float(blocked_row["blocked_expected_edge_value"]), 1.44, places=6)
        self.assertAlmostEqual(float(blocked_row["matured_20d_avg_counterfactual_edge_bps"]), 292.0, places=6)
        self.assertEqual(len(decision_rows), 2)
        self.assertEqual(str(decision_rows[0]["dynamic_liquidity_bucket"]), "CORE")
        self.assertAlmostEqual(float(decision_rows[0]["realized_edge_bps"]), 774.0, places=6)
        self.assertEqual(len(decision_summary_rows), 1)
        self.assertEqual(str(decision_summary_rows[0]["decision_primary_liquidity_bucket"]), "CORE")
        self.assertAlmostEqual(float(decision_summary_rows[0]["decision_avg_dynamic_order_adv_pct"]), 0.009, places=6)

    def test_weekly_control_timeseries_rows_build_from_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            row_w13 = {
                "portfolio_id": "US:watchlist",
                "market": "US",
                "dominant_driver": "RISK",
                "market_profile_tuning_action": "OBSERVE",
                "weekly_return": 0.01,
                "signal_quality_score": 0.55,
                "execution_cost_gap": 9.0,
                "execution_gate_blocked_weight": 0.01,
                "strategy_control_weight_delta": 0.02,
                "risk_overlay_weight_delta": 0.05,
            }
            row_w14 = dict(row_w13)
            row_w14.update(
                {
                    "dominant_driver": "EXECUTION",
                    "weekly_return": 0.02,
                    "signal_quality_score": 0.61,
                    "execution_cost_gap": 4.0,
                    "execution_gate_blocked_weight": 0.04,
                    "strategy_control_weight_delta": 0.04,
                    "risk_overlay_weight_delta": 0.02,
                }
            )
            _persist_weekly_tuning_history(
                db_path,
                [row_w13],
                week_label="2026-W13",
                week_start="2026-03-23",
                window_start="2026-03-17T00:00:00+00:00",
                window_end="2026-03-24T00:00:00+00:00",
            )
            _persist_weekly_tuning_history(
                db_path,
                [row_w14],
                week_label="2026-W14",
                week_start="2026-03-30",
                window_start="2026-03-24T00:00:00+00:00",
                window_end="2026-03-31T00:00:00+00:00",
            )

            rows = _build_weekly_control_timeseries_rows(db_path, [row_w14], limit=6)
            self.assertEqual(len(rows), 2)
            latest = rows[-1]
            self.assertEqual(str(latest["week_label"]), "2026-W14")
            self.assertEqual(str(latest["dominant_driver"]), "EXECUTION")
            self.assertAlmostEqual(float(latest["control_total_weight"]), 0.10, places=6)
            self.assertAlmostEqual(float(latest["strategy_control_share"]), 0.4, places=6)
            self.assertAlmostEqual(float(latest["risk_overlay_share"]), 0.2, places=6)
            self.assertAlmostEqual(float(latest["execution_gate_share"]), 0.4, places=6)

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
                        "risk_market_profile_budget_net_tightening": 0.04,
                        "risk_market_profile_budget_gross_tightening": 0.06,
                        "risk_throttle_net_tightening": 0.10,
                        "risk_throttle_gross_tightening": 0.12,
                        "risk_recovery_net_credit": 0.02,
                        "risk_recovery_gross_credit": 0.01,
                        "risk_layered_throttle_text": "budget 6.0% | throttle 12.0%(Stress) | recovery +2.0%",
                        "risk_dominant_throttle_layer": "STRESS",
                        "risk_dominant_throttle_layer_label": "Stress",
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
        self.assertAlmostEqual(float(row["risk_market_profile_budget_weight_delta"]), 0.06, places=6)
        self.assertAlmostEqual(float(row["risk_throttle_weight_delta"]), 0.12, places=6)
        self.assertAlmostEqual(float(row["risk_recovery_weight_credit"]), 0.02, places=6)
        self.assertEqual(str(row["risk_dominant_throttle_layer"]), "STRESS")
        self.assertEqual(str(row["risk_dominant_throttle_layer_label"]), "Stress")
        self.assertIn("budget 6.0%", str(row["risk_layered_split_text"]))
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
