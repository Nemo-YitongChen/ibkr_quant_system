from __future__ import annotations

import csv
import unittest
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile, TemporaryDirectory
from pathlib import Path
import json
import threading
from unittest.mock import Mock, patch

from src.analysis.investment import InvestmentPlanConfig, InvestmentScoringConfig, make_investment_plan, score_investment_candidate
from src.analysis.investment_backtest import InvestmentBacktestConfig, compute_investment_backtest_from_bars
from src.analysis.investment_shadow_ml import (
    InvestmentShadowModelConfig,
    apply_investment_shadow_model,
    train_investment_shadow_model,
)
from src.analysis.investment_portfolio import (
    InvestmentPaperConfig,
    build_target_allocations,
    is_rebalance_due,
    simulate_rebalance,
)
from src.portfolio.investment_allocator import (
    InvestmentExecutionConfig,
    build_investment_rebalance_orders,
)
from src.offhours.compute_mid import compute_mid_from_bars
from src.offhours.compute_long import compute_long_from_bars
from src.app.investment_engine import ExecutionSessionProfile, InvestmentExecutionEngine, _is_placeholder_account_id
from src.app.investment_guard import InvestmentGuardConfig, build_investment_guard_orders
from src.common.storage import Storage, build_investment_risk_history_row
from src.data.adapters import MarketDataAdapter
from src.ibkr.contracts import make_stock_contract
from src.ibkr.market_data import MarketDataService
from src.ibkr.market_data import OHLCVBar
from src.strategies.mid_regime import RegimeConfig
from src.tools.generate_investment_report import (
    _apply_weekly_feedback_penalties,
    _build_market_sentiment,
    _collect_symbol_feature_results,
    _layered_scan_config,
    _maybe_collect_research_scanner_symbols,
    _normalize_market_symbol,
)
from src.tools.label_investment_snapshots import (
    _build_skip_summary_rows,
    _build_skip_metadata,
    _load_daily_bars_for_labeling,
    _build_snapshot_outcome_result,
    build_snapshot_outcome,
)


def _bars(n: int = 500, start: float = 100.0, step: float = 0.2):
    out = []
    t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
    for i in range(n):
        close = start + (i * step)
        out.append(
            OHLCVBar(
                time=t0 + timedelta(days=i),
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=1_000_000 + i,
            )
        )
    return out


class InvestmentModuleTests(unittest.TestCase):
    def test_labeling_daily_bars_prefers_ibkr_loader_before_yfinance_cache(self):
        class _FakeLoader:
            def get_daily_bars(self, symbol, days):
                return (_bars(n=20, start=50.0, step=0.1), "ibkr")

        rows, source = _load_daily_bars_for_labeling(
            "AAPL",
            "US",
            90,
            {"US": _FakeLoader()},
        )
        self.assertEqual(source, "ibkr")
        self.assertGreaterEqual(len(rows), 10)

    def test_snapshot_outcome_result_reports_missing_history_reason(self):
        outcome, reason = _build_snapshot_outcome_result(
            {
                "snapshot_id": "snap-1",
                "market": "US",
                "portfolio_id": "US:watchlist",
                "symbol": "AAPL",
                "ts": "2026-03-01T00:00:00+00:00",
            },
            [],
            5,
        )
        self.assertIsNone(outcome)
        self.assertEqual(reason, "NO_HISTORY_BARS")

    def test_skip_summary_rows_group_by_portfolio_reason_and_horizon(self):
        rows = _build_skip_summary_rows(
            [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "AAPL",
                    "snapshot_ts": "2026-03-01T00:00:00+00:00",
                    "horizon_days": 5,
                    "skip_reason": "NO_HISTORY_BARS",
                },
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "MSFT",
                    "snapshot_ts": "2026-03-02T00:00:00+00:00",
                    "horizon_days": 5,
                    "skip_reason": "NO_HISTORY_BARS",
                },
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["skip_reason_label"], "历史数据为空")
        self.assertEqual(int(rows[0]["skip_count"]), 2)
        self.assertEqual(int(rows[0]["symbol_count"]), 2)
        self.assertIn("AAPL", str(rows[0]["sample_symbols"]))

    def test_skip_summary_rows_include_ready_estimate_for_insufficient_forward_bars(self):
        metadata = _build_skip_metadata(
            {"ts": "2020-01-03T00:00:00+00:00"},
            _bars(n=5),
            5,
            "INSUFFICIENT_FORWARD_BARS",
        )
        rows = _build_skip_summary_rows(
            [
                {
                    "market": "US",
                    "portfolio_id": "US:watchlist",
                    "symbol": "AAPL",
                    "snapshot_ts": "2020-01-03T00:00:00+00:00",
                    "horizon_days": 5,
                    "skip_reason": "INSUFFICIENT_FORWARD_BARS",
                    **metadata,
                }
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertGreaterEqual(int(rows[0]["min_remaining_forward_bars"]), 1)
        self.assertGreaterEqual(int(rows[0]["max_remaining_forward_bars"]), int(rows[0]["min_remaining_forward_bars"]))
        self.assertTrue(str(rows[0]["estimated_ready_start_ts"]))
        self.assertTrue(str(rows[0]["estimated_ready_end_ts"]))

    def test_storage_persists_normalized_investment_risk_history(self):
        with NamedTemporaryFile(suffix=".db") as tmp:
            storage = Storage(tmp.name)
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="paper-risk-1",
                    ts="2026-03-19T00:00:00+00:00",
                    market="US",
                    portfolio_id="US:watchlist",
                    source_kind="paper",
                    source_label="Dry Run",
                    report_dir="reports_investment/watchlist",
                    risk_overlay={
                        "dynamic_scale": 0.81,
                        "dynamic_net_exposure": 0.69,
                        "dynamic_gross_exposure": 0.77,
                        "avg_pair_correlation": 0.63,
                        "stress_worst_loss": 0.087,
                        "stress_worst_scenario_label": "流动性恶化",
                        "notes": ["规范化表已接入。"],
                        "correlation_reduced_symbols": ["AAPL", "MSFT"],
                    },
                )
            )
            rows = storage.get_recent_investment_risk_history(
                "US",
                "US:watchlist",
                source_kind="paper",
                limit=5,
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_label"], "Dry Run")
        self.assertAlmostEqual(float(rows[0]["dynamic_net_exposure"]), 0.69, places=6)
        self.assertIn("AAPL", str(rows[0]["correlation_reduced_symbols_json"]))

    def test_execution_configs_have_enough_run_capacity_for_target_allocation(self):
        import yaml

        config_paths = [
            Path("config/investment_execution_us.yaml"),
            Path("config/investment_execution_hk.yaml"),
            Path("config/investment_execution_asx.yaml"),
            Path("config/investment_execution_xetra.yaml"),
            Path("config/investment_execution_us_overnight.yaml"),
        ]
        for path in config_paths:
            with self.subTest(path=str(path)):
                with path.open("r", encoding="utf-8") as f:
                    raw = yaml.safe_load(f) or {}
                execution = dict(raw.get("execution") or {})
                account_allocation_pct = float(execution.get("account_allocation_pct", 0.0) or 0.0)
                max_order_value_pct = float(execution.get("max_order_value_pct", 0.0) or 0.0)
                max_orders_per_run = int(execution.get("max_orders_per_run", 0) or 0)
                manual_review_order_value_pct = float(execution.get("manual_review_order_value_pct", 0.0) or 0.0)

                self.assertGreaterEqual(
                    max_order_value_pct * max_orders_per_run + 1e-9,
                    account_allocation_pct,
                )
                self.assertLessEqual(max_order_value_pct, manual_review_order_value_pct + 1e-9)

    def test_placeholder_account_id_detection(self):
        self.assertTrue(_is_placeholder_account_id(""))
        self.assertTrue(_is_placeholder_account_id("UXXXXXXX"))

    def test_make_investment_plan_scales_borderline_rebalance_signal(self):
        base_row = {
            "symbol": "AAPL",
            "action": "ACCUMULATE",
            "direction": "LONG",
            "score": 0.36,
            "score_before_cost": 0.40,
            "execution_score": 0.20,
            "mid_scale": 0.80,
            "accumulate_threshold": 0.35,
            "hold_threshold": 0.10,
            "rebalance_flag": 1,
            "execution_ready": 1,
        }
        baseline = make_investment_plan(
            dict(base_row),
            vix=12.0,
            cfg=InvestmentPlanConfig(no_trade_band_pct=0.0, turnover_penalty_scale=0.0),
        )
        adjusted = make_investment_plan(
            dict(base_row),
            vix=12.0,
            cfg=InvestmentPlanConfig(no_trade_band_pct=0.05, turnover_penalty_scale=0.20),
        )
        self.assertGreater(float(baseline["allocation_mult"]), float(adjusted["allocation_mult"]))
        self.assertEqual(int(adjusted["plan_no_trade_band_applied"]), 1)
        self.assertEqual(int(adjusted["plan_turnover_penalty_applied"]), 1)
        self.assertAlmostEqual(float(adjusted["plan_no_trade_band_threshold"]), 0.35, places=6)
        self.assertIn("no-trade band", str(adjusted["notes"]))
        self.assertIn("turnover penalty", str(adjusted["notes"]))
        self.assertFalse(_is_placeholder_account_id("DU1234567"))

    def test_backtest_generates_forward_return_summary(self):
        result = compute_investment_backtest_from_bars(
            "TEST",
            _bars(),
            scoring_cfg=InvestmentScoringConfig(),
            cfg=InvestmentBacktestConfig(sample_step_days=10, min_trade_samples=1),
        )
        self.assertEqual(result["symbol"], "TEST")
        self.assertGreater(result["bt_signal_samples"], 0)
        self.assertIn("bt_avg_ret_30d", result)
        self.assertIn("bt_hit_rate_90d", result)

    def test_mid_and_long_can_share_same_daily_bar_payload(self):
        bars = _bars(n=600, start=100.0, step=0.15)
        mid = compute_mid_from_bars("TEST", bars, lookback_days=180)
        long = compute_long_from_bars("TEST", bars, years=2)
        self.assertIsNotNone(mid)
        self.assertIsNotNone(long)
        self.assertEqual(mid["symbol"], "TEST")
        self.assertEqual(long["symbol"], "TEST")
        self.assertIn("regime_state_v2", mid)
        self.assertEqual(mid["regime_state_v2"]["state"], mid["regime_state"])

    def test_layered_scan_config_prefers_bounded_stage_sizes_and_scanner_rules(self):
        cfg = _layered_scan_config(
            market_universe_cfg={
                "layered_scan": {
                    "broad_limit": 180,
                    "deep_limit": 54,
                    "enrichment_limit": 24,
                    "history_workers": 6,
                    "network_reserve_ratio": 0.45,
                }
            },
            investment_cfg={},
            ibkr_cfg={"scanner_enabled": True},
            max_universe=100,
            top_n=10,
            fundamentals_top_k=8,
            backtest_top_k=6,
            use_audit_recent=False,
            research_only_yfinance=True,
        )
        self.assertEqual(cfg.broad_limit, 100)
        self.assertEqual(cfg.deep_limit, 54)
        self.assertEqual(cfg.enrichment_limit, 24)
        self.assertEqual(cfg.history_workers, 6)
        self.assertAlmostEqual(cfg.network_reserve_ratio, 0.45, places=6)
        self.assertFalse(cfg.include_scanner)

    def test_layered_scan_config_allows_explicit_scanner_override_for_research_only(self):
        cfg = _layered_scan_config(
            market_universe_cfg={
                "layered_scan": {
                    "include_scanner": True,
                }
            },
            investment_cfg={},
            ibkr_cfg={"scanner_enabled": True},
            max_universe=100,
            top_n=10,
            fundamentals_top_k=8,
            backtest_top_k=6,
            use_audit_recent=False,
            research_only_yfinance=True,
        )
        self.assertTrue(cfg.include_scanner)

    @patch("src.tools.generate_investment_report._scanner_symbols")
    @patch("src.tools.generate_investment_report.set_delayed_frozen")
    @patch("src.tools.generate_investment_report.connect_ib")
    def test_research_only_scanner_symbols_connects_only_for_scanner(self, mock_connect_ib, mock_set_delayed, mock_scanner_symbols):
        ib = Mock()
        mock_connect_ib.return_value = ib
        mock_scanner_symbols.return_value = ["600519.SS", "000858.SZ", "688981.SS"]

        cfg = _layered_scan_config(
            market_universe_cfg={"layered_scan": {"include_scanner": True}},
            investment_cfg={},
            ibkr_cfg={"scanner_enabled": True},
            max_universe=100,
            top_n=10,
            fundamentals_top_k=8,
            backtest_top_k=6,
            use_audit_recent=False,
            research_only_yfinance=True,
        )

        symbols = _maybe_collect_research_scanner_symbols(
            resolved_market="CN",
            host="127.0.0.1",
            port=4002,
            client_id=151,
            request_timeout_sec=12.0,
            db_path="audit.db",
            ibkr_cfg={
                "scanner_enabled": True,
                "scanner_instrument": "STOCK.HK",
                "scanner_location_codes": ["STK.HK.SEHKNTL", "STK.HK.SEHKSZSE", "STK.HK.SEHKSTAR"],
            },
            layered_cfg=cfg,
        )

        self.assertEqual(symbols, ["600519.SS", "000858.SZ", "688981.SS"])
        mock_connect_ib.assert_called_once_with("127.0.0.1", 4002, 151, request_timeout=12.0)
        mock_set_delayed.assert_called_once_with(ib)
        mock_scanner_symbols.assert_called_once()
        ib.disconnect.assert_called_once()

    def test_market_symbol_normalization_covers_hk_asx_and_cn(self):
        self.assertEqual(_normalize_market_symbol("700", "HK"), "0700.HK")
        self.assertEqual(_normalize_market_symbol("BHP", "ASX"), "BHP.AX")
        self.assertEqual(_normalize_market_symbol("600519", "CN"), "600519.SS")
        self.assertEqual(_normalize_market_symbol("000858", "CN"), "000858.SZ")

    def test_market_sentiment_summary_reports_risk_on_bias(self):
        sentiment = _build_market_sentiment(
            {
                "markets": {
                    "tickers": {
                        "^VIX": {"close": 14.0, "ret1d": -0.02, "ret5d": -0.06},
                        "SPY": {"close": 520.0, "ret1d": 0.028, "ret5d": 0.065},
                        "QQQ": {"close": 450.0, "ret1d": 0.034, "ret5d": 0.090},
                        "XLF": {"close": 40.0, "ret1d": 0.018, "ret5d": 0.045},
                    }
                },
                "macro_events": [],
            },
            "US",
        )
        self.assertEqual(sentiment["label"], "RISK_ON")
        self.assertGreater(sentiment["score"], 0.25)
        self.assertIn("回撤入场", sentiment["guidance"])
        self.assertGreater(sentiment["breadth_positive_ratio"], 0.5)
        self.assertGreaterEqual(sentiment["leadership_spread_5d"], -0.05)

    def test_snapshot_storage_tracks_pending_and_labeled_outcomes(self):
        with NamedTemporaryFile(suffix=".db") as tmp:
            storage = Storage(tmp.name)
            snapshot_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
            storage.insert_investment_candidate_snapshot(
                {
                    "snapshot_id": "run|final|AAA",
                    "ts": snapshot_ts,
                    "market": "US",
                    "portfolio_id": "US:test",
                    "stage": "final",
                    "symbol": "AAA",
                    "action": "ACCUMULATE",
                    "direction": "LONG",
                    "score": 0.42,
                    "model_recommendation_score": 0.42,
                    "execution_score": 0.21,
                    "data_quality_score": 0.88,
                    "source_coverage": 1.0,
                    "missing_ratio": 0.08,
                    "execution_ready": 1,
                }
            )
            pending = storage.get_pending_investment_candidate_snapshots(
                market="US",
                portfolio_id="US:test",
                stage="final",
                horizon_days=5,
                limit=10,
            )
            self.assertEqual(len(pending), 1)
            self.assertAlmostEqual(float(pending[0]["data_quality_score"] or 0.0), 0.88, places=6)
            self.assertAlmostEqual(float(pending[0]["source_coverage"] or 0.0), 1.0, places=6)
            self.assertAlmostEqual(float(pending[0]["missing_ratio"] or 0.0), 0.08, places=6)
            storage.upsert_investment_candidate_outcome(
                {
                    "snapshot_id": "run|final|AAA",
                    "market": "US",
                    "portfolio_id": "US:test",
                    "symbol": "AAA",
                    "horizon_days": 5,
                    "snapshot_ts": snapshot_ts,
                    "outcome_ts": datetime.now(timezone.utc).isoformat(),
                    "direction": "LONG",
                    "start_close": 100.0,
                    "end_close": 110.0,
                    "future_return": 0.10,
                    "max_drawdown": -0.03,
                    "max_runup": 0.12,
                    "outcome_label": "POSITIVE",
                }
            )
            pending_after = storage.get_pending_investment_candidate_snapshots(
                market="US",
                portfolio_id="US:test",
                stage="final",
                horizon_days=5,
                limit=10,
            )
            recent = storage.get_recent_investment_candidate_outcomes(
                market="US",
                portfolio_id="US:test",
                horizon_days=5,
                limit=10,
            )
            self.assertEqual(pending_after, [])
            self.assertEqual(len(recent), 1)
            self.assertEqual(recent[0]["outcome_label"], "POSITIVE")

    def test_build_snapshot_outcome_computes_forward_path(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        bars = [
            OHLCVBar(
                time=start + timedelta(days=i),
                open=100.0 + i,
                high=101.0 + i,
                low=99.0 + i,
                close=100.0 + (2 * i),
                volume=1_000_000.0,
            )
            for i in range(8)
        ]
        snapshot = {
            "snapshot_id": "run|final|AAA",
            "ts": start.isoformat(),
            "market": "US",
            "portfolio_id": "US:test",
            "stage": "final",
            "symbol": "AAA",
            "direction": "LONG",
            "action": "ACCUMULATE",
            "score": 0.5,
            "model_recommendation_score": 0.5,
            "execution_score": 0.2,
        }
        outcome = build_snapshot_outcome(snapshot, bars, horizon_days=5)
        self.assertIsNotNone(outcome)
        if outcome is None:
            return
        self.assertEqual(outcome["snapshot_id"], "run|final|AAA")
        self.assertGreater(outcome["future_return"], 0.05)
        self.assertGreaterEqual(outcome["max_runup"], outcome["future_return"])
        self.assertIn(outcome["outcome_label"], {"OUTPERFORM", "POSITIVE"})

    def test_recommendation_trend_improves_investment_score(self):
        long_row = {
            "symbol": "SAP.DE",
            "long_score": 0.35,
            "trend_vs_ma200": 0.10,
            "mdd_1y": -0.08,
            "rebalance_flag": 0,
            "last_close": 180.0,
        }
        mid_row = {
            "symbol": "SAP.DE",
            "mid_scale": 0.72,
            "trend_slope_60d": 0.12,
            "regime_composite": 0.20,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 180.0,
        }
        base = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18},
            recommendation={},
            cfg=InvestmentScoringConfig(recommendation_weight=0.10),
        )
        improved = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18},
            recommendation={
                "recommendation_score": 0.60,
                "strong_buy": 5,
                "buy": 4,
                "hold": 1,
                "sell": 0,
                "strong_sell": 0,
                "recommendation_total": 10,
            },
            cfg=InvestmentScoringConfig(recommendation_weight=0.10),
        )
        self.assertGreater(improved["score"], base["score"])
        self.assertEqual(improved["strong_buy"], 5)

    def test_score_investment_candidate_emits_execution_quality_fields(self):
        long_row = {
            "symbol": "AAPL",
            "market": "US",
            "long_score": 0.30,
            "trend_vs_ma200": 0.12,
            "mdd_1y": -0.06,
            "rebalance_flag": 0,
            "last_close": 180.0,
        }
        mid_row = {
            "symbol": "AAPL",
            "market": "US",
            "mid_scale": 0.72,
            "trend_slope_60d": 0.10,
            "regime_composite": 0.22,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 180.0,
            "regime_state_v2": {"state": "RISK_ON"},
        }
        scored = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            market_sentiment_score=0.32,
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18},
            recommendation={"recommendation_score": 0.4},
            cfg=InvestmentScoringConfig(),
        )
        self.assertIn("model_recommendation_score", scored)
        self.assertIn("execution_score", scored)
        self.assertIn("execution_ready", scored)
        self.assertGreater(scored["execution_score"], 0.0)
        self.assertEqual(int(scored["execution_ready"]), 1)

    def test_low_data_quality_penalizes_score_and_execution_readiness(self):
        long_row = {
            "symbol": "AAPL",
            "market": "US",
            "long_score": 0.30,
            "trend_vs_ma200": 0.12,
            "mdd_1y": -0.06,
            "rebalance_flag": 0,
            "last_close": 180.0,
        }
        mid_row = {
            "symbol": "AAPL",
            "market": "US",
            "mid_scale": 0.72,
            "trend_slope_60d": 0.10,
            "regime_composite": 0.22,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 180.0,
            "regime_state_v2": {"state": "RISK_ON"},
        }
        high_quality = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            market_sentiment_score=0.32,
            data_quality_score=0.92,
            source_coverage=1.0,
            missing_ratio=0.06,
            history_source="ibkr",
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18},
            recommendation={"recommendation_score": 0.4},
            cfg=InvestmentScoringConfig(),
        )
        low_quality = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            market_sentiment_score=0.32,
            data_quality_score=0.34,
            source_coverage=0.72,
            missing_ratio=0.58,
            history_source="yfinance",
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18},
            recommendation={"recommendation_score": 0.4},
            cfg=InvestmentScoringConfig(),
        )
        self.assertGreater(high_quality["score"], low_quality["score"])
        self.assertEqual(int(high_quality["execution_ready"]), 1)
        self.assertEqual(int(low_quality["execution_ready"]), 0)
        self.assertIn("low_data_quality", list(low_quality["signal_decision"].get("gates_blocked", []) or []))

    def test_microstructure_improves_score_and_execution_bias(self):
        long_row = {
            "symbol": "AAPL",
            "market": "US",
            "long_score": 0.30,
            "trend_vs_ma200": 0.12,
            "mdd_1y": -0.06,
            "rebalance_flag": 0,
            "last_close": 180.0,
        }
        mid_row = {
            "symbol": "AAPL",
            "market": "US",
            "mid_scale": 0.72,
            "trend_slope_60d": 0.10,
            "regime_composite": 0.22,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 180.0,
            "regime_state_v2": {"state": "RISK_ON"},
        }
        weak = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            market_sentiment_score=0.18,
            microstructure_score=-0.35,
            micro_breakout_5m=-0.18,
            micro_reversal_5m=-0.10,
            micro_volume_burst_5m=-0.08,
            earnings_in_14d=False,
            macro_high_risk=False,
            cfg=InvestmentScoringConfig(),
        )
        strong = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            market_sentiment_score=0.18,
            microstructure_score=0.52,
            micro_breakout_5m=0.22,
            micro_reversal_5m=0.08,
            micro_volume_burst_5m=0.12,
            earnings_in_14d=False,
            macro_high_risk=False,
            cfg=InvestmentScoringConfig(),
        )
        self.assertGreater(float(strong["score"]), float(weak["score"]))
        self.assertGreater(float(strong["execution_score"]), float(weak["execution_score"]))
        self.assertAlmostEqual(float(strong["microstructure_score"]), 0.52, places=6)

    def test_shadow_ml_trains_and_scores_candidates(self):
        training_rows = []
        for idx in range(80):
            quality = 0.45 + (idx / 160.0)
            base_score = -0.20 + (idx / 100.0)
            future_return = (base_score * 0.08) + ((quality - 0.5) * 0.10)
            training_rows.append(
                {
                    "symbol": f"S{idx:03d}",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "ACCUMULATE" if idx % 3 else "HOLD",
                    "score": base_score,
                    "model_recommendation_score": base_score,
                    "execution_score": base_score * 0.8,
                    "analyst_recommendation_score": base_score * 0.5,
                    "market_sentiment_score": 0.10,
                    "data_quality_score": quality,
                    "source_coverage": 1.0 if idx % 4 else 0.72,
                    "missing_ratio": max(0.0, 0.40 - quality * 0.3),
                    "execution_ready": 1,
                    "future_return": future_return,
                    "outcome_ts": datetime.now(timezone.utc).isoformat(),
                }
            )
        model = train_investment_shadow_model(
            training_rows,
            cfg=InvestmentShadowModelConfig(min_samples=20, horizon_days=20, lookback_days=720),
        )
        self.assertTrue(bool(model.get("enabled", False)))
        self.assertEqual(str(model.get("model_version", "") or ""), "ridge_v2")
        scored_rows, summary = apply_investment_shadow_model(
            [
                {
                    "symbol": "HIGH",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "ACCUMULATE",
                    "score": 0.85,
                    "model_recommendation_score": 0.85,
                    "execution_score": 0.60,
                    "analyst_recommendation_score": 0.40,
                    "market_sentiment_score": 0.12,
                    "data_quality_score": 0.92,
                    "source_coverage": 1.0,
                    "missing_ratio": 0.05,
                    "execution_ready": 1,
                },
                {
                    "symbol": "LOW",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "WATCH",
                    "score": -0.10,
                    "model_recommendation_score": -0.10,
                    "execution_score": -0.08,
                    "analyst_recommendation_score": -0.10,
                    "market_sentiment_score": -0.05,
                    "data_quality_score": 0.42,
                    "source_coverage": 0.72,
                    "missing_ratio": 0.48,
                    "execution_ready": 0,
                },
            ],
            model=model,
            cfg=InvestmentShadowModelConfig(horizon_days=20),
        )
        self.assertTrue(bool(summary.get("enabled", False)))
        self.assertEqual(str(summary.get("model_version", "") or ""), "ridge_v2")
        self.assertEqual(len(scored_rows), 2)
        self.assertGreater(float(scored_rows[0]["shadow_ml_score"]), float(scored_rows[1]["shadow_ml_score"]))
        self.assertGreater(float(scored_rows[0]["shadow_ml_positive_prob"]), float(scored_rows[1]["shadow_ml_positive_prob"]))

    def test_shadow_ml_v2_can_read_extended_features_from_snapshot_details(self):
        now = datetime.now(timezone.utc)
        training_rows = []
        for idx in range(60):
            is_good = idx % 2 == 0
            training_rows.append(
                {
                    "symbol": f"S{idx:03d}",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "ACCUMULATE" if is_good else "HOLD",
                    "score": 0.30 if is_good else 0.05,
                    "model_recommendation_score": 0.30 if is_good else 0.05,
                    "execution_score": 0.18 if is_good else 0.02,
                    "analyst_recommendation_score": 0.10,
                    "market_sentiment_score": 0.08,
                    "data_quality_score": 0.88,
                    "source_coverage": 0.92,
                    "missing_ratio": 0.04,
                    "execution_ready": 1 if is_good else 0,
                    "future_return": 0.035 if is_good else -0.015,
                    "outcome_ts": (now - timedelta(days=idx)).isoformat(),
                    "details": json.dumps(
                        {
                            "expected_cost_bps": 12.0 if is_good else 42.0,
                            "liquidity_score": 0.82 if is_good else 0.35,
                            "avg_daily_dollar_volume": 8_000_000.0 if is_good else 500_000.0,
                            "microstructure_score": 0.42 if is_good else -0.30,
                            "micro_breakout_5m": 0.16 if is_good else -0.12,
                            "micro_reversal_5m": 0.05 if is_good else -0.08,
                            "micro_volume_burst_5m": 0.12 if is_good else -0.10,
                            "returns_ewma_vol_20d": 0.022 if is_good else 0.060,
                            "returns_downside_vol_20d": 0.015 if is_good else 0.052,
                        },
                        ensure_ascii=False,
                    ),
                }
            )
        model = train_investment_shadow_model(training_rows, cfg=InvestmentShadowModelConfig(min_samples=40))
        self.assertTrue(bool(model.get("enabled", False)))
        scored_rows, _summary = apply_investment_shadow_model(
            [
                {
                    "symbol": "GOOD",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "ACCUMULATE",
                    "score": 0.28,
                    "model_recommendation_score": 0.28,
                    "execution_score": 0.16,
                    "analyst_recommendation_score": 0.08,
                    "market_sentiment_score": 0.08,
                    "data_quality_score": 0.90,
                    "source_coverage": 0.95,
                    "missing_ratio": 0.03,
                    "execution_ready": 1,
                    "details": training_rows[0]["details"],
                },
                {
                    "symbol": "BAD",
                    "stage": "final",
                    "scan_tier": "final",
                    "action": "HOLD",
                    "score": 0.10,
                    "model_recommendation_score": 0.10,
                    "execution_score": 0.03,
                    "analyst_recommendation_score": 0.02,
                    "market_sentiment_score": 0.02,
                    "data_quality_score": 0.75,
                    "source_coverage": 0.80,
                    "missing_ratio": 0.08,
                    "execution_ready": 0,
                    "details": training_rows[1]["details"],
                },
            ],
            model=model,
            cfg=InvestmentShadowModelConfig(min_samples=40),
        )
        self.assertGreater(float(scored_rows[0]["shadow_ml_score"]), float(scored_rows[1]["shadow_ml_score"]))

    def test_weekly_feedback_penalty_downgrades_repeated_weak_signal(self):
        rows, summary = _apply_weekly_feedback_penalties(
            [
                {
                    "symbol": "0700.HK",
                    "score": 0.42,
                    "model_recommendation_score": 0.42,
                    "execution_score": 0.18,
                    "execution_ready": 1,
                    "action": "ACCUMULATE",
                    "mid_scale": 0.72,
                    "trend_vs_ma200": 0.11,
                    "rebalance_flag": 0,
                    "data_quality_score": 0.92,
                    "missing_ratio": 0.04,
                    "earnings_in_14d": False,
                    "signal_decision": {"gates_blocked": [], "reasons": [], "context": {}},
                },
                {
                    "symbol": "0005.HK",
                    "score": 0.38,
                    "model_recommendation_score": 0.38,
                    "execution_score": 0.16,
                    "execution_ready": 1,
                    "action": "ACCUMULATE",
                    "mid_scale": 0.70,
                    "trend_vs_ma200": 0.10,
                    "rebalance_flag": 0,
                    "data_quality_score": 0.90,
                    "missing_ratio": 0.05,
                    "earnings_in_14d": False,
                    "signal_decision": {"gates_blocked": [], "reasons": [], "context": {}},
                },
            ],
            scoring_cfg=InvestmentScoringConfig(accumulate_threshold=0.38, hold_threshold=0.10, execution_ready_threshold=0.08),
            weekly_feedback_cfg={
                "signal_penalties": [
                    {
                        "symbol": "0700.HK",
                        "score_penalty": 0.12,
                        "execution_penalty": 0.11,
                        "repeat_count": 2,
                        "cooldown_days": 14,
                        "reason": "repeat_shadow_weak_signal",
                    }
                ]
            },
        )
        penalized = next(row for row in rows if row["symbol"] == "0700.HK")
        untouched = next(row for row in rows if row["symbol"] == "0005.HK")
        self.assertTrue(bool(summary.get("enabled", False)))
        self.assertEqual(int(summary.get("applied_candidate_count", 0) or 0), 1)
        self.assertEqual(penalized["action"], "HOLD")
        self.assertEqual(int(penalized["execution_ready"]), 0)
        self.assertGreater(float(untouched["score"]), float(penalized["score"]))
        self.assertIn("weekly_feedback_signal", list(penalized["signal_decision"].get("gates_blocked", []) or []))

    def test_expected_cost_penalty_reduces_score_and_execution_score(self):
        long_row = {
            "symbol": "AAPL",
            "market": "US",
            "long_score": 0.22,
            "trend_vs_ma200": 0.08,
            "mdd_1y": -0.10,
            "rebalance_flag": 0,
            "last_close": 210.0,
        }
        mid_row = {
            "symbol": "AAPL",
            "market": "US",
            "mid_scale": 0.68,
            "trend_slope_60d": 0.06,
            "regime_composite": 0.10,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 210.0,
        }
        low_cost = score_investment_candidate(
            long_row,
            mid_row,
            vix=16.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            expected_cost_bps=8.0,
            spread_proxy_bps=2.0,
            slippage_proxy_bps=5.0,
            commission_proxy_bps=1.0,
            cfg=InvestmentScoringConfig(),
        )
        high_cost = score_investment_candidate(
            long_row,
            mid_row,
            vix=16.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            expected_cost_bps=78.0,
            spread_proxy_bps=24.0,
            slippage_proxy_bps=53.0,
            commission_proxy_bps=1.0,
            cfg=InvestmentScoringConfig(),
        )
        self.assertAlmostEqual(float(low_cost["score_before_cost"]), float(high_cost["score_before_cost"]), places=6)
        self.assertLess(float(high_cost["score"]), float(low_cost["score"]))
        self.assertLess(float(high_cost["execution_score"]), float(low_cost["execution_score"]))
        self.assertIn("high_expected_cost", list(high_cost["signal_decision"].get("gates_blocked", []) or []))

    def test_weekly_execution_hotspot_penalty_increases_cost_and_reduces_execution_readiness(self):
        rows, summary = _apply_weekly_feedback_penalties(
            [
                {
                    "symbol": "AAPL",
                    "score": 0.41,
                    "model_recommendation_score": 0.41,
                    "execution_score": 0.12,
                    "execution_ready": 1,
                    "action": "ACCUMULATE",
                    "mid_scale": 0.72,
                    "trend_vs_ma200": 0.11,
                    "rebalance_flag": 0,
                    "data_quality_score": 0.92,
                    "missing_ratio": 0.04,
                    "earnings_in_14d": False,
                    "expected_cost_bps": 18.0,
                    "slippage_proxy_bps": 12.0,
                    "cost_penalty": 0.018,
                    "execution_cost_penalty": 0.032,
                    "signal_decision": {"gates_blocked": [], "reasons": [], "context": {}},
                }
            ],
            scoring_cfg=InvestmentScoringConfig(accumulate_threshold=0.38, hold_threshold=0.10, execution_ready_threshold=0.08),
            weekly_feedback_cfg={
                "execution_penalties": [
                    {
                        "symbol": "AAPL",
                        "score_penalty": 0.01,
                        "execution_penalty": 0.03,
                        "expected_cost_bps_add": 8.0,
                        "slippage_proxy_bps_add": 6.0,
                        "session_count": 2,
                        "reason": "repeat_execution_hotspot",
                    }
                ]
            },
        )
        self.assertTrue(bool(summary.get("enabled", False)))
        penalized = rows[0]
        self.assertGreater(float(penalized["expected_cost_bps"]), 18.0)
        self.assertGreater(float(penalized["weekly_feedback_expected_cost_bps_add"]), 0.0)
        self.assertEqual(str(penalized["weekly_feedback_penalty_kind"]), "execution")
        self.assertLess(float(penalized["execution_score"]), 0.12)
        self.assertEqual(int(penalized["execution_ready"]), 0)

    def test_commodity_proxy_is_scored_neutrally_without_equity_fundamentals(self):
        long_row = {
            "symbol": "GLD",
            "market": "US",
            "long_score": 0.18,
            "trend_vs_ma200": 0.06,
            "mdd_1y": -0.10,
            "rebalance_flag": 0,
            "last_close": 220.0,
        }
        mid_row = {
            "symbol": "GLD",
            "market": "US",
            "mid_scale": 0.64,
            "trend_slope_60d": 0.08,
            "regime_composite": 0.12,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 220.0,
        }
        scored = score_investment_candidate(
            long_row,
            mid_row,
            vix=16.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={},
            recommendation={},
            cfg=InvestmentScoringConfig(),
        )
        self.assertEqual(scored["asset_class"], "commodity_proxy")
        self.assertEqual(scored["asset_theme"], "gold")
        self.assertEqual(scored["valuation_score"], 0.0)
        self.assertEqual(scored["margin_score"], 0.0)
        self.assertEqual(scored["operating_margin_score"], 0.0)
        self.assertEqual(scored["revenue_growth_score"], 0.0)
        self.assertEqual(scored["roe_score"], 0.0)

    def test_target_allocations_prioritize_accumulate(self):
        cfg = InvestmentPaperConfig(max_holdings=2, max_single_weight=0.7, min_position_weight=0.1)
        ranked = [
            {"symbol": "AAA", "score": 0.8},
            {"symbol": "BBB", "score": 0.6},
            {"symbol": "CCC", "score": 0.9},
        ]
        plans = [
            {"symbol": "AAA", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BBB", "action": "HOLD", "allocation_mult": 0.8},
            {"symbol": "CCC", "action": "WATCH", "allocation_mult": 1.0},
        ]
        weights = build_target_allocations(ranked, plans, cfg=cfg)
        self.assertIn("AAA", weights)
        self.assertIn("BBB", weights)
        self.assertNotIn("CCC", weights)
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=6)

    def test_target_allocations_keep_cash_when_breadth_is_too_narrow(self):
        cfg = InvestmentPaperConfig(max_holdings=8, max_single_weight=0.18, min_position_weight=0.04)
        ranked = [{"symbol": "RWE.DE", "score": 0.48}]
        plans = [{"symbol": "RWE.DE", "action": "ACCUMULATE", "allocation_mult": 0.76}]
        weights = build_target_allocations(ranked, plans, cfg=cfg)
        self.assertEqual(list(weights.keys()), ["RWE.DE"])
        self.assertAlmostEqual(weights["RWE.DE"], 0.18, places=6)
        self.assertAlmostEqual(sum(weights.values()), 0.18, places=6)

    def test_target_allocations_respect_sector_caps(self):
        cfg = InvestmentPaperConfig(
            max_holdings=4,
            max_single_weight=0.30,
            max_sector_weight=0.35,
            max_net_exposure=0.80,
            min_position_weight=0.05,
        )
        ranked = [
            {"symbol": "AAA", "score": 0.8, "sector": "Tech", "country": "US", "market": "US"},
            {"symbol": "BBB", "score": 0.7, "sector": "Tech", "country": "US", "market": "US"},
            {"symbol": "CCC", "score": 0.6, "sector": "Health", "country": "US", "market": "US"},
        ]
        plans = [
            {"symbol": "AAA", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BBB", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "CCC", "action": "ACCUMULATE", "allocation_mult": 1.0},
        ]
        weights = build_target_allocations(ranked, plans, cfg=cfg)
        self.assertLessEqual(weights.get("AAA", 0.0) + weights.get("BBB", 0.0), 0.35 + 1e-9)
        self.assertLessEqual(sum(weights.values()), 0.80 + 1e-9)

    def test_target_allocations_apply_dynamic_risk_overlay_for_crowded_portfolio(self):
        cfg = InvestmentPaperConfig(
            max_holdings=3,
            max_single_weight=0.32,
            max_net_exposure=0.90,
            max_gross_exposure=0.90,
            min_position_weight=0.05,
            stress_loss_soft_limit=0.05,
        )
        ranked = [
            {
                "symbol": "AAA",
                "score": 0.82,
                "sector": "Tech",
                "industry": "Semis",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.11,
                "mdd_1y": -0.24,
                "liquidity_score": 0.18,
                "expected_cost_bps": 52.0,
                "market_sentiment_score": -0.28,
                "data_quality_score": 0.62,
            },
            {
                "symbol": "BBB",
                "score": 0.79,
                "sector": "Tech",
                "industry": "Semis",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.10,
                "mdd_1y": -0.22,
                "liquidity_score": 0.20,
                "expected_cost_bps": 48.0,
                "market_sentiment_score": -0.24,
                "data_quality_score": 0.64,
            },
            {
                "symbol": "CCC",
                "score": 0.75,
                "sector": "Tech",
                "industry": "Software",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.09,
                "mdd_1y": -0.18,
                "liquidity_score": 0.22,
                "expected_cost_bps": 44.0,
                "market_sentiment_score": -0.20,
                "data_quality_score": 0.66,
            },
        ]
        plans = [
            {"symbol": "AAA", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BBB", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "CCC", "action": "ACCUMULATE", "allocation_mult": 1.0},
        ]
        weights, risk = build_target_allocations(ranked, plans, cfg=cfg, return_details=True)
        self.assertTrue(risk["enabled"])
        self.assertGreater(float(risk["avg_pair_correlation"]), float(cfg.correlation_soft_limit))
        self.assertLess(float(risk["dynamic_net_exposure"]), float(cfg.max_net_exposure))
        self.assertLessEqual(
            sum(abs(float(weight)) for weight in weights.values()),
            float(risk["dynamic_gross_exposure"]) + 1e-9,
        )
        self.assertTrue(list(risk.get("layered_throttles", []) or []))
        self.assertGreater(float(risk.get("throttle_gross_tightening", 0.0) or 0.0), 0.0)
        self.assertTrue(str(risk.get("layered_throttle_text", "") or ""))
        self.assertTrue(risk["correlation_reduced_symbols"])
        self.assertIn(str(risk["final_stress_worst_scenario"]), {"index_drop", "volatility_spike", "liquidity_shock"})

    def test_target_allocations_apply_market_profile_budget_and_recovery(self):
        cfg = InvestmentPaperConfig(
            max_holdings=3,
            max_single_weight=0.30,
            max_net_exposure=0.90,
            max_gross_exposure=0.90,
            min_position_weight=0.05,
            correlation_soft_limit=0.34,
            market_profile_net_exposure_budget=0.78,
            market_profile_gross_exposure_budget=0.80,
            market_profile_short_exposure_budget=0.10,
            dynamic_recovery_max_bonus=0.08,
        )
        ranked = [
            {
                "symbol": "AAA",
                "score": 0.82,
                "sector": "Technology",
                "industry": "Software",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.03,
                "mdd_1y": -0.08,
                "liquidity_score": 0.90,
                "expected_cost_bps": 12.0,
                "market_sentiment_score": 0.18,
                "data_quality_score": 0.94,
            },
            {
                "symbol": "BBB",
                "score": 0.79,
                "sector": "Healthcare",
                "industry": "Pharma",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.028,
                "mdd_1y": -0.07,
                "liquidity_score": 0.88,
                "expected_cost_bps": 11.0,
                "market_sentiment_score": 0.16,
                "data_quality_score": 0.92,
            },
            {
                "symbol": "CCC",
                "score": 0.76,
                "sector": "Industrials",
                "industry": "Rail",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.026,
                "mdd_1y": -0.06,
                "liquidity_score": 0.86,
                "expected_cost_bps": 10.0,
                "market_sentiment_score": 0.15,
                "data_quality_score": 0.93,
            },
        ]
        plans = [
            {"symbol": "AAA", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BBB", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "CCC", "action": "ACCUMULATE", "allocation_mult": 1.0},
        ]
        weights, risk = build_target_allocations(ranked, plans, cfg=cfg, return_details=True)
        self.assertTrue(weights)
        self.assertAlmostEqual(float(risk["market_profile_net_exposure_budget"]), 0.78, places=6)
        self.assertAlmostEqual(float(risk["market_profile_gross_exposure_budget"]), 0.80, places=6)
        self.assertGreater(float(risk["market_profile_budget_tightening_net"]), 0.0)
        self.assertLess(
            float(risk["throttle_pre_recovery_net_exposure"]),
            float(risk["market_profile_net_exposure_budget"]),
        )
        self.assertTrue(bool(risk["recovery_active"]))
        self.assertGreater(float(risk["recovery_net_credit"]), 0.0)
        self.assertGreater(
            float(risk["dynamic_net_exposure"]),
            float(risk["throttle_pre_recovery_net_exposure"]),
        )
        self.assertTrue(str(risk["dominant_throttle_layer"]))
        self.assertIn("recovery", str(risk["layered_throttle_text"]))

    def test_target_allocations_keep_dynamic_scale_high_for_diversified_portfolio(self):
        cfg = InvestmentPaperConfig(
            max_holdings=3,
            max_single_weight=0.28,
            max_net_exposure=0.88,
            max_gross_exposure=0.88,
            min_position_weight=0.05,
        )
        ranked = [
            {
                "symbol": "AAPL",
                "score": 0.82,
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.03,
                "mdd_1y": -0.08,
                "liquidity_score": 0.88,
                "expected_cost_bps": 11.0,
                "market_sentiment_score": 0.24,
                "data_quality_score": 0.93,
            },
            {
                "symbol": "BHP.AX",
                "score": 0.77,
                "sector": "Materials",
                "industry": "Metals",
                "country": "AU",
                "market": "ASX",
                "direction": "LONG",
                "atr_pct": 0.02,
                "mdd_1y": -0.06,
                "liquidity_score": 0.80,
                "expected_cost_bps": 12.0,
                "market_sentiment_score": 0.18,
                "data_quality_score": 0.90,
            },
            {
                "symbol": "0005.HK",
                "score": 0.74,
                "sector": "Financials",
                "industry": "Banks",
                "country": "HK",
                "market": "HK",
                "direction": "LONG",
                "atr_pct": 0.025,
                "mdd_1y": -0.07,
                "liquidity_score": 0.79,
                "expected_cost_bps": 13.0,
                "market_sentiment_score": 0.16,
                "data_quality_score": 0.91,
            },
        ]
        plans = [
            {"symbol": "AAPL", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BHP.AX", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "0005.HK", "action": "ACCUMULATE", "allocation_mult": 1.0},
        ]
        weights, risk = build_target_allocations(ranked, plans, cfg=cfg, return_details=True)
        self.assertGreater(float(risk["dynamic_scale"]), 0.90)
        self.assertLess(float(risk["avg_pair_correlation"]), float(cfg.correlation_soft_limit))
        self.assertAlmostEqual(
            float(risk["dynamic_net_exposure"]),
            float(cfg.max_net_exposure),
            delta=0.08,
        )
        self.assertTrue(weights)

    def test_target_allocations_prefers_returns_based_risk_metrics_when_history_exists(self):
        cfg = InvestmentPaperConfig(
            max_holdings=3,
            max_single_weight=0.30,
            max_net_exposure=0.90,
            max_gross_exposure=0.90,
            min_position_weight=0.05,
        )
        shared_returns = json.dumps([0.01, -0.02, 0.015, -0.01, 0.012] * 8, ensure_ascii=False)
        ranked = [
            {
                "symbol": "AAA",
                "score": 0.82,
                "sector": "Tech",
                "industry": "Semis",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.03,
                "mdd_1y": -0.10,
                "liquidity_score": 0.80,
                "expected_cost_bps": 12.0,
                "market_sentiment_score": 0.10,
                "data_quality_score": 0.92,
                "return_series_60d_json": shared_returns,
            },
            {
                "symbol": "BBB",
                "score": 0.79,
                "sector": "Health",
                "industry": "Pharma",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.025,
                "mdd_1y": -0.09,
                "liquidity_score": 0.82,
                "expected_cost_bps": 13.0,
                "market_sentiment_score": 0.12,
                "data_quality_score": 0.91,
                "return_series_60d_json": shared_returns,
            },
            {
                "symbol": "CCC",
                "score": 0.76,
                "sector": "Industrials",
                "industry": "Rail",
                "country": "US",
                "market": "US",
                "direction": "LONG",
                "atr_pct": 0.022,
                "mdd_1y": -0.08,
                "liquidity_score": 0.84,
                "expected_cost_bps": 11.0,
                "market_sentiment_score": 0.11,
                "data_quality_score": 0.93,
                "return_series_60d_json": shared_returns,
            },
        ]
        plans = [
            {"symbol": "AAA", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "BBB", "action": "ACCUMULATE", "allocation_mult": 1.0},
            {"symbol": "CCC", "action": "ACCUMULATE", "allocation_mult": 1.0},
        ]
        _weights, risk = build_target_allocations(ranked, plans, cfg=cfg, return_details=True)
        self.assertTrue(bool(risk.get("returns_based_enabled", False)))
        self.assertGreaterEqual(int(risk.get("returns_based_symbol_count", 0) or 0), 3)
        self.assertEqual(str(risk.get("correlation_source", "") or ""), "returns+proxy_fallback")
        self.assertGreater(float(risk.get("avg_pair_correlation", 0.0) or 0.0), 0.90)
        self.assertGreater(float(risk.get("returns_based_var_95_1d", 0.0) or 0.0), 0.0)

    def test_rebalance_due_logic(self):
        now = datetime(2026, 3, 13, tzinfo=timezone.utc)  # Friday
        self.assertTrue(is_rebalance_due("", now, frequency="weekly", rebalance_weekday=4))
        self.assertFalse(
            is_rebalance_due("2026-03-10T00:00:00+00:00", now, frequency="weekly", rebalance_weekday=3)
        )

    def test_simulate_rebalance_creates_trades(self):
        positions = {"OLD": {"qty": 10.0, "cost_basis": 100.0, "last_price": 100.0}}
        target_weights = {"NEW": 1.0}
        updated, trades, cash_after, equity_after = simulate_rebalance(
            positions,
            cash=1000.0,
            price_map={"OLD": 100.0, "NEW": 50.0},
            target_weights=target_weights,
        )
        self.assertTrue(any(t["action"] == "SELL" for t in trades))
        self.assertTrue(any(t["action"] == "BUY" for t in trades))
        self.assertGreater(equity_after, 0.0)
        self.assertIn("NEW", updated)

    def test_simulate_rebalance_supports_fractional_qty(self):
        cfg = InvestmentPaperConfig(allow_fractional_qty=True, fractional_qty_decimals=3)
        updated, trades, cash_after, equity_after = simulate_rebalance(
            {},
            cash=1000.0,
            price_map={"GLD": 200.0},
            target_weights={"GLD": 0.333},
            cfg=cfg,
        )
        buy = next(t for t in trades if t["action"] == "BUY")
        self.assertAlmostEqual(buy["qty"], 1.665, places=3)
        self.assertIn("GLD", updated)
        self.assertGreater(equity_after, 0.0)

    def test_simulate_rebalance_supports_short_targets(self):
        updated, trades, cash_after, equity_after = simulate_rebalance(
            {},
            cash=1000.0,
            price_map={"TSLA": 50.0},
            target_weights={"TSLA": -0.50},
        )
        self.assertTrue(any(t["symbol"] == "TSLA" and t["action"] == "SELL" for t in trades))
        self.assertLess(updated["TSLA"]["qty"], 0.0)
        self.assertAlmostEqual(equity_after, 1000.0, places=6)
        self.assertGreater(cash_after, 1000.0)

    def test_build_investment_rebalance_orders_sells_then_buys(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.05,
            cash_buffer_floor=500.0,
            min_trade_value=200.0,
            max_order_value_pct=1.0,
            weight_tolerance=0.01,
            max_orders_per_run=4,
            account_allocation_pct=1.0,
        )
        orders = build_investment_rebalance_orders(
            {
                "OLD": {"qty": 10.0, "market_price": 100.0},
                "KEEP": {"qty": 2.0, "market_price": 50.0},
            },
            price_map={"OLD": 100.0, "KEEP": 50.0, "NEW": 25.0},
            target_weights={"KEEP": 0.25, "NEW": 0.50},
            broker_equity=2000.0,
            broker_cash=900.0,
            cfg=cfg,
            lot_size_map={},
        )
        self.assertTrue(any(row["symbol"] == "OLD" and row["action"] == "SELL" for row in orders))

    def test_build_investment_rebalance_orders_support_fractional_qty(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.0,
            cash_buffer_floor=0.0,
            min_trade_value=50.0,
            max_order_value_pct=1.0,
            weight_tolerance=0.0,
            max_orders_per_run=4,
            account_allocation_pct=1.0,
            allow_fractional_qty=True,
            fractional_qty_decimals=3,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"GLD": 200.0},
            target_weights={"GLD": 0.25},
            broker_equity=1000.0,
            broker_cash=1000.0,
            cfg=cfg,
            lot_size_map={},
        )
        buy = next(row for row in orders if row["symbol"] == "GLD" and row["action"] == "BUY")
        self.assertAlmostEqual(float(buy["delta_qty"]), 1.25, places=3)
        self.assertEqual(buy["action"], "BUY")

    def test_build_investment_rebalance_orders_can_open_short(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.0,
            cash_buffer_floor=0.0,
            min_trade_value=100.0,
            max_order_value_pct=1.0,
            weight_tolerance=0.0,
            max_orders_per_run=4,
            account_allocation_pct=1.0,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"TSLA": 50.0},
            target_weights={"TSLA": -0.25},
            broker_equity=1000.0,
            broker_cash=1000.0,
            cfg=cfg,
            lot_size_map={},
        )
        short_order = next(row for row in orders if row["symbol"] == "TSLA" and row["action"] == "SELL")
        self.assertEqual(float(short_order["delta_qty"]), 5.0)
        self.assertLess(float(short_order["target_qty"]), 0.0)
        self.assertLess(float(short_order["target_weight"]), 0.0)

    def test_build_investment_rebalance_orders_prioritize_cost_adjusted_alpha_for_entries(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.0,
            cash_buffer_floor=0.0,
            min_trade_value=100.0,
            max_order_value_pct=1.0,
            max_orders_per_run=1,
            account_allocation_pct=1.0,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"AAA": 100.0, "BBB": 100.0},
            target_weights={"AAA": 0.50, "BBB": 0.50},
            broker_equity=10000.0,
            broker_cash=10000.0,
            cfg=cfg,
            lot_size_map={},
            priority_context_map={
                "AAA": {
                    "score": 0.62,
                    "execution_score": 0.20,
                    "liquidity_score": 0.70,
                    "expected_cost_bps": 12.0,
                },
                "BBB": {
                    "score": 0.66,
                    "execution_score": 0.12,
                    "liquidity_score": 0.20,
                    "expected_cost_bps": 135.0,
                },
            },
        )
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["symbol"], "AAA")
        self.assertGreater(float(orders[0]["priority_score"]), 0.0)

    def test_build_investment_rebalance_orders_carry_expected_edge_metrics(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.0,
            cash_buffer_floor=0.0,
            min_trade_value=100.0,
            max_order_value_pct=1.0,
            max_orders_per_run=1,
            account_allocation_pct=1.0,
            edge_score_to_bps_scale=150.0,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"AAA": 100.0},
            target_weights={"AAA": 0.50},
            broker_equity=10000.0,
            broker_cash=10000.0,
            cfg=cfg,
            lot_size_map={},
            priority_context_map={
                "AAA": {
                    "score": 0.62,
                    "score_before_cost": 0.66,
                    "execution_score": 0.20,
                    "liquidity_score": 0.70,
                    "expected_edge_threshold": 0.35,
                    "expected_edge_score": 0.31,
                    "expected_cost_bps": 12.0,
                },
            },
        )
        self.assertEqual(len(orders), 1)
        self.assertAlmostEqual(float(orders[0]["expected_edge_threshold"]), 0.35, places=6)
        self.assertAlmostEqual(float(orders[0]["expected_edge_score"]), 0.31, places=6)
        self.assertAlmostEqual(float(orders[0]["expected_edge_bps"]), 46.5, places=6)

    def test_build_investment_rebalance_orders_respects_cash_buffer_without_zeroing_target(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.08,
            cash_buffer_floor=1500.0,
            min_trade_value=1000.0,
            max_order_value_pct=0.10,
            account_allocation_pct=0.08,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"AAA": 20.0},
            target_weights={"AAA": 1.0},
            broker_equity=100000.0,
            broker_cash=100000.0,
            cfg=cfg,
            lot_size_map={},
        )
        self.assertTrue(orders)
        self.assertEqual(orders[0]["action"], "BUY")
        self.assertGreater(orders[0]["order_value"], 0.0)

    def test_build_investment_rebalance_orders_allows_single_min_lot_override(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.08,
            cash_buffer_floor=1500.0,
            min_trade_value=2000.0,
            max_order_value_pct=0.01,
            account_allocation_pct=0.03,
            allow_min_lot_buy_override=True,
            min_lot_buy_override_value_pct=0.03,
        )
        orders = build_investment_rebalance_orders(
            {},
            price_map={"0883.HK": 29.38},
            target_weights={"0883.HK": 1.0},
            broker_equity=1_001_212.67,
            broker_cash=1_000_347.44,
            cfg=cfg,
            lot_size_map={"0883.HK": 1000},
        )
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["symbol"], "0883.HK")
        self.assertEqual(orders[0]["action"], "BUY")
        self.assertEqual(int(orders[0]["delta_qty"]), 1000)
        self.assertEqual(orders[0]["reason"], "rebalance_up_min_lot_override")

    def test_build_investment_rebalance_orders_allows_single_min_lot_sell_override(self):
        cfg = InvestmentExecutionConfig(
            min_cash_buffer_pct=0.08,
            cash_buffer_floor=1500.0,
            min_trade_value=2000.0,
            max_order_value_pct=0.01,
            account_allocation_pct=0.03,
            allow_min_lot_sell_override=True,
            min_lot_sell_override_value_pct=0.03,
        )
        orders = build_investment_rebalance_orders(
            {"0883.HK": {"qty": 1000.0, "market_price": 29.22}},
            price_map={"0883.HK": 29.22},
            target_weights={},
            broker_equity=1_001_231.98,
            broker_cash=995_114.05,
            cfg=cfg,
            lot_size_map={"0883.HK": 1000},
        )
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["symbol"], "0883.HK")
        self.assertEqual(orders[0]["action"], "SELL")
        self.assertEqual(int(orders[0]["delta_qty"]), 1000)
        self.assertEqual(orders[0]["reason"], "rebalance_down_min_lot_override")

    def test_build_investment_guard_orders_triggers_stop_and_take_profit(self):
        execution_cfg = InvestmentExecutionConfig(
            min_trade_value=500.0,
            lot_size=1,
            allow_min_lot_sell_override=True,
            min_lot_sell_override_value_pct=0.05,
        )
        guard_cfg = InvestmentGuardConfig(
            stop_loss_pct=0.08,
            trailing_stop_pct=0.06,
            trailing_stop_min_gain_pct=0.08,
            take_profit_pct=0.18,
            take_profit_pullback_pct=0.03,
            trim_fraction=0.5,
            max_actions_per_run=4,
            min_trade_value=500.0,
        )
        orders = build_investment_guard_orders(
            {
                "AAA": {"qty": 100.0, "avg_cost": 100.0, "market_price": 88.0},
                "BBB": {"qty": 90.0, "avg_cost": 100.0, "market_price": 122.0},
            },
            metrics_by_symbol={
                "AAA": {"ref_price": 88.0, "recent_high": 103.0, "atr": 2.0},
                "BBB": {"ref_price": 122.0, "recent_high": 127.0, "atr": 2.5},
            },
            broker_equity=100000.0,
            execution_cfg=execution_cfg,
            guard_cfg=guard_cfg,
            lot_size_map={},
        )
        self.assertEqual(len(orders), 2)
        self.assertEqual(orders[0]["symbol"], "AAA")
        self.assertIn("stop", orders[0]["reason"])
        self.assertEqual(orders[1]["symbol"], "BBB")
        self.assertEqual(orders[1]["reason"], "guard_take_profit_trim")

    def test_market_data_adapter_falls_back_to_yfinance_5m(self):
        md = Mock()
        md.get_5m_bars.side_effect = RuntimeError("ibkr unavailable")
        adapter = MarketDataAdapter(md)
        yf_bars = _bars(n=12, start=50.0, step=0.1)[-12:]
        with patch("src.data.adapters.fetch_intraday_bars_yf", return_value=yf_bars):
            bars, source = adapter.get_5m_bars_with_source("RWE.DE", need=8, fallback_days=5)
        self.assertEqual(source, "yfinance_5m")
        self.assertEqual(len(bars), 8)
        self.assertAlmostEqual(float(bars[-1].close), float(yf_bars[-1].close), places=6)

    def test_market_data_adapter_falls_back_when_sync_path_receives_awaitable(self):
        async def _async_ib_response():
            return []

        md = Mock()
        md.get_daily_bars.return_value = _async_ib_response()
        adapter = MarketDataAdapter(md)
        yf_bars = _bars(n=6, start=80.0, step=0.2)
        with patch("src.data.adapters.fetch_daily_bars_yf", return_value=yf_bars):
            bars, source = adapter.get_daily_bars("BHP.AX", days=30)
        self.assertEqual(source, "yfinance")
        self.assertEqual(len(bars), len(yf_bars))
        self.assertAlmostEqual(float(bars[-1].close), float(yf_bars[-1].close), places=6)

    def test_market_data_adapter_falls_back_to_yfinance_in_worker_thread_without_ib_sync_warning_path(self):
        class _FakeIB:
            def __init__(self):
                self.calls = 0

            def reqHistoricalData(self, **kwargs):
                self.calls += 1
                return []

        ib = _FakeIB()
        md = MarketDataService(ib=ib)
        md.register("BHP.AX", make_stock_contract("BHP", default_exchange="SMART", default_currency="AUD"))
        adapter = MarketDataAdapter(md)
        yf_bars = _bars(n=6, start=80.0, step=0.2)
        result: dict[str, object] = {}

        def _worker() -> None:
            bars, source = adapter.get_daily_bars("BHP.AX", days=30)
            result["bars"] = bars
            result["source"] = source

        with patch("src.data.adapters.fetch_daily_bars_yf", return_value=yf_bars):
            thread = threading.Thread(target=_worker, name="history-worker-no-loop")
            thread.start()
            thread.join(timeout=5)

        self.assertEqual(result.get("source"), "yfinance")
        self.assertEqual(len(result.get("bars", [])), len(yf_bars))
        self.assertEqual(ib.calls, 0)

    def test_collect_symbol_feature_results_prefetches_history_on_owner_thread(self):
        owner_thread_id = threading.get_ident()
        seen_thread_ids: list[int] = []

        class _FakeAdapter:
            def get_daily_bars(self, symbol: str, days: int):
                seen_thread_ids.append(threading.get_ident())
                return _bars(n=max(300, days), start=50.0, step=0.1), "ibkr"

        results = _collect_symbol_feature_results(
            ["AAA", "BBB"],
            data_adapter=_FakeAdapter(),
            market="US",
            shared_days=260,
            mid_lookback_days=180,
            long_years=1,
            regime_cfg=RegimeConfig(),
            history_workers=2,
            progress_interval=10,
            owner_thread_history=True,
        )

        self.assertEqual(len(results), 2)
        self.assertTrue(seen_thread_ids)
        self.assertTrue(all(thread_id == owner_thread_id for thread_id in seen_thread_ids))
        self.assertTrue(all(str(row.get("history_source") or "") == "ibkr" for row in results))
        self.assertGreater(float(results[0]["quality_metrics"]["expected_cost_bps"]), 0.0)

    def test_investment_execution_engine_requires_real_account_id(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return []

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="UXXXXXXX",
                storage=Storage(tmp.name),
                market="HK",
                portfolio_id="HK:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(),
            )
            with self.assertRaises(ValueError):
                engine._account_snapshot()

    def test_investment_execution_engine_allows_empty_positions_for_valid_account(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            account = "DUQ152001"
            tag = "NetLiquidation"
            value = "100000"

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [SummaryRow()]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="HK",
                portfolio_id="HK:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(),
            )
            self.assertEqual(engine._broker_positions(), {})

    def test_investment_execution_engine_reuses_cached_account_snapshot(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def __init__(self):
                self.summary_calls = 0

            def accountSummary(self, *args, **kwargs):
                self.summary_calls += 1
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "25000"),
                    SummaryRow("DUQ152001", "BuyingPower", "50000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

        with NamedTemporaryFile(suffix=".db") as tmp:
            fake_ib = FakeIB()
            storage = Storage(tmp.name)
            cfg = InvestmentExecutionConfig(account_snapshot_ttl_sec=900)
            engine = InvestmentExecutionEngine(
                ib=fake_ib,
                account_id="DUQ152001",
                storage=storage,
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=cfg,
            )
            first = engine._account_snapshot()
            second = engine._account_snapshot()
            self.assertEqual(fake_ib.summary_calls, 1)
            self.assertEqual(first["netliq"], 100000.0)
            self.assertEqual(second["cash"], 25000.0)

    def test_investment_execution_engine_filters_positions_by_market(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            account = "DUQ152001"
            tag = "NetLiquidation"
            value = "100000"

        class Contract:
            def __init__(self, symbol: str, exchange: str, currency: str):
                self.symbol = symbol
                self.exchange = exchange
                self.currency = currency

        class PortfolioRow:
            def __init__(self, account: str, symbol: str, exchange: str, currency: str, position: float):
                self.account = account
                self.contract = Contract(symbol, exchange, currency)
                self.position = position
                self.averageCost = 10.0
                self.marketPrice = 10.0
                self.marketValue = position * 10.0

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [SummaryRow()]

            def portfolio(self, *args, **kwargs):
                return [
                    PortfolioRow("DUQ152001", "883", "SEHK", "HKD", 1000.0),
                    PortfolioRow("DUQ152001", "AAPL", "SMART", "USD", 10.0),
                ]

            def positions(self, *args, **kwargs):
                return []

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(),
            )
            positions = engine._broker_positions()
            self.assertIn("AAPL", positions)
            self.assertNotIn("0883.HK", positions)

    def test_investment_execution_engine_blocks_buy_when_opportunity_waits(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="XETRA",
                portfolio_id="XETRA:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(),
            )
            report_dir = Path(td)
            (report_dir / "investment_opportunity_scan.csv").write_text(
                "\n".join(
                    [
                        "symbol,entry_status,entry_reason",
                        "RWE.DE,WAIT_EVENT,wait for earnings",
                    ]
                ),
                encoding="utf-8",
            )
            allowed, blocked = engine._apply_opportunity_gates(
                report_dir,
                [
                    {
                        "symbol": "RWE.DE",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 100.0,
                        "delta_qty": 100.0,
                        "ref_price": 55.0,
                        "target_weight": 0.18,
                        "order_value": 5500.0,
                        "reason": "rebalance_up",
                    }
                ],
            )
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "BLOCKED_OPPORTUNITY")
            self.assertEqual(blocked[0]["opportunity_status"], "WAIT_EVENT")
            self.assertIn("opportunity_reason", blocked[0])

    def test_investment_execution_engine_blocks_buy_when_quality_is_too_low(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    min_model_recommendation_score=0.15,
                    min_execution_score=0.05,
                    require_execution_ready=True,
                ),
            )
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,score,model_recommendation_score,execution_score,execution_ready",
                        "AAPL,0.02,0.02,0.01,0",
                    ]
                ),
                encoding="utf-8",
            )
            allowed, blocked = engine._apply_quality_gates(
                report_dir,
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 10.0,
                        "delta_qty": 10.0,
                        "ref_price": 180.0,
                        "target_weight": 0.10,
                        "order_value": 1800.0,
                        "reason": "rebalance_up",
                    }
                ],
            )
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "BLOCKED_QUALITY")
            self.assertEqual(blocked[0]["quality_status"], "LOW_QUALITY")
            self.assertIn("execution_not_ready", blocked[0]["quality_reason"])

    def test_investment_execution_engine_blocks_short_entry_when_short_execution_not_allowed(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="ASX",
                portfolio_id="ASX:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    min_model_recommendation_score=0.15,
                    min_execution_score=0.05,
                    require_execution_ready=True,
                ),
            )
            report_dir = Path(td)
            (report_dir / "investment_short_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,direction,score,model_recommendation_score,execution_score,execution_ready,short_execution_allowed",
                        "BHP.AX,SHORT,0.62,0.62,0.22,0,0",
                    ]
                ),
                encoding="utf-8",
            )
            allowed, blocked = engine._apply_quality_gates(
                report_dir,
                [
                    {
                        "symbol": "BHP.AX",
                        "action": "SELL",
                        "current_qty": 0.0,
                        "target_qty": -100.0,
                        "delta_qty": 100.0,
                        "ref_price": 45.0,
                        "target_weight": -0.15,
                        "order_value": 4500.0,
                        "reason": "rebalance_add_short",
                    }
                ],
            )
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "BLOCKED_QUALITY")
            self.assertIn("short_execution_not_allowed", blocked[0]["quality_reason"])

    def test_investment_execution_engine_routes_large_order_to_manual_review(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    manual_review_enabled=True,
                    manual_review_order_value_pct=0.10,
                ),
            )
            allowed, blocked = engine._apply_manual_review_gates(
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 80.0,
                        "delta_qty": 80.0,
                        "ref_price": 200.0,
                        "target_weight": 0.16,
                        "order_value": 16000.0,
                        "reason": "rebalance_up",
                    }
                ],
                broker_equity=100000.0,
            )
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "REVIEW_REQUIRED")
            self.assertEqual(blocked[0]["manual_review_status"], "REVIEW_REQUIRED")
            self.assertIn("exceeds auto-submit threshold", blocked[0]["manual_review_reason"])
            self.assertEqual(blocked[0]["user_reason_label"], "大额订单待人工确认")
            self.assertIn("自动提交阈值", blocked[0]["user_reason"])

    def test_investment_execution_engine_routes_low_shadow_ml_order_to_manual_review(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as report_dir:
            report_path = Path(report_dir)
            (report_path / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,score,model_recommendation_score,execution_score,execution_ready,shadow_ml_enabled,shadow_ml_score,shadow_ml_positive_prob,shadow_ml_training_samples",
                        "AAPL,ACCUMULATE,0.62,0.62,0.31,1,1,-0.08,0.44,120",
                    ]
                ),
                encoding="utf-8",
            )
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    shadow_ml_review_enabled=True,
                    shadow_ml_min_score_auto_submit=0.00,
                    shadow_ml_min_positive_prob_auto_submit=0.50,
                    shadow_ml_min_training_samples=80,
                ),
            )
            allowed, blocked = engine._apply_shadow_ml_review_gates(
                report_path,
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 50.0,
                        "delta_qty": 50.0,
                        "ref_price": 200.0,
                        "target_weight": 0.10,
                        "order_value": 10000.0,
                        "reason": "rebalance_up",
                    }
                ],
            )
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "REVIEW_REQUIRED")
            self.assertEqual(blocked[0]["manual_review_status"], "REVIEW_REQUIRED")
            self.assertEqual(blocked[0]["shadow_review_status"], "REVIEW_REQUIRED")
            self.assertIn("shadow ML burn-in requires review", blocked[0]["manual_review_reason"])
            self.assertEqual(blocked[0]["user_reason_label"], "模型保护期复核")
            self.assertIn("模型仍在保护期", blocked[0]["user_reason"])
            self.assertIn("shadow_ml_review", blocked[0]["reason"])

    def test_investment_execution_engine_defers_hotspot_symbol_in_open_session(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    execution_hotspot_penalties=[
                        {
                            "symbol": "AAPL",
                            "execution_penalty": 0.05,
                            "expected_cost_bps_add": 12.0,
                            "slippage_proxy_bps_add": 8.0,
                            "session_count": 2,
                            "session_labels": "开盘,尾盘",
                            "reason": "repeat_execution_hotspot",
                        }
                    ]
                ),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="OPEN",
                    session_label="开盘",
                    execution_style="TWAP_LITE_OPEN",
                    aggressiveness=0.72,
                    participation_scale=0.70,
                    limit_buffer_scale=1.25,
                )
            )

            allowed, blocked = engine._apply_execution_hotspot_gates(
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 40.0,
                        "delta_qty": 40.0,
                        "ref_price": 200.0,
                        "target_weight": 0.08,
                        "order_value": 8000.0,
                        "reason": "rebalance_up",
                    }
                ]
            )

            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "DEFERRED_EXECUTION_HOTSPOT")
            self.assertEqual(blocked[0]["hotspot_penalty_status"], "DEFERRED")
            self.assertIn("开盘存在重复执行热点", blocked[0]["hotspot_penalty_reason"])
            self.assertIn("execution_hotspot_defer", blocked[0]["reason"])

    def test_investment_execution_engine_defers_orders_during_portfolio_risk_alert_in_open_session(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp:
            storage = Storage(tmp.name)
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-old",
                    ts="2026-03-18T01:00:00+00:00",
                    market="US",
                    portfolio_id="US:test",
                    source_kind="execution",
                    source_label="执行",
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
                    run_id="exec-new",
                    ts=datetime.now(timezone.utc).isoformat(),
                    market="US",
                    portfolio_id="US:test",
                    source_kind="execution",
                    source_label="执行",
                    risk_overlay={
                        "dynamic_scale": 0.74,
                        "dynamic_net_exposure": 0.68,
                        "dynamic_gross_exposure": 0.76,
                        "avg_pair_correlation": 0.66,
                        "stress_worst_loss": 0.089,
                        "stress_worst_scenario_label": "流动性恶化",
                    },
                )
            )
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=storage,
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    risk_alert_guard_enabled=True,
                    risk_alert_manual_review_order_value_pct=0.20,
                ),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="OPEN",
                    session_label="开盘",
                    execution_style="TWAP_LITE_OPEN",
                    aggressiveness=0.72,
                    participation_scale=0.70,
                    limit_buffer_scale=1.25,
                )
            )

            allowed, blocked, summary = engine._apply_portfolio_risk_alert_gates(
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 40.0,
                        "delta_qty": 40.0,
                        "ref_price": 200.0,
                        "target_weight": 0.08,
                        "order_value": 8000.0,
                        "reason": "rebalance_up",
                    }
                ],
                broker_equity=100000.0,
            )

            self.assertEqual(summary["alert_level"], "ALERT")
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "DEFERRED_RISK_ALERT")
            self.assertEqual(blocked[0]["risk_alert_status"], "DEFERRED")
            self.assertIn("风险告警期间", blocked[0]["risk_alert_reason"])
            self.assertIn("risk_alert_defer", blocked[0]["reason"])

    def test_investment_execution_engine_slows_orders_during_portfolio_risk_watch_midday(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp:
            storage = Storage(tmp.name)
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-old",
                    ts="2026-03-18T01:00:00+00:00",
                    market="US",
                    portfolio_id="US:test",
                    source_kind="execution",
                    source_label="执行",
                    risk_overlay={
                        "dynamic_scale": 0.90,
                        "dynamic_net_exposure": 0.84,
                        "dynamic_gross_exposure": 0.92,
                        "avg_pair_correlation": 0.50,
                        "stress_worst_loss": 0.058,
                        "stress_worst_scenario_label": "指数下跌",
                    },
                )
            )
            storage.insert_investment_risk_history(
                build_investment_risk_history_row(
                    run_id="exec-new",
                    ts=datetime.now(timezone.utc).isoformat(),
                    market="US",
                    portfolio_id="US:test",
                    source_kind="execution",
                    source_label="执行",
                    risk_overlay={
                        "dynamic_scale": 0.84,
                        "dynamic_net_exposure": 0.80,
                        "dynamic_gross_exposure": 0.88,
                        "avg_pair_correlation": 0.56,
                        "stress_worst_loss": 0.071,
                        "stress_worst_scenario_label": "波动抬升",
                    },
                )
            )
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=storage,
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    order_type="MKT",
                    adv_max_participation_pct=0.05,
                    adv_split_trigger_pct=0.02,
                    max_slices_per_symbol=4,
                    risk_alert_guard_enabled=True,
                    risk_alert_force_min_slices_alert=2,
                ),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            allowed, blocked, summary = engine._apply_portfolio_risk_alert_gates(
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 50.0,
                        "delta_qty": 50.0,
                        "ref_price": 100.0,
                        "target_weight": 0.10,
                        "order_value": 5000.0,
                        "avg_daily_dollar_volume": 100000.0,
                        "avg_daily_volume": 1000.0,
                        "expected_cost_bps": 18.0,
                        "spread_proxy_bps": 4.0,
                        "slippage_proxy_bps": 12.0,
                        "commission_proxy_bps": 2.0,
                        "reason": "rebalance_up",
                    }
                ],
                broker_equity=100000.0,
            )
            self.assertEqual(summary["alert_level"], "WATCH")
            self.assertEqual(len(allowed), 1)
            self.assertEqual(blocked, [])
            self.assertEqual(allowed[0]["risk_alert_status"], "SLOWED")

            split_rows, liquidity_blocked = engine._split_execution_orders(allowed)
            self.assertEqual(liquidity_blocked, [])
            self.assertGreaterEqual(len(split_rows), 2)
            self.assertTrue(all(str(row.get("execution_order_type") or "") == "LMT" for row in split_rows))
            self.assertTrue(all(str(row.get("risk_alert_status") or "") == "SLOWED" for row in split_rows))

    def test_investment_execution_engine_slows_hotspot_symbol_and_forces_extra_slices_midday(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    order_type="MKT",
                    adv_max_participation_pct=0.05,
                    adv_split_trigger_pct=0.02,
                    max_slices_per_symbol=4,
                    execution_hotspot_penalties=[
                        {
                            "symbol": "AAPL",
                            "execution_penalty": 0.04,
                            "expected_cost_bps_add": 9.0,
                            "slippage_proxy_bps_add": 7.0,
                            "session_count": 3,
                            "session_labels": "开盘,午盘,尾盘",
                            "reason": "repeat_execution_hotspot",
                        }
                    ],
                ),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            allowed, blocked = engine._apply_execution_hotspot_gates(
                [
                    {
                        "symbol": "AAPL",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 50.0,
                        "delta_qty": 50.0,
                        "ref_price": 100.0,
                        "target_weight": 0.10,
                        "order_value": 5000.0,
                        "avg_daily_dollar_volume": 100000.0,
                        "avg_daily_volume": 1000.0,
                        "expected_cost_bps": 18.0,
                        "spread_proxy_bps": 4.0,
                        "slippage_proxy_bps": 12.0,
                        "commission_proxy_bps": 2.0,
                        "reason": "rebalance_up",
                    }
                ]
            )
            self.assertEqual(len(allowed), 1)
            self.assertEqual(blocked, [])
            self.assertEqual(allowed[0]["hotspot_penalty_status"], "SLOWED")

            split_rows, liquidity_blocked = engine._split_execution_orders(allowed)
            self.assertEqual(liquidity_blocked, [])
            self.assertEqual(len(split_rows), 4)
            self.assertTrue(all(str(row.get("execution_order_type") or "") == "LMT" for row in split_rows))
            self.assertTrue(all(str(row.get("hotspot_penalty_status") or "") == "SLOWED" for row in split_rows))
            self.assertTrue(all(float(row.get("limit_price_buffer_bps_effective") or 0.0) > 10.2 for row in split_rows))
            self.assertEqual(sorted(int(float(row.get("slice_index") or 0)) for row in split_rows), [1, 2, 3, 4])

    def test_investment_execution_summary_reports_target_capacity_and_idle_gap(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "100000"),
                    SummaryRow("DUQ152001", "BuyingPower", "200000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

            def sleep(self, *_args, **_kwargs):
                return None

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,last_close,score,model_recommendation_score,execution_score,execution_ready,direction,market",
                        "AAPL,100,0.70,0.70,0.30,1,LONG,US",
                        "MSFT,100,0.68,0.68,0.28,1,LONG,US",
                        "NVDA,100,0.66,0.66,0.27,1,LONG,US",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult,direction,execution_ready",
                        "AAPL,ACCUMULATE,1.0,LONG,1",
                        "MSFT,ACCUMULATE,1.0,LONG,1",
                        "NVDA,ACCUMULATE,1.0,LONG,1",
                    ]
                ),
                encoding="utf-8",
            )

            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(max_holdings=3, max_single_weight=0.22),
                execution_cfg=InvestmentExecutionConfig(
                    min_cash_buffer_pct=0.0,
                    cash_buffer_floor=0.0,
                    min_trade_value=100.0,
                    max_order_value_pct=0.08,
                    max_orders_per_run=2,
                    account_allocation_pct=0.30,
                    manual_review_enabled=True,
                    manual_review_order_value_pct=0.10,
                ),
            )
            engine.run(report_dir=str(report_dir), submit=False)
            summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))

            self.assertIn("target_capital", summary)
            self.assertIn("theoretical_execution_capacity", summary)
            self.assertIn("planned_order_value", summary)
            self.assertIn("idle_capital_gap", summary)
            self.assertIn("risk_dynamic_net_exposure", summary)
            self.assertIn("risk_dynamic_gross_exposure", summary)
            self.assertIn("risk_avg_pair_correlation", summary)
            self.assertIn("risk_stress_worst_scenario_label", summary)
            self.assertGreater(float(summary["target_capital"]), 0.0)
            self.assertLessEqual(
                float(summary["theoretical_execution_capacity"]),
                float(summary["target_capital"]) + 1e-9,
            )
            self.assertLessEqual(
                float(summary["planned_order_value"]),
                float(summary["theoretical_execution_capacity"]) + 1e-9,
            )
            self.assertAlmostEqual(
                float(summary["idle_capital_gap"]),
                float(summary["target_capital"]) - float(summary["planned_order_value"]),
                places=6,
            )

    def test_investment_execution_engine_splits_orders_with_adv_cap_and_session_style(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "100000"),
                    SummaryRow("DUQ152001", "BuyingPower", "200000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

            def sleep(self, *_args, **_kwargs):
                return None

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,last_close,score,score_before_cost,model_recommendation_score,execution_score,execution_ready,direction,market,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score",
                        "AAPL,100,0.74,0.78,0.74,0.33,1,LONG,US,100000,1000,18,4,12,2,0.72",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult,direction,execution_ready,score,score_before_cost,model_recommendation_score,execution_score,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score",
                        "AAPL,ACCUMULATE,1.0,LONG,1,0.74,0.78,0.74,0.33,100000,1000,18,4,12,2,0.72",
                    ]
                ),
                encoding="utf-8",
            )

            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(max_holdings=1, max_single_weight=0.30),
                execution_cfg=InvestmentExecutionConfig(
                    min_cash_buffer_pct=0.0,
                    cash_buffer_floor=0.0,
                    min_trade_value=100.0,
                    max_order_value_pct=0.08,
                    max_orders_per_run=4,
                    account_allocation_pct=0.30,
                    adv_max_participation_pct=0.05,
                    adv_split_trigger_pct=0.02,
                    max_slices_per_symbol=4,
                    edge_gate_enabled=False,
                    manual_review_enabled=True,
                    manual_review_order_value_pct=0.10,
                ),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            engine.run(report_dir=str(report_dir), submit=False)
            summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))
            plan_rows = list(csv.DictReader((report_dir / "investment_execution_plan.csv").open("r", encoding="utf-8", newline="")))

            order_rows = [row for row in plan_rows if str(row.get("status") or "") == "PLANNED"]
            self.assertEqual(int(summary["parent_order_count"]), 1)
            self.assertEqual(int(summary["order_count"]), 3)
            self.assertEqual(int(summary["split_order_count"]), 2)
            self.assertEqual(int(summary["adv_capped_order_count"]), 1)
            self.assertEqual(str(summary["execution_style"]), "VWAP_LITE_MIDDAY")
            self.assertGreater(float(summary["planned_execution_cost_total"]), 0.0)
            self.assertEqual(len(order_rows), 3)
            self.assertTrue(all(str(row.get("execution_style") or "") == "VWAP_LITE_MIDDAY" for row in order_rows))
            self.assertTrue(all(str(row.get("execution_order_type") or "") == "LMT" for row in order_rows))
            self.assertEqual(sorted(int(float(row.get("slice_index") or 0)) for row in order_rows), [1, 2, 3])
            self.assertTrue(all(float(row.get("expected_cost_bps") or 0.0) > 0.0 for row in order_rows))

    def test_investment_execution_engine_blocks_entries_when_edge_is_below_cost_buffer(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "100000"),
                    SummaryRow("DUQ152001", "BuyingPower", "200000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

            def sleep(self, *_args, **_kwargs):
                return None

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,last_close,score,score_before_cost,model_recommendation_score,execution_score,execution_ready,direction,market,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score",
                        "AAPL,100,0.36,0.38,0.36,0.20,1,LONG,US,100000,1000,18,4,12,2,0.72",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult,direction,execution_ready,score,score_before_cost,model_recommendation_score,execution_score,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score,expected_edge_threshold,expected_edge_score",
                        "AAPL,ACCUMULATE,1.0,LONG,1,0.36,0.38,0.36,0.20,100000,1000,18,4,12,2,0.72,0.35,0.03",
                    ]
                ),
                encoding="utf-8",
            )

            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(max_holdings=1, max_single_weight=0.30),
                execution_cfg=InvestmentExecutionConfig(
                    min_cash_buffer_pct=0.0,
                    cash_buffer_floor=0.0,
                    min_trade_value=100.0,
                    max_order_value_pct=0.08,
                    max_orders_per_run=4,
                    account_allocation_pct=0.30,
                    edge_gate_enabled=True,
                    min_expected_edge_bps=18.0,
                    edge_cost_buffer_bps=6.0,
                    edge_score_to_bps_scale=140.0,
                    manual_review_enabled=True,
                    manual_review_order_value_pct=0.10,
                ),
            )

            engine.run(report_dir=str(report_dir), submit=False)
            summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))
            plan_rows = list(csv.DictReader((report_dir / "investment_execution_plan.csv").open("r", encoding="utf-8", newline="")))

            self.assertEqual(int(summary["blocked_edge_order_count"]), 1)
            self.assertEqual(int(summary["order_count"]), 0)
            self.assertEqual(len(plan_rows), 1)
            self.assertEqual(str(plan_rows[0]["status"]), "BLOCKED_EDGE")
            self.assertEqual(str(plan_rows[0]["user_reason_label"]), "边际收益不够覆盖成本")
            self.assertIn("expected_edge", str(plan_rows[0]["edge_gate_reason"]))

    def test_investment_execution_engine_uses_market_profile_edge_override_from_report(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "100000"),
                    SummaryRow("DUQ152001", "BuyingPower", "200000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

            def sleep(self, *_args, **_kwargs):
                return None

        from src.common.market_structure import load_market_structure

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,last_close,score,score_before_cost,model_recommendation_score,execution_score,execution_ready,direction,market,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score",
                        "AAPL,100,0.43,0.45,0.43,0.20,1,LONG,US,25000000,250000,18,4,12,2,0.80",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult,direction,execution_ready,score,score_before_cost,model_recommendation_score,execution_score,avg_daily_dollar_volume,avg_daily_volume,expected_cost_bps,spread_proxy_bps,slippage_proxy_bps,commission_proxy_bps,liquidity_score,expected_edge_threshold,expected_edge_score",
                        "AAPL,ACCUMULATE,1.0,LONG,1,0.43,0.45,0.43,0.20,25000000,250000,18,4,12,2,0.80,0.35,0.18",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_adaptive_strategy_summary.json").write_text(
                json.dumps(
                    {
                        "adaptive_strategy": {"name": "ACM-RS", "summary_text": "test"},
                        "summary": {"enabled": True},
                        "active_market_execution": {
                            "profile_key": "US",
                            "overrides": {
                                "min_expected_edge_bps": 16.0,
                                "edge_cost_buffer_bps": 2.0,
                            },
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="US",
                portfolio_id="US:test",
                paper_cfg=InvestmentPaperConfig(max_holdings=1, max_single_weight=0.30),
                execution_cfg=InvestmentExecutionConfig(
                    min_cash_buffer_pct=0.0,
                    cash_buffer_floor=0.0,
                    min_trade_value=100.0,
                    max_order_value_pct=0.08,
                    max_orders_per_run=4,
                    account_allocation_pct=0.30,
                    edge_gate_enabled=True,
                    min_expected_edge_bps=18.0,
                    edge_cost_buffer_bps=6.0,
                    edge_score_to_bps_scale=140.0,
                    manual_review_enabled=False,
                    shadow_ml_review_enabled=False,
                    risk_alert_guard_enabled=False,
                ),
                market_structure=load_market_structure(Path("."), "US"),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            engine.run(report_dir=str(report_dir), submit=False)
            summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))
            plan_rows = list(csv.DictReader((report_dir / "investment_execution_plan.csv").open("r", encoding="utf-8", newline="")))

            self.assertEqual(int(summary["blocked_edge_order_count"]), 0)
            self.assertGreater(int(summary["order_count"]), 0)
            self.assertEqual(str(summary["adaptive_strategy_active_market_execution"]["profile_key"]), "US")
            self.assertEqual(str(plan_rows[0]["status"]), "PLANNED")

    def test_investment_execution_engine_dynamic_market_rules_raise_edge_threshold(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        from src.common.market_structure import load_market_structure

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="HK",
                portfolio_id="HK:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    edge_gate_enabled=True,
                    min_expected_edge_bps=18.0,
                    edge_cost_buffer_bps=2.0,
                    edge_score_to_bps_scale=140.0,
                ),
                market_structure=load_market_structure(Path("."), "HK"),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            market_rule_allowed, market_rule_blocked = engine._apply_market_rule_gates(
                [
                    {
                        "symbol": "0700.HK",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 100.0,
                        "delta_qty": 100.0,
                        "ref_price": 100.0,
                        "target_weight": 0.10,
                        "order_value": 10000.0,
                        "lot_size": 100.0,
                        "avg_daily_dollar_volume": 500000.0,
                        "avg_daily_volume": 5000.0,
                        "expected_cost_bps": 18.0,
                        "spread_proxy_bps": 4.0,
                        "slippage_proxy_bps": 12.0,
                        "commission_proxy_bps": 2.0,
                        "liquidity_score": 0.25,
                        "score_before_cost": 0.50,
                        "expected_edge_threshold": 0.35,
                        "expected_edge_score": 0.15,
                        "reason": "rebalance_up",
                    }
                ]
            )
            self.assertEqual(market_rule_blocked, [])
            self.assertEqual(len(market_rule_allowed), 1)

            allowed, blocked = engine._apply_expected_edge_gates(market_rule_allowed)
            self.assertEqual(allowed, [])
            self.assertEqual(len(blocked), 1)
            self.assertEqual(blocked[0]["status"], "BLOCKED_EDGE")
            self.assertGreater(float(blocked[0]["edge_gate_dynamic_floor_bps"]), 18.0)
            self.assertGreater(float(blocked[0]["edge_gate_dynamic_buffer_bps"]), 2.0)
            self.assertGreater(float(blocked[0]["edge_gate_threshold_bps"]), 20.0)
            self.assertIn("floor=", str(blocked[0]["edge_gate_reason"]))

    def test_investment_execution_engine_dynamic_market_rules_force_limit_slicing(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

        from src.common.market_structure import load_market_structure

        with NamedTemporaryFile(suffix=".db") as tmp:
            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="HK",
                portfolio_id="HK:test",
                paper_cfg=InvestmentPaperConfig(),
                execution_cfg=InvestmentExecutionConfig(
                    order_type="MKT",
                    adv_max_participation_pct=0.05,
                    adv_split_trigger_pct=0.02,
                    max_slices_per_symbol=4,
                    limit_price_buffer_bps=6.0,
                    edge_gate_enabled=False,
                ),
                market_structure=load_market_structure(Path("."), "HK"),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            market_rule_allowed, market_rule_blocked = engine._apply_market_rule_gates(
                [
                    {
                        "symbol": "0700.HK",
                        "action": "BUY",
                        "current_qty": 0.0,
                        "target_qty": 3000.0,
                        "delta_qty": 3000.0,
                        "ref_price": 100.0,
                        "target_weight": 0.30,
                        "order_value": 300000.0,
                        "lot_size": 100.0,
                        "avg_daily_dollar_volume": 5000000.0,
                        "avg_daily_volume": 50000.0,
                        "expected_cost_bps": 20.0,
                        "spread_proxy_bps": 4.0,
                        "slippage_proxy_bps": 14.0,
                        "commission_proxy_bps": 2.0,
                        "liquidity_score": 0.55,
                        "reason": "rebalance_up",
                    }
                ]
            )
            self.assertEqual(market_rule_blocked, [])
            self.assertEqual(len(market_rule_allowed), 1)

            split_rows, liquidity_blocked = engine._split_execution_orders(market_rule_allowed)
            self.assertEqual(liquidity_blocked, [])
            self.assertEqual(len(split_rows), 3)
            self.assertTrue(all(str(row.get("execution_order_type") or "") == "LMT" for row in split_rows))
            self.assertTrue(all(str(row.get("dynamic_liquidity_bucket") or "") == "CORE" for row in split_rows))
            self.assertTrue(all(int(float(row.get("slice_count") or 0)) == 3 for row in split_rows))
            self.assertEqual(sorted(int(float(row.get("slice_index") or 0)) for row in split_rows), [1, 2, 3])
            self.assertTrue(all(float(row.get("limit_price_buffer_bps_effective") or 0.0) > 5.0 for row in split_rows))
            self.assertTrue(all(float(row.get("adv_cap_order_value") or 0.0) < 300000.0 for row in split_rows))

    def test_investment_execution_run_reports_market_rule_blocks(self):
        class DummyEvent:
            def __iadd__(self, other):
                return self

        class SummaryRow:
            def __init__(self, account: str, tag: str, value: str):
                self.account = account
                self.tag = tag
                self.value = value

        class FakeIB:
            orderStatusEvent = DummyEvent()
            errorEvent = DummyEvent()
            execDetailsEvent = DummyEvent()
            commissionReportEvent = DummyEvent()

            def accountSummary(self, *args, **kwargs):
                return [
                    SummaryRow("DUQ152001", "NetLiquidation", "100000"),
                    SummaryRow("DUQ152001", "TotalCashValue", "100000"),
                    SummaryRow("DUQ152001", "BuyingPower", "200000"),
                ]

            def portfolio(self, *args, **kwargs):
                return []

            def positions(self, *args, **kwargs):
                return []

            def sleep(self, *_args, **_kwargs):
                return None

        from src.common.market_structure import load_market_structure

        with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as td:
            report_dir = Path(td)
            (report_dir / "investment_candidates.csv").write_text(
                "\n".join(
                    [
                        "symbol,last_close,score,model_recommendation_score,execution_score,execution_ready,direction,market",
                        "510300.SS,4.00,0.62,0.62,0.31,1,LONG,CN",
                    ]
                ),
                encoding="utf-8",
            )
            (report_dir / "investment_plan.csv").write_text(
                "\n".join(
                    [
                        "symbol,action,allocation_mult,direction,execution_ready",
                        "510300.SS,ACCUMULATE,1.0,LONG,1",
                    ]
                ),
                encoding="utf-8",
            )

            engine = InvestmentExecutionEngine(
                ib=FakeIB(),
                account_id="DUQ152001",
                storage=Storage(tmp.name),
                market="CN",
                portfolio_id="CN:test",
                paper_cfg=InvestmentPaperConfig(max_holdings=1, max_single_weight=0.30),
                execution_cfg=InvestmentExecutionConfig(
                    edge_gate_enabled=False,
                    manual_review_enabled=False,
                    shadow_ml_review_enabled=False,
                    risk_alert_guard_enabled=False,
                ),
                market_structure=load_market_structure(Path("."), "CN"),
            )
            engine._current_execution_session_profile = Mock(
                return_value=ExecutionSessionProfile(
                    session_bucket="MIDDAY",
                    session_label="午盘",
                    execution_style="VWAP_LITE_MIDDAY",
                    aggressiveness=0.55,
                    participation_scale=1.0,
                    limit_buffer_scale=0.85,
                )
            )

            engine.run(report_dir=str(report_dir), submit=False)
            summary = json.loads((report_dir / "investment_execution_summary.json").read_text(encoding="utf-8"))
            plan_rows = list(csv.DictReader((report_dir / "investment_execution_plan.csv").open("r", encoding="utf-8", newline="")))

            self.assertEqual(int(summary["blocked_market_rule_order_count"]), 1)
            self.assertEqual(int(summary["order_count"]), 0)
            self.assertEqual(len(plan_rows), 1)
            self.assertEqual(str(plan_rows[0]["status"]), "BLOCKED_MARKET_RULE")
            self.assertEqual(str(plan_rows[0]["market_rule_status"]), "BLOCKED_RESEARCH_ONLY")
            self.assertEqual(str(plan_rows[0]["user_reason_label"]), "当前市场仅研究")

    def test_investment_signal_decision_is_structured(self):
        long_row = {
            "symbol": "AAPL",
            "long_score": 0.35,
            "trend_vs_ma200": 0.12,
            "mdd_1y": -0.05,
            "rebalance_flag": 0,
            "last_close": 180.0,
            "market": "US",
        }
        mid_row = {
            "symbol": "AAPL",
            "mid_scale": 0.70,
            "trend_slope_60d": 0.10,
            "regime_composite": 0.25,
            "regime_state": "RISK_ON",
            "regime_reason": "test",
            "risk_on": True,
            "last_close": 180.0,
            "market": "US",
            "regime_state_v2": {"state": "RISK_ON", "risk_budget_scale": 0.7},
        }
        row = score_investment_candidate(
            long_row,
            mid_row,
            vix=14.0,
            earnings_in_14d=False,
            macro_high_risk=False,
            fundamentals={"profit_margin": 0.18, "operating_margin": 0.20, "revenue_growth": 0.08, "roe": 0.18, "market": "US"},
        )
        self.assertIn("signal_decision", row)
        self.assertEqual(row["signal_decision"]["symbol"], "AAPL")
        self.assertEqual(row["signal_decision"]["regime_state"]["state"], "RISK_ON")


if __name__ == "__main__":
    unittest.main()
