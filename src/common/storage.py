import sqlite3
from typing import Any, Dict, List
from datetime import datetime, timedelta, timezone
import json


def build_investment_risk_history_row(
    *,
    run_id: str,
    market: str,
    portfolio_id: str,
    source_kind: str,
    risk_overlay: Dict[str, Any] | None,
    ts: str = "",
    source_label: str = "",
    report_dir: str = "",
    account_id: str = "",
    details: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """把 risk_overlay 规整成可查询的一行，避免后续只能从 details JSON 里反复回推。"""
    risk = dict(risk_overlay or {})
    source_kind_norm = str(source_kind or "").strip().lower() or "paper"
    stress_scenarios = dict(risk.get("final_stress_scenarios", {}) or risk.get("stress_scenarios", {}) or {})
    notes = [str(item).strip() for item in list(risk.get("notes", []) or []) if str(item).strip()]
    correlation_reduced = [
        str(item).strip()
        for item in list(risk.get("correlation_reduced_symbols", []) or [])
        if str(item).strip()
    ]
    details_payload = dict(details or {})
    if report_dir and not str(details_payload.get("report_dir") or "").strip():
        details_payload["report_dir"] = str(report_dir)
    if account_id and not str(details_payload.get("account_id") or "").strip():
        details_payload["account_id"] = str(account_id)
    if risk and "risk_overlay" not in details_payload:
        details_payload["risk_overlay"] = risk
    return {
        "run_id": str(run_id or "").strip(),
        "ts": str(ts or datetime.utcnow().isoformat()),
        "market": str(market or "").upper(),
        "portfolio_id": str(portfolio_id or "").strip(),
        "source_kind": source_kind_norm,
        "source_label": str(source_label or ("执行" if source_kind_norm == "execution" else "Dry Run")),
        "report_dir": str(report_dir or "").strip(),
        "account_id": str(account_id or "").strip(),
        "dynamic_scale": float(risk.get("dynamic_scale", 1.0) or 1.0),
        "dynamic_net_exposure": float(risk.get("dynamic_net_exposure", 0.0) or 0.0),
        "dynamic_gross_exposure": float(risk.get("dynamic_gross_exposure", 0.0) or 0.0),
        "dynamic_short_exposure": float(risk.get("dynamic_short_exposure", 0.0) or 0.0),
        "applied_net_exposure": float(risk.get("applied_net_exposure", 0.0) or 0.0),
        "applied_gross_exposure": float(risk.get("applied_gross_exposure", 0.0) or 0.0),
        "avg_pair_correlation": float(
            risk.get("final_avg_pair_correlation", risk.get("avg_pair_correlation", 0.0)) or 0.0
        ),
        "max_pair_correlation": float(
            risk.get("final_max_pair_correlation", risk.get("max_pair_correlation", 0.0)) or 0.0
        ),
        "top_sector_share": float(risk.get("top_sector_share", 0.0) or 0.0),
        "stress_index_drop_loss": float(stress_scenarios.get("index_drop", {}).get("loss", 0.0) or 0.0),
        "stress_volatility_spike_loss": float(stress_scenarios.get("volatility_spike", {}).get("loss", 0.0) or 0.0),
        "stress_liquidity_shock_loss": float(stress_scenarios.get("liquidity_shock", {}).get("loss", 0.0) or 0.0),
        "stress_worst_loss": float(risk.get("final_stress_worst_loss", risk.get("stress_worst_loss", 0.0)) or 0.0),
        "stress_worst_scenario": str(
            risk.get("final_stress_worst_scenario", risk.get("stress_worst_scenario", "")) or ""
        ),
        "stress_worst_scenario_label": str(
            risk.get("final_stress_worst_scenario_label", risk.get("stress_worst_scenario_label", "")) or ""
        ),
        "notes_json": notes,
        "correlation_reduced_symbols_json": correlation_reduced,
        "stress_scenarios_json": stress_scenarios,
        "details": details_payload,
    }


class Storage:
    """SQLite-backed audit/risk event store used by execution and risk modules."""

    def __init__(self, db_path: str = "audit.db"):
        self.db_path = db_path
        self._init_db()

    def _conn(self):
        # Keep connections short-lived; context managers commit/rollback automatically.
        return sqlite3.connect(self.db_path)

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column_def: str) -> None:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
        except Exception:
            pass

    def _init_db(self):
        with self._conn() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                account_id TEXT,
                symbol TEXT,
                exchange TEXT,
                currency TEXT,
                action TEXT,
                qty REAL,
                order_type TEXT,
                order_id INTEGER,
                parent_id INTEGER,
                status TEXT,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                order_id INTEGER,
                exec_id TEXT,
                symbol TEXT,
                action TEXT,
                qty REAL,
                price REAL,
                pnl REAL,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                kind TEXT,
                value REAL,
                details TEXT
            )""")
            # Phase1: per-bar signal audit
            c.execute("""
            CREATE TABLE IF NOT EXISTS signals_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                symbol TEXT,
                bar_end_time TEXT,
                o REAL,
                h REAL,
                l REAL,
                c REAL,
                v REAL,
                last3_close TEXT,
                range20 REAL,
                mr_sig REAL,
                bo_sig REAL,
                short_sig REAL,
                mid_scale REAL,
                total_sig REAL,
                threshold REAL,
                should_trade INTEGER,
                action TEXT,
                reason TEXT
            )""")
            # Phase1: per-day market data quality summary
            c.execute("""
            CREATE TABLE IF NOT EXISTS md_quality (
                day TEXT,
                symbol TEXT,
                buckets INTEGER,
                duplicates INTEGER,
                max_gap_sec INTEGER,
                last_end_time TEXT,
                updated_ts TEXT,
                PRIMARY KEY (day, symbol)
            )""")
            # Phase1-B+: persistent market-data blacklist / cooldown (per symbol)
            c.execute("""
            CREATE TABLE IF NOT EXISTS md_blacklist (
                symbol TEXT PRIMARY KEY,
                status TEXT,
                reason TEXT,
                until_ts INTEGER,
                updated_ts TEXT
            )""")
            
            # ---- Phase1-C: forward-compatible columns (best-effort) ----
            self._ensure_column(c, "signals_audit", "channel TEXT")
            self._ensure_column(c, "signals_audit", "can_trade_short INTEGER")
            self._ensure_column(c, "signals_audit", "risk_gate TEXT")
            self._ensure_column(c, "signals_audit", "atr_stop REAL")
            self._ensure_column(c, "signals_audit", "slippage_bps REAL")
            self._ensure_column(c, "signals_audit", "gap_addon_pct REAL")
            self._ensure_column(c, "signals_audit", "liquidity_haircut REAL")
            self._ensure_column(c, "signals_audit", "event_risk TEXT")
            self._ensure_column(c, "signals_audit", "event_risk_reason TEXT")
            self._ensure_column(c, "signals_audit", "short_borrow_fee_bps REAL")
            self._ensure_column(c, "signals_audit", "short_borrow_source TEXT")
            self._ensure_column(c, "signals_audit", "risk_allowed INTEGER")
            self._ensure_column(c, "signals_audit", "block_reasons TEXT")
            self._ensure_column(c, "signals_audit", "risk_snapshot_json TEXT")
            self._ensure_column(c, "signals_audit", "regime_state_v2_json TEXT")
            self._ensure_column(c, "signals_audit", "signal_decision_json TEXT")
            self._ensure_column(c, "signals_audit", "risk_decision_json TEXT")

            self._ensure_column(c, "fills", "expected_price REAL")
            self._ensure_column(c, "fills", "expected_slippage_bps REAL")
            self._ensure_column(c, "fills", "actual_slippage_bps REAL")
            self._ensure_column(c, "fills", "slippage_bps_deviation REAL")
            self._ensure_column(c, "fills", "event_risk_reason TEXT")
            self._ensure_column(c, "fills", "short_borrow_source TEXT")
            self._ensure_column(c, "fills", "risk_snapshot_json TEXT")
            self._ensure_column(c, "fills", "portfolio_id TEXT")
            self._ensure_column(c, "fills", "system_kind TEXT")
            self._ensure_column(c, "fills", "execution_run_id TEXT")
            self._ensure_column(c, "fills", "order_submit_ts TEXT")
            self._ensure_column(c, "fills", "fill_delay_seconds REAL")

            self._ensure_column(c, "risk_events", "symbol TEXT")
            self._ensure_column(c, "risk_events", "order_id INTEGER")
            self._ensure_column(c, "risk_events", "exec_id TEXT")
            self._ensure_column(c, "risk_events", "expected_price REAL")
            self._ensure_column(c, "risk_events", "actual_price REAL")
            self._ensure_column(c, "risk_events", "expected_slippage_bps REAL")
            self._ensure_column(c, "risk_events", "actual_slippage_bps REAL")
            self._ensure_column(c, "risk_events", "slippage_bps_deviation REAL")
            self._ensure_column(c, "risk_events", "event_risk_reason TEXT")
            self._ensure_column(c, "risk_events", "short_borrow_source TEXT")
            self._ensure_column(c, "risk_events", "risk_snapshot_json TEXT")
            self._ensure_column(c, "risk_events", "portfolio_id TEXT")
            self._ensure_column(c, "risk_events", "system_kind TEXT")
            self._ensure_column(c, "risk_events", "execution_run_id TEXT")
            self._ensure_column(c, "orders", "portfolio_id TEXT")
            self._ensure_column(c, "orders", "system_kind TEXT")
            self._ensure_column(c, "orders", "execution_run_id TEXT")
            self._ensure_column(c, "orders", "execution_intent_json TEXT")
            c.execute("""
            CREATE TABLE IF NOT EXISTS regime_state (
                market TEXT PRIMARY KEY,
                ts TEXT,
                regime_state TEXT,
                snapshot_json TEXT,
                adapted_cfg_json TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                report_dir TEXT,
                rebalance_due INTEGER,
                executed INTEGER,
                cash_before REAL,
                cash_after REAL,
                equity_before REAL,
                equity_after REAL,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                qty REAL,
                cost_basis REAL,
                last_price REAL,
                market_value REAL,
                weight REAL,
                status TEXT,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                action TEXT,
                qty REAL,
                price REAL,
                trade_value REAL,
                reason TEXT,
                details TEXT
            )""")
            self._ensure_column(c, "investment_runs", "portfolio_id TEXT")
            self._ensure_column(c, "investment_positions", "portfolio_id TEXT")
            self._ensure_column(c, "investment_trades", "portfolio_id TEXT")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_execution_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                account_id TEXT,
                report_dir TEXT,
                submitted INTEGER,
                order_count INTEGER,
                order_value REAL,
                broker_equity REAL,
                broker_cash REAL,
                target_equity REAL,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_risk_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                source_kind TEXT,
                source_label TEXT,
                report_dir TEXT,
                account_id TEXT,
                dynamic_scale REAL,
                dynamic_net_exposure REAL,
                dynamic_gross_exposure REAL,
                dynamic_short_exposure REAL,
                applied_net_exposure REAL,
                applied_gross_exposure REAL,
                avg_pair_correlation REAL,
                max_pair_correlation REAL,
                top_sector_share REAL,
                stress_index_drop_loss REAL,
                stress_volatility_spike_loss REAL,
                stress_liquidity_shock_loss REAL,
                stress_worst_loss REAL,
                stress_worst_scenario TEXT,
                stress_worst_scenario_label TEXT,
                notes_json TEXT,
                correlation_reduced_symbols_json TEXT,
                stress_scenarios_json TEXT,
                details TEXT
            )""")
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_risk_history_lookup "
                "ON investment_risk_history (market, portfolio_id, source_kind, ts DESC)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_risk_history_run "
                "ON investment_risk_history (run_id, source_kind)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_feedback_automation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                feedback_kind TEXT,
                feedback_kind_label TEXT,
                feedback_action TEXT,
                calibration_apply_mode TEXT,
                calibration_apply_mode_label TEXT,
                calibration_basis TEXT,
                calibration_basis_label TEXT,
                feedback_base_confidence REAL,
                feedback_calibration_score REAL,
                feedback_confidence REAL,
                feedback_sample_count INTEGER,
                feedback_calibration_sample_count INTEGER,
                outcome_maturity_ratio REAL,
                outcome_maturity_label TEXT,
                outcome_pending_sample_count INTEGER,
                outcome_ready_estimate_end_ts TEXT,
                alert_bucket TEXT,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_feedback_automation_history_key "
                "ON investment_feedback_automation_history (week_label, portfolio_id, feedback_kind)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_feedback_automation_history_lookup "
                "ON investment_feedback_automation_history (market, portfolio_id, feedback_kind, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_feedback_threshold_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                feedback_kind TEXT,
                feedback_kind_label TEXT,
                suggestion_action TEXT,
                suggestion_label TEXT,
                summary_signal TEXT,
                tracked_count INTEGER,
                avg_active_weeks REAL,
                base_auto_confidence REAL,
                suggested_auto_confidence REAL,
                base_auto_base_confidence REAL,
                suggested_auto_base_confidence REAL,
                base_auto_calibration_score REAL,
                suggested_auto_calibration_score REAL,
                base_auto_maturity_ratio REAL,
                suggested_auto_maturity_ratio REAL,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_feedback_threshold_history_key "
                "ON investment_feedback_threshold_history (week_label, market, feedback_kind)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_feedback_threshold_history_lookup "
                "ON investment_feedback_threshold_history (market, feedback_kind, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_market_profile_patch_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                profile TEXT,
                tuning_target TEXT,
                tuning_action TEXT,
                tuning_bias TEXT,
                review_required INTEGER,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_market_profile_patch_history_key "
                "ON investment_market_profile_patch_history (week_label, portfolio_id)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_market_profile_patch_history_lookup "
                "ON investment_market_profile_patch_history (market, portfolio_id, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_weekly_tuning_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                active_market_profile TEXT,
                dominant_driver TEXT,
                market_profile_tuning_action TEXT,
                weekly_return REAL,
                max_drawdown REAL,
                turnover REAL,
                outcome_sample_count INTEGER,
                signal_quality_score REAL,
                execution_cost_gap REAL,
                execution_gate_blocked_weight REAL,
                strategy_control_weight_delta REAL,
                risk_overlay_weight_delta REAL,
                risk_feedback_action TEXT,
                execution_feedback_action TEXT,
                shadow_apply_mode TEXT,
                risk_apply_mode TEXT,
                execution_apply_mode TEXT,
                market_profile_ready_for_manual_apply INTEGER,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_weekly_tuning_history_key "
                "ON investment_weekly_tuning_history (week_label, portfolio_id)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_weekly_tuning_history_lookup "
                "ON investment_weekly_tuning_history (market, portfolio_id, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_weekly_decision_evidence_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                run_id TEXT,
                parent_order_key TEXT,
                symbol TEXT,
                action TEXT,
                decision_status TEXT,
                candidate_snapshot_id TEXT,
                candidate_stage TEXT,
                order_value REAL,
                fill_notional REAL,
                signal_score REAL,
                expected_edge_bps REAL,
                expected_cost_bps REAL,
                edge_gate_threshold_bps REAL,
                blocked_market_rule_order_count INTEGER,
                blocked_edge_order_count INTEGER,
                blocked_gate_order_count INTEGER,
                dynamic_liquidity_bucket TEXT,
                dynamic_order_adv_pct REAL,
                slice_count INTEGER,
                strategy_control_weight_delta REAL,
                risk_overlay_weight_delta REAL,
                risk_market_profile_budget_weight_delta REAL,
                risk_throttle_weight_delta REAL,
                risk_recovery_weight_credit REAL,
                execution_gate_blocked_weight REAL,
                realized_slippage_bps REAL,
                realized_edge_bps REAL,
                execution_capture_bps REAL,
                first_fill_delay_seconds REAL,
                outcome_5d_bps REAL,
                outcome_20d_bps REAL,
                outcome_60d_bps REAL,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_weekly_decision_evidence_history_key "
                "ON investment_weekly_decision_evidence_history (week_label, portfolio_id, run_id, parent_order_key)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_weekly_decision_evidence_history_lookup "
                "ON investment_weekly_decision_evidence_history (market, portfolio_id, week_start DESC, ts DESC)"
            )
            self._ensure_column(c, "investment_weekly_decision_evidence_history", "order_value REAL")
            self._ensure_column(c, "investment_weekly_decision_evidence_history", "fill_notional REAL")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_trading_quality_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                window_start TEXT,
                window_end TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                evidence_layer TEXT,
                evidence_key TEXT,
                sample_count INTEGER,
                filled_count INTEGER,
                blocked_count INTEGER,
                filled_avg_expected_edge_bps REAL,
                filled_avg_expected_cost_bps REAL,
                filled_avg_realized_slippage_bps REAL,
                filled_avg_realized_edge_bps REAL,
                filled_avg_outcome_20d_bps REAL,
                blocked_avg_outcome_20d_bps REAL,
                post_cost_edge_delta_bps REAL,
                rule_quality TEXT,
                recommendation TEXT,
                evidence_summary TEXT,
                details TEXT
            )""")
            c.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_trading_quality_evidence_key "
                "ON investment_trading_quality_evidence (week_label, portfolio_id, evidence_layer, evidence_key)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_trading_quality_evidence_lookup "
                "ON investment_trading_quality_evidence (market, portfolio_id, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_patch_review_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                week_label TEXT,
                week_start TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                patch_kind TEXT,
                feedback_signature TEXT,
                review_status TEXT,
                review_status_label TEXT,
                ready_for_manual_apply INTEGER,
                profile TEXT,
                scope TEXT,
                config_file TEXT,
                config_path TEXT,
                config_commit_sha TEXT,
                config_diff_note TEXT,
                operator_note TEXT,
                details TEXT
            )""")
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_investment_patch_review_history_lookup "
                "ON investment_patch_review_history (market, portfolio_id, patch_kind, week_start DESC, ts DESC)"
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_execution_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                action TEXT,
                current_qty REAL,
                target_qty REAL,
                delta_qty REAL,
                ref_price REAL,
                target_weight REAL,
                order_value REAL,
                order_type TEXT,
                broker_order_id INTEGER,
                status TEXT,
                reason TEXT,
                details TEXT
            )""")
            self._ensure_column(c, "investment_execution_orders", "execution_intent_json TEXT")
            self._ensure_column(c, "investment_execution_orders", "score_before_cost REAL")
            self._ensure_column(c, "investment_execution_orders", "expected_cost_bps REAL")
            self._ensure_column(c, "investment_execution_orders", "expected_edge_threshold REAL")
            self._ensure_column(c, "investment_execution_orders", "expected_edge_score REAL")
            self._ensure_column(c, "investment_execution_orders", "expected_edge_bps REAL")
            self._ensure_column(c, "investment_execution_orders", "edge_gate_threshold_bps REAL")
            self._ensure_column(c, "investment_execution_orders", "session_bucket TEXT")
            self._ensure_column(c, "investment_execution_orders", "session_label TEXT")
            self._ensure_column(c, "investment_execution_orders", "execution_style TEXT")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_broker_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                qty REAL,
                avg_cost REAL,
                market_price REAL,
                market_value REAL,
                weight REAL,
                source TEXT,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                account_id TEXT,
                netliq REAL,
                cash REAL,
                buying_power REAL,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_analysis_states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                analysis_run_id TEXT,
                status TEXT,
                lifecycle TEXT,
                action TEXT,
                entry_status TEXT,
                score REAL,
                held_qty REAL,
                report_dir TEXT,
                run_kind TEXT,
                reason TEXT,
                details TEXT
            )""")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_analysis_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                analysis_run_id TEXT,
                event_kind TEXT,
                from_status TEXT,
                to_status TEXT,
                from_lifecycle TEXT,
                to_lifecycle TEXT,
                action TEXT,
                entry_status TEXT,
                score REAL,
                held_qty REAL,
                report_dir TEXT,
                run_kind TEXT,
                summary TEXT,
                details TEXT
            )""")
            c.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_analysis_state_key
                ON investment_analysis_states (market, portfolio_id, symbol)
                """
            )
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_candidate_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                report_dir TEXT,
                analysis_run_id TEXT,
                stage TEXT,
                symbol TEXT,
                action TEXT,
                direction TEXT,
                score REAL,
                model_recommendation_score REAL,
                execution_score REAL,
                analyst_recommendation_score REAL,
                market_sentiment_score REAL,
                scan_tier TEXT,
                source_reasons TEXT,
                entry_style TEXT,
                execution_ready INTEGER,
                details TEXT
            )""")
            self._ensure_column(c, "investment_candidate_snapshots", "data_quality_score REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "source_coverage REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "missing_ratio REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "expected_cost_bps REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "cost_penalty REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "score_before_cost REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "execution_score_before_cost REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "expected_edge_threshold REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "expected_edge_score REAL")
            self._ensure_column(c, "investment_candidate_snapshots", "expected_edge_bps REAL")
            c.execute("""
            CREATE TABLE IF NOT EXISTS investment_candidate_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT,
                ts TEXT,
                market TEXT,
                portfolio_id TEXT,
                symbol TEXT,
                horizon_days INTEGER,
                snapshot_ts TEXT,
                outcome_ts TEXT,
                direction TEXT,
                start_close REAL,
                end_close REAL,
                future_return REAL,
                max_drawdown REAL,
                max_runup REAL,
                outcome_label TEXT,
                details TEXT
            )""")
            c.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_candidate_snapshot_key
                ON investment_candidate_snapshots (snapshot_id)
                """
            )
            c.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_investment_candidate_outcome_key
                ON investment_candidate_outcomes (snapshot_id, horizon_days)
                """
            )

    def insert_order(self, row: Dict[str, Any]):
        # Accept sparse payloads so callers can evolve fields gradually.
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO orders ({cols}) VALUES ({qs})", list(row.values()))

    def update_order_status(self, order_id: int, status: str):
        with self._conn() as c:
            c.execute(
                """
                UPDATE orders
                SET status=?
                WHERE id = (
                    SELECT id FROM orders
                    WHERE order_id=?
                    ORDER BY id DESC
                    LIMIT 1
                )
                """,
                (status, int(order_id)),
            )

    def insert_fill(self, row: Dict[str, Any]):
        # Mirror order insert path for execution/fill audit rows.
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO fills ({cols}) VALUES ({qs})", list(row.values()))

    def insert_risk_event(self, kind: str, value: float, details: str = "", **extra: Any):
        row: Dict[str, Any] = {
            "ts": datetime.utcnow().isoformat(),
            "kind": kind,
            "value": value,
            "details": details,
        }
        for key, val in extra.items():
            if val is None:
                continue
            row[key] = val
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO risk_events ({cols}) VALUES ({qs})", list(row.values()))

    # -------- Phase1 helpers --------
    def insert_signal_audit(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO signals_audit ({cols}) VALUES ({qs})", list(row.values()))

    def upsert_md_quality(
        self,
        day: str,
        symbol: str,
        buckets: int,
        duplicates: int,
        max_gap_sec: int,
        last_end_time: str,
    ):
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO md_quality (day, symbol, buckets, duplicates, max_gap_sec, last_end_time, updated_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    day,
                    symbol,
                    int(buckets),
                    int(duplicates),
                    int(max_gap_sec),
                    last_end_time,
                    datetime.utcnow().isoformat(),
                ),
            )

    def get_md_quality(self, day: str):
        with self._conn() as c:
            return list(
                c.execute(
                    "SELECT day, symbol, buckets, duplicates, max_gap_sec, last_end_time, updated_ts "
                    "FROM md_quality WHERE day=?",
                    (day,),
                )
            )

    # -------- Phase1-B+: md_blacklist helpers --------
    def upsert_md_blacklist(self, symbol: str, status: str, reason: str, until_ts: int):
        """Insert/update persistent blacklist record."""
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO md_blacklist (symbol, status, reason, until_ts, updated_ts) VALUES (?, ?, ?, ?, ?)",
                (symbol, status, reason, int(until_ts), datetime.utcnow().isoformat()),
            )

    def get_md_blacklist(self):
        """Return all blacklist rows."""
        with self._conn() as c:
            return list(c.execute("SELECT symbol, status, reason, until_ts, updated_ts FROM md_blacklist"))

    def get_md_blacklist_active(self, now_ts: int):
        """Return active blacklist rows (until_ts > now_ts)."""
        with self._conn() as c:
            return list(
                c.execute(
                    "SELECT symbol, status, reason, until_ts, updated_ts FROM md_blacklist WHERE until_ts > ?",
                    (int(now_ts),),
                )
            )

    def upsert_regime_state(self, market: str, regime_state: str, snapshot: Dict[str, Any], adapted_cfg: Dict[str, Any]):
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO regime_state (market, ts, regime_state, snapshot_json, adapted_cfg_json) VALUES (?, ?, ?, ?, ?)",
                (
                    str(market).upper(),
                    datetime.utcnow().isoformat(),
                    regime_state,
                    json.dumps(snapshot, ensure_ascii=False),
                    json.dumps(adapted_cfg, ensure_ascii=False),
                ),
            )

    def get_regime_state(self, market: str):
        with self._conn() as c:
            return c.execute(
                "SELECT market, ts, regime_state, snapshot_json, adapted_cfg_json FROM regime_state WHERE market=?",
                (str(market).upper(),),
            ).fetchone()

    def get_order_by_order_id(self, order_id: int) -> Dict[str, Any]:
        with self._conn() as c:
            cur = c.execute(
                "SELECT * FROM orders WHERE order_id=? ORDER BY id DESC LIMIT 1",
                (int(order_id),),
            )
            row = cur.fetchone()
            if row is None:
                return {}
            out = {desc[0]: row[idx] for idx, desc in enumerate(cur.description or [])}
            details = out.get("details")
            if isinstance(details, str) and details:
                try:
                    out["details_json"] = json.loads(details)
                except Exception:
                    out["details_json"] = {}
            else:
                out["details_json"] = {}
            return out

    def insert_investment_run(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_runs ({cols}) VALUES ({qs})", list(row.values()))

    def insert_investment_position(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_positions ({cols}) VALUES ({qs})", list(row.values()))

    def insert_investment_trade(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_trades ({cols}) VALUES ({qs})", list(row.values()))

    def insert_investment_execution_run(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_execution_runs ({cols}) VALUES ({qs})", list(row.values()))

    def insert_investment_risk_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "")
        row["source_kind"] = str(row.get("source_kind") or "").strip().lower()
        for key in ("notes_json", "correlation_reduced_symbols_json", "stress_scenarios_json", "details"):
            value = row.get(key)
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(value, ensure_ascii=False)
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_risk_history ({cols}) VALUES ({qs})", list(row.values()))

    def upsert_investment_feedback_automation_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "")
        row["feedback_kind"] = str(row.get("feedback_kind") or "").strip().lower()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_feedback_automation_history
                WHERE week_label=? AND portfolio_id=? AND feedback_kind=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("portfolio_id") or ""),
                    str(row.get("feedback_kind") or "").strip().lower(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_feedback_automation_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def upsert_investment_feedback_threshold_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["feedback_kind"] = str(row.get("feedback_kind") or "").strip().lower()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_feedback_threshold_history
                WHERE week_label=? AND market=? AND feedback_kind=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("market") or "").upper(),
                    str(row.get("feedback_kind") or "").strip().lower(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_feedback_threshold_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def upsert_investment_market_profile_patch_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "").strip()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_market_profile_patch_history
                WHERE week_label=? AND portfolio_id=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("portfolio_id") or "").strip(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_market_profile_patch_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def upsert_investment_weekly_tuning_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "").strip()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_weekly_tuning_history
                WHERE week_label=? AND portfolio_id=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("portfolio_id") or "").strip(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_weekly_tuning_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def upsert_investment_weekly_decision_evidence_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "").strip()
        row["run_id"] = str(row.get("run_id") or "").strip()
        row["parent_order_key"] = str(row.get("parent_order_key") or "").strip()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_weekly_decision_evidence_history
                WHERE week_label=? AND portfolio_id=? AND run_id=? AND parent_order_key=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("portfolio_id") or "").strip(),
                    str(row.get("run_id") or "").strip(),
                    str(row.get("parent_order_key") or "").strip(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_weekly_decision_evidence_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def upsert_investment_trading_quality_evidence(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "").strip()
        row["evidence_layer"] = str(row.get("evidence_layer") or "").strip().upper()
        row["evidence_key"] = str(row.get("evidence_key") or "").strip().upper()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                """
                DELETE FROM investment_trading_quality_evidence
                WHERE week_label=? AND portfolio_id=? AND evidence_layer=? AND evidence_key=?
                """,
                (
                    str(row.get("week_label") or "").strip(),
                    str(row.get("portfolio_id") or "").strip(),
                    str(row.get("evidence_layer") or "").strip().upper(),
                    str(row.get("evidence_key") or "").strip().upper(),
                ),
            )
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_trading_quality_evidence ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def insert_investment_patch_review_history(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        row["week_label"] = str(row.get("week_label") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "").strip()
        row["patch_kind"] = str(row.get("patch_kind") or "").strip().lower()
        details = row.get("details")
        if isinstance(details, (dict, list)):
            row["details"] = json.dumps(details, ensure_ascii=False)
        with self._conn() as c:
            cols = ",".join(row.keys())
            qs = ",".join(["?"] * len(row))
            c.execute(
                f"INSERT INTO investment_patch_review_history ({cols}) VALUES ({qs})",
                list(row.values()),
            )

    def update_investment_execution_run(self, run_id: str, **fields: Any):
        if not str(run_id or "").strip() or not fields:
            return
        cols = ",".join(f"{key}=?" for key in fields.keys())
        params = list(fields.values()) + [str(run_id)]
        with self._conn() as c:
            c.execute(f"UPDATE investment_execution_runs SET {cols} WHERE run_id=?", params)

    def get_recent_investment_risk_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        source_kind: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_risk_history
                WHERE market=? AND (?='' OR portfolio_id=?) AND (?='' OR source_kind=?)
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or ""),
                    str(portfolio_id or ""),
                    str(source_kind or "").strip().lower(),
                    str(source_kind or "").strip().lower(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_recent_investment_feedback_automation_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        feedback_kind: str = "",
        limit: int = 24,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_feedback_automation_history
                WHERE market=? AND (?='' OR portfolio_id=?) AND (?='' OR feedback_kind=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or ""),
                    str(portfolio_id or ""),
                    str(feedback_kind or "").strip().lower(),
                    str(feedback_kind or "").strip().lower(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_feedback_threshold_history(
        self,
        market: str,
        *,
        feedback_kind: str = "",
        limit: int = 24,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_feedback_threshold_history
                WHERE market=? AND (?='' OR feedback_kind=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(feedback_kind or "").strip().lower(),
                    str(feedback_kind or "").strip().lower(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_market_profile_patch_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        limit: int = 24,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_market_profile_patch_history
                WHERE market=? AND (?='' OR portfolio_id=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or "").strip(),
                    str(portfolio_id or "").strip(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_weekly_tuning_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        limit: int = 24,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_weekly_tuning_history
                WHERE market=? AND (?='' OR portfolio_id=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or "").strip(),
                    str(portfolio_id or "").strip(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_weekly_decision_evidence_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        symbol: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_weekly_decision_evidence_history
                WHERE market=? AND (?='' OR portfolio_id=?) AND (?='' OR symbol=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or "").strip(),
                    str(portfolio_id or "").strip(),
                    str(symbol or "").strip().upper(),
                    str(symbol or "").strip().upper(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_trading_quality_evidence(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        evidence_layer: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_trading_quality_evidence
                WHERE market=? AND (?='' OR portfolio_id=?) AND (?='' OR evidence_layer=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or "").strip(),
                    str(portfolio_id or "").strip(),
                    str(evidence_layer or "").strip().upper(),
                    str(evidence_layer or "").strip().upper(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def get_recent_investment_patch_review_history(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        patch_kind: str = "",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_patch_review_history
                WHERE market=? AND (?='' OR portfolio_id=?) AND (?='' OR patch_kind=?)
                ORDER BY week_start DESC, ts DESC, id DESC
                LIMIT ?
                """,
                (
                    market_code,
                    str(portfolio_id or "").strip(),
                    str(portfolio_id or "").strip(),
                    str(patch_kind or "").strip().lower(),
                    str(patch_kind or "").strip().lower(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    row["details_json"] = json.loads(details)
                except Exception:
                    row["details_json"] = {}
            else:
                row["details_json"] = {}
            out.append(row)
        return out

    def insert_investment_execution_order(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_execution_orders ({cols}) VALUES ({qs})", list(row.values()))

    def update_investment_execution_order_status(self, broker_order_id: int, status: str):
        with self._conn() as c:
            c.execute(
                """
                UPDATE investment_execution_orders
                SET status=?
                WHERE id = (
                    SELECT id FROM investment_execution_orders
                    WHERE broker_order_id=?
                    ORDER BY id DESC
                    LIMIT 1
                )
                """,
                (str(status), int(broker_order_id)),
            )

    def get_recent_shadow_review_orders(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_execution_orders
                WHERE market=? AND (?='' OR portfolio_id=?) AND status='REVIEW_REQUIRED'
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (market_code, str(portfolio_id or ""), str(portfolio_id or ""), max(1, int(limit))),
            ).fetchall()
        scoped: list[Dict[str, Any]] = []
        for raw in rows:
            row = dict(raw)
            details_json: Dict[str, Any] = {}
            details = row.get("details")
            if isinstance(details, str) and details:
                try:
                    details_json = json.loads(details)
                except Exception:
                    details_json = {}
            row["details_json"] = details_json
            shadow_status = str(details_json.get("shadow_review_status") or "").strip().upper()
            shadow_reason = str(details_json.get("shadow_review_reason") or "").strip()
            manual_reason = str(details_json.get("manual_review_reason") or "").strip()
            reason_blob = " ".join(
                [
                    str(row.get("reason") or ""),
                    shadow_reason,
                    manual_reason,
                ]
            ).lower()
            if shadow_status == "REVIEW_REQUIRED" or "shadow" in reason_blob:
                scoped.append(row)
        return scoped

    def insert_investment_broker_position(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_broker_positions ({cols}) VALUES ({qs})", list(row.values()))

    def insert_account_snapshot(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        details = row.get("details")
        if isinstance(details, dict):
            row["details"] = json.dumps(details, ensure_ascii=False)
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO account_snapshots ({cols}) VALUES ({qs})", list(row.values()))

    def upsert_investment_analysis_state(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        details = row.get("details")
        if isinstance(details, dict):
            row["details"] = json.dumps(details, ensure_ascii=False)
        market = str(row.get("market") or "").upper()
        portfolio_id = str(row.get("portfolio_id") or "")
        symbol = str(row.get("symbol") or "").upper()
        row["market"] = market
        row["portfolio_id"] = portfolio_id
        row["symbol"] = symbol
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(
                "DELETE FROM investment_analysis_states WHERE market=? AND portfolio_id=? AND symbol=?",
                (market, portfolio_id, symbol),
            )
            c.execute(f"INSERT INTO investment_analysis_states ({cols}) VALUES ({qs})", list(row.values()))

    def insert_investment_analysis_event(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        details = row.get("details")
        if isinstance(details, dict):
            row["details"] = json.dumps(details, ensure_ascii=False)
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "")
        row["symbol"] = str(row.get("symbol") or "").upper()
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT INTO investment_analysis_events ({cols}) VALUES ({qs})", list(row.values()))

    def get_investment_analysis_state_map(self, market: str, portfolio_id: str = "") -> Dict[str, Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_analysis_states
                WHERE market=? AND (?='' OR portfolio_id=?)
                ORDER BY ts DESC, id DESC
                """,
                (market_code, str(portfolio_id or ""), str(portfolio_id or "")),
            ).fetchall()
        out: Dict[str, Dict[str, Any]] = {}
        for raw in rows:
            row = dict(raw)
            symbol = str(row.get("symbol") or "").upper()
            if symbol and symbol not in out:
                out[symbol] = row
        return out

    def get_recent_investment_analysis_events(
        self,
        market: str,
        portfolio_id: str = "",
        *,
        limit: int = 20,
    ) -> list[Dict[str, Any]]:
        market_code = str(market or "").upper()
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_analysis_events
                WHERE market=? AND (?='' OR portfolio_id=?)
                ORDER BY ts DESC, id DESC
                LIMIT ?
                """,
                (market_code, str(portfolio_id or ""), str(portfolio_id or ""), max(1, int(limit))),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_latest_account_snapshot(
        self,
        account_id: str,
        *,
        max_age_sec: int | None = None,
    ) -> Dict[str, Any]:
        account_id = str(account_id or "").strip()
        if not account_id:
            return {}
        with self._conn() as c:
            cur = c.execute(
                """
                SELECT ts, account_id, netliq, cash, buying_power, details
                FROM account_snapshots
                WHERE account_id=?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (account_id,),
            )
            row = cur.fetchone()
            if row is None:
                return {}
            out = {desc[0]: row[idx] for idx, desc in enumerate(cur.description or [])}
        ts_raw = str(out.get("ts") or "").strip()
        if max_age_sec is not None and ts_raw:
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts.tzinfo is not None:
                    ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
                if datetime.utcnow() - ts > timedelta(seconds=max(0, int(max_age_sec))):
                    return {}
            except Exception:
                return {}
        details = out.get("details")
        if isinstance(details, str) and details:
            try:
                out["details_json"] = json.loads(details)
            except Exception:
                out["details_json"] = {}
        else:
            out["details_json"] = {}
        return out

    def get_latest_investment_run(self, market: str, portfolio_id: str = "") -> Dict[str, Any]:
        with self._conn() as c:
            if str(portfolio_id or "").strip():
                cur = c.execute(
                    "SELECT * FROM investment_runs WHERE market=? AND portfolio_id=? ORDER BY ts DESC, id DESC LIMIT 1",
                    (str(market).upper(), str(portfolio_id)),
                )
            else:
                cur = c.execute(
                    "SELECT * FROM investment_runs WHERE market=? ORDER BY ts DESC, id DESC LIMIT 1",
                    (str(market).upper(),),
                )
            row = cur.fetchone()
            if row is None:
                return {}
            out = {desc[0]: row[idx] for idx, desc in enumerate(cur.description or [])}
            details = out.get("details")
            if isinstance(details, str) and details:
                try:
                    out["details_json"] = json.loads(details)
                except Exception:
                    out["details_json"] = {}
            else:
                out["details_json"] = {}
            return out

    def get_latest_investment_positions(self, market: str, portfolio_id: str = "") -> Dict[str, Dict[str, Any]]:
        latest = self.get_latest_investment_run(market, portfolio_id=portfolio_id)
        run_id = str(latest.get("run_id", "") or "").strip()
        if not run_id:
            return {}
        with self._conn() as c:
            cur = c.execute(
                "SELECT symbol, qty, cost_basis, last_price, market_value, weight, status, details "
                "FROM investment_positions WHERE market=? AND run_id=? AND (?='' OR portfolio_id=?)",
                (str(market).upper(), run_id, str(portfolio_id), str(portfolio_id)),
            )
            out: Dict[str, Dict[str, Any]] = {}
            for row in cur.fetchall():
                symbol = str(row[0]).upper()
                details_json: Dict[str, Any] = {}
                if isinstance(row[7], str) and row[7]:
                    try:
                        details_json = json.loads(row[7])
                    except Exception:
                        details_json = {}
                out[symbol] = {
                    "qty": float(row[1] or 0.0),
                    "cost_basis": float(row[2] or 0.0),
                    "last_price": float(row[3] or 0.0),
                    "market_value": float(row[4] or 0.0),
                    "weight": float(row[5] or 0.0),
                    "status": str(row[6] or ""),
                    "details_json": details_json,
                }
            return out

    def insert_investment_candidate_snapshot(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        details = row.get("details")
        if isinstance(details, dict):
            row["details"] = json.dumps(details, ensure_ascii=False)
        if "snapshot_id" in row:
            row["snapshot_id"] = str(row.get("snapshot_id") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "")
        row["stage"] = str(row.get("stage") or "").strip().lower()
        row["symbol"] = str(row.get("symbol") or "").upper()
        row["direction"] = str(row.get("direction") or "LONG").upper()
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(f"INSERT OR REPLACE INTO investment_candidate_snapshots ({cols}) VALUES ({qs})", list(row.values()))

    def upsert_investment_candidate_outcome(self, row: Dict[str, Any]):
        row = dict(row)
        row.setdefault("ts", datetime.utcnow().isoformat())
        details = row.get("details")
        if isinstance(details, dict):
            row["details"] = json.dumps(details, ensure_ascii=False)
        row["snapshot_id"] = str(row.get("snapshot_id") or "").strip()
        row["market"] = str(row.get("market") or "").upper()
        row["portfolio_id"] = str(row.get("portfolio_id") or "")
        row["symbol"] = str(row.get("symbol") or "").upper()
        row["direction"] = str(row.get("direction") or "LONG").upper()
        cols = ",".join(row.keys())
        qs = ",".join(["?"] * len(row))
        with self._conn() as c:
            c.execute(
                "DELETE FROM investment_candidate_outcomes WHERE snapshot_id=? AND horizon_days=?",
                (str(row.get("snapshot_id") or "").strip(), int(row.get("horizon_days") or 0)),
            )
            c.execute(f"INSERT INTO investment_candidate_outcomes ({cols}) VALUES ({qs})", list(row.values()))

    def get_pending_investment_candidate_snapshots(
        self,
        *,
        market: str = "",
        portfolio_id: str = "",
        stage: str = "",
        horizon_days: int,
        limit: int = 200,
    ) -> list[Dict[str, Any]]:
        cutoff = datetime.utcnow() - timedelta(days=max(1, int(horizon_days)))
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT s.*
                FROM investment_candidate_snapshots s
                LEFT JOIN investment_candidate_outcomes o
                  ON s.snapshot_id=o.snapshot_id AND o.horizon_days=?
                WHERE o.id IS NULL
                  AND s.snapshot_id!=''
                  AND s.ts <= ?
                  AND (?='' OR s.market=?)
                  AND (?='' OR s.portfolio_id=?)
                  AND (?='' OR s.stage=?)
                ORDER BY s.ts ASC, s.id ASC
                LIMIT ?
                """,
                (
                    int(horizon_days),
                    cutoff.isoformat(),
                    str(market or "").upper(),
                    str(market or "").upper(),
                    str(portfolio_id or ""),
                    str(portfolio_id or ""),
                    str(stage or "").strip().lower(),
                    str(stage or "").strip().lower(),
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_recent_investment_candidate_outcomes(
        self,
        *,
        market: str = "",
        portfolio_id: str = "",
        horizon_days: int | None = None,
        limit: int = 200,
    ) -> list[Dict[str, Any]]:
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """
                SELECT *
                FROM investment_candidate_outcomes
                WHERE (?='' OR market=?)
                  AND (?='' OR portfolio_id=?)
                  AND (? IS NULL OR horizon_days=?)
                ORDER BY outcome_ts DESC, id DESC
                LIMIT ?
                """,
                (
                    str(market or "").upper(),
                    str(market or "").upper(),
                    str(portfolio_id or ""),
                    str(portfolio_id or ""),
                    None if horizon_days is None else int(horizon_days),
                    None if horizon_days is None else int(horizon_days),
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_investment_snapshot_training_rows(
        self,
        *,
        market: str = "",
        portfolio_id: str = "",
        horizon_days: int,
        stages: list[str] | tuple[str, ...] | None = None,
        direction: str = "LONG",
        limit: int = 4000,
    ) -> list[Dict[str, Any]]:
        normalized_stages = [str(item or "").strip().lower() for item in list(stages or []) if str(item or "").strip()]
        stage_clause = ""
        params: list[Any] = [
            int(horizon_days),
            str(market or "").upper(),
            str(market or "").upper(),
            str(portfolio_id or ""),
            str(portfolio_id or ""),
            str(direction or "").upper(),
            str(direction or "").upper(),
        ]
        if normalized_stages:
            placeholders = ",".join("?" for _ in normalized_stages)
            stage_clause = f" AND s.stage IN ({placeholders})"
            params.extend(normalized_stages)
        params.append(max(1, int(limit)))
        with self._conn() as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                f"""
                SELECT
                    s.*,
                    o.horizon_days,
                    o.snapshot_ts,
                    o.outcome_ts,
                    o.future_return,
                    o.max_drawdown,
                    o.max_runup,
                    o.outcome_label
                FROM investment_candidate_snapshots s
                JOIN investment_candidate_outcomes o
                  ON s.snapshot_id=o.snapshot_id
                WHERE o.horizon_days=?
                  AND (?='' OR s.market=?)
                  AND (?='' OR s.portfolio_id=?)
                  AND (?='' OR s.direction=?)
                  {stage_clause}
                ORDER BY o.outcome_ts DESC, s.id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]
