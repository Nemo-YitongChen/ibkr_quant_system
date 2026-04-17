from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest.mock import Mock

from src.analysis.investment_portfolio import InvestmentPaperConfig
from src.app.investment_engine import ExecutionSessionProfile, InvestmentExecutionEngine
from src.app.investment_opportunity import InvestmentOpportunityConfig, InvestmentOpportunityEngine
from src.common.adaptive_strategy import apply_adaptive_defensive_opportunity_policy, load_adaptive_strategy
from src.common.market_structure import MarketStructureConfig, load_market_structure, market_structure_summary
from src.common.storage import Storage
from src.common.user_explanations import annotate_opportunity_user_explanation
from src.portfolio.investment_allocator import InvestmentExecutionConfig
from src.tools.generate_investment_report import _compute_cost_metrics


class _DummyEvent:
    def __iadd__(self, other):
        return self


class _FakeIB:
    orderStatusEvent = _DummyEvent()
    errorEvent = _DummyEvent()
    execDetailsEvent = _DummyEvent()
    commissionReportEvent = _DummyEvent()


def test_load_hk_market_structure_includes_fee_stack() -> None:
    structure = load_market_structure(Path("."), "HK")
    assert structure.market == "HK"
    assert structure.order_rules.odd_lot_discount_risk is True
    assert round(structure.costs.total_one_side_bps(), 3) == 11.27


def test_compute_cost_metrics_uses_market_structure_fee_floor() -> None:
    structure = load_market_structure(Path("."), "HK")
    metrics = _compute_cost_metrics("0700.HK", daily_bars=[], market="HK", market_structure=structure)
    assert metrics["commission_proxy_bps"] == round(structure.costs.total_one_side_bps(), 6)
    assert metrics["expected_cost_bps"] == round(structure.costs.total_one_side_bps(), 6)


def test_investment_execution_engine_small_account_routes_non_etf_to_market_structure_review() -> None:
    with NamedTemporaryFile(suffix=".db") as tmp, TemporaryDirectory() as report_dir:
        report_path = Path(report_dir)
        (report_path / "investment_candidates.csv").write_text(
            "\n".join(
                [
                    "symbol,action,score,model_recommendation_score,execution_score,execution_ready,asset_class",
                    "AAPL,ACCUMULATE,0.62,0.62,0.31,1,equity",
                    "SPY,ACCUMULATE,0.55,0.55,0.24,1,etf",
                ]
            ),
            encoding="utf-8",
        )
        engine = InvestmentExecutionEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(tmp.name),
            market="US",
            portfolio_id="US:test",
            paper_cfg=InvestmentPaperConfig(),
            execution_cfg=InvestmentExecutionConfig(manual_review_enabled=True, manual_review_order_value_pct=0.10),
            market_structure=load_market_structure(Path("."), "US"),
        )
        allowed, blocked = engine._apply_market_structure_review_gates(
            report_path,
            [
                {
                    "symbol": "AAPL",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 10.0,
                    "delta_qty": 10.0,
                    "ref_price": 200.0,
                    "target_weight": 0.16,
                    "order_value": 2000.0,
                    "reason": "rebalance_up",
                },
                {
                    "symbol": "SPY",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 5.0,
                    "delta_qty": 5.0,
                    "ref_price": 500.0,
                    "target_weight": 0.10,
                    "order_value": 2500.0,
                    "reason": "rebalance_up",
                },
            ],
            broker_equity=10000.0,
        )
        assert len(allowed) == 1
        assert allowed[0]["symbol"] == "SPY"
        assert len(blocked) == 1
        assert blocked[0]["symbol"] == "AAPL"
        assert blocked[0]["manual_review_status"] == "REVIEW_REQUIRED"
        assert "market structure review required" in blocked[0]["manual_review_reason"]


def test_market_structure_summary_marks_small_account_rule_active() -> None:
    structure = load_market_structure(Path("."), "US")
    summary = market_structure_summary(structure, broker_equity=10000.0)
    assert summary["small_account_rule_active"] is True
    assert summary["summary_text"].startswith("settlement=T+1")


def test_investment_opportunity_engine_small_account_prefers_etf_entries() -> None:
    with NamedTemporaryFile(suffix=".db") as tmp:
        engine = InvestmentOpportunityEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(tmp.name),
            market="US",
            portfolio_id="US:test",
            execution_cfg=InvestmentExecutionConfig(),
            opportunity_cfg=InvestmentOpportunityConfig(),
            market_structure=load_market_structure(Path("."), "US"),
        )
        rows = engine._apply_market_structure_guidance(
            [
                {"symbol": "AAPL", "entry_status": "ENTRY_NOW", "entry_reason": "-", "asset_class": "equity"},
                {"symbol": "SPY", "entry_status": "ENTRY_NOW", "entry_reason": "-", "asset_class": "etf"},
            ],
            broker_equity=10000.0,
        )
        assert rows[0]["entry_status"] == "WAIT_ACCOUNT_RULE"
        assert rows[0]["market_structure_status"] == "SMALL_ACCOUNT_ETF_FIRST"
        assert rows[0]["user_reason_label"] == "账户规则限制"
        assert "优先 ETF" in rows[0]["entry_reason"]
        assert "优先 ETF" in rows[0]["user_reason"]
        assert rows[1]["entry_status"] == "ENTRY_NOW"
        assert rows[1]["market_structure_status"] == "CLEAR"
        assert rows[1]["user_reason_label"] == "可开始分批"


def test_investment_opportunity_engine_applies_adaptive_defensive_waits() -> None:
    with NamedTemporaryFile(suffix=".db") as tmp:
        engine = InvestmentOpportunityEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(tmp.name),
            market="US",
            portfolio_id="US:test",
            execution_cfg=InvestmentExecutionConfig(),
            opportunity_cfg=InvestmentOpportunityConfig(ma_slow_days=50),
            market_structure=load_market_structure(Path("."), "US"),
            adaptive_strategy=load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml"),
        )
        assert engine.opportunity_cfg.ma_slow_days == 120
        rows = engine._apply_market_structure_guidance(
            [
                {
                    "symbol": "SPY",
                    "entry_status": "ENTRY_NOW",
                    "entry_reason": "-",
                    "asset_class": "etf",
                    "regime_state": "RISK_OFF",
                }
            ],
            broker_equity=50000.0,
        )
        adjusted = [annotate_opportunity_user_explanation(dict(row)) for row in apply_adaptive_defensive_opportunity_policy(rows, engine.adaptive_strategy)]
        assert adjusted[0]["entry_status"] == "WAIT_DEFENSIVE_REGIME"
        assert adjusted[0]["adaptive_strategy_status"] == "DEFENSIVE_REGIME_CAP"
        assert adjusted[0]["user_reason_label"] == "防守阶段先观察"
        assert "防守阶段" in adjusted[0]["entry_reason"]


def test_investment_execution_market_rule_gate_blocks_research_only_entries() -> None:
    with NamedTemporaryFile(suffix=".db") as tmp:
        engine = InvestmentExecutionEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(tmp.name),
            market="CN",
            portfolio_id="CN:test",
            paper_cfg=InvestmentPaperConfig(),
            execution_cfg=InvestmentExecutionConfig(edge_gate_enabled=False),
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

        allowed, blocked = engine._apply_market_rule_gates(
            [
                {
                    "symbol": "510300.SS",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 100.0,
                    "delta_qty": 100.0,
                    "ref_price": 4.0,
                    "target_weight": 0.10,
                    "order_value": 400.0,
                    "lot_size": 100.0,
                    "reason": "rebalance_up",
                }
            ]
        )

        assert allowed == []
        assert len(blocked) == 1
        assert blocked[0]["status"] == "BLOCKED_MARKET_RULE"
        assert blocked[0]["market_rule_status"] == "BLOCKED_RESEARCH_ONLY"
        assert blocked[0]["user_reason_label"] == "当前市场仅研究"


def test_investment_execution_market_rule_gate_blocks_board_lot_mismatch() -> None:
    with NamedTemporaryFile(suffix=".db") as tmp:
        custom_structure = MarketStructureConfig.from_dict(
            {
                "market": "CN",
                "research_only": False,
                "order_rules": {
                    "buy_lot_multiple": 100,
                    "day_turnaround_allowed": False,
                    "odd_lot_auto_match": False,
                    "odd_lot_discount_risk": True,
                    "price_limit_pct": 10.0,
                },
            }
        )
        engine = InvestmentExecutionEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(tmp.name),
            market="CN",
            portfolio_id="CN:test",
            paper_cfg=InvestmentPaperConfig(),
            execution_cfg=InvestmentExecutionConfig(edge_gate_enabled=False),
            market_structure=custom_structure,
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

        allowed, blocked = engine._apply_market_rule_gates(
            [
                {
                    "symbol": "600519.SS",
                    "action": "BUY",
                    "current_qty": 0.0,
                    "target_qty": 150.0,
                    "delta_qty": 150.0,
                    "ref_price": 10.0,
                    "target_weight": 0.10,
                    "order_value": 1500.0,
                    "lot_size": 100.0,
                    "reason": "rebalance_up",
                }
            ]
        )

        assert allowed == []
        assert len(blocked) == 1
        assert blocked[0]["status"] == "BLOCKED_MARKET_RULE"
        assert blocked[0]["market_rule_status"] == "BLOCKED_BOARD_LOT"
        assert blocked[0]["user_reason_label"] == "整手规则限制"
