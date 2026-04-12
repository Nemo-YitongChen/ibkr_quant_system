from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from src.analysis.report import write_investment_md
from src.app.investment_guard import InvestmentGuardConfig, InvestmentGuardEngine
from src.common.adaptive_strategy import (
    adaptive_strategy_context,
    apply_adaptive_defensive_rank_cap,
    load_adaptive_strategy,
)
from src.common.storage import Storage
from src.common.user_explanations import annotate_guard_user_explanation
from src.portfolio.investment_allocator import InvestmentExecutionConfig


class _DummyEvent:
    def __iadd__(self, other):
        return self


class _FakeIB:
    orderStatusEvent = _DummyEvent()
    errorEvent = _DummyEvent()
    execDetailsEvent = _DummyEvent()
    commissionReportEvent = _DummyEvent()


def test_load_adaptive_strategy_framework_defaults() -> None:
    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    context = adaptive_strategy_context(cfg)
    assert context["name"] == "ACM-RS"
    assert context["regime"]["long_ma_window"] == 120
    assert context["relative_strength"]["lookback_long"] == 126
    assert context["execution"]["rebalance_frequency"] == "weekly"


def test_write_investment_md_includes_adaptive_strategy_section() -> None:
    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    with TemporaryDirectory() as tmp:
        report_path = Path(tmp) / "investment_report.md"
        write_investment_md(
            str(report_path),
            "Investment Candidate Report",
            [],
            [],
            {
                "summary": {},
                "market_profile": {},
                "market_structure": {},
                "adaptive_strategy": adaptive_strategy_context(cfg),
            },
        )
        text = report_path.read_text(encoding="utf-8")
        assert "策略框架" in text
        assert "ACM-RS" in text
        assert "相对强弱: R126 / R63 / Vol20" in text
        assert "执行节奏: signal=daily_close" in text


def test_apply_adaptive_defensive_rank_cap_blocks_accumulate_in_risk_off() -> None:
    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    rows, summary = apply_adaptive_defensive_rank_cap(
        [
            {
                "symbol": "AAPL",
                "action": "ACCUMULATE",
                "score": 0.62,
                "execution_ready": 1,
                "regime_state": "RISK_OFF",
                "signal_decision": {"gates_blocked": [], "reasons": [], "context": {}},
            }
        ],
        cfg,
    )
    assert summary["defensive_cap_count"] == 1
    assert rows[0]["action"] == "WATCH"
    assert rows[0]["execution_ready"] == 0
    assert rows[0]["adaptive_strategy_status"] == "DEFENSIVE_REGIME_CAP"
    assert "adaptive_defensive_regime" in rows[0]["signal_decision"]["gates_blocked"]


def test_guard_runtime_summary_marks_defensive_market_sentiment() -> None:
    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    with TemporaryDirectory() as tmp:
        base = Path(tmp)
        (base / "market_sentiment.json").write_text('{"label":"DEFENSIVE","score":-0.4}', encoding="utf-8")
        engine = InvestmentGuardEngine(
            ib=_FakeIB(),
            account_id="DUQ152001",
            storage=Storage(str(base / "audit.db")),
            market="US",
            portfolio_id="US:test",
            execution_cfg=InvestmentExecutionConfig(),
            guard_cfg=InvestmentGuardConfig(),
            adaptive_strategy=cfg,
        )
        summary = engine._adaptive_guard_runtime_summary(base)
        assert summary["guard_status"] == "DEFENSIVE_REGIME"
        assert "防守阶段" in summary["reason"]


def test_guard_user_explanation_prefers_defensive_note() -> None:
    row = annotate_guard_user_explanation(
        {
            "reason": "guard_take_profit_trim",
            "adaptive_strategy_note": "防守环境下优先锁定部分利润，避免把已有浮盈重新暴露给回撤。",
        }
    )
    assert row["user_reason_label"] == "先锁定利润"
    assert "优先锁定部分利润" in row["user_reason"]
