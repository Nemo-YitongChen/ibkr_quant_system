from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.analysis.report import write_investment_md
from src.app.investment_guard import InvestmentGuardConfig, InvestmentGuardEngine
from src.common.adaptive_strategy import (
    adaptive_strategy_context,
    adaptive_strategy_effective_controls,
    adaptive_strategy_effective_control_fields,
    adaptive_strategy_market_execution_overrides,
    adaptive_strategy_market_plan_overrides,
    adaptive_strategy_market_regime_overrides,
    adaptive_strategy_summary_fields,
    apply_active_market_execution_overrides,
    apply_adaptive_strategy_execution_controls,
    apply_adaptive_strategy_plan_overrides,
    apply_adaptive_strategy_regime_overrides,
    apply_adaptive_strategy_weight_cap,
    apply_adaptive_defensive_rank_cap,
    load_adaptive_strategy,
    load_report_adaptive_strategy_payload,
)
from src.common.storage import Storage
from src.common.user_explanations import annotate_guard_user_explanation
from src.portfolio.investment_allocator import InvestmentExecutionConfig
from src.strategies.mid_regime import RegimeConfig


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
    assert "US" in context["market_profiles"]
    assert context["market_profiles"]["HK"]["no_trade_band_pct"] > context["market_profiles"]["US"]["no_trade_band_pct"]
    assert context["market_profiles"]["HK"]["min_expected_edge_bps"] > context["market_profiles"]["US"]["min_expected_edge_bps"]
    assert context["market_profiles"]["HK"]["regime_risk_on_threshold"] >= context["market_profiles"]["US"]["regime_risk_on_threshold"]


def test_load_adaptive_strategy_supports_layered_market_override(tmp_path: Path) -> None:
    base_cfg = tmp_path / "adaptive_base.yaml"
    override_cfg = tmp_path / "adaptive_us_override.yaml"
    base_cfg.write_text(
        "\n".join(
            [
                "adaptive_strategy:",
                "  meta:",
                '    name: "ACM-RS"',
                "  execution:",
                '    rebalance_frequency: "weekly"',
                "  market_profiles:",
                "    DEFAULT:",
                '      label: "Base"',
                "      min_expected_edge_bps: 18.0",
                "      edge_cost_buffer_bps: 6.0",
                "    US:",
                '      label: "US base"',
                "      min_expected_edge_bps: 16.0",
                "      edge_cost_buffer_bps: 5.0",
            ]
        ),
        encoding="utf-8",
    )
    override_cfg.write_text(
        "\n".join(
            [
                f"extends: {base_cfg}",
                "adaptive_strategy:",
                "  market_profiles:",
                "    US:",
                "      edge_cost_buffer_bps: 4.0",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_adaptive_strategy(tmp_path, str(override_cfg))
    context = adaptive_strategy_context(cfg)

    assert context["market_profiles"]["US"]["min_expected_edge_bps"] == 16.0
    assert context["market_profiles"]["US"]["edge_cost_buffer_bps"] == 4.0
    assert len(context["config_sources"]) == 2


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
    assert summary["defensive_regime_detected"] is True
    assert summary["active_regime_states"] == ["RISK_OFF"]
    assert rows[0]["action"] == "WATCH"
    assert rows[0]["execution_ready"] == 0
    assert rows[0]["adaptive_strategy_status"] == "DEFENSIVE_REGIME_CAP"
    assert "adaptive_defensive_regime" in rows[0]["signal_decision"]["gates_blocked"]


def test_load_report_adaptive_strategy_payload_flattens_runtime_fields() -> None:
    with TemporaryDirectory() as tmp:
        report_dir = Path(tmp)
        (report_dir / "investment_adaptive_strategy_summary.json").write_text(
            json.dumps(
                {
                    "adaptive_strategy": {
                        "name": "ACM-RS",
                        "display_name": "Adaptive Cross-Market Relative Strength",
                        "summary_text": "ACM-RS | RS=126/63/20 | rebalance=weekly | entry_delay=15-30m",
                    },
                    "summary": {
                        "enabled": True,
                        "defensive_cap_count": 2,
                        "defensive_regime_detected": True,
                        "active_regime_states": ["RISK_OFF"],
                        "top_defensive_symbols": ["AAPL", "MSFT"],
                    },
                    "active_market_plan": {
                        "profile_key": "US",
                        "summary_text": "staged=3x | no_trade_band=3.0%",
                    },
                    "active_market_regime": {
                        "profile_key": "US",
                        "summary_text": "vol=1.00%/1.80% | risk_on=0.50",
                    },
                    "active_market_execution": {
                        "profile_key": "US",
                        "summary_text": "min_edge=16.0bps | edge_buffer=5.0bps",
                        "overrides": {
                            "min_expected_edge_bps": 16.0,
                            "edge_cost_buffer_bps": 5.0,
                        },
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        payload = load_report_adaptive_strategy_payload(report_dir)
        fields = adaptive_strategy_summary_fields(payload)
        assert fields["adaptive_strategy_name"] == "ACM-RS"
        assert fields["adaptive_strategy_defensive_caps"] == 2
        assert fields["adaptive_strategy_defensive_regime"] is True
        assert fields["adaptive_strategy_active_regime_states"] == ["RISK_OFF"]
        assert fields["adaptive_strategy_top_defensive_symbols"] == ["AAPL", "MSFT"]
        assert "defensive_caps=2" in fields["adaptive_strategy_runtime_note"]
        assert "AAPL" in fields["adaptive_strategy_runtime_note"]
        assert payload["active_market_execution"]["overrides"]["min_expected_edge_bps"] == 16.0
        assert fields["adaptive_strategy_active_market_profile"] == "US"
        assert fields["adaptive_strategy_active_market_execution_summary"] == "min_edge=16.0bps | edge_buffer=5.0bps"
        assert "当前使用 US 市场档案" in fields["adaptive_strategy_active_market_note"]


def test_adaptive_strategy_effective_controls_scale_paper_target_weight() -> None:
    payload = {
        "adaptive_strategy": adaptive_strategy_context(load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")),
        "summary": {
            "enabled": True,
            "defensive_cap_count": 1,
            "defensive_regime_detected": True,
            "active_regime_states": ["RISK_OFF"],
            "top_defensive_symbols": ["AAPL"],
        },
    }
    controls = adaptive_strategy_effective_controls(
        payload,
        portfolio_equity=100000.0,
        base_target_invested_weight=0.60,
    )
    fields = adaptive_strategy_effective_control_fields(controls)
    scaled = apply_adaptive_strategy_weight_cap({"AAPL": 0.60}, controls)
    assert controls["account_size_bucket"] == "medium"
    assert controls["applied"] is True
    assert controls["effective_target_invested_weight"] == 0.30
    assert scaled["AAPL"] == 0.30
    assert fields["strategy_effective_controls_applied"] is True
    assert "策略主动转入防守" in fields["strategy_effective_controls_human_note"]


def test_adaptive_strategy_effective_controls_scale_execution_caps() -> None:
    payload = {
        "adaptive_strategy": adaptive_strategy_context(load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")),
        "summary": {
            "enabled": True,
            "defensive_cap_count": 2,
            "defensive_regime_detected": True,
            "active_regime_states": ["RISK_OFF"],
            "top_defensive_symbols": ["AAPL", "MSFT"],
        },
    }
    controls = adaptive_strategy_effective_controls(
        payload,
        portfolio_equity=100000.0,
        base_target_invested_weight=0.60,
        base_account_allocation_pct=0.60,
        base_max_order_value_pct=0.50,
    )
    effective_cfg = apply_adaptive_strategy_execution_controls(
        InvestmentExecutionConfig(account_allocation_pct=0.60, max_order_value_pct=0.50),
        controls,
    )
    assert controls["applied"] is True
    assert controls["effective_target_invested_weight"] == 0.30
    assert controls["effective_account_allocation_pct"] == 0.50
    assert round(float(controls["effective_max_order_value_pct"]), 6) == round(0.50 * (5.0 / 6.0), 6)
    assert effective_cfg.account_allocation_pct == 0.50
    assert round(float(effective_cfg.max_order_value_pct), 6) == round(0.50 * (5.0 / 6.0), 6)


def test_apply_adaptive_strategy_plan_overrides_uses_market_profile() -> None:
    from src.analysis.investment import InvestmentPlanConfig

    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    base = InvestmentPlanConfig()
    us_overrides = adaptive_strategy_market_plan_overrides(cfg, "US")
    cn_plan = apply_adaptive_strategy_plan_overrides(base, cfg, market="CN")
    assert us_overrides["profile_key"] == "US"
    assert "no_trade_band_pct" in us_overrides["overrides"]
    assert cn_plan.rebalance_window_days == 35
    assert cn_plan.turnover_penalty_scale == 0.28
    assert cn_plan.no_trade_band_pct == 0.065


def test_apply_adaptive_strategy_regime_overrides_uses_market_profile() -> None:
    cfg = load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml")
    base = RegimeConfig()
    hk_overrides = adaptive_strategy_market_regime_overrides(cfg, "HK")
    hk_regime = apply_adaptive_strategy_regime_overrides(base, cfg, market="HK")
    assert hk_overrides["profile_key"] == "HK"
    assert hk_regime.vol_elevated == 0.012
    assert hk_regime.vol_extreme == 0.022
    assert hk_regime.risk_on_threshold == 0.52


def test_apply_active_market_execution_overrides_uses_payload() -> None:
    cfg = InvestmentExecutionConfig(min_expected_edge_bps=18.0, edge_cost_buffer_bps=6.0)
    payload = {
        "active_market_execution": {
            "profile_key": "HK",
            "overrides": {
                "min_expected_edge_bps": 26.0,
                "edge_cost_buffer_bps": 9.0,
            },
        }
    }
    effective = apply_active_market_execution_overrides(cfg, payload)
    hk_execution = adaptive_strategy_market_execution_overrides(
        load_adaptive_strategy(Path("."), "config/adaptive_strategy_framework.yaml"),
        "HK",
    )
    assert hk_execution["overrides"]["min_expected_edge_bps"] == 26.0
    assert effective.min_expected_edge_bps == 26.0
    assert effective.edge_cost_buffer_bps == 9.0


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
