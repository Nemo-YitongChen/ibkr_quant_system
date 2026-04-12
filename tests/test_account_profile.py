from __future__ import annotations

from pathlib import Path

from src.common.account_profile import apply_account_profile, load_account_profiles, resolved_account_profile_summary
from src.portfolio.investment_allocator import InvestmentExecutionConfig


def test_load_account_profiles_resolves_small_medium_large_bands() -> None:
    profiles = load_account_profiles(Path("."), "config/account_profiles.yaml")
    assert profiles.resolve(10000.0).name == "small"
    assert profiles.resolve(50000.0).name == "medium"
    assert profiles.resolve(300000.0).name == "large"


def test_apply_account_profile_overrides_execution_config_for_small_account() -> None:
    base_cfg = InvestmentExecutionConfig(
        min_trade_value=500.0,
        max_order_value_pct=0.05,
        max_orders_per_run=6,
        account_allocation_pct=0.30,
    )
    effective_cfg, summary = apply_account_profile(
        base_cfg,
        load_account_profiles(Path("."), "config/account_profiles.yaml"),
        broker_equity=10000.0,
    )
    assert summary["name"] == "small"
    assert effective_cfg.min_trade_value == 1000.0
    assert effective_cfg.max_order_value_pct == 0.12
    assert effective_cfg.max_orders_per_run == 5
    assert effective_cfg.account_allocation_pct == 0.25


def test_resolved_account_profile_summary_exposes_preferred_instruments() -> None:
    summary = resolved_account_profile_summary(
        load_account_profiles(Path("."), "config/account_profiles.yaml"),
        broker_equity=200000.0,
    )
    assert summary["label"] == "大资金"
    assert "Large Cap Basket" in summary["preferred_instruments"]
    assert summary["summary_text"].startswith("profile=大资金")
