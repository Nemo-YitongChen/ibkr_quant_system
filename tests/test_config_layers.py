from __future__ import annotations

from pathlib import Path

from src.common.config_layers import deep_merge_dicts, load_layered_config


def test_deep_merge_dicts_preserves_nested_defaults() -> None:
    merged = deep_merge_dicts(
        {"paper": {"initial_cash": 100000, "risk": {"net": 1.0, "gross": 1.2}}},
        {"paper": {"risk": {"net": 0.8}}},
    )

    assert merged["paper"]["initial_cash"] == 100000
    assert merged["paper"]["risk"]["net"] == 0.8
    assert merged["paper"]["risk"]["gross"] == 1.2


def test_load_layered_config_applies_defaults_and_extends(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    shared = tmp_path / "shared.yaml"
    market = tmp_path / "market.yaml"
    base.write_text(
        "paper:\n  initial_cash: 100000\n  max_holdings: 8\n  risk:\n    net: 1.0\n    gross: 1.2\n",
        encoding="utf-8",
    )
    shared.write_text("paper:\n  risk:\n    gross: 1.1\n", encoding="utf-8")
    market.write_text(
        f"extends: {shared}\npaper:\n  max_holdings: 10\n  risk:\n    net: 0.8\n",
        encoding="utf-8",
    )

    loaded = load_layered_config(tmp_path, str(market), default_paths=(str(base),))

    assert loaded.payload["paper"]["initial_cash"] == 100000
    assert loaded.payload["paper"]["max_holdings"] == 10
    assert loaded.payload["paper"]["risk"]["net"] == 0.8
    assert loaded.payload["paper"]["risk"]["gross"] == 1.1
    assert len(loaded.sources) == 3


def test_project_paper_and_execution_configs_can_load_as_overrides() -> None:
    base_dir = Path(__file__).resolve().parents[1]

    paper = load_layered_config(
        base_dir,
        "config/investment_paper_us.yaml",
        default_paths=("config/investment_paper.yaml",),
    ).payload
    execution = load_layered_config(
        base_dir,
        "config/investment_execution_hk.yaml",
        default_paths=("config/investment_execution.yaml",),
    ).payload

    assert paper["paper"]["initial_cash"] == 100000.0
    assert paper["paper"]["max_sector_weight"] == 0.38
    assert execution["execution"]["execution_hotspot_force_limit_order"] is True
    assert execution["execution"]["allow_min_lot_buy_override"] is True
