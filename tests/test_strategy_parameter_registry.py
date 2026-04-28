from __future__ import annotations

from pathlib import Path

from src.common.strategy_parameter_registry import (
    load_strategy_parameter_registry,
    strategy_parameter_field_meta,
    strategy_parameter_priority,
    strategy_parameter_proposed_value,
)


def test_strategy_parameter_registry_loads_project_defaults() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    registry = load_strategy_parameter_registry(base_dir)

    edge_meta = strategy_parameter_field_meta("edge_cost_buffer_bps", registry=registry)
    assert edge_meta["field_label"] == "edge cost buffer"
    assert edge_meta["step"] == 1.0
    assert strategy_parameter_priority("EXECUTION_GATE", "edge_cost_buffer_bps", registry=registry) == (
        1,
        "先改低风险 buffer",
    )


def test_strategy_parameter_proposed_value_uses_bounds_and_precision() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    registry = load_strategy_parameter_registry(base_dir)

    assert strategy_parameter_proposed_value("edge_cost_buffer_bps", 5.0, "RELAX_LOWER", registry=registry) == 4.0
    assert strategy_parameter_proposed_value("edge_cost_buffer_bps", 1.0, "RELAX_LOWER", registry=registry) == 1.0
    assert strategy_parameter_proposed_value(
        "regime_risk_on_threshold",
        0.50,
        "RECALIBRATE_RELAX",
        registry=registry,
    ) == 0.48
    assert strategy_parameter_proposed_value("max_slices_per_symbol", 3, "INCREASE", registry=registry) == 4


def test_strategy_parameter_registry_supports_custom_override_file(tmp_path: Path) -> None:
    registry_path = tmp_path / "strategy_parameter_registry.yaml"
    registry_path.write_text(
        "\n".join(
            [
                "fields:",
                "  no_trade_band_pct:",
                '    field_label: "no-trade band"',
                "    step: 0.01",
                "    bounds: [0.01, 0.10]",
                "    precision: 3",
                "priorities:",
                "  REGIME_PLAN:",
                "    no_trade_band_pct:",
                "      rank: 1",
                '      label: "先改 no-trade band"',
            ]
        ),
        encoding="utf-8",
    )
    registry = load_strategy_parameter_registry(tmp_path, str(registry_path))

    assert strategy_parameter_proposed_value("no_trade_band_pct", 0.03, "INCREASE", registry=registry) == 0.04
    assert strategy_parameter_priority("REGIME_PLAN", "no_trade_band_pct", registry=registry) == (
        1,
        "先改 no-trade band",
    )
