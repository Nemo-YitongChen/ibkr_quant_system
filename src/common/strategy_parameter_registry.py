from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml

from .runtime_paths import resolve_repo_path


DEFAULT_STRATEGY_PARAMETER_REGISTRY = "config/strategy_parameter_registry.yaml"


@dataclass(frozen=True)
class StrategyParameterRegistry:
    fields: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    priorities: Dict[str, Dict[str, Tuple[int, str]]] = field(default_factory=dict)


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def load_strategy_parameter_registry(
    base_dir: Path,
    explicit_path: str | None = None,
) -> StrategyParameterRegistry:
    raw_path = str(explicit_path or DEFAULT_STRATEGY_PARAMETER_REGISTRY)
    payload = _read_yaml(resolve_repo_path(base_dir, raw_path))
    fields = {
        str(name).strip(): dict(meta or {})
        for name, meta in dict(payload.get("fields") or {}).items()
        if str(name).strip() and isinstance(meta, dict)
    }
    priorities: Dict[str, Dict[str, Tuple[int, str]]] = {}
    for scope, field_rows in dict(payload.get("priorities") or {}).items():
        scope_key = str(scope or "").strip().upper()
        if not scope_key or not isinstance(field_rows, dict):
            continue
        priorities[scope_key] = {}
        for field_name, raw_meta in dict(field_rows or {}).items():
            meta = dict(raw_meta or {}) if isinstance(raw_meta, dict) else {}
            priorities[scope_key][str(field_name).strip()] = (
                int(meta.get("rank", 9) or 9),
                str(meta.get("label", "后续再评估") or "后续再评估"),
            )
    return StrategyParameterRegistry(fields=fields, priorities=priorities)


def strategy_parameter_field_meta(
    field: str,
    *,
    registry: StrategyParameterRegistry | None = None,
) -> Dict[str, Any]:
    if registry is None:
        return {}
    return dict(registry.fields.get(str(field or "").strip()) or {})


def strategy_parameter_priority(
    scope: str,
    field: str,
    *,
    registry: StrategyParameterRegistry | None = None,
) -> Tuple[int, str]:
    if registry is None:
        return 9, "后续再评估"
    scope_code = str(scope or "").strip().upper()
    field_name = str(field or "").strip()
    return registry.priorities.get(scope_code, {}).get(field_name, (9, "后续再评估"))


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def strategy_parameter_proposed_value(
    field: str,
    current_value: Any,
    change_hint: str,
    *,
    registry: StrategyParameterRegistry | None = None,
) -> Any:
    meta = strategy_parameter_field_meta(field, registry=registry)
    if not meta:
        return current_value
    try:
        current = float(current_value)
    except Exception:
        return current_value
    step = float(meta.get("step", 0.0) or 0.0)
    raw_bounds = list(meta.get("bounds", [-1e9, 1e9]) or [-1e9, 1e9])
    lower = float(raw_bounds[0]) if raw_bounds else -1e9
    upper = float(raw_bounds[1]) if len(raw_bounds) > 1 else 1e9
    precision = int(meta.get("precision", 4) or 4)
    direction = str(change_hint or "").strip().upper()
    if direction in {"RELAX_LOWER", "LOWER", "REDUCE", "RECALIBRATE_RELAX"}:
        proposed = current - step
    elif direction in {"INCREASE", "HIGHER", "TIGHTEN_HIGHER"}:
        proposed = current + step
    else:
        proposed = current
    proposed = _clamp(float(proposed), lower, upper)
    if precision <= 0:
        return int(round(proposed))
    return round(float(proposed), precision)
