from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from .runtime_paths import resolve_repo_path


@dataclass(frozen=True)
class LayeredConfig:
    payload: Dict[str, Any]
    sources: Tuple[str, ...]


def deep_merge_dicts(base: Dict[str, Any] | None, override: Dict[str, Any] | None) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base or {})
    for key, value in dict(override or {}).items():
        if key in {"extends", "defaults"}:
            continue
        if isinstance(out.get(key), dict) and isinstance(value, dict):
            out[key] = deep_merge_dicts(dict(out.get(key) or {}), dict(value or {}))
        else:
            out[key] = value
    return out


def _read_yaml(path: Path) -> Dict[str, Any]:
    import yaml

    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _as_path_list(value: Any) -> Tuple[str, ...]:
    if value in (None, "", []):
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value if str(item or "").strip())
    return ()


def load_layered_config(
    base_dir: Path,
    primary_path: str,
    *,
    default_paths: Iterable[str] = (),
) -> LayeredConfig:
    merged: Dict[str, Any] = {}
    sources: list[str] = []

    def _merge_path(raw_path: str) -> None:
        nonlocal merged
        path = resolve_repo_path(base_dir, str(raw_path or ""))
        payload = _read_yaml(path)
        if not payload and not path.exists():
            return
        for parent_path in _as_path_list(payload.get("defaults")) + _as_path_list(payload.get("extends")):
            _merge_path(parent_path)
        merged = deep_merge_dicts(merged, payload)
        sources.append(str(path))

    seen_defaults = set()
    for raw_default in list(default_paths or []):
        default_text = str(raw_default or "").strip()
        if not default_text or default_text in seen_defaults:
            continue
        seen_defaults.add(default_text)
        _merge_path(default_text)
    _merge_path(primary_path)
    return LayeredConfig(payload=merged, sources=tuple(sources))
