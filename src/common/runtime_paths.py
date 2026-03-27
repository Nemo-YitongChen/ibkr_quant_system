from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class RuntimeScope:
    mode: str
    execution_mode: str
    account_id: str

    @property
    def label(self) -> str:
        return "_".join(
            [
                _slugify_part(self.mode or "unknown"),
                _slugify_part(self.execution_mode or "intraday"),
                _slugify_part(self.account_id or "unknown_account"),
            ]
        )

    def root(self, base_dir: Path) -> Path:
        return (base_dir / "runtime_data" / self.label).resolve()


def scope_from_ibkr_config(cfg: Dict[str, Any]) -> RuntimeScope:
    return RuntimeScope(
        mode=str(cfg.get("mode", "") or "unknown").strip().lower(),
        execution_mode=str(cfg.get("execution_mode", "") or "intraday").strip().lower(),
        account_id=str(cfg.get("account_id", "") or "unknown_account").strip(),
    )


def resolve_repo_path(base_dir: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path.resolve()
    for candidate in (base_dir / path, base_dir / "config" / path, Path.cwd() / path, Path.cwd() / "config" / path):
        if candidate.exists():
            return candidate.resolve()
    return (base_dir / path).resolve()


def resolve_scoped_runtime_path(base_dir: Path, path_str: str, scope: RuntimeScope) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path.resolve()
    return (scope.root(base_dir) / path).resolve()


def _slugify_part(value: str) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "default"
