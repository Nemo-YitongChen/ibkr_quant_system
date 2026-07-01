from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from ..common.cli import build_cli_parser, emit_cli_summary
from ..common.runtime_paths import resolve_repo_path
from ..common.supervisor_runtime_status import (
    build_supervisor_runtime_status,
    supervisor_runtime_markdown,
)

BASE_DIR = Path(__file__).resolve().parents[2]


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _summary_dir(cfg: Dict[str, Any], runtime_root: str) -> Path:
    raw = str(cfg.get("summary_out_dir", "reports_supervisor") or "reports_supervisor")
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    if runtime_root and bool(cfg.get("scope_summary_out_dir", False)):
        return (resolve_repo_path(BASE_DIR, runtime_root) / raw).resolve()
    return resolve_repo_path(BASE_DIR, raw)


def build_parser() -> argparse.ArgumentParser:
    parser = build_cli_parser(
        description="Build a read-only Supervisor runtime status artifact.",
        command="ibkr-quant-supervisor-runtime",
        examples=[
            "python -m src.tools.review_supervisor_runtime --config config/supervisor.yaml --runtime_root runtime_data/paper_investment_only_duq152001",
        ],
        notes=[
            "Does not stop Supervisor, start Supervisor, connect to IBKR, or submit orders.",
        ],
    )
    parser.add_argument("--config", default="config/supervisor.yaml", help="Supervisor config path.")
    parser.add_argument("--runtime_root", default="", help="Optional scoped runtime root.")
    parser.add_argument("--summary_dir", default="", help="Optional explicit reports_supervisor directory.")
    parser.add_argument("--out_dir", default="", help="Output directory. Defaults to summary_dir.")
    return parser


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    cfg_path = resolve_repo_path(BASE_DIR, str(args.config))
    cfg = _load_yaml(cfg_path)
    summary_dir = (
        resolve_repo_path(BASE_DIR, str(args.summary_dir))
        if str(args.summary_dir or "").strip()
        else _summary_dir(cfg, str(args.runtime_root or ""))
    )
    out_dir = resolve_repo_path(BASE_DIR, str(args.out_dir)) if str(args.out_dir or "").strip() else summary_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_supervisor_runtime_status(
        summary_dir=summary_dir,
        config_path=cfg_path,
        repo_root=BASE_DIR,
    )
    json_path = out_dir / "supervisor_runtime_status.json"
    md_path = out_dir / "supervisor_runtime_status.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(supervisor_runtime_markdown(payload), encoding="utf-8")
    emit_cli_summary(
        command="ibkr-quant-supervisor-runtime",
        headline="supervisor runtime status review complete",
        summary={
            "health_status": payload.get("health_status"),
            "supervisor_status": payload.get("supervisor_status"),
            "liveness": payload.get("supervisor_liveness_status"),
            "code_revision_status": payload.get("supervisor_code_revision_status"),
            "next_action": payload.get("next_action"),
            "blocks_recovery_refresh": payload.get("blocks_recovery_refresh"),
        },
        artifacts={"summary_json": json_path, "markdown": md_path},
    )


if __name__ == "__main__":
    main()
