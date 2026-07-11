from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from ..common.evidence_index_maintenance import apply_evidence_indexes, inspect_evidence_indexes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or explicitly create SQLite indexes required by weekly evidence review.",
    )
    parser.add_argument(
        "--db",
        default="runtime_data/paper_investment_only_duq152001/audit.db",
        help="SQLite audit database path.",
    )
    parser.add_argument(
        "--out_dir",
        default="runtime_data/paper_investment_only_duq152001/reports_supervisor/evidence_index_maintenance",
        help="Output directory for evidence_index_maintenance.json/md.",
    )
    parser.add_argument("--timeout_sec", type=float, default=30.0, help="SQLite connection timeout.")
    parser.add_argument("--apply", action="store_true", help="Create missing indexes. Without this flag the command is read-only.")
    return parser


def _rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if payload.get("applied"):
        after = dict(payload.get("after") or {})
        return [dict(row) for row in list(after.get("rows") or []) if isinstance(row, dict)]
    return [dict(row) for row in list(payload.get("rows") or []) if isinstance(row, dict)]


def _summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("applied"):
        return dict(payload.get("after") or {})
    return payload


def _write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    summary = _summary(payload)
    lines = [
        "# Evidence Index Maintenance",
        "",
        f"- Status: `{summary.get('status') or ''}`",
        f"- Applied: `{bool(payload.get('applied', False))}`",
        f"- DB: `{summary.get('db_path') or payload.get('db_path') or ''}`",
        f"- DB size bytes: `{summary.get('db_size_bytes', 0)}`",
        f"- Missing indexes: `{summary.get('missing_index_count', 0)}`",
        f"- Ready indexes: `{summary.get('ready_index_count', 0)}`",
        f"- Blocked index definitions: `{summary.get('blocked_index_count', 0)}`",
        "",
        "| Index | Table | Status | Missing Columns | Action Required |",
        "|---|---|---:|---|---:|",
    ]
    for row in _rows(payload):
        lines.append(
            "| {index} | {table} | {status} | {missing} | {action} |".format(
                index=row.get("index_name") or "",
                table=row.get("table") or "",
                status=row.get("status") or "",
                missing=row.get("missing_columns") or "",
                action=row.get("action_required"),
            )
        )
    actions = [dict(row) for row in list(payload.get("actions") or []) if isinstance(row, dict)]
    if actions:
        lines.extend(["", "## Apply Actions", "", "| Index | Action | Reason | Elapsed sec |", "|---|---:|---|---:|"])
        for row in actions:
            lines.append(
                "| {index} | {action} | {reason} | {elapsed} |".format(
                    index=row.get("index_name") or "",
                    action=row.get("action") or "",
                    reason=row.get("reason") or "",
                    elapsed=row.get("elapsed_sec", 0.0),
                )
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: List[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if bool(args.apply):
        payload = apply_evidence_indexes(args.db, timeout_sec=float(args.timeout_sec))
    else:
        payload = inspect_evidence_indexes(args.db, timeout_sec=float(args.timeout_sec))
    json_path = out_dir / "evidence_index_maintenance.json"
    md_path = out_dir / "evidence_index_maintenance.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(md_path, payload)
    summary = _summary(payload)
    print(
        "evidence-index-maintenance: "
        f"status={summary.get('status')} "
        f"missing={summary.get('missing_index_count', 0)} "
        f"ready={summary.get('ready_index_count', 0)} "
        f"applied={bool(payload.get('applied', False))}"
    )
    print(f"json={json_path}")


if __name__ == "__main__":
    main()
