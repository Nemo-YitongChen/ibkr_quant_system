from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from src.common.evidence_index_maintenance import apply_evidence_indexes, inspect_evidence_indexes
from src.common.storage import Storage
from src.tools.maintain_evidence_indexes import main as maintain_evidence_indexes_main


FIXED_NOW = datetime(2026, 7, 11, 0, 0, tzinfo=timezone.utc)


def _index_names(db_path, table: str) -> set[str]:
    with sqlite3.connect(str(db_path)) as conn:
        return {str(row[1]) for row in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_inspect_evidence_indexes_reports_missing_without_creating(tmp_path):
    db_path = tmp_path / "audit.db"
    Storage(str(db_path))

    payload = inspect_evidence_indexes(db_path, now=FIXED_NOW)

    assert payload["status"] == "missing_indexes"
    assert payload["missing_index_count"] == 6
    assert "idx_investment_candidate_outcomes_weekly_lookup" not in _index_names(
        db_path,
        "investment_candidate_outcomes",
    )


def test_apply_evidence_indexes_creates_missing_indexes(tmp_path):
    db_path = tmp_path / "audit.db"
    Storage(str(db_path))

    payload = apply_evidence_indexes(db_path, now=FIXED_NOW)

    assert payload["applied"] is True
    assert payload["before"]["missing_index_count"] == 6
    assert payload["after"]["status"] == "ready"
    assert payload["after"]["missing_index_count"] == 0
    assert "idx_investment_candidate_outcomes_weekly_lookup" in _index_names(
        db_path,
        "investment_candidate_outcomes",
    )


def test_maintain_evidence_indexes_cli_writes_dry_run_artifacts(tmp_path):
    db_path = tmp_path / "audit.db"
    out_dir = tmp_path / "out"
    Storage(str(db_path))

    maintain_evidence_indexes_main(["--db", str(db_path), "--out_dir", str(out_dir)])

    payload = json.loads((out_dir / "evidence_index_maintenance.json").read_text(encoding="utf-8"))
    assert payload["status"] == "missing_indexes"
    assert payload["missing_index_count"] == 6
    assert (out_dir / "evidence_index_maintenance.md").exists()
    assert "idx_fills_weekly_lookup" not in _index_names(db_path, "fills")
