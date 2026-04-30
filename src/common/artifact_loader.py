from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping

from .artifact_contracts import ArtifactContract


@dataclass
class LoadedArtifact:
    artifact_key: str
    label: str
    format: str
    path: str
    exists: bool
    source: str
    payload: Any
    columns: List[str]
    row_count: int
    file_mtime: str
    file_mtime_ts: float | None
    generated_at: str
    generated_at_source: str
    schema_version: str
    schema_version_source: str


def _iso_from_ts(ts: float | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return ""


def _path_state(path: Path) -> tuple[str, str, float | None]:
    mtime_ts = path.stat().st_mtime if path.exists() else None
    return str(path), _iso_from_ts(mtime_ts), mtime_ts


def _read_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(raw) if isinstance(raw, dict) else {}


def _read_csv_rows(path: Path) -> tuple[List[Dict[str, Any]], List[str]]:
    if not path.exists():
        return [], []
    try:
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
            columns = list(reader.fieldnames or [])
    except Exception:
        return [], []
    return rows, columns


def _rows_columns(rows: List[Dict[str, Any]]) -> List[str]:
    columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if str(key) not in columns:
                columns.append(str(key))
    return columns


def _json_row_payload(section: List[Dict[str, Any]]) -> tuple[Dict[str, Any], List[str]]:
    rows = [dict(row) for row in list(section or []) if isinstance(row, dict)]
    return {"row_count": len(rows), "rows": rows}, _rows_columns(rows)


def _payload_row_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, Mapping) or not payload:
        return 0
    rows = payload.get("rows")
    if isinstance(rows, list):
        return len(rows)
    declared = payload.get("row_count")
    try:
        if declared not in (None, ""):
            return int(float(declared))
    except Exception:
        pass
    return 1


def _pick_text_field(payload: Any, field_names: tuple[str, ...]) -> str:
    if not isinstance(payload, Mapping):
        return ""
    for field in field_names:
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _load_fallback_section(
    base_dir: Path,
    *,
    fallback_filename: str,
    fallback_section: str,
    format: str,
    loaded: Mapping[str, LoadedArtifact] | None = None,
) -> tuple[Any, List[str], bool, str, str, float | None]:
    fallback_path = base_dir / fallback_filename
    path_text, fallback_mtime, fallback_mtime_ts = _path_state(fallback_path)
    fallback_payload = _read_json_dict(fallback_path)
    if loaded:
        for artifact in loaded.values():
            if Path(str(artifact.path or "")).name == fallback_filename and isinstance(artifact.payload, Mapping):
                fallback_payload = dict(artifact.payload)
                break
    section = fallback_payload.get(fallback_section)
    if format == "json":
        if isinstance(section, dict):
            return dict(section), list(section.keys()), True, path_text, fallback_mtime, fallback_mtime_ts
        if isinstance(section, list):
            payload, columns = _json_row_payload(section)
            return payload, columns, True, path_text, fallback_mtime, fallback_mtime_ts
        return {}, [], False, path_text, fallback_mtime, fallback_mtime_ts
    if isinstance(section, list):
        rows = [dict(row) for row in section if isinstance(row, dict)]
        columns = _rows_columns(rows)
        return rows, columns, True, path_text, fallback_mtime, fallback_mtime_ts
    return [], [], False, path_text, fallback_mtime, fallback_mtime_ts


def load_artifact(
    base_dir: Path,
    contract: ArtifactContract,
    *,
    loaded: Mapping[str, LoadedArtifact] | None = None,
) -> LoadedArtifact:
    path = base_dir / contract.filename
    payload: Any = {} if contract.format == "json" else []
    columns: List[str] = []
    exists = False
    source = "missing"
    file_mtime_ts = path.stat().st_mtime if path.exists() else None
    file_mtime = _iso_from_ts(file_mtime_ts)

    if contract.format == "json":
        payload = _read_json_dict(path)
        exists = path.exists()
        source = "file" if exists else "missing"
    else:
        payload, columns = _read_csv_rows(path)
        exists = path.exists()
        source = "file" if exists else "missing"

    if not exists and contract.fallback_filename and contract.fallback_section:
        payload, columns, exists, fallback_path, fallback_mtime, fallback_mtime_ts = _load_fallback_section(
            base_dir,
            fallback_filename=contract.fallback_filename,
            fallback_section=contract.fallback_section,
            format=contract.format,
            loaded=loaded,
        )
        if exists:
            path = Path(fallback_path)
            file_mtime = fallback_mtime
            file_mtime_ts = fallback_mtime_ts
            source = f"fallback:{contract.fallback_section}"

    generated_at = _pick_text_field(payload, contract.generated_at_fields)
    generated_at_source = "payload" if generated_at else ""
    if not generated_at and contract.inherit_generated_at_from and loaded:
        inherited = loaded.get(contract.inherit_generated_at_from)
        if inherited and str(inherited.generated_at or "").strip():
            generated_at = str(inherited.generated_at or "")
            generated_at_source = f"inherited:{contract.inherit_generated_at_from}"
    if not generated_at and file_mtime:
        generated_at = file_mtime
        generated_at_source = "file_mtime"

    schema_version = _pick_text_field(payload, contract.schema_version_fields)
    schema_version_source = "payload" if schema_version else ""
    if not schema_version and contract.inherit_schema_version_from and loaded:
        inherited = loaded.get(contract.inherit_schema_version_from)
        if inherited and str(inherited.schema_version or "").strip():
            schema_version = str(inherited.schema_version or "")
            schema_version_source = f"inherited:{contract.inherit_schema_version_from}"

    row_count = _payload_row_count(payload)
    if not columns and isinstance(payload, list):
        columns = _rows_columns([dict(row) for row in payload if isinstance(row, Mapping)])
    if not columns and isinstance(payload, Mapping) and isinstance(payload.get("rows"), list):
        columns = _rows_columns([dict(row) for row in list(payload.get("rows") or []) if isinstance(row, Mapping)])

    return LoadedArtifact(
        artifact_key=contract.artifact_key,
        label=contract.label,
        format=contract.format,
        path=str(path),
        exists=bool(exists),
        source=source,
        payload=payload,
        columns=columns,
        row_count=int(row_count),
        file_mtime=file_mtime,
        file_mtime_ts=file_mtime_ts,
        generated_at=generated_at,
        generated_at_source=generated_at_source,
        schema_version=schema_version,
        schema_version_source=schema_version_source,
    )


def load_artifact_set(
    base_dir: Path,
    contracts: Mapping[str, ArtifactContract],
) -> Dict[str, LoadedArtifact]:
    loaded: Dict[str, LoadedArtifact] = {}
    for key, contract in contracts.items():
        loaded[key] = load_artifact(base_dir, contract, loaded=loaded)
    return loaded
