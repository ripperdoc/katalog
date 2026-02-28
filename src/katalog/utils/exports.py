from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable

from katalog.config import current_workspace


def build_tables_from_stats(stats: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    tables: dict[str, list[dict[str, Any]]] = {}

    summary = stats.get("summary") or {}
    summary_rows = _flatten_to_rows(summary)
    if summary_rows:
        tables["summary"] = summary_rows

    breakdowns = stats.get("breakdowns") or {}
    for name, rows in breakdowns.items():
        if isinstance(rows, list):
            tables[f"breakdown_{name}"] = _normalize_rows(rows)

    coverage = stats.get("coverage") or {}
    if isinstance(coverage, dict):
        coverage_rows = []
        for key, entry in coverage.items():
            if not isinstance(entry, dict):
                continue
            coverage_rows.append(
                {
                    "metric": key,
                    "present": entry.get("present"),
                    "missing": entry.get("missing"),
                }
            )
        if coverage_rows:
            tables["coverage"] = coverage_rows

    duplicates = stats.get("duplicates") or {}
    if isinstance(duplicates, dict) and duplicates:
        tables["duplicates"] = [duplicates]

    largest_assets = stats.get("largest_assets")
    if isinstance(largest_assets, list) and largest_assets:
        tables["largest_assets"] = _normalize_rows(largest_assets)

    return tables


def write_csv_tables(
    tables: dict[str, list[dict[str, Any]]],
    *,
    prefix: str,
    directory: Path | None = None,
) -> list[Path]:
    workspace = current_workspace()
    if directory is None and workspace is None:
        raise ValueError("Workspace is not configured for exports")
    export_dir = directory or (workspace / "exports")
    export_dir.mkdir(parents=True, exist_ok=True)

    paths: list[Path] = []
    for table_name, rows in tables.items():
        if not rows:
            continue
        safe_name = _safe_filename(table_name)
        base_path = export_dir / f"{prefix}_{safe_name}.csv"
        path = _unique_path(base_path)
        _write_csv(path, rows)
        paths.append(path)
    return paths


def analyzer_export_dir(
    *,
    changeset_id: int,
    analyzer_plugin_id: str,
    actor_id: int | None = None,
    workspace: Path | None = None,
) -> Path:
    """
    Return a standard export directory for analyzer runs.

    Current layout:
    <workspace>/exports/<changeset_id>/<analyzer_slug>[_actor-<id>]/
    """
    active_workspace = workspace or current_workspace()
    if active_workspace is None:
        raise ValueError("Workspace is not configured for exports")
    root = active_workspace / "exports" / str(int(changeset_id))
    slug = _safe_filename(analyzer_plugin_id)
    name = f"{slug}_actor-{actor_id}" if actor_id is not None else slug
    target = root / name
    target.mkdir(parents=True, exist_ok=True)
    return target


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = _collect_headers(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: _format_cell(row.get(name)) for name in fieldnames})


def _collect_headers(rows: Iterable[dict[str, Any]]) -> list[str]:
    seen: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.append(key)
    return seen


def _format_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return str(value)
    return value


def _safe_filename(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in name)
    return cleaned.strip("_") or "table"


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _flatten_to_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, value in payload.items():
        if isinstance(value, dict):
            nested = _flatten_dict(value, prefix=key)
            for nested_key, nested_value in nested.items():
                rows.append({"metric": nested_key, "value": nested_value})
        else:
            rows.append({"metric": key, "value": value})
    return rows


def _flatten_dict(payload: dict[str, Any], *, prefix: str) -> dict[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in payload.items():
        label = f"{prefix}.{key}"
        if isinstance(value, dict):
            flattened.update(_flatten_dict(value, prefix=label))
        else:
            flattened[label] = value
    return flattened


def _normalize_rows(rows: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized.append(row)
        else:
            normalized.append({"value": row})
    return normalized
