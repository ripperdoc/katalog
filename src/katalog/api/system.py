import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from katalog.api.helpers import ApiError
from katalog.config import current_db_path, current_db_url, current_workspace
from katalog.db.system import get_system_repo
from katalog.db.metadata import sync_config_db
from katalog.plugins.registry import get_actor_instance
from katalog.sources.base import SourcePlugin

router = APIRouter()


async def auth_callback_api(actor: int, authorization_response: str) -> dict[str, str]:
    plugin = await get_actor_instance(actor)
    if not isinstance(plugin, SourcePlugin):
        raise ApiError(status_code=400, detail="Actor is not a source")
    plugin.authorize(authorization_response=authorization_response)
    return {"status": "ok"}


async def sync_config() -> dict[str, str]:
    """Requests to sync config"""
    await sync_config_db()

    return {"status": "ok"}


def _file_size(path: Path | None) -> int:
    if path is None:
        return 0
    try:
        if path.exists() and path.is_file():
            return int(path.stat().st_size)
    except OSError:
        return 0
    return 0


def _scan_path(path: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "path": str(path),
        "exists": False,
        "is_symlink": False,
        "bytes": 0,
        "file_count": 0,
        "directory_count": 0,
        "error_count": 0,
    }
    if not path.exists():
        return stats

    stats["exists"] = True
    if path.is_symlink():
        stats["is_symlink"] = True
        return stats
    if path.is_file():
        stats["file_count"] = 1
        stats["bytes"] = _file_size(path)
        return stats

    stack: list[Path] = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            stats["directory_count"] += 1
                            stack.append(Path(entry.path))
                            continue
                        if entry.is_file(follow_symlinks=False):
                            stats["file_count"] += 1
                            stats["bytes"] += int(
                                entry.stat(follow_symlinks=False).st_size
                            )
                    except OSError:
                        stats["error_count"] += 1
        except OSError:
            stats["error_count"] += 1
    return stats


def _workspace_entries(workspace: Path) -> tuple[list[dict[str, Any]], int]:
    entries: list[dict[str, Any]] = []
    try:
        children = sorted(workspace.iterdir(), key=lambda item: item.name.lower())
    except OSError:
        return entries, 0

    workspace_total_bytes = 0
    for child in children:
        child_stats = _scan_path(child)
        try:
            if child.is_symlink():
                kind = "symlink"
            else:
                kind = "directory" if child.is_dir() else "file"
        except OSError:
            kind = "unknown"
        workspace_total_bytes += int(child_stats.get("bytes") or 0)
        entries.append(
            {
                "name": child.name,
                "kind": kind,
                "bytes": int(child_stats.get("bytes") or 0),
                "file_count": int(child_stats.get("file_count") or 0),
                "directory_count": int(child_stats.get("directory_count") or 0),
                "error_count": int(child_stats.get("error_count") or 0),
            }
        )
    entries.sort(key=lambda row: (-int(row["bytes"]), str(row["name"])))
    return entries, workspace_total_bytes


def _database_file_sizes(db_path: Path | None) -> dict[str, Any]:
    if db_path is None:
        return {
            "db_path": None,
            "db_bytes": 0,
            "wal_path": None,
            "wal_bytes": 0,
            "shm_path": None,
            "shm_bytes": 0,
            "total_bytes": 0,
        }
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    db_bytes = _file_size(db_path)
    wal_bytes = _file_size(wal_path)
    shm_bytes = _file_size(shm_path)
    return {
        "db_path": str(db_path),
        "db_bytes": db_bytes,
        "wal_path": str(wal_path),
        "wal_bytes": wal_bytes,
        "shm_path": str(shm_path),
        "shm_bytes": shm_bytes,
        "total_bytes": db_bytes + wal_bytes + shm_bytes,
    }


async def workspace_size_stats() -> dict[str, Any]:
    workspace = current_workspace()
    db_path = current_db_path()

    db_stats = await get_system_repo().database_size_stats()
    db_files = _database_file_sizes(db_path)
    workspace_entries, workspace_total_bytes = _workspace_entries(workspace)
    cache_stats = _scan_path(workspace / "cache")

    sqlite_stats = db_stats.get("sqlite") or {}
    tables = db_stats.get("tables") or []

    largest_tables = [
        {
            "name": str(table.get("name") or ""),
            "row_count": table.get("row_count"),
            "size_bytes": table.get("size_bytes"),
        }
        for table in tables
    ]
    largest_tables.sort(key=lambda row: -(int(row.get("size_bytes") or 0)))
    largest_tables = largest_tables[:5]

    return {
        "summary": {
            "workspace_total_bytes": workspace_total_bytes,
            "cache_total_bytes": int(cache_stats.get("bytes") or 0),
            "database_disk_total_bytes": int(db_files.get("total_bytes") or 0),
            "database_pages_used_bytes": int(
                sqlite_stats.get("db_pages_used_bytes") or 0
            ),
            "table_count": int(sqlite_stats.get("table_count") or 0),
            "total_table_rows": int(sqlite_stats.get("total_table_rows") or 0),
            "workspace_entry_count": len(workspace_entries),
        },
        "workspace": {
            "path": str(workspace),
            "entries": workspace_entries,
            "cache": cache_stats,
        },
        "database": {
            "url": current_db_url(),
            "files": db_files,
            "sqlite": sqlite_stats,
            "tables": tables,
            "indexes": db_stats.get("indexes") or [],
        },
        "highlights": {
            "largest_tables_by_size": largest_tables,
        },
    }


@router.post("/auth/{actor}")
async def auth_callback(actor: int, request: Request):
    await auth_callback_api(actor, str(request.url))
    return RedirectResponse(url="/", status_code=303)


@router.post("/sync")
async def sync_config_rest():
    return await sync_config()


@router.get("/stats")
async def workspace_size_stats_rest():
    return await workspace_size_stats()
