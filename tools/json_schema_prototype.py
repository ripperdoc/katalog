"""Standalone prototype of the single-table + JSON metadata schema.

This file is intentionally disconnected from the rest of the codebase. It defines
the proposed schema using Tortoise ORM and exposes a minimal query helper that
supports filtering and sorting (no FTS/VSS here). It is meant for experimentation
and can be run against an in-memory SQLite database.

When executed, this module now starts a small FastAPI app that exposes a single
`/query` endpoint for experimenting with filters and sorts against the prototype
database. It does not integrate with the rest of the application.
"""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from typing import Any, List, Literal, Optional, Sequence, Tuple

from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel, Field
import uvicorn

from tortoise import Tortoise, fields
from tortoise.models import Model


class AssetRecord(Model):
    """Append-only asset record with actor-scoped metadata JSON."""

    id = fields.IntField(pk=True)
    asset_id = fields.CharField(max_length=36, index=True)
    actor_id = fields.CharField(max_length=64, index=True)
    changeset_id = fields.IntField(index=True)
    tombstone = fields.BooleanField(default=False)
    metadata_json = fields.JSONField()
    content_hash = fields.CharField(
        max_length=40, null=True, description="SHA1 of metadata_json"
    )
    created_at = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "asset_record"
        indexes = [
            ("asset_id", "actor_id", "changeset_id"),
            ("actor_id", "asset_id"),
            ("changeset_id",),
        ]


async def init_db(db_url: str = "sqlite://:memory:") -> None:
    """Initialize SQLite + JSON1 and create the asset_current view.

    Safe to call multiple times; the view/index creation statements are idempotent.
    """

    await Tortoise.init(db_url=db_url, modules={"models": [__name__]})
    await Tortoise.generate_schemas()

    # View selecting latest non-tombstoned row per (asset, actor).
    await AssetRecord.raw(
        """
        CREATE VIEW IF NOT EXISTS asset_current AS
        SELECT r.*
        FROM asset_record r
        WHERE r.tombstone = 0
          AND r.changeset_id = (
            SELECT MAX(r2.changeset_id)
            FROM asset_record r2
            WHERE r2.asset_id = r.asset_id
              AND r2.actor_id = r.actor_id
          );
        """
    )

    # Partial index to speed lookups of live rows.
    await AssetRecord.raw(
        """
        CREATE INDEX IF NOT EXISTS idx_asset_current_live
          ON asset_record(asset_id, actor_id, changeset_id)
          WHERE tombstone = 0;
        """
    )

    # Expression index to speed sorting/filtering on common JSON keys.
    # Example: ORDER BY json_extract(metadata_json, '$."file/size".v[0]') DESC
    await AssetRecord.raw(
        """
        CREATE INDEX IF NOT EXISTS idx_asset_record_file_size
          ON asset_record(
            json_extract(metadata_json, '$."file/size".v[0]')
          )
          WHERE tombstone = 0;
        """
    )


def _stable_hash(json_obj: object) -> str:
    """Deterministic SHA1 of JSON-serializable object using sorted keys."""

    import json

    payload = json.dumps(json_obj, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha1(payload).hexdigest()


async def append_version(
    *,
    asset_id: str,
    actor_id: str,
    changeset_id: int,
    metadata_json: dict,
    tombstone: bool = False,
) -> Optional[AssetRecord]:
    """Append a new version if metadata or tombstone changed.

    Returns the created AssetRecord or None if skipped because nothing changed.
    """

    latest = await (
        AssetRecord.filter(asset_id=asset_id, actor_id=actor_id)
        .order_by("-changeset_id")
        .first()
    )

    content_hash = _stable_hash(metadata_json)

    if latest:
        if bool(latest.tombstone) == tombstone and latest.content_hash == content_hash:
            return None  # no-op

    record = await AssetRecord.create(
        asset_id=asset_id,
        actor_id=actor_id,
        changeset_id=changeset_id,
        tombstone=tombstone,
        metadata_json=metadata_json,
        content_hash=content_hash,
    )
    return record


FilterOp = Literal["=", "!=", ">", ">=", "<", "<=", "like"]
Filter = Tuple[str, FilterOp, object]  # (metadata_key, op, value)


async def query_assets(
    *,
    filters: Sequence[Filter] = (),
    sort_by: str = "changeset_id",
    sort_dir: Literal["asc", "desc"] = "desc",
    limit: int = 200,
    offset: int = 0,
    use_current_view: bool = True,
) -> List[dict]:
    """Query assets with JSON-based filters and sorting.

    - filters operate on metadata_json via json_extract using dot paths like "core/path".
    - sort_by can be any column of the view/table or json_extract path (prefix with "json:").
    - when use_current_view is True, queries run against asset_current (latest per actor).
    """

    base = "asset_current" if use_current_view else "asset_record"
    where_clauses: List[str] = []
    params: List[object] = []

    for key, op, value in filters:
        json_path = f'$."{key}".v[0]'
        where_clauses.append(f"json_extract(metadata_json, ?) {op} ?")
        params.extend([json_path, value])

    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    if sort_by.startswith("json:"):
        json_key = sort_by.split(":", 1)[1]
        order_expr = f"json_extract(metadata_json, '$.\"{json_key}\".v[0]')"
    else:
        order_expr = sort_by

    order_sql = f" ORDER BY {order_expr} {sort_dir.upper()}"
    limit_sql = " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    sql = f"SELECT * FROM {base}{where_sql}{order_sql}{limit_sql}"
    start = time.perf_counter()
    conn = AssetRecord._meta.db
    rows = await conn.execute_query_dict(sql, params)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.debug("SQL query finished in {:.1f} ms", elapsed_ms)
    return rows


def _default_db_url() -> str:
    """Resolve default DB path (env override allowed)."""

    env_path = os.getenv("KATALOG_DB")
    if env_path:
        return f"sqlite:///{env_path}"

    repo_root = Path(__file__).resolve().parent.parent
    db_path = repo_root / "test_workspace" / "katalog.db"
    return f"sqlite:///{db_path}"


class FilterModel(BaseModel):
    key: str = Field(..., description='Metadata key, e.g. "file/path"')
    op: FilterOp
    value: Any


class QueryRequest(BaseModel):
    filters: List[FilterModel] = Field(default_factory=list)
    sort_by: str = "changeset_id"
    sort_dir: Literal["asc", "desc"] = "desc"
    limit: int = 200
    offset: int = 0
    use_current_view: bool = True


class QueryResponse(BaseModel):
    rows: List[dict]
    elapsed_ms: float
    count: int


app = FastAPI(title="Katalog JSON Schema Prototype", version="0.1.0")


@app.on_event("startup")
async def _startup() -> None:
    db_url = _default_db_url()
    if db_url.startswith("sqlite:///"):
        db_path = Path(db_url.replace("sqlite:///", ""))
        if not db_path.exists():
            raise RuntimeError(
                f"Database not found at {db_path}. Set KATALOG_DB env var or create the file."
            )
    await init_db(db_url=db_url)
    logger.info("FastAPI prototype started with db_url={}", db_url)


@app.on_event("shutdown")
async def _shutdown() -> None:
    await Tortoise.close_connections()
    logger.info("Closed database connections")


@app.post("/query", response_model=QueryResponse)
async def query_endpoint(payload: QueryRequest) -> QueryResponse:
    """Execute a filtered/sorted asset query against the prototype DB."""

    filter_tuples: List[Filter] = [
        (flt.key, flt.op, flt.value) for flt in payload.filters
    ]

    start = time.perf_counter()
    try:
        rows = await query_assets(
            filters=filter_tuples,
            sort_by=payload.sort_by,
            sort_dir=payload.sort_dir,
            limit=payload.limit,
            offset=payload.offset,
            use_current_view=payload.use_current_view,
        )
    except Exception as exc:  # pragma: no cover - prototype endpoint
        logger.exception("Query failed")
        raise HTTPException(status_code=400, detail=str(exc))

    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "Query completed in {:.1f} ms ({} rows, sort_by={}, sort_dir={})",
        elapsed_ms,
        len(rows),
        payload.sort_by,
        payload.sort_dir,
    )

    return QueryResponse(rows=rows, elapsed_ms=elapsed_ms, count=len(rows))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001, reload=False)
