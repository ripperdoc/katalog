from __future__ import annotations

import json
import time
from typing import Any, Iterable, Mapping

from katalog.constants.metadata import get_metadata_def_by_id
from katalog.db.utils import build_where
from katalog.db.sqlspec.sql_helpers import execute, select
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import (
    CHANGESET_ACTOR_TABLE,
    CHANGESET_TABLE,
    METADATA_TABLE,
)
from katalog.db.sqlspec.query_values import decode_metadata_value
from katalog.models.core import Actor, Changeset, OpStatus
from katalog.models.query import ChangesetChangesResponse


class SqlspecChangesetRepo:
    async def get_or_none(self, **filters: Any) -> Changeset | None:
        rows = await self.list_rows(limit=1, **filters)
        return rows[0] if rows else None

    async def get(self, **filters: Any) -> Changeset:
        changeset = await self.get_or_none(**filters)
        if changeset is None:
            raise ValueError("Changeset not found")
        return changeset

    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Changeset]:
        where_sql, params = build_where(filters)
        order_sql = f"ORDER BY {order_by}" if order_by else ""
        limit_sql = "LIMIT :limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, message, running_time_ms, status, data "
            f"FROM {CHANGESET_TABLE} {where_sql} {order_sql} {limit_sql}"
        )
        async with session_scope() as session:
            rows = await select(session, sql, params)
        return [Changeset.model_validate(_normalize_changeset_row(row)) for row in rows]

    async def list_for_actor(self, actor_id: int) -> list[Changeset]:
        sql = f"""
        SELECT c.id, c.message, c.running_time_ms, c.status, c.data
        FROM {CHANGESET_TABLE} c
        JOIN {CHANGESET_ACTOR_TABLE} ca ON ca.changeset_id = c.id
        WHERE ca.actor_id = :actor_id
        ORDER BY c.id DESC
        """
        async with session_scope() as session:
            rows = await select(session, sql, {"actor_id": int(actor_id)})
        return [Changeset.model_validate(_normalize_changeset_row(row)) for row in rows]

    async def begin(
        self,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        data: Mapping[str, Any] | None = None,
        actors: Iterable[Actor] | None = None,
        message: str | None = None,
    ) -> Changeset:
        existing_in_progress = await self.get_or_none(status=OpStatus.IN_PROGRESS)
        if existing_in_progress is not None:
            raise ValueError(
                f"Changeset {existing_in_progress.id} is already in progress; finish or cancel it first"
            )
        changeset_id = int(time.time() * 1000)
        if await self.get_or_none(id=changeset_id):
            raise ValueError(f"Changeset with id {changeset_id} already exists")
        changeset = await self.create(
            id=changeset_id,
            status=status,
            message=message,
            data=dict(data) if data else None,
        )
        actor_list = list(actors or [])
        await self.add_actors(changeset, actor_list)
        if actor_list:
            changeset.actor_ids = [int(a.id) for a in actor_list if a.id is not None]
        return changeset

    async def create(
        self,
        *,
        id: int,
        status: OpStatus,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> Changeset:
        async with session_scope() as session:
            await execute(
                session,
                f"""
                INSERT INTO {CHANGESET_TABLE} (id, message, running_time_ms, status, data)
                VALUES (:id, :message, :running_time_ms, :status, :data)
                """,
                {
                    "id": int(id),
                    "message": message,
                    "running_time_ms": None,
                    "status": status.value
                    if isinstance(status, OpStatus)
                    else str(status),
                    "data": data,
                },
            )
            await session.commit()
        return await self.get(id=id)

    async def create_auto(
        self,
        *,
        status: OpStatus,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> Changeset:
        changeset_id = int(time.time() * 1000)
        if await self.get_or_none(id=changeset_id):
            raise ValueError(f"Changeset with id {changeset_id} already exists")
        return await self.create(
            id=changeset_id,
            status=status,
            message=message,
            data=data,
        )

    async def add_actors(self, changeset: Changeset, actors: Iterable[Actor]) -> None:
        actor_list = [actor for actor in actors if actor.id is not None]
        if not actor_list:
            return
        async with session_scope() as session:
            existing_rows = await select(
                session,
                f"""
                SELECT actor_id FROM {CHANGESET_ACTOR_TABLE}
                WHERE changeset_id = :changeset_id
                """,
                {"changeset_id": int(changeset.id)},
            )
            existing = {int(row["actor_id"]) for row in existing_rows}
            payload = []
            for actor in actor_list:
                actor_id = actor.id
                if actor_id is None:
                    continue
                if int(actor_id) in existing:
                    continue
                payload.append(
                    {
                        "changeset_id": int(changeset.id),
                        "actor_id": int(actor_id),
                    }
                )
            if payload:
                await session.execute_many(
                    f"""
                    INSERT INTO {CHANGESET_ACTOR_TABLE} (changeset_id, actor_id)
                    VALUES (:changeset_id, :actor_id)
                    """,
                    payload,
                )
                await session.commit()
        if actor_list:
            current = changeset.actor_ids or []
            updated = set(current)
            for actor in actor_list:
                if actor.id is not None:
                    updated.add(int(actor.id))
            changeset.actor_ids = sorted(updated)

    async def load_actor_ids(self, changeset: Changeset) -> list[int]:
        async with session_scope() as session:
            rows = await select(
                session,
                f"""
                SELECT actor_id FROM {CHANGESET_ACTOR_TABLE}
                WHERE changeset_id = :changeset_id
                ORDER BY actor_id
                """,
                {"changeset_id": int(changeset.id)},
            )
        changeset.actor_ids = [int(row["actor_id"]) for row in rows]
        return changeset.actor_ids

    async def save(
        self, changeset: Changeset, *, update_data: dict[str, Any] | None = None
    ) -> None:
        async with session_scope() as session:
            await execute(
                session,
                f"""
                UPDATE {CHANGESET_TABLE}
                SET message = :message,
                    running_time_ms = :running_time_ms,
                    status = :status,
                    data = :data
                WHERE id = :id
                """,
                {
                    "id": int(changeset.id),
                    "message": changeset.message,
                    "running_time_ms": changeset.running_time_ms,
                    "status": changeset.status.value
                    if isinstance(changeset.status, OpStatus)
                    else str(changeset.status),
                    "data": update_data if update_data is not None else changeset.data,
                },
            )
            await session.commit()

    async def delete(self, changeset: Changeset) -> None:
        async with session_scope() as session:
            await execute(
                session,
                f"DELETE FROM {CHANGESET_TABLE} WHERE id = :id",
                {"id": int(changeset.id)},
            )
            await session.commit()

    async def list_changeset_metadata_changes(
        self,
        changeset_id: int,
        *,
        offset: int = 0,
        limit: int = 200,
        include_total: bool = True,
    ) -> "ChangesetChangesResponse":
        started_at = time.perf_counter()
        if limit < 0 or offset < 0:
            raise ValueError("offset and limit must be non-negative")

        sql = f"""
        SELECT
            id,
            asset_id,
            actor_id,
            changeset_id,
            metadata_key_id,
            value_type,
            value_text,
            value_int,
            value_real,
            value_datetime,
            value_json,
            value_relation_id,
            value_collection_id,
            removed
        FROM {METADATA_TABLE}
        WHERE changeset_id = ?
        ORDER BY id
        LIMIT ? OFFSET ?
        """
        params = [changeset_id, limit, offset]

        async with session_scope(analysis=True) as session:
            rows_started = time.perf_counter()
            rows = await select(session, sql, params)
            duration_rows_ms = int((time.perf_counter() - rows_started) * 1000)

            items: list[dict[str, Any]] = []
            for row in rows:
                try:
                    registry = get_metadata_def_by_id(int(row["metadata_key_id"]))
                    key_str = str(registry.key)
                except KeyError:
                    key_str = f"id:{row['metadata_key_id']}"
                items.append(
                    {
                        "id": int(row["id"]),
                        "asset_id": int(row["asset_id"]),
                        "actor_id": int(row["actor_id"]),
                        "changeset_id": int(row["changeset_id"]),
                        "metadata_key": key_str,
                        "metadata_key_id": int(row["metadata_key_id"]),
                        "value_type": int(row["value_type"]),
                        "value": decode_metadata_value(row),
                        "removed": bool(row["removed"]),
                    }
                )

            total_count = None
            if include_total:
                count_sql = (
                    f"SELECT COUNT(*) AS cnt FROM {METADATA_TABLE} "
                    f"WHERE changeset_id = ?"
                )
                count_started = time.perf_counter()
                count_rows = await select(session, count_sql, [changeset_id])
                count_duration_ms = int((time.perf_counter() - count_started) * 1000)
                total_count = int(count_rows[0]["cnt"]) if count_rows else 0
            else:
                count_duration_ms = None

        duration_ms = int((time.perf_counter() - started_at) * 1000)

        return ChangesetChangesResponse.model_validate(
            {
                "items": items,
                "stats": {
                    "returned": len(items),
                    "total": total_count,
                    "duration_ms": duration_ms,
                    "duration_rows_ms": duration_rows_ms,
                    "duration_count_ms": count_duration_ms,
                },
                "pagination": {"offset": offset, "limit": limit},
            }
        )


def _normalize_changeset_row(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    data = row.get("data")
    if isinstance(data, str):
        try:
            row["data"] = json.loads(data)
        except Exception:
            row["data"] = None
        data = row.get("data")
    if isinstance(data, dict) and "stats" in data:
        row["stats"] = data.get("stats")
    status = row.get("status")
    if status is not None and not isinstance(status, OpStatus):
        try:
            row["status"] = OpStatus(status)
        except Exception:
            row["status"] = OpStatus(str(status))
    return row
