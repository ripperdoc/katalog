from __future__ import annotations

from typing import Any
import json
from datetime import datetime, timezone

from katalog.db.utils import build_where, datetime_to_iso, to_utc_datetime
from katalog.db.sqlspec.sql_helpers import execute, scalar, select
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import ACTOR_TABLE
from katalog.models.core import Actor, ActorType


class SqlspecActorRepo:
    async def get_or_none(self, **filters: Any) -> Actor | None:
        rows = await self.list_rows(limit=1, **filters)
        return rows[0] if rows else None

    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Actor]:
        where_sql, params = build_where(filters)
        order_sql = f"ORDER BY {order_by}" if order_by else ""
        limit_sql = "LIMIT :limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, name, plugin_id, identity_key, config, config_toml, type, disabled, created_at, updated_at "
            f"FROM {ACTOR_TABLE} {where_sql} {order_sql} {limit_sql}"
        )
        async with session_scope() as session:
            rows = await select(session, sql, params)
        return [Actor.model_validate(_normalize_actor_row(row)) for row in rows]

    async def create(self, **fields: Any) -> Actor:
        created_at = to_utc_datetime(fields.get("created_at")) or datetime.now(
            timezone.utc
        )
        updated_at = to_utc_datetime(fields.get("updated_at")) or created_at
        async with session_scope() as session:
            await execute(
                session,
                f"""
                INSERT INTO {ACTOR_TABLE} (
                    name, plugin_id, identity_key, config, config_toml, type, disabled, created_at, updated_at
                ) VALUES (
                    :name, :plugin_id, :identity_key, :config, :config_toml, :type, :disabled, :created_at, :updated_at
                )
                """,
                {
                    "name": fields["name"],
                    "plugin_id": fields.get("plugin_id"),
                    "identity_key": fields.get("identity_key"),
                    "config": fields.get("config"),
                    "config_toml": fields.get("config_toml"),
                    "type": int(fields["type"])
                    if isinstance(fields["type"], ActorType)
                    else int(fields["type"]),
                    "disabled": 1 if fields.get("disabled") else 0,
                    "created_at": datetime_to_iso(created_at),
                    "updated_at": datetime_to_iso(updated_at),
                },
            )
            await session.commit()
            actor_id = await scalar(session, "SELECT last_insert_rowid() AS id")
        actor = await self.get_or_none(id=int(actor_id))
        if actor is None:
            raise RuntimeError("Failed to load created actor")
        return actor

    async def save(self, actor: Actor) -> None:
        if actor.id is None:
            raise ValueError("Actor must have id to save")
        if actor.created_at is None:
            actor.created_at = datetime.now(timezone.utc)
        actor.updated_at = datetime.now(timezone.utc)
        async with session_scope() as session:
            await execute(
                session,
                f"""
                UPDATE {ACTOR_TABLE}
                SET name = :name,
                    plugin_id = :plugin_id,
                    identity_key = :identity_key,
                    config = :config,
                    config_toml = :config_toml,
                    type = :type,
                    disabled = :disabled,
                    created_at = :created_at,
                    updated_at = :updated_at
                WHERE id = :id
                """,
                {
                    "id": int(actor.id),
                    "name": actor.name,
                    "plugin_id": actor.plugin_id,
                    "identity_key": actor.identity_key,
                    "config": actor.config,
                    "config_toml": actor.config_toml,
                    "type": int(actor.type)
                    if isinstance(actor.type, ActorType)
                    else int(actor.type),
                    "disabled": 1 if actor.disabled else 0,
                    "created_at": datetime_to_iso(actor.created_at),
                    "updated_at": datetime_to_iso(actor.updated_at),
                },
            )
            await session.commit()


def _normalize_actor_row(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    config = row.get("config")
    if isinstance(config, str):
        try:
            row["config"] = json.loads(config)
        except Exception:
            row["config"] = None
    row["created_at"] = to_utc_datetime(row.get("created_at"))
    row["updated_at"] = to_utc_datetime(row.get("updated_at"))
    return row
