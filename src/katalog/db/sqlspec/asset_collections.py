from __future__ import annotations

from typing import Any
from datetime import datetime, timezone

import json

from katalog.db.sqlspec.sql_helpers import execute, scalar, select
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import ASSET_COLLECTION_TABLE, ASSET_TABLE, METADATA_TABLE
from katalog.db.utils import build_where, datetime_to_iso, to_utc_datetime
from katalog.constants.metadata import MetadataType
from katalog.db.sqlspec.assets import _build_assets_where
from katalog.models.assets import AssetCollection, CollectionRefreshMode


class SqlspecAssetCollectionRepo:
    async def get_or_none(self, **filters: Any) -> AssetCollection | None:
        rows = await self.list_rows(limit=1, **filters)
        return rows[0] if rows else None

    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[AssetCollection]:
        where_sql, params = build_where(filters)
        order_sql = f"ORDER BY {order_by}" if order_by else ""
        limit_sql = "LIMIT :limit" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
        sql = (
            f"SELECT id, name, description, source, membership_key_id, item_count, refresh_mode, created_at, updated_at "
            f"FROM {ASSET_COLLECTION_TABLE} {where_sql} {order_sql} {limit_sql}"
        )
        async with session_scope() as session:
            rows = await select(session, sql, params)
        return [
            AssetCollection.model_validate(_normalize_collection_row(row))
            for row in rows
        ]

    async def create(self, **fields: Any) -> AssetCollection:
        refresh_mode = fields.get("refresh_mode")
        if refresh_mode is not None and hasattr(refresh_mode, "value"):
            refresh_mode = refresh_mode.value
        if refresh_mode is None:
            refresh_mode = CollectionRefreshMode.ON_DEMAND.value
        created_at = to_utc_datetime(fields.get("created_at")) or datetime.now(
            timezone.utc
        )
        updated_at = to_utc_datetime(fields.get("updated_at")) or created_at
        async with session_scope() as session:
            await execute(
                session,
                f"""
                INSERT INTO {ASSET_COLLECTION_TABLE} (
                    name, description, source, membership_key_id, item_count,
                    refresh_mode, created_at, updated_at
                ) VALUES (
                    :name, :description, :source, :membership_key_id, :item_count,
                    :refresh_mode, :created_at, :updated_at
                )
                """,
                {
                    "name": fields["name"],
                    "description": fields.get("description"),
                    "source": fields.get("source"),
                    "membership_key_id": fields.get("membership_key_id"),
                    "item_count": fields.get("asset_count", 0),
                    "refresh_mode": refresh_mode,
                    "created_at": datetime_to_iso(created_at),
                    "updated_at": datetime_to_iso(updated_at),
                },
            )
            await session.commit()
            collection_id = await scalar(session, "SELECT last_insert_rowid() AS id")
        collection = await self.get_or_none(id=int(collection_id))
        if collection is None:
            raise RuntimeError("Failed to load created collection")
        return collection

    async def save(self, collection: AssetCollection) -> None:
        if collection.id is None:
            raise ValueError("Collection must have id to save")
        if collection.created_at is None:
            collection.created_at = datetime.now(timezone.utc)
        collection.updated_at = datetime.now(timezone.utc)
        async with session_scope() as session:
            await execute(
                session,
                f"""
                UPDATE {ASSET_COLLECTION_TABLE}
                SET name = :name,
                    description = :description,
                    source = :source,
                    membership_key_id = :membership_key_id,
                    item_count = :item_count,
                    refresh_mode = :refresh_mode,
                    created_at = :created_at,
                    updated_at = :updated_at
                WHERE id = :id
                """,
                {
                    "id": int(collection.id),
                    "name": collection.name,
                    "description": collection.description,
                    "source": collection.source,
                    "membership_key_id": collection.membership_key_id,
                    "item_count": collection.asset_count,
                    "refresh_mode": collection.refresh_mode.value
                    if hasattr(collection.refresh_mode, "value")
                    else str(collection.refresh_mode),
                    "created_at": datetime_to_iso(collection.created_at),
                    "updated_at": datetime_to_iso(collection.updated_at),
                },
            )
            await session.commit()

    async def delete(self, collection_id: int) -> None:
        async with session_scope() as session:
            await execute(
                session,
                f"DELETE FROM {ASSET_COLLECTION_TABLE} WHERE id = :id",
                {"id": int(collection_id)},
            )
            await session.commit()

    async def add_collection_members_for_query(
        self,
        *,
        collection_id: int,
        membership_key_id: int,
        actor_id: int,
        changeset_id: int,
        query_actor_id: int | None,
        filters: list[str] | None,
        search: str | None,
    ) -> int:
        where_sql, filter_params = _build_assets_where(
            actor_id=query_actor_id,
            filters=filters,
            search=search,
            extra_where=None,
        )
        insert_sql = f"""
        INSERT INTO {METADATA_TABLE} (
            asset_id,
            actor_id,
            changeset_id,
            metadata_key_id,
            value_type,
            value_collection_id,
            removed,
            confidence
        )
        SELECT
            a.id,
            ?,
            ?,
            ?,
            ?,
            ?,
            0,
            NULL
        FROM {ASSET_TABLE} a
        {where_sql}
        """
        params = [
            actor_id,
            changeset_id,
            membership_key_id,
            int(MetadataType.COLLECTION),
            collection_id,
            *filter_params,
        ]
        async with session_scope() as session:
            result = await execute(session, insert_sql, params)
            await session.commit()
        try:
            return int(result.rowcount)
        except Exception:
            return 0


def _normalize_collection_row(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    source = row.get("source")
    if isinstance(source, str):
        try:
            row["source"] = json.loads(source)
        except Exception:
            row["source"] = None
    refresh_mode = row.get("refresh_mode")
    if refresh_mode is not None and not isinstance(refresh_mode, CollectionRefreshMode):
        try:
            row["refresh_mode"] = CollectionRefreshMode(refresh_mode)
        except Exception:
            row["refresh_mode"] = CollectionRefreshMode.ON_DEMAND
    row["created_at"] = to_utc_datetime(row.get("created_at"))
    row["updated_at"] = to_utc_datetime(row.get("updated_at"))
    return row
