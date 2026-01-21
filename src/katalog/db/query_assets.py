import time
from typing import Any, Mapping

from tortoise import Tortoise

from katalog.constants.metadata import (
    ASSET_ACTOR_ID,
    ASSET_CANONICAL_URI,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    METADATA_REGISTRY_BY_ID,
    MetadataKey,
    get_metadata_id,
)
from katalog.models import Asset, Metadata
from katalog.views import ViewSpec

from katalog.db.query_fields import asset_sort_fields
from katalog.db.query_filters import filter_conditions
from katalog.db.query_search import _fts5_query_from_user_text
from katalog.db.query_sort import sort_conditions
from katalog.db.query_values import _decode_metadata_value


async def list_assets_for_view(
    view: ViewSpec,
    *,
    actor_id: int | None = None,
    offset: int = 0,
    limit: int = 100,
    sort: tuple[str, str] | None = None,
    filters: list[str] | None = None,
    columns: set[str] | None = None,
    search: str | None = None,
    include_total: bool = True,
    extra_where: tuple[str, list[Any]] | None = None,
) -> dict[str, Any]:
    """List assets constrained by a ViewSpec with offset pagination."""

    started_at = time.perf_counter()
    assets_query_ms: int | None = None
    metadata_query_ms: int | None = None
    count_query_ms: int | None = None

    if limit < 0 or offset < 0:
        raise ValueError("offset and limit must be non-negative")

    column_map = view.column_map()
    requested_columns = set(columns) if columns else set(column_map)
    unknown = requested_columns - set(column_map)
    if unknown:
        raise ValueError(f"Unknown columns requested: {sorted(unknown)}")

    order_by_clause = sort_conditions(sort, view)

    asset_table = Asset._meta.db_table
    metadata_table = Metadata._meta.db_table
    # WHERE clause builder
    conditions, filter_params = filter_conditions(filters)

    if actor_id is not None:
        # Scope to assets that have metadata from the actor.
        conditions.append(
            "EXISTS (SELECT 1 FROM metadata m WHERE m.asset_id = a.id AND m.actor_id = ?)"
        )
        filter_params.extend([actor_id])

    if extra_where:
        conditions.append(extra_where[0])
        filter_params.extend(extra_where[1])

    if search is not None and search.strip():
        fts_query = _fts5_query_from_user_text(search)
        if not fts_query:
            raise ValueError("Invalid search query")
        conditions.append(
            "a.id IN (SELECT rowid FROM asset_search WHERE asset_search MATCH ?)"
        )
        filter_params.append(fts_query)

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Determine which metadata keys to include; reduces workload when projecting columns.
    metadata_keys = [
        col_id for col_id in requested_columns if col_id not in asset_sort_fields
    ]
    metadata_ids = [get_metadata_id(MetadataKey(key)) for key in metadata_keys]

    conn = Tortoise.get_connection("default")

    # NOTE this approach means we cannot sort by metadata columns
    assets_sql = f"""
    SELECT
        a.id AS asset_id,
        NULL AS asset_actor_id,
        a.external_id,
        a.canonical_uri
    FROM {asset_table} a
    {where_sql}
    ORDER BY {order_by_clause}
    LIMIT ? OFFSET ?
    """
    assets_params = list(filter_params) + [limit, offset]

    assets_started = time.perf_counter()
    asset_rows = await conn.execute_query_dict(assets_sql, assets_params)
    assets_query_ms = int((time.perf_counter() - assets_started) * 1000)

    assets: dict[int, dict[str, Any]] = {}
    ordered_columns: list[Mapping[str, Any]] = []
    for col in view.columns:
        if col.id in requested_columns:
            ordered_columns.append(col.to_dict())

    page_asset_ids: list[int] = []
    for row in asset_rows:
        asset_id = int(row["asset_id"])
        page_asset_ids.append(asset_id)
        asset_entry: dict[str, Any] = {
            str(ASSET_ID): asset_id,
            str(ASSET_ACTOR_ID): row["asset_actor_id"],
            str(ASSET_EXTERNAL_ID): row["external_id"],
            str(ASSET_CANONICAL_URI): row["canonical_uri"],
        }
        for key in metadata_keys:
            asset_entry[key] = None
        assets[asset_id] = asset_entry

    if page_asset_ids and metadata_ids:
        asset_placeholders = ", ".join("?" for _ in page_asset_ids)
        key_placeholders = ", ".join("?" for _ in metadata_ids)
        metadata_sql = f"""
        WITH latest_snap AS (
            SELECT
                m.asset_id,
                m.metadata_key_id,
                MAX(m.changeset_id) AS changeset_id
            FROM {metadata_table} m
            WHERE
                m.removed = 0
                AND m.asset_id IN ({asset_placeholders})
                AND m.metadata_key_id IN ({key_placeholders})
            GROUP BY m.asset_id, m.metadata_key_id
        ),
        latest_id AS (
            SELECT
                m.asset_id,
                m.metadata_key_id,
                MAX(m.id) AS id
            FROM {metadata_table} m
            JOIN latest_snap ls
                ON ls.asset_id = m.asset_id
                AND ls.metadata_key_id = m.metadata_key_id
                AND ls.changeset_id = m.changeset_id
            WHERE m.removed = 0
            GROUP BY m.asset_id, m.metadata_key_id
        )
        SELECT
            m.asset_id,
            m.metadata_key_id,
            m.value_type,
            m.value_text,
            m.value_int,
            m.value_real,
            m.value_datetime,
            m.value_json,
            m.value_relation_id,
            m.value_collection_id
        FROM {metadata_table} m
        JOIN latest_id li ON li.id = m.id
        """
        metadata_params: list[Any] = list(page_asset_ids) + list(metadata_ids)
        metadata_started = time.perf_counter()
        metadata_rows = await conn.execute_query_dict(metadata_sql, metadata_params)
        metadata_query_ms = int((time.perf_counter() - metadata_started) * 1000)

        for row in metadata_rows:
            asset_id = int(row["asset_id"])
            asset_entry = assets.get(asset_id)
            if asset_entry is None:
                continue

            key_def = METADATA_REGISTRY_BY_ID.get(int(row["metadata_key_id"]))
            if key_def is None:
                continue

            value = _decode_metadata_value(row)

            key_str = str(key_def.key)
            if key_str not in requested_columns:
                continue
            asset_entry[key_str] = value

    total_count = None
    if include_total:
        count_sql = f"SELECT COUNT(*) as cnt FROM {asset_table} a {where_sql}"
        count_started = time.perf_counter()
        count_rows = await conn.execute_query_dict(count_sql, filter_params)
        count_query_ms = int((time.perf_counter() - count_started) * 1000)
        total_count = int(count_rows[0]["cnt"]) if count_rows else 0

    duration_ms = int((time.perf_counter() - started_at) * 1000)

    return {
        "items": list(assets.values()),
        "schema": ordered_columns,
        "stats": {
            "returned": len(assets),
            "total": total_count,
            "duration_ms": duration_ms,
            "duration_assets_ms": assets_query_ms,
            "duration_metadata_ms": metadata_query_ms,
            "duration_count_ms": count_query_ms,
        },
        "pagination": {"offset": offset, "limit": limit},
    }
