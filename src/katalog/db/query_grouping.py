import time
from typing import Any

from tortoise import Tortoise

from katalog.constants.metadata import (
    ASSET_ACTOR_ID,
    ASSET_CANONICAL_URI,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    get_metadata_id,
)
from katalog.constants.metadata import MetadataKey as MK
from katalog.models import Asset, Metadata
from katalog.views import ViewSpec

from katalog.db.query_fields import asset_filter_fields
from katalog.db.query_filters import filter_conditions
from katalog.db.query_search import _fts5_query_from_user_text


def _resolve_group_field(group_by: str) -> tuple[str, str]:
    """
    Map a grouping key to SQL expression and type ('asset' or 'metadata').
    """

    if group_by in asset_filter_fields:
        return asset_filter_fields[group_by][0], "asset"
    return group_by, "metadata"


async def list_grouped_assets(
    view: ViewSpec,
    *,
    group_by: str,
    actor_id: int | None = None,
    offset: int = 0,
    limit: int = 50,
    filters: list[str] | None = None,
    search: str | None = None,
    include_total: bool = True,
) -> dict[str, Any]:
    """
    Return aggregate groups (no members) for the given grouping key.

    Group rows mimic asset rows so SimpleTable can render nested rows with the
    same schema; row_kind='group' distinguishes them.
    """

    field_expr, field_type = _resolve_group_field(group_by)
    started_at = time.perf_counter()

    conditions, filter_params = filter_conditions(filters)
    if actor_id is not None:
        conditions.insert(
            0,
            "EXISTS (SELECT 1 FROM metadata m WHERE m.asset_id = a.id AND m.actor_id = ?)",
        )
        filter_params.insert(0, actor_id)
    if search is not None and search.strip():
        fts_query = _fts5_query_from_user_text(search)
        if not fts_query:
            raise ValueError("Invalid search query")
        conditions.append(
            "a.id IN (SELECT rowid FROM asset_search WHERE asset_search MATCH ?)"
        )
        filter_params.append(fts_query)
    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    conn = Tortoise.get_connection("default")

    if field_type == "asset":
        group_sql = f"""
        SELECT
            {field_expr} AS group_value,
            COUNT(*) AS size,
            GROUP_CONCAT(a.id) AS asset_ids
        FROM {Asset._meta.db_table} a
        {where_sql}
        GROUP BY {field_expr}
        ORDER BY size DESC, group_value
        LIMIT ? OFFSET ?
        """
        params = list(filter_params) + [limit, offset]
        count_sql = (
            f"SELECT COUNT(DISTINCT {field_expr}) AS cnt "
            f"FROM {Asset._meta.db_table} a {where_sql}"
        )
        count_params = list(filter_params)
    else:
        metadata_table = Metadata._meta.db_table
        registry_id = get_metadata_id(MK(group_by))
        group_sql = f"""
        WITH filtered AS (
            SELECT a.id AS asset_id
            FROM {Asset._meta.db_table} a
            {where_sql}
        ),
        latest AS (
            SELECT
                m.asset_id,
                lower(trim(m.value_text)) AS val,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC
                ) AS rn
            FROM {metadata_table} m
            JOIN filtered f ON f.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        ),
        current AS (
            SELECT asset_id, val AS group_value
            FROM latest
            WHERE rn = 1 AND group_value != ''
        )
        SELECT
            group_value,
            COUNT(*) AS size,
            GROUP_CONCAT(asset_id) AS asset_ids
        FROM current
        GROUP BY group_value
        ORDER BY size DESC, group_value
        LIMIT ? OFFSET ?
        """
        params = list(filter_params) + [registry_id, limit, offset]
        count_sql = f"""
        WITH filtered AS (
            SELECT a.id AS asset_id
            FROM {Asset._meta.db_table} a
            {where_sql}
        ),
        latest AS (
            SELECT
                m.asset_id,
                lower(trim(m.value_text)) AS val,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC
                ) AS rn
            FROM {metadata_table} m
            JOIN filtered f ON f.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        ),
        current AS (
            SELECT asset_id, val AS group_value
            FROM latest
            WHERE rn = 1 AND group_value != ''
        )
        SELECT COUNT(DISTINCT group_value) AS cnt FROM current
        """
        count_params = list(filter_params) + [registry_id]

    rows = await conn.execute_query_dict(group_sql, params)
    total_groups = None
    if include_total:
        count_rows = await conn.execute_query_dict(count_sql, count_params)
        total_groups = int(count_rows[0]["cnt"]) if count_rows else 0

    items: list[dict[str, Any]] = []
    for row in rows:
        asset_ids = [int(a) for a in (row.get("asset_ids") or "").split(",") if a]
        items.append(
            {
                "row_kind": "group",
                "group_key": group_by,
                "group_value": row.get("group_value"),
                "group_size": int(row.get("size") or 0),
                "sample_asset_ids": asset_ids[:5],
                # mimic asset columns so the table can render a unified schema
                str(ASSET_ID): f"group:{row.get('group_value')}",
                str(ASSET_ACTOR_ID): None,
                str(ASSET_EXTERNAL_ID): None,
                str(ASSET_CANONICAL_URI): None,
            }
        )

    duration_ms = int((time.perf_counter() - started_at) * 1000)

    return {
        "mode": "groups",
        "group_by": group_by,
        "items": items,
        "stats": {
            "returned": len(items),
            "total_groups": total_groups,
            "duration_ms": duration_ms,
        },
        "pagination": {"offset": offset, "limit": limit},
    }


def build_group_member_filter(group_by: str, group_value: str) -> tuple[str, list[Any]]:
    """Return SQL predicate and params to restrict assets to a group value."""

    field_expr, field_type = _resolve_group_field(group_by)
    if field_type == "asset":
        return f"{field_expr} = ?", [group_value]

    registry_id = get_metadata_id(MK(group_by))
    metadata_table = Metadata._meta.db_table
    predicate = (
        "a.id IN (\n"
        "        WITH latest AS (\n"
        "            SELECT\n"
        "                m.asset_id,\n"
        "                lower(trim(m.value_text)) AS val,\n"
        "                ROW_NUMBER() OVER (PARTITION BY m.asset_id ORDER BY m.changeset_id DESC) AS rn\n"
        f"            FROM {metadata_table} m\n"
        "            WHERE m.metadata_key_id = ?\n"
        "              AND m.removed = 0\n"
        "              AND m.value_text IS NOT NULL\n"
        "        )\n"
        "        SELECT asset_id FROM latest WHERE rn = 1 AND val = ?\n"
        "    )"
    )
    return predicate, [registry_id, group_value]
