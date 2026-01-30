import time
from typing import Any

from tortoise import Tortoise

from katalog.constants.metadata import METADATA_REGISTRY_BY_ID
from katalog.models import Metadata

from katalog.db.query_values import _decode_metadata_value


async def list_changeset_metadata_changes(
    changeset_id: int,
    *,
    offset: int = 0,
    limit: int = 200,
    include_total: bool = True,
) -> dict[str, Any]:
    """Return paginated metadata rows belonging to a changeset."""

    started_at = time.perf_counter()
    metadata_table = Metadata._meta.db_table
    try:
        conn = Tortoise.get_connection("analysis")
    except Exception:
        conn = Tortoise.get_connection("default")

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
    FROM {metadata_table}
    WHERE changeset_id = ?
    ORDER BY id
    LIMIT ? OFFSET ?
    """
    params = [changeset_id, limit, offset]

    rows_started = time.perf_counter()
    rows = await conn.execute_query_dict(sql, params)
    duration_rows_ms = int((time.perf_counter() - rows_started) * 1000)

    items: list[dict[str, Any]] = []
    for row in rows:
        registry = METADATA_REGISTRY_BY_ID.get(int(row["metadata_key_id"]))
        key_str = str(registry.key) if registry else f"id:{row['metadata_key_id']}"
        items.append(
            {
                "id": int(row["id"]),
                "asset_id": int(row["asset_id"]),
                "actor_id": int(row["actor_id"]),
                "changeset_id": int(row["changeset_id"]),
                "metadata_key": key_str,
                "metadata_key_id": int(row["metadata_key_id"]),
                "value_type": int(row["value_type"]),
                "value": _decode_metadata_value(row),
                "removed": bool(row["removed"]),
            }
        )

    total_count = None
    if include_total:
        count_sql = (
            f"SELECT COUNT(*) AS cnt FROM {metadata_table} WHERE changeset_id = ?"
        )
        count_started = time.perf_counter()
        count_rows = await conn.execute_query_dict(count_sql, [changeset_id])
        count_duration_ms = int((time.perf_counter() - count_started) * 1000)
        total_count = int(count_rows[0]["cnt"]) if count_rows else 0
    else:
        count_duration_ms = None

    duration_ms = int((time.perf_counter() - started_at) * 1000)

    return {
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
