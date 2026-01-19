import json
import time
from pathlib import Path
from typing import Any, Mapping

from loguru import logger
from tortoise import Tortoise

from katalog.config import DB_PATH
from katalog.metadata import (
    ASSET_EXTERNAL_ID,
    ASSET_CANONICAL_URI,
    ASSET_ID,
    ASSET_PROVIDER_ID,
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataDef,
    MetadataKey,
    get_metadata_id,
)
from katalog.metadata import MetadataKey as MK
from katalog.models import (
    Asset,
    AssetState,
    AssetStateStatus,
    Metadata,
    MetadataRegistry,
    MetadataType,
)
from katalog.views import ViewSpec


def _fts5_query_from_user_text(raw: str) -> str:
    """Convert arbitrary user input into a safe FTS5 MATCH query.

    We intentionally do not expose FTS query syntax to the UI search box.
    Characters like '-' can be parsed as operators and crash the query.

    Important: our FTS table is created with `detail='none'` for minimal index
    size, which means FTS5 phrase queries (double-quoted terms) are not
    supported.

    We therefore create an AND query of *tokens* only.

    Example:
    input:  "foo-bar baz" -> "foo AND bar AND baz"
    """

    text = (raw or "").strip()
    if not text:
        return ""

    # Extract only alphanumeric runs; everything else is treated as a separator.
    # This intentionally splits on '_' as well, because the unicode61 tokenizer
    # may treat it as a separator. If we pass a token containing '_' through to
    # FTS5, it may be internally split into multiple adjacent tokens, which then
    # becomes a phrase query (unsupported with detail='none').
    cleaned = text.replace('"', " ")
    parts: list[str] = []
    buf: list[str] = []
    for ch in cleaned:
        if ch.isalnum():
            buf.append(ch)
        else:
            if buf:
                parts.append("".join(buf))
                buf.clear()
    if buf:
        parts.append("".join(buf))
    if not parts:
        return ""

    # Prevent accidental operator injection / parse errors for reserved keywords.
    # Appending '*' makes it a term token (prefix query), including the exact
    # keyword itself, without needing phrase quotes.
    reserved = {"and", "or", "not", "near"}
    safe_parts: list[str] = []
    for part in parts:
        lowered = part.lower()
        safe_parts.append(f"{part}*" if lowered in reserved else part)

    return " AND ".join(safe_parts)


async def setup_db(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite://{db_path}"

    db_missing = not db_path.exists()
    try:
        conn = Tortoise.get_connection("default")
    except Exception:
        conn = None

    needs_init = db_missing or conn is None

    if needs_init and conn is not None:
        await Tortoise.close_connections()

    if needs_init:
        await Tortoise.init(
            db_url=db_url,
            modules={"models": ["katalog.models"]},
            use_tz=False,  # Preserve whatever tzinfo we hand in; no UTC normalization.
        )

    await Tortoise.generate_schemas()

    # Ensure composite index for fast latest-metadata lookups.
    conn = Tortoise.get_connection("default")
    await conn.execute_script(
        """
        -- SQLite tuning for high-volume ingest.
        -- WAL + NORMAL synchronous is typically a large speed-up for write-heavy workloads.
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -65536; -- KiB; ~64 MiB
        PRAGMA busy_timeout = 5000;
        PRAGMA wal_autocheckpoint = 1000;

        CREATE INDEX IF NOT EXISTS idx_metadata_asset_key_snapshot
        ON metadata(asset_id, metadata_key_id, snapshot_id);

        -- Optional: targeted index for current MD5 lookups in analyzers/processors.
        -- Uncomment after measuring baseline performance.
        -- CREATE INDEX IF NOT EXISTS idx_metadata_md5_current
        -- ON metadata(metadata_key_id, removed, value_text)
        -- WHERE metadata_key_id = (SELECT id FROM metadataregistry WHERE key = 'hash/md5')
        --   AND removed = 0;

        -- Full-text search index for current metadata (one row per asset_id).
        -- Keep it minimal: one column, no positional detail.
        CREATE VIRTUAL TABLE IF NOT EXISTS asset_search
        USING fts5(doc, tokenize='unicode61', detail='none');
        """
    )
    return db_path


async def sync_metadata_registry() -> None:
    """Upsert MetadataRegistry rows from the in-memory registry without deleting existing rows."""

    for definition in list(METADATA_REGISTRY.values()):
        await MetadataRegistry.update_or_create(
            plugin_id=definition.plugin_id,
            key=str(definition.key),
            defaults={
                "value_type": definition.value_type,
                "title": definition.title,
                "description": definition.description,
                "width": definition.width,
            },
        )

    # Reload to capture generated IDs and rebuild mappings.
    METADATA_REGISTRY_BY_ID.clear()
    for row in await MetadataRegistry.all():
        updated = MetadataDef(
            plugin_id=row.plugin_id,
            key=MetadataKey(row.key),
            registry_id=int(row.id),
            value_type=row.value_type,
            title=row.title,
            description=row.description,
            width=row.width,
        )
        METADATA_REGISTRY[updated.key] = updated
        METADATA_REGISTRY_BY_ID[int(row.id)] = updated


async def sync_config():
    """Initialize database and registry. Legacy name kept for compatibility."""
    await setup_db(DB_PATH)
    await sync_metadata_registry()
    logger.info("Synchronized database schema and metadata registry")


asset_filter_fields = {
    str(ASSET_ID): ("a.id", "int"),
    str(ASSET_EXTERNAL_ID): ("a.external_id", "str"),
    str(ASSET_CANONICAL_URI): ("a.canonical_uri", "str"),
}


def _decode_metadata_value(row: Mapping[str, Any]) -> Any:
    """Decode a metadata row into a Python value based on value_type.

    Shared by asset listings and snapshot change listings to keep value handling
    consistent between endpoints.
    """

    value_type_raw = row.get("value_type")
    value_type = (
        value_type_raw
        if isinstance(value_type_raw, MetadataType)
        else MetadataType(int(value_type_raw))
    )

    if value_type == MetadataType.STRING:
        return row.get("value_text")
    if value_type == MetadataType.INT:
        return row.get("value_int")
    if value_type == MetadataType.FLOAT:
        return row.get("value_real")
    if value_type == MetadataType.DATETIME:
        dt = row.get("value_datetime")
        return dt.isoformat() if hasattr(dt, "isoformat") else dt
    if value_type == MetadataType.JSON:
        return row.get("value_json")
    if value_type == MetadataType.RELATION:
        return row.get("value_relation_id")
    return None


def _decode_metadata_value(row: Mapping[str, Any]) -> Any:
    """Decode a metadata row's value based on its value_type.

    Shared between asset listings and snapshot change listings to keep value
    handling consistent.
    """

    value_type_raw = row.get("value_type")
    value_type = (
        value_type_raw
        if isinstance(value_type_raw, MetadataType)
        else MetadataType(int(value_type_raw))
    )

    if value_type == MetadataType.STRING:
        return row.get("value_text")
    if value_type == MetadataType.INT:
        return row.get("value_int")
    if value_type == MetadataType.FLOAT:
        return row.get("value_real")
    if value_type == MetadataType.DATETIME:
        dt = row.get("value_datetime")
        return dt.isoformat() if hasattr(dt, "isoformat") else dt
    if value_type == MetadataType.JSON:
        return row.get("value_json")
    if value_type == MetadataType.RELATION:
        return row.get("value_relation_id")
    return None


def _metadata_filter_condition(filt: Mapping[str, Any]) -> tuple[str, list[Any]]:
    """Build SQL predicate + params for a metadata-based filter."""

    accessor = filt.get("accessor")
    operator = filt.get("operator")
    value = filt.get("value")
    values = filt.get("values")

    key = MetadataKey(accessor) if accessor is not None else None
    definition = METADATA_REGISTRY.get(key)
    if definition is None:
        raise ValueError(f"Filtering not supported for column: {accessor}")

    registry_id = get_metadata_id(definition.key)
    metadata_table = Metadata._meta.db_table

    col_map: dict[MetadataType, tuple[str, str]] = {
        MetadataType.STRING: ("m.value_text", "str"),
        MetadataType.INT: ("m.value_int", "int"),
        MetadataType.FLOAT: ("m.value_real", "float"),
        MetadataType.DATETIME: ("m.value_datetime", "datetime"),
        MetadataType.JSON: ("m.value_json", "str"),
        MetadataType.RELATION: ("m.value_relation_id", "int"),
    }
    try:
        column_name, col_type = col_map[definition.value_type]
    except KeyError:  # pragma: no cover
        raise ValueError(f"Unsupported metadata type for filtering: {definition.value_type}")

    def cast_value(val: Any) -> Any:
        if val is None:
            return None
        if col_type == "int":
            return int(val)
        if col_type == "float":
            return float(val)
        return val

    string_ops = {"contains", "notContains", "startsWith", "endsWith"}

    if operator in {
        "equals",
        "notEquals",
        "greaterThan",
        "lessThan",
        "greaterThanOrEqual",
        "lessThanOrEqual",
    }:
        if value is None:
            raise ValueError("Filter value is required")
        op_map = {
            "equals": "=",
            "notEquals": "!=",
            "greaterThan": ">",
            "lessThan": "<",
            "greaterThanOrEqual": ">=",
            "lessThanOrEqual": "<=",
        }
        predicate = f"{column_name} {op_map[operator]} ?"
        value_params = [cast_value(value)]
    elif col_type == "str" and operator in string_ops:
        if value is None:
            raise ValueError("Filter value is required")
        pattern = str(value)
        if operator == "contains":
            predicate = f"{column_name} LIKE ?"
            value_params = [f"%{pattern}%"]
        elif operator == "notContains":
            predicate = f"{column_name} NOT LIKE ?"
            value_params = [f"%{pattern}%"]
        elif operator == "startsWith":
            predicate = f"{column_name} LIKE ?"
            value_params = [f"{pattern}%"]
        else:  # endsWith
            predicate = f"{column_name} LIKE ?"
            value_params = [f"%{pattern}"]
    elif operator in {"between", "notBetween"}:
        if not values or len(values) != 2:
            raise ValueError("Filter values must contain two entries for between")
        op = "BETWEEN" if operator == "between" else "NOT BETWEEN"
        predicate = f"{column_name} {op} ? AND ?"
        value_params = [cast_value(values[0]), cast_value(values[1])]
    elif operator == "isEmpty":
        non_null = (
            f"{column_name} IS NOT NULL AND {column_name} != ''"
            if col_type == "str"
            else f"{column_name} IS NOT NULL"
        )
        condition = (
            "NOT EXISTS ("
            f"SELECT 1 FROM {metadata_table} m "
            "WHERE m.asset_id = a.id "
            "AND m.metadata_key_id = ? "
            "AND m.removed = 0 "
            "AND m.snapshot_id = ("
            f"    SELECT MAX(m2.snapshot_id) FROM {metadata_table} m2 "
            "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
            ") "
            f"AND {non_null}"
            ")"
        )
        return condition, [registry_id, registry_id]
    elif operator == "isNotEmpty":
        non_null = (
            f"{column_name} IS NOT NULL AND {column_name} != ''"
            if col_type == "str"
            else f"{column_name} IS NOT NULL"
        )
        condition = (
            "EXISTS ("
            f"SELECT 1 FROM {metadata_table} m "
            "WHERE m.asset_id = a.id "
            "AND m.metadata_key_id = ? "
            "AND m.removed = 0 "
            "AND m.snapshot_id = ("
            f"    SELECT MAX(m2.snapshot_id) FROM {metadata_table} m2 "
            "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
            ") "
            f"AND {non_null}"
            ")"
        )
        return condition, [registry_id, registry_id]
    else:
        raise ValueError(f"Unsupported filter operator: {operator}")

    condition = (
        "EXISTS ("
        f"SELECT 1 FROM {metadata_table} m "
        "WHERE m.asset_id = a.id "
        "AND m.metadata_key_id = ? "
        "AND m.removed = 0 "
        "AND m.snapshot_id = ("
        f"    SELECT MAX(m2.snapshot_id) FROM {metadata_table} m2 "
        "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
        ") "
        f"AND {predicate}"
        ")"
    )
    params = [registry_id, registry_id, *value_params]
    return condition, params


def filter_conditions(filters):
    filters = filters or []
    conditions = []
    filter_params = []
    for raw in filters:
        try:
            filt = json.loads(raw)
        except Exception:
            raise ValueError("Invalid filter format")
        accessor = filt.get("accessor")
        operator = filt.get("operator")
        value = filt.get("value")
        values = filt.get("values")

        if accessor in asset_filter_fields:
            column_name, col_type = asset_filter_fields[accessor]

            def cast_value(val: Any) -> Any:
                if col_type == "int":
                    return int(val) if val is not None else None
                return val

            if operator in {
                "equals",
                "notEquals",
                "greaterThan",
                "lessThan",
                "greaterThanOrEqual",
                "lessThanOrEqual",
            }:
                if value is None:
                    raise ValueError("Filter value is required")
                op_map = {
                    "equals": "=",
                    "notEquals": "!=",
                    "greaterThan": ">",
                    "lessThan": "<",
                    "greaterThanOrEqual": ">=",
                    "lessThanOrEqual": "<=",
                }
                conditions.append(f"{column_name} {op_map[operator]} ?")
                filter_params.append(cast_value(value))
            elif col_type == "str" and operator in {
                "contains",
                "notContains",
                "startsWith",
                "endsWith",
            }:
                if value is None:
                    raise ValueError("Filter value is required")
                pattern = str(value)
                if operator == "contains":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"%{pattern}%")
                elif operator == "notContains":
                    conditions.append(f"{column_name} NOT LIKE ?")
                    filter_params.append(f"%{pattern}%")
                elif operator == "startsWith":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"{pattern}%")
                elif operator == "endsWith":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"%{pattern}")
            elif operator in {"between", "notBetween"}:
                if not values or len(values) != 2:
                    raise ValueError("Filter values must contain two entries for between")
                op = "BETWEEN" if operator == "between" else "NOT BETWEEN"
                conditions.append(f"{column_name} {op} ? AND ?")
                filter_params.append(cast_value(values[0]))
                filter_params.append(cast_value(values[1]))
            elif operator == "isEmpty":
                conditions.append(f"{column_name} IS NULL")
            elif operator == "isNotEmpty":
                conditions.append(f"{column_name} IS NOT NULL")
            else:
                raise ValueError(f"Unsupported filter operator: {operator}")
        else:
            condition, params = _metadata_filter_condition(filt)
            conditions.append(condition)
            filter_params.extend(params)
    return conditions, filter_params


asset_sort_fields = {
    str(ASSET_ID): "a.id",
    # Provider sort temporarily disabled to avoid expensive lookups; see list_assets_for_view.
    # str(ASSET_PROVIDER_ID): "asset_provider_id",
    str(ASSET_EXTERNAL_ID): "a.external_id",
    str(ASSET_CANONICAL_URI): "a.canonical_uri",
}


def sort_conditions(sort: tuple[str, str] | None, view: ViewSpec):
    sort_col, sort_dir = (
        sort
        if sort is not None
        else (view.default_sort[0] if view.default_sort else (str(ASSET_ID), "asc"))
    )
    sort_dir = sort_dir.lower()
    if sort_dir not in {"asc", "desc"}:
        raise ValueError("sort direction must be 'asc' or 'desc'")
    sort_spec = view.column_map().get(sort_col)
    if sort_spec is None:
        raise ValueError(f"Unknown sort column: {sort_col}")
    if not sort_spec.sortable:
        raise ValueError(f"Sorting not supported for column: {sort_col}")

    if sort_col == str(ASSET_PROVIDER_ID):
        raise ValueError("Sorting by provider is temporarily disabled")
    if sort_col not in asset_sort_fields:
        raise ValueError(f"Sorting not implemented for column: {sort_col}")
    return f"{asset_sort_fields[sort_col]} {sort_dir.upper()}, a.id ASC"


# region grouping


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
    provider_id: int | None = None,
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
    if provider_id is not None:
        conditions.insert(
            0,
            "EXISTS (SELECT 1 FROM assetstate aps WHERE aps.asset_id = a.id AND aps.provider_id = ? AND aps.state = ?)",
        )
        filter_params.insert(0, AssetStateStatus.ACTIVE.value)
        filter_params.insert(0, provider_id)
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
                    ORDER BY m.snapshot_id DESC
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
                    ORDER BY m.snapshot_id DESC
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
                str(ASSET_PROVIDER_ID): None,
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
        "                ROW_NUMBER() OVER (PARTITION BY m.asset_id ORDER BY m.snapshot_id DESC) AS rn\n"
        f"            FROM {metadata_table} m\n"
        "            WHERE m.metadata_key_id = ?\n"
        "              AND m.removed = 0\n"
        "              AND m.value_text IS NOT NULL\n"
        "        )\n"
        "        SELECT asset_id FROM latest WHERE rn = 1 AND val = ?\n"
        "    )"
    )
    return predicate, [registry_id, group_value]


# endregion


async def list_assets_for_view(
    view: ViewSpec,
    *,
    provider_id: int | None = None,
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
    # NOTE: We deliberately avoid joining AssetState here to keep the query fast.

    # WHERE clause builder
    conditions, filter_params = filter_conditions(filters)

    if provider_id is not None:
        # Scope to assets active for the provider; EXISTS keeps the query lightweight.
        conditions.append(
            "EXISTS (SELECT 1 FROM assetstate aps WHERE aps.asset_id = a.id AND aps.provider_id = ? AND aps.state = ?)"
        )
        filter_params.extend([provider_id, AssetStateStatus.ACTIVE.value])

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
        NULL AS asset_provider_id,
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
            str(ASSET_PROVIDER_ID): row["asset_provider_id"],
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
                MAX(m.snapshot_id) AS snapshot_id
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
                AND ls.snapshot_id = m.snapshot_id
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
            m.value_relation_id
        FROM {metadata_table} m
        JOIN latest_id li ON li.id = m.id
        """
        metadata_params: list[Any] = list(page_asset_ids) + list(metadata_ids)
        metadata_started = time.perf_counter()
        metadata_rows = await conn.execute_query_dict(metadata_sql, metadata_params)
        metadata_query_ms = int((time.perf_counter() - metadata_started) * 1000)

        for row in metadata_rows:
            asset_id = int(row["asset_id"])
            asset_entry = assets.get(asset_id)  # type: ignore
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


async def list_snapshot_metadata_changes(
    snapshot_id: int,
    *,
    offset: int = 0,
    limit: int = 200,
    include_total: bool = True,
) -> dict[str, Any]:
    """Return paginated metadata rows belonging to a snapshot."""

    started_at = time.perf_counter()
    metadata_table = Metadata._meta.db_table
    conn = Tortoise.get_connection("default")

    if limit < 0 or offset < 0:
        raise ValueError("offset and limit must be non-negative")

    sql = f"""
    SELECT
        id,
        asset_id,
        provider_id,
        snapshot_id,
        metadata_key_id,
        value_type,
        value_text,
        value_int,
        value_real,
        value_datetime,
        value_json,
        value_relation_id,
        removed
    FROM {metadata_table}
    WHERE snapshot_id = ?
    ORDER BY id
    LIMIT ? OFFSET ?
    """
    params = [snapshot_id, limit, offset]

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
                "provider_id": int(row["provider_id"]),
                "snapshot_id": int(row["snapshot_id"]),
                "metadata_key": key_str,
                "metadata_key_id": int(row["metadata_key_id"]),
                "value_type": int(row["value_type"]),
                "value": _decode_metadata_value(row),
                "removed": bool(row["removed"]),
            }
        )

    total_count = None
    if include_total:
        count_sql = f"SELECT COUNT(*) AS cnt FROM {metadata_table} WHERE snapshot_id = ?"
        count_started = time.perf_counter()
        count_rows = await conn.execute_query_dict(count_sql, [snapshot_id])
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
