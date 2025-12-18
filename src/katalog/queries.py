from pathlib import Path
from typing import Any, Mapping

from tortoise import Tortoise

from katalog.metadata import (
    ACCESS_OWNER,
    ACCESS_SHARED_WITH,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    HASH_MD5,
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    TIME_CREATED,
    TIME_MODIFIED,
    MetadataDef,
    MetadataKey,
)
from katalog.models import Asset, Metadata, MetadataRegistry, MetadataType


async def setup(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite://{db_path}"
    await Tortoise.init(db_url=db_url, modules={"models": ["katalog.models"]})
    await Tortoise.generate_schemas()
    await sync_metadata_registry()

    # Ensure composite index for fast latest-metadata lookups.
    conn = Tortoise.get_connection("default")
    await conn.execute_script(
        """
        CREATE INDEX IF NOT EXISTS idx_metadata_asset_key_snapshot
        ON metadata(asset_id, metadata_key_id, snapshot_id);
        """
    )
    return db_path


async def sync_metadata_registry() -> None:
    """Replace MetadataRegistry contents with in-memory registry (dev convenience)."""

    await MetadataRegistry.all().delete()

    rows = []
    for definition in list(METADATA_REGISTRY.values()):
        rows.append(
            MetadataRegistry(
                plugin_id=definition.plugin_id,
                key=str(definition.key),
                value_type=definition.value_type,
                title=definition.title,
                description=definition.description,
                width=definition.width,
            )
        )

    if rows:
        await MetadataRegistry.bulk_create(rows)

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


example_asset_response = {
    "assets": [
        {
            "id": 1,  #
            "canonical_id": "",
            "canonical_uri": "file:///path/to/asset1.jpg",
            "created": 1678901234,
            "seen": 1678901234,
            "deleted": 1678901234,
            "metadata": {
                FILE_PATH: {
                    "value": "/path/to/asset1.jpg",
                    "count": 3,
                },
                FILE_NAME: {
                    "value": "asset1.jpg",
                    "count": 2,
                },
                FILE_SIZE: {
                    "value": 102400,
                    "count": 43,
                },
                TIME_CREATED: {
                    "value": "2001-02-03T04:05:06Z",
                    "count": 3,
                },
                TIME_MODIFIED: {
                    "value": "2001-02-03T04:05:06Z",
                    "count": 2,
                },
                ACCESS_OWNER: {
                    "value": "some@email.com",
                    "coubt": 4,
                },
                ACCESS_SHARED_WITH: {
                    "value": "some@email.com",
                    "count": 2,
                },
                HASH_MD5: {
                    "value": "d41d8cd98f00b204e9800998ecf8427e",
                    "count": 1,
                },
            },
        },
    ],
    "schema": {},  # Each key is the string metadata key and the value is the MetadataDef as JSON. Only contains metadata present above.
    "stats": {},  # Number of assets returned,
}


async def list_assets_with_metadata(
    *, provider_id: int | None = None
) -> dict[str, Any]:
    """List assets with their metadata for UI consumption.

    Returns one JSON object per asset (Asset fields at root), plus a `metadata`
    dict containing metadata key-value pairs, currently using a default setup but in future depending on which view is requested.

    Pagination, Sorting and filtering will currently be done client side. The only filtering done here is on provider_id if provided.
    """
    asset_table = Asset._meta.db_table
    metadata_table = Metadata._meta.db_table

    where_provider = ""
    params: list[Any] = []
    if provider_id is not None:
        where_provider = "WHERE a.provider_id = ?"
        params.append(provider_id)

    # Window over metadata to grab the most recent value per (asset, key) and its count.
    sql = f"""
    WITH ranked AS (
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
            m.snapshot_id,
            COUNT(*) OVER (PARTITION BY m.asset_id, m.metadata_key_id) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY m.asset_id, m.metadata_key_id ORDER BY m.snapshot_id DESC) AS rn
        FROM {metadata_table} m
        WHERE m.removed = 0
    )
    SELECT
        a.id AS asset_id,
        a.provider_id AS asset_provider_id,
        a.canonical_id,
        a.canonical_uri,
        a.created_snapshot_id,
        a.last_snapshot_id,
        a.deleted_snapshot_id,
        r.metadata_key_id,
        r.value_type,
        r.value_text,
        r.value_int,
        r.value_real,
        r.value_datetime,
        r.value_json,
        r.value_relation_id,
        r.cnt AS metadata_count
    FROM {asset_table} a
    LEFT JOIN ranked r ON r.asset_id = a.id AND r.rn = 1
    {where_provider}
    ORDER BY a.id
    """

    conn = Tortoise.get_connection("default")
    rows = await conn.execute_query_dict(sql, params)

    assets: dict[int, dict[str, Any]] = {}
    schema: dict[str, Mapping[str, Any]] = {}

    for row in rows:
        asset_id = int(row["asset_id"])
        asset_entry = assets.get(asset_id)
        if asset_entry is None:
            asset_entry = {
                "id": asset_id,
                "canonical_id": row["canonical_id"],
                "canonical_uri": row["canonical_uri"],
                "created": row["created_snapshot_id"],
                "seen": row["last_snapshot_id"],
                "deleted": row["deleted_snapshot_id"],
                "metadata": {},
            }
            assets[asset_id] = asset_entry

        key_id = row["metadata_key_id"]
        if key_id is None:
            continue

        key_def = METADATA_REGISTRY_BY_ID.get(int(key_id))
        if key_def is None:
            continue

        value_type = MetadataType(row["value_type"])
        value: Any
        if value_type == MetadataType.STRING:
            value = row["value_text"]
        elif value_type == MetadataType.INT:
            value = row["value_int"]
        elif value_type == MetadataType.FLOAT:
            value = row["value_real"]
        elif value_type == MetadataType.DATETIME:
            dt = row["value_datetime"]
            value = dt.isoformat() if dt is not None else None
        elif value_type == MetadataType.JSON:
            value = row["value_json"]
        elif value_type == MetadataType.RELATION:
            value = row["value_relation_id"]
        else:
            continue

        key_str = str(key_def.key)
        asset_entry["metadata"][key_str] = {
            "value": value,
            "count": int(row["metadata_count"]),
        }
        schema[key_str] = {
            "plugin_id": key_def.plugin_id,
            "key": str(key_def.key),
            "registry_id": key_def.registry_id,
            "value_type": key_def.value_type,
            "title": key_def.title,
            "description": key_def.description,
            "width": key_def.width,
        }

    return {
        "assets": list(assets.values()),
        "schema": schema,
        "stats": {"assets": len(assets)},
    }
