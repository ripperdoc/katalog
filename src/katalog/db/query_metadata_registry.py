from pathlib import Path

from loguru import logger
from tortoise import Tortoise

from katalog.config import DB_PATH
from katalog.constants.metadata import (
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataDef,
    MetadataKey,
)
from katalog.models import MetadataRegistry


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
            modules={
                "models": [
                    "katalog.models.core",
                    "katalog.models.assets",
                    "katalog.models.metadata",
                ]
            },
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
    from katalog.editors.user_editor import ensure_user_editor

    await ensure_user_editor()
    logger.info("Synchronized database schema and metadata registry")
