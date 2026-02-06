from pathlib import Path

from loguru import logger

from katalog.config import DB_PATH
from katalog.constants.metadata import (
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataDef,
    MetadataKey,
    MetadataType,
)
from katalog.db.sqlspec.sql_helpers import execute, select
from katalog.db.sqlspec import init_db, session_scope

METADATA_REGISTRY_TABLE = "metadata_registry"


async def setup_db(db_path: Path | None) -> Path | None:
    if db_path is not None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        if not db_path.exists():
            db_path.touch()

    await init_db()
    return db_path


async def sync_metadata_registry() -> None:
    """Upsert MetadataRegistry rows from the in-memory registry without deleting existing rows."""

    async with session_scope() as session:
        for definition in list(METADATA_REGISTRY.values()):
            await execute(
                session,
                f"""
                INSERT INTO {METADATA_REGISTRY_TABLE} (
                    plugin_id,
                    key,
                    value_type,
                    title,
                    description,
                    width
                )
                VALUES (:plugin_id, :key, :value_type, :title, :description, :width)
                ON CONFLICT(plugin_id, key) DO UPDATE SET
                    value_type = excluded.value_type,
                    title = excluded.title,
                    description = excluded.description,
                    width = excluded.width
                """,
                {
                    "plugin_id": definition.plugin_id,
                    "key": str(definition.key),
                    "value_type": int(definition.value_type),
                    "title": definition.title,
                    "description": definition.description,
                    "width": definition.width,
                },
            )

        await session.commit()

        rows = await select(
            session,
            f"""
            SELECT id, plugin_id, key, value_type, title, description, width
            FROM {METADATA_REGISTRY_TABLE}
            ORDER BY id
            """,
        )

    # Reload to capture generated IDs and rebuild mappings.
    METADATA_REGISTRY_BY_ID.clear()
    for row in rows:
        key = MetadataKey(row["key"])
        existing = METADATA_REGISTRY.get(key)
        updated = MetadataDef(
            plugin_id=row["plugin_id"],
            key=key,
            registry_id=int(row["id"]),
            value_type=MetadataType(int(row["value_type"])),
            title=row.get("title") or "",
            description=row.get("description") or "",
            width=row.get("width"),
            skip_false=existing.skip_false if existing else False,
            clear_on_false=existing.clear_on_false if existing else False,
        )
        METADATA_REGISTRY[updated.key] = updated
        if updated.registry_id is not None:
            METADATA_REGISTRY_BY_ID[int(updated.registry_id)] = updated


async def sync_config_db():
    """Initialize database and registry. Legacy name kept for compatibility."""
    await setup_db(DB_PATH)
    await sync_metadata_registry()
    from katalog.editors.user_editor import ensure_user_editor

    await ensure_user_editor()
    logger.info("Synchronized database schema and metadata registry")
