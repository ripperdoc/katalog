#!/usr/bin/env python3
"""One-time migration for actors schema.

What it does:
- Removes UNIQUE constraint from actors.name by recreating the actors table.
- Adds actors.identity_key column.
- Backfills identity_key for all actors using stable type+plugin+config hash.
- Adds unique partial index on actor identity_key.

Usage:
    uv run python tools/migrate_actor_identity.py /path/to/workspace/katalog.db
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

CREATE_ACTORS_SQL = """
CREATE TABLE actors (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    plugin_id TEXT,
    identity_key TEXT,
    config JSON,
    config_toml TEXT,
    type INTEGER NOT NULL,
    disabled BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME
)
"""

CREATE_CHANGESET_ACTORS_SQL = """
CREATE TABLE changeset_actors (
    id INTEGER PRIMARY KEY,
    changeset_id INTEGER NOT NULL REFERENCES changesets(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    UNIQUE (changeset_id, actor_id)
)
"""

CREATE_ASSETS_SQL = """
CREATE TABLE assets (
    id INTEGER PRIMARY KEY,
    canonical_asset_id INTEGER REFERENCES assets(id) ON DELETE RESTRICT,
    actor_id INTEGER REFERENCES actors(id) ON DELETE RESTRICT,
    namespace TEXT NOT NULL,
    external_id TEXT NOT NULL,
    canonical_uri TEXT NOT NULL,
    UNIQUE (namespace, external_id)
)
"""

CREATE_METADATA_SQL = """
CREATE TABLE metadata (
    id INTEGER PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    changeset_id INTEGER NOT NULL REFERENCES changesets(id) ON DELETE CASCADE,
    metadata_key_id INTEGER NOT NULL REFERENCES metadata_registry(id) ON DELETE RESTRICT,
    value_type INTEGER NOT NULL,
    value_text TEXT,
    value_int INTEGER,
    value_real REAL,
    value_datetime DATETIME,
    value_json JSON,
    value_relation_id INTEGER REFERENCES assets(id) ON DELETE CASCADE,
    value_collection_id INTEGER REFERENCES asset_collections(id) ON DELETE CASCADE,
    removed BOOLEAN NOT NULL DEFAULT 0,
    confidence REAL
)
"""


def _stable_identity_key(
    actor_type: int | None, plugin_id: str | None, config: Any
) -> str | None:
    if actor_type is None:
        return None
    if not plugin_id:
        return None
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            config = {}
    if config is None:
        config = {}
    if not isinstance(config, dict):
        config = {}

    config_json = json.dumps(
        config,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    payload = f"{int(actor_type)}|{plugin_id}|{config_json}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row[1]) for row in rows}


def _recreate_dependent_tables(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE changeset_actors RENAME TO changeset_actors_old")
    conn.execute(CREATE_CHANGESET_ACTORS_SQL)
    conn.execute(
        """
        INSERT INTO changeset_actors (id, changeset_id, actor_id)
        SELECT id, changeset_id, actor_id
        FROM changeset_actors_old
        """
    )
    conn.execute("DROP TABLE changeset_actors_old")

    conn.execute("ALTER TABLE assets RENAME TO assets_old")
    conn.execute(CREATE_ASSETS_SQL)
    conn.execute(
        """
        INSERT INTO assets (id, canonical_asset_id, actor_id, namespace, external_id, canonical_uri)
        SELECT id, canonical_asset_id, actor_id, namespace, external_id, canonical_uri
        FROM assets_old
        """
    )
    conn.execute("DROP TABLE assets_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_asset_canonical_asset_id
        ON assets (canonical_asset_id)
        """
    )

    conn.execute("ALTER TABLE metadata RENAME TO metadata_old")
    conn.execute(CREATE_METADATA_SQL)
    conn.execute(
        """
        INSERT INTO metadata (
            id, asset_id, actor_id, changeset_id, metadata_key_id, value_type,
            value_text, value_int, value_real, value_datetime, value_json,
            value_relation_id, value_collection_id, removed, confidence
        )
        SELECT
            id, asset_id, actor_id, changeset_id, metadata_key_id, value_type,
            value_text, value_int, value_real, value_datetime, value_json,
            value_relation_id, value_collection_id, removed, confidence
        FROM metadata_old
        """
    )
    conn.execute("DROP TABLE metadata_old")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metadata_asset_key_changeset
        ON metadata (asset_id, metadata_key_id, changeset_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_metadata_key_collection
        ON metadata (metadata_key_id, value_collection_id)
        """
    )


def migrate(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")

        old_columns = _column_names(conn, "actors")
        has_identity = "identity_key" in old_columns

        # Recreate actors table without UNIQUE(name), preserving IDs.
        conn.execute("ALTER TABLE actors RENAME TO actors_old")
        conn.execute(CREATE_ACTORS_SQL)

        if has_identity:
            conn.execute(
                """
                INSERT INTO actors (id, name, plugin_id, identity_key, config, config_toml, type, disabled, created_at, updated_at)
                SELECT id, name, plugin_id, identity_key, config, config_toml, type, disabled, created_at, updated_at
                FROM actors_old
                """
            )
        else:
            conn.execute(
                """
                INSERT INTO actors (id, name, plugin_id, config, config_toml, type, disabled, created_at, updated_at)
                SELECT id, name, plugin_id, config, config_toml, type, disabled, created_at, updated_at
                FROM actors_old
                """
            )

        conn.execute("DROP TABLE actors_old")

        # SQLite rewrites child FK targets during table renames, so rebuild dependent
        # tables to ensure they reference the final `actors` table, not `actors_old`.
        _recreate_dependent_tables(conn)

        rows = conn.execute(
            "SELECT id, plugin_id, config, type FROM actors",
        ).fetchall()

        for row in rows:
            identity_key = _stable_identity_key(
                row["type"], row["plugin_id"], row["config"]
            )
            conn.execute(
                "UPDATE actors SET identity_key = ? WHERE id = ?",
                (identity_key, int(row["id"])),
            )

        conn.execute("DROP INDEX IF EXISTS idx_actors_processor_identity_unique")
        conn.execute("DROP INDEX IF EXISTS idx_actors_identity_unique")
        conn.execute(
            """
            CREATE UNIQUE INDEX idx_actors_identity_unique
            ON actors (identity_key)
            WHERE identity_key IS NOT NULL
            """
        )

        conn.execute("COMMIT")
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate actors schema to identity-key model")
    parser.add_argument("db_path", type=Path, help="Path to katalog.db")
    args = parser.parse_args()

    migrate(args.db_path.resolve())
    print(f"Migration completed: {args.db_path}")


if __name__ == "__main__":
    main()
