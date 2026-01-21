"""
One-time migration helper: copy the current EAV-style katalog database into the
prototype JSON-per-actor schema (tools/json_schema_prototype.py).

Assumptions:
- The source DB uses the existing katalog schema (assets, metadata, actors, etc.).
- Treat all data as belonging to a single changeset/version per (asset, actor); we
  pick the latest changeset_id we find for each pair.
- Only non-removed metadata rows are migrated.

Usage:
    python tools/migrate_to_json_schema.py \
        --source-db /path/to/katalog.db \
        --dest-db /path/to/json_schema.db

The script is intentionally standalone and does not modify the existing DB.
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from json_schema_prototype import append_version, init_db

# Mirrors katalog.metadata.MetadataType
STRING, INT, FLOAT, DATETIME, JSON, RELATION = range(6)


def _convert_value(row: sqlite3.Row) -> Any:
    """Convert a metadata row into a Python scalar based on value_type."""
    vt = row["value_type"]
    if vt == STRING:
        return row["value_text"]
    if vt == INT:
        return row["value_int"]
    if vt == FLOAT:
        return row["value_real"]
    if vt == DATETIME:
        dt = row["value_datetime"]
        if dt is None:
            return None
        # SQLite stores as ISO string; parse and re-emit ISO to keep stable text form.
        try:
            return datetime.fromisoformat(dt).isoformat()
        except ValueError:
            return dt
    if vt == JSON:
        return row["value_json"]
    if vt == RELATION:
        return row["value_relation_id"]
    return None


async def migrate(source_db: Path, dest_db: Path) -> None:
    conn = sqlite3.connect(source_db)
    conn.row_factory = sqlite3.Row

    actors = {
        row["id"]: row["name"] for row in conn.execute("SELECT id, name FROM actor")
    }
    key_registry = {
        row["id"]: row["key"]
        for row in conn.execute("SELECT id, key FROM metadataregistry")
    }

    assets = {
        row["id"]: {
            "external_id": row["external_id"],
            "canonical_uri": row["canonical_uri"],
        }
        for row in conn.execute("SELECT id, external_id, canonical_uri FROM asset")
    }

    # Group metadata per (asset_id, actor_id)
    grouped: Dict[Tuple[int, int], List[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT *
        FROM metadata
        WHERE removed = 0
        """
    ):
        grouped[(row["asset_id"], row["actor_id"])].append(row)

    # Track max changeset per (asset, actor) to preserve ordering
    latest_changeset: Dict[Tuple[int, int], int] = {}
    for (asset_id, actor_id), rows in grouped.items():
        latest_changeset[(asset_id, actor_id)] = max(
            r["changeset_id"] or 0 for r in rows
        )

    await init_db(db_url=f"sqlite:///{dest_db}")

    for (asset_id, actor_id), rows in grouped.items():
        metadata_json: Dict[str, Any] = {}
        for r in rows:
            key = key_registry.get(
                r["metadata_key_id"], f"unknown/{r['metadata_key_id']}"
            )
            val = _convert_value(r)
            if val is None:
                continue
            slot = metadata_json.setdefault(
                key, {"v": [], "source": actors.get(actor_id)}
            )
            if val not in slot["v"]:
                slot["v"].append(val)

        # add core asset fields if missing
        asset_info = assets.get(asset_id)
        if asset_info:
            metadata_json.setdefault(
                "asset/external_id", {"v": [asset_info["external_id"]]}
            )
            metadata_json.setdefault(
                "asset/canonical_uri", {"v": [asset_info["canonical_uri"]]}
            )

        changeset_id = latest_changeset.get((asset_id, actor_id), 0) or 0
        await append_version(
            asset_id=str(asset_id),
            actor_id=str(actor_id),
            changeset_id=changeset_id,
            metadata_json=metadata_json,
            tombstone=False,
        )

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate katalog DB to JSON schema prototype."
    )
    parser.add_argument(
        "--source-db", required=True, help="Path to existing katalog.db"
    )
    parser.add_argument(
        "--dest-db", required=True, help="Path to write the new JSON-schema DB"
    )
    args = parser.parse_args()

    src = Path(args.source_db).expanduser().resolve()
    dest = Path(args.dest_db).expanduser().resolve()
    dest.parent.mkdir(parents=True, exist_ok=True)

    asyncio.run(migrate(src, dest))
    print(f"Migration completed. New DB at: {dest}")


if __name__ == "__main__":
    main()
