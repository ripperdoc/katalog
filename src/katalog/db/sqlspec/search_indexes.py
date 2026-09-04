from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from katalog.db.sqlspec.sql_helpers import execute, select
from katalog.db.sqlspec.tables import METADATA_TABLE

_SEARCH_INDEX_TABLE_PATTERN = re.compile(
    r"(?:fts_index_actor_\d+|vec_index_actor_\d+_\d+)"
)


async def delete_search_index_rows_for_assets(
    session: Any,
    *,
    asset_ids: Sequence[int],
) -> None:
    """Delete physical FTS/vector rows backed by metadata for the assets."""
    if not asset_ids:
        return

    table_rows = await select(
        session,
        "SELECT name FROM sqlite_master WHERE type = 'table'",
    )
    table_names = [
        str(row["name"])
        for row in table_rows
        if _SEARCH_INDEX_TABLE_PATTERN.fullmatch(str(row["name"]))
    ]
    if not table_names:
        return

    params = {
        f"asset_{index}": int(asset_id) for index, asset_id in enumerate(asset_ids)
    }
    placeholders = ", ".join(f":{name}" for name in params)
    for table_name in table_names:
        # Table names come from sqlite_master and must match the numeric-only
        # Katalog index naming scheme before they are interpolated.
        await execute(
            session,
            f"""
            DELETE FROM "{table_name}"
            WHERE rowid IN (
                SELECT id FROM {METADATA_TABLE}
                WHERE asset_id IN ({placeholders})
            )
            """,
            params,
        )
