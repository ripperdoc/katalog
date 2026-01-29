from __future__ import annotations

from typing import Any

from katalog.analyzers.base import AnalyzerScope


def build_scoped_assets_cte(
    scope: AnalyzerScope,
    *,
    asset_table: str,
    metadata_table: str,
    alias: str = "scoped_assets",
) -> tuple[str, list[Any]]:
    """Build a CTE for scoped asset ids and its params."""

    if scope.kind == "all":
        cte_sql = f"{alias} AS (SELECT a.id AS asset_id FROM {asset_table} a)"
        return cte_sql, []

    if scope.kind == "asset":
        if scope.asset_id is None:
            raise ValueError("asset_id is required for asset scope")
        cte_sql = (
            f"{alias} AS (SELECT a.id AS asset_id FROM {asset_table} a WHERE a.id = ?)"
        )
        return cte_sql, [int(scope.asset_id)]

    if scope.kind == "collection":
        if scope.collection_id is None or scope.collection_key_id is None:
            raise ValueError("collection_id and collection_key_id are required")
        cte_sql = (
            f"{alias} AS ("
            f"    SELECT a.id AS asset_id FROM {asset_table} a WHERE a.id IN ("
            "        WITH latest AS ("
            "            SELECT"
            "                m.asset_id,"
            "                m.removed,"
            "                ROW_NUMBER() OVER ("
            "                    PARTITION BY m.asset_id, m.value_collection_id, m.actor_id"
            "                    ORDER BY m.changeset_id DESC, m.id DESC"
            "                ) AS rn"
            f"            FROM {metadata_table} m"
            "            WHERE m.metadata_key_id = ?"
            "              AND m.value_collection_id = ?"
            "        )"
            "        SELECT asset_id FROM latest WHERE rn = 1 AND removed = 0"
            "    )"
            ")"
        )
        return cte_sql, [int(scope.collection_key_id), int(scope.collection_id)]

    raise ValueError(f"Unknown analyzer scope kind: {scope.kind}")
