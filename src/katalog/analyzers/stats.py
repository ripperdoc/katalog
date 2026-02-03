from __future__ import annotations

from typing import Any

from loguru import logger
from katalog.analyzers.base import Analyzer, AnalyzerResult, AnalyzerScope
from katalog.analyzers.utils import build_scoped_assets_cte
from katalog.constants.metadata import (
    FILE_EXTENSION,
    FILE_SIZE,
    FILE_TYPE,
    HASH_MD5,
    TIME_MODIFIED,
    get_metadata_id,
)
from katalog.db.sqlspec.sql_helpers import select
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import ASSET_TABLE, METADATA_TABLE
from katalog.models import Changeset
from katalog.utils.exports import build_tables_from_stats, write_csv_tables
from katalog.config import WORKSPACE


class StatsAnalyzer(Analyzer):
    """Aggregate basic stats for assets and metadata."""

    plugin_id = "katalog.analyzers.stats.StatsAnalyzer"
    title = "Asset statistics"
    description = "Aggregate counts, sizes, types, and metadata coverage."
    outputs = frozenset()
    output_kind = "stats"
    supports_single_asset = False

    def should_run(self, *, changeset: Changeset) -> bool:  # noqa: D401
        """Currently always runs; future versions may add change detection."""

        return True

    async def run(
        self, *, changeset: Changeset, scope: AnalyzerScope
    ) -> AnalyzerResult:
        if scope.kind == "asset":
            raise ValueError("Stats analyzer does not support single-asset scope")

        logger.info("Stats analyzer starting ({kind})", kind=scope.kind)
        metadata_table = METADATA_TABLE
        asset_table = ASSET_TABLE
        scoped_cte, scoped_params = build_scoped_assets_cte(
            scope,
            asset_table=asset_table,
            metadata_table=metadata_table,
        )

        async with session_scope(analysis=True) as session:
            asset_count = await self._count_assets(session, scoped_cte, scoped_params)
            logger.info("Stats analyzer assets counted: {count}", count=asset_count)
            size_stats = await self._size_stats(
                session, scoped_cte, scoped_params, metadata_table
            )
            type_breakdown = await self._string_breakdown(
                session,
                scoped_cte,
                scoped_params,
                metadata_table,
                get_metadata_id(FILE_TYPE),
                limit=50,
            )
            extension_breakdown = await self._string_breakdown(
                session,
                scoped_cte,
                scoped_params,
                metadata_table,
                get_metadata_id(FILE_EXTENSION),
                limit=50,
            )
            modified_stats = await self._modified_stats(
                session, scoped_cte, scoped_params, metadata_table
            )
            coverage = await self._coverage_stats(
                session, scoped_cte, scoped_params, metadata_table, asset_count
            )
            duplicates = await self._duplicate_stats(
                session, scoped_cte, scoped_params, metadata_table
            )
            largest_assets = await self._largest_assets(
                session, scoped_cte, scoped_params, metadata_table
            )

        logger.info("Stats analyzer aggregation finished")

        output = {
            "summary": {
                "asset_count": asset_count,
                "total_bytes": size_stats.get("total"),
                "size": size_stats,
                "modified": modified_stats,
            },
            "breakdowns": {
                "file_type": type_breakdown,
                "file_extension": extension_breakdown,
            },
            "coverage": coverage,
            "duplicates": duplicates,
            "largest_assets": largest_assets,
        }

        tables = build_tables_from_stats(output)
        prefix = f"changeset-{changeset.id}_actor-{self.actor.id}_stats"
        csv_paths = write_csv_tables(tables, prefix=prefix)
        if csv_paths:
            output["exports"] = {
                "csv": [
                    str(path.relative_to(WORKSPACE))
                    if path.is_relative_to(WORKSPACE)
                    else str(path)
                    for path in csv_paths
                ]
            }

        return AnalyzerResult(output=output)

    @staticmethod
    async def _count_assets(
        session,
        scoped_cte: str,
        scoped_params: list[Any],
    ) -> int:
        sql = f"""
        WITH {scoped_cte}
        SELECT COUNT(*) AS cnt FROM scoped_assets
        """
        rows = await select(session, sql, scoped_params)
        return int(rows[0]["cnt"]) if rows else 0

    async def _size_stats(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
    ) -> dict[str, Any]:
        size_key_id = get_metadata_id(FILE_SIZE)
        stats_sql = f"""
        WITH {scoped_cte},
        latest_size AS (
            SELECT
                m.asset_id,
                m.value_int AS size,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_int IS NOT NULL
        )
        SELECT
            COUNT(*) AS cnt,
            SUM(size) AS total,
            MIN(size) AS min,
            MAX(size) AS max,
            AVG(size) AS avg
        FROM latest_size
        WHERE rn = 1
        """
        rows = await select(session, stats_sql, [*scoped_params, size_key_id])
        row = rows[0] if rows else {}
        return {
            "count": int(row.get("cnt") or 0),
            "total": int(row.get("total") or 0),
            "min": int(row.get("min") or 0),
            "max": int(row.get("max") or 0),
            "avg": float(row.get("avg") or 0),
        }

    async def _string_breakdown(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        key_id: int,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        sql = f"""
        WITH {scoped_cte},
        latest_values AS (
            SELECT
                m.asset_id,
                m.value_text AS val,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        )
        SELECT
            val AS value,
            COUNT(*) AS cnt
        FROM latest_values
        WHERE rn = 1 AND value != ''
        GROUP BY value
        ORDER BY cnt DESC
        LIMIT ?
        """
        rows = await select(session, sql, [*scoped_params, key_id, limit])
        return [{"value": row["value"], "count": int(row["cnt"])} for row in rows]

    async def _modified_stats(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
    ) -> dict[str, Any]:
        modified_key_id = get_metadata_id(TIME_MODIFIED)
        sql = f"""
        WITH {scoped_cte},
        latest_mod AS (
            SELECT
                m.asset_id,
                m.value_datetime AS modified_at,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_datetime IS NOT NULL
        )
        SELECT
            MIN(modified_at) AS min,
            MAX(modified_at) AS max
        FROM latest_mod
        WHERE rn = 1
        """
        rows = await select(session, sql, [*scoped_params, modified_key_id])
        row = rows[0] if rows else {}
        return {
            "min": row.get("min"),
            "max": row.get("max"),
        }

    async def _coverage_stats(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        asset_count: int,
    ) -> dict[str, Any]:
        sql = f"""
        WITH {scoped_cte},
        latest_keys AS (
            SELECT
                m.metadata_key_id,
                m.asset_id,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id, m.metadata_key_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.removed = 0
        )
        SELECT
            metadata_key_id AS key_id,
            COUNT(*) AS cnt
        FROM latest_keys
        WHERE rn = 1
        GROUP BY key_id
        """
        rows = await select(session, sql, scoped_params)
        coverage = []
        for row in rows:
            coverage.append(
                {
                    "key_id": int(row["key_id"]),
                    "count": int(row["cnt"]),
                    "coverage": float(row["cnt"] or 0) / max(1, asset_count),
                }
            )
        return {"keys": coverage}

    async def _duplicate_stats(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
    ) -> dict[str, Any]:
        md5_key_id = get_metadata_id(HASH_MD5)
        sql = f"""
        WITH {scoped_cte},
        latest_hash AS (
            SELECT
                m.asset_id,
                m.value_text AS md5,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        ),
        current AS (
            SELECT md5 FROM latest_hash WHERE rn = 1
        )
        SELECT
            md5,
            COUNT(*) AS cnt
        FROM current
        GROUP BY md5
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 50
        """
        rows = await select(session, sql, [*scoped_params, md5_key_id])
        return {
            "groups": [{"md5": row["md5"], "count": int(row["cnt"])} for row in rows]
        }

    async def _largest_assets(
        self,
        session,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
    ) -> list[dict[str, Any]]:
        size_key_id = get_metadata_id(FILE_SIZE)
        sql = f"""
        WITH {scoped_cte},
        latest_size AS (
            SELECT
                m.asset_id,
                m.value_int AS size,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_int IS NOT NULL
        )
        SELECT asset_id, size
        FROM latest_size
        WHERE rn = 1
        ORDER BY size DESC
        LIMIT 50
        """
        rows = await select(session, sql, [*scoped_params, size_key_id])
        return [
            {"asset_id": int(row["asset_id"]), "size": int(row["size"])} for row in rows
        ]
