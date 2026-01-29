from __future__ import annotations

from typing import Any

from loguru import logger
from tortoise import Tortoise

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
from katalog.models import Asset, Changeset, Metadata


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
        conn = Tortoise.get_connection("default")
        metadata_table = Metadata._meta.db_table
        asset_table = Asset._meta.db_table
        scoped_cte, scoped_params = build_scoped_assets_cte(
            scope,
            asset_table=asset_table,
            metadata_table=metadata_table,
        )

        asset_count = await self._count_assets(
            conn, scoped_cte, scoped_params, asset_table
        )
        logger.info("Stats analyzer assets counted: {count}", count=asset_count)
        size_stats = await self._size_stats(
            conn, scoped_cte, scoped_params, metadata_table
        )
        type_breakdown = await self._string_breakdown(
            conn,
            scoped_cte,
            scoped_params,
            metadata_table,
            get_metadata_id(FILE_TYPE),
            limit=50,
        )
        extension_breakdown = await self._string_breakdown(
            conn,
            scoped_cte,
            scoped_params,
            metadata_table,
            get_metadata_id(FILE_EXTENSION),
            limit=50,
        )
        modified_stats = await self._modified_stats(
            conn, scoped_cte, scoped_params, metadata_table
        )
        coverage = await self._coverage_stats(
            conn, scoped_cte, scoped_params, metadata_table, asset_count
        )
        duplicates = await self._duplicate_stats(
            conn, scoped_cte, scoped_params, metadata_table
        )
        largest_assets = await self._largest_assets(
            conn, scoped_cte, scoped_params, metadata_table
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

        return AnalyzerResult(output=output)

    @staticmethod
    async def _count_assets(
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        asset_table: str,
    ) -> int:
        sql = f"""
        WITH {scoped_cte}
        SELECT COUNT(*) AS cnt FROM scoped_assets
        """
        rows = await conn.execute_query_dict(sql, scoped_params)
        return int(rows[0]["cnt"]) if rows else 0

    async def _size_stats(
        self,
        conn,
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
        rows = await conn.execute_query_dict(stats_sql, scoped_params + [size_key_id])
        if not rows:
            return {
                "count": 0,
                "total": 0,
                "min": None,
                "max": None,
                "avg": None,
                "median": None,
                "p90": None,
            }
        row = rows[0]
        count = int(row.get("cnt") or 0)
        total = int(row.get("total") or 0)
        min_val = row.get("min")
        max_val = row.get("max")
        avg_val = row.get("avg")

        median = None
        p90 = None
        if count > 0:
            median_offset = int((count - 1) / 2)
            p90_offset = int((count - 1) * 0.9)
            median = await self._size_percentile(
                conn,
                scoped_cte,
                scoped_params,
                metadata_table,
                size_key_id,
                median_offset,
            )
            p90 = await self._size_percentile(
                conn,
                scoped_cte,
                scoped_params,
                metadata_table,
                size_key_id,
                p90_offset,
            )

        return {
            "count": count,
            "total": total,
            "min": int(min_val) if min_val is not None else None,
            "max": int(max_val) if max_val is not None else None,
            "avg": float(avg_val) if avg_val is not None else None,
            "median": median,
            "p90": p90,
        }

    async def _size_percentile(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        size_key_id: int,
        offset: int,
    ) -> int | None:
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
        SELECT size
        FROM latest_size
        WHERE rn = 1
        ORDER BY size
        LIMIT 1 OFFSET ?
        """
        rows = await conn.execute_query_dict(
            sql, scoped_params + [size_key_id, int(offset)]
        )
        if not rows:
            return None
        value = rows[0].get("size")
        return int(value) if value is not None else None

    async def _modified_stats(
        self,
        conn,
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
                m.value_datetime AS dt,
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
            MIN(dt) AS oldest,
            MAX(dt) AS newest,
            SUM(CASE WHEN dt >= datetime('now', '-30 days') THEN 1 ELSE 0 END) AS last_30d,
            SUM(CASE WHEN dt >= datetime('now', '-365 days') THEN 1 ELSE 0 END) AS last_365d,
            SUM(CASE WHEN dt >= datetime('now', '-3650 days') THEN 1 ELSE 0 END) AS last_3650d,
            COUNT(*) AS count
        FROM latest_mod
        WHERE rn = 1
        """
        rows = await conn.execute_query_dict(sql, scoped_params + [modified_key_id])
        if not rows:
            return {
                "count": 0,
                "oldest": None,
                "newest": None,
                "recent_counts": {"30d": 0, "365d": 0, "3650d": 0},
            }
        row = rows[0]
        return {
            "count": int(row.get("count") or 0),
            "oldest": row.get("oldest"),
            "newest": row.get("newest"),
            "recent_counts": {
                "30d": int(row.get("last_30d") or 0),
                "365d": int(row.get("last_365d") or 0),
                "3650d": int(row.get("last_3650d") or 0),
            },
        }

    async def _string_breakdown(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        metadata_key_id: int,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = f"""
        WITH {scoped_cte},
        latest_val AS (
            SELECT
                m.asset_id,
                lower(trim(m.value_text)) AS val,
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
        SELECT val AS value, COUNT(*) AS count
        FROM latest_val
        WHERE rn = 1 AND val != ''
        GROUP BY val
        ORDER BY count DESC, val
        LIMIT ?
        """
        rows = await conn.execute_query_dict(
            sql, scoped_params + [metadata_key_id, int(limit)]
        )
        return [
            {"value": row.get("value") or "", "count": int(row.get("count") or 0)}
            for row in rows
        ]

    async def _coverage_stats(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        asset_count: int,
    ) -> dict[str, Any]:
        coverage: dict[str, Any] = {}
        targets = {
            "file_size": (get_metadata_id(FILE_SIZE), "value_int IS NOT NULL"),
            "file_type": (
                get_metadata_id(FILE_TYPE),
                "value_text IS NOT NULL AND trim(value_text) != ''",
            ),
            "time_modified": (
                get_metadata_id(TIME_MODIFIED),
                "value_datetime IS NOT NULL",
            ),
            "hash_md5": (
                get_metadata_id(HASH_MD5),
                "value_text IS NOT NULL AND trim(value_text) != ''",
            ),
        }
        for label, (key_id, condition) in targets.items():
            count = await self._count_latest_values(
                conn,
                scoped_cte,
                scoped_params,
                metadata_table,
                key_id,
                condition,
            )
            coverage[label] = {
                "present": count,
                "missing": max(asset_count - count, 0),
            }
        return coverage

    async def _count_latest_values(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        metadata_key_id: int,
        value_condition: str,
    ) -> int:
        sql = f"""
        WITH {scoped_cte},
        latest_val AS (
            SELECT
                m.asset_id,
                m.value_text,
                m.value_int,
                m.value_real,
                m.value_datetime,
                m.value_json,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id
                    ORDER BY m.changeset_id DESC, m.id DESC
                ) AS rn
            FROM {metadata_table} AS m
            JOIN scoped_assets s ON s.asset_id = m.asset_id
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
        )
        SELECT COUNT(*) AS cnt
        FROM latest_val
        WHERE rn = 1 AND ({value_condition})
        """
        rows = await conn.execute_query_dict(sql, scoped_params + [metadata_key_id])
        return int(rows[0]["cnt"]) if rows else 0

    async def _duplicate_stats(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
    ) -> dict[str, Any]:
        hash_key_id = get_metadata_id(HASH_MD5)
        sql = f"""
        WITH {scoped_cte},
        latest_md5 AS (
            SELECT
                m.asset_id,
                lower(trim(m.value_text)) AS md5,
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
        current_md5 AS (
            SELECT asset_id, md5
            FROM latest_md5
            WHERE rn = 1 AND md5 != ''
        ),
        groups AS (
            SELECT md5, COUNT(*) AS file_count
            FROM current_md5
            GROUP BY md5
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) AS group_count, SUM(file_count) AS asset_count
        FROM groups
        """
        rows = await conn.execute_query_dict(sql, scoped_params + [hash_key_id])
        if not rows:
            return {"group_count": 0, "asset_count": 0}
        row = rows[0]
        return {
            "group_count": int(row.get("group_count") or 0),
            "asset_count": int(row.get("asset_count") or 0),
        }

    async def _largest_assets(
        self,
        conn,
        scoped_cte: str,
        scoped_params: list[Any],
        metadata_table: str,
        *,
        limit: int = 10,
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
        LIMIT ?
        """
        rows = await conn.execute_query_dict(
            sql, scoped_params + [size_key_id, int(limit)]
        )
        return [
            {
                "asset_id": int(row.get("asset_id") or 0),
                "size": int(row.get("size") or 0),
            }
            for row in rows
        ]
