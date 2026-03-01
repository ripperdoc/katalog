from __future__ import annotations

from typing import Any

from loguru import logger

from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import scalar, select


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class SqlspecSystemRepo:
    async def database_size_stats(self) -> dict[str, Any]:
        async with session_scope() as session:
            page_size = int(await scalar(session, "PRAGMA page_size") or 0)
            page_count = int(await scalar(session, "PRAGMA page_count") or 0)
            freelist_count = int(await scalar(session, "PRAGMA freelist_count") or 0)

            db_pages_total_bytes = page_size * page_count
            db_pages_free_bytes = page_size * freelist_count
            db_pages_used_bytes = max(0, db_pages_total_bytes - db_pages_free_bytes)

            schema_rows = await select(
                session,
                """
                SELECT name, type
                FROM sqlite_master
                WHERE type IN ('table', 'index')
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY type, name
                """,
            )

            table_names = [str(row["name"]) for row in schema_rows if row.get("type") == "table"]
            index_names = [str(row["name"]) for row in schema_rows if row.get("type") == "index"]

            dbstat_available = True
            dbstat_sizes_by_name: dict[str, tuple[int, int]] = {}
            try:
                dbstat_rows = await select(
                    session,
                    """
                    SELECT name, SUM(pgsize) AS bytes, COUNT(*) AS pages
                    FROM dbstat
                    GROUP BY name
                    """,
                )
                dbstat_sizes_by_name = {
                    str(row["name"]): (int(row.get("bytes") or 0), int(row.get("pages") or 0))
                    for row in dbstat_rows
                }
            except Exception as exc:  # noqa: BLE001
                logger.debug("SQLite dbstat unavailable, table byte sizes not exact: {}", exc)
                dbstat_available = False

            table_stats: list[dict[str, Any]] = []
            for table_name in table_names:
                row_count: int | None = None
                row_count_error: str | None = None
                try:
                    row_count = int(
                        await scalar(
                            session,
                            f"SELECT COUNT(*) AS cnt FROM {_quote_identifier(table_name)}",
                        )
                        or 0
                    )
                except Exception as exc:  # noqa: BLE001
                    row_count_error = str(exc)
                    logger.warning("Failed to count rows for table '{}': {}", table_name, exc)

                size_bytes, size_pages = dbstat_sizes_by_name.get(table_name, (0, 0))
                item: dict[str, Any] = {
                    "name": table_name,
                    "row_count": row_count,
                    "size_bytes": size_bytes if dbstat_available else None,
                    "size_pages": size_pages if dbstat_available else None,
                }
                if row_count_error:
                    item["row_count_error"] = row_count_error
                table_stats.append(item)

            index_stats: list[dict[str, Any]] = []
            for index_name in index_names:
                size_bytes, size_pages = dbstat_sizes_by_name.get(index_name, (0, 0))
                index_stats.append(
                    {
                        "name": index_name,
                        "size_bytes": size_bytes if dbstat_available else None,
                        "size_pages": size_pages if dbstat_available else None,
                    }
                )

            table_stats.sort(
                key=lambda item: (
                    -(int(item.get("size_bytes") or 0)),
                    str(item.get("name") or ""),
                )
            )
            index_stats.sort(
                key=lambda item: (
                    -(int(item.get("size_bytes") or 0)),
                    str(item.get("name") or ""),
                )
            )

            total_table_rows = sum(
                int(item["row_count"]) for item in table_stats if item.get("row_count") is not None
            )
            table_size_total_bytes = sum(int(item.get("size_bytes") or 0) for item in table_stats)
            index_size_total_bytes = sum(int(item.get("size_bytes") or 0) for item in index_stats)
            internal_size_total_bytes = (
                max(0, db_pages_used_bytes - table_size_total_bytes - index_size_total_bytes)
                if dbstat_available
                else None
            )

            return {
                "sqlite": {
                    "page_size_bytes": page_size,
                    "page_count": page_count,
                    "freelist_count": freelist_count,
                    "db_pages_total_bytes": db_pages_total_bytes,
                    "db_pages_free_bytes": db_pages_free_bytes,
                    "db_pages_used_bytes": db_pages_used_bytes,
                    "table_count": len(table_stats),
                    "index_count": len(index_stats),
                    "total_table_rows": total_table_rows,
                    "dbstat_available": dbstat_available,
                    "table_size_total_bytes": table_size_total_bytes if dbstat_available else None,
                    "index_size_total_bytes": index_size_total_bytes if dbstat_available else None,
                    "internal_size_total_bytes": internal_size_total_bytes,
                },
                "tables": table_stats,
                "indexes": index_stats,
            }
