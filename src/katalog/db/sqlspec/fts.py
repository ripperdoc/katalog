from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.query_search import fts5_query_from_user_text
from katalog.db.sqlspec.sql_helpers import execute, scalar, select
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.db.fts import FtsPoint, FtsSearchHit


def fts_table_name(actor_id: int) -> str:
    return f"fts_index_actor_{int(actor_id)}"


class SqlspecFtsRepo:
    async def is_ready(self) -> tuple[bool, str | None]:
        try:
            async with session_scope(analysis=True) as session:
                await select(session, "SELECT 1")
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    async def has_index_records(self, *, actor_id: int) -> bool:
        table = fts_table_name(actor_id)
        async with session_scope(analysis=True) as session:
            table_rows = await select(
                session,
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                [table],
            )
            if not table_rows:
                return False
            data_rows = await select(session, f'SELECT 1 FROM "{table}" LIMIT 1')
        return bool(data_rows)

    async def upsert_asset_points(
        self,
        *,
        asset_id: int,
        actor_id: int,
        metadata_key_ids: Sequence[int],
        points: Sequence[FtsPoint],
    ) -> int:
        table = fts_table_name(actor_id)
        async with session_scope(analysis=True) as session:
            await self._ensure_table(session, actor_id=actor_id)
            if metadata_key_ids:
                key_placeholders = ", ".join("?" for _ in metadata_key_ids)
                await execute(
                    session,
                    f"""
                    DELETE FROM "{table}"
                    WHERE rowid IN (
                        SELECT id FROM {METADATA_TABLE}
                        WHERE asset_id = ?
                          AND metadata_key_id IN ({key_placeholders})
                    )
                    """,
                    [int(asset_id), *[int(key_id) for key_id in metadata_key_ids]],
                )
            else:
                await execute(
                    session,
                    f"""
                    DELETE FROM "{table}"
                    WHERE rowid IN (
                        SELECT id FROM {METADATA_TABLE}
                        WHERE asset_id = ?
                    )
                    """,
                    [int(asset_id)],
                )

            updated = 0
            for point in points:
                text = str(point.text or "").strip()
                if not text:
                    continue
                await execute(
                    session,
                    f'INSERT OR REPLACE INTO "{table}"(rowid, doc) VALUES (?, ?)',
                    [int(point.metadata_id), text],
                )
                updated += 1

            await session.commit()
            return updated

    async def search(
        self,
        *,
        actor_id: int,
        query_text: str,
        limit: int,
        offset: int = 0,
        asset_ids: Sequence[int] | None = None,
        metadata_key_ids: Sequence[int] | None = None,
    ) -> tuple[list[FtsSearchHit], int]:
        if limit <= 0:
            return [], 0
        if asset_ids is not None and len(asset_ids) == 0:
            return [], 0
        if metadata_key_ids is not None and len(metadata_key_ids) == 0:
            return [], 0

        table = fts_table_name(actor_id)
        fts_query = fts5_query_from_user_text(query_text)
        if not fts_query:
            raise ValueError("Invalid search query")

        async with session_scope(analysis=True) as session:
            table_rows = await select(
                session,
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                [table],
            )
            if not table_rows:
                return [], 0

            clauses = [f'"{table}" MATCH ?', "m.removed = 0"]
            params: list[Any] = [fts_query]
            if asset_ids:
                asset_placeholders = ", ".join("?" for _ in asset_ids)
                clauses.append(f"m.asset_id IN ({asset_placeholders})")
                params.extend(int(value) for value in asset_ids)
            if metadata_key_ids:
                key_placeholders = ", ".join("?" for _ in metadata_key_ids)
                clauses.append(f"m.metadata_key_id IN ({key_placeholders})")
                params.extend(int(value) for value in metadata_key_ids)
            where_sql = " AND ".join(clauses)

            total = int(
                await scalar(
                    session,
                    f"""
                    SELECT COUNT(*) AS cnt
                    FROM "{table}" f
                    JOIN {METADATA_TABLE} m ON m.id = f.rowid
                    WHERE {where_sql}
                    """,
                    params,
                )
                or 0
            )

            rows = await select(
                session,
                f"""
                SELECT
                    f.rowid AS metadata_id,
                    m.asset_id,
                    m.metadata_key_id,
                    m.value_text AS source_text,
                    bm25("{table}") AS rank
                FROM "{table}" f
                JOIN {METADATA_TABLE} m ON m.id = f.rowid
                WHERE {where_sql}
                ORDER BY rank ASC, f.rowid DESC
                LIMIT ? OFFSET ?
                """,
                [*params, int(limit), int(offset)],
            )
            hits = [
                FtsSearchHit(
                    metadata_id=int(row["metadata_id"]),
                    asset_id=int(row["asset_id"]),
                    metadata_key_id=int(row["metadata_key_id"]),
                    source_text=str(row["source_text"] or ""),
                    rank=float(row["rank"]) if row.get("rank") is not None else None,
                )
                for row in rows
            ]
            return hits, total

    async def _ensure_table(self, session: Any, *, actor_id: int) -> str:
        table = fts_table_name(actor_id)
        await execute(
            session,
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS "{table}"
            USING fts5(doc, tokenize='porter unicode61', detail='none')
            """,
        )
        return table
