from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import execute, select
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.db.vectors import VectorPoint, VectorSearchHit


class SqlspecVectorRepo:
    async def is_ready(self) -> tuple[bool, str | None]:
        try:
            async with session_scope(analysis=True) as session:
                await select(session, "SELECT 1")
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    async def upsert_asset_points(
        self,
        *,
        asset_id: int,
        actor_id: int,
        dim: int,
        metadata_key_ids: Sequence[int],
        points: Sequence[VectorPoint],
    ) -> int:
        async with session_scope(analysis=True) as session:
            vec_table = await self._ensure_vec_table(
                session,
                actor_id=actor_id,
                dim=dim,
            )

            if metadata_key_ids:
                key_placeholders = ", ".join("?" for _ in metadata_key_ids)
                await execute(
                    session,
                    f"""
                    DELETE FROM "{vec_table}"
                    WHERE rowid IN (
                        SELECT id FROM {METADATA_TABLE}
                        WHERE asset_id = ?
                          AND metadata_key_id IN ({key_placeholders})
                    )
                    """,
                    [int(asset_id), *[int(key_id) for key_id in metadata_key_ids]],
                )

            updated = 0
            for point in points:
                metadata_id = int(point.metadata_id)
                await execute(session, f'DELETE FROM "{vec_table}" WHERE rowid = ?', [metadata_id])
                await execute(
                    session,
                    f'INSERT INTO "{vec_table}"(rowid, embedding) VALUES (?, ?)',
                    [metadata_id, json.dumps(list(point.vector), separators=(",", ":"))],
                )
                updated += 1

            await session.commit()
            return updated

    async def search(
        self,
        *,
        actor_id: int,
        dim: int,
        query_vector: Sequence[float],
        limit: int,
        asset_ids: Sequence[int] | None = None,
    ) -> list[VectorSearchHit]:
        if limit <= 0:
            return []

        async with session_scope(analysis=True) as session:
            vec_table = self._vec_table_name(actor_id, dim)
            table_exists = await select(
                session,
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                [vec_table],
            )
            if not table_exists:
                return []

            params: list[Any] = [
                json.dumps(list(query_vector), separators=(",", ":")),
                int(limit),
            ]
            asset_clause = ""
            if asset_ids:
                placeholders = ", ".join("?" for _ in asset_ids)
                asset_clause = f" AND m.asset_id IN ({placeholders})"
                params.extend(int(aid) for aid in asset_ids)

            sql = f"""
                SELECT
                    v.rowid AS metadata_id,
                    m.asset_id,
                    m.metadata_key_id,
                    m.value_text AS source_text,
                    v.distance
                FROM "{vec_table}" v
                JOIN {METADATA_TABLE} m ON m.id = v.rowid
                WHERE v.embedding MATCH ?
                  AND k = ?
                  AND m.removed = 0
                  {asset_clause}
                ORDER BY v.distance ASC
                LIMIT ?
            """
            params.append(int(limit))
            rows = await select(session, sql, params)
            return [
                VectorSearchHit(
                    point_id=int(row["metadata_id"]),
                    asset_id=int(row["asset_id"]),
                    metadata_key_id=int(row["metadata_key_id"]),
                    metadata_id=int(row["metadata_id"]),
                    source_text=str(row["source_text"] or ""),
                    distance=float(row["distance"]),
                )
                for row in rows
            ]

    async def _ensure_vec_table(self, session: Any, *, actor_id: int, dim: int) -> str:
        table = self._vec_table_name(actor_id, dim)
        await execute(
            session,
            f'CREATE VIRTUAL TABLE IF NOT EXISTS "{table}" USING vec0(embedding float[{int(dim)}])',
        )
        return table

    @staticmethod
    def _vec_table_name(actor_id: int, dim: int) -> str:
        return f"vec_index_actor_{int(actor_id)}_{int(dim)}"
