from __future__ import annotations

from typing import Any, Sequence, TYPE_CHECKING
from typing import Iterable

from katalog.constants.metadata import (
    ASSET_SEARCH_DOC,
    INTERNAL_FTS_REINDEX,
    INTERNAL_VECTOR_REINDEX,
    METADATA_REGISTRY,
    MetadataType,
    MetadataKey,
    VECTOR_INDEXED_COUNT,
    get_metadata_def_by_key,
    get_metadata_id,
)
from katalog.db.fts import FtsPoint, get_fts_repo
from katalog.db.vectors import VectorPoint, get_vector_repo
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.db.sqlspec.sql_helpers import select
from katalog.models.metadata import _metadata_to_row, _normalize_metadata_row, Metadata
from katalog.db.sqlspec.sql_helpers import execute
from katalog.db.actors import get_actor_repo
from katalog.processors.vector_index import KreuzbergVectorIndexProcessor
from katalog.models import make_metadata
from katalog.vectors.embedding import embed_text_kreuzberg

if TYPE_CHECKING:
    from katalog.models.assets import Asset
    from katalog.models.metadata import MetadataChanges


class SqlspecMetadataRepo:
    async def for_asset(
        self,
        asset: Asset | int,
        *,
        include_removed: bool = False,
        session: Any | None = None,
    ) -> Sequence[Metadata]:
        from katalog.models.assets import Asset

        asset_id = asset.id if isinstance(asset, Asset) else int(asset)
        if asset_id is None:
            return []
        where_sql = "WHERE asset_id = :asset_id"
        params = {"asset_id": int(asset_id)}
        if not include_removed:
            where_sql += " AND removed = 0"
        sql = (
            f"SELECT id, asset_id, actor_id, changeset_id, metadata_key_id, value_type, "
            f"value_text, value_int, value_real, value_datetime, value_json, value_relation_id, "
            f"value_collection_id, removed, confidence "
            f"FROM {METADATA_TABLE} {where_sql} ORDER BY metadata_key_id, id"
        )

        async def _fetch(active_session: Any) -> Sequence[Metadata]:
            rows = await select(active_session, sql, params)
            return [
                Metadata.model_validate(_normalize_metadata_row(row)) for row in rows
            ]

        if session is not None:
            return await _fetch(session)
        async with session_scope() as active:
            return await _fetch(active)

    async def for_assets(
        self,
        asset_ids: Sequence[int],
        *,
        include_removed: bool = False,
        session: Any | None = None,
    ) -> dict[int, list[Metadata]]:
        if not asset_ids:
            return {}
        placeholders = ", ".join("?" for _ in asset_ids)
        where_sql = f"WHERE asset_id IN ({placeholders})"
        if not include_removed:
            where_sql += " AND removed = 0"
        sql = (
            f"SELECT id, asset_id, actor_id, changeset_id, metadata_key_id, value_type, "
            f"value_text, value_int, value_real, value_datetime, value_json, value_relation_id, "
            f"value_collection_id, removed, confidence "
            f"FROM {METADATA_TABLE} {where_sql} ORDER BY asset_id, metadata_key_id, id"
        )

        async def _fetch(active_session: Any) -> dict[int, list[Metadata]]:
            rows = await select(active_session, sql, list(asset_ids))
            grouped: dict[int, list[Metadata]] = {}
            for row in rows:
                entry = Metadata.model_validate(_normalize_metadata_row(row))
                if entry.asset_id is None:
                    continue
                grouped.setdefault(int(entry.asset_id), []).append(entry)
            return grouped

        if session is not None:
            return await _fetch(session)
        async with session_scope() as active:
            return await _fetch(active)

    async def bulk_create(
        self, metadata: Sequence[Metadata], *, session: Any | None = None
    ) -> None:
        if not metadata:
            return

        rows = [_metadata_to_row(entry) for entry in metadata]
        sql = f"""
        INSERT INTO {METADATA_TABLE} (
            asset_id, actor_id, changeset_id, metadata_key_id, value_type,
            value_text, value_int, value_real, value_datetime, value_json,
            value_relation_id, value_collection_id, removed, confidence
        ) VALUES (
            :asset_id, :actor_id, :changeset_id, :metadata_key_id, :value_type,
            :value_text, :value_int, :value_real, :value_datetime, :value_json,
            :value_relation_id, :value_collection_id, :removed, :confidence
        )
        """

        async def _insert(active_session: Any, *, commit: bool) -> None:
            await active_session.execute_many(sql, rows)
            if commit:
                await active_session.commit()

        if session is not None:
            await _insert(session, commit=False)
        else:
            async with session_scope() as active:
                await _insert(active, commit=True)

    async def persist_changes(
        self,
        changes: "MetadataChanges",
        *,
        changeset: Any,
        existing_metadata: Sequence[Metadata] | None = None,
        session: Any | None = None,
    ) -> set["MetadataKey"]:
        async def _persist(active_session: Any, *, commit: bool) -> set["MetadataKey"]:
            resolved_asset = changes.asset
            if resolved_asset is None:
                raise ValueError("MetadataChanges.asset is not set for persistence")
            if existing_metadata is None:
                loaded_metadata = await self.for_asset(
                    resolved_asset, include_removed=True, session=active_session
                )
            else:
                loaded_metadata = existing_metadata
            to_create, changed_keys = changes.prepare_persist(
                changeset=changeset,
                existing_metadata=loaded_metadata,
            )
            if to_create:
                await self.bulk_create(to_create, session=active_session)
                if commit:
                    await active_session.commit()
            return changed_keys

        if session is not None:
            return await _persist(session, commit=False)
        async with session_scope() as active:
            return await _persist(active, commit=True)

    async def persist_changes_batch(
        self,
        changeset: Any,
        changes_list: Sequence["MetadataChanges"],
        existing_metadata_by_asset: dict[int, list[Metadata]],
        *,
        session: Any | None = None,
    ) -> tuple[int, int, int]:
        async def _persist_batch(
            active_session: Any, *, commit: bool
        ) -> tuple[int, int, int, set[tuple[int, int]], set[tuple[int, int]]]:
            normal_rows: list[Metadata] = []
            fts_reindex_requests: set[tuple[int, int]] = set()
            vector_reindex_requests: set[tuple[int, int]] = set()
            fts_reindex_key_id = int(get_metadata_id(INTERNAL_FTS_REINDEX))
            vector_reindex_key_id = int(get_metadata_id(INTERNAL_VECTOR_REINDEX))
            for changes in changes_list:
                asset = changes.asset
                if asset is None:
                    continue
                if asset.id is None:
                    continue
                existing = existing_metadata_by_asset.get(int(asset.id), [])
                to_create, _changed = changes.prepare_persist(
                    changeset=changeset,
                    existing_metadata=existing,
                )
                for entry in to_create:
                    metadata_key_id = entry.metadata_key_id
                    if metadata_key_id is None:
                        continue
                    if int(metadata_key_id) == fts_reindex_key_id:
                        if entry.asset_id is None or entry.actor_id is None:
                            continue
                        fts_reindex_requests.add((int(entry.asset_id), int(entry.actor_id)))
                        continue
                    if int(metadata_key_id) == vector_reindex_key_id:
                        if entry.asset_id is None or entry.actor_id is None:
                            continue
                        vector_reindex_requests.add((int(entry.asset_id), int(entry.actor_id)))
                        continue
                    normal_rows.append(entry)

            if normal_rows:
                await self.bulk_create(normal_rows, session=active_session)
            if commit:
                await active_session.commit()
            return len(normal_rows), 0, 0, fts_reindex_requests, vector_reindex_requests

        if session is not None:
            normal_rows, search_rows, delete_rows, _requests, _vector_requests = await _persist_batch(
                session, commit=False
            )
            return normal_rows, search_rows, delete_rows
        async with session_scope() as active:
            await execute(active, "BEGIN")
            try:
                normal_rows, _search_rows, _delete_rows, requests, vector_requests = await _persist_batch(
                    active, commit=False
                )
                await execute(active, "COMMIT")
                fts_indexed_count = await self._apply_fts_reindex_requests(requests=requests)
                vector_indexed_count = await self._apply_vector_reindex_requests(
                    requests=vector_requests,
                    changeset=changeset,
                )
                return normal_rows, fts_indexed_count + vector_indexed_count, 0
            except Exception:
                await execute(active, "ROLLBACK")
                raise

    async def _apply_fts_reindex_requests(
        self, *, requests: set[tuple[int, int]]
    ) -> int:
        if not requests:
            return 0
        searchable_key_ids = _searchable_metadata_key_ids()
        if not searchable_key_ids:
            return 0

        total_indexed = 0
        fts_repo = get_fts_repo()
        for asset_id, actor_id in sorted(requests):
            entries = await self.for_asset(asset_id, include_removed=False)
            points = _to_fts_points(entries, searchable_key_ids=searchable_key_ids)
            indexed = await fts_repo.upsert_asset_points(
                asset_id=int(asset_id),
                actor_id=int(actor_id),
                metadata_key_ids=sorted(searchable_key_ids),
                points=points,
            )
            total_indexed += int(indexed)
        return total_indexed

    async def _apply_vector_reindex_requests(
        self, *, requests: set[tuple[int, int]], changeset: Any
    ) -> int:
        if not requests:
            return 0
        actor_repo = get_actor_repo()
        vector_repo = get_vector_repo()
        total_indexed = 0
        indexed_count_entries: list[Metadata] = []
        for asset_id, actor_id in sorted(requests):
            actor = await actor_repo.get_or_none(id=int(actor_id))
            if actor is None or not actor.plugin_id:
                continue
            if actor.plugin_id != KreuzbergVectorIndexProcessor.plugin_id:
                continue
            config = KreuzbergVectorIndexProcessor.ConfigModel.model_validate(actor.config or {})
            dependencies = frozenset(MetadataKey(key) for key in config.metadata_keys)
            metadata_key_ids: list[int] = []
            for key in dependencies:
                registry_id = get_metadata_def_by_key(key).registry_id
                if registry_id is not None:
                    metadata_key_ids.append(int(registry_id))
            entries = await self.for_asset(asset_id, include_removed=False)
            points = await _to_vector_points(
                entries,
                dependencies=dependencies,
                model=config.embedding_model,
                backend=str(config.embedding_backend),
                normalize=bool(config.embedding_normalize),
                batch_size=int(config.embedding_batch_size),
                dim=int(config.dimension),
                min_text_length=int(config.min_text_length),
                max_points=int(config.max_points),
            )
            indexed = await vector_repo.upsert_asset_points(
                asset_id=int(asset_id),
                actor_id=int(actor_id),
                dim=int(config.dimension),
                metadata_key_ids=metadata_key_ids,
                points=points,
            )
            indexed_count_entries.append(
                make_metadata(
                    VECTOR_INDEXED_COUNT,
                    int(indexed),
                    actor_id=int(actor_id),
                    asset_id=int(asset_id),
                    changeset_id=int(changeset.id) if changeset.id is not None else None,
                )
            )
            total_indexed += int(indexed)
        if indexed_count_entries:
            await self.bulk_create(indexed_count_entries)
        return total_indexed


def _searchable_metadata_key_ids() -> set[int]:
    key_ids: set[int] = set()
    for key, definition in METADATA_REGISTRY.items():
        key_str = str(key)
        if key == ASSET_SEARCH_DOC or key_str.startswith("asset/") or key_str.startswith("internal/"):
            continue
        if definition.searchable is not None:
            if not definition.searchable:
                continue
        elif definition.value_type not in {MetadataType.STRING, MetadataType.JSON}:
            continue
        key_ids.add(int(get_metadata_id(key)))
    return key_ids


def _to_fts_points(
    entries: Sequence[Metadata], *, searchable_key_ids: set[int]
) -> list[FtsPoint]:
    points: list[FtsPoint] = []
    for entry in entries:
        metadata_id = entry.id
        metadata_key_id = entry.metadata_key_id
        if metadata_id is None or metadata_key_id is None:
            continue
        if int(metadata_key_id) not in searchable_key_ids:
            continue
        value = entry.value
        if value is None:
            continue
        text = value.isoformat() if hasattr(value, "isoformat") else str(value)
        cleaned = text.strip()
        if not cleaned:
            continue
        points.append(FtsPoint(metadata_id=int(metadata_id), text=cleaned))
    return points


async def _to_vector_points(
    entries: Sequence[Metadata],
    *,
    dependencies: set[MetadataKey] | frozenset[MetadataKey],
    model: str,
    backend: str,
    normalize: bool,
    batch_size: int,
    dim: int,
    min_text_length: int,
    max_points: int,
) -> list[VectorPoint]:
    key_id_to_key: dict[int, MetadataKey] = {}
    for key in dependencies:
        registry_id = get_metadata_def_by_key(key).registry_id
        if registry_id is None:
            continue
        key_id_to_key[int(registry_id)] = key
    points: list[VectorPoint] = []
    for entry in entries:
        entry_id = entry.id
        metadata_key_id = entry.metadata_key_id
        if entry_id is None or metadata_key_id is None:
            continue
        if int(metadata_key_id) not in key_id_to_key:
            continue
        value = entry.value
        if not isinstance(value, str):
            continue
        text = value.strip()
        if len(text) < min_text_length:
            continue
        vector = await embed_text_kreuzberg(
            text,
            model=model,
            backend=backend,
            normalize=normalize,
            batch_size=batch_size,
            dim=dim,
        )
        points.append(VectorPoint(metadata_id=int(entry_id), vector=vector))
        if len(points) >= max_points:
            break
    return points

    async def list_active_collection_asset_ids(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
        asset_ids: Sequence[int],
    ) -> list[int]:
        if not asset_ids:
            return []
        asset_placeholders = ", ".join("?" for _ in asset_ids)
        sql = f"""
            WITH latest AS (
                SELECT
                    m.asset_id,
                    m.removed,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id, m.value_collection_id
                        ORDER BY m.changeset_id DESC, m.id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                WHERE m.metadata_key_id = ?
                  AND m.value_collection_id = ?
                  AND m.asset_id IN ({asset_placeholders})
            )
            SELECT asset_id FROM latest WHERE rn = 1 AND removed = 0
        """
        async with session_scope() as session:
            rows = await select(
                session,
                sql,
                [membership_key_id, collection_id, *asset_ids],
            )
        return [int(row["asset_id"]) for row in rows]

    async def list_removed_collection_asset_ids(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
        actor_id: int,
        changeset_id: int,
        asset_ids: Sequence[int],
    ) -> set[int]:
        if not asset_ids:
            return set()
        asset_placeholders = ", ".join("?" for _ in asset_ids)
        sql = f"""
            SELECT asset_id
            FROM {METADATA_TABLE}
            WHERE metadata_key_id = ?
              AND value_collection_id = ?
              AND actor_id = ?
              AND changeset_id = ?
              AND removed = 1
              AND asset_id IN ({asset_placeholders})
        """
        async with session_scope() as session:
            rows = await select(
                session,
                sql,
                [membership_key_id, collection_id, actor_id, changeset_id, *asset_ids],
            )
        return {int(row["asset_id"]) for row in rows}

    async def count_active_collection_assets(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
    ) -> int:
        sql = f"""
            WITH latest AS (
                SELECT
                    m.asset_id,
                    m.removed,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id, m.value_collection_id
                        ORDER BY m.changeset_id DESC, m.id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                WHERE m.metadata_key_id = ?
                  AND m.value_collection_id = ?
            )
            SELECT COUNT(*) AS cnt FROM latest WHERE rn = 1 AND removed = 0
        """
        async with session_scope() as session:
            rows = await select(session, sql, [membership_key_id, collection_id])
        return int(rows[0]["cnt"]) if rows else 0
