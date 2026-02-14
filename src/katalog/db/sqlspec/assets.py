from __future__ import annotations

import time
from typing import Any, Mapping, Sequence

from katalog.constants.metadata import (
    ASSET_ACTOR_ID,
    ASSET_CANONICAL_URI,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    ASSET_LOST,
    ASSET_NAMESPACE,
    METADATA_REGISTRY_BY_ID,
    MetadataKey,
    MetadataType,
    get_metadata_id,
)
from katalog.db.sqlspec.sql_helpers import execute, scalar, select, select_one_or_none
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import ASSET_TABLE, METADATA_TABLE
from katalog.db.utils import build_where

from katalog.models.assets import Asset
from katalog.models.metadata import Metadata
from katalog.models.query import AssetQuery
from katalog.models.query import AssetsListResponse, GroupedAssetsResponse
from katalog.models.views import ViewSpec
from katalog.db.metadata import get_metadata_repo
from katalog.db.sqlspec.query_fields import asset_filter_fields, asset_sort_fields
from katalog.db.sqlspec.query_filters import filter_conditions
from katalog.db.sqlspec.query_search import fts5_query_from_user_text
from katalog.db.sqlspec.query_sort import sort_conditions
from katalog.db.sqlspec.query_values import decode_metadata_value


def _build_assets_where(
    *,
    actor_id: int | None,
    filters: list[Any] | None,
    search: str | None,
) -> tuple[str, list[Any]]:
    conditions, filter_params = filter_conditions(filters)

    if actor_id is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM metadata m WHERE m.asset_id = a.id AND m.actor_id = ?)"
        )
        filter_params.extend([actor_id])

    if search is not None and search.strip():
        fts_query = fts5_query_from_user_text(search)
        if not fts_query:
            raise ValueError("Invalid search query")
        conditions.append(
            "a.id IN (SELECT rowid FROM asset_search WHERE asset_search MATCH ?)"
        )
        filter_params.append(fts_query)

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where_sql, filter_params


def _resolve_group_field(group_by: str) -> tuple[str, str]:
    if group_by in asset_filter_fields:
        return asset_filter_fields[group_by][0], "asset"
    return group_by, "metadata"


async def _has_canonical_merges(session, asset_table: str) -> bool:
    rows = await select(
        session,
        f"SELECT 1 FROM {asset_table} WHERE canonical_asset_id IS NOT NULL LIMIT 1",
    )
    return bool(rows)


class SqlspecAssetRepo:
    async def get_or_none(self, **filters: Any) -> Asset | None:
        rows = await self.list_rows(limit=1, **filters)
        return rows[0] if rows else None

    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **filters: Any,
    ) -> list[Asset]:
        where_sql, params = build_where(filters)
        order_sql = f"ORDER BY {order_by}" if order_by else ""
        limit_sql = "LIMIT :limit" if limit is not None else ""
        offset_sql = "OFFSET :offset" if offset is not None else ""
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        sql = (
            f"SELECT id, canonical_asset_id, actor_id, namespace, external_id, canonical_uri "
            f"FROM {ASSET_TABLE} {where_sql} {order_sql} {limit_sql} {offset_sql}"
        )
        async with session_scope() as session:
            rows = await select(session, sql, params)
        return [Asset.model_validate(row) for row in rows]

    async def save_record(
        self,
        asset: Asset,
        *,
        changeset: Any,
        actor: Any | None,
        session: Any | None = None,
    ) -> bool:
        _ = changeset
        if actor is None:
            raise ValueError("actor must be supplied to save_record")

        async def _do_save(active_session: Any, *, commit: bool) -> bool:
            was_created = False
            if asset.id is None:
                existing = await select_one_or_none(
                    active_session,
                    f"""
                    SELECT id, canonical_uri, canonical_asset_id, actor_id
                    FROM {ASSET_TABLE}
                    WHERE namespace = :namespace AND external_id = :external_id
                    """,
                    {"namespace": asset.namespace, "external_id": asset.external_id},
                )
                if existing:
                    asset.id = int(existing["id"])
                    asset.canonical_uri = existing["canonical_uri"]
                    asset.canonical_asset_id = existing.get("canonical_asset_id")
                    asset.actor_id = existing.get("actor_id")
                else:
                    was_created = True
                    if asset.actor_id is None:
                        asset.actor_id = actor.id
                    await execute(
                        active_session,
                        f"""
                        INSERT INTO {ASSET_TABLE} (
                            canonical_asset_id,
                            actor_id,
                            namespace,
                            external_id,
                            canonical_uri
                        )
                        VALUES (
                            :canonical_asset_id,
                            :actor_id,
                            :namespace,
                            :external_id,
                            :canonical_uri
                        )
                        """,
                        {
                            "canonical_asset_id": asset.canonical_asset_id,
                            "actor_id": asset.actor_id,
                            "namespace": asset.namespace,
                            "external_id": asset.external_id,
                            "canonical_uri": asset.canonical_uri,
                        },
                    )
                    asset.id = int(
                        await scalar(active_session, "SELECT last_insert_rowid() AS id")
                    )
            else:
                await execute(
                    active_session,
                    f"""
                    UPDATE {ASSET_TABLE}
                    SET canonical_asset_id = :canonical_asset_id,
                        actor_id = :actor_id,
                        namespace = :namespace,
                        external_id = :external_id,
                        canonical_uri = :canonical_uri
                    WHERE id = :id
                    """,
                    {
                        "id": int(asset.id),
                        "canonical_asset_id": asset.canonical_asset_id,
                        "actor_id": asset.actor_id,
                        "namespace": asset.namespace,
                        "external_id": asset.external_id,
                        "canonical_uri": asset.canonical_uri,
                    },
                )
            if commit:
                await active_session.commit()
            return was_created

        if session is not None:
            return await _do_save(session, commit=False)
        async with session_scope() as active:
            return await _do_save(active, commit=True)

    async def load_metadata(
        self,
        asset: Asset,
        *,
        include_removed: bool = True,
        session: Any | None = None,
    ) -> Sequence["Metadata"]:
        if asset.id is None:
            return []
        md_db = get_metadata_repo()
        return await md_db.for_asset(
            asset, include_removed=include_removed, session=session
        )

    async def mark_unseen_as_lost(
        self,
        *,
        changeset: Any,
        actor_ids: Sequence[int],
        seen_asset_ids: Sequence[int] | None = None,
    ) -> int:
        if not actor_ids:
            return 0

        affected = 0
        md_db = get_metadata_repo()
        seen_set = {int(a) for a in (seen_asset_ids or [])}

        async with session_scope() as session:
            for pid in actor_ids:
                seen_clause = ""
                params: dict[str, Any] = {"actor_id": int(pid)}
                if seen_set:
                    placeholders = ", ".join(
                        f":seen_{idx}" for idx, _ in enumerate(seen_set)
                    )
                    seen_clause = f"AND asset_id NOT IN ({placeholders})"
                    params.update(
                        {f"seen_{idx}": int(val) for idx, val in enumerate(seen_set)}
                    )

                rows = await select(
                    session,
                    f"""
                    SELECT DISTINCT asset_id
                    FROM {METADATA_TABLE}
                    WHERE actor_id = :actor_id
                      {seen_clause}
                    """,
                    params,
                )
                asset_ids = [int(r["asset_id"]) for r in rows]
                if not asset_ids:
                    continue

                lost_key_id = get_metadata_id(ASSET_LOST)
                now_rows = []
                for aid in asset_ids:
                    md = Metadata(
                        asset_id=aid,
                        actor_id=pid,
                        changeset_id=changeset.id,
                        metadata_key_id=lost_key_id,
                        value_type=MetadataType.INT,
                        value_int=1,
                        removed=False,
                    )
                    now_rows.append(md)

                await md_db.bulk_create(now_rows, session=session)
                affected += len(now_rows)

        return affected

    async def count_assets_for_query(
        self,
        *,
        query: AssetQuery,
    ) -> int:
        asset_table = ASSET_TABLE
        where_sql, filter_params = _build_assets_where(
            actor_id=None,
            filters=query.filters,
            search=query.search,
        )

        async with session_scope() as session:
            count_rows = await select(
                session,
                "SELECT COUNT(DISTINCT COALESCE(a.canonical_asset_id, a.id)) as cnt "
                f"FROM {asset_table} a {where_sql}",
                filter_params,
            )
        return int(count_rows[0]["cnt"]) if count_rows else 0

    async def list_asset_ids_for_query(
        self,
        *,
        query: AssetQuery,
    ) -> list[int]:
        offset = query.offset
        limit = query.limit
        if limit < 0 or offset < 0:
            raise ValueError("offset and limit must be non-negative")

        asset_table = ASSET_TABLE
        where_sql, filter_params = _build_assets_where(
            actor_id=None,
            filters=query.filters,
            search=query.search,
        )

        async with session_scope() as session:
            asset_rows = await select(
                session,
                f"""
                WITH effective AS (
                    SELECT DISTINCT COALESCE(a.canonical_asset_id, a.id) AS effective_id
                    FROM {asset_table} a
                    {where_sql}
                )
                SELECT a.id AS asset_id
                FROM {asset_table} a
                JOIN effective e ON e.effective_id = a.id
                ORDER BY a.id ASC
                LIMIT ? OFFSET ?
                """,
                [*filter_params, limit, offset],
            )
        return [int(row["asset_id"]) for row in asset_rows]

    async def list_assets_for_view_db(
        self,
        view: "ViewSpec",
        *,
        query: AssetQuery,
    ) -> "AssetsListResponse":
        started_at = time.perf_counter()
        assets_query_ms: int | None = None
        metadata_query_ms: int | None = None
        count_query_ms: int | None = None

        offset = query.offset
        limit = query.limit
        sort = query.sort[0] if query.sort else None
        filters = query.filters
        columns = None
        search = query.search

        if limit < 0 or offset < 0:
            raise ValueError("offset and limit must be non-negative")

        column_map = view.column_map()
        requested_columns = set(columns) if columns else set(column_map)
        unknown = requested_columns - set(column_map)
        if unknown:
            raise ValueError(f"Unknown columns requested: {sorted(unknown)}")

        order_by_clause = sort_conditions(
            sort, view, metadata_aggregation=query.metadata_aggregation
        )

        asset_table = ASSET_TABLE
        metadata_table = METADATA_TABLE
        where_sql, filter_params = _build_assets_where(
            actor_id=None,
            filters=filters,
            search=search,
        )

        metadata_keys = [
            col_id for col_id in requested_columns if col_id not in asset_sort_fields
        ]
        metadata_ids = [get_metadata_id(MetadataKey(key)) for key in metadata_keys]

        async with session_scope() as session:
            has_merges = await _has_canonical_merges(session, asset_table)

            if has_merges:
                assets_sql = f"""
                WITH effective AS (
                    SELECT DISTINCT COALESCE(a.canonical_asset_id, a.id) AS effective_id
                    FROM {asset_table} a
                    {where_sql}
                )
                SELECT
                    a.id AS asset_id,
                    a.actor_id AS asset_actor_id,
                    a.namespace,
                    a.external_id,
                    a.canonical_uri
                FROM {asset_table} a
                JOIN effective e ON e.effective_id = a.id
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
                """
            else:
                assets_sql = f"""
                SELECT
                    a.id AS asset_id,
                    a.actor_id AS asset_actor_id,
                    a.namespace,
                    a.external_id,
                    a.canonical_uri
                FROM {asset_table} a
                {where_sql}
                ORDER BY {order_by_clause}
                LIMIT ? OFFSET ?
                """
            assets_params = list(filter_params) + [limit, offset]

            assets_started = time.perf_counter()
            asset_rows = await select(session, assets_sql, assets_params)
            assets_query_ms = int((time.perf_counter() - assets_started) * 1000)

            assets: dict[int, dict[str, Any]] = {}
            ordered_columns: list[Mapping[str, Any]] = []
            for col in view.columns:
                if col.id in requested_columns:
                    ordered_columns.append(col.model_dump(mode="json"))

            page_asset_ids: list[int] = []
            for row in asset_rows:
                asset_id = int(row["asset_id"])
                page_asset_ids.append(asset_id)
                asset_entry: dict[str, Any] = {
                    str(ASSET_ID): asset_id,
                    str(ASSET_ACTOR_ID): row["asset_actor_id"],
                    str(ASSET_NAMESPACE): row["namespace"],
                    str(ASSET_EXTERNAL_ID): row["external_id"],
                    str(ASSET_CANONICAL_URI): row["canonical_uri"],
                }
                for key in metadata_keys:
                    asset_entry[key] = None
                assets[asset_id] = asset_entry

            if page_asset_ids and metadata_ids:
                asset_placeholders = ", ".join("?" for _ in page_asset_ids)
                key_placeholders = ", ".join("?" for _ in metadata_ids)
                if has_merges:
                    metadata_sql = f"""
                    WITH group_assets AS (
                        SELECT
                            a.id AS asset_id,
                            COALESCE(a.canonical_asset_id, a.id) AS effective_id
                        FROM {asset_table} a
                        WHERE a.id IN ({asset_placeholders})
                           OR a.canonical_asset_id IN ({asset_placeholders})
                    ),
                    latest_snap AS (
                        SELECT
                            ga.effective_id AS asset_id,
                            m.metadata_key_id,
                            MAX(m.changeset_id) AS changeset_id
                        FROM {metadata_table} m
                        JOIN group_assets ga ON ga.asset_id = m.asset_id
                        WHERE
                            m.removed = 0
                            AND m.metadata_key_id IN ({key_placeholders})
                        GROUP BY ga.effective_id, m.metadata_key_id
                    ),
                    latest_id AS (
                        SELECT
                            ga.effective_id AS asset_id,
                            m.metadata_key_id,
                            MAX(m.id) AS id
                        FROM {metadata_table} m
                        JOIN group_assets ga ON ga.asset_id = m.asset_id
                        JOIN latest_snap ls
                            ON ls.asset_id = ga.effective_id
                            AND ls.metadata_key_id = m.metadata_key_id
                            AND ls.changeset_id = m.changeset_id
                        WHERE m.removed = 0
                        GROUP BY ga.effective_id, m.metadata_key_id
                    )
                    SELECT
                        li.asset_id,
                        m.metadata_key_id,
                        m.value_type,
                        m.value_text,
                        m.value_int,
                        m.value_real,
                        m.value_datetime,
                        m.value_json,
                        m.value_relation_id,
                        m.value_collection_id
                    FROM {metadata_table} m
                    JOIN latest_id li ON li.id = m.id
                    """
                    metadata_params: list[Any] = (
                        list(page_asset_ids) + list(page_asset_ids) + list(metadata_ids)
                    )
                else:
                    metadata_sql = f"""
                    WITH latest_snap AS (
                        SELECT
                            m.asset_id,
                            m.metadata_key_id,
                            MAX(m.changeset_id) AS changeset_id
                        FROM {metadata_table} m
                        WHERE
                            m.removed = 0
                            AND m.asset_id IN ({asset_placeholders})
                            AND m.metadata_key_id IN ({key_placeholders})
                        GROUP BY m.asset_id, m.metadata_key_id
                    ),
                    latest_id AS (
                        SELECT
                            m.asset_id,
                            m.metadata_key_id,
                            MAX(m.id) AS id
                        FROM {metadata_table} m
                        JOIN latest_snap ls
                            ON ls.asset_id = m.asset_id
                            AND ls.metadata_key_id = m.metadata_key_id
                            AND ls.changeset_id = m.changeset_id
                        WHERE m.removed = 0
                        GROUP BY m.asset_id, m.metadata_key_id
                    )
                    SELECT
                        m.asset_id,
                        m.metadata_key_id,
                        m.value_type,
                        m.value_text,
                        m.value_int,
                        m.value_real,
                        m.value_datetime,
                        m.value_json,
                        m.value_relation_id,
                        m.value_collection_id
                    FROM {metadata_table} m
                    JOIN latest_id li ON li.id = m.id
                    """
                    metadata_params = list(page_asset_ids) + list(metadata_ids)
                metadata_started = time.perf_counter()
                metadata_rows = await select(session, metadata_sql, metadata_params)
                metadata_query_ms = int((time.perf_counter() - metadata_started) * 1000)

                for row in metadata_rows:
                    asset_id = int(row["asset_id"])
                    if asset_id not in assets:
                        continue
                    asset_entry = assets[asset_id]

                    key_def = METADATA_REGISTRY_BY_ID.get(int(row["metadata_key_id"]))
                    if key_def is None:
                        continue

                    value = decode_metadata_value(row)

                    key_str = str(key_def.key)
                    if key_str not in requested_columns:
                        continue
                    asset_entry[key_str] = value

            total_count = None
            if query.metadata_include_counts:
                if has_merges:
                    count_sql = (
                        "SELECT COUNT(DISTINCT COALESCE(a.canonical_asset_id, a.id)) as cnt "
                        f"FROM {asset_table} a {where_sql}"
                    )
                else:
                    count_sql = (
                        f"SELECT COUNT(*) as cnt FROM {asset_table} a {where_sql}"
                    )
                count_started = time.perf_counter()
                count_rows = await select(session, count_sql, filter_params)
                count_query_ms = int((time.perf_counter() - count_started) * 1000)
                total_count = int(count_rows[0]["cnt"]) if count_rows else 0

        duration_ms = int((time.perf_counter() - started_at) * 1000)

        return AssetsListResponse.model_validate(
            {
                "items": list(assets.values()),
                "schema": ordered_columns,
                "stats": {
                    "returned": len(assets),
                    "total": total_count,
                    "duration_ms": duration_ms,
                    "duration_assets_ms": assets_query_ms,
                    "duration_metadata_ms": metadata_query_ms,
                    "duration_count_ms": count_query_ms,
                },
                "pagination": {"offset": offset, "limit": limit},
            }
        )

    async def list_grouped_assets_db(
        self,
        view: "ViewSpec",
        *,
        group_by: str,
        query: AssetQuery,
    ) -> "GroupedAssetsResponse":
        _ = view
        offset = query.offset
        limit = query.limit
        filters = query.filters
        search = query.search
        group_by = query.group_by or group_by
        field_expr, field_type = _resolve_group_field(group_by)
        started_at = time.perf_counter()

        conditions, filter_params = filter_conditions(filters)
        if search is not None and search.strip():
            fts_query = fts5_query_from_user_text(search)
            if not fts_query:
                raise ValueError("Invalid search query")
            conditions.append(
                "a.id IN (SELECT rowid FROM asset_search WHERE asset_search MATCH ?)"
            )
            filter_params.append(fts_query)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        if field_type == "asset":
            group_sql = f"""
            SELECT
                {field_expr} AS group_value,
                COUNT(*) AS size,
                GROUP_CONCAT(a.id) AS asset_ids
            FROM {ASSET_TABLE} a
            {where_sql}
            GROUP BY {field_expr}
            ORDER BY size DESC, group_value
            LIMIT ? OFFSET ?
            """
            params = list(filter_params) + [limit, offset]
            count_sql = (
                f"SELECT COUNT(DISTINCT {field_expr}) AS cnt "
                f"FROM {ASSET_TABLE} a {where_sql}"
            )
            count_params = list(filter_params)
        else:
            registry_id = get_metadata_id(MetadataKey(group_by))
            group_sql = f"""
            WITH filtered AS (
                SELECT a.id AS asset_id
                FROM {ASSET_TABLE} a
                {where_sql}
            ),
            latest AS (
                SELECT
                    m.asset_id,
                    lower(trim(m.value_text)) AS val,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id
                        ORDER BY m.changeset_id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                JOIN filtered f ON f.asset_id = m.asset_id
                WHERE m.metadata_key_id = ?
                  AND m.removed = 0
                  AND m.value_text IS NOT NULL
            ),
            current AS (
                SELECT asset_id, val AS group_value
                FROM latest
                WHERE rn = 1 AND group_value != ''
            )
            SELECT
                group_value,
                COUNT(*) AS size,
                GROUP_CONCAT(asset_id) AS asset_ids
            FROM current
            GROUP BY group_value
            ORDER BY size DESC, group_value
            LIMIT ? OFFSET ?
            """
            params = list(filter_params) + [registry_id, limit, offset]
            count_sql = f"""
            WITH filtered AS (
                SELECT a.id AS asset_id
                FROM {ASSET_TABLE} a
                {where_sql}
            ),
            latest AS (
                SELECT
                    m.asset_id,
                    lower(trim(m.value_text)) AS val,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id
                        ORDER BY m.changeset_id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                JOIN filtered f ON f.asset_id = m.asset_id
                WHERE m.metadata_key_id = ?
                  AND m.removed = 0
                  AND m.value_text IS NOT NULL
            ),
            current AS (
                SELECT asset_id, val AS group_value
                FROM latest
                WHERE rn = 1 AND group_value != ''
            )
            SELECT COUNT(DISTINCT group_value) AS cnt FROM current
            """
            count_params = list(filter_params) + [registry_id]

        async with session_scope() as session:
            rows = await select(session, group_sql, params)
            total_groups = None
            if query.metadata_include_counts:
                count_rows = await select(session, count_sql, count_params)
                total_groups = int(count_rows[0]["cnt"]) if count_rows else 0

        items: list[dict[str, Any]] = []
        for row in rows:
            asset_ids = [int(a) for a in (row.get("asset_ids") or "").split(",") if a]
            items.append(
                {
                    "row_kind": "group",
                    "group_key": group_by,
                    "group_value": row.get("group_value"),
                    "group_size": int(row.get("size") or 0),
                    "sample_asset_ids": asset_ids[:5],
                    str(ASSET_ID): f"group:{row.get('group_value')}",
                    str(ASSET_ACTOR_ID): None,
                    str(ASSET_NAMESPACE): None,
                    str(ASSET_EXTERNAL_ID): None,
                    str(ASSET_CANONICAL_URI): None,
                }
            )

        duration_ms = int((time.perf_counter() - started_at) * 1000)

        return GroupedAssetsResponse.model_validate(
            {
                "mode": "groups",
                "group_by": group_by,
                "items": items,
                "stats": {
                    "returned": len(items),
                    "total_groups": total_groups,
                    "duration_ms": duration_ms,
                },
                "pagination": {"offset": offset, "limit": limit},
            }
        )

    def build_group_member_filter(
        self, group_by: str, group_value: str
    ) -> tuple[str, list[Any]]:
        field_expr, field_type = _resolve_group_field(group_by)
        if field_type == "asset":
            return f"{field_expr} = ?", [group_value]

        registry_id = get_metadata_id(MetadataKey(group_by))
        predicate = (
            "a.id IN (\n"
            "        WITH latest AS (\n"
            "            SELECT\n"
            "                m.asset_id,\n"
            "                lower(trim(m.value_text)) AS val,\n"
            "                ROW_NUMBER() OVER (PARTITION BY m.asset_id ORDER BY m.changeset_id DESC) AS rn\n"
            f"            FROM {METADATA_TABLE} m\n"
            "            WHERE m.metadata_key_id = ?\n"
            "              AND m.removed = 0\n"
            "              AND m.value_text IS NOT NULL\n"
            "        )\n"
            "        SELECT asset_id FROM latest WHERE rn = 1 AND val = ?\n"
            "    )"
        )
        return predicate, [registry_id, group_value]
