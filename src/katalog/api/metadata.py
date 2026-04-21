from time import perf_counter

from katalog.api.helpers import ApiError
from katalog.api.search import ensure_fts_index_ready, semantic_hits_for_query
from katalog.constants.metadata import (
    MetadataDef,
    editable_metadata_schema,
    get_metadata_id,
    metadata_key_for_id_or_fallback,
    metadata_registry_by_id_for_current_db,
)
from katalog.db.assets import get_asset_repo
from katalog.db.fts import get_fts_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import MetadataChanges
from katalog.models.query import EditableMetadataSchemaResponse, AssetQuery, Pagination, QueryStats


async def metadata_schema_editable() -> EditableMetadataSchemaResponse:
    """Return JSON schema for editable metadata (non-asset/ keys)."""
    schema, ui_schema = editable_metadata_schema()
    return EditableMetadataSchemaResponse(schema=schema, uiSchema=ui_schema)


async def metadata_registry() -> dict[str, dict[int, MetadataDef]]:
    """Return metadata registry keyed by registry id."""
    return {"registry": metadata_registry_by_id_for_current_db()}


async def list_metadata(query: AssetQuery) -> dict:
    """List metadata rows for metadata-granularity queries."""
    await ensure_fts_index_ready(query)
    if query.search_granularity != "metadata":
        raise ApiError(
            status_code=400,
            detail="search_granularity must be 'metadata' for metadata search",
        )
    if query.search_mode == "fts" and (query.search or "").strip():
        return await _list_metadata_fts(query)
    if query.search_mode in {"semantic", "hybrid"}:
        return await _list_metadata_semantic(query)
    return await _list_metadata_direct(query)


async def _list_metadata_fts(query: AssetQuery) -> dict:
    """List metadata rows using full-text search hits."""
    started = perf_counter()
    search_text = (query.search or "").strip()
    if not search_text:
        stats = QueryStats(returned=0, total=0, duration_ms=0)
        pagination = Pagination(offset=query.offset, limit=query.limit)
        return {
            "items": [],
            "stats": stats.model_dump(),
            "pagination": pagination.model_dump(),
        }

    if query.search_index is None:
        raise ApiError(status_code=400, detail="search_index is required for fts metadata search")

    asset_db = get_asset_repo()
    scope_asset_ids: list[int] | None = None
    if query.filters:
        scoped_query = query.model_copy(
            update={
                "search": None,
                "search_granularity": "asset",
                "offset": 0,
                "limit": 1_000_000,
                "sort": None,
                "group_by": None,
            }
        )
        scope_asset_ids = await asset_db.list_asset_ids_for_query(query=scoped_query)
        if not scope_asset_ids:
            stats = QueryStats(returned=0, total=0, duration_ms=int((perf_counter() - started) * 1000))
            pagination = Pagination(offset=query.offset, limit=query.limit)
            return {
                "items": [],
                "stats": stats.model_dump(),
                "pagination": pagination.model_dump(),
            }

    metadata_key_ids = (
        [int(get_metadata_id(key)) for key in (query.search_metadata_keys or [])]
        or None
    )
    fts_db = get_fts_repo()
    hits, total = await fts_db.search(
        actor_id=int(query.search_index),
        query_text=search_text,
        limit=int(query.limit),
        offset=int(query.offset),
        asset_ids=scope_asset_ids,
        metadata_key_ids=metadata_key_ids,
    )
    asset_ids = sorted({hit.asset_id for hit in hits})
    assets = await asset_db.list_rows(order_by="id", id__in=asset_ids) if asset_ids else []
    assets_by_id = {int(asset.id): asset for asset in assets if asset.id is not None}

    items = []
    for hit in hits:
        asset = assets_by_id.get(hit.asset_id)
        metadata_key = str(metadata_key_for_id_or_fallback(int(hit.metadata_key_id)))
        items.append(
            {
                "asset_id": hit.asset_id,
                "metadata_id": hit.metadata_id,
                "metadata_key_id": hit.metadata_key_id,
                "metadata_key": metadata_key,
                "value": hit.source_text,
                "text": hit.source_text,
                "fts_rank": hit.rank,
                "distance": None,
                "score": None,
                "cosine_similarity": None,
                "asset_namespace": asset.namespace if asset else None,
                "asset_external_id": asset.external_id if asset else None,
                "asset_canonical_uri": asset.canonical_uri if asset else None,
            }
        )

    stats = QueryStats(
        returned=len(items),
        total=total,
        duration_ms=int((perf_counter() - started) * 1000),
    )
    pagination = Pagination(offset=query.offset, limit=query.limit)
    return {
        "items": items,
        "stats": stats.model_dump(),
        "pagination": pagination.model_dump(),
    }


async def _list_metadata_semantic(query: AssetQuery) -> dict:
    """List metadata rows using semantic vector hits."""
    hits, total, duration_ms = await semantic_hits_for_query(query)
    start = int(query.offset)
    end = start + int(query.limit)
    page = hits[start:end]

    asset_ids = sorted({hit.asset_id for hit in page})
    asset_db = get_asset_repo()
    assets = await asset_db.list_rows(order_by="id", id__in=asset_ids) if asset_ids else []
    assets_by_id = {int(asset.id): asset for asset in assets if asset.id is not None}

    items = []
    for hit in page:
        asset = assets_by_id.get(hit.asset_id)
        items.append(
            {
                "asset_id": hit.asset_id,
                "metadata_id": hit.metadata_id,
                "metadata_key_id": hit.metadata_key_id,
                "metadata_key": hit.metadata_key,
                "value": hit.text,
                "distance": hit.distance,
                "score": hit.score,
                "cosine_similarity": hit.cosine_similarity,
                "text": hit.text,
                "asset_namespace": asset.namespace if asset else None,
                "asset_external_id": asset.external_id if asset else None,
                "asset_canonical_uri": asset.canonical_uri if asset else None,
            }
        )
    stats = QueryStats(returned=len(items), total=total, duration_ms=duration_ms)
    pagination = Pagination(offset=query.offset, limit=query.limit)
    return {
        "items": items,
        "stats": stats.model_dump(),
        "pagination": pagination.model_dump(),
    }


async def _list_metadata_direct(query: AssetQuery) -> dict:
    """List metadata rows directly from scoped assets without search."""
    asset_db = get_asset_repo()
    metadata_db = get_metadata_repo()
    scoped_query = query.model_copy(
        update={
            "search_granularity": "asset",
            "offset": 0,
            "limit": 1_000_000,
            "sort": None,
            "group_by": None,
        }
    )
    asset_ids = await asset_db.list_asset_ids_for_query(query=scoped_query)
    if not asset_ids:
        stats = QueryStats(returned=0, total=0, duration_ms=0)
        pagination = Pagination(offset=query.offset, limit=query.limit)
        return {
            "items": [],
            "stats": stats.model_dump(),
            "pagination": pagination.model_dump(),
        }

    metadata_by_asset = await metadata_db.for_assets(
        asset_ids,
        include_removed=bool(query.metadata_include_removed),
    )
    actor_filter = set(query.metadata_actor_ids or [])
    key_filter = {str(key) for key in (query.search_metadata_keys or [])}
    aggregated_rows: list[dict] = []
    for asset_id in asset_ids:
        entries = list(metadata_by_asset.get(int(asset_id), []))
        if actor_filter:
            entries = [
                entry
                for entry in entries
                if entry.actor_id is not None and int(entry.actor_id) in actor_filter
            ]
        if query.metadata_aggregation == "latest":
            current = MetadataChanges(loaded=entries).current()
            latest_entries = [entry for values in current.values() for entry in values]
            entries = sorted(
                latest_entries,
                key=lambda item: (
                    int(item.changeset_id or 0),
                    int(item.id or 0),
                ),
                reverse=True,
            )
        else:
            entries = sorted(
                entries,
                key=lambda item: (
                    int(item.changeset_id or 0),
                    int(item.id or 0),
                ),
                reverse=True,
            )
        for entry in entries:
            key = str(entry.key)
            if key_filter and key not in key_filter:
                continue
            value = entry.value
            text = value if isinstance(value, str) else (str(value) if value is not None else "")
            aggregated_rows.append(
                {
                    "asset_id": int(asset_id),
                    "metadata_id": int(entry.id) if entry.id is not None else None,
                    "metadata_key_id": int(entry.metadata_key_id)
                    if entry.metadata_key_id is not None
                    else None,
                    "metadata_key": key,
                    "value": value,
                    "distance": None,
                    "score": None,
                    "cosine_similarity": None,
                    "text": text,
                    "actor_id": int(entry.actor_id) if entry.actor_id is not None else None,
                    "changeset_id": int(entry.changeset_id)
                    if entry.changeset_id is not None
                    else None,
                    "removed": bool(entry.removed),
                }
            )

    total = len(aggregated_rows)
    start = int(query.offset)
    end = start + int(query.limit)
    page = aggregated_rows[start:end]

    page_asset_ids = sorted({int(item["asset_id"]) for item in page})
    assets = (
        await asset_db.list_rows(order_by="id", id__in=page_asset_ids)
        if page_asset_ids
        else []
    )
    assets_by_id = {int(asset.id): asset for asset in assets if asset.id is not None}

    items = []
    for item in page:
        asset = assets_by_id.get(int(item["asset_id"]))
        row = dict(item)
        row["asset_namespace"] = asset.namespace if asset else None
        row["asset_external_id"] = asset.external_id if asset else None
        row["asset_canonical_uri"] = asset.canonical_uri if asset else None
        items.append(row)

    stats = QueryStats(returned=len(items), total=total, duration_ms=0)
    pagination = Pagination(offset=query.offset, limit=query.limit)
    return {
        "items": items,
        "stats": stats.model_dump(),
        "pagination": pagination.model_dump(),
    }
