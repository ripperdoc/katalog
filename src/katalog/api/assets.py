from typing import Any, Sequence

from katalog.api.search import ensure_fts_index_ready, semantic_hits_for_query
from katalog.constants.metadata import MetadataKey
from katalog.db.assets import get_asset_repo
from katalog.editors.user_editor import ensure_user_editor
from katalog.models import Asset, Metadata, MetadataChanges, make_metadata
from katalog.models.query import AssetQuery
from katalog.models.views import get_view
from katalog.api.helpers import ApiError
from katalog.api.schemas import (
    AssetsListResponse,
    GroupedAssetsResponse,
    ManualEditResult,
)
from katalog.db.metadata import get_metadata_repo
from loguru import logger



async def list_assets(query: AssetQuery) -> AssetsListResponse:
    """List assets for a query, including semantic search when requested."""
    await ensure_fts_index_ready(query)
    if query.search_mode in {"semantic", "hybrid"} and query.search_granularity == "asset":
        return await _list_assets_semantic(query)
    view = get_view(query.view_id or "default")
    db = get_asset_repo()
    # TODO: metadata_actor_ids support is intentionally skipped for now.
    return await db.list_assets_for_view_db(view, query=query)


async def list_grouped_assets(
    group_by: str,
    query: AssetQuery,
) -> GroupedAssetsResponse:
    """
    Grouped asset listing: returns group aggregates (row_kind='group').
    """

    view = get_view("default")
    db = get_asset_repo()
    # TODO: metadata_actor_ids support is intentionally skipped for now.
    return await db.list_grouped_assets_db(
        view,
        group_by=group_by,
        query=query,
    )


async def create_asset() -> None:
    """Raise because direct asset creation is not supported."""
    raise NotImplementedError("Direct asset creation is not supported")


async def get_asset(asset_id: int) -> tuple[Asset, Sequence[Metadata]]:
    """Return one asset and all of its metadata rows."""
    db = get_asset_repo()
    asset = await db.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    metadata = await db.load_metadata(asset, include_removed=True)
    return asset, metadata


async def manual_edit_asset(asset_id: int, payload: dict[str, Any]) -> ManualEditResult:
    """Apply manual metadata edits for an asset in an in-progress changeset."""
    changeset_id = payload.get("changeset_id")
    if changeset_id is None:
        raise ApiError(status_code=400, detail="changeset_id is required")

    from katalog.db.changesets import get_changeset_repo
    from katalog.models import OpStatus

    db = get_changeset_repo()
    changeset = await db.get_or_none(id=int(changeset_id))
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")

    db = get_asset_repo()
    asset = await db.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    actor = await ensure_user_editor()

    # Build metadata from payload (dict of key -> value)
    metadata_entries: list[Metadata] = []
    for key, value in payload.get("metadata", {}).items():
        try:
            mk = MetadataKey(key)
            md = make_metadata(mk, value, actor_id=actor.id)
        except Exception as exc:
            raise ApiError(status_code=400, detail=f"Invalid metadata {key}: {exc}")
        md.asset_id = asset.id
        md.changeset_id = changeset.id
        metadata_entries.append(md)

    # Apply changes
    loaded = await db.load_metadata(asset)
    changes = MetadataChanges(asset=asset, loaded=loaded, staged=metadata_entries)
    md_db = get_metadata_repo()
    changed_keys = await md_db.persist_changes(
        changes, changeset=changeset
    )
    logger.bind(changeset_id=changeset.id).info(
        "tasks_progress queued=None running=0 finished={finished} kind=edits",
        finished=len(metadata_entries),
    )

    return ManualEditResult(
        asset_id=asset_id,
        changeset_id=changeset.id,
        changed_keys=[str(k) for k in changed_keys],
    )


async def update_asset() -> None:
    """Raise because generic asset patching is not implemented."""
    raise NotImplementedError()


async def _list_assets_semantic(query: AssetQuery) -> AssetsListResponse:
    """List assets ranked by semantic search hits."""
    hits, _total_hits, _duration_ms = await semantic_hits_for_query(query)

    ordered_asset_ids: list[int] = []
    seen_asset_ids: set[int] = set()
    for hit in hits:
        if hit.asset_id in seen_asset_ids:
            continue
        seen_asset_ids.add(hit.asset_id)
        ordered_asset_ids.append(hit.asset_id)

    top_hit_by_asset: dict[int, dict[str, Any]] = {}
    for hit in hits:
        best = top_hit_by_asset.get(hit.asset_id)
        if best is None or float(hit.score) > float(best["score"]):
            top_hit_by_asset[hit.asset_id] = {
                "score": float(hit.score),
                "cosine_similarity": float(hit.cosine_similarity),
                "text": hit.text,
                "distance": float(hit.distance),
                "metadata_key": hit.metadata_key,
            }

    start = int(query.offset)
    end = start + int(query.limit)
    page_asset_ids = ordered_asset_ids[start:end]

    scoped_query = query.model_copy(
        update={
            "search": None,
            "search_mode": "fts",
            "search_granularity": "asset",
            "offset": 0,
            "limit": max(1, len(page_asset_ids)),
        }
    )
    if page_asset_ids:
        scoped_query.filters = [
            *(scoped_query.filters or []),
            {"key": "asset/id", "op": "in", "values": [str(asset_id) for asset_id in page_asset_ids]},
        ]
    else:
        scoped_query.filters = [
            *(scoped_query.filters or []),
            {"key": "asset/id", "op": "equals", "value": "-1"},
        ]

    view = get_view(scoped_query.view_id or "default")
    db = get_asset_repo()
    response = await db.list_assets_for_view_db(view, query=scoped_query)
    rank = {asset_id: index for index, asset_id in enumerate(page_asset_ids)}
    sorted_items = sorted(response.items, key=lambda item: rank.get(int(item.asset_id), 10**9))
    updated_items: list[Any] = []
    for item in sorted_items:
        best = top_hit_by_asset.get(int(item.asset_id))
        updates: dict[str, Any]
        if best is None:
            updates = {
                "search_score": None,
                "search_cosine_similarity": None,
                "search_match": None,
                "search_distance": None,
                "search_metadata_key": None,
            }
        else:
            updates = {
                "search_score": float(best["score"]),
                "search_cosine_similarity": float(best["cosine_similarity"]),
                "search_match": str(best.get("text") or ""),
                "search_distance": float(best["distance"]),
                "search_metadata_key": str(best.get("metadata_key") or ""),
            }
        updated_items.append(item.model_copy(update=updates))

    response.items = updated_items
    if query.search_include_matches and response.items:
        matches_by_asset: dict[int, list[dict[str, Any]]] = {}
        for hit in hits:
            matches_by_asset.setdefault(hit.asset_id, []).append(
                {
                    "metadata_id": hit.metadata_id,
                    "metadata_key_id": hit.metadata_key_id,
                    "metadata_key": hit.metadata_key,
                    "distance": hit.distance,
                    "score": hit.score,
                    "cosine_similarity": hit.cosine_similarity,
                    "text": hit.text,
                }
            )
        response.items = [
            item.model_copy(
                update={"search_matches": matches_by_asset.get(int(item.asset_id), [])}
            )
            for item in response.items
        ]
    response.stats.total = len(ordered_asset_ids)
    response.stats.returned = len(response.items)
    response.pagination.offset = query.offset
    response.pagination.limit = query.limit
    return response
