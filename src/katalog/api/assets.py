from typing import Any, Optional, Sequence

from fastapi import APIRouter, Query, Request

from katalog.api.search import semantic_hits_for_query
from katalog.constants.metadata import MetadataKey
from katalog.db.assets import get_asset_repo
from katalog.editors.user_editor import ensure_user_editor
from katalog.models import Asset, Metadata, MetadataChanges, make_metadata
from katalog.models.query import AssetQuery
from katalog.models.views import get_view
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query
from katalog.api.schemas import (
    AssetsListResponse,
    GroupedAssetsResponse,
    ManualEditResult,
)
from katalog.db.metadata import get_metadata_repo
from loguru import logger


router = APIRouter()


async def list_assets(query: AssetQuery) -> AssetsListResponse:
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
    raise NotImplementedError("Direct asset creation is not supported")


async def get_asset(asset_id: int) -> tuple[Asset, Sequence[Metadata]]:
    db = get_asset_repo()
    asset = await db.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    metadata = await db.load_metadata(asset, include_removed=True)
    return asset, metadata


async def manual_edit_asset(asset_id: int, payload: dict[str, Any]) -> ManualEditResult:
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
    raise NotImplementedError()


async def _list_assets_semantic(query: AssetQuery) -> AssetsListResponse:
    hits, _total_hits, _duration_ms = await semantic_hits_for_query(query)

    ordered_asset_ids: list[int] = []
    seen_asset_ids: set[int] = set()
    for hit in hits:
        if hit.asset_id in seen_asset_ids:
            continue
        seen_asset_ids.add(hit.asset_id)
        ordered_asset_ids.append(hit.asset_id)

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
    response.items.sort(key=lambda item: rank.get(int(item.asset_id), 10**9))
    if query.search_include_matches:
        matches_by_asset: dict[int, list[dict[str, Any]]] = {}
        for hit in hits:
            matches_by_asset.setdefault(hit.asset_id, []).append(
                {
                    "metadata_id": hit.metadata_id,
                    "metadata_key_id": hit.metadata_key_id,
                    "metadata_key": hit.metadata_key,
                    "distance": hit.distance,
                    "score": hit.score,
                    "text": hit.text,
                }
            )
        for item in response.items:
            item.__dict__["search_matches"] = matches_by_asset.get(int(item.asset_id), [])
    response.stats.total = len(ordered_asset_ids)
    response.stats.returned = len(response.items)
    response.pagination.offset = query.offset
    response.pagination.limit = query.limit
    return response


@router.get("/assets")
async def list_assets_rest(
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: list[str] | None = Query(None),
    filters: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
):
    try:
        query = build_asset_query(
            view_id="default",
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            search=search,
            metadata_actor_ids=metadata_actor_ids,
            metadata_include_removed=metadata_include_removed,
            metadata_aggregation=metadata_aggregation,
            metadata_include_counts=metadata_include_counts,
        )
    except Exception as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    return await list_assets(query=query)


@router.get("/assets/grouped")
async def list_grouped_assets_rest(
    group_by: str = Query(
        ..., description="Grouping key, e.g. 'hash/md5' or 'asset/actor_id'"
    ),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    sort: list[str] | None = Query(None),
    filters: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
):
    try:
        query = build_asset_query(
            view_id="default",
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            search=search,
            group_by=group_by,
            metadata_actor_ids=metadata_actor_ids,
            metadata_include_removed=metadata_include_removed,
            metadata_aggregation=metadata_aggregation,
            metadata_include_counts=metadata_include_counts,
        )
    except Exception as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    return await list_grouped_assets(group_by=group_by, query=query)


@router.post("/assets")
async def create_asset_rest(request: Request):
    return await create_asset()


@router.get("/assets/{asset_id}")
async def get_asset_rest(asset_id: int):
    asset, metadata = await get_asset(asset_id)
    return {"asset": asset, "metadata": metadata}


@router.post("/assets/{asset_id}/manual-edit")
async def manual_edit_asset_rest(asset_id: int, request: Request):
    payload = await request.json()
    return await manual_edit_asset(asset_id, payload)


@router.patch("/assets/{asset_id}")
async def update_asset_rest(asset_id: int):
    return await update_asset()
