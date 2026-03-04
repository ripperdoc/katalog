from typing import Literal, Optional

from fastapi import APIRouter, Query, Request

from katalog.api.assets import (
    create_asset,
    get_asset,
    list_assets,
    list_grouped_assets,
    manual_edit_asset,
    update_asset,
)
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query

router = APIRouter()


@router.get("/assets")
async def list_assets_rest(
    view_id: Optional[str] = Query("default"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: list[str] | None = Query(None),
    filters: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    search_mode: Literal["fts", "semantic", "hybrid"] | None = Query(None),
    search_index: int | None = Query(None),
    search_top_k: int | None = Query(None, ge=1),
    search_metadata_keys: list[str] | None = Query(None),
    search_min_score: float | None = Query(None),
    search_include_matches: bool = Query(False),
    search_dimension: int | None = Query(None, ge=1),
    search_embedding_model: str | None = Query(None),
    search_embedding_backend: Literal["preset", "fastembed"] | None = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
    metadata_include_linked_sidecars: bool = Query(False),
    columns: list[str] | None = Query(None),
    include_lost_assets: bool = Query(False),
):
    try:
        query = build_asset_query(
            view_id=view_id,
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            search=search,
            search_mode=search_mode,
            search_index=search_index,
            search_top_k=search_top_k,
            search_metadata_keys=search_metadata_keys,
            search_min_score=search_min_score,
            search_include_matches=search_include_matches,
            search_dimension=search_dimension,
            search_embedding_model=search_embedding_model,
            search_embedding_backend=search_embedding_backend,
            metadata_actor_ids=metadata_actor_ids,
            metadata_include_removed=metadata_include_removed,
            metadata_aggregation=metadata_aggregation,
            metadata_include_counts=metadata_include_counts,
            metadata_include_linked_sidecars=metadata_include_linked_sidecars,
            columns=columns,
            include_lost_assets=include_lost_assets,
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
    include_lost_assets: bool = Query(False),
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
            include_lost_assets=include_lost_assets,
        )
    except Exception as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    return await list_grouped_assets(group_by=group_by, query=query)


@router.post("/assets")
async def create_asset_rest(request: Request):
    _ = request
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
    _ = asset_id
    return await update_asset()
