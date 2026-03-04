from typing import Optional

from fastapi import APIRouter, Query, Request

from katalog.api.collections import (
    CollectionCreate,
    CollectionRemoveAssets,
    CollectionUpdate,
    create_collection,
    delete_collection,
    get_collection,
    list_collection_assets,
    list_collections,
    remove_collection_assets,
    update_collection,
)
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query

router = APIRouter()


@router.get("/collections")
async def list_collections_rest():
    collections = await list_collections()
    return {"collections": collections}


@router.post("/collections")
async def create_collection_rest(request: Request):
    payload = CollectionCreate.model_validate(await request.json())
    collection = await create_collection(payload)
    return {"collection": collection}


@router.get("/collections/{collection_id}")
async def get_collection_rest(collection_id: int):
    collection = await get_collection(collection_id)
    return {"collection": collection}


@router.patch("/collections/{collection_id}")
async def update_collection_rest(collection_id: int, request: Request):
    payload = CollectionUpdate.model_validate(await request.json())
    collection = await update_collection(collection_id, payload)
    return {"collection": collection}


@router.get("/collections/{collection_id}/assets")
async def list_collection_assets_rest(
    collection_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    view_id: str = Query("default"),
    sort: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
):
    try:
        query = build_asset_query(
            view_id=view_id,
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
    return await list_collection_assets(
        collection_id=collection_id,
        query=query,
    )


@router.delete("/collections/{collection_id}")
async def delete_collection_rest(collection_id: int):
    return await delete_collection(collection_id)


@router.post("/collections/{collection_id}/remove")
async def remove_collection_assets_rest(collection_id: int, request: Request):
    payload = CollectionRemoveAssets.model_validate(await request.json())
    return await remove_collection_assets(collection_id, payload)
