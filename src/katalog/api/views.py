from typing import Optional

from fastapi import APIRouter, Query

from katalog.db.assets import get_asset_repo
from katalog.api.query_utils import build_asset_query
from katalog.models.query import AssetQuery
from katalog.models.views import ViewSpec, get_view, list_views
from katalog.api.helpers import ApiError
from katalog.api.schemas import AssetsListResponse

router = APIRouter()


async def get_view_api(view_id: str) -> ViewSpec:
    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")
    return view


async def list_assets_for_view(
    view_id: str,
    query: AssetQuery,
) -> AssetsListResponse:
    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")

    try:
        # TODO: metadata_actor_ids support is intentionally skipped for now.
        db = get_asset_repo()
        return await db.list_assets_for_view_db(
            view,
            query=query,
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


@router.get("/views")
async def list_views_rest():
    views = list_views()
    return {"views": views}


@router.get("/views/{view_id}")
async def get_view_rest(view_id: str):
    view = await get_view_api(view_id)
    return {"view": view}


@router.get("/views/{view_id}/assets")
async def list_assets_for_view_rest(
    view_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
    metadata_include_linked_sidecars: bool = Query(False),
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
            metadata_include_linked_sidecars=metadata_include_linked_sidecars,
        )
    except Exception as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    return await list_assets_for_view(view_id=view_id, query=query)
