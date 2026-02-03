from typing import Optional

from fastapi import APIRouter, Query

from katalog.db.assets import get_asset_repo
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
    actor_id: Optional[int],
    offset: int,
    limit: int,
    sort: Optional[tuple[str, str]],
    columns: list[str] | None,
    search: Optional[str],
    filters: list[str] | None,
) -> AssetsListResponse:
    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")

    try:
        db = get_asset_repo()
        return await db.list_assets_for_view_db(
            view,
            actor_id=actor_id,
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
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
    actor_id: Optional[int] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    columns: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
):
    sort_tuple: tuple[str, str] | None = None
    if sort:
        if ":" in sort:
            col, direction = sort.split(":", 1)
        else:
            col, direction = sort, "asc"
        sort_tuple = (col, direction)

    return await list_assets_for_view(
        view_id=view_id,
        actor_id=actor_id,
        offset=offset,
        limit=limit,
        sort=sort_tuple,
        columns=columns,
        search=search,
        filters=filters,
    )
