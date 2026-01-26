from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from katalog.db import list_assets_for_view
from katalog.models.views import get_view, list_views

router = APIRouter()


@router.get("/views")
async def list_views_endpoint():
    return {"views": [v.to_dict() for v in list_views()]}


@router.get("/views/{view_id}")
async def get_view_endpoint(view_id: str):
    try:
        view = get_view(view_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="View not found")
    return {"view": view.to_dict()}


@router.get("/views/{view_id}/assets")
async def list_assets_for_view_endpoint(
    view_id: str,
    actor_id: Optional[int] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    columns: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
):
    try:
        view = get_view(view_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="View not found")

    sort_tuple: tuple[str, str] | None = None
    if sort:
        if ":" in sort:
            col, direction = sort.split(":", 1)
        else:
            col, direction = sort, "asc"
        sort_tuple = (col, direction)

    try:
        return await list_assets_for_view(
            view,
            actor_id=actor_id,
            offset=offset,
            limit=limit,
            sort=sort_tuple,
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
