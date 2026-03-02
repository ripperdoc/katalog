from fastapi import APIRouter

from katalog.models.views import ViewSpec, get_view, list_views
from katalog.api.helpers import ApiError

router = APIRouter()


async def get_view_api(view_id: str) -> ViewSpec:
    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")
    return view


@router.get("/views")
async def list_views_rest():
    views = list_views()
    return {"views": views}


@router.get("/views/{view_id}")
async def get_view_rest(view_id: str):
    view = await get_view_api(view_id)
    return {"view": view}
