from fastapi import APIRouter

from katalog.api.views import get_view_api, list_views_api

router = APIRouter()


@router.get("/views")
async def list_views_rest():
    views = await list_views_api()
    return {"views": views}


@router.get("/views/{view_id}")
async def get_view_rest(view_id: str):
    view = await get_view_api(view_id)
    return {"view": view}
