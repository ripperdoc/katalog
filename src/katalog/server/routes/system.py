from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from katalog.api.system import auth_callback_api, sync_config, workspace_size_stats

router = APIRouter()


async def _auth_callback(actor: int, request: Request):
    await auth_callback_api(actor, str(request.url))
    return RedirectResponse(url="/", status_code=303)


@router.get("/auth/{actor}")
async def auth_callback_get(actor: int, request: Request):
    return await _auth_callback(actor, request)


@router.post("/auth/{actor}")
async def auth_callback_post(actor: int, request: Request):
    return await _auth_callback(actor, request)


@router.post("/sync")
async def sync_config_rest():
    return await sync_config()


@router.get("/stats")
async def workspace_size_stats_rest():
    return await workspace_size_stats()
