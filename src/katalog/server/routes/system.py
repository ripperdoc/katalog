from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from katalog.api.system import auth_api, sync_config, workspace_size_stats

router = APIRouter()


def _is_oauth_callback(request: Request) -> bool:
    params = request.query_params
    return "code" in params or "error" in params


async def _auth(actor: int, request: Request):
    if _is_oauth_callback(request):
        await auth_api(actor, authorization_response=str(request.url))
        return RedirectResponse(url="/", status_code=303)

    result = await auth_api(actor)
    authorization_url = result.get("authorization_url")
    if authorization_url:
        return RedirectResponse(url=authorization_url, status_code=303)
    return RedirectResponse(url="/", status_code=303)


@router.get("/auth/{actor}")
async def auth_callback_get(actor: int, request: Request):
    return await _auth(actor, request)


@router.post("/auth/{actor}")
async def auth_callback_post(actor: int, request: Request):
    return await _auth(actor, request)


@router.post("/sync")
async def sync_config_rest():
    return await sync_config()


@router.get("/stats")
async def workspace_size_stats_rest():
    return await workspace_size_stats()
