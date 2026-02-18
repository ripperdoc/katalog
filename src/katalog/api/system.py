from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from katalog.api.helpers import ApiError
from katalog.db.metadata import sync_config_db
from katalog.plugins.registry import get_actor_instance
from katalog.sources.base import SourcePlugin

router = APIRouter()


async def auth_callback_api(actor: int, authorization_response: str) -> dict[str, str]:
    plugin = await get_actor_instance(actor)
    if not isinstance(plugin, SourcePlugin):
        raise ApiError(status_code=400, detail="Actor is not a source")
    plugin.authorize(authorization_response=authorization_response)
    return {"status": "ok"}


async def sync_config() -> dict[str, str]:
    """Requests to sync config"""
    await sync_config_db()

    return {"status": "ok"}


@router.post("/auth/{actor}")
async def auth_callback(actor: int, request: Request):
    await auth_callback_api(actor, str(request.url))
    return RedirectResponse(url="/", status_code=303)


@router.post("/sync")
async def sync_config_rest():
    return await sync_config()
