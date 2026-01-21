from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from katalog.db import sync_config
from katalog.sources.runtime import get_source_plugin

router = APIRouter()


@router.post("/auth/{actor}")
async def auth_callback(actor: int, request: Request):
    get_source_plugin(actor).authorize(authorization_response=request.url)
    return RedirectResponse(url="/", status_code=303)


@router.post("/sync")
async def sync():
    """Requests to sync config"""
    await sync_config()

    return {"status": "ok"}
