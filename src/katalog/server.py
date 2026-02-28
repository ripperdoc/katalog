import logging
import os
import sys
from functools import partial
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from loguru import logger

from katalog.api import (
    actors,
    assets,
    changesets,
    collections,
    metadata,
    operations,
    plugins,
    system,
    views,
    workflows,
)
from katalog.api.helpers import ApiError
from katalog.lifespan import app_lifespan

logging.getLogger("uvicorn.access").disabled = True
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<8}</level> | {message}",
)

def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.lower() in {"1", "true", "yes", "on"}


app = FastAPI(
    lifespan=partial(app_lifespan, init_mode="full", log_discovered_plugins=True)
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    started_at = perf_counter()
    response = await call_next(request)
    duration_ms = int((perf_counter() - started_at) * 1000)
    client = request.client.host if request.client else "-"
    http_version = request.scope.get("http_version", "1.1")
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    logger.info(
        '"{method} {path} HTTP/{http_version}" {status} {duration_ms}ms',
        client=client,
        method=request.method,
        path=path,
        http_version=http_version,
        status=response.status_code,
        duration_ms=duration_ms,
    )
    return response


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    _ = request
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=exc.headers,
    )


app.include_router(assets.router)
app.include_router(views.router)
app.include_router(collections.router)
app.include_router(operations.router)
app.include_router(changesets.router)
app.include_router(actors.router)
app.include_router(plugins.router)
app.include_router(metadata.router)
app.include_router(system.router)
app.include_router(workflows.router)

if _env_flag("KATALOG_ENABLE_MCP"):
    from katalog.mcp import create_mcp_http_app

    app.mount("/mcp", create_mcp_http_app(path="/"))
    logger.info("MCP endpoint enabled at /mcp")
