import logging
import sys
from time import perf_counter

from contextlib import asynccontextmanager

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
from katalog.config import DB_URL, WORKSPACE
from katalog.lifespan import app_lifespan

logging.getLogger("uvicorn.access").disabled = True
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<8}</level> | {message}",
)

if WORKSPACE is None or DB_URL is None:
    raise RuntimeError("KATALOG_WORKSPACE must be set when running the server")
logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DB_URL}")


@asynccontextmanager
async def lifespan(app):
    _ = app
    async with app_lifespan(init_mode="full", log_discovered_plugins=True):
        yield


app = FastAPI(lifespan=lifespan)


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
