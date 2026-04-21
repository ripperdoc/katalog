import logging
import os
import sys
from functools import partial
from pathlib import Path
from time import perf_counter

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

from katalog.api.helpers import ApiError
from katalog.lifespan import app_lifespan
from katalog.server.routes import (
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


logging.getLogger("uvicorn.access").disabled = True
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level:<8}</level> | {message}",
)


def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.lower() in {"1", "true", "yes", "on"}


API_PREFIX = "/api"


def _resolve_ui_dist() -> Path | None:
    """Resolve built UI assets from package data or local repo checkout."""
    configured = os.environ.get("KATALOG_UI_DIST")
    if configured:
        candidate = Path(configured).expanduser().resolve()
        if candidate.exists():
            return candidate
    package_candidate = Path(__file__).resolve().parent / "ui_dist"
    if package_candidate.exists():
        return package_candidate
    repo_candidate = Path(__file__).resolve().parents[3] / "ui" / "dist"
    if repo_candidate.exists():
        return repo_candidate
    return None


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        is_frontend_route = not (
            path.startswith("api/")
            or path == "api"
            or path.startswith("docs")
            or path.startswith("openapi")
            or path.startswith("redoc")
            or path.startswith("mcp/")
            or path == "mcp"
        )
        if (
            response.status_code == 404
            and scope.get("method") == "GET"
            and is_frontend_route
        ):
            return await super().get_response("index.html", scope)
        return response


app = FastAPI(
    lifespan=partial(app_lifespan, runtime_mode="read_write", log_discovered_plugins=True)
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


app.include_router(assets.router, prefix=API_PREFIX)
app.include_router(views.router, prefix=API_PREFIX)
app.include_router(collections.router, prefix=API_PREFIX)
app.include_router(operations.router, prefix=API_PREFIX)
app.include_router(changesets.router, prefix=API_PREFIX)
app.include_router(actors.router, prefix=API_PREFIX)
app.include_router(plugins.router, prefix=API_PREFIX)
app.include_router(metadata.router, prefix=API_PREFIX)
app.include_router(system.router, prefix=API_PREFIX)
app.include_router(workflows.router, prefix=API_PREFIX)

if _env_flag("KATALOG_ENABLE_MCP"):
    from katalog.mcp import create_mcp_http_app

    app.mount("/mcp", create_mcp_http_app(path="/"))
    logger.info("MCP endpoint enabled at /mcp")

ui_dist = _resolve_ui_dist()
if ui_dist is not None:
    app.mount("/", SPAStaticFiles(directory=ui_dist, html=True), name="ui")
else:

    @app.get("/", include_in_schema=False)
    async def root_not_configured():
        return PlainTextResponse(
            "UI build not found. Open /docs for API docs or configure KATALOG_UI_DIST.",
            status_code=404,
        )
