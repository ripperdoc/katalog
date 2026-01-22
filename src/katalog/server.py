import asyncio

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger
from tortoise import Tortoise

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
)
from katalog.api.state import RUNNING_CHANGESETS, event_manager
from katalog.config import DB_URL, WORKSPACE
from katalog.db import sync_config
from katalog.plugins.registry import refresh_plugins

logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DB_URL}")


@asynccontextmanager
async def lifespan(app):
    # run startup logic
    plugins_found = refresh_plugins()
    if plugins_found:
        logger.info(
            "Discovered plugins ({}): {}",
            len(plugins_found),
            ", ".join(sorted(plugins_found.keys())),
        )
    else:
        logger.warning("No plugins discovered via entry points")

    await sync_config()
    event_manager.bind_loop(asyncio.get_running_loop())
    event_manager.ensure_sink()
    try:
        yield
    finally:
        # Best-effort cancel running changeset tasks on shutdown to avoid reload hangs.
        for snap in list(RUNNING_CHANGESETS.values()):
            snap.cancel()
        for snap in list(RUNNING_CHANGESETS.values()):
            await snap.wait_cancelled(timeout=5)
        # run shutdown logic
        await Tortoise.close_connections()


app = FastAPI(lifespan=lifespan)

app.include_router(assets.router)
app.include_router(views.router)
app.include_router(collections.router)
app.include_router(operations.router)
app.include_router(changesets.router)
app.include_router(actors.router)
app.include_router(plugins.router)
app.include_router(metadata.router)
app.include_router(system.router)
