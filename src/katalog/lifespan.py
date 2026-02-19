from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal

from loguru import logger

InitMode = Literal["full", "fast"]


@asynccontextmanager
async def app_lifespan(
    *,
    init_mode: InitMode = "full",
    log_discovered_plugins: bool = False,
) -> AsyncIterator[None]:
    from katalog.api.state import RUNNING_CHANGESETS, event_manager
    from katalog.db.sqlspec import close_db

    if init_mode == "full":
        from katalog.db.metadata import sync_config_db
        from katalog.plugins.registry import refresh_plugins

        plugins_found = refresh_plugins()
        if plugins_found:
            if log_discovered_plugins:
                logger.info(
                    "Discovered plugins ({}): {}",
                    len(plugins_found),
                    ", ".join(sorted(plugins_found.keys())),
                )
        else:
            logger.warning("No plugins discovered via entry points")
        await sync_config_db()
    else:
        from katalog.config import DB_PATH
        from katalog.db.sqlspec.query_metadata_registry import (
            setup_db,
            sync_metadata_registry,
        )

        await setup_db(DB_PATH)
        # Fast mode avoids plugin discovery and user-editor setup.
        # We still need registry ids loaded for metadata-key lookups.
        await sync_metadata_registry()

    event_manager.bind_loop(asyncio.get_running_loop())
    event_manager.ensure_sink()
    try:
        yield
    finally:
        for snap in list(RUNNING_CHANGESETS.values()):
            snap.cancel()
        for snap in list(RUNNING_CHANGESETS.values()):
            await snap.wait_cancelled(timeout=5)
        await close_db()
