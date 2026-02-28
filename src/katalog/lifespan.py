from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Literal

from loguru import logger
from katalog.config import (
    build_app_context,
    current_db_url,
    current_workspace,
    use_app_context,
)

InitMode = Literal["full", "fast"]


@asynccontextmanager
async def app_lifespan(
    app: Any = None,
    *,
    init_mode: InitMode = "full",
    log_discovered_plugins: bool = False,
    workspace: str | Path | None = None,
    db_url: str | None = None,
) -> AsyncIterator[None]:
    _ = app
    from katalog.constants.metadata import clear_metadata_registry_cache
    from katalog.db.sqlspec import close_db
    from katalog.plugins.registry import close_instance_cache
    from katalog.utils.blob_cache import close_blob_caches

    app_context = build_app_context(workspace=workspace, db_url=db_url)
    with use_app_context(app_context):
        logger.info("Using workspace: {}", current_workspace())
        logger.info("Using database: {}", current_db_url())

        event_manager = app_context.event_manager
        running_changesets = app_context.running_changesets

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
            from katalog.config import current_db_path
            from katalog.db.sqlspec.query_metadata_registry import (
                setup_db,
                sync_metadata_registry,
            )

            await setup_db(current_db_path())
            # Fast mode avoids plugin discovery and user-editor setup.
            # We still need registry ids loaded for metadata-key lookups.
            await sync_metadata_registry()

        event_manager.bind_loop(asyncio.get_running_loop())
        event_manager.ensure_sink()
        try:
            yield
        finally:
            for snap in list(running_changesets.values()):
                snap.cancel()
            for snap in list(running_changesets.values()):
                await snap.wait_cancelled(timeout=5)
            running_changesets.clear()
            event_manager.close()
            await close_instance_cache()
            close_blob_caches()
            clear_metadata_registry_cache()
            await close_db()
