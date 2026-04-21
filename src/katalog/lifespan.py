from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Literal

from loguru import logger
from katalog.config import (
    RuntimeMode,
    build_app_context,
    current_db_url,
    current_workspace,
    use_app_context,
)

InitMode = Literal["full", "fast"]


def _runtime_mode_from_legacy(init_mode: InitMode | None) -> RuntimeMode | None:
    if init_mode is None:
        return None
    if init_mode == "fast":
        return "fast_read"
    return "read_write"


def _resolve_runtime_mode(
    *,
    requested_mode: RuntimeMode,
    read_only_effective: bool,
) -> RuntimeMode:
    if requested_mode == "fast_read":
        return "fast_read"
    if requested_mode == "read_only":
        return "read_only"
    if read_only_effective:
        return "read_only"
    return "read_write"


@asynccontextmanager
async def app_lifespan(
    app: Any = None,
    *,
    runtime_mode: RuntimeMode = "read_write",
    init_mode: InitMode | None = None,
    read_only_requested: bool | None = None,
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
    legacy_runtime_mode = _runtime_mode_from_legacy(init_mode)
    requested_runtime_mode = legacy_runtime_mode or runtime_mode
    if read_only_requested is not None:
        app_context.read_only_requested = bool(read_only_requested)
    if requested_runtime_mode == "read_only":
        app_context.read_only_requested = True
    app_context.read_only_effective = (
        app_context.read_only_requested or app_context.install_profile == "readonly"
    )
    app_context.runtime_mode = _resolve_runtime_mode(
        requested_mode=requested_runtime_mode,
        read_only_effective=app_context.read_only_effective,
    )
    if app_context.runtime_mode == "read_only":
        app_context.read_only_effective = True

    with use_app_context(app_context):
        logger.info("Using workspace: {}", current_workspace())
        logger.info("Using database: {}", current_db_url())
        logger.info(
            "Runtime mode: {} (read_only_requested={}, install_profile={}, read_only_effective={})",
            app_context.runtime_mode,
            app_context.read_only_requested,
            app_context.install_profile,
            app_context.read_only_effective,
        )

        event_manager = app_context.event_manager
        running_changesets = app_context.running_changesets

        if app_context.runtime_mode == "read_write":
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
        elif app_context.runtime_mode == "fast_read" and not app_context.read_only_effective:
            from katalog.config import current_db_path
            from katalog.db.sqlspec.query_metadata_registry import (
                setup_db,
                sync_metadata_registry,
            )

            await setup_db(current_db_path())
            # Fast mode avoids plugin discovery and user-editor setup.
            # We still need registry ids loaded for metadata-key lookups.
            await sync_metadata_registry()
        else:
            from katalog.db.sqlspec.query_metadata_registry import (
                load_metadata_registry_cache,
            )

            try:
                await load_metadata_registry_cache()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    "Read-only startup requires an initialized workspace with metadata registry."
                ) from exc

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
