import asyncio
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Literal, Mapping, Sequence, TypeVar

import typer
from loguru import logger

T = TypeVar("T")
InitMode = Literal["full", "fast"]


@asynccontextmanager
async def cli_lifespan(*, init_mode: InitMode = "full") -> Any:
    from katalog.api.state import RUNNING_CHANGESETS, event_manager
    from katalog.db.sqlspec import close_db
    from katalog.processors.executor_pool import processor_executor_scope
    # CLI commands are one-shot: keep executors inside this lifespan so
    # process-pool workers are shut down deterministically on exit.
    async with processor_executor_scope():
        if init_mode == "full":
            from katalog.db.metadata import sync_config_db
            from katalog.plugins.registry import refresh_plugins

            plugins_found = refresh_plugins()
            if not plugins_found:
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


def run_cli(task: Callable[[], Awaitable[T]], *, init_mode: InitMode = "full") -> T:
    async def _run() -> T:
        async with cli_lifespan(init_mode=init_mode):
            return await task()

    return asyncio.run(_run())


def wants_json(ctx: typer.Context) -> bool:
    return bool(ctx.obj and ctx.obj.get("json"))


def render_table(rows: Sequence[dict], headers: Sequence[str], keys: Sequence[str]) -> None:
    widths = [
        max(len(headers[i]), max(len(row[keys[i]]) for row in rows))
        for i in range(len(headers))
    ]
    header_line = "  ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    typer.echo(header_line)
    typer.echo("  ".join("-" * width for width in widths))
    for row in rows:
        typer.echo(
            "  ".join(row[keys[i]].ljust(widths[i]) for i in range(len(headers)))
        )


def changeset_summary(changeset: Any) -> dict[str, Any]:
    status = changeset.status.value if hasattr(changeset.status, "value") else str(changeset.status)
    return {
        "id": changeset.id,
        "status": status,
        "started_at": changeset.started_at_iso() if hasattr(changeset, "started_at_iso") else None,
        "elapsed_seconds": (
            changeset.running_time_ms / 1000.0
            if getattr(changeset, "running_time_ms", None) is not None
            else None
        ),
        "scan_metrics": ((changeset.data or {}).get("scan_metrics") if getattr(changeset, "data", None) else None),
        "message": getattr(changeset, "message", None),
    }


def print_changeset_summary(
    summary: Mapping[str, Any],
    *,
    label: str = "Changeset",
) -> None:
    typer.echo(f"{label}: {summary['id']}")
    if summary.get("started_at"):
        typer.echo(f"Started: {summary['started_at']}")
    typer.echo(f"Status: {summary['status']}")
    if summary.get("elapsed_seconds") is not None:
        typer.echo(f"Elapsed: {summary['elapsed_seconds']:.2f}s")
    scan_metrics = summary.get("scan_metrics")
    if scan_metrics:
        scan_seconds = scan_metrics.get("scan_seconds")
        if scan_seconds is not None:
            typer.echo(f"Scan time: {scan_seconds:.2f}s")
        for key, title in [
            ("assets_seen", "Assets seen"),
            ("assets_saved", "Assets saved"),
            ("assets_added", "Assets added"),
            ("assets_changed", "Assets changed"),
            ("assets_ignored", "Assets ignored"),
            ("assets_lost", "Assets lost"),
        ]:
            value = scan_metrics.get(key)
            if value is not None:
                typer.echo(f"{title}: {value}")
