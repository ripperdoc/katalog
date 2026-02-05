import asyncio
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Sequence, TypeVar

import typer
from loguru import logger

T = TypeVar("T")


@asynccontextmanager
async def cli_lifespan() -> Any:
    from katalog.api.state import RUNNING_CHANGESETS, event_manager
    from katalog.db.metadata import sync_config_db
    from katalog.db.sqlspec import close_db
    from katalog.plugins.registry import refresh_plugins

    plugins_found = refresh_plugins()
    if not plugins_found:
        logger.warning("No plugins discovered via entry points")
    await sync_config_db()
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


def run_cli(task: Callable[[], Awaitable[T]]) -> T:
    async def _run() -> T:
        async with cli_lifespan():
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
