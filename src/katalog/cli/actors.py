import json
from typing import Any

import typer

from . import _bootstrap_actors_from_toml, _reset_workspace, actors_app
from .utils import render_table, run_cli, wants_json


@actors_app.command("list")
def list_actors(ctx: typer.Context) -> None:
    """List all actors in the workspace."""

    async def _run() -> list[Any]:
        from katalog.api.actors import list_actors as list_actors_api

        return await list_actors_api()

    actors = run_cli(_run)
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {"actors": [actor.model_dump() for actor in actors]},
                default=str,
            )
        )
        return

    if not actors:
        typer.echo("No actors found")
        return

    rows = [
        {
            "id": str(actor.id or "-"),
            "name": actor.name,
            "type": actor.type.name if hasattr(actor.type, "name") else str(actor.type),
            "plugin_id": actor.plugin_id or "-",
            "disabled": "yes" if actor.disabled else "no",
        }
        for actor in actors
    ]
    headers = ["ID", "Name", "Type", "Plugin", "Disabled"]
    keys = ["id", "name", "type", "plugin_id", "disabled"]
    render_table(rows, headers, keys)


@actors_app.command("show")
def show_actor(actor_id: int, ctx: typer.Context) -> None:
    """Show details for a single actor."""

    async def _run() -> tuple[Any, list[Any]]:
        from katalog.api.actors import get_actor as get_actor_api

        return await get_actor_api(actor_id)

    actor, changesets = run_cli(_run)
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {
                    "actor": actor.model_dump(),
                    "changesets": [c.model_dump() for c in changesets],
                },
                default=str,
            )
        )
        return

    typer.echo(f"ID: {actor.id}")
    typer.echo(f"Name: {actor.name}")
    typer.echo(f"Type: {actor.type.name}")
    typer.echo(f"Plugin: {actor.plugin_id}")
    typer.echo(f"Disabled: {'yes' if actor.disabled else 'no'}")
    typer.echo(f"Changesets: {len(changesets)}")


@actors_app.command("run")
def run_actor(
    actor_id: int,
    ctx: typer.Context,
    reset_workspace: bool = typer.Option(
        False,
        "--reset-workspace",
        help="Delete katalog.db and actors cache before scanning",
    ),
    bootstrap_actors: bool = typer.Option(
        False,
        "--bootstrap-actors",
        help="Bootstrap actors from workspace katalog.toml before scanning",
    ),
    trace_memory: bool = typer.Option(
        False,
        "--trace-memory",
        help="Report max RSS after the scan (platform-dependent units)",
    ),
) -> None:
    """Run a source scan for the given actor id without starting the server."""
    ws = ctx.obj["workspace"]

    if reset_workspace:
        _reset_workspace(ws)

    async def _run() -> dict[str, Any]:
        import resource

        from katalog.api.operations import run_source

        if bootstrap_actors:
            await _bootstrap_actors_from_toml(ws)

        changeset = await run_source(actor_id, finalize=True)

        max_rss = None
        if trace_memory:
            max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        return {
            "changeset_id": changeset.id,
            "status": changeset.status.value
            if hasattr(changeset.status, "value")
            else str(changeset.status),
            "started_at": changeset.started_at_iso(),
            "elapsed_seconds": (
                changeset.running_time_ms / 1000.0
                if changeset.running_time_ms is not None
                else None
            ),
            "max_rss": max_rss,
            "scan_metrics": (changeset.data or {}).get("scan_metrics"),
        }

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return

    typer.echo(f"Changeset: {result['changeset_id']}")
    if result.get("started_at"):
        typer.echo(f"Started: {result['started_at']}")
    typer.echo(f"Status: {result['status']}")
    if result.get("elapsed_seconds") is not None:
        typer.echo(f"Elapsed: {result['elapsed_seconds']:.2f}s")
    if result.get("max_rss") is not None:
        typer.echo(f"Max RSS: {result['max_rss']} (platform-dependent units)")
    scan_metrics = result.get("scan_metrics")
    if scan_metrics:
        typer.echo(f"Scan time: {scan_metrics.get('scan_seconds'):.2f}s")
        typer.echo(f"Persist time: {scan_metrics.get('persist_seconds'):.2f}s")
        if scan_metrics.get("persist_first_delay_seconds") is not None:
            typer.echo(
                "Persist first delay: "
                f"{scan_metrics.get('persist_first_delay_seconds'):.2f}s"
            )
        if scan_metrics.get("persist_after_scan_seconds") is not None:
            typer.echo(
                "Persist after scan: "
                f"{scan_metrics.get('persist_after_scan_seconds'):.2f}s"
            )
