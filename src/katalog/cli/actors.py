import json
import pathlib
from typing import Any

import typer

from . import _reset_workspace, actors_app
from .utils import changeset_summary, print_changeset_summary, render_table, run_cli, wants_json

@actors_app.command("list")
def list_actors(ctx: typer.Context) -> None:
    """List all actors in the workspace."""

    async def _run() -> list[Any]:
        from katalog.api.actors import list_actors as list_actors_api

        return await list_actors_api()

    actors = run_cli(_run, init_mode="fast")
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

    actor, changesets = run_cli(_run, init_mode="fast")
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
    workflow_file: str | None = typer.Option(
        None,
        "--workflow",
        help="Sync actors from this workflow TOML before scanning",
    ),
    skip_processors: bool = typer.Option(
        False,
        "--skip-processors",
        help="Skip running processors as part of the scan",
    ),
    benchmark: bool = typer.Option(
        False,
        "--benchmark",
        help="Benchmark mode: report max RSS and delete the changeset after the run",
    ),
) -> None:
    """Run a source scan for the given actor id without starting the server."""
    ws = ctx.obj["workspace"]

    if reset_workspace:
        _reset_workspace(ws)

    async def _run() -> dict[str, Any]:
        import resource

        from katalog.api.operations import run_source
        from katalog.workflows import sync_workflow_file

        if workflow_file:
            await sync_workflow_file(pathlib.Path(workflow_file))

        changeset = await run_source(
            actor_id,
            finalize=True,
            run_processors=not skip_processors,
        )

        max_rss_mb = None
        if benchmark:
            max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            max_rss_mb = max_rss / (1024 * 1024)
            from katalog.api.changesets import delete_changeset as delete_changeset_api

            await delete_changeset_api(int(changeset.id))

        return {
            "changeset": changeset_summary(changeset),
            "max_rss_mb": max_rss_mb,
            "deleted": benchmark,
        }

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return

    print_changeset_summary(result["changeset"])
    if result.get("max_rss_mb") is not None:
        typer.echo(f"Max RSS: {result['max_rss_mb']:.2f} MB")
    if result.get("deleted"):
        typer.echo("Deleted: yes")
