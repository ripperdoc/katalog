import json
from typing import Any

import typer

from . import actors_app
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
