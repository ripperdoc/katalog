import json
import pathlib
from typing import Any

import asyncclick as click

from . import _reset_workspace, actors_app
from .utils import changeset_summary, print_changeset_summary, render_table, wants_json, with_lifespan


@actors_app.command("list")
@with_lifespan(runtime_mode="fast_read")
async def list_actors(ctx: click.Context) -> None:
    """List all actors in the workspace."""
    from katalog.api.actors import list_actors as list_actors_api

    actors = await list_actors_api()
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {"actors": [actor.model_dump() for actor in actors]},
                default=str,
            )
        )
        return

    if not actors:
        click.echo("No actors found")
        return

    rows = [
        {
            "id": str(actor.id or "-"),
            "name": actor.name,
            "type": actor.type.name if hasattr(actor.type, "name") else str(actor.type),
            "plugin_id": actor.plugin_id or "-",
            "identity_key": actor.identity_key or "-",
            "disabled": "yes" if actor.disabled else "no",
        }
        for actor in actors
    ]
    headers = ["ID", "Name", "Type", "Plugin", "Identity Key", "Disabled"]
    keys = ["id", "name", "type", "plugin_id", "identity_key", "disabled"]
    render_table(rows, headers, keys)


@actors_app.command("show")
@click.argument("actor_id", type=int)
@with_lifespan(runtime_mode="fast_read")
async def show_actor(ctx: click.Context, actor_id: int) -> None:
    """Show details for a single actor."""
    from katalog.api.actors import get_actor as get_actor_api

    actor, changesets = await get_actor_api(actor_id)
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {
                    "actor": actor.model_dump(),
                    "changesets": [c.model_dump() for c in changesets],
                },
                default=str,
            )
        )
        return

    click.echo(f"ID: {actor.id}")
    click.echo(f"Name: {actor.name}")
    click.echo(f"Type: {actor.type.name}")
    click.echo(f"Plugin: {actor.plugin_id}")
    click.echo(f"Identity key: {actor.identity_key}")
    click.echo(f"Disabled: {'yes' if actor.disabled else 'no'}")
    click.echo(f"Changesets: {len(changesets)}")


@actors_app.command("run")
@click.argument("actor_id", type=int)
@click.option(
    "--reset-workspace",
    is_flag=True,
    default=False,
    help="Delete katalog.db and actors cache before scanning",
)
@click.option(
    "--workflow",
    "workflow_file",
    default=None,
    help="Sync actors from this workflow TOML before scanning",
)
@click.option(
    "--skip-processors",
    is_flag=True,
    default=False,
    help="Skip running processors as part of the scan",
)
@click.option(
    "--benchmark",
    is_flag=True,
    default=False,
    help="Benchmark mode: report max RSS and delete the changeset after the run",
)
@with_lifespan()
async def run_actor(
    ctx: click.Context,
    actor_id: int,
    reset_workspace: bool,
    workflow_file: str | None,
    skip_processors: bool,
    benchmark: bool,
) -> None:
    """Run a source scan for the given actor id without starting the server."""
    ws = ctx.obj["workspace"]
    if reset_workspace:
        _reset_workspace(ws)

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

    result: dict[str, Any] = {
        "changeset": changeset_summary(changeset),
        "max_rss_mb": max_rss_mb,
        "deleted": benchmark,
    }

    if wants_json(ctx):
        click.echo(json.dumps(result, default=str))
        return

    print_changeset_summary(result["changeset"])
    if result.get("max_rss_mb") is not None:
        click.echo(f"Max RSS: {result['max_rss_mb']:.2f} MB")
    if result.get("deleted"):
        click.echo("Deleted: yes")


@actors_app.command("authorize")
@click.argument("actor_id", type=int)
@with_lifespan()
async def authorize_actor(ctx: click.Context, actor_id: int) -> None:
    """Start source authorization for the given actor id."""
    from katalog.api.operations import authorize_source

    result = await authorize_source(actor_id)
    if wants_json(ctx):
        click.echo(json.dumps(result, default=str))
        return

    if result.get("authorization_url"):
        click.echo("Authorization required")
        click.echo(str(result["authorization_url"]))
        return

    click.echo("Source is authorized")
