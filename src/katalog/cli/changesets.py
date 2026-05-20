import json
from typing import Any

import asyncclick as click

from . import changesets_app
from .utils import render_table, wants_json, with_lifespan


@changesets_app.command("list")
@with_lifespan(runtime_mode="fast_read")
async def list_changesets(ctx: click.Context) -> None:
    """List changesets in the workspace."""
    from katalog.api.changesets import list_changesets as list_changesets_api

    changesets = await list_changesets_api()
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {"changesets": [changeset.model_dump() for changeset in changesets]},
                default=str,
            )
        )
        return

    if not changesets:
        click.echo("No changesets found")
        return

    rows = [
        {
            "id": str(changeset.id),
            "status": changeset.status.value
            if hasattr(changeset.status, "value")
            else str(changeset.status),
            "message": changeset.message or "-",
            "actors": ",".join(str(a) for a in (changeset.actor_ids or [])) or "-",
        }
        for changeset in changesets
    ]
    headers = ["ID", "Status", "Message", "Actors"]
    keys = ["id", "status", "message", "actors"]
    render_table(rows, headers, keys)


@changesets_app.command("show")
@click.argument("changeset_id", type=int)
@with_lifespan(runtime_mode="fast_read")
async def show_changeset(ctx: click.Context, changeset_id: int) -> None:
    """Show details for a single changeset."""
    from katalog.api.changesets import get_changeset as get_changeset_api

    changeset, logs, running = await get_changeset_api(changeset_id)
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {
                    "changeset": changeset.model_dump(),
                    "logs": logs,
                    "running": running,
                },
                default=str,
            )
        )
        return

    click.echo(f"ID: {changeset.id}")
    click.echo(
        "Status: "
        + (
            changeset.status.value
            if hasattr(changeset.status, "value")
            else str(changeset.status)
        )
    )
    click.echo(f"Message: {changeset.message or '-'}")
    click.echo(
        "Actor IDs: "
        + (", ".join(str(a) for a in (changeset.actor_ids or [])) or "-")
    )
    click.echo(f"Running time (ms): {changeset.running_time_ms or '-'}")
    click.echo(f"Running: {'yes' if running else 'no'}")
    click.echo(f"Logs: {len(logs)} entries")


@changesets_app.command("delete")
@click.argument("changeset_id", type=int)
@with_lifespan(runtime_mode="fast_read")
async def delete_changeset(ctx: click.Context, changeset_id: int) -> None:
    """Delete a changeset and all related rows."""
    from katalog.api.changesets import delete_changeset as delete_changeset_api

    result: dict[str, int | str] = await delete_changeset_api(changeset_id)
    if wants_json(ctx):
        click.echo(json.dumps(result, default=str))
        return

    click.echo(f"Deleted changeset {result['changeset_id']}")
