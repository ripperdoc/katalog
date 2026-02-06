import json
from typing import Any

import typer

from . import changesets_app
from .utils import render_table, run_cli, wants_json


@changesets_app.command("list")
def list_changesets(ctx: typer.Context) -> None:
    """List changesets in the workspace."""

    async def _run() -> list[Any]:
        from katalog.api.changesets import list_changesets as list_changesets_api

        return await list_changesets_api()

    changesets = run_cli(_run)
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {"changesets": [changeset.model_dump() for changeset in changesets]},
                default=str,
            )
        )
        return

    if not changesets:
        typer.echo("No changesets found")
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
def show_changeset(changeset_id: int, ctx: typer.Context) -> None:
    """Show details for a single changeset."""

    async def _run() -> tuple[Any, list[str], bool]:
        from katalog.api.changesets import get_changeset as get_changeset_api

        return await get_changeset_api(changeset_id)

    changeset, logs, running = run_cli(_run)
    if wants_json(ctx):
        typer.echo(
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

    typer.echo(f"ID: {changeset.id}")
    typer.echo(
        "Status: "
        + (
            changeset.status.value
            if hasattr(changeset.status, "value")
            else str(changeset.status)
        )
    )
    typer.echo(f"Message: {changeset.message or '-'}")
    typer.echo(
        "Actor IDs: "
        + (", ".join(str(a) for a in (changeset.actor_ids or [])) or "-")
    )
    typer.echo(f"Running time (ms): {changeset.running_time_ms or '-'}")
    typer.echo(f"Running: {'yes' if running else 'no'}")
    typer.echo(f"Logs: {len(logs)} entries")


@changesets_app.command("delete")
def delete_changeset(changeset_id: int, ctx: typer.Context) -> None:
    """Delete a changeset and all related rows."""

    async def _run() -> dict[str, int | str]:
        from katalog.api.changesets import delete_changeset as delete_changeset_api

        return await delete_changeset_api(changeset_id)

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return

    typer.echo(f"Deleted changeset {result['changeset_id']}")
