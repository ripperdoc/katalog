import json
from typing import Any

import typer

from . import views_app
from .utils import render_table, run_cli, wants_json


@views_app.command("list")
def list_views(ctx: typer.Context) -> None:
    """List available asset views."""

    async def _run() -> list[Any]:
        from katalog.api.views import list_views_api

        return await list_views_api()

    views = run_cli(_run, runtime_mode="fast_read")
    if wants_json(ctx):
        typer.echo(json.dumps({"views": [view.model_dump() for view in views]}, default=str))
        return

    if not views:
        typer.echo("No views found")
        return

    rows = [
        {
            "id": view.id,
            "name": view.name,
            "columns": str(len(view.columns or [])),
            "default_columns": "yes" if view.default_columns else "no",
        }
        for view in views
    ]
    render_table(rows, ["ID", "Name", "Columns", "Default Cols"], ["id", "name", "columns", "default_columns"])


@views_app.command("get")
def get_view(view_id: str, ctx: typer.Context) -> None:
    """Show one asset view by id."""

    async def _run() -> Any:
        from katalog.api.views import get_view_api

        return await get_view_api(view_id)

    view = run_cli(_run, runtime_mode="fast_read")
    if wants_json(ctx):
        typer.echo(json.dumps({"view": view.model_dump()}, default=str))
        return

    typer.echo(f"ID: {view.id}")
    typer.echo(f"Name: {view.name}")
    typer.echo(f"Columns: {len(view.columns or [])}")
    typer.echo(f"Default columns: {', '.join(view.default_columns) if view.default_columns else '-'}")
