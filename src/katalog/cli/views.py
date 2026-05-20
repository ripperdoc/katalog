import json

import asyncclick as click

from . import views_app
from .utils import render_table, wants_json, with_lifespan


@views_app.command("list")
@with_lifespan(runtime_mode="fast_read")
async def list_views(ctx: click.Context) -> None:
    """List available asset views."""
    from katalog.api.views import list_views_api

    views = await list_views_api()
    if wants_json(ctx):
        click.echo(json.dumps({"views": [view.model_dump() for view in views]}, default=str))
        return

    if not views:
        click.echo("No views found")
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
@click.argument("view_id", type=str)
@with_lifespan(runtime_mode="fast_read")
async def get_view(ctx: click.Context, view_id: str) -> None:
    """Show one asset view by id."""
    from katalog.api.views import get_view_api

    view = await get_view_api(view_id)
    if wants_json(ctx):
        click.echo(json.dumps({"view": view.model_dump()}, default=str))
        return

    click.echo(f"ID: {view.id}")
    click.echo(f"Name: {view.name}")
    click.echo(f"Columns: {len(view.columns or [])}")
    click.echo(f"Default columns: {', '.join(view.default_columns) if view.default_columns else '-'}")
