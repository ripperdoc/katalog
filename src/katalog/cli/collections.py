import json
from typing import Any

import typer

from . import collections_app
from .utils import render_table, run_cli, wants_json


@collections_app.command("list")
def list_collections(ctx: typer.Context) -> None:
    """List asset collections in the workspace."""

    async def _run() -> list[Any]:
        from katalog.api.collections import list_collections as list_collections_api

        return await list_collections_api()

    collections = run_cli(_run, init_mode="fast")
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {"collections": [collection.model_dump() for collection in collections]},
                default=str,
            )
        )
        return

    if not collections:
        typer.echo("No collections found")
        return

    rows = [
        {
            "id": str(collection.id or "-"),
            "name": collection.name,
            "count": str(collection.asset_count),
            "refresh": collection.refresh_mode.value
            if hasattr(collection.refresh_mode, "value")
            else str(collection.refresh_mode),
        }
        for collection in collections
    ]
    headers = ["ID", "Name", "Assets", "Refresh"]
    keys = ["id", "name", "count", "refresh"]
    render_table(rows, headers, keys)


@collections_app.command("show")
def show_collection(collection_id: int, ctx: typer.Context) -> None:
    """Show details for a single collection."""

    async def _run() -> Any:
        from katalog.api.collections import get_collection as get_collection_api

        return await get_collection_api(collection_id)

    collection = run_cli(_run, init_mode="fast")
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {"collection": collection.model_dump()},
                default=str,
            )
        )
        return

    typer.echo(f"ID: {collection.id}")
    typer.echo(f"Name: {collection.name}")
    typer.echo(f"Description: {collection.description or '-'}")
    typer.echo(
        "Refresh mode: "
        + (
            collection.refresh_mode.value
            if hasattr(collection.refresh_mode, "value")
            else str(collection.refresh_mode)
        )
    )
    typer.echo(f"Assets: {collection.asset_count}")
    typer.echo("Source: yes" if collection.source else "Source: no")
