import json

import asyncclick as click

from . import collections_app
from .utils import render_table, wants_json, with_lifespan


@collections_app.command("list")
@with_lifespan(runtime_mode="fast_read")
async def list_collections(ctx: click.Context) -> None:
    """List asset collections in the workspace."""
    from katalog.api.collections import list_collections as list_collections_api

    collections = await list_collections_api()
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {"collections": [collection.model_dump() for collection in collections]},
                default=str,
            )
        )
        return

    if not collections:
        click.echo("No collections found")
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
@click.argument("collection_id", type=int)
@with_lifespan(runtime_mode="fast_read")
async def show_collection(ctx: click.Context, collection_id: int) -> None:
    """Show details for a single collection."""
    from katalog.api.collections import get_collection as get_collection_api

    collection = await get_collection_api(collection_id)
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {"collection": collection.model_dump()},
                default=str,
            )
        )
        return

    click.echo(f"ID: {collection.id}")
    click.echo(f"Name: {collection.name}")
    click.echo(f"Description: {collection.description or '-'}")
    click.echo(
        "Refresh mode: "
        + (
            collection.refresh_mode.value
            if hasattr(collection.refresh_mode, "value")
            else str(collection.refresh_mode)
        )
    )
    click.echo(f"Assets: {collection.asset_count}")
    click.echo("Source: yes" if collection.source else "Source: no")
