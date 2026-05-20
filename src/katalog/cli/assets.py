import json

import asyncclick as click

from katalog.models.query import AssetQuery

from . import assets_app
from .utils import render_table, wants_json, with_lifespan


@assets_app.command("list")
@click.option("--limit", "-l", type=int, default=100, show_default=True, help="Max assets to list")
@click.option("--offset", "-o", type=int, default=0, show_default=True, help="Offset into result set")
@click.option(
    "--view-id",
    type=str,
    default="default",
    show_default=True,
    help="Asset view id to query (run `katalog views list` to discover ids).",
)
@click.option(
    "--include-linked-sidecars",
    is_flag=True,
    default=False,
    help="Project linked sidecar metadata onto target assets.",
)
@with_lifespan(runtime_mode="fast_read")
async def list_assets(
    ctx: click.Context,
    limit: int,
    offset: int,
    view_id: str,
    include_linked_sidecars: bool,
) -> None:
    """List assets in the workspace."""
    from katalog.api.assets import list_assets as list_assets_api

    query = AssetQuery(
        view_id=view_id,
        limit=limit,
        offset=offset,
        metadata_include_linked_sidecars=include_linked_sidecars,
    )
    response = await list_assets_api(query=query)

    if wants_json(ctx):
        click.echo(json.dumps(response.model_dump(), default=str))
        return

    if not response.items:
        click.echo("No assets found")
        return

    rows = [
        {
            "id": str(item.asset_id),
            "namespace": item.asset_namespace or "-",
            "external_id": item.asset_external_id or "-",
            "actor_id": str(item.asset_actor_id) if item.asset_actor_id else "-",
        }
        for item in response.items
    ]
    headers = ["ID", "Namespace", "External ID", "Actor"]
    keys = ["id", "namespace", "external_id", "actor_id"]
    render_table(rows, headers, keys)
    click.echo(f"Total returned: {response.stats.returned}")


@assets_app.command("show")
@click.argument("asset_id", type=int)
@with_lifespan(runtime_mode="fast_read")
async def show_asset(ctx: click.Context, asset_id: int) -> None:
    """Show details for a single asset."""
    from katalog.api.assets import get_asset as get_asset_api

    asset, metadata = await get_asset_api(asset_id)
    if wants_json(ctx):
        click.echo(
            json.dumps(
                {
                    "asset": asset.model_dump(),
                    "metadata": [entry.model_dump() for entry in metadata],
                },
                default=str,
            )
        )
        return

    click.echo(f"ID: {asset.id}")
    click.echo(f"Namespace: {asset.namespace}")
    click.echo(f"External ID: {asset.external_id}")
    click.echo(f"Canonical URI: {asset.canonical_uri}")
    click.echo(f"Actor ID: {asset.actor_id if asset.actor_id else '-'}")
    click.echo(f"Metadata entries: {len(metadata)}")
