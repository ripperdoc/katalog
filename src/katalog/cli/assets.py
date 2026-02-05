import json
from typing import Any

import typer

from katalog.models.query import AssetQuery

from . import assets_app
from .utils import render_table, run_cli, wants_json


@assets_app.command("list")
def list_assets(
    ctx: typer.Context,
    limit: int = typer.Option(100, "--limit", "-l", help="Max assets to list"),
    offset: int = typer.Option(0, "--offset", "-o", help="Offset into result set"),
) -> None:
    """List assets in the workspace."""

    async def _run() -> Any:
        from katalog.api.assets import list_assets as list_assets_api

        query = AssetQuery(view_id="default", limit=limit, offset=offset)
        return await list_assets_api(query=query)

    response = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(response.model_dump(), default=str))
        return

    if not response.items:
        typer.echo("No assets found")
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
    typer.echo(f"Total returned: {response.stats.returned}")


@assets_app.command("show")
def show_asset(asset_id: int, ctx: typer.Context) -> None:
    """Show details for a single asset."""

    async def _run() -> tuple[Any, list[Any]]:
        from katalog.api.assets import get_asset as get_asset_api

        return await get_asset_api(asset_id)

    asset, metadata = run_cli(_run)
    if wants_json(ctx):
        typer.echo(
            json.dumps(
                {
                    "asset": asset.model_dump(),
                    "metadata": [entry.model_dump() for entry in metadata],
                },
                default=str,
            )
        )
        return

    typer.echo(f"ID: {asset.id}")
    typer.echo(f"Namespace: {asset.namespace}")
    typer.echo(f"External ID: {asset.external_id}")
    typer.echo(f"Canonical URI: {asset.canonical_uri}")
    typer.echo(f"Actor ID: {asset.actor_id if asset.actor_id else '-'}")
    typer.echo(f"Metadata entries: {len(metadata)}")
