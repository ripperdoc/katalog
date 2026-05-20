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


@assets_app.command("get")
@click.argument("asset_id", type=int)
@click.option(
    "--actor-id",
    "metadata_actor_ids",
    type=int,
    multiple=True,
    help="Filter metadata to actor id (repeatable).",
)
@click.option(
    "--include-removed",
    is_flag=True,
    default=False,
    help="Include removed metadata rows (only valid with --aggregation object).",
)
@click.option(
    "--aggregation",
    type=str,
    default="latest",
    show_default=True,
    help="Metadata aggregation: latest, current, object.",
)
@with_lifespan(runtime_mode="fast_read")
async def get_asset_cli(
    ctx: click.Context,
    asset_id: int,
    metadata_actor_ids: tuple[int, ...],
    include_removed: bool,
    aggregation: str,
) -> None:
    """Get one asset with projected metadata."""
    from katalog.api.assets import get_asset_serialized as get_asset_api

    agg = aggregation.strip().lower()
    if agg not in {"latest", "current", "object"}:
        raise click.BadParameter("--aggregation must be one of: latest, current, object")

    try:
        row = await get_asset_api(
            asset_id,
            metadata_actor_ids=list(metadata_actor_ids) or None,
            metadata_include_removed=include_removed,
            metadata_aggregation=agg,
        )
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc
    if wants_json(ctx):
        click.echo(json.dumps(row, default=str))
        return

    click.echo(f"ID: {row.get('asset/id')}")
    click.echo(f"Namespace: {row.get('asset/namespace')}")
    click.echo(f"External ID: {row.get('asset/external_id')}")
    click.echo(f"Canonical URI: {row.get('asset/canonical_uri')}")
    click.echo(f"Actor ID: {row.get('asset/actor_id') or '-'}")
    metadata_keys = [key for key in row.keys() if not key.startswith("asset/")]
    click.echo(f"Metadata keys: {len(metadata_keys)}")
