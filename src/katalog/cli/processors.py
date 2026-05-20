import json
from typing import Any

import asyncclick as click

from . import processors_app
from .utils import changeset_summary, print_changeset_summary, wants_json, with_lifespan


@processors_app.command("run")
@click.option(
    "--processor-id",
    "processor_ids",
    "-p",
    multiple=True,
    type=int,
    help="Processor actor id to run (repeatable, defaults to all)",
)
@click.option(
    "--asset-id",
    "asset_ids",
    "-a",
    multiple=True,
    type=int,
    help="Asset id to process (repeatable, defaults to all)",
)
@click.option(
    "--benchmark",
    is_flag=True,
    default=False,
    help="Benchmark mode: report max RSS and delete the changeset after the run",
)
@with_lifespan()
async def run_processors(
    ctx: click.Context,
    processor_ids: tuple[int, ...],
    asset_ids: tuple[int, ...],
    benchmark: bool,
) -> None:
    """Run processors for assets without starting the server."""

    import resource

    from katalog.api.operations import run_processors as run_processors_api

    changeset = await run_processors_api(
        processor_ids=list(processor_ids) or None,
        asset_ids=list(asset_ids) or None,
        finalize=True,
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
