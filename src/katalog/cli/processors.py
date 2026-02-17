import json
from typing import Any

import typer

from . import processors_app
from .utils import changeset_summary, print_changeset_summary, run_cli, wants_json


@processors_app.command("run")
def run_processors(
    ctx: typer.Context,
    processor_ids: list[int] = typer.Option(
        None,
        "--processor-id",
        "-p",
        help="Processor actor id to run (repeatable, defaults to all)",
    ),
    asset_ids: list[int] = typer.Option(
        None,
        "--asset-id",
        "-a",
        help="Asset id to process (repeatable, defaults to all)",
    ),
    benchmark: bool = typer.Option(
        False,
        "--benchmark",
        help="Benchmark mode: report max RSS and delete the changeset after the run",
    ),
) -> None:
    """Run processors for assets without starting the server."""

    async def _run() -> dict[str, Any]:
        import resource

        from katalog.api.operations import run_processors as run_processors_api

        changeset = await run_processors_api(
            processor_ids=processor_ids or None,
            asset_ids=asset_ids or None,
            finalize=True,
        )
        max_rss_mb = None
        if benchmark:
            max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            max_rss_mb = max_rss / (1024 * 1024)
            from katalog.api.changesets import delete_changeset as delete_changeset_api

            await delete_changeset_api(int(changeset.id))
        return {
            "changeset": changeset_summary(changeset),
            "max_rss_mb": max_rss_mb,
            "deleted": benchmark,
        }

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return

    print_changeset_summary(result["changeset"])
    if result.get("max_rss_mb") is not None:
        typer.echo(f"Max RSS: {result['max_rss_mb']:.2f} MB")
    if result.get("deleted"):
        typer.echo("Deleted: yes")
