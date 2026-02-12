import json
import pathlib
from typing import Any

import typer

from . import workflows_app
from .utils import run_cli, wants_json


def _resolve_workflow_path(ctx: typer.Context, workflow_file: str) -> pathlib.Path:
    ws = pathlib.Path(ctx.obj["workspace"])
    path = pathlib.Path(workflow_file)
    if not path.is_absolute():
        path = ws / path
    return path.resolve()


@workflows_app.command("sync")
def sync_workflow(
    ctx: typer.Context,
    workflow_file: str = typer.Option(
        "workflow.toml",
        "--file",
        "-f",
        help="Path to workflow TOML file (relative to workspace by default)",
    ),
) -> None:
    """Sync workflow actors into the database."""

    async def _run() -> dict[str, Any]:
        from katalog.workflows import sync_workflow_file

        path = _resolve_workflow_path(ctx, workflow_file)
        actors = await sync_workflow_file(path)
        return {
            "workflow_file": str(path),
            "actors": [actor.model_dump(mode="json") for actor in actors],
            "count": len(actors),
        }

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    typer.echo(f"Workflow: {result['workflow_file']}")
    typer.echo(f"Actors synced: {result['count']}")


@workflows_app.command("run")
def run_workflow(
    ctx: typer.Context,
    workflow_file: str = typer.Option(
        "workflow.toml",
        "--file",
        "-f",
        help="Path to workflow TOML file (relative to workspace by default)",
    ),
) -> None:
    """Run workflow actors. Expects actors to already be synced."""

    async def _run() -> dict[str, Any]:
        from katalog.workflows import run_workflow_file

        path = _resolve_workflow_path(ctx, workflow_file)
        return await run_workflow_file(path, sync_first=False)

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    typer.echo(f"Workflow: {result['workflow_file']}")
    typer.echo(f"Sources run: {result['sources_run']}")
    typer.echo(f"Processors run: {result['processors_run']}")
    if result.get("processor_changeset"):
        typer.echo(f"Processor changeset: {result['processor_changeset']}")


@workflows_app.command("apply")
def apply_workflow(
    ctx: typer.Context,
    workflow_file: str = typer.Option(
        "workflow.toml",
        "--file",
        "-f",
        help="Path to workflow TOML file (relative to workspace by default)",
    ),
) -> None:
    """Sync workflow actors and then run the workflow."""

    async def _run() -> dict[str, Any]:
        from katalog.workflows import run_workflow_file

        path = _resolve_workflow_path(ctx, workflow_file)
        return await run_workflow_file(path, sync_first=True)

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    typer.echo(f"Workflow: {result['workflow_file']}")
    typer.echo(f"Sources run: {result['sources_run']}")
    typer.echo(f"Processors run: {result['processors_run']}")
    if result.get("processor_changeset"):
        typer.echo(f"Processor changeset: {result['processor_changeset']}")
