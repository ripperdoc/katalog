import json
import pathlib
from typing import Any

import typer

from . import workflows_app
from .utils import changeset_summary, print_changeset_summary, run_cli, wants_json


def _resolve_workflow_path(ctx: typer.Context, workflow_file: str) -> pathlib.Path:
    ws = pathlib.Path(ctx.obj["workspace"])
    path = pathlib.Path(workflow_file)
    if path.is_absolute():
        return path.resolve()

    cwd_candidate = path.resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    return (ws / path).resolve()


async def _summaries_for_changesets(changeset_ids: list[int]) -> list[dict[str, Any]]:
    from katalog.api.changesets import get_changeset as get_changeset_api

    summaries: list[dict[str, Any]] = []
    for changeset_id in changeset_ids:
        changeset, _logs, _running = await get_changeset_api(int(changeset_id))
        summaries.append(changeset_summary(changeset))
    return summaries


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
        result = await run_workflow_file(path, sync_first=False)
        changeset_ids = [
            *result.source_changesets,
            *([result.processor_changeset] if result.processor_changeset else []),
            *result.analyzer_changesets,
        ]
        payload = result.model_dump(mode="json")
        payload["changeset_summaries"] = await _summaries_for_changesets(changeset_ids)
        return payload

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    typer.echo(f"Workflow: {result['workflow_file']}")
    typer.echo(f"Sources run: {result['sources_run']}")
    typer.echo(f"Processors run: {result['processors_run']}")
    typer.echo(f"Analyzers run: {result.get('analyzers_run', 0)}")
    for summary in result.get("changeset_summaries", []):
        print_changeset_summary(summary)


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
        result = await run_workflow_file(path, sync_first=True)
        changeset_ids = [
            *result.source_changesets,
            *([result.processor_changeset] if result.processor_changeset else []),
            *result.analyzer_changesets,
        ]
        payload = result.model_dump(mode="json")
        payload["changeset_summaries"] = await _summaries_for_changesets(changeset_ids)
        return payload

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    typer.echo(f"Workflow: {result['workflow_file']}")
    typer.echo(f"Sources run: {result['sources_run']}")
    typer.echo(f"Processors run: {result['processors_run']}")
    typer.echo(f"Analyzers run: {result.get('analyzers_run', 0)}")
    for summary in result.get("changeset_summaries", []):
        print_changeset_summary(summary)
