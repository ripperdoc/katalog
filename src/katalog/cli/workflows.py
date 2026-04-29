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


def _extract_changeset_ids(result: dict[str, Any]) -> list[int]:
    source_changesets = [int(v) for v in result.get("source_changesets", [])]
    processor_changeset = result.get("processor_changeset")
    analyzer_changesets = [int(v) for v in result.get("analyzer_changesets", [])]
    ids: list[int] = [*source_changesets]
    if processor_changeset is not None:
        ids.append(int(processor_changeset))
    ids.extend(analyzer_changesets)
    return ids


@workflows_app.command("start", hidden=True)
def start_workflow_command(
    ctx: typer.Context,
    workflow_file: str = typer.Option(
        "workflow.toml",
        "--file",
        "-f",
        help="Path to workflow TOML file (relative to workspace by default)",
    ),
    always_process: bool | None = typer.Option(
        None,
        "--always-process/--respect-skip",
        help="Override workflow skip behavior for processors.",
    ),
) -> None:
    """Run workflow execution and wait for completion."""
    _run_workflow_command(ctx, workflow_file, always_process=always_process)


@workflows_app.command("run")
def run_workflow_command(
    ctx: typer.Context,
    workflow_file: str = typer.Option(
        "workflow.toml",
        "--file",
        "-f",
        help="Path to workflow TOML file (relative to workspace by default)",
    ),
    always_process: bool | None = typer.Option(
        None,
        "--always-process/--respect-skip",
        help="Override workflow skip behavior for processors.",
    ),
) -> None:
    """Run workflow execution and wait for completion."""
    _run_workflow_command(ctx, workflow_file, always_process=always_process)


def _run_workflow_command(
    ctx: typer.Context,
    workflow_file: str,
    *,
    always_process: bool | None,
) -> None:
    """Shared implementation for synchronous workflow CLI commands."""

    async def _run() -> dict[str, Any]:
        from katalog.api.workflows import run_workflow
        from katalog.workflows import load_workflow_spec

        path = _resolve_workflow_path(ctx, workflow_file)
        spec = load_workflow_spec(path)
        completed = await run_workflow(spec, always_process=always_process)
        result_payload = completed.get("result") or {}
        changeset_ids = _extract_changeset_ids(result_payload)
        completed["changeset_summaries"] = await _summaries_for_changesets(changeset_ids)
        return completed

    result = run_cli(_run)
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return
    workflow = result.get("workflow", {})
    run_result = result.get("result", {})
    typer.echo(f"Workflow: {workflow.get('file_path', workflow_file)}")
    typer.echo(f"Sources run: {run_result.get('sources_run', 0)}")
    typer.echo(f"Processors run: {run_result.get('processors_run', 0)}")
    typer.echo(f"Analyzers run: {run_result.get('analyzers_run', 0)}")
    for summary in result.get("changeset_summaries", []):
        print_changeset_summary(summary)
