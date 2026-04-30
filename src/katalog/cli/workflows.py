import json
import pathlib
import shutil
from typing import Any

import typer

from . import workflows_app
from .utils import changeset_summary, print_changeset_summary, run_cli, wants_json
from katalog.workflows.contracts import (
    WorkflowAllAssetsInput,
    WorkflowAssetIdsInput,
    WorkflowCollectionInput,
    WorkflowInputSpec,
    WorkflowSourceActorsInput,
)


def _workspace_path(ctx: typer.Context) -> pathlib.Path:
    return pathlib.Path(ctx.obj["workspace"]).resolve()


def _resolve_workflow_path(ctx: typer.Context, workflow_file: str) -> pathlib.Path:
    ws = _workspace_path(ctx)
    path = pathlib.Path(workflow_file)
    if path.is_absolute():
        return path.resolve()

    cwd_candidate = path.resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    return (ws / path).resolve()


def _is_within_workspace(path: pathlib.Path, workspace: pathlib.Path) -> bool:
    try:
        path.relative_to(workspace)
        return True
    except ValueError:
        return False


def _workflow_name_from_file_arg(ctx: typer.Context, workflow_file: str) -> str:
    workspace = _workspace_path(ctx)
    source = _resolve_workflow_path(ctx, workflow_file)
    if not source.exists():
        raise typer.BadParameter(
            f"Workflow file does not exist: {source}",
            param_hint="--file/-f",
        )

    if _is_within_workspace(source, workspace):
        return source.name

    target = (workspace / source.name).resolve()
    if not typer.confirm(
        f"Workflow file '{source}' is outside workspace. Copy into workspace as '{target.name}'?",
        default=True,
    ):
        raise typer.Abort()

    if target.exists() and not typer.confirm(
        f"'{target.name}' already exists in workspace. Overwrite?",
        default=False,
    ):
        raise typer.Abort()

    shutil.copy2(source, target)
    typer.echo(f"Copied workflow into workspace: {target}")
    return target.name


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


def _build_cli_workflow_input(
    *,
    input_all: bool,
    input_actor: list[int],
    input_collection: int | None,
    input_asset: list[int],
) -> WorkflowInputSpec | None:
    selected = 0
    if input_all:
        selected += 1
    if input_actor:
        selected += 1
    if input_collection is not None:
        selected += 1
    if input_asset:
        selected += 1
    if selected == 0:
        return None
    if selected > 1:
        raise typer.BadParameter(
            "Input override flags are mutually exclusive. Use exactly one of --input-all, "
            "--input-actor, --input-collection, or --input-asset."
        )
    if input_all:
        return WorkflowAllAssetsInput()
    if input_collection is not None:
        return WorkflowCollectionInput(collection_id=int(input_collection))
    if input_actor:
        return WorkflowSourceActorsInput(actor_ids=sorted(set(int(value) for value in input_actor)))
    return WorkflowAssetIdsInput(asset_ids=sorted(set(int(value) for value in input_asset)))


@workflows_app.command("start", hidden=True)
def start_workflow_command(
    ctx: typer.Context,
    workflow_name: str | None = typer.Argument(
        None,
        help="Workflow file name in workspace (e.g. workflow.demo.toml)",
    ),
    workflow_file: str | None = typer.Option(
        None,
        "--file",
        "-f",
        help="Workflow file path. If outside workspace, CLI can copy it into workspace first.",
    ),
    always_process: bool | None = typer.Option(
        None,
        "--always-process/--respect-skip",
        help="Override workflow skip behavior for processors.",
    ),
    input_all: bool = typer.Option(
        False,
        "--input-all",
        help="Override workflow input to all assets in the workspace.",
    ),
    input_actor: list[int] = typer.Option(
        [],
        "--input-actor",
        help="Override workflow input to one or more source actor ids (repeatable).",
    ),
    input_collection: int | None = typer.Option(
        None,
        "--input-collection",
        help="Override workflow input to one collection id.",
    ),
    input_asset: list[int] = typer.Option(
        [],
        "--input-asset",
        help="Override workflow input to one or more asset ids (repeatable).",
    ),
) -> None:
    """Run workflow execution and wait for completion."""
    _run_workflow_command(
        ctx,
        workflow_name=workflow_name,
        workflow_file=workflow_file,
        always_process=always_process,
        workflow_input=_build_cli_workflow_input(
            input_all=input_all,
            input_actor=input_actor,
            input_collection=input_collection,
            input_asset=input_asset,
        ),
    )


@workflows_app.command("run")
def run_workflow_command(
    ctx: typer.Context,
    workflow_name: str | None = typer.Argument(
        None,
        help="Workflow file name in workspace (e.g. workflow.demo.toml)",
    ),
    workflow_file: str | None = typer.Option(
        None,
        "--file",
        "-f",
        help="Workflow file path. If outside workspace, CLI can copy it into workspace first.",
    ),
    always_process: bool | None = typer.Option(
        None,
        "--always-process/--respect-skip",
        help="Override workflow skip behavior for processors.",
    ),
    input_all: bool = typer.Option(
        False,
        "--input-all",
        help="Override workflow input to all assets in the workspace.",
    ),
    input_actor: list[int] = typer.Option(
        [],
        "--input-actor",
        help="Override workflow input to one or more source actor ids (repeatable).",
    ),
    input_collection: int | None = typer.Option(
        None,
        "--input-collection",
        help="Override workflow input to one collection id.",
    ),
    input_asset: list[int] = typer.Option(
        [],
        "--input-asset",
        help="Override workflow input to one or more asset ids (repeatable).",
    ),
) -> None:
    """Run workflow execution and wait for completion."""
    _run_workflow_command(
        ctx,
        workflow_name=workflow_name,
        workflow_file=workflow_file,
        always_process=always_process,
        workflow_input=_build_cli_workflow_input(
            input_all=input_all,
            input_actor=input_actor,
            input_collection=input_collection,
            input_asset=input_asset,
        ),
    )


def _run_workflow_command(
    ctx: typer.Context,
    *,
    workflow_name: str | None,
    workflow_file: str | None,
    always_process: bool | None,
    workflow_input: WorkflowInputSpec | None,
) -> None:
    """Shared implementation for synchronous workflow CLI commands."""
    resolved_workflow_name = workflow_name
    if workflow_file:
        if workflow_name:
            raise typer.BadParameter(
                "Provide either workflow name argument or --file/-f, not both."
            )
        resolved_workflow_name = _workflow_name_from_file_arg(ctx, workflow_file)
    if not resolved_workflow_name:
        raise typer.BadParameter(
            "Missing workflow name. Run 'katalog workflows run <workflow-name>' or pass --file."
        )

    async def _run() -> dict[str, Any]:
        from katalog.api.workflows import run_workflow

        completed = await run_workflow(
            resolved_workflow_name,
            always_process=always_process,
            workflow_input=workflow_input,
        )
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
    typer.echo(f"Workflow: {workflow.get('file_path', resolved_workflow_name)}")
    typer.echo(f"Sources run: {run_result.get('sources_run', 0)}")
    typer.echo(f"Processors run: {run_result.get('processors_run', 0)}")
    typer.echo(f"Analyzers run: {run_result.get('analyzers_run', 0)}")
    for summary in result.get("changeset_summaries", []):
        print_changeset_summary(summary)
