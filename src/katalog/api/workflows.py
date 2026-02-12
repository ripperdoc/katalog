from __future__ import annotations

import pathlib

from fastapi import APIRouter

from katalog.api.helpers import ApiError
from katalog.config import WORKSPACE
from katalog.workflows import (
    discover_workflow_files,
    load_workflow_spec,
    run_workflow_file,
    start_workflow_file,
    sync_workflow_file,
    workflow_status,
)

router = APIRouter()


def _workspace() -> pathlib.Path:
    if WORKSPACE is None:
        raise ApiError(status_code=500, detail="Workspace is not configured")
    return pathlib.Path(WORKSPACE)


def _resolve_workflow_file(workflow_name: str) -> pathlib.Path:
    files = discover_workflow_files(_workspace())
    by_name = {path.name: path for path in files}
    file_path = by_name.get(workflow_name)
    if file_path is None:
        raise ApiError(status_code=404, detail="Workflow not found")
    return file_path


@router.get("/workflows")
async def list_workflows_rest():
    results: list[dict] = []
    for file_path in discover_workflow_files(_workspace()):
        try:
            results.append(await workflow_status(file_path))
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "file_name": file_path.name,
                    "file_path": str(file_path),
                    "name": file_path.stem,
                    "description": None,
                    "version": None,
                    "actor_count": 0,
                    "source_count": 0,
                    "processor_count": 0,
                    "analyzer_count": 0,
                    "resolved_actor_count": 0,
                    "status": "invalid",
                    "actor_names": [],
                    "processor_stages": [],
                    "error": str(exc),
                }
            )
    return {"workflows": results}


@router.get("/workflows/{workflow_name}")
async def get_workflow_rest(workflow_name: str):
    file_path = _resolve_workflow_file(workflow_name)
    try:
        spec = load_workflow_spec(file_path)
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=400, detail=str(exc)) from exc

    status = await workflow_status(file_path)
    return {
        "workflow": {
            "file_name": spec.file_name,
            "file_path": spec.file_path,
            "workflow_id": spec.workflow_id,
            "name": spec.name,
            "description": spec.description,
            "version": spec.version,
            "actors": [
                {
                    "name": actor.name,
                    "plugin_id": actor.plugin_id,
                    "actor_type": actor.actor_type.name,
                    "disabled": actor.disabled,
                }
                for actor in spec.actors
            ],
            "status": status["status"],
            "resolved_actor_count": status["resolved_actor_count"],
        }
    }


@router.post("/workflows/{workflow_name}/sync")
async def sync_workflow_rest(workflow_name: str):
    file_path = _resolve_workflow_file(workflow_name)
    actors = await sync_workflow_file(file_path)
    status = await workflow_status(file_path)
    return {
        "status": "synced",
        "workflow": status,
        "actors": [actor.model_dump(mode="json") for actor in actors],
    }


@router.post("/workflows/{workflow_name}/run")
async def run_workflow_rest(workflow_name: str):
    file_path = _resolve_workflow_file(workflow_name)
    result = await start_workflow_file(file_path, sync_first=False)
    status = await workflow_status(file_path)
    return {
        "status": "started",
        "workflow": status,
        "result": result,
        "changeset": result.get("changeset"),
    }


@router.post("/workflows/{workflow_name}/apply")
async def apply_workflow_rest(workflow_name: str):
    file_path = _resolve_workflow_file(workflow_name)
    result = await start_workflow_file(file_path, sync_first=True)
    status = await workflow_status(file_path)
    return {
        "status": "started",
        "workflow": status,
        "result": result,
        "changeset": result.get("changeset"),
    }
