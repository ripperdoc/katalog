from fastapi import APIRouter

from katalog.api.workflows import (
    apply_workflow,
    get_workflow,
    list_workflows,
    run_workflow,
    sync_workflow,
)

router = APIRouter()


@router.get("/workflows")
async def list_workflows_rest():
    workflows = await list_workflows()
    return {"workflows": workflows}


@router.get("/workflows/{workflow_name}")
async def get_workflow_rest(workflow_name: str):
    workflow = await get_workflow(workflow_name)
    return {"workflow": workflow}


@router.post("/workflows/{workflow_name}/sync")
async def sync_workflow_rest(workflow_name: str):
    return await sync_workflow(workflow_name)


@router.post("/workflows/{workflow_name}/run")
async def run_workflow_rest(workflow_name: str):
    return await run_workflow(workflow_name)


@router.post("/workflows/{workflow_name}/apply")
async def apply_workflow_rest(workflow_name: str):
    return await apply_workflow(workflow_name)
