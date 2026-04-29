from fastapi import APIRouter

from katalog.api.workflows import (
    get_workflow,
    list_workflows,
    start_workflow,
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


@router.post("/workflows/{workflow_name}/start")
async def start_workflow_rest(
    workflow_name: str,
    always_process: bool | None = None,
):
    return await start_workflow(
        workflow_name,
        always_process=always_process,
    )
