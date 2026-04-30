from fastapi import APIRouter
from pydantic import BaseModel

from katalog.api.workflows import (
    get_workflow,
    list_workflows,
    start_workflow,
)
from katalog.workflows.contracts import WorkflowInputSpec, parse_workflow_input_payload

router = APIRouter()


class WorkflowStartRequest(BaseModel):
    always_process: bool | None = None
    input: dict | None = None


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
    payload: WorkflowStartRequest | None = None,
):
    parsed_input: WorkflowInputSpec | None = None
    if payload is not None and payload.input is not None:
        parsed_input = parse_workflow_input_payload(payload.input)
    return await start_workflow(
        workflow_name,
        always_process=payload.always_process if payload is not None else None,
        workflow_input=parsed_input,
    )
