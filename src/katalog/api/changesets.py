import asyncio
import json

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from katalog.db import list_changeset_metadata_changes
from katalog.models import Changeset, OpStatus
from katalog.utils.changeset_events import sse_event

from katalog.api.helpers import ensure_manual_actor
from katalog.api.state import RUNNING_CHANGESETS, event_manager

router = APIRouter()


@router.post("/changesets")
async def create_changeset():
    raise NotImplementedError("Not supported to create changesets directly")


@router.post("/changesets/manual/start")
async def start_manual_changeset():
    actor = await ensure_manual_actor()
    try:
        changeset = await Changeset.begin(actor=actor, status=OpStatus.IN_PROGRESS)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return changeset.to_dict()


@router.post("/changesets/{changeset_id}/finish")
async def finish_changeset(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise HTTPException(status_code=409, detail="Changeset is not in progress")
    await changeset.finalize(status=OpStatus.COMPLETED)
    return {"changeset": (await Changeset.get(id=changeset_id)).to_dict()}


@router.get("/changesets")
async def list_changesets():
    changesets = await Changeset.all().order_by("-started_at").prefetch_related("actor")
    return {"changesets": [s.to_dict() for s in changesets]}


@router.get("/changesets/{changeset_id}")
async def get_changeset(changeset_id: int, stream: bool = Query(False)):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    await changeset.fetch_related("actor")

    if stream:
        return await stream_changeset_events(changeset_id)
    return {
        "changeset": changeset.to_dict(),
        "logs": event_manager.get_buffer(changeset_id),
        "running": changeset.status == OpStatus.IN_PROGRESS,
    }


@router.delete("/changesets/{changeset_id}")
async def delete_changeset(changeset_id: int):
    """Undo a changeset by deleting it (cascade removes related rows)."""
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")

    await changeset.delete()
    return {"status": "deleted", "changeset_id": changeset_id}


@router.get("/changesets/{changeset_id}/changes")
async def list_changeset_changes(
    changeset_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")

    try:
        return await list_changeset_metadata_changes(
            changeset_id, offset=offset, limit=limit, include_total=True
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.patch("/changesets/{changeset_id}")
async def update_changeset(changeset_id: int):
    raise NotImplementedError()


@router.get("/changesets/{changeset_id}/events")
async def stream_changeset_events(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")

    await changeset.fetch_related("actor")
    history, queue = event_manager.subscribe(changeset_id)
    run_state = RUNNING_CHANGESETS.get(changeset_id)
    done_event = run_state.done_event if run_state else asyncio.Event()
    if run_state is None and changeset.status != OpStatus.IN_PROGRESS:
        done_event.set()

    async def event_generator():
        try:
            for line in history:
                yield sse_event("log", line)
            while True:
                done_waiter = asyncio.create_task(done_event.wait())
                log_waiter = asyncio.create_task(queue.get())
                done, pending = await asyncio.wait(
                    {done_waiter, log_waiter}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if log_waiter in done:
                    message = log_waiter.result()
                    yield sse_event("log", message)
                else:
                    log_waiter.cancel()
                if done_waiter in done:
                    latest = await Changeset.get(id=changeset_id)
                    await latest.fetch_related("actor")
                    yield sse_event("changeset", json.dumps(latest.to_dict()))
                    break
        finally:
            event_manager.unsubscribe(changeset_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/changesets/{changeset_id}/cancel")
async def cancel_changeset(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    run_state = RUNNING_CHANGESETS.get(changeset_id)
    if run_state is None or run_state.done_event.is_set():
        # Nothing running: finalize as CANCELED
        await changeset.finalize(status=OpStatus.CANCELED)
        latest = await Changeset.get(id=changeset_id)
        await latest.fetch_related("actor")
        return {"status": "cancelled", "changeset": latest.to_dict()}

    run_state.cancel_event.set()
    for task in list(run_state.changeset.tasks):
        task.cancel()
    run_state.task.cancel()

    return {"status": "cancellation_requested"}
