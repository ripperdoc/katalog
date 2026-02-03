import asyncio
import json

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from katalog.db import list_changeset_metadata_changes
from katalog.models import Changeset, OpStatus
from katalog.utils.changeset_events import sse_event

from katalog.editors.user_editor import ensure_user_editor
from katalog.api.state import RUNNING_CHANGESETS, event_manager
from katalog.api.helpers import ApiError

router = APIRouter()


class ChangesetUpdate(BaseModel):
    message: str | None = Field(default=None)


async def create_changeset_api() -> dict:
    actor = await ensure_user_editor()
    try:
        changeset = await Changeset.begin(
            actors=[actor],
            message="Manual edit",
            status=OpStatus.IN_PROGRESS,
            data={"manual": True},
        )
    except ValueError as exc:
        raise ApiError(status_code=409, detail=str(exc))
    return changeset.to_dict()


async def finish_changeset_api(changeset_id: int) -> dict:
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")
    await changeset.finalize(status=OpStatus.COMPLETED)
    return {"changeset": (await Changeset.get(id=changeset_id)).to_dict()}


async def list_changesets_api() -> dict:
    changesets = (
        await Changeset.all().order_by("-id").prefetch_related("actor_links__actor")
    )
    return {"changesets": [s.to_dict() for s in changesets]}


async def get_changeset_api(changeset_id: int) -> dict:
    changeset = await Changeset.get_or_none(id=changeset_id).prefetch_related(
        "actor_links__actor"
    )
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    return {
        "changeset": changeset.to_dict(),
        "logs": event_manager.get_buffer(changeset_id),
        "running": changeset.status == OpStatus.IN_PROGRESS,
    }


async def delete_changeset_api(changeset_id: int) -> dict:
    """Undo a changeset by deleting it (cascade removes related rows)."""
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    await changeset.delete()
    return {"status": "deleted", "changeset_id": changeset_id}


async def list_changeset_changes_api(
    changeset_id: int,
    offset: int = 0,
    limit: int = 200,
) -> dict:
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    try:
        return await list_changeset_metadata_changes(
            changeset_id, offset=offset, limit=limit, include_total=True
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


async def update_changeset_api(changeset_id: int, payload: ChangesetUpdate) -> dict:
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    if payload.message is not None:
        changeset.message = payload.message
        await changeset.save()

    return {"changeset": changeset.to_dict()}


async def stream_changeset_events_api(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    history, queue = event_manager.subscribe(changeset_id)
    run_state = RUNNING_CHANGESETS.get(changeset_id)
    done_event = (
        run_state.done_event if run_state and run_state.done_event else asyncio.Event()
    )
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
                    yield sse_event("changeset", json.dumps(latest.to_dict()))
                    break
        finally:
            event_manager.unsubscribe(changeset_id, queue)

    return event_generator()


async def cancel_changeset_api(changeset_id: int) -> dict:
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    running_changeset = RUNNING_CHANGESETS.get(changeset_id)
    if running_changeset is None or (
        running_changeset.done_event and running_changeset.done_event.is_set()
    ):
        # Nothing running (maybe after restart). If still in progress, finalize as canceled.
        if changeset.status == OpStatus.IN_PROGRESS:
            await changeset.finalize(status=OpStatus.CANCELED)
            changeset = await Changeset.get(id=changeset_id)
            await changeset.fetch_related("actor_links__actor")
        return {"status": "cancelled", "changeset": changeset.to_dict()}

    # Signal cancellation and wait briefly
    running_changeset.cancel()
    try:
        await running_changeset.wait_cancelled(timeout=10)
    except asyncio.TimeoutError:
        return {"status": "cancellation_requested"}

    latest = await Changeset.get(id=changeset_id)
    await latest.fetch_related("actor_links__actor")
    return {"status": "cancelled", "changeset": latest.to_dict()}


@router.post("/changesets")
async def create_changeset():
    return await create_changeset_api()


@router.post("/changesets/{changeset_id}/finish")
async def finish_changeset(changeset_id: int):
    return await finish_changeset_api(changeset_id)


@router.get("/changesets")
async def list_changesets():
    return await list_changesets_api()


@router.get("/changesets/{changeset_id}")
async def get_changeset(changeset_id: int, stream: bool = Query(False)):
    _ = stream
    return await get_changeset_api(changeset_id)


@router.delete("/changesets/{changeset_id}")
async def delete_changeset(changeset_id: int):
    return await delete_changeset_api(changeset_id)


@router.get("/changesets/{changeset_id}/changes")
async def list_changeset_changes(
    changeset_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
):
    return await list_changeset_changes_api(
        changeset_id,
        offset=offset,
        limit=limit,
    )


@router.patch("/changesets/{changeset_id}")
async def update_changeset(changeset_id: int, request: Request):
    payload = ChangesetUpdate.model_validate(await request.json())
    return await update_changeset_api(changeset_id, payload)


@router.get("/changesets/{changeset_id}/events")
async def stream_changeset_events(changeset_id: int):
    event_generator = await stream_changeset_events_api(changeset_id)
    return StreamingResponse(event_generator, media_type="text/event-stream")


@router.post("/changesets/{changeset_id}/cancel")
async def cancel_changeset(changeset_id: int):
    return await cancel_changeset_api(changeset_id)
