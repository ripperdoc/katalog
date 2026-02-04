import asyncio
import json

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from katalog.models import Changeset, OpStatus
from katalog.db.changesets import get_changeset_repo
from katalog.utils.changeset_events import sse_event

from katalog.editors.user_editor import ensure_user_editor
from katalog.api.state import RUNNING_CHANGESETS, event_manager
from katalog.api.helpers import ApiError
from katalog.api.schemas import ChangesetChangesResponse

router = APIRouter()


class ChangesetUpdate(BaseModel):
    message: str | None = Field(default=None)


async def create_changeset() -> Changeset:
    actor = await ensure_user_editor()
    db = get_changeset_repo()
    try:
        changeset = await db.begin(
            actors=[actor],
            message="Manual edit",
            status=OpStatus.IN_PROGRESS,
            data={"manual": True},
        )
    except ValueError as exc:
        raise ApiError(status_code=409, detail=str(exc))
    return changeset


async def finish_changeset(changeset_id: int) -> Changeset:
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")
    await changeset.finalize(status=OpStatus.COMPLETED)
    latest = await db.get(id=changeset_id)
    await db.load_actor_ids(latest)
    return latest


async def list_changesets() -> list[Changeset]:
    db = get_changeset_repo()
    changesets = await db.list_rows(order_by="id DESC")
    for changeset in changesets:
        await db.load_actor_ids(changeset)
    return changesets


async def get_changeset(
    changeset_id: int,
) -> tuple[Changeset, list[str], bool]:
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    await db.load_actor_ids(changeset)

    return (
        changeset,
        event_manager.get_buffer(changeset_id),
        changeset.status == OpStatus.IN_PROGRESS,
    )


async def delete_changeset(changeset_id: int) -> dict[str, int | str]:
    """Undo a changeset by deleting it (cascade removes related rows)."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    await db.delete(changeset)
    return {"status": "deleted", "changeset_id": changeset_id}


async def list_changeset_changes(
    changeset_id: int,
    offset: int = 0,
    limit: int = 200,
) -> ChangesetChangesResponse:
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    try:
        return await db.list_changeset_metadata_changes(
            changeset_id, offset=offset, limit=limit, include_total=True
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


async def update_changeset(changeset_id: int, payload: ChangesetUpdate) -> Changeset:
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    if payload.message is not None:
        changeset.message = payload.message
        await db.save(changeset)
    await db.load_actor_ids(changeset)
    return changeset


async def stream_changeset_events(changeset_id: int):
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    history, queue = event_manager.subscribe(changeset_id)
    run_state = RUNNING_CHANGESETS.get(changeset_id)
    done_event = (
        run_state.done_event if run_state and run_state.done_event else asyncio.Event()
    )
    if run_state is None and changeset.status != OpStatus.IN_PROGRESS:
        done_event.set()
    poll_task: asyncio.Task | None = None
    if run_state is None and changeset.status == OpStatus.IN_PROGRESS:
        async def _poll_status() -> None:
            while not done_event.is_set():
                await asyncio.sleep(1.0)
                latest = await db.get(id=changeset_id)
                if latest.status != OpStatus.IN_PROGRESS:
                    done_event.set()
                    break

        poll_task = asyncio.create_task(_poll_status())

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
                    latest = await db.get(id=changeset_id)
                    await db.load_actor_ids(latest)
                    payload = latest.model_dump(mode="json")
                    yield sse_event("changeset", json.dumps(payload))
                    break
        finally:
            event_manager.unsubscribe(changeset_id, queue)
            if poll_task is not None:
                poll_task.cancel()

    return event_generator()


async def cancel_changeset(
    changeset_id: int,
) -> tuple[str, Changeset | None]:
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    running_changeset = RUNNING_CHANGESETS.get(changeset_id)
    if running_changeset is None or (
        running_changeset.done_event and running_changeset.done_event.is_set()
    ):
        # Nothing running (maybe after restart). If still in progress, finalize as canceled.
        if changeset.status == OpStatus.IN_PROGRESS:
            await changeset.finalize(status=OpStatus.CANCELED)
            changeset = await db.get(id=changeset_id)
            await db.load_actor_ids(changeset)
        return "cancelled", changeset

    # Signal cancellation and wait briefly
    running_changeset.cancel()
    try:
        await running_changeset.wait_cancelled(timeout=10)
    except asyncio.TimeoutError:
        return "cancellation_requested", None

    latest = await db.get(id=changeset_id)
    await db.load_actor_ids(latest)
    return "cancelled", latest


@router.post("/changesets/{changeset_id}/cancel")
async def cancel_changeset_rest(changeset_id: int):
    status, changeset = await cancel_changeset(changeset_id)
    return {"status": status, "changeset": changeset}


@router.post("/changesets")
async def create_changeset_rest():
    changeset = await create_changeset()
    return {"changeset": changeset}


@router.post("/changesets/{changeset_id}/finish")
async def finish_changeset_rest(changeset_id: int):
    changeset = await finish_changeset(changeset_id)
    return {"changeset": changeset}


@router.get("/changesets")
async def list_changesets_rest():
    changesets = await list_changesets()
    return {"changesets": changesets}


@router.get("/changesets/{changeset_id}")
async def get_changeset_rest(changeset_id: int, stream: bool = Query(False)):
    _ = stream
    changeset, logs, running = await get_changeset(changeset_id)
    return {"changeset": changeset, "logs": logs, "running": running}


@router.delete("/changesets/{changeset_id}")
async def delete_changeset_rest(changeset_id: int):
    return await delete_changeset(changeset_id)


@router.get("/changesets/{changeset_id}/changes")
async def list_changeset_changes_rest(
    changeset_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
):
    return await list_changeset_changes(
        changeset_id,
        offset=offset,
        limit=limit,
    )


@router.patch("/changesets/{changeset_id}")
async def update_changeset_rest(changeset_id: int, request: Request):
    payload = ChangesetUpdate.model_validate(await request.json())
    changeset = await update_changeset(changeset_id, payload)
    return {"changeset": changeset}


@router.get("/changesets/{changeset_id}/events")
async def stream_changeset_events_rest(changeset_id: int):
    event_generator = await stream_changeset_events(changeset_id)
    return StreamingResponse(event_generator, media_type="text/event-stream")
