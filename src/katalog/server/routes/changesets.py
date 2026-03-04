from typing import Literal

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from katalog.api.changesets import (
    ChangesetUpdate,
    cancel_changeset,
    create_changeset,
    delete_changeset,
    finish_changeset,
    get_changeset,
    list_changeset_changes,
    list_changeset_diff,
    list_changesets,
    stream_changeset_events,
    update_changeset,
)

router = APIRouter()


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
    view: Literal["raw", "diff"] = Query("raw"),
    offset: int = Query(0, ge=0),
    limit: int = Query(200, ge=1, le=1000),
    from_changeset_id: int | None = Query(None, ge=1),
    to_changeset_id: int | None = Query(None, ge=1),
    sort: list[str] | None = Query(None),
    filters: list[str] | None = Query(None),
    search: str | None = Query(None),
):
    if view == "diff":
        return await list_changeset_diff(
            changeset_id,
            offset=offset,
            limit=limit,
            from_changeset_id=from_changeset_id,
            to_changeset_id=to_changeset_id,
            sort=sort,
            filters=filters,
            search=search,
        )
    return await list_changeset_changes(
        changeset_id,
        offset=offset,
        limit=limit,
        from_changeset_id=from_changeset_id,
        to_changeset_id=to_changeset_id,
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
