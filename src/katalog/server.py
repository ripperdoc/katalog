import asyncio
import json
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from loguru import logger
from tortoise import Tortoise

from katalog.config import DB_URL, WORKSPACE
from katalog.models import Asset, OpStatus, Provider, ProviderType, Snapshot
from katalog.processors.runtime import run_processors, sort_processors
from katalog.queries import list_assets_for_view, sync_config
from katalog.sources.runtime import get_source_plugin, run_sources
from katalog.utils.snapshot_events import (
    SnapshotEventManager,
    SnapshotRunState,
    sse_event,
)
from katalog.views import get_view, list_views

logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DB_URL}")


@asynccontextmanager
async def lifespan(app):
    # run startup logic
    await sync_config()
    event_manager.bind_loop(asyncio.get_running_loop())
    event_manager.ensure_sink()
    try:
        yield
    finally:
        # run shutdown logic
        await Tortoise.close_connections()


app = FastAPI(lifespan=lifespan)

event_manager = SnapshotEventManager()

RUNNING_SNAPSHOTS: dict[int, SnapshotRunState] = {}

# region ASSETS


@app.get("/assets")
async def list_assets(provider_id: Optional[int] = None):
    view = get_view("default")
    return await list_assets_for_view(
        view,
        provider_id=provider_id,
    )


@app.post("/assets")
async def create_asset(request: Request):
    raise NotImplementedError("Direct asset creation is not supported")


@app.get("/assets/{asset_id}")
async def get_asset(asset_id: int):
    raise NotImplementedError()


@app.patch("/assets/{asset_id}")
async def update_asset(asset_id: int):
    raise NotImplementedError()


# endregion

# region DATA VIEWS


@app.get("/views")
async def list_views_endpoint():
    return {"views": [v.to_dict() for v in list_views()]}


@app.get("/views/{view_id}")
async def get_view_endpoint(view_id: str):
    try:
        view = get_view(view_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="View not found")
    return {"view": view.to_dict()}


@app.get("/views/{view_id}/assets")
async def list_assets_for_view_endpoint(
    view_id: str,
    provider_id: Optional[int] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    columns: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
):
    if search:
        raise HTTPException(status_code=400, detail="Search is not yet supported")
    if filters:
        raise HTTPException(status_code=400, detail="Filtering is not yet supported")
    try:
        view = get_view(view_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="View not found")

    sort_tuple: tuple[str, str] | None = None
    if sort:
        if ":" in sort:
            col, direction = sort.split(":", 1)
        else:
            col, direction = sort, "asc"
        sort_tuple = (col, direction)

    try:
        return await list_assets_for_view(
            view,
            provider_id=provider_id,
            offset=offset,
            limit=limit,
            sort=sort_tuple,
            columns=set(columns) if columns else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# endregion

# region PROVIDER OPERATIONS


@app.post("/sources/run")
async def do_run_sources(request: Request, ids: list[int] | None = Query(None)):
    """Scan selected or all sources and run processors for changed assets."""
    target_ids = set(ids or [])

    if target_ids:
        sources = await Provider.filter(
            type=ProviderType.SOURCE, id__in=sorted(target_ids)
        ).order_by("id")
        if len(sources) != len(target_ids):
            raise HTTPException(status_code=404, detail="One or more sources not found")
    else:
        sources = await Provider.filter(type=ProviderType.SOURCE).order_by("id")

    if not sources:
        raise HTTPException(status_code=404, detail="No sources configured")
    provider_for_snapshot = None
    if len(sources) == 1:
        provider_for_snapshot = sources[0]
    snapshot = await Snapshot.begin(
        provider=provider_for_snapshot, status=OpStatus.IN_PROGRESS
    )

    cancel_event = asyncio.Event()
    done_event = asyncio.Event()

    async def is_cancelled() -> bool:
        return cancel_event.is_set()

    async def runner():
        try:
            with logger.contextualize(snapshot_id=snapshot.id):
                logger.info(f"Starting scan for snapshot {snapshot.id}")
                await run_sources(
                    snapshot=snapshot,
                    sources=sources,
                    is_cancelled=is_cancelled,
                )
            await snapshot.finalize(status=OpStatus.COMPLETED)
        except asyncio.CancelledError:
            with logger.contextualize(snapshot_id=snapshot.id):
                logger.info(f"Cancelled snapshot {snapshot.id}")
            try:
                await snapshot.finalize(status=OpStatus.CANCELED)
            finally:
                raise
        except Exception:
            with logger.contextualize(snapshot_id=snapshot.id):
                logger.exception(f"Snapshot {snapshot.id} failed")
            await snapshot.finalize(status=OpStatus.ERROR)
        finally:
            done_event.set()
            RUNNING_SNAPSHOTS.pop(snapshot.id, None)

    task = asyncio.create_task(runner())
    state = SnapshotRunState(
        snapshot=snapshot, task=task, cancel_event=cancel_event, done_event=done_event
    )
    task.add_done_callback(lambda _: done_event.set())
    RUNNING_SNAPSHOTS[snapshot.id] = state

    return snapshot.to_dict()


@app.post("/processors/run")
async def do_run_processor(ids: list[int] | None = Query(None)):
    processor_pipeline = await sort_processors()
    if not processor_pipeline:
        raise HTTPException(status_code=400, detail="No processor providers configured")

    target_ids = set(ids or [])

    assets_query = Asset.filter(deleted_snapshot_id__isnull=True)
    if target_ids:
        assets_query = assets_query.filter(id__in=sorted(target_ids))
    # TODO this can be slow if there are many assets
    assets = await assets_query
    if target_ids and len(assets) != len(target_ids):
        raise HTTPException(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found to process")

    async with Snapshot.context() as snapshot:
        return await run_processors(snapshot=snapshot, assets=assets)
    return snapshot


@app.post("/analyzers/{analyzer_id}/run")
async def do_run_analyzers(asset_ids: list[int] | None = Query(None)):
    target_ids = set(asset_ids or [])

    assets_query = Asset.filter(deleted_snapshot_id__isnull=True)
    if target_ids:
        assets_query = assets_query.filter(id__in=sorted(target_ids))
    # TODO this can be slow if there are many assets
    assets = await assets_query
    if target_ids and len(assets) != len(target_ids):
        raise HTTPException(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found to process")

    raise NotImplementedError("Analyzer execution not yet implemented")


# endregion

# region SNAPSHOTS


@app.post("/snapshots")
async def create_snapshot(request: Request):
    raise NotImplementedError("Not supported to create snapshots directly")


@app.get("/snapshots")
async def list_snapshots():
    snapshots = (
        await Snapshot.all().order_by("-started_at").prefetch_related("provider")
    )
    return {"snapshots": [s.to_dict() for s in snapshots]}


@app.get("/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: int, stream: bool = Query(False)):
    snapshot = await Snapshot.get_or_none(id=snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    await snapshot.fetch_related("provider")

    if stream:
        return await stream_snapshot_events(snapshot_id)
    return {
        "snapshot": snapshot.to_dict(),
        "logs": event_manager.get_buffer(snapshot_id),
        "running": snapshot.status == OpStatus.IN_PROGRESS,
    }


@app.patch("/snapshots/{snapshot_id}")
async def update_snapshot(snapshot_id: int):
    raise NotImplementedError()


@app.get("/snapshots/{snapshot_id}/events")
async def stream_snapshot_events(snapshot_id: int):
    snapshot = await Snapshot.get_or_none(id=snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    await snapshot.fetch_related("provider")
    history, queue = event_manager.subscribe(snapshot_id)
    run_state = RUNNING_SNAPSHOTS.get(snapshot_id)
    done_event = run_state.done_event if run_state else asyncio.Event()
    if run_state is None and snapshot.status != OpStatus.IN_PROGRESS:
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
                    latest = await Snapshot.get(id=snapshot_id)
                    await latest.fetch_related("provider")
                    yield sse_event("snapshot", json.dumps(latest.to_dict()))
                    break
        finally:
            event_manager.unsubscribe(snapshot_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/snapshots/{snapshot_id}/cancel")
async def cancel_snapshot(snapshot_id: int):
    snapshot = await Snapshot.get_or_none(id=snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    run_state = RUNNING_SNAPSHOTS.get(snapshot_id)
    if run_state is None or run_state.done_event.is_set():
        if snapshot.status != OpStatus.IN_PROGRESS:
            return {"status": "not_running", "snapshot": snapshot.to_dict()}
        raise HTTPException(
            status_code=409, detail="Snapshot is marked in progress but not running"
        )

    run_state.cancel_event.set()
    for task in list(run_state.snapshot.tasks):
        task.cancel()
    run_state.task.cancel()

    return {"status": "cancellation_requested"}


# endregion

# region PROVIDERS


@app.post("/providers")
async def create_provider(request: Request):
    raise NotImplementedError()


@app.get("/providers")
async def list_providers():
    providers = await Provider.all().order_by("id")
    return {"providers": [p.to_dict() for p in providers]}


@app.get("/providers/{provider_id}")
async def get_provider(provider_id: int):
    provider = await Provider.get_or_none(id=provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    snapshots = await Snapshot.filter(provider=provider).order_by("-started_at")
    for snapshot in snapshots:
        await snapshot.fetch_related("provider")
    return {
        "provider": provider.to_dict(),
        "snapshots": [s.to_dict() for s in snapshots],
    }


@app.patch("/providers/{provider_id}")
async def update_provider(provider_id: int):
    raise NotImplementedError()


# endregion

# region OTHER


@app.post("/auth/{provider}")
async def auth_callback(provider: int, request: Request):
    get_source_plugin(provider).authorize(authorization_response=request.url)
    return RedirectResponse(url="/", status_code=303)


@app.post("/sync")
async def sync():
    """Requests to sync config"""
    await sync_config()

    return {"status": "ok"}


# endregion
