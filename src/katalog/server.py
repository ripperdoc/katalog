import asyncio
import traceback
import json
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError
import tomllib
from fastapi.responses import RedirectResponse, StreamingResponse
from loguru import logger
from tortoise import Tortoise

from katalog.config import DB_URL, WORKSPACE
from katalog.models import Asset, Metadata, OpStatus, Provider, ProviderType, Snapshot
from katalog.processors.runtime import run_processors, sort_processors
from katalog.analyzers.runtime import run_analyzers
from katalog.queries import (
    list_assets_for_view,
    list_grouped_assets,
    build_group_member_filter,
    sync_config,
)
from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)
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
    plugins = refresh_plugins()
    if plugins:
        logger.info(
            "Discovered plugins ({}): {}",
            len(plugins),
            ", ".join(sorted(plugins.keys())),
        )
    else:
        logger.warning("No plugins discovered via entry points")

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
    return await list_assets_for_view(view, provider_id=provider_id)


@app.get("/assets/grouped")
async def list_grouped_assets_endpoint(
    group_by: str = Query(
        ..., description="Grouping key, e.g. 'hash/md5' or 'a.provider_id'"
    ),
    group_value: Optional[str] = Query(
        None,
        description="When set, returns members of this group value instead of the group list.",
    ),
    provider_id: Optional[int] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    filters: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
):
    """
    Grouped asset listing:
    - Without group_value: returns group aggregates (row_kind='group').
    - With group_value: returns assets within that group (row_kind='asset').
    """

    view = get_view("default")

    if group_value is None:
        return await list_grouped_assets(
            view,
            group_by=group_by,
            provider_id=provider_id,
            offset=offset,
            limit=limit,
            filters=filters,
            search=search,
            include_total=True,
        )

    extra_where = build_group_member_filter(group_by, group_value)
    members = await list_assets_for_view(
        view,
        provider_id=provider_id,
        offset=offset,
        limit=limit,
        sort=None,
        filters=filters,
        columns=None,
        search=search,
        include_total=True,
        extra_where=extra_where,
    )
    # Tag rows so UI can distinguish assets returned via grouping.
    for item in members.get("items", []):
        item["row_kind"] = "asset"
        item["group_key"] = group_by
        item["group_value"] = group_value
    members["mode"] = "members"
    members["group_by"] = group_by
    members["group_value"] = group_value
    return members


@app.post("/assets")
async def create_asset(request: Request):
    raise NotImplementedError("Direct asset creation is not supported")


@app.get("/assets/{asset_id}")
async def get_asset(asset_id: int):
    asset = await Asset.get_or_none(id=asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")

    metadata = await Metadata.for_asset(asset, include_removed=True)
    return {
        "asset": asset.to_dict(),
        "metadata": [m.to_dict() for m in metadata],
    }


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
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
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
        except Exception as exc:
            with logger.contextualize(snapshot_id=snapshot.id):
                logger.exception(f"Snapshot {snapshot.id} failed: {exc}")
            try:
                tb = traceback.format_exc()
                meta = dict(snapshot.metadata or {})
                meta["error_message"] = str(exc)
                meta["error_traceback"] = tb
                snapshot.metadata = meta
            except Exception:
                # Best-effort; don't block finalization on metadata failure.
                pass
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

    assets_query = Asset.all()
    if target_ids:
        assets_query = assets_query.filter(id__in=sorted(target_ids))
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
async def do_run_analyzers(analyzer_id: str):
    """Run all analyzers or a specific analyzer provider id."""

    target_ids: list[int] | None
    if analyzer_id.lower() == "all":
        target_ids = None
    else:
        try:
            target_ids = [int(analyzer_id)]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="analyzer_id must be an integer provider id or 'all'",
            )

    try:
        results = await run_analyzers(target_ids)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("Analyzer execution failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return {"analyzers": results}


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


class ProviderCreate(BaseModel):
    name: str = Field(min_length=1)
    plugin_id: str
    config: dict[str, Any] | None = None
    config_toml: str | None = None


class ProviderUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None
    config_toml: str | None = None


def _validate_and_normalize_config(
    plugin_cls, config: dict[str, Any] | None
) -> dict[str, Any]:
    """Validate provider config against plugin config_model (if declared) and return normalized dict."""
    config_model = getattr(plugin_cls, "config_model", None)
    if config_model is None:
        return config or {}
    try:
        model = config_model.model_validate(config or {})
    except ValidationError as exc:
        # Use JSON-serializable error payload for REST clients.
        raise HTTPException(
            status_code=400,
            detail={"message": "Invalid config", "errors": exc.errors()},
        ) from exc
    config_json = model.model_dump(mode="json", by_alias=False)
    return config_json


@app.get("/plugins")
async def list_plugins_endpoint():
    plugins = [p.to_dict() for p in refresh_plugins().values()]
    return {"plugins": plugins}


@app.post("/providers")
async def create_provider(request: Request):
    payload = ProviderCreate.model_validate(await request.json())
    spec: PluginSpec | None = get_plugin_spec(payload.plugin_id)
    if spec is None:
        spec = refresh_plugins().get(payload.plugin_id)
    if spec is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(payload.plugin_id)
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Plugin not found") from exc

    existing = await Provider.get_or_none(name=payload.name)
    if existing:
        raise HTTPException(status_code=400, detail="Provider name already exists")

    raw_config: dict[str, Any] | None = payload.config
    if payload.config_toml is not None:
        try:
            raw_config = tomllib.loads(payload.config_toml)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail={"message": "Invalid TOML", "errors": str(exc)},
            ) from exc

    normalized_config = _validate_and_normalize_config(plugin_cls, raw_config)

    provider = await Provider.create(
        name=payload.name,
        plugin_id=payload.plugin_id,
        type=spec.provider_type,
        config=normalized_config,
        config_toml=payload.config_toml,
    )
    return {"provider": provider.to_dict()}


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
async def update_provider(provider_id: int, request: Request):
    provider = await Provider.get_or_none(id=provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    payload = ProviderUpdate.model_validate(await request.json())
    if payload.name:
        provider.name = payload.name
    if payload.config is not None or payload.config_toml is not None:
        try:
            plugin_cls = get_plugin_class(provider.plugin_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=404, detail="Plugin not found") from exc
        raw_config = payload.config
        if payload.config_toml is not None:
            try:
                raw_config = tomllib.loads(payload.config_toml)
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail={"message": "Invalid TOML", "errors": str(exc)},
                ) from exc
            provider.config_toml = payload.config_toml
        provider.config = _validate_and_normalize_config(plugin_cls, raw_config)
    await provider.save()
    return {"provider": provider.to_dict()}


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
