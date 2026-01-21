import asyncio
import traceback
import json
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError
from fastapi.responses import RedirectResponse, StreamingResponse
from loguru import logger
from tortoise import Tortoise

from katalog.config import DB_URL, WORKSPACE
from katalog.models import (
    Asset,
    AssetCollection,
    CollectionItem,
    CollectionRefreshMode,
    MetadataKey,
    make_metadata,
    MetadataChangeSet,
    Metadata,
    OpStatus,
    Provider,
    ProviderType,
    Changeset,
)
from katalog.processors.runtime import run_processors, sort_processors
from katalog.analyzers.runtime import run_analyzers
from katalog.queries import (
    list_assets_for_view,
    list_grouped_assets,
    build_group_member_filter,
    sync_config,
    list_changeset_metadata_changes,
)
from katalog.metadata import editable_metadata_schema, METADATA_REGISTRY_BY_ID
from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)
from katalog.sources.user_editor import UserEditorSource
from katalog.sources.runtime import get_source_plugin, run_sources
from katalog.utils.changeset_events import (
    ChangesetEventManager,
    ChangesetRunState,
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
        # Best-effort cancel running changeset tasks on shutdown to avoid reload hangs.
        cancel_waits: list[asyncio.Task] = []
        for state in list(RUNNING_CHANGESETS.values()):
            try:
                state.cancel_event.set()
                state.task.cancel()
                cancel_waits.append(asyncio.create_task(state.done_event.wait()))
            except Exception:
                logger.exception("Failed to cancel changeset task on shutdown")
        if cancel_waits:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*cancel_waits, return_exceptions=True), timeout=5
                )
            except Exception:
                logger.warning(
                    "Timeout while waiting for changeset tasks to cancel during shutdown"
                )
        # run shutdown logic
        await Tortoise.close_connections()


app = FastAPI(lifespan=lifespan)

event_manager = ChangesetEventManager()

RUNNING_CHANGESETS: dict[int, ChangesetRunState] = {}

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


@app.post("/assets/{asset_id}/manual-edit")
async def manual_edit_asset(asset_id: int, request: Request):
    payload = await request.json()
    changeset_id = payload.get("changeset_id")
    if changeset_id is None:
        raise HTTPException(status_code=400, detail="changeset_id is required")

    changeset = await Changeset.get_or_none(id=int(changeset_id))
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise HTTPException(status_code=409, detail="Changeset is not in progress")

    asset = await Asset.get_or_none(id=asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail="Asset not found")

    provider = await _ensure_manual_provider()

    # Build metadata from payload (dict of key -> value)
    metadata_entries: list[Metadata] = []
    for key, value in payload.get("metadata", {}).items():
        try:
            mk = MetadataKey(key)
            md = make_metadata(mk, value, provider_id=provider.id)
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail=f"Invalid metadata {key}: {exc}"
            )
        md.asset = asset
        md.changeset = changeset
        metadata_entries.append(md)

    # Apply changes
    loaded = await asset.load_metadata()
    change_set = MetadataChangeSet(loaded=loaded, staged=metadata_entries)
    changed_keys = await change_set.persist(asset=asset, changeset=changeset)

    return {
        "asset_id": asset_id,
        "changeset_id": changeset.id,
        "changed_keys": [str(k) for k in changed_keys],
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

# region COLLECTIONS


class CollectionCreate(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None
    asset_ids: list[int] = Field(default_factory=list)
    source: dict[str, Any] | None = None
    refresh_mode: str | CollectionRefreshMode | None = None


class CollectionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    refresh_mode: str | CollectionRefreshMode | None = None


@app.get("/collections")
async def list_collections():
    collections = await AssetCollection.all().order_by("-created_at")
    result = []
    for col in collections:
        count = await CollectionItem.filter(collection_id=col.id).count()
        result.append(col.to_dict(asset_count=count))
    return {"collections": result}


@app.post("/collections")
async def create_collection(request: Request):
    payload = CollectionCreate.model_validate(await request.json())

    try:
        asset_ids = [int(a) for a in payload.asset_ids]
    except Exception:
        raise HTTPException(status_code=400, detail="asset_ids must be integers")

    existing = await AssetCollection.get_or_none(name=payload.name)
    if existing:
        raise HTTPException(status_code=400, detail="Collection name already exists")

    refresh_mode = payload.refresh_mode or CollectionRefreshMode.ON_DEMAND
    if isinstance(refresh_mode, str):
        try:
            refresh_mode = CollectionRefreshMode(refresh_mode)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="refresh_mode must be 'live' or 'on_demand'",
            )

    # Validate asset ids exist
    found_ids = await Asset.filter(id__in=asset_ids).values_list("id", flat=True)
    missing = set(asset_ids) - set(int(a) for a in found_ids)
    if missing:
        raise HTTPException(
            status_code=404,
            detail=f"Assets not found: {sorted(missing)}",
        )

    collection = await AssetCollection.create(
        name=payload.name,
        description=payload.description,
        source=payload.source,
        refresh_mode=refresh_mode,
    )
    # Bulk insert membership
    items = [
        CollectionItem(collection_id=collection.id, asset_id=aid) for aid in asset_ids
    ]
    if items:
        await CollectionItem.bulk_create(items, ignore_conflicts=True)
    count = len(items)
    return {"collection": collection.to_dict(asset_count=count)}


@app.get("/collections/{collection_id}")
async def get_collection(collection_id: int):
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    count = await CollectionItem.filter(collection_id=collection.id).count()
    return {"collection": collection.to_dict(asset_count=count)}


@app.patch("/collections/{collection_id}")
async def update_collection(collection_id: int, request: Request):
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")

    payload = CollectionUpdate.model_validate(await request.json())

    if payload.name:
        existing = await AssetCollection.get_or_none(name=payload.name)
        if existing and existing.id != collection.id:
            raise HTTPException(
                status_code=400, detail="Collection name already exists"
            )
        collection.name = payload.name

    if payload.description is not None:
        collection.description = payload.description

    if payload.refresh_mode:
        try:
            collection.refresh_mode = (
                CollectionRefreshMode(payload.refresh_mode)
                if isinstance(payload.refresh_mode, str)
                else payload.refresh_mode
            )
        except Exception:
            raise HTTPException(
                status_code=400, detail="refresh_mode must be 'live' or 'on_demand'"
            )

    await collection.save()
    count = await CollectionItem.filter(collection_id=collection.id).count()
    return {"collection": collection.to_dict(asset_count=count)}


@app.get("/collections/{collection_id}/assets")
async def list_collection_assets(
    collection_id: int,
    view_id: str = Query("default"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    columns: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
):
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")

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

    extra_where = (
        "a.id IN (SELECT asset_id FROM collectionitem ci WHERE ci.collection_id = ?)",
        [collection.id],
    )

    try:
        return await list_assets_for_view(
            view,
            offset=offset,
            limit=limit,
            sort=sort_tuple,
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
            include_total=True,
            extra_where=extra_where,
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
    provider_for_changeset = None
    if len(sources) == 1:
        provider_for_changeset = sources[0]
    changeset = await Changeset.begin(
        provider=provider_for_changeset, status=OpStatus.IN_PROGRESS
    )

    cancel_event = asyncio.Event()
    done_event = asyncio.Event()

    async def is_cancelled() -> bool:
        return cancel_event.is_set()

    async def runner():
        try:
            with logger.contextualize(changeset_id=changeset.id):
                logger.info(f"Starting scan for changeset {changeset.id}")
                await run_sources(
                    changeset=changeset,
                    sources=sources,
                    is_cancelled=is_cancelled,
                )
            await changeset.finalize(status=OpStatus.COMPLETED)
        except asyncio.CancelledError:
            with logger.contextualize(changeset_id=changeset.id):
                logger.info(f"Cancelled changeset {changeset.id}")
            try:
                await changeset.finalize(status=OpStatus.CANCELED)
            finally:
                raise
        except Exception as exc:
            with logger.contextualize(changeset_id=changeset.id):
                logger.exception(f"Changeset {changeset.id} failed: {exc}")
            try:
                tb = traceback.format_exc()
                meta = dict(changeset.metadata or {})
                meta["error_message"] = str(exc)
                meta["error_traceback"] = tb
                changeset.metadata = meta
            except Exception:
                # Best-effort; don't block finalization on metadata failure.
                pass
            await changeset.finalize(status=OpStatus.ERROR)
        finally:
            done_event.set()
            RUNNING_CHANGESETS.pop(changeset.id, None)

    task = asyncio.create_task(runner())
    state = ChangesetRunState(
        changeset=changeset, task=task, cancel_event=cancel_event, done_event=done_event
    )
    task.add_done_callback(lambda _: done_event.set())
    RUNNING_CHANGESETS[changeset.id] = state

    return changeset.to_dict()


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

    async with Changeset.context() as changeset:
        return await run_processors(changeset=changeset, assets=assets)
    return changeset


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

# region CHANGESETS


@app.post("/changesets")
async def create_changeset(request: Request):
    raise NotImplementedError("Not supported to create changesets directly")


@app.post("/changesets/manual/start")
async def start_manual_changeset():
    provider = await _ensure_manual_provider()
    try:
        changeset = await Changeset.begin(
            provider=provider, status=OpStatus.IN_PROGRESS
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return changeset.to_dict()


@app.post("/changesets/{changeset_id}/finish")
async def finish_changeset(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise HTTPException(status_code=409, detail="Changeset is not in progress")
    await changeset.finalize(status=OpStatus.COMPLETED)
    return {"changeset": (await Changeset.get(id=changeset_id)).to_dict()}


@app.get("/changesets")
async def list_changesets():
    changesets = (
        await Changeset.all().order_by("-started_at").prefetch_related("provider")
    )
    return {"changesets": [s.to_dict() for s in changesets]}


@app.get("/changesets/{changeset_id}")
async def get_changeset(changeset_id: int, stream: bool = Query(False)):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    await changeset.fetch_related("provider")

    if stream:
        return await stream_changeset_events(changeset_id)
    return {
        "changeset": changeset.to_dict(),
        "logs": event_manager.get_buffer(changeset_id),
        "running": changeset.status == OpStatus.IN_PROGRESS,
    }


@app.delete("/changesets/{changeset_id}")
async def delete_changeset(changeset_id: int):
    """Undo a changeset by deleting it (cascade removes related rows)."""
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")

    await changeset.delete()
    return {"status": "deleted", "changeset_id": changeset_id}


@app.get("/changesets/{changeset_id}/changes")
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


@app.patch("/changesets/{changeset_id}")
async def update_changeset(changeset_id: int):
    raise NotImplementedError()


@app.get("/changesets/{changeset_id}/events")
async def stream_changeset_events(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")

    await changeset.fetch_related("provider")
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
                    await latest.fetch_related("provider")
                    yield sse_event("changeset", json.dumps(latest.to_dict()))
                    break
        finally:
            event_manager.unsubscribe(changeset_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/changesets/{changeset_id}/cancel")
async def cancel_changeset(changeset_id: int):
    changeset = await Changeset.get_or_none(id=changeset_id)
    if changeset is None:
        raise HTTPException(status_code=404, detail="Changeset not found")
    run_state = RUNNING_CHANGESETS.get(changeset_id)
    if run_state is None or run_state.done_event.is_set():
        # Nothing running: finalize as CANCELED
        await changeset.finalize(status=OpStatus.CANCELED)
        latest = await Changeset.get(id=changeset_id)
        await latest.fetch_related("provider")
        return {"status": "cancelled", "changeset": latest.to_dict()}

    run_state.cancel_event.set()
    for task in list(run_state.changeset.tasks):
        task.cancel()
    run_state.task.cancel()

    return {"status": "cancellation_requested"}


# endregion

# region PROVIDERS


class ProviderCreate(BaseModel):
    name: str = Field(min_length=1)
    plugin_id: str
    config: dict[str, Any] | None = None


class ProviderUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None


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


async def _ensure_manual_provider() -> Provider:
    """Return the first Provider configured with the UserEditorSource plugin."""
    provider = await Provider.get_or_none(plugin_id=UserEditorSource.plugin_id)
    if provider is None:
        raise HTTPException(
            status_code=400,
            detail="No manual edit provider configured. Create a provider using the UserEditorSource plugin.",
        )
    return provider


@app.get("/plugins")
async def list_plugins_endpoint():
    plugins = [p.to_dict() for p in refresh_plugins().values()]
    return {"plugins": plugins}


def _config_schema_for_plugin(plugin_id: str) -> dict[str, Any]:
    spec: PluginSpec | None = get_plugin_spec(plugin_id) or refresh_plugins().get(
        plugin_id
    )
    if spec is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(plugin_id)
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Plugin not found") from exc
    config_model = getattr(plugin_cls, "config_model", None)
    if config_model is None:
        return {"schema": {"type": "object", "properties": {}}}
    return {"schema": config_model.model_json_schema()}


@app.get("/plugins/{plugin_id}/config/schema")
async def get_plugin_config_schema(plugin_id: str):
    return _config_schema_for_plugin(plugin_id)


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

    normalized_config = _validate_and_normalize_config(plugin_cls, raw_config)

    provider = await Provider.create(
        name=payload.name,
        plugin_id=payload.plugin_id,
        type=spec.provider_type,
        config=normalized_config,
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
    changesets = await Changeset.filter(provider=provider).order_by("-started_at")
    for changeset in changesets:
        await changeset.fetch_related("provider")
    return {
        "provider": provider.to_dict(),
        "changesets": [s.to_dict() for s in changesets],
    }


@app.get("/providers/{provider_id}/config/schema")
async def get_provider_config_schema(provider_id: int):
    provider = await Provider.get_or_none(id=provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    schema_payload = _config_schema_for_plugin(provider.plugin_id)
    return {**schema_payload, "value": provider.config or {}}


@app.patch("/providers/{provider_id}")
async def update_provider(provider_id: int, request: Request):
    provider = await Provider.get_or_none(id=provider_id)
    if provider is None:
        raise HTTPException(status_code=404, detail="Provider not found")
    payload = ProviderUpdate.model_validate(await request.json())
    if payload.name:
        provider.name = payload.name
    if payload.config is not None:
        try:
            plugin_cls = get_plugin_class(provider.plugin_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=404, detail="Plugin not found") from exc
        provider.config = _validate_and_normalize_config(plugin_cls, payload.config)
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
@app.get("/metadata/schema/editable")
async def metadata_schema_editable():
    """Return JSON schema for editable metadata (non-asset/ keys)."""
    schema, ui_schema = editable_metadata_schema()
    return {"schema": schema, "uiSchema": ui_schema}


@app.get("/metadata/registry")
async def metadata_registry():
    """Return metadata registry keyed by registry id."""
    return {
        "registry": {
            key: value.to_dict() for key, value in METADATA_REGISTRY_BY_ID.items()
        }
    }
