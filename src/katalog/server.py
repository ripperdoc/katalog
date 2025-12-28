from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from loguru import logger
from tortoise import Tortoise

from katalog.analyzers.runtime import run_analyzers
from katalog.config import WORKSPACE
from katalog.models import Asset, Provider, ProviderType, Snapshot
from katalog.queries import list_assets_with_metadata, setup, sync_config
from katalog.processors.runtime import run_processors, sort_processors
from katalog.sources.runtime import run_sources


@asynccontextmanager
async def lifespan(app):
    # run startup logic
    await setup(db_path)
    try:
        yield
    finally:
        # run shutdown logic
        await Tortoise.close_connections()


app = FastAPI(lifespan=lifespan)


db_path = WORKSPACE / "katalog.db"
DATABASE_URL = f"sqlite:///{db_path}"

logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DATABASE_URL}")


# region ASSETS


@app.get("/assets")
async def list_assets(provider_id: Optional[int] = None):
    return await list_assets_with_metadata(provider_id=provider_id)


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

# region PROVIDER OPERATIONS


@app.post("/sources/run")
async def do_run_sources(request: Request, ids: list[int] | None = Query(None)):
    """Scan selected or all sources and run processors for changed assets."""
    target_ids = set(ids or [])
    await sync_config()

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
    async with Snapshot.context(provider=provider_for_snapshot) as snapshot:
        await run_sources(
            snapshot=snapshot,
            sources=sources,
            is_cancelled=request.is_disconnected,
        )
    return snapshot


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
    raise NotImplementedError()


@app.get("/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: int):
    raise NotImplementedError()


@app.patch("/snapshots/{snapshot_id}")
async def update_snapshot(snapshot_id: int):
    raise NotImplementedError()


# endregion

# region PROVIDERS


@app.post("/providers")
async def create_provider(request: Request):
    raise NotImplementedError()


@app.get("/providers")
async def list_providers():
    raise NotImplementedError()


@app.get("/providers/{provider_id}")
async def get_provider(provider_id: int):
    raise NotImplementedError()


@app.patch("/providers/{provider_id}")
async def update_provider(provider_id: int):
    raise NotImplementedError()


# endregion
