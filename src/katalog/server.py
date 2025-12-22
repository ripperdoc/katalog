import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from loguru import logger
from tortoise import Tortoise

from katalog.analyzers.runtime import run_analyzers
from katalog.config import WORKSPACE
from katalog.models import Asset, OpStatus, Provider, ProviderType, Snapshot
from katalog.queries import list_assets_with_metadata, setup
from katalog.processors.runtime import (
    DEFAULT_PROCESSOR_CONCURRENCY,
    enqueue_asset_processing,
    sort_processors,
)
from katalog.sources.runtime import run_source_snapshot


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
@app.post("/sources/{id}/run")
async def do_run_sources(
    request: Request, id: Optional[int] = None, ids: list[int] | None = Query(None)
):
    """Scan one or more sources and run processors for their assets."""
    processor_pipeline = await sort_processors()
    target_ids = set(ids or [])
    if id is not None:
        target_ids.add(int(id))

    if target_ids:
        providers = await Provider.filter(
            type=ProviderType.SOURCE, id__in=sorted(target_ids)
        ).order_by("id")
        if len(providers) != len(target_ids):
            raise HTTPException(status_code=404, detail="One or more sources not found")
    else:
        providers = await Provider.filter(type=ProviderType.SOURCE).order_by("id")

    if not providers:
        raise HTTPException(status_code=404, detail="No sources configured")

    snapshots = []
    for source_record in providers:
        snapshots.append(
            await run_source_snapshot(
                source_record=source_record,
                processor_pipeline=processor_pipeline,
                is_cancelled=request.is_disconnected,
            )
        )
    return snapshots[0] if len(snapshots) == 1 else snapshots


@app.post("/processors/run")
@app.post("/processors/{id}/run")
async def do_run_processor(id: Optional[int] = None, ids: list[int] | None = Query(None)):
    processor_pipeline = await sort_processors()
    if not processor_pipeline:
        raise HTTPException(status_code=400, detail="No processor providers configured")

    target_ids = set(ids or [])
    if id is not None:
        target_ids.add(int(id))

    assets_query = Asset.filter(deleted_snapshot_id__isnull=True)
    if target_ids:
        assets_query = assets_query.filter(id__in=sorted(target_ids))
    assets = await assets_query
    if target_ids and len(assets) != len(target_ids):
        raise HTTPException(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found to process")

    by_provider: dict[int, list[Asset]] = {}
    for asset in assets:
        by_provider.setdefault(int(asset.provider_id), []).append(asset)

    snapshots = []
    for provider_id, provider_assets in by_provider.items():
        provider_record = await Provider.get_or_none(id=provider_id)
        if provider_record is None:
            logger.warning(f"Skipping assets for missing provider {provider_id}")
            continue
        snapshots.append(
            await _run_processor_snapshot(
                provider=provider_record,
                assets=provider_assets,
                processor_pipeline=processor_pipeline,
            )
        )
    return snapshots[0] if len(snapshots) == 1 else snapshots


@app.post("/analyzers/run")
@app.post("/analyzers/{id}/run")
async def do_run_analyzers(id: Optional[int] = None):
    if id is None:
        result = await run_analyzers(None)
    else:
        result = await run_analyzers([int(id)])
    return result


async def _run_processor_snapshot(
    *, provider: Provider, assets: list[Asset], processor_pipeline
):
    """Run processors for a list of assets belonging to one provider."""
    snapshot = await Snapshot.begin(provider)
    processor_semaphore = asyncio.Semaphore(DEFAULT_PROCESSOR_CONCURRENCY)
    try:
        for asset in assets:
            snapshot.stats.assets_seen += 1
            snapshot.stats.assets_processed += 1
            initial_changes = await asset.upsert(snapshot=snapshot, metadata=None)
            await enqueue_asset_processing(
                asset=asset,
                snapshot=snapshot,
                stages=processor_pipeline,
                tasks=snapshot.tasks,
                semaphore=processor_semaphore,
                initial_changes=initial_changes,
                force_run=True,
            )
    except asyncio.CancelledError:
        logger.info(f"Processor snapshot {snapshot} canceled by client")
        for task in snapshot.tasks:
            task.cancel()
        await snapshot.finalize(status=OpStatus.CANCELED)
        raise
    except Exception:
        await snapshot.finalize(status=OpStatus.ERROR)
        raise

    await snapshot.finalize(status=OpStatus.COMPLETED)
    return snapshot


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
