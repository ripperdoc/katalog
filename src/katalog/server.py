import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from loguru import logger
from tortoise import Tortoise

from katalog.analyzers.runtime import run_analyzers
from katalog.config import WORKSPACE
from katalog.models import (
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    SnapshotStats,
)
from katalog.queries import list_assets_with_metadata, setup
from katalog.processors.runtime import (
    drain_processor_tasks,
    process_asset,
    sort_processors,
)
from katalog.sources.base import make_source_instance


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
async def do_run_sources(id: Optional[int] = None):
    source_record = await Provider.get_or_none(id=id)
    if not source_record or source_record.type != ProviderType.SOURCE:
        raise HTTPException(status_code=404, detail=f"Unknown source '{id}'")

    source_plugin = make_source_instance(source_record)
    # since_snapshot = database.get_cutoff_snapshot(provider_id)
    since_snapshot = None
    processor_pipeline = await sort_processors()
    logger.info(f"Snapshotting source: {source_record}")
    snapshot = await Snapshot.begin(source_record)
    stats = SnapshotStats()
    processor_tasks: list[asyncio.Task[Any]] = []
    scan_result = None
    try:
        scan_result = await source_plugin.scan(since_snapshot=since_snapshot)
        async for result in scan_result.iterator:
            # logger.debug(f"Seen asset {result.asset.id} from source {provider_id}")
            try:
                result.asset.attach_accessor(source_plugin.get_accessor(result.asset))
            except Exception:
                logger.exception(
                    f"Failed to attach accessor for record {result.asset.id} in source {id}"
                )
            stats.assets_seen += 1
            changes = await result.asset.upsert(result, snapshot, stats)

            if processor_pipeline:
                stats.assets_processed += 1
                processor_tasks.append(
                    asyncio.create_task(
                        process_asset(
                            asset=result.asset,
                            snapshot=snapshot,
                            stages=processor_pipeline,
                            initial_changes=changes,
                            stats=stats,
                        )
                    )
                )
    except asyncio.CancelledError:
        logger.info(
            f"Snapshot {snapshot} for source {source_record} canceled by client"
        )
        for task in processor_tasks:
            task.cancel()
        await drain_processor_tasks(processor_tasks)
        await snapshot.finalize(status=OpStatus.CANCELED, stats=stats)
        raise
    except Exception:
        await drain_processor_tasks(processor_tasks)
        await snapshot.finalize(status=OpStatus.ERROR, stats=stats)
        raise
    await drain_processor_tasks(processor_tasks)
    await snapshot.finalize(status=scan_result.status, stats=stats)
    return {
        "snapshot": snapshot,
        "stats": stats.to_dict(),
    }


@app.post("/processors/{id}run")
async def do_run_processor(id: Optional[int] = None):
    # if id is None:
    #     result = await run_analyzers(None)
    # else:
    #     result = await run_analyzers([int(id)])
    return result


@app.post("/analyzers/run")
@app.post("/analyzers/{id}/run")
async def do_run_analyzers(id: Optional[int] = None):
    if id is None:
        result = await run_analyzers(None)
    else:
        result = await run_analyzers([int(id)])
    return result


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
