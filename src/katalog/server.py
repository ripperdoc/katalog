import asyncio
from loguru import logger

from fastapi import FastAPI, HTTPException
from typing import Any, Literal, Optional

from katalog.analyzers.runtime import run_analyzers
from katalog.sources.base import make_source_instance
from katalog.models import OpStatus, Provider, ProviderType, SnapshotStats, Snapshot
from katalog.config import WORKSPACE
from katalog.processors.runtime import (
    ProcessorTaskResult,
    process_asset,
    sort_processors,
)

app = FastAPI()


db_path = WORKSPACE / "katalog.db"
DATABASE_URL = f"sqlite:///{db_path}"

# Configure basic logging and report which database file is being used
logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DATABASE_URL}")


async def _drain_processor_tasks(tasks: list[asyncio.Task[Any]]) -> tuple[int, int]:
    if not tasks:
        return 0, 0
    results = await asyncio.gather(*tasks, return_exceptions=True)
    modified = 0
    failures = 0
    for result in results:
        if isinstance(result, Exception):
            logger.opt(exception=result).error("Processor task failed")
            failures += 1
            continue
        if isinstance(result, ProcessorTaskResult):
            if result.changes:
                modified += 1
            failures += result.failures
            continue
        if result:
            modified += 1
    tasks.clear()
    return modified, failures


@app.post("/snapshot/{provider_id}")
async def snapshot_source(provider_id: int):
    source_record = await Provider.get_or_none(id=provider_id)
    if not source_record or source_record.type != ProviderType.SOURCE:
        raise HTTPException(status_code=404, detail=f"Unknown source '{provider_id}'")

    source_plugin = make_source_instance(source_record)
    # since_snapshot = database.get_cutoff_snapshot(provider_id)
    since_snapshot = None
    processor_pipeline = await sort_processors()
    logger.info(f"Snapshotting source: {source_record}")
    snapshot = await Snapshot.begin(source_record)
    stats = SnapshotStats()
    seen = 0
    added = 0
    updated = 0
    processor_modified = 0
    processor_failed = 0
    processor_tasks: list[asyncio.Task[Any]] = []
    scan_handle = None
    try:
        scan_handle = await source_plugin.scan(since_snapshot=since_snapshot)
        async for result in scan_handle.iterator:
            # logger.debug(f"Seen asset {result.asset.id} from source {provider_id}")
            try:
                result.asset.attach_accessor(source_plugin.get_accessor(result.asset))
            except Exception:
                logger.exception(
                    f"Failed to attach accessor for record {result.asset.id} in source {provider_id}"
                )
            stats.assets_seen += 1
            changes = database.upsert_asset(
                result.asset, result.metadata, snapshot, stats=stats
            )

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
        delta_modified, delta_failed = await _drain_processor_tasks(processor_tasks)
        processor_modified += delta_modified
        processor_failed += delta_failed
        await snapshot.finalize(status=OpStatus.CANCELED, stats=stats)
        raise
    except Exception:
        delta_modified, delta_failed = await _drain_processor_tasks(processor_tasks)
        processor_modified += delta_modified
        processor_failed += delta_failed
        await snapshot.finalize(status=OpStatus.ERROR, stats=stats)
        raise
    delta_modified, delta_failed = await _drain_processor_tasks(processor_tasks)
    processor_modified += delta_modified
    processor_failed += delta_failed
    await snapshot.finalize(status=scan_handle.status, stats=stats)
    seen = stats.assets_seen
    added = stats.assets_added
    updated = stats.assets_changed
    return {
        "status": "snapshot complete",
        "source": provider_id,
        "snapshot_id": snapshot.id,
        "snapshot_status": scan_handle.status,
        "stats": {
            "seen": seen,
            "updated": updated,
            "added": added,
            "processor_modified": processor_modified,
            "processor_failed": processor_failed,
        },
        "snapshot_stats": stats.to_dict(),
    }


@app.get("/assets")
def list_assets(provider_id: Optional[int] = None):
    return database.list_records_with_metadata(
        provider_id=provider_id, view=selected_view
    )


@app.post("/analyzers/run")
async def run_all_analyzers():
    analyzers = _get_analyzers()
    if not analyzers:
        return {"status": "no analyzers configured"}
    results = await run_analyzers(database=database, analyzers=analyzers)
    return {
        "status": "analysis complete",
        "results": results,
    }
