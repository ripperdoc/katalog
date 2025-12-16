import asyncio
from loguru import logger

from fastapi import FastAPI, HTTPException
from typing import Any, Literal, Optional

from katalog.analyzers.runtime import AnalyzerEntry, load_analyzers, run_analyzers
from katalog.sources.base import SourcePlugin
from katalog.config import WORKSPACE
from katalog.db import Database
from katalog.processors.runtime import (
    ProcessorStage,
    ProcessorTaskResult,
    process as run_processors,
    sort_processors,
)
from katalog.utils.utils import import_plugin_class, load_plugin_configs

app = FastAPI()


db_path = WORKSPACE / "katalog.db"
DATABASE_URL = f"sqlite:///{db_path}"

# Configure basic logging and report which database file is being used
logger.info(f"Using workspace: {WORKSPACE}")
logger.info(f"Using database: {DATABASE_URL}")

database = Database(db_path)
database.initialize_schema()
SOURCE_CONFIGS: dict[str, dict[str, Any]] = {}
PROCESSOR_CONFIGS: list[dict[str, Any]] = []
ANALYZER_CONFIGS: list[dict[str, Any]] = []
PROCESSOR_PIPELINE: list[ProcessorStage] | None = None
ANALYZER_PIPELINE: list[AnalyzerEntry] | None = None
SOURCE_CACHE: dict[str, SourcePlugin] = {}

SOURCE_CONFIGS, PROCESSOR_CONFIGS, ANALYZER_CONFIGS = load_plugin_configs(
    database=database,
    workspace=WORKSPACE,
)


def _get_source_config_or_404(provider_id: str) -> dict[str, Any]:
    config = SOURCE_CONFIGS.get(provider_id)
    if not config:
        raise HTTPException(status_code=404, detail=f"Unknown source '{provider_id}'")
    return config


def _get_source_plugin(provider_id: str, source_cfg: dict[str, Any]) -> SourcePlugin:
    cached = SOURCE_CACHE.get(provider_id)
    if cached is not None:
        return cached
    class_path = source_cfg.get("class")
    if not class_path:
        raise ValueError(f"Source {provider_id} is missing a 'class'")
    SourceClass = import_plugin_class(class_path)
    source = SourceClass(**source_cfg)
    SOURCE_CACHE[provider_id] = source
    return source


def _get_processor_pipeline() -> list[ProcessorStage]:
    global PROCESSOR_PIPELINE
    if PROCESSOR_PIPELINE is None:
        PROCESSOR_PIPELINE = sort_processors(PROCESSOR_CONFIGS, database=database)
    return PROCESSOR_PIPELINE


def _get_analyzers() -> list[AnalyzerEntry]:
    global ANALYZER_PIPELINE
    if ANALYZER_PIPELINE is None:
        ANALYZER_PIPELINE = load_analyzers(ANALYZER_CONFIGS, database=database)
    return ANALYZER_PIPELINE


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
async def snapshot_source(provider_id: str):
    source_cfg = _get_source_config_or_404(provider_id)
    source_plugin = _get_source_plugin(provider_id, source_cfg)
    since_snapshot = database.get_latest_snapshot(provider_id)
    processor_pipeline = _get_processor_pipeline()
    logger.info("Snapshotting source: {}", provider_id)
    snapshot = database.begin_snapshot(provider_id)
    seen = 0
    added = 0
    updated = 0
    processor_modified = 0
    processor_failed = 0
    processor_tasks: list[asyncio.Task[Any]] = []
    try:
        scan_handle = await source_plugin.scan(since_snapshot=since_snapshot)
        async for result in scan_handle.iterator:
            try:
                result.asset.attach_accessor(source_plugin.get_accessor(result.asset))
            except Exception:
                logger.exception(
                    "Failed to attach accessor for record {} in source {}",
                    result.asset.id,
                    provider_id,
                )
            changes = database.upsert_asset(result.asset, result.metadata, snapshot)
            seen += 1
            if "asset" in changes:
                added += 1
            if changes:
                updated += 1
            if processor_pipeline:
                processor_tasks.append(
                    asyncio.create_task(
                        run_processors(
                            record=result.asset,
                            snapshot=snapshot,
                            database=database,
                            stages=processor_pipeline,
                            initial_changes=changes,
                        )
                    )
                )
    except Exception:
        delta_modified, delta_failed = await _drain_processor_tasks(processor_tasks)
        processor_modified += delta_modified
        processor_failed += delta_failed
        database.finalize_snapshot(snapshot, status="failed")
        raise
    delta_modified, delta_failed = await _drain_processor_tasks(processor_tasks)
    processor_modified += delta_modified
    processor_failed += delta_failed
    final_status = "partial" if since_snapshot else "full"
    database.finalize_snapshot(snapshot, status=final_status)
    return {
        "status": "snapshot complete",
        "source": provider_id,
        "snapshot_id": snapshot.id,
        "snapshot_status": final_status,
        "stats": {
            "seen": seen,
            "updated": updated,
            "added": added,
            "processor_modified": processor_modified,
            "processor_failed": processor_failed,
        },
    }


@app.get("/records")
def list_files(provider_id: Optional[str] = None, view: str = "flat"):
    if view not in {"flat", "complete"}:
        raise ValueError("view must be 'flat' or 'complete'")
    selected_view: Literal["flat", "complete"] = (
        "flat" if view == "flat" else "complete"
    )
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
