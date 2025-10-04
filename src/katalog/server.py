import asyncio
import tomllib
from loguru import logger

from fastapi import FastAPI, HTTPException
from typing import Any, Literal, Optional

from katalog.analyzers.runtime import AnalyzerEntry, load_analyzers, run_analyzers
from katalog.clients.base import SourceClient
from katalog.config import WORKSPACE
from katalog.db import Database
from katalog.processors.runtime import (
    ProcessorStage,
    process as run_processors,
    sort_processors,
)
from katalog.utils.utils import import_client_class

app = FastAPI()


db_path = WORKSPACE / "katalog.db"
DATABASE_URL = f"sqlite:///{db_path}"

CONFIG_PATH = WORKSPACE / "katalog.toml"

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
CLIENT_CACHE: dict[str, SourceClient] = {}


def _load_workspace_config() -> tuple[
    dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]
]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing katalog config: {CONFIG_PATH}")
    with CONFIG_PATH.open("rb") as f:
        config = tomllib.load(f)
    sources = config.get("sources", []) or []
    result: dict[str, dict[str, Any]] = {}
    for raw in sources:
        source_id = raw.get("id")
        if not source_id:
            raise ValueError("Each source entry must define an 'id'.")
        if source_id in result:
            raise ValueError(f"Duplicate source id '{source_id}' in {CONFIG_PATH}")
        result[source_id] = raw
    processors = config.get("processors", []) or []
    analyzers = config.get("analyzers", []) or []
    return result, processors, analyzers


def _import_source_class(source_cfg: dict[str, Any]):
    class_path = source_cfg.get("class")
    if not class_path:
        raise ValueError(f"Source {source_cfg.get('id')} is missing a 'class'")
    return import_client_class(class_path)


def _plugin_id_for(source_cfg: dict[str, Any]) -> str:
    SourceClass = _import_source_class(source_cfg)
    return getattr(SourceClass, "PLUGIN_ID", SourceClass.__module__)


def _ensure_sources_registered() -> None:
    for source_id, cfg in SOURCE_CONFIGS.items():
        plugin_id = _plugin_id_for(cfg)
        database.ensure_source(
            source_id,
            title=cfg.get("title"),
            plugin_id=plugin_id,
            config=cfg,
        )


def _get_source_config_or_404(source_id: str) -> dict[str, Any]:
    config = SOURCE_CONFIGS.get(source_id)
    if not config:
        raise HTTPException(status_code=404, detail=f"Unknown source '{source_id}'")
    return config


def _get_client(source_id: str, source_cfg: dict[str, Any]) -> SourceClient:
    cached = CLIENT_CACHE.get(source_id)
    if cached is not None:
        return cached
    SourceClass = _import_source_class(source_cfg)
    client = SourceClass(**source_cfg)
    CLIENT_CACHE[source_id] = client
    return client


def _get_processor_pipeline() -> list[ProcessorStage]:
    global PROCESSOR_PIPELINE
    if PROCESSOR_PIPELINE is None:
        PROCESSOR_PIPELINE = sort_processors(PROCESSOR_CONFIGS)
    return PROCESSOR_PIPELINE


def _get_analyzers() -> list[AnalyzerEntry]:
    global ANALYZER_PIPELINE
    if ANALYZER_PIPELINE is None:
        ANALYZER_PIPELINE = load_analyzers(ANALYZER_CONFIGS, database=database)
    return ANALYZER_PIPELINE


async def _drain_processor_tasks(tasks: list[asyncio.Task[Any]]) -> int:
    if not tasks:
        return 0
    results = await asyncio.gather(*tasks, return_exceptions=True)
    modified = 0
    for result in results:
        if isinstance(result, Exception):
            logger.opt(exception=result).error("Processor task failed")
            continue
        if result:
            modified += 1
    tasks.clear()
    return modified


SOURCE_CONFIGS, PROCESSOR_CONFIGS, ANALYZER_CONFIGS = _load_workspace_config()
_ensure_sources_registered()


@app.post("/snapshot/{source_id}")
async def snapshot_source(source_id: str):
    source_cfg = _get_source_config_or_404(source_id)
    client = _get_client(source_id, source_cfg)
    processor_pipeline = _get_processor_pipeline()
    logger.info("Snapshotting source: {}", source_id)
    snapshot = database.begin_snapshot(source_id)
    seen = 0
    added = 0
    updated = 0
    processor_modified = 0
    processor_tasks: list[asyncio.Task[Any]] = []
    try:
        async for record, metadata in client.scan():
            try:
                record.attach_accessor(client.get_accessor(record))
            except Exception:
                logger.exception(
                    "Failed to attach accessor for record {} in source {}",
                    record.id,
                    source_id,
                )
            changes = database.upsert_file_record(record, metadata, snapshot)
            seen += 1
            if "file_record" in changes:
                added += 1
            if changes:
                updated += 1
            if processor_pipeline:
                processor_tasks.append(
                    asyncio.create_task(
                        run_processors(
                            record=record,
                            snapshot=snapshot,
                            database=database,
                            stages=processor_pipeline,
                            initial_changes=changes,
                        )
                    )
                )
    except Exception:
        processor_modified += await _drain_processor_tasks(processor_tasks)
        database.finalize_snapshot(snapshot, partial=True)
        raise
    processor_modified += await _drain_processor_tasks(processor_tasks)
    database.finalize_snapshot(snapshot)
    return {
        "status": "snapshot complete",
        "source": source_id,
        "snapshot_id": snapshot.id,
        "stats": {
            "seen": seen,
            "updated": updated,
            "added": added,
            "processor_modified": processor_modified,
        },
    }


@app.get("/records")
def list_files(source_id: Optional[str] = None, view: str = "flat"):
    if view not in {"flat", "complete"}:
        raise ValueError("view must be 'flat' or 'complete'")
    selected_view: Literal["flat", "complete"] = (
        "flat" if view == "flat" else "complete"
    )
    return database.list_records_with_metadata(source_id=source_id, view=selected_view)


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
