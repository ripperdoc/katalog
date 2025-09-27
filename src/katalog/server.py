import tomllib
from loguru import logger

from fastapi import FastAPI, HTTPException
from typing import Any, Literal

from katalog.clients.base import SourceClient
from katalog.config import WORKSPACE
from katalog.db import Database
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
CLIENT_CACHE: dict[str, SourceClient] = {}


def _load_source_configs() -> dict[str, dict[str, Any]]:
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
    return result


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


SOURCE_CONFIGS = _load_source_configs()
_ensure_sources_registered()


@app.post("/snapshot/{source_id}")
async def snapshot_source(source_id: str):
    source_cfg = _get_source_config_or_404(source_id)
    client = _get_client(source_id, source_cfg)
    logger.info("Snapshotting source: {}", source_id)
    snapshot = database.begin_snapshot(source_id)
    seen = 0
    added = 0
    updated = 0
    try:
        async for record in client.scan():
            changes = database.upsert_file_record(record, snapshot)
            seen += 1
            if "file_record" in changes:
                added += 1
            if changes:
                updated += 1
    except Exception:
        database.finalize_snapshot(snapshot, partial=True)
        raise
    database.finalize_snapshot(snapshot)
    return {
        "status": "snapshot complete",
        "source": source_id,
        "snapshot_id": snapshot.id,
        "stats": {
            "seen": seen,
            "updated": updated,
            "added": added,
        },
    }


@app.get("/files/{source_id}")
def list_files(source_id: str, view: str = "flat"):
    if view not in {"flat", "complete"}:
        raise ValueError("view must be 'flat' or 'complete'")
    selected_view: Literal["flat", "complete"] = (
        "flat" if view == "flat" else "complete"
    )
    return database.list_files_with_metadata(source_id, view=selected_view)
