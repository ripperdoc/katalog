import json
import tomllib
from loguru import logger

from fastapi import FastAPI

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


# Read sources from katalog.toml and scan all sources
@app.post("/initialize")
async def initialize_sources():
    with CONFIG_PATH.open("rb") as f:
        config = tomllib.load(f)
    sources = config.get("sources", [])
    source_map = {}
    for source in sources:
        id = source.get("id", None)
        if not id:
            raise ValueError(f"Source must have an 'id' field: {json.dumps(source)}")
        SourceClass = import_client_class(source.get("class"))
        client = SourceClass(**source)
        if id in source_map:
            raise ValueError(f"Duplicate source ID: {id}")
        source_map[id] = {"client": client, "config": source}

    for source_id, payload in source_map.items():
        client = payload["client"]
        source_cfg = payload["config"]
        database.ensure_source(
            source_id,
            title=source_cfg.get("title"),
            plugin_id=getattr(client, "PLUGIN_ID", client.__class__.__module__),
            config=source_cfg,
        )

    for payload in source_map.values():
        client = payload["client"]
        logger.info(f"Scanning source: {client.id}")
        ctx = database.begin_scan(client.id)
        async for record in client.scan():
            database.upsert_file_record(record, ctx)
        database.finalize_scan(ctx)

    return {"status": "scan complete", "sources": list(source_map.keys())}


@app.get("/list")
def list_local_files():
    rows = database.conn.execute(
        "SELECT id, source_id, canonical_uri, path, filename, size_bytes, checksum_md5, mime_type, last_seen_at FROM file_records"
    ).fetchall()
    return [dict(row) for row in rows]
