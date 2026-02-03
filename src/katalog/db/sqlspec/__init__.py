from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
import os
from pathlib import Path
from typing import Any, AsyncIterator

from loguru import logger
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from katalog.config import DB_PATH

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
SCHEMA_PATH = SQL_DIR / "schema.sql"

spec = SQLSpec()
_ACTIVE_SESSION: ContextVar[Any | None] = ContextVar("sqlspec_active_session", default=None)
_CONFIG: AiosqliteConfig | None = None
_ANALYSIS_CONFIG: AiosqliteConfig | None = None


def _build_config(db_url: str) -> AiosqliteConfig:
    if db_url.startswith("sqlite:///"):
        database = db_url[len("sqlite:///") :]
    elif db_url.startswith("sqlite://"):
        database = db_url[len("sqlite://") :]
    else:
        database = db_url

    use_uri = database.startswith("file:")
    connection_config: dict[str, Any] = {"database": database}
    if use_uri:
        connection_config["uri"] = True

    return AiosqliteConfig(connection_config=connection_config)


def _default_db_url() -> str:
    return os.environ.get("KATALOG_DATABASE_URL") or f"sqlite:///{DB_PATH}"


def configure_sqlspec(db_url: str | None = None) -> None:
    global _CONFIG, _ANALYSIS_CONFIG
    db_url = db_url or _default_db_url()
    _CONFIG = _build_config(db_url)
    _ANALYSIS_CONFIG = _CONFIG


def _get_config(*, analysis: bool = False) -> AiosqliteConfig:
    if _CONFIG is None or _ANALYSIS_CONFIG is None:
        configure_sqlspec()
    if analysis:
        assert _ANALYSIS_CONFIG is not None
        return _ANALYSIS_CONFIG
    assert _CONFIG is not None
    return _CONFIG


@asynccontextmanager
async def session_scope(*, analysis: bool = False) -> AsyncIterator[Any]:
    active = _ACTIVE_SESSION.get()
    if active is not None:
        yield active
        return

    config = _get_config(analysis=analysis)
    async with spec.provide_session(config) as session:
        yield session


@asynccontextmanager
async def test_session(db_url: str) -> AsyncIterator[Any]:
    configure_sqlspec(db_url)
    config = _get_config()
    async with spec.provide_session(config) as session:
        token = _ACTIVE_SESSION.set(session)
        try:
            yield session
        finally:
            _ACTIVE_SESSION.reset(token)


async def init_db() -> None:
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"SQL schema not found: {SCHEMA_PATH}")

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    async with session_scope() as session:
        await session.execute_script(schema_sql)

    logger.info("Initialized SQLSpec database schema")


async def close_db() -> None:
    await spec.close_all_pools()


def db_path() -> Path:
    return DB_PATH
