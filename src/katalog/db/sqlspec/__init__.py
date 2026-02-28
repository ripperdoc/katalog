from __future__ import annotations

from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar
from pathlib import Path
from typing import Any, AsyncIterator

from loguru import logger
import sqlite_vec
from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig

from katalog import config as app_config

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
SCHEMA_PATH = SQL_DIR / "schema.sql"

spec = SQLSpec()
_ACTIVE_SESSION: ContextVar[Any | None] = ContextVar("sqlspec_active_session", default=None)


def _sqlspec_state() -> tuple[AiosqliteConfig | None, set[int]]:
    state = app_config.current_app_context().state
    config = state.get("sqlspec_config")
    loaded_extensions = state.get("sqlspec_loaded_extensions")
    if loaded_extensions is None:
        loaded_extensions = set()
        state["sqlspec_loaded_extensions"] = loaded_extensions
    return config, loaded_extensions


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
    return app_config.current_db_url()


def configure_sqlspec(db_url: str | None = None) -> None:
    resolved_db_url = db_url or _default_db_url()
    app_config.current_app_context().state["sqlspec_config"] = _build_config(
        resolved_db_url
    )


def reset_sqlspec_config() -> None:
    state = app_config.current_app_context().state
    state.pop("sqlspec_config", None)
    loaded_extensions = state.get("sqlspec_loaded_extensions")
    if loaded_extensions is not None:
        loaded_extensions.clear()


def _get_config(*, analysis: bool = False) -> AiosqliteConfig:
    _ = analysis
    config, _ = _sqlspec_state()
    if config is None:
        config = _build_config(_default_db_url())
        app_config.current_app_context().state["sqlspec_config"] = config
    return config


@asynccontextmanager
async def session_scope(*, analysis: bool = False) -> AsyncIterator[Any]:
    active = _ACTIVE_SESSION.get()
    if active is not None:
        yield active
        return

    config = _get_config(analysis=analysis)
    async with spec.provide_session(config) as session:
        await _ensure_sqlite_extensions(session)
        yield session


@asynccontextmanager
async def test_session(db_url: str) -> AsyncIterator[Any]:
    try:
        active_context = app_config.current_app_context()
    except RuntimeError:
        active_context = None

    context_scope = (
        nullcontext()
        if active_context is not None and active_context.db_url == db_url
        else app_config.use_app_context(app_config.build_app_context(db_url=db_url))
    )

    with context_scope:
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


async def _ensure_sqlite_extensions(session: Any) -> None:
    _, loaded_extensions = _sqlspec_state()
    conn = getattr(session, "connection", None)
    if conn is None:
        return

    raw_conn = getattr(conn, "_conn", None)
    conn_id = id(raw_conn) if raw_conn is not None else id(conn)
    if conn_id in loaded_extensions:
        return

    await conn.enable_load_extension(True)
    try:
        await conn.load_extension(sqlite_vec.loadable_path())
        loaded_extensions.add(conn_id)
    finally:
        await conn.enable_load_extension(False)


async def close_db() -> None:
    await spec.close_all_pools()
    try:
        reset_sqlspec_config()
    except RuntimeError:
        # Only lifespan/test-session managed contexts can own SQLSpec resources.
        pass


def db_path() -> Path:
    path = app_config.current_db_path()
    if path is None:
        raise ValueError("Workspace is not configured for database path access")
    return path
