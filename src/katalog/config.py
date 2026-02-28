import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from katalog.utils.changeset_events import ChangesetEventManager

PORT = 8000


@dataclass
class AppContext:
    workspace: Path
    db_url: str
    db_path: Path | None
    event_manager: ChangesetEventManager = field(default_factory=ChangesetEventManager)
    running_changesets: dict[int, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)


_ACTIVE_APP_CONTEXT: ContextVar[AppContext | None] = ContextVar(
    "katalog_active_app_context", default=None
)
_GLOBAL_APP_CONTEXT: AppContext | None = None


def _resolve_workspace_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser().resolve()


@contextmanager
def use_app_context(context: AppContext) -> Iterator[AppContext]:
    global _GLOBAL_APP_CONTEXT
    previous_global = _GLOBAL_APP_CONTEXT
    _GLOBAL_APP_CONTEXT = context
    token = _ACTIVE_APP_CONTEXT.set(context)
    try:
        yield context
    finally:
        _ACTIVE_APP_CONTEXT.reset(token)
        _GLOBAL_APP_CONTEXT = previous_global


def current_app_context() -> AppContext:
    context = _ACTIVE_APP_CONTEXT.get()
    if context is not None:
        return context
    if _GLOBAL_APP_CONTEXT is not None:
        return _GLOBAL_APP_CONTEXT
    raise RuntimeError(
        "No active AppContext. Run this operation inside app_lifespan()."
    )


def _workspace_from_env() -> Path | None:
    return _resolve_workspace_path(os.environ.get("KATALOG_WORKSPACE"))


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    path_raw = db_url[len("sqlite:///") :]
    if not path_raw:
        return None
    if path_raw == ":memory:":
        return None
    if path_raw.startswith("file:"):
        return None
    return Path(path_raw).expanduser().resolve()


def build_app_context(
    *,
    workspace: str | Path | None = None,
    db_url: str | None = None,
) -> AppContext:
    explicit_workspace = _resolve_workspace_path(workspace)
    resolved_workspace = explicit_workspace or _workspace_from_env()
    resolved_db_url = db_url
    if resolved_db_url is None and explicit_workspace is None:
        resolved_db_url = os.environ.get("KATALOG_DATABASE_URL")
    if resolved_db_url is None and resolved_workspace is not None:
        resolved_db_url = f"sqlite:///{resolved_workspace / 'katalog.db'}"
    if resolved_db_url is None:
        raise RuntimeError(
            "No database configured. Set KATALOG_DATABASE_URL or KATALOG_WORKSPACE, "
            "or pass workspace/db_url to app_lifespan()."
        )

    resolved_db_path = _sqlite_path_from_url(resolved_db_url)
    if resolved_workspace is None:
        if resolved_db_path is None:
            raise RuntimeError(
                "No workspace configured. Set KATALOG_WORKSPACE or pass workspace to app_lifespan()."
            )
        resolved_workspace = resolved_db_path.parent

    return AppContext(
        workspace=resolved_workspace,
        db_url=resolved_db_url,
        db_path=resolved_db_path,
    )


def current_workspace() -> Path:
    return current_app_context().workspace


def current_db_path() -> Path | None:
    return current_app_context().db_path


def current_db_url() -> str:
    return current_app_context().db_url


def actor_path(actor_id: int, subfolder: str | Path | None = None):
    """Returns a path for storing actor-specific data in the workspace. Ensure the path exists."""
    workspace = current_workspace()
    path = workspace / "actors" / str(actor_id)
    if subfolder:
        path = path / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path
