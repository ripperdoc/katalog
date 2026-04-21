import os
import importlib.util
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal

from katalog.utils.changeset_events import ChangesetEventManager

PORT = 8000
RuntimeMode = Literal["read_write", "read_only", "fast_read"]
InstallProfile = Literal["write", "readonly", "unknown"]


@dataclass
class AppContext:
    workspace: Path
    db_url: str
    db_path: Path | None
    runtime_mode: RuntimeMode = "read_write"
    install_profile: InstallProfile = "unknown"
    read_only_requested: bool = False
    read_only_effective: bool = False
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
        try:
            _ACTIVE_APP_CONTEXT.reset(token)
        except ValueError as exc:
            # Async-generator shutdown may close lifespan contexts from a different
            # context (e.g. Ctrl+C), where token reset is not allowed.
            if "different Context" not in str(exc):
                raise
        finally:
            _GLOBAL_APP_CONTEXT = previous_global


def current_app_context() -> AppContext:
    if _GLOBAL_APP_CONTEXT is not None:
        context = _ACTIVE_APP_CONTEXT.get()
        if context is not None:
            return context
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

    install_profile = _install_profile_from_env()
    read_only_requested = _env_flag("KATALOG_READ_ONLY")
    read_only_effective = read_only_requested or install_profile == "readonly"
    return AppContext(
        workspace=resolved_workspace,
        db_url=resolved_db_url,
        db_path=resolved_db_path,
        install_profile=install_profile,
        read_only_requested=read_only_requested,
        read_only_effective=read_only_effective,
    )


def _env_flag(name: str) -> bool:
    value = os.environ.get(name, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _install_profile_from_env() -> InstallProfile:
    raw = os.environ.get("KATALOG_INSTALL_PROFILE", "").strip().lower()
    if raw in {"write", "writable", "read_write"}:
        return "write"
    if raw in {"readonly", "read_only", "ro"}:
        return "readonly"
    return _detect_install_profile_from_dependencies()


def _has_module(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _detect_install_profile_from_dependencies() -> InstallProfile:
    """Infer install profile from known write-extra dependencies."""
    write_modules = [
        "crawlee",
        "google_auth_oauthlib",
        "magic",
    ]
    if all(_has_module(module_name) for module_name in write_modules):
        return "write"
    return "readonly"


def current_workspace() -> Path:
    return current_app_context().workspace


def current_db_path() -> Path | None:
    return current_app_context().db_path


def current_db_url() -> str:
    return current_app_context().db_url


def current_runtime_mode() -> RuntimeMode:
    return current_app_context().runtime_mode


def actor_path(actor_id: int, subfolder: str | Path | None = None):
    """Returns a path for storing actor-specific data in the workspace. Ensure the path exists."""
    workspace = current_workspace()
    path = workspace / "actors" / str(actor_id)
    if subfolder:
        path = path / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path
