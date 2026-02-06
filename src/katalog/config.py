import os
import sys
from pathlib import Path

PORT = 8000

def _is_test_environment() -> bool:
    return bool(
        os.environ.get("PYTEST_CURRENT_TEST")
        or os.environ.get("KATALOG_TESTING")
        or "pytest" in sys.modules
    )


workspace_env = os.environ.get("KATALOG_WORKSPACE")
if workspace_env:
    WORKSPACE: Path | None = Path(workspace_env).expanduser().resolve()
else:
    if not _is_test_environment():
        raise ValueError("KATALOG_WORKSPACE environment variable is not set")
    WORKSPACE = None

DB_PATH: Path | None = WORKSPACE / "katalog.db" if WORKSPACE else None
DB_URL = os.environ.get("KATALOG_DATABASE_URL")
if DB_URL is None and DB_PATH is not None:
    DB_URL = f"sqlite:///{DB_PATH}"
if DB_URL is None and _is_test_environment():
    DB_URL = "sqlite:///:memory:"


def actor_path(actor_id: int, subfolder: str | Path | None = None):
    """Returns a path for storing actor-specific data in the workspace. Ensure the path exists."""
    if WORKSPACE is None:
        raise ValueError("Workspace is not configured for actor data storage")
    path = WORKSPACE / "actors" / str(actor_id)
    if subfolder:
        path = path / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path
