import os
from pathlib import Path

PORT = 8000

workspace_env = os.environ.get("KATALOG_WORKSPACE", "hg_workspace")
if not workspace_env:
    raise ValueError("KATALOG_WORKSPACE environment variable is not set")
WORKSPACE = Path(workspace_env).expanduser().resolve()

DB_PATH = WORKSPACE / "katalog.db"
DB_URL = f"sqlite:///{DB_PATH}"


def actor_path(actor_id: int, subfolder: str | Path | None = None):
    """Returns a path for storing actor-specific data in the workspace. Ensure the path exists."""
    path = WORKSPACE / "actors" / str(actor_id)
    if subfolder:
        path = path / subfolder
    path.mkdir(parents=True, exist_ok=True)
    return path
