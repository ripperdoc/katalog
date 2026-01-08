import os
from pathlib import Path

PORT = 8000

workspace_env = os.environ.get("KATALOG_WORKSPACE", "hg_workspace")
if not workspace_env:
    raise ValueError("KATALOG_WORKSPACE environment variable is not set")
WORKSPACE = Path(workspace_env).expanduser().resolve()

DB_PATH = WORKSPACE / "katalog.db"
DB_URL = f"sqlite:///{DB_PATH}"
