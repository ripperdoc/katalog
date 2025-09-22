import os
from pathlib import Path


workspace_env = os.environ.get("KATALOG_WORKSPACE")
if not workspace_env:
    raise ValueError("KATALOG_WORKSPACE environment variable is not set")
WORKSPACE = Path(workspace_env).expanduser().resolve()
