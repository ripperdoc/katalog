import os
from pathlib import Path
import tomllib

from loguru import logger

PORT = 8000

workspace_env = os.environ.get("KATALOG_WORKSPACE", "hg_workspace")
if not workspace_env:
    raise ValueError("KATALOG_WORKSPACE environment variable is not set")
WORKSPACE = Path(workspace_env).expanduser().resolve()

DB_PATH = WORKSPACE / "katalog.db"
DB_URL = f"sqlite:///{DB_PATH}"


def read_config_file():
    config_file = {}
    try:
        with (WORKSPACE / "katalog.toml").open("rb") as handle:
            config_file = tomllib.load(handle)
    except FileNotFoundError:
        logger.warning(f"No katalog.toml found in {WORKSPACE}; using defaults")
    return config_file
