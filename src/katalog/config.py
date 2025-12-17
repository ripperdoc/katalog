import os
from pathlib import Path
import tomllib


workspace_env = os.environ.get("KATALOG_WORKSPACE", "hg_workspace")
if not workspace_env:
    raise ValueError("KATALOG_WORKSPACE environment variable is not set")
WORKSPACE = Path(workspace_env).expanduser().resolve()

config_file = None
with (WORKSPACE / "katalog.toml").open("rb") as handle:
    config_file = tomllib.load(handle)
print(config_file)
