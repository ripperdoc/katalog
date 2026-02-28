"""
Shows example code for how katalog can be imported and used by other Python code.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from katalog.api.assets import list_assets
from katalog.lifespan import app_lifespan
from katalog.models.query import AssetQuery


async def _run(workspace: Path) -> None:
    async with app_lifespan(init_mode="fast", workspace=workspace):
        response = await list_assets(AssetQuery(view_id="default", offset=0, limit=10))
        rows = [item.model_dump(mode="json") for item in response.items]
        print(json.dumps(rows, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List first 10 assets from a workspace via katalog API."
    )
    parser.add_argument(
        "--workspace",
        default="workspace_test",
        help="Workspace folder path (default: workspace_test)",
    )
    args = parser.parse_args()

    workspace = Path(args.workspace).expanduser().resolve()
    if not workspace.exists() or not workspace.is_dir():
        raise SystemExit(f"Workspace does not exist or is not a directory: {workspace}")

    asyncio.run(_run(workspace))


if __name__ == "__main__":
    main()
