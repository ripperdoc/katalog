from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import textwrap


def _run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> None:
    print(f"+ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env, cwd=str(cwd) if cwd else None)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create a clean core-only venv, install katalog from this checkout, "
            "and run basic smoke checks."
        )
    )
    parser.add_argument(
        "--workspace",
        default="workspace_test",
        help="Workspace path to use for checks (default: workspace_test)",
    )
    parser.add_argument(
        "--venv",
        default=".tmp/smoke-core-venv",
        help="Virtualenv path to create/use (default: .tmp/smoke-core-venv)",
    )
    parser.add_argument(
        "--no-recreate",
        action="store_true",
        help="Reuse an existing venv instead of recreating it",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    workspace = (repo_root / args.workspace).resolve()
    venv_dir = (repo_root / args.venv).resolve()
    python_bin = venv_dir / "bin" / "python"
    katalog_bin = venv_dir / "bin" / "katalog"

    if not workspace.exists():
        print(f"Workspace not found: {workspace}", file=sys.stderr)
        return 1

    if venv_dir.exists() and not args.no_recreate:
        shutil.rmtree(venv_dir)

    if not venv_dir.exists():
        _run(["uv", "venv", str(venv_dir)], cwd=repo_root)

    _run(
        ["uv", "pip", "install", "--python", str(python_bin), "-U", str(repo_root)],
        cwd=repo_root,
    )

    env = os.environ.copy()
    env["KATALOG_WORKSPACE"] = str(workspace)
    # We want to verify default/core behavior, not force an explicit mode.
    env.pop("KATALOG_INSTALL_PROFILE", None)
    env.pop("KATALOG_READ_ONLY", None)

    _run([str(katalog_bin), "-w", str(workspace), "stats"], env=env, cwd=repo_root)
    _run([str(katalog_bin), "-w", str(workspace), "--json", "actors", "list"], env=env, cwd=repo_root)
    _run([str(katalog_bin), "-w", str(workspace), "--json", "assets", "list", "--limit", "1"], env=env, cwd=repo_root)

    probe_script = textwrap.dedent(
        """
        from fastapi.testclient import TestClient
        from katalog.server.app import app

        paths = [
            "/",
            "/api/views",
            "/api/assets?view_id=default&offset=0&limit=1",
            "/api/plugins",
            "/api/stats",
        ]

        with TestClient(app) as client:
            for path in paths:
                response = client.get(path)
                print(path, response.status_code)
                if response.status_code != 200:
                    raise SystemExit(1)
        """
    ).strip()
    _run([str(python_bin), "-c", probe_script], env=env, cwd=repo_root)

    print()
    print("Core smoke test passed.")
    print(f"venv: {venv_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
