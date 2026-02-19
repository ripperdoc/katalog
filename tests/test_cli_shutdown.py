from __future__ import annotations

import json
import os
import subprocess
import sys
from textwrap import dedent
from pathlib import Path

import pytest


def _write_minimal_workflow(path: Path, *, source_root: Path) -> None:
    workflow = dedent(
        """
        [[actors]]
        name = "Local Files"
        plugin_id = "katalog.sources.filesystem.FilesystemClient"
        root_path = {root_path}
        include_patterns = ["input.txt"]
        max_files = 10

        [[actors]]
        name = "Mime"
        plugin_id = "katalog.processors.mime_type.MimeTypeProcessor"

        [[actors]]
        name = "Extract"
        plugin_id = "katalog.processors.kreuzberg_document_extract.KreuzbergDocumentExtractProcessor"
        enable_chunking = false
        """
    ).strip()
    path.write_text(
        workflow.format(root_path=json.dumps(str(source_root))) + "\n",
        encoding="utf-8",
    )


def test_workflow_apply_cli_exits_cleanly_for_temp_workspace(tmp_path: Path) -> None:
    pytest.importorskip("kreuzberg")

    workspace = tmp_path / "workspace"
    source_root = workspace / "data"
    workspace.mkdir()
    source_root.mkdir()
    (source_root / "input.txt").write_text("hello from shutdown test\n", encoding="utf-8")

    workflow_file = workspace / "workflow.toml"
    _write_minimal_workflow(workflow_file, source_root=source_root)

    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else src_path
    )

    command = [
        sys.executable,
        "-m",
        "katalog.cli",
        "--workspace",
        str(workspace),
        "workflows",
        "apply",
        "--file",
        str(workflow_file),
    ]

    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
            timeout=40,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        pytest.fail(f"CLI did not exit within timeout.\nstderr:\n{stderr}")

    assert completed.returncode == 0, (
        "CLI exited with non-zero status.\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
