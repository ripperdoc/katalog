from __future__ import annotations

from pathlib import Path

import pytest

from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.sqlspec.sql_helpers import select
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.models import ActorType, OpStatus
from katalog.sources.runtime import run_sources


@pytest.mark.asyncio
async def test_filesystem_scan_partial_persists_metadata(
    tmp_path: Path,
    db_session,
):
    _ = db_session
    root = tmp_path / "fs_root"
    root.mkdir()
    for idx in range(3):
        (root / f"file_{idx}.txt").write_text(f"hello {idx}")

    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    actor = await actor_db.create(
        name="filesystem-test",
        plugin_id="katalog.sources.filesystem.FilesystemClient",
        type=ActorType.SOURCE,
        config={"root_path": str(root), "max_files": 2},
    )

    changeset = await changeset_db.begin(
        actors=[actor],
        message="Filesystem scan",
        status=OpStatus.IN_PROGRESS,
    )
    status = await run_sources(sources=[actor], changeset=changeset)
    await changeset.finalize(status=status)

    assert status == OpStatus.PARTIAL

    asset_db = get_asset_repo()
    assets = await asset_db.list_rows()
    assert len(assets) == 2

    rows = await select(
        db_session,
        f"SELECT COUNT(*) AS cnt FROM {METADATA_TABLE}",
    )
    assert int(rows[0]["cnt"]) > 0
