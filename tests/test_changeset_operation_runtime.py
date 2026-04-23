from __future__ import annotations

import asyncio

import pytest

from katalog.api.operations import run_source
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.models import ActorType, OpStatus
from katalog.runtime.state import get_running_changesets
from katalog.sources.fake_assets import FakeAssetSource


@pytest.mark.asyncio
async def test_changeset_start_operation_finalizes_error_on_exception(db_session):
    _ = db_session
    repo = get_changeset_repo()
    changeset = await repo.create_auto(
        status=OpStatus.IN_PROGRESS, message="runtime exception handling test"
    )

    async def _boom():
        raise RuntimeError("plugin exploded")

    with pytest.raises(RuntimeError, match="plugin exploded"):
        await changeset.start_operation(_boom)

    saved = await repo.get(id=changeset.id)
    assert saved.status == OpStatus.ERROR
    assert saved.running_time_ms is not None
    assert saved.data is not None
    assert "plugin exploded" in str(saved.data.get("error_message"))


@pytest.mark.asyncio
async def test_run_source_background_failure_finalizes_and_cleans_tracking(
    db_session, monkeypatch
):
    _ = db_session
    actor_repo = get_actor_repo()
    changeset_repo = get_changeset_repo()

    actor = await actor_repo.create(
        name="failing-source",
        plugin_id=FakeAssetSource.plugin_id,
        type=ActorType.SOURCE,
        config={"namespace": "runtime-failure-test", "total_assets": 1},
    )
    assert actor.id is not None

    async def _scan_raises(self):
        _ = self
        raise RuntimeError("scan exploded")

    monkeypatch.setattr(FakeAssetSource, "scan", _scan_raises)

    changeset = await run_source(int(actor.id), finalize=False)
    assert changeset.done_event is not None

    await asyncio.wait_for(changeset.done_event.wait(), timeout=2.0)
    saved = await changeset_repo.get(id=changeset.id)

    assert saved.status == OpStatus.ERROR
    assert saved.data is not None
    assert "scan exploded" in str(saved.data.get("error_message"))
    assert int(changeset.id) not in get_running_changesets()
