from __future__ import annotations

import pytest

from katalog.api.helpers import ApiError
from katalog.api.operations import run_source
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.models import ActorType
from katalog.sources.fake_assets import FakeAssetSource


@pytest.mark.asyncio
async def test_run_source_fails_fast_when_source_not_ready_without_changeset(
    db_session, monkeypatch
):
    _ = db_session
    actor_repo = get_actor_repo()
    changeset_repo = get_changeset_repo()

    actor = await actor_repo.create(
        name="not-ready-source",
        plugin_id=FakeAssetSource.plugin_id,
        type=ActorType.SOURCE,
        config={"namespace": "not-ready", "total_assets": 1},
    )
    assert actor.id is not None

    async def _not_ready(self):
        _ = self
        return False, "intentional test failure"

    monkeypatch.setattr(FakeAssetSource, "is_ready", _not_ready)

    with pytest.raises(ApiError) as exc:
        await run_source(int(actor.id), finalize=False)

    assert exc.value.status_code == 409
    assert "Source is not ready" in str(exc.value.detail)
    assert "intentional test failure" in str(exc.value.detail)

    changesets = await changeset_repo.list_rows(order_by="id")
    assert changesets == []

