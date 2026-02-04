from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio

from katalog.db.sqlspec import close_db, test_session
from katalog.db.metadata import sync_config_db
from katalog.models import ActorType, OpStatus
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.sources.runtime import run_sources
from katalog.plugins import registry as plugin_registry


@pytest_asyncio.fixture
async def db_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("KATALOG_WORKSPACE", str(workspace))

    mem_name = f"memdb_{uuid4().hex}"
    db_url = f"sqlite:///file:{mem_name}?mode=memory&cache=shared"
    monkeypatch.setenv("KATALOG_DATABASE_URL", db_url)

    async with test_session(db_url) as session:
        plugin_registry._INSTANCE_CACHE.clear()
        await sync_config_db()
        yield session
    plugin_registry._INSTANCE_CACHE.clear()
    await close_db()


@pytest_asyncio.fixture
async def pipeline_db(db_session):
    _ = db_session
    yield


@pytest_asyncio.fixture
async def seeded_assets(db_session):
    _ = db_session

    fake_plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    actor = await actor_db.get_or_none(plugin_id=fake_plugin_id)
    if actor is None:
        actor = await actor_db.create(
            name="Fake Assets",
            plugin_id=fake_plugin_id,
            type=ActorType.SOURCE,
            config={
                "total_assets": 100,
                "seed": 1,
                "batch_delay_ms": 0,
                "batch_jitter_ms": 0,
            },
        )
    else:
        actor.config = {
            "total_assets": 100,
            "seed": 1,
            "batch_delay_ms": 0,
            "batch_jitter_ms": 0,
        }
        await actor_db.save(actor)

    changeset = await changeset_db.begin(
        actors=[actor],
        message="Test seed",
        status=OpStatus.IN_PROGRESS,
    )
    status = await run_sources(sources=[actor], changeset=changeset)
    await changeset.finalize(status=status)

    return actor
