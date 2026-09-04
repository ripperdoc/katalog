from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from katalog.config import build_app_context, use_app_context
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import sync_config_db
from katalog.db.sqlspec import close_db, configure_sqlspec, session_scope
from katalog.models import ActorType, OpStatus
from katalog.plugins import registry as plugin_registry
from katalog.sources.runtime import run_sources


@pytest.fixture(autouse=True)
def default_app_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
):
    module_name = getattr(request.module, "__name__", "")
    if module_name.endswith("test_api_workspace"):
        yield
        return

    workspace = tmp_path / ".app_context_workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("KATALOG_WORKSPACE", str(workspace))
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)

    with use_app_context(build_app_context(workspace=workspace)):
        yield


@pytest_asyncio.fixture
async def db_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_url = f"sqlite:///{tmp_path / 'katalog.db'}"
    monkeypatch.setenv("KATALOG_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("KATALOG_DATABASE_URL", db_url)

    configure_sqlspec(db_url)
    plugin_registry.clear_instance_cache()
    await sync_config_db()
    async with session_scope() as session:
        # Keep direct SQL access available to tests without making repository
        # calls share this connection. Repository-owned sessions must commit
        # before their writes become visible here.
        yield session
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
