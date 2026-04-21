from __future__ import annotations

import asyncio
import os

import pytest

from katalog.api.actors import ActorCreate, create_actor
from katalog.api.assets import list_assets
from katalog.api.helpers import ApiError
from katalog.config import current_db_url, current_workspace
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import execute
from katalog.lifespan import app_lifespan
from katalog.models import ActorType, Asset, OpStatus
from katalog.models.query import AssetQuery


@pytest.mark.asyncio
async def test_app_lifespan_workspace_allows_api_calls_without_global_env(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)

    workspace = tmp_path / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    async with app_lifespan(init_mode="fast", workspace=workspace):
        assert current_workspace() == workspace
        assert current_db_url() == f"sqlite:///{workspace / 'katalog.db'}"

        response = await list_assets(
            AssetQuery.model_validate(
                {
                    "view_id": "default",
                    "offset": 0,
                    "limit": 10,
                }
            )
        )

        assert response.items == []
        assert response.stats.returned == 0
        assert (workspace / "katalog.db").exists()

    assert os.environ.get("KATALOG_WORKSPACE") is None
    assert os.environ.get("KATALOG_DATABASE_URL") is None


@pytest.mark.asyncio
async def test_app_lifespan_workspace_restores_existing_environment(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    original_workspace = tmp_path / "original"
    original_workspace.mkdir(parents=True, exist_ok=True)
    original_db_url = f"sqlite:///{original_workspace / 'katalog.db'}"

    monkeypatch.setenv("KATALOG_WORKSPACE", str(original_workspace))
    monkeypatch.setenv("KATALOG_DATABASE_URL", original_db_url)

    temporary_workspace = tmp_path / "temporary"
    temporary_workspace.mkdir(parents=True, exist_ok=True)

    async with app_lifespan(init_mode="fast", workspace=temporary_workspace):
        # Runtime context should switch, but process env should remain unchanged.
        assert current_workspace() == temporary_workspace
        assert current_db_url() == f"sqlite:///{temporary_workspace / 'katalog.db'}"
        assert os.environ.get("KATALOG_WORKSPACE") == str(original_workspace)
        assert os.environ.get("KATALOG_DATABASE_URL") == original_db_url

    assert os.environ.get("KATALOG_WORKSPACE") == str(original_workspace)
    assert os.environ.get("KATALOG_DATABASE_URL") == original_db_url
    with pytest.raises(RuntimeError):
        current_workspace()
    with pytest.raises(RuntimeError):
        current_db_url()


@pytest.mark.asyncio
async def test_app_lifespan_accepts_workspace_context(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)

    workspace = tmp_path / "workspace_lifespan"
    workspace.mkdir(parents=True, exist_ok=True)

    async with app_lifespan(init_mode="fast", workspace=workspace):
        assert current_workspace() == workspace
        assert current_db_url() == f"sqlite:///{workspace / 'katalog.db'}"
        response = await list_assets(
            AssetQuery.model_validate(
                {
                    "view_id": "default",
                    "offset": 0,
                    "limit": 5,
                }
            )
        )
        assert response.items == []


@pytest.mark.asyncio
async def test_runtime_workspace_context_overrides_env_for_db_access(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    original_workspace = tmp_path / "workspace_original"
    original_workspace.mkdir(parents=True, exist_ok=True)
    temporary_workspace = tmp_path / "workspace_temporary"
    temporary_workspace.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("KATALOG_WORKSPACE", str(original_workspace))
    monkeypatch.setenv(
        "KATALOG_DATABASE_URL",
        f"sqlite:///{original_workspace / 'katalog.db'}",
    )

    async with app_lifespan(init_mode="fast"):
        actor = await get_actor_repo().create(
            name="source-actor",
            plugin_id="plugin.source",
            type=ActorType.SOURCE,
        )
        changeset = await get_changeset_repo().create_auto(status=OpStatus.IN_PROGRESS)
        await get_changeset_repo().add_actors(changeset, [actor])
        await get_asset_repo().save_record(
            Asset(
                namespace="test",
                external_id="asset-1",
                canonical_uri="file:///asset-1",
                actor_id=actor.id,
            ),
            changeset=changeset,
            actor=actor,
        )
        original_response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 10})
        )
        assert len(original_response.items) == 1

    async with app_lifespan(init_mode="fast", workspace=temporary_workspace):
        temporary_response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 10})
        )
        assert temporary_response.items == []

    async with app_lifespan(init_mode="fast"):
        restored_response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 10})
        )
        assert len(restored_response.items) == 1


@pytest.mark.asyncio
async def test_app_lifespan_close_in_different_context_does_not_crash(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)

    workspace = tmp_path / "workspace_generator_close"
    workspace.mkdir(parents=True, exist_ok=True)

    context_manager = app_lifespan(init_mode="fast", workspace=workspace)
    await context_manager.__aenter__()

    # This simulates event-loop shutdown closing the async generator in
    # another context, which currently crashes while resetting ContextVar token.
    async def close_lifespan_generator() -> None:
        await context_manager.gen.aclose()

    await asyncio.create_task(close_lifespan_generator())

    with pytest.raises(RuntimeError):
        current_workspace()
    with pytest.raises(RuntimeError):
        current_db_url()


@pytest.mark.asyncio
async def test_read_only_mode_blocks_write_operations(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)
    monkeypatch.delenv("KATALOG_READ_ONLY", raising=False)

    workspace = tmp_path / "workspace_read_only"
    workspace.mkdir(parents=True, exist_ok=True)

    # Initialize workspace once in read-write mode.
    async with app_lifespan(runtime_mode="read_write", workspace=workspace):
        response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 1})
        )
        assert response.items == []

    async with app_lifespan(runtime_mode="read_only", workspace=workspace):
        with pytest.raises(ApiError) as exc_info:
            await create_actor(
                ActorCreate(
                    name="Should fail in read-only",
                    plugin_id="katalog.sources.fake_assets.FakeAssetSource",
                )
            )

        err = exc_info.value
        assert err.status_code == 403
        assert isinstance(err.detail, dict)
        assert err.detail.get("action") == "create_actor"
        assert err.detail.get("runtime_mode") == "read_only"
        assert err.detail.get("read_only_effective") is True


@pytest.mark.asyncio
async def test_read_only_mode_opens_db_as_read_only(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)
    monkeypatch.delenv("KATALOG_READ_ONLY", raising=False)

    workspace = tmp_path / "workspace_db_ro"
    workspace.mkdir(parents=True, exist_ok=True)

    # Initialize schema/db in read-write mode first.
    async with app_lifespan(runtime_mode="read_write", workspace=workspace):
        response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 1})
        )
        assert response.items == []

    async with app_lifespan(runtime_mode="read_only", workspace=workspace):
        with pytest.raises(Exception) as exc_info:
            async with session_scope() as session:
                await execute(
                    session,
                    """
                    INSERT INTO actors (name, type, disabled)
                    VALUES (?, ?, ?)
                    """,
                    ["should-fail", int(ActorType.SOURCE), 0],
                )
                await session.commit()

        # We expect sqlite/sqlspec to reject writes on a read-only connection.
        assert "readonly" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_fast_read_mode_respects_effective_read_only_profile(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("KATALOG_WORKSPACE", raising=False)
    monkeypatch.delenv("KATALOG_DATABASE_URL", raising=False)
    monkeypatch.delenv("KATALOG_READ_ONLY", raising=False)
    monkeypatch.setenv("KATALOG_INSTALL_PROFILE", "write")

    workspace = tmp_path / "workspace_fast_read_ro"
    workspace.mkdir(parents=True, exist_ok=True)

    # Initialize workspace once in write mode, then open with fast_read under readonly profile.
    async with app_lifespan(runtime_mode="read_write", workspace=workspace):
        response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 1})
        )
        assert response.items == []

    monkeypatch.setenv("KATALOG_INSTALL_PROFILE", "readonly")
    async with app_lifespan(runtime_mode="fast_read", workspace=workspace):
        response = await list_assets(
            AssetQuery.model_validate({"view_id": "default", "offset": 0, "limit": 1})
        )
        assert response.items == []
