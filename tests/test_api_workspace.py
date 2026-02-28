from __future__ import annotations

import os

import pytest

from katalog.api.assets import list_assets
from katalog.config import current_db_url, current_workspace
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
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
