from __future__ import annotations

import pytest

from katalog.api.actors import ActorCreate, ActorUpdate, create_actor, update_actor
from katalog.api.helpers import ApiError


@pytest.mark.asyncio
async def test_create_actor_defaults_identity_key_to_plugin_id(db_session) -> None:
    _ = db_session
    plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    actor = await create_actor(
        ActorCreate(
            name="fake-source-default-id",
            plugin_id=plugin_id,
            config={"namespace": "identity_default", "total_assets": 0},
        )
    )
    assert actor.identity_key == plugin_id


@pytest.mark.asyncio
async def test_create_actor_duplicate_identity_key_is_rejected(db_session) -> None:
    _ = db_session
    plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    await create_actor(
        ActorCreate(
            name="fake-source-one",
            plugin_id=plugin_id,
            config={"namespace": "identity_dup_one", "total_assets": 0},
        )
    )

    with pytest.raises(ApiError) as exc_info:
        await create_actor(
            ActorCreate(
                name="fake-source-two",
                plugin_id=plugin_id,
                config={"namespace": "identity_dup_two", "total_assets": 0},
            )
        )
    assert exc_info.value.status_code == 400
    assert "identity_key" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_update_actor_config_keeps_identity_key_stable(db_session) -> None:
    _ = db_session
    plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    actor = await create_actor(
        ActorCreate(
            name="fake-source-stable-id",
            plugin_id=plugin_id,
            identity_key="fake-source-stable-id",
            config={"namespace": "identity_stable_before", "total_assets": 0},
        )
    )
    updated = await update_actor(
        int(actor.id),
        ActorUpdate(config={"namespace": "identity_stable_after", "total_assets": 0}),
    )
    assert updated.identity_key == "fake-source-stable-id"
