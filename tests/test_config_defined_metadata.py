from __future__ import annotations

from uuid import uuid4

import pytest

from katalog.api.actors import ActorCreate, create_actor
from katalog.api.helpers import ApiError
from katalog.constants.metadata import (
    FILE_TITLE,
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataKey,
    MetadataType,
    get_metadata_def_by_key,
)
from katalog.db.actors import get_actor_repo
from katalog.db.metadata import sync_config_db
from katalog.models import ActorType
from katalog.sources.fake_assets import FakeAssetSource


def _install_config_metadata_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _defs(cls, config):
        namespace = str((config or {}).get("namespace") or "fake")
        return [
            {
                "key": f"test/{namespace}_product_name",
                "value_type": MetadataType.STRING,
                "title": "Product name",
                "description": "Config-defined metadata key for tests.",
            }
        ]

    monkeypatch.setattr(
        FakeAssetSource,
        "metadata_definitions_from_config",
        classmethod(_defs),
    )


def _cleanup_registry_key(raw_key: str) -> None:
    key = MetadataKey(raw_key)
    definition = METADATA_REGISTRY.pop(key, None)
    if definition is not None and definition.registry_id is not None:
        METADATA_REGISTRY_BY_ID.pop(int(definition.registry_id), None)


@pytest.mark.asyncio
async def test_create_actor_syncs_config_defined_metadata(db_session, monkeypatch):
    _ = db_session
    _install_config_metadata_hook(monkeypatch)

    namespace = f"cfg_meta_create_{uuid4().hex[:10]}"
    metadata_key = f"test/{namespace}_product_name"
    try:
        actor = await create_actor(
            ActorCreate(
                name="config-metadata-create",
                plugin_id=FakeAssetSource.plugin_id,
                config={"namespace": namespace, "total_assets": 0},
            )
        )
        assert actor.plugin_id == FakeAssetSource.plugin_id
        definition = get_metadata_def_by_key(MetadataKey(metadata_key))
        assert definition.plugin_id == FakeAssetSource.plugin_id
        assert definition.value_type == MetadataType.STRING
        assert definition.registry_id is not None
    finally:
        _cleanup_registry_key(metadata_key)


@pytest.mark.asyncio
async def test_sync_config_db_backfills_actor_config_metadata(db_session, monkeypatch):
    _ = db_session
    _install_config_metadata_hook(monkeypatch)

    namespace = f"cfg_meta_sync_{uuid4().hex[:10]}"
    metadata_key = f"test/{namespace}_product_name"
    actor_repo = get_actor_repo()

    try:
        await actor_repo.create(
            name="config-metadata-sync",
            plugin_id=FakeAssetSource.plugin_id,
            type=ActorType.SOURCE,
            config={"namespace": namespace, "total_assets": 0},
        )
        assert MetadataKey(metadata_key) not in METADATA_REGISTRY

        await sync_config_db()

        definition = get_metadata_def_by_key(MetadataKey(metadata_key))
        assert definition.plugin_id == FakeAssetSource.plugin_id
        assert definition.value_type == MetadataType.STRING
        assert definition.registry_id is not None
    finally:
        _cleanup_registry_key(metadata_key)


@pytest.mark.asyncio
async def test_create_actor_allows_reusing_existing_metadata_key(
    db_session, monkeypatch
):
    _ = db_session

    def _defs(cls, config):
        _ = cls, config
        return [
            {
                "key": str(FILE_TITLE),
                "value_type": MetadataType.STRING,
                "title": "Title",
            }
        ]

    monkeypatch.setattr(
        FakeAssetSource,
        "metadata_definitions_from_config",
        classmethod(_defs),
    )

    actor = await create_actor(
        ActorCreate(
            name="config-metadata-reuse-existing",
            plugin_id=FakeAssetSource.plugin_id,
            config={"namespace": "reuse_existing", "total_assets": 0},
        )
    )
    assert actor.plugin_id == FakeAssetSource.plugin_id
    definition = get_metadata_def_by_key(MetadataKey(str(FILE_TITLE)))
    assert definition.plugin_id == "katalog.metadata"
    assert definition.value_type == MetadataType.STRING


@pytest.mark.asyncio
async def test_create_actor_reused_key_type_mismatch_still_errors(
    db_session, monkeypatch
):
    _ = db_session

    def _defs(cls, config):
        _ = cls, config
        return [
            {
                "key": str(FILE_TITLE),
                "value_type": MetadataType.INT,
                "title": "Title as int (invalid)",
            }
        ]

    monkeypatch.setattr(
        FakeAssetSource,
        "metadata_definitions_from_config",
        classmethod(_defs),
    )

    with pytest.raises(ApiError) as exc:
        await create_actor(
            ActorCreate(
                name="config-metadata-type-mismatch",
                plugin_id=FakeAssetSource.plugin_id,
                config={"namespace": "reuse_mismatch", "total_assets": 0},
            )
        )
    assert exc.value.status_code == 400
    assert "different type" in str(exc.value.detail)
