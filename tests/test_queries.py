"""Integration-style tests for list_assets_with_metadata using an in-memory SQLite DB."""

from pathlib import Path

import pytest
import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import FILE_PATH, METADATA_REGISTRY, get_metadata_id
from katalog.models import Asset, Metadata, OpStatus, Provider, ProviderType, Snapshot
from katalog.queries import list_assets_with_metadata, setup_db


@pytest_asyncio.fixture
async def db(tmp_path: Path):
    db_path = tmp_path / "katalog_test.sqlite3"
    await setup_db(db_path)
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.mark.asyncio
async def test_list_assets_with_metadata_filters_and_picks_latest(db: None):
    provider = await Provider.create(
        name="provider-1", plugin_id="plugin-1", type=ProviderType.SOURCE
    )
    other_provider = await Provider.create(
        name="provider-2", plugin_id="plugin-2", type=ProviderType.SOURCE
    )

    snap1 = await Snapshot.create(provider=provider, status=OpStatus.COMPLETED)
    snap2 = await Snapshot.create(provider=provider, status=OpStatus.COMPLETED)
    snap_other = await Snapshot.create(
        provider=other_provider, status=OpStatus.COMPLETED
    )

    asset = await Asset.create(
        provider=provider,
        canonical_id="asset-1",
        canonical_uri="file:///asset-1",
        created_snapshot=snap1,
        last_snapshot=snap2,
    )
    other_asset = await Asset.create(
        provider=other_provider,
        canonical_id="asset-2",
        canonical_uri="file:///asset-2",
        created_snapshot=snap_other,
        last_snapshot=snap_other,
    )

    key_def = METADATA_REGISTRY[FILE_PATH]
    key_id = get_metadata_id(FILE_PATH)
    await Metadata.create(
        asset=asset,
        provider=provider,
        snapshot=snap1,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/old/path",
        removed=False,
    )
    await Metadata.create(
        asset=asset,
        provider=provider,
        snapshot=snap2,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/new/path",
        removed=False,
    )
    await Metadata.create(
        asset=other_asset,
        provider=other_provider,
        snapshot=snap_other,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/other/path",
        removed=False,
    )

    result = await list_assets_with_metadata(provider_id=provider.id)

    assert result["stats"]["assets"] == 1
    assert len(result["assets"]) == 1

    asset_entry = result["assets"][0]
    assert asset_entry["id"] == asset.id
    assert asset_entry["created"] == snap1.id
    assert asset_entry["seen"] == snap2.id
    assert asset_entry["deleted"] is None

    meta = asset_entry["metadata"]
    key_str = str(FILE_PATH)
    assert meta[key_str] == {"value": "/new/path", "count": 2}

    schema = result["schema"][key_str]
    assert schema["registry_id"] == key_id
    assert schema["key"] == key_str
    assert schema["plugin_id"]
