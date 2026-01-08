"""Integration-style tests for list_assets_for_view using an in-memory SQLite DB."""

from pathlib import Path

import pytest
import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import FILE_PATH, METADATA_REGISTRY, get_metadata_id
from katalog.models import Asset, Metadata, OpStatus, Provider, ProviderType, Snapshot
from katalog.queries import list_assets_for_view, setup_db, sync_metadata_registry
from katalog.views import default_view


@pytest_asyncio.fixture
async def db(tmp_path: Path):
    db_path = tmp_path / "katalog_test.sqlite3"
    await setup_db(db_path)
    await sync_metadata_registry()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.mark.asyncio
async def test_list_assets_for_view_filters_and_picks_latest(db: None):
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

    view = default_view()
    result = await list_assets_for_view(
        view, provider_id=provider.id, include_total=True
    )

    assert result["stats"]["returned"] == 1
    assert result["stats"]["total"] == 1
    assert len(result["items"]) == 1

    asset_entry = result["items"][0]
    assert asset_entry[str(FILE_PATH)] == "/new/path"
    assert asset_entry["asset/created_snapshot"] == snap1.id
    assert asset_entry["asset/last_snapshot"] == snap2.id
    assert asset_entry["asset/deleted_snapshot"] is None

    key_str = str(FILE_PATH)
    schema_by_key = {col["key"]: col for col in result["schema"]}
    schema = schema_by_key[key_str]
    assert schema["registry_id"] == key_id
    assert schema["key"] == key_str
    assert schema["plugin_id"]


@pytest.mark.asyncio
async def test_list_assets_for_view_search_allows_special_chars(db: None):
    provider = await Provider.create(
        name="provider-1", plugin_id="plugin-1", type=ProviderType.SOURCE
    )
    snap = await Snapshot.create(provider=provider, status=OpStatus.COMPLETED)
    asset = await Asset.create(
        provider=provider,
        canonical_id="asset-1",
        canonical_uri="file:///asset-1",
        created_snapshot=snap,
        last_snapshot=snap,
    )

    # Populate the FTS table so the MATCH query is actually exercised and can
    # return the asset.
    conn = Tortoise.get_connection("default")
    await conn.execute_query(
        "INSERT INTO asset_search(rowid, doc) VALUES (?, ?)",
        [asset.id, "a-b_c"],
    )

    view = default_view()
    result = await list_assets_for_view(view, search="a-b_c", include_total=True)

    assert result["stats"]["returned"] == 1
    assert result["items"][0]["asset/id"] == asset.id
