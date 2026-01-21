"""Integration-style tests for list_assets_for_view using an in-memory SQLite DB."""

from pathlib import Path

import pytest
import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import FILE_PATH, METADATA_REGISTRY, get_metadata_id
from katalog.models import Asset, Metadata, OpStatus, Actor, ActorType, Changeset
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
    actor = await Actor.create(
        name="actor-1", plugin_id="plugin-1", type=ActorType.SOURCE
    )
    other_actor = await Actor.create(
        name="actor-2", plugin_id="plugin-2", type=ActorType.SOURCE
    )

    snap1 = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)
    snap2 = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)
    snap_other = await Changeset.create(actor=other_actor, status=OpStatus.COMPLETED)

    asset = Asset(
        external_id="asset-1",
        canonical_uri="file:///asset-1",
    )
    await asset.save_record(changeset=snap1, actor=actor)
    await asset.save_record(changeset=snap2, actor=actor)

    other_asset = Asset(
        external_id="asset-2",
        canonical_uri="file:///asset-2",
    )
    await other_asset.save_record(changeset=snap_other, actor=other_actor)

    key_def = METADATA_REGISTRY[FILE_PATH]
    key_id = get_metadata_id(FILE_PATH)
    await Metadata.create(
        asset=asset,
        actor=actor,
        changeset=snap1,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/old/path",
        removed=False,
    )
    await Metadata.create(
        asset=asset,
        actor=actor,
        changeset=snap2,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/new/path",
        removed=False,
    )
    await Metadata.create(
        asset=other_asset,
        actor=other_actor,
        changeset=snap_other,
        metadata_key_id=key_id,
        value_type=key_def.value_type,
        value_text="/other/path",
        removed=False,
    )

    view = default_view()
    result = await list_assets_for_view(view, actor_id=actor.id, include_total=True)

    assert result["stats"]["returned"] == 1
    assert result["stats"]["total"] == 1
    assert len(result["items"]) == 1

    asset_entry = result["items"][0]
    assert asset_entry[str(FILE_PATH)] == "/new/path"
    key_str = str(FILE_PATH)
    schema_by_key = {col["key"]: col for col in result["schema"]}
    schema = schema_by_key[key_str]
    assert schema["registry_id"] == key_id
    assert schema["key"] == key_str
    assert schema["plugin_id"]


@pytest.mark.asyncio
async def test_list_assets_for_view_search_allows_special_chars(db: None):
    actor = await Actor.create(
        name="actor-1", plugin_id="plugin-1", type=ActorType.SOURCE
    )
    snap = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)
    asset = Asset(
        external_id="asset-1",
        canonical_uri="file:///asset-1",
    )
    await asset.save_record(changeset=snap, actor=actor)

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
