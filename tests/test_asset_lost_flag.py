"""Tests for asset/lost metadata semantics when clearing and setting."""

from datetime import datetime, UTC

import pytest
import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import ASSET_LOST, METADATA_REGISTRY, get_metadata_id
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Actor,
    ActorType,
    Changeset,
)
from katalog.queries import setup_db, sync_metadata_registry


@pytest_asyncio.fixture
async def db(tmp_path):
    db_path = tmp_path / "katalog_test.sqlite3"
    await setup_db(db_path)
    await sync_metadata_registry()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.mark.asyncio
async def test_clear_lost_flag_when_previous_true(db: None):
    actor = await Actor.create(name="p1", plugin_id="plug", type=ActorType.SOURCE)
    snap1 = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)
    snap2 = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)

    asset = Asset(external_id="a1", canonical_uri="file:///a1")
    await asset.save_record(changeset=snap1, actor=actor)

    lost_key_id = get_metadata_id(ASSET_LOST)
    await Metadata.create(
        asset=asset,
        actor=actor,
        changeset=snap1,
        metadata_key_id=lost_key_id,
        value_type=METADATA_REGISTRY[ASSET_LOST].value_type,
        value_int=1,
        removed=False,
    )

    loaded = await asset.load_metadata()
    change_set = MetadataChangeSet(
        loaded=loaded,
        staged=[
            Metadata(
                metadata_key_id=lost_key_id,
                value_type=METADATA_REGISTRY[ASSET_LOST].value_type,
                removed=False,
                actor_id=actor.id,
            )
        ],
    )
    change_set.staged[0].set_value(None)
    changed = await change_set.persist(asset=asset, changeset=snap2)

    # One new row (removed=True) should be written
    all_rows = await Metadata.filter(asset=asset, metadata_key_id=lost_key_id).order_by(
        "id"
    )
    assert len(all_rows) == 2
    assert any(md.removed is True for md in all_rows)
    # Current state should have no lost flag
    current = MetadataChangeSet._current_metadata(all_rows, actor.id)
    assert ASSET_LOST not in current
    assert ASSET_LOST in changed


@pytest.mark.asyncio
async def test_clear_lost_noop_when_not_set(db: None):
    actor = await Actor.create(name="p2", plugin_id="plug", type=ActorType.SOURCE)
    snap1 = await Changeset.create(actor=actor, status=OpStatus.COMPLETED)
    asset = Asset(external_id="a2", canonical_uri="file:///a2")
    await asset.save_record(changeset=snap1, actor=actor)

    lost_key_id = get_metadata_id(ASSET_LOST)
    loaded = await asset.load_metadata()
    change_set = MetadataChangeSet(
        loaded=loaded,
        staged=[
            Metadata(
                metadata_key_id=lost_key_id,
                value_type=METADATA_REGISTRY[ASSET_LOST].value_type,
                removed=False,
                actor_id=actor.id,
            )
        ],
    )
    change_set.staged[0].set_value(None)
    changed = await change_set.persist(asset=asset, changeset=snap1)

    # No new rows should be written because nothing to clear
    all_rows = await Metadata.filter(asset=asset, metadata_key_id=lost_key_id)
    assert len(all_rows) == 0
    assert ASSET_LOST not in changed
