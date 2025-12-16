from __future__ import annotations
from typing import Any, cast

import pytest

from katalog.db import Database
from katalog.models import ACCESS_OWNER, AssetRecord, make_metadata


PROVIDER_ID = "test-provider"
PLUGIN_ID = "example.plugin"


def _make_database() -> Database:
    db = Database(":memory:")
    db.initialize_schema()
    db.ensure_source(
        PROVIDER_ID,
        title="Test Provider",
        plugin_id=PLUGIN_ID,
        config={"kind": "test"},
        provider_type="source",
    )
    return db


def _make_asset_record() -> AssetRecord:
    return AssetRecord(
        id="asset-1",
        provider_id=PROVIDER_ID,
        canonical_uri="test://asset-1",
    )


def _metadata_values(provider_id: str, values: list[Any]):
    return [make_metadata(provider_id, ACCESS_OWNER, value) for value in values]


def test_metadata_additions_and_removals_are_tracked():
    db = _make_database()
    record = _make_asset_record()

    snapshot1 = db.begin_snapshot(PROVIDER_ID)
    initial_values = ["alice", "bob", "carol"]
    changed = db.upsert_asset(
        record,
        _metadata_values(PROVIDER_ID, initial_values),
        snapshot1,
    )
    db.finalize_snapshot(snapshot1, status="full")

    assert str(ACCESS_OWNER) in changed

    snapshot2 = db.begin_snapshot(PROVIDER_ID)
    new_values = ["alice", "bob", "dave"]
    changed_second = db.upsert_asset(
        record,
        _metadata_values(PROVIDER_ID, new_values),
        snapshot2,
    )
    db.finalize_snapshot(snapshot2, status="full")

    assert str(ACCESS_OWNER) in changed_second

    entries = db.get_latest_metadata_for_file(record.id, metadata_key=ACCESS_OWNER)
    active = [entry for entry in entries if not entry.removed]
    removed = [entry for entry in entries if entry.removed]

    active_values = sorted(cast(str, entry.value) for entry in active)
    removed_values = sorted(cast(str, entry.value) for entry in removed)

    assert active_values == sorted(new_values)
    assert removed_values == ["carol"]
    db.close()


def test_no_changes_when_values_identical():
    db = _make_database()
    record = _make_asset_record()

    snapshot1 = db.begin_snapshot(PROVIDER_ID)
    values = ["alice", "bob"]
    db.upsert_asset(record, _metadata_values(PROVIDER_ID, values), snapshot1)
    db.finalize_snapshot(snapshot1, status="full")

    snapshot2 = db.begin_snapshot(PROVIDER_ID)
    changed = db.upsert_asset(record, _metadata_values(PROVIDER_ID, values), snapshot2)
    db.finalize_snapshot(snapshot2, status="full")

    entries = db.get_latest_metadata_for_file(record.id, metadata_key=ACCESS_OWNER)
    assert len(entries) == len(values)
    assert changed == set()

    active_values = sorted(
        cast(str, entry.value) for entry in entries if not entry.removed
    )
    assert active_values == sorted(values)
    db.close()


def test_metadata_value_cleared_by_none():
    db = _make_database()
    record = _make_asset_record()

    snapshot1 = db.begin_snapshot(PROVIDER_ID)
    db.upsert_asset(record, _metadata_values(PROVIDER_ID, ["alice"]), snapshot1)
    db.finalize_snapshot(snapshot1, status="full")

    snapshot2 = db.begin_snapshot(PROVIDER_ID)
    changed = db.upsert_asset(record, _metadata_values(PROVIDER_ID, [None]), snapshot2)
    db.finalize_snapshot(snapshot2, status="full")

    assert str(ACCESS_OWNER) in changed

    entries = db.get_latest_metadata_for_file(record.id, metadata_key=ACCESS_OWNER)
    active = [entry for entry in entries if not entry.removed]
    removed = [entry for entry in entries if entry.removed]

    assert not active
    assert [cast(str, entry.value) for entry in removed] == ["alice"]
    db.close()
