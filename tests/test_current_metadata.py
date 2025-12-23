"""Tests for MetadataChangeSet current/changed logic."""

from katalog.metadata import FILE_PATH
from katalog.models import MetadataChangeSet
from tests.utils.metadata_helpers import mem_md, registry_stub


def test_current_metadata_dedup_latest_wins(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=1, provider_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=2, provider_id=2),
    ]

    cs = MetadataChangeSet(entries)
    result = cs.current()

    assert list(result.keys()) == [FILE_PATH]
    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].snapshot_id == 2
    assert vals[0].provider_id == 2


def test_current_metadata_removed_suppresses_prior(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=1, provider_id=1),
        mem_md(
            key=FILE_PATH,
            value="/tmp/a",
            snapshot_id=2,
            provider_id=1,
            removed=True,
        ),
    ]

    cs = MetadataChangeSet(entries)
    result = cs.current()

    assert FILE_PATH not in result or result[FILE_PATH] == []


def test_current_metadata_removed_only_target_value(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=1, provider_id=1),
        mem_md(key=FILE_PATH, value="/tmp/b", snapshot_id=1, provider_id=1),
        mem_md(
            key=FILE_PATH,
            value="/tmp/a",
            snapshot_id=2,
            provider_id=1,
            removed=True,
        ),
    ]

    cs = MetadataChangeSet(entries)
    result = cs.current()

    assert list(result.keys()) == [FILE_PATH]
    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].value_text == "/tmp/b"


def test_current_metadata_keeps_distinct_values_ordered_by_snapshot(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/b", snapshot_id=1, provider_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=3, provider_id=1),
    ]

    cs = MetadataChangeSet(entries)
    result = cs.current()

    vals = result[FILE_PATH]
    assert [v.value_text for v in vals] == ["/tmp/a", "/tmp/b"]


def test_current_metadata_merge_providers_same_value_latest_snapshot(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=5, provider_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", snapshot_id=6, provider_id=2),
    ]

    cs = MetadataChangeSet(entries)
    result = cs.current()

    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].provider_id == 2
    assert vals[0].snapshot_id == 6
