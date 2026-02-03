"""Tests for MetadataChangeSet current/changed logic."""

from katalog.constants.metadata import FILE_PATH
from katalog.models import MetadataChanges
from tests.utils.metadata_helpers import mem_md, registry_stub


def test_current_metadata_dedup_latest_wins(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=1, actor_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=2, actor_id=2),
    ]

    cs = MetadataChanges(loaded=entries)
    result = cs.current()

    assert list(result.keys()) == [FILE_PATH]
    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].changeset_id == 2
    assert vals[0].actor_id == 2


def test_current_metadata_removed_suppresses_prior(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=1, actor_id=1),
        mem_md(
            key=FILE_PATH,
            value="/tmp/a",
            changeset_id=2,
            actor_id=1,
            removed=True,
        ),
    ]

    cs = MetadataChanges(loaded=entries)
    result = cs.current()

    assert FILE_PATH not in result or result[FILE_PATH] == []


def test_current_metadata_removed_only_target_value(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=1, actor_id=1),
        mem_md(key=FILE_PATH, value="/tmp/b", changeset_id=1, actor_id=1),
        mem_md(
            key=FILE_PATH,
            value="/tmp/a",
            changeset_id=2,
            actor_id=1,
            removed=True,
        ),
    ]

    cs = MetadataChanges(loaded=entries)
    result = cs.current()

    assert list(result.keys()) == [FILE_PATH]
    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].value_text == "/tmp/b"


def test_current_metadata_keeps_distinct_values_ordered_by_changeset(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/b", changeset_id=1, actor_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=3, actor_id=1),
    ]

    cs = MetadataChanges(loaded=entries)
    result = cs.current()

    vals = result[FILE_PATH]
    assert [v.value_text for v in vals] == ["/tmp/a", "/tmp/b"]


def test_current_metadata_merge_actors_same_value_latest_changeset(registry_stub):
    entries = [
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=5, actor_id=1),
        mem_md(key=FILE_PATH, value="/tmp/a", changeset_id=6, actor_id=2),
    ]

    cs = MetadataChanges(loaded=entries)
    result = cs.current()

    vals = result[FILE_PATH]
    assert len(vals) == 1
    assert vals[0].actor_id == 2
    assert vals[0].changeset_id == 6
