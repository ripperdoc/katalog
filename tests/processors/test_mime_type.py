from __future__ import annotations

import pytest

from katalog.constants.metadata import (
    FILE_TYPE,
    MetadataDef,
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
)
from katalog.models import Actor, ActorType, Asset, MetadataChanges, make_metadata
from katalog.processors.mime_type import MimeTypeProcessor


@pytest.fixture(autouse=True)
def register_file_type_id():
    """Ensure registry ids exist for FILE_TYPE without patching lookup helpers."""
    original = METADATA_REGISTRY[FILE_TYPE]
    original_by_id = dict(METADATA_REGISTRY_BY_ID)

    updated = MetadataDef(
        plugin_id=original.plugin_id,
        key=FILE_TYPE,
        registry_id=1,
        value_type=original.value_type,
        title=original.title,
        description=original.description,
        width=original.width,
    )
    METADATA_REGISTRY[FILE_TYPE] = updated
    METADATA_REGISTRY_BY_ID[1] = updated

    yield

    METADATA_REGISTRY[FILE_TYPE] = original
    METADATA_REGISTRY_BY_ID.clear()
    METADATA_REGISTRY_BY_ID.update(original_by_id)


def make_actor() -> Actor:
    return Actor(id=1, name="p", plugin_id="p", type=ActorType.SOURCE)


def make_record() -> Asset:
    asset = Asset()
    asset.id = 1
    asset.actor_id = 1
    asset.external_id = "cid"
    asset.canonical_uri = "uri://file"
    return asset


def test_should_run_when_mime_missing():
    processor = MimeTypeProcessor(actor=make_actor())
    record = make_record()
    changes = MetadataChanges([])
    assert processor.should_run(record, changes) is True


def test_should_skip_when_mime_present_and_no_change():
    processor = MimeTypeProcessor(actor=make_actor())
    record = make_record()
    md = make_metadata(FILE_TYPE, "text/plain", actor_id=record.actor_id)
    md.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    md.changeset_id = 1
    changes = MetadataChanges([md])
    assert processor.should_run(record, changes) is False
