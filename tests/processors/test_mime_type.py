from __future__ import annotations

import pytest

from katalog.constants.metadata import (
    HASH_MD5,
    FILE_TYPE,
    MetadataDef,
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
)
from katalog.models import Actor, ActorType, Asset, MetadataChanges, make_metadata
from katalog.processors.mime_type import MimeTypeProcessor


@pytest.fixture(autouse=True)
def register_file_type_id():
    """Ensure registry ids exist for MIME + hash keys without patching lookup helpers."""
    original_file_type = METADATA_REGISTRY[FILE_TYPE]
    original_hash_md5 = METADATA_REGISTRY[HASH_MD5]
    original_by_id = dict(METADATA_REGISTRY_BY_ID)

    updated_file_type = MetadataDef(
        plugin_id=original_file_type.plugin_id,
        key=FILE_TYPE,
        registry_id=1,
        value_type=original_file_type.value_type,
        title=original_file_type.title,
        description=original_file_type.description,
        width=original_file_type.width,
    )
    updated_hash_md5 = MetadataDef(
        plugin_id=original_hash_md5.plugin_id,
        key=HASH_MD5,
        registry_id=2,
        value_type=original_hash_md5.value_type,
        title=original_hash_md5.title,
        description=original_hash_md5.description,
        width=original_hash_md5.width,
    )
    METADATA_REGISTRY[FILE_TYPE] = updated_file_type
    METADATA_REGISTRY[HASH_MD5] = updated_hash_md5
    METADATA_REGISTRY_BY_ID[1] = updated_file_type
    METADATA_REGISTRY_BY_ID[2] = updated_hash_md5

    yield

    METADATA_REGISTRY[FILE_TYPE] = original_file_type
    METADATA_REGISTRY[HASH_MD5] = original_hash_md5
    METADATA_REGISTRY_BY_ID.clear()
    METADATA_REGISTRY_BY_ID.update(original_by_id)


def make_actor() -> Actor:
    return Actor(id=1, name="p", plugin_id="p", type=ActorType.SOURCE)


def make_record() -> Asset:
    return Asset(
        id=1,
        actor_id=1,
        namespace="test",
        external_id="cid",
        canonical_uri="uri://file",
    )


def test_should_run_when_mime_missing():
    processor = MimeTypeProcessor(actor=make_actor())
    record = make_record()
    changes = MetadataChanges(loaded=[])
    assert processor.should_run(record, changes) is True


def test_should_skip_when_mime_present_and_no_change():
    processor = MimeTypeProcessor(actor=make_actor())
    record = make_record()
    md = make_metadata(FILE_TYPE, "text/plain", actor_id=record.actor_id)
    md.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    md.changeset_id = 1
    changes = MetadataChanges(loaded=[md])
    assert processor.should_run(record, changes) is False


def test_should_skip_when_only_octet_stream_and_disabled():
    processor = MimeTypeProcessor(actor=make_actor(), run_on_octet_stream=False)
    record = make_record()
    md = make_metadata(FILE_TYPE, "application/octet-stream", actor_id=99)
    md.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    md.changeset_id = 1
    changes = MetadataChanges(loaded=[md])
    assert processor.should_run(record, changes) is False


def test_should_run_when_only_octet_stream_and_enabled():
    processor = MimeTypeProcessor(actor=make_actor(), run_on_octet_stream=True)
    record = make_record()
    md = make_metadata(FILE_TYPE, "application/octet-stream", actor_id=99)
    md.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    md.changeset_id = 1
    changes = MetadataChanges(loaded=[md])
    assert processor.should_run(record, changes) is True


def test_should_skip_when_processor_already_wrote_file_type():
    processor = MimeTypeProcessor(actor=make_actor(), run_on_octet_stream=True)
    record = make_record()
    md = make_metadata(FILE_TYPE, "application/octet-stream", actor_id=processor.actor.id)
    md.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    md.changeset_id = 1
    changes = MetadataChanges(loaded=[md])
    assert processor.should_run(record, changes) is False


def test_should_run_when_hash_changed():
    processor = MimeTypeProcessor(actor=make_actor(), run_on_octet_stream=False)
    record = make_record()

    existing_type = make_metadata(FILE_TYPE, "text/plain", actor_id=processor.actor.id)
    existing_type.metadata_key_id = METADATA_REGISTRY[FILE_TYPE].registry_id
    existing_type.changeset_id = 1

    old_hash = make_metadata(HASH_MD5, "aaaa", actor_id=record.actor_id)
    old_hash.metadata_key_id = METADATA_REGISTRY[HASH_MD5].registry_id
    old_hash.changeset_id = 1

    new_hash = make_metadata(HASH_MD5, "bbbb", actor_id=record.actor_id)
    new_hash.metadata_key_id = METADATA_REGISTRY[HASH_MD5].registry_id
    new_hash.changeset_id = 2

    changes = MetadataChanges(loaded=[existing_type, old_hash], staged=[new_hash])
    assert processor.should_run(record, changes) is True
