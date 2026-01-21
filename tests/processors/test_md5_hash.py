from __future__ import annotations

from datetime import UTC, datetime
import hashlib

import pytest

from katalog.metadata import (
    FILE_SIZE,
    HASH_MD5,
    TIME_MODIFIED,
    MetadataDef,
    METADATA_REGISTRY_BY_ID,
    METADATA_REGISTRY,
)
from katalog.models import (
    Asset,
    Actor,
    ActorType,
    MetadataChangeSet,
    make_metadata,
)
from katalog.processors.md5_hash import MD5HashProcessor
from tests.utils.fakes import MemoryAccessor


@pytest.fixture(autouse=True)
def register_metadata_ids():
    """Ensure registry ids exist without patching the lookup helpers."""
    tracked_keys = (HASH_MD5, FILE_SIZE, TIME_MODIFIED)
    originals = {key: METADATA_REGISTRY[key] for key in tracked_keys}
    original_by_id = dict(METADATA_REGISTRY_BY_ID)

    for idx, key in enumerate(tracked_keys, start=1):
        definition = originals[key]
        updated = MetadataDef(
            plugin_id=definition.plugin_id,
            key=key,
            registry_id=idx,
            value_type=definition.value_type,
            title=definition.title,
            description=definition.description,
            width=definition.width,
        )
        METADATA_REGISTRY[key] = updated
        METADATA_REGISTRY_BY_ID[idx] = updated

    yield

    METADATA_REGISTRY.update(originals)
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


def with_hash_cache(asset: Asset) -> Asset:
    md = make_metadata(HASH_MD5, "existing", actor_id=asset.actor_id)
    md.metadata_key_id = METADATA_REGISTRY[HASH_MD5].registry_id
    md.changeset_id = 1
    asset._metadata_cache = [md]
    return asset


def test_should_run_skips_when_hash_already_present_and_no_change():
    processor = MD5HashProcessor(actor=make_actor())
    record = with_hash_cache(make_record())
    cs = MetadataChangeSet(record._metadata_cache or [])
    assert processor.should_run(record, cs) is False


def test_should_run_when_hash_missing():
    processor = MD5HashProcessor(actor=make_actor())
    record = make_record()
    cs = MetadataChangeSet([])
    should_run = processor.should_run(record, cs)
    assert should_run is True


def test_runs_when_fingerprint_changed_even_with_existing_hash():
    processor = MD5HashProcessor(actor=make_actor())
    record = with_hash_cache(make_record())
    cs = MetadataChangeSet(record._metadata_cache or [], staged=[])
    md = make_metadata(FILE_SIZE, 1, actor_id=record.actor_id)
    md.metadata_key_id = METADATA_REGISTRY[FILE_SIZE].registry_id
    md.changeset_id = 2
    cs.add([md])
    assert processor.should_run(record, cs) is True
    cs = MetadataChangeSet(record._metadata_cache or [], staged=[])
    md = make_metadata(
        TIME_MODIFIED,
        datetime(2001, 1, 1, tzinfo=UTC),
        actor_id=record.actor_id,
    )
    md.metadata_key_id = METADATA_REGISTRY[TIME_MODIFIED].registry_id
    md.changeset_id = 2
    cs.add([md])
    assert processor.should_run(record, cs) is True


@pytest.mark.asyncio
async def test_run_computes_expected_hash():
    processor = MD5HashProcessor(actor=make_actor())
    record = make_record()
    payload = b"hello world"
    record.attach_accessor(MemoryAccessor(payload))

    cs = MetadataChangeSet([])
    result = await processor.run(record, cs)

    assert len(result.metadata) == 1
    metadata = result.metadata[0]
    assert metadata.value_text == hashlib.md5(payload).hexdigest()
