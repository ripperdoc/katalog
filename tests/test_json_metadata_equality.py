from __future__ import annotations

from typing import Any, cast

import pytest

from katalog.constants.metadata import FILE_TAGS, MetadataType, get_metadata_id
from katalog.models import (
    MetadataChanges,
    OpStatus,
    Changeset,
    Metadata,
    make_metadata,
)
from katalog.models.assets import Asset
from katalog.models.metadata import _normalize_metadata_row


@pytest.mark.asyncio
async def test_json_metadata_rejects_non_serializable_values(pipeline_db):
    # sets are not JSON-serializable
    with pytest.raises(ValueError):
        make_metadata(FILE_TAGS, cast(Any, {"a", "b"}), actor_id=1)


@pytest.mark.asyncio
async def test_changed_keys_json_dict_order_does_not_matter(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    loaded = [
        make_metadata(FILE_TAGS, {"a": 1, "b": 2}, actor_id=1, asset=asset)
    ]
    staged = [
        make_metadata(FILE_TAGS, {"b": 2, "a": 1}, actor_id=1, asset=asset)
    ]

    changes = MetadataChanges(asset=asset, loaded=loaded, staged=staged)
    assert changes.changed_keys() == set()


@pytest.mark.asyncio
async def test_changed_keys_json_list_compares_by_value_not_identity(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    loaded = [make_metadata(FILE_TAGS, ["a", "b"], actor_id=1, asset=asset)]
    staged = [
        make_metadata(FILE_TAGS, ["a", "b"], actor_id=1, asset=asset)
    ]  # different list instance

    changes = MetadataChanges(asset=asset, loaded=loaded, staged=staged)
    assert changes.changed_keys() == set()


@pytest.mark.asyncio
async def test_changed_keys_json_detects_actual_change(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    loaded = [
        make_metadata(FILE_TAGS, {"a": 1, "b": 2}, actor_id=1, asset=asset)
    ]
    staged = [
        make_metadata(FILE_TAGS, {"a": 1, "b": 3}, actor_id=1, asset=asset)
    ]

    changes = MetadataChanges(asset=asset, loaded=loaded, staged=staged)
    assert FILE_TAGS in changes.changed_keys()


@pytest.mark.asyncio
async def test_persist_json_does_not_crash_and_dedupes_existing_value(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    existing = make_metadata(FILE_TAGS, ["a", "b"], actor_id=1, asset=asset)
    existing.changeset_id = 1

    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)

    loaded = [existing]
    staged = [make_metadata(FILE_TAGS, ["a", "b"], actor_id=1, asset=asset)]
    changes = MetadataChanges(asset=asset, loaded=loaded, staged=staged)
    to_create, changed = changes.prepare_persist(
        changeset=changeset2,
        existing_metadata=loaded,
    )
    assert changed == set()
    assert to_create == []


@pytest.mark.asyncio
async def test_persist_json_empty_object_is_saved(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    changeset = Changeset(id=3, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_TAGS, {}, actor_id=1, asset=asset)]

    changes = MetadataChanges(asset=asset, loaded=[], staged=staged)
    to_create, changed_keys = changes.prepare_persist(
        changeset=changeset,
        existing_metadata=[],
    )

    assert FILE_TAGS in changed_keys
    assert len(to_create) == 1
    assert to_create[0].value_json == {}


@pytest.mark.asyncio
async def test_persist_json_dedupes_when_existing_value_loaded_as_json_text(pipeline_db):
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    existing_row = {
        "id": 1,
        "asset_id": 1,
        "actor_id": 1,
        "changeset_id": 1,
        "metadata_key_id": int(get_metadata_id(FILE_TAGS)),
        "value_type": int(MetadataType.JSON),
        "value_text": None,
        "value_int": None,
        "value_real": None,
        "value_datetime": None,
        "value_json": "{}",
        "value_relation_id": None,
        "value_collection_id": None,
        "removed": 0,
        "confidence": None,
    }
    existing = Metadata.model_validate(_normalize_metadata_row(existing_row))
    assert existing.value_json == {}

    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_TAGS, {}, actor_id=1, asset=asset)]
    changes = MetadataChanges(asset=asset, loaded=[existing], staged=staged)
    to_create, changed = changes.prepare_persist(
        changeset=changeset2,
        existing_metadata=[existing],
    )

    assert changed == set()
    assert to_create == []
