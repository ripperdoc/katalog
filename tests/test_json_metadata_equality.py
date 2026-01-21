from __future__ import annotations

from typing import Any, cast

import pytest

from katalog.metadata import FILE_TAGS
from katalog.models import (
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Changeset,
    make_metadata,
)
from tests.utils.pipeline_helpers import PipelineFixture


@pytest.mark.asyncio
async def test_json_metadata_rejects_non_serializable_values(pipeline_db):
    # sets are not JSON-serializable
    with pytest.raises(ValueError):
        make_metadata(FILE_TAGS, cast(Any, {"a", "b"}), provider_id=1)


@pytest.mark.asyncio
async def test_changed_keys_json_dict_order_does_not_matter(pipeline_db):
    fx = await PipelineFixture.create()
    loaded = [fx.metadata(FILE_TAGS, {"a": 1, "b": 2})]
    staged = [fx.metadata(FILE_TAGS, {"b": 2, "a": 1})]

    change_set = MetadataChangeSet(loaded=loaded, staged=staged)
    assert change_set.changed_keys() == set()


@pytest.mark.asyncio
async def test_changed_keys_json_list_compares_by_value_not_identity(pipeline_db):
    fx = await PipelineFixture.create()
    loaded = [fx.metadata(FILE_TAGS, ["a", "b"])]
    staged = [fx.metadata(FILE_TAGS, ["a", "b"])]  # different list instance

    change_set = MetadataChangeSet(loaded=loaded, staged=staged)
    assert change_set.changed_keys() == set()


@pytest.mark.asyncio
async def test_changed_keys_json_detects_actual_change(pipeline_db):
    fx = await PipelineFixture.create()
    loaded = [fx.metadata(FILE_TAGS, {"a": 1, "b": 2})]
    staged = [fx.metadata(FILE_TAGS, {"a": 1, "b": 3})]

    change_set = MetadataChangeSet(loaded=loaded, staged=staged)
    assert FILE_TAGS in change_set.changed_keys()


@pytest.mark.asyncio
async def test_persist_json_does_not_crash_and_dedupes_existing_value(pipeline_db):
    fx = await PipelineFixture.create()

    existing = fx.metadata(FILE_TAGS, ["a", "b"])
    await existing.save()

    # New changeset to simulate a later run.
    changeset2 = await Changeset.create(
        provider=fx.provider, status=OpStatus.IN_PROGRESS
    )

    loaded = await fx.asset.load_metadata()
    staged = [make_metadata(FILE_TAGS, ["a", "b"], provider_id=fx.provider.id)]
    change_set = MetadataChangeSet(loaded=loaded, staged=staged)

    changed = await change_set.persist(asset=fx.asset, changeset=changeset2)
    assert changed == set()

    # Ensure we didn't insert a duplicate row.
    key_id = existing.metadata_key_id
    count = await Metadata.filter(
        asset_id=fx.asset.id,
        provider_id=fx.provider.id,
        metadata_key_id=key_id,
    ).count()
    assert count == 1
