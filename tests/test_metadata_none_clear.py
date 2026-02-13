from __future__ import annotations

import pytest

from katalog.constants.metadata import FILE_NAME, FILE_PATH
from katalog.models import (
    MetadataChanges,
    OpStatus,
    Changeset,
    make_metadata,
)
from katalog.models.assets import Asset


@pytest.mark.asyncio
async def test_persist_none_does_not_write_null_row_when_no_prior_value(
    pipeline_db,
) -> None:
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_PATH, None, actor_id=1, asset=asset)]

    cs = MetadataChanges(asset=asset, loaded=[], staged=staged)
    to_create, changed = cs.prepare_persist(
        changeset=changeset2,
        existing_metadata=[],
    )

    assert changed == set()
    assert to_create == []


@pytest.mark.asyncio
async def test_persist_none_clears_single_existing_value(pipeline_db) -> None:
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    existing = make_metadata(FILE_PATH, "/tmp/a", actor_id=1, asset=asset)
    existing.changeset_id = 1

    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_PATH, None, actor_id=1, asset=asset)]
    cs = MetadataChanges(asset=asset, loaded=[existing], staged=staged)

    to_create, changed = cs.prepare_persist(
        changeset=changeset2,
        existing_metadata=[existing],
    )
    assert FILE_PATH in changed
    assert len(to_create) == 1
    assert to_create[0].removed is True
    assert to_create[0].value_text == "/tmp/a"


@pytest.mark.asyncio
async def test_persist_none_clears_multiple_existing_values(pipeline_db) -> None:
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    a = make_metadata(FILE_PATH, "/tmp/a", actor_id=1, asset=asset)
    b = make_metadata(FILE_PATH, "/tmp/b", actor_id=1, asset=asset)
    a.changeset_id = 1
    b.changeset_id = 1

    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_PATH, None, actor_id=1, asset=asset)]
    cs = MetadataChanges(asset=asset, loaded=[a, b], staged=staged)

    to_create, changed = cs.prepare_persist(
        changeset=changeset2,
        existing_metadata=[a, b],
    )
    assert FILE_PATH in changed
    assert len(to_create) == 2
    assert {row.value_text for row in to_create} == {"/tmp/a", "/tmp/b"}
    assert all(row.removed for row in to_create)


@pytest.mark.asyncio
async def test_missing_key_in_staged_keeps_existing_value_unchanged(
    pipeline_db,
) -> None:
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    existing_path = make_metadata(FILE_PATH, "/tmp/a", actor_id=1, asset=asset)
    existing_path.changeset_id = 1

    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_NAME, "doc.txt", actor_id=1, asset=asset)]
    cs = MetadataChanges(asset=asset, loaded=[existing_path], staged=staged)

    to_create, changed = cs.prepare_persist(
        changeset=changeset2,
        existing_metadata=[existing_path],
    )
    assert FILE_NAME in changed
    assert FILE_PATH not in changed
    assert len(to_create) == 1
    assert to_create[0].value_text == "doc.txt"


@pytest.mark.asyncio
async def test_persist_allows_remove_then_readd_same_value(pipeline_db) -> None:
    asset = Asset(id=1, namespace="test", external_id="a", canonical_uri="file:///a")
    existing_path = make_metadata(FILE_PATH, "/tmp/a", actor_id=1, asset=asset)
    existing_path.changeset_id = 1

    # Remove it.
    changeset2 = Changeset(id=2, status=OpStatus.IN_PROGRESS)
    cs2 = MetadataChanges(
        asset=asset,
        loaded=[existing_path],
        staged=[make_metadata(FILE_PATH, "/tmp/a", actor_id=1, removed=True, asset=asset)],
    )
    to_create2, changed2 = cs2.prepare_persist(
        changeset=changeset2,
        existing_metadata=[existing_path],
    )
    assert FILE_PATH in changed2
    assert len(to_create2) == 1
    assert to_create2[0].removed is True

    # Re-add it (should not be deduped away).
    changeset3 = Changeset(id=3, status=OpStatus.IN_PROGRESS)
    existing_after_remove = [existing_path, to_create2[0]]
    cs3 = MetadataChanges(
        asset=asset,
        loaded=existing_after_remove,
        staged=[make_metadata(FILE_PATH, "/tmp/a", actor_id=1, asset=asset)],
    )
    to_create3, changed3 = cs3.prepare_persist(
        changeset=changeset3,
        existing_metadata=existing_after_remove,
    )
    assert FILE_PATH in changed3
    assert len(to_create3) == 1
    assert to_create3[0].removed is False
