from __future__ import annotations

import pytest

from katalog.api.assets import list_assets
from katalog.api.changesets import create_changeset, list_changeset_changes
from katalog.api.collections import (
    CollectionCreate,
    CollectionRemoveAssets,
    create_collection,
    list_collection_assets,
    remove_collection_assets,
)
from katalog.models.query import AssetQuery


@pytest.mark.asyncio
async def test_remove_asset_from_collection(seeded_assets):
    _ = seeded_assets

    assets_payload = await list_assets(AssetQuery.model_validate({"view_id": "default"}))
    items = assets_payload.items
    assert len(items) >= 2

    asset_ids = [int(item.asset_id) for item in items[:2]]

    collection_payload = await create_collection(
        CollectionCreate(name="Test Collection", asset_ids=asset_ids)
    )
    collection_id = collection_payload.id
    assert collection_id is not None

    list_payload = await list_collection_assets(
        collection_id=collection_id,
        query=AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 1000,
            }
        ),
    )
    assert list_payload.stats.total == 2
    assert len(list_payload.items) == 2

    changeset = await create_changeset()
    changeset_id = changeset.id
    assert changeset_id is not None

    remove_payload = await remove_collection_assets(
        collection_id,
        CollectionRemoveAssets(asset_ids=[asset_ids[0]], changeset_id=changeset_id),
    )
    assert remove_payload.removed == 1

    remove_again = await remove_collection_assets(
        collection_id,
        CollectionRemoveAssets(asset_ids=[asset_ids[0]], changeset_id=changeset_id),
    )
    assert remove_again.removed == 0

    list_payload = await list_collection_assets(
        collection_id=collection_id,
        query=AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 1000,
            }
        ),
    )
    assert list_payload.stats.total == 1
    assert len(list_payload.items) == 1

    changes_payload = await list_changeset_changes(changeset_id, offset=0, limit=200)
    removed_rows = [row for row in changes_payload.items if row.removed]
    assert len(removed_rows) == 1
