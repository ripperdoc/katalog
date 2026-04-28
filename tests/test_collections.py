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
from katalog.api.helpers import ApiError
from katalog.db.asset_collections import get_asset_collection_repo
from katalog.db.changesets import get_changeset_repo
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


@pytest.mark.asyncio
async def test_create_collection_rejects_unknown_asset_ids_without_partial_writes(
    seeded_assets,
):
    _ = seeded_assets

    collection_db = get_asset_collection_repo()
    changeset_db = get_changeset_repo()

    existing_changesets = await changeset_db.list_rows()
    existing_collections = await collection_db.list_rows()
    collection_name = "Invalid Collection IDs"

    with pytest.raises(ApiError) as exc_info:
        await create_collection(
            CollectionCreate(name=collection_name, asset_ids=[0, 999999999])
        )

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert isinstance(detail, dict)
    assert detail.get("message") == "Some asset_ids do not exist"
    assert int(detail.get("missing_count", 0)) >= 1

    # No partial collection row should have been created.
    assert await collection_db.get_or_none(name=collection_name) is None
    updated_collections = await collection_db.list_rows()
    assert len(updated_collections) == len(existing_collections)

    # No "Created collection ..." changeset should be created when validation fails.
    updated_changesets = await changeset_db.list_rows()
    assert len(updated_changesets) == len(existing_changesets)


@pytest.mark.asyncio
async def test_create_collection_rejects_mixed_valid_and_unknown_asset_ids(
    seeded_assets,
):
    _ = seeded_assets

    assets_payload = await list_assets(AssetQuery.model_validate({"view_id": "default"}))
    items = assets_payload.items
    assert items
    valid_asset_id = int(items[0].asset_id)
    missing_asset_id = max(int(item.asset_id) for item in items) + 1

    with pytest.raises(ApiError) as exc_info:
        await create_collection(
            CollectionCreate(
                name="Mixed Invalid Collection IDs",
                asset_ids=[valid_asset_id, missing_asset_id],
            )
        )

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    assert isinstance(detail, dict)
    assert detail.get("message") == "Some asset_ids do not exist"
    assert missing_asset_id in (detail.get("missing_asset_ids") or [])
