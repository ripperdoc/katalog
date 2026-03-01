from __future__ import annotations

import pytest

from katalog.api.assets import list_assets
from katalog.constants.metadata import ASSET_LOST
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import OpStatus, make_metadata
from katalog.models.query import AssetQuery


@pytest.mark.asyncio
async def test_list_assets_allows_dynamic_columns(seeded_assets):
    _ = seeded_assets

    response = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 10,
                "columns": [
                    "asset/id",
                    "asset/external_id",
                    "document/lang",
                    "asset/lost",
                ],
            }
        )
    )

    assert response.items
    first = response.items[0].model_dump(mode="json", by_alias=True)
    assert "document/lang" in first
    assert "asset/lost" in first

    schema_ids = [entry.id for entry in response.schema_]
    assert "document/lang" in schema_ids
    assert "asset/lost" in schema_ids


@pytest.mark.asyncio
async def test_list_assets_allows_dynamic_sort_key(seeded_assets):
    _ = seeded_assets

    response = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 10,
                "columns": ["asset/id", "document/lang"],
                "sort": [["document/lang", "asc"]],
            }
        )
    )

    assert response.items
    first = response.items[0].model_dump(mode="json", by_alias=True)
    assert "document/lang" in first


@pytest.mark.asyncio
async def test_list_assets_excludes_lost_by_default(seeded_assets):
    actor = seeded_assets
    assert actor.id is not None

    all_before = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 200,
                "include_lost_assets": True,
            }
        )
    )
    active_before = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 200,
            }
        )
    )

    asset_id = int(active_before.items[0].asset_id)
    changeset = await get_changeset_repo().create_auto(status=OpStatus.COMPLETED)
    await get_metadata_repo().bulk_create(
        [
            make_metadata(
                ASSET_LOST,
                1,
                actor_id=int(actor.id),
                asset_id=asset_id,
                changeset_id=changeset.id,
            )
        ]
    )

    all_after = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 200,
                "include_lost_assets": True,
            }
        )
    )
    active_after = await list_assets(
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 200,
            }
        )
    )

    all_after_ids = {int(item.asset_id) for item in all_after.items}
    active_after_ids = {int(item.asset_id) for item in active_after.items}

    assert all_after.stats.total == all_before.stats.total
    assert asset_id in all_after_ids
    assert asset_id not in active_after_ids
    assert active_after.stats.total == (active_before.stats.total or 0) - 1


def test_asset_query_rejects_unknown_column_id():
    with pytest.raises(ValueError):
        AssetQuery.model_validate(
            {
                "view_id": "default",
                "offset": 0,
                "limit": 10,
                "columns": ["unknown/field"],
            }
        )
