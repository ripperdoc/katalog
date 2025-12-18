"""Tests for metadata upsert behavior on Asset."""

from typing import Any, Iterable

import pytest
from tortoise import Tortoise

from katalog.metadata import FILE_PATH, MetadataKey, get_metadata_registry_id
from katalog.models import (
    Asset,
    Metadata,
    Provider,
    ProviderType,
    Snapshot,
    make_metadata,
)
from katalog.queries import sync_metadata_registry


async def _init_db() -> None:
    await Tortoise.init(
        db_url="sqlite://:memory:", modules={"models": ["katalog.models"]}
    )
    await Tortoise.generate_schemas()
    await sync_metadata_registry()


async def _teardown_db() -> None:
    await Tortoise.close_connections()


def md(
    provider_id: int, key: MetadataKey, value: Any, *, asset: Asset, snapshot: Snapshot
) -> Metadata:
    return make_metadata(provider_id, key, value, asset=asset, snapshot=snapshot)


async def _create_asset_with_snapshot(
    *, provider_name: str = "provider-1"
) -> tuple[Asset, Snapshot, Provider]:
    provider = await Provider.create(
        name=provider_name,
        plugin_id=f"plugin-{provider_name}",
        type=ProviderType.SOURCE,
    )
    snapshot = await Snapshot.begin(provider)
    asset = await Asset.create(
        provider=provider,
        canonical_id=f"canonical-{provider_name}",
        canonical_uri=f"uri://{provider_name}",
        created_snapshot=snapshot,
        last_snapshot=snapshot,
        deleted_snapshot=None,
    )
    return asset, snapshot, provider


async def _upsert(asset: Asset, snapshot: Snapshot, metadata: Iterable[Metadata]):
    return await asset.upsert(snapshot=snapshot, metadata=list(metadata))


@pytest.mark.asyncio
async def test_upsert_adds_first_metadata_value():
    await _init_db()
    try:
        asset, snapshot, provider = await _create_asset_with_snapshot()
        meta = md(provider.id, FILE_PATH, "/tmp/file1", asset=asset, snapshot=snapshot)

        changes = await _upsert(asset, snapshot, [meta])

        assert {FILE_PATH} == changes
        rows = await Metadata.filter(
            asset=asset,
            metadata_key_id=get_metadata_registry_id(FILE_PATH),
        ).all()
        assert len(rows) == 1
        assert rows[0].value_text == "/tmp/file1"
        assert rows[0].removed is False
    finally:
        await _teardown_db()
