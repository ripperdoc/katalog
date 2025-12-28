from __future__ import annotations

from dataclasses import dataclass

import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import MetadataKey
from katalog.models import (
    Asset,
    Metadata,
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    make_metadata,
)
from katalog.queries import sync_metadata_registry


async def init_db() -> None:
    await Tortoise.init(
        db_url="sqlite://:memory:", modules={"models": ["katalog.models"]}, use_tz=False
    )
    await Tortoise.generate_schemas()
    await sync_metadata_registry()


async def teardown_db() -> None:
    await Tortoise.close_connections()


@dataclass
class PipelineFixture:
    provider: Provider
    snapshot: Snapshot
    asset: Asset

    @classmethod
    async def create(cls) -> "PipelineFixture":
        provider = await Provider.create(
            name="source-provider",
            plugin_id="plugin.source",
            type=ProviderType.SOURCE,
        )
        snapshot = await Snapshot.create(provider=provider, status=OpStatus.IN_PROGRESS)
        asset = await Asset.create(
            provider=provider,
            canonical_id="asset-1",
            canonical_uri="file:///asset-1",
            created_snapshot=snapshot,
            last_snapshot=snapshot,
        )
        return cls(provider=provider, snapshot=snapshot, asset=asset)

    def metadata(
        self,
        key: MetadataKey,
        value,
        *,
        removed: bool = False,
    ) -> Metadata:
        md = make_metadata(
            key,
            value,
            provider_id=self.provider.id,
            removed=removed,
            asset=self.asset,
            snapshot=self.snapshot,
        )
        return md


@pytest_asyncio.fixture
async def pipeline_db():
    await init_db()
    try:
        yield
    finally:
        await teardown_db()
