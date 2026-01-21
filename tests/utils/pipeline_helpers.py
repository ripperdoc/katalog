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
    Changeset,
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
    changeset: Changeset
    asset: Asset

    @classmethod
    async def create(cls) -> "PipelineFixture":
        provider = await Provider.create(
            name="source-provider",
            plugin_id="plugin.source",
            type=ProviderType.SOURCE,
        )
        changeset = await Changeset.create(
            provider=provider, status=OpStatus.IN_PROGRESS
        )
        asset = Asset(
            external_id="asset-1",
            canonical_uri="file:///asset-1",
        )
        await asset.save_record(changeset=changeset, provider=provider)
        return cls(provider=provider, changeset=changeset, asset=asset)

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
            changeset=self.changeset,
        )
        return md


@pytest_asyncio.fixture
async def pipeline_db():
    await init_db()
    try:
        yield
    finally:
        await teardown_db()
