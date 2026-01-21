from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, AsyncGenerator, Sequence

import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import MetadataKey, get_metadata_id
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Provider,
    ProviderType,
    Changeset,
    make_metadata,
)
from katalog.queries import sync_metadata_registry


def md(key: MetadataKey, value: Any, removed: bool = False) -> Metadata:
    """Makes a partial Metadata instance for testing."""
    return make_metadata(key, value, removed=removed)


async def _init_db() -> None:
    await Tortoise.init(
        db_url="sqlite://:memory:", modules={"models": ["katalog.models"]}, use_tz=False
    )
    await Tortoise.generate_schemas()
    await sync_metadata_registry()


async def _teardown_db() -> None:
    await Tortoise.close_connections()


@dataclass
class UpsertFixture:
    asset: Asset
    changeset: Changeset
    provider: Provider

    @staticmethod
    async def _ensure_provider(provider_id: int) -> Provider:
        provider, _ = await Provider.get_or_create(
            id=provider_id,
            defaults={
                "name": f"provider-{provider_id}",
                "plugin_id": f"plugin-{provider_id}",
                "type": ProviderType.SOURCE,
            },
        )
        return provider

    @staticmethod
    async def _ensure_changeset(*, provider: Provider, changeset_id: int) -> Changeset:
        changeset, _ = await Changeset.get_or_create(
            id=changeset_id,
            defaults={
                "provider": provider,
                "status": OpStatus.COMPLETED,
                "started_at": datetime.now(UTC),
                "completed_at": datetime.now(UTC),
            },
        )
        return changeset

    @classmethod
    async def create(
        cls, *, provider_id: int = 0, changeset_id: int = 0
    ) -> "UpsertFixture":
        provider = await cls._ensure_provider(provider_id)
        changeset = await cls._ensure_changeset(
            provider=provider, changeset_id=changeset_id
        )
        asset = Asset(
            external_id=f"canonical-{provider_id}",
            canonical_uri=f"uri://{provider_id}",
        )
        await asset.save_record(changeset=changeset, provider=provider)
        return cls(asset=asset, changeset=changeset, provider=provider)

    async def upsert(
        self, *, provider_id: int, changeset_id: int, metas: Sequence[Metadata]
    ) -> set[MetadataKey]:
        provider = await self._ensure_provider(provider_id)
        changeset = await self._ensure_changeset(
            provider=provider, changeset_id=changeset_id
        )
        for m in metas:
            m.provider = provider
            m.changeset = changeset
            m.asset = self.asset
        await self.asset.save_record(changeset=changeset, provider=provider)
        change_set = MetadataChangeSet(
            loaded=await self.asset.load_metadata(), staged=list(metas)
        )
        return await change_set.persist(asset=self.asset, changeset=changeset)

    async def fetch_rows(self, key: MetadataKey) -> list[Metadata]:
        return (
            await Metadata.filter(
                asset=self.asset, metadata_key_id=get_metadata_id(key)
            )
            .order_by("id")
            .all()
        )

    async def add_initial(
        self, provider_id: int, changeset_id: int, metas: Sequence[Metadata]
    ) -> None:
        records: list[Metadata] = []
        provider = await self._ensure_provider(provider_id)
        changeset = await self._ensure_changeset(
            provider=provider, changeset_id=changeset_id
        )
        for m in metas:
            m.provider = provider
            m.changeset = changeset
            m.asset = self.asset
            records.append(m)

        if records:
            await Metadata.bulk_create(records)


@pytest_asyncio.fixture
async def ctx() -> AsyncGenerator[UpsertFixture, None]:
    await _init_db()
    ctx = await UpsertFixture.create()
    try:
        yield ctx
    finally:
        await _teardown_db()
