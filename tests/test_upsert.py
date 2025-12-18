"""Tests for metadata upsert behavior on Asset."""

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, AsyncGenerator, Iterable

import pytest
import pytest_asyncio
from tortoise import Tortoise

from katalog.metadata import FILE_PATH, MetadataKey, get_metadata_id
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


@dataclass
class UpsertFixture:
    asset: Asset
    snapshot: Snapshot
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
    async def _ensure_snapshot(*, provider: Provider, snapshot_id: int) -> Snapshot:
        snapshot, _ = await Snapshot.get_or_create(
            id=snapshot_id,
            defaults={
                "provider": provider,
                "status": OpStatus.COMPLETED,
                "started_at": datetime.now(UTC),
                "completed_at": datetime.now(UTC),
            },
        )
        return snapshot

    @classmethod
    async def create(
        cls, *, provider_id: int = 0, snapshot_id: int = 0
    ) -> "UpsertFixture":
        provider = await cls._ensure_provider(provider_id)
        snapshot = await cls._ensure_snapshot(
            provider=provider, snapshot_id=snapshot_id
        )
        asset = await Asset.create(
            provider=provider,
            canonical_id=f"canonical-{provider_id}",
            canonical_uri=f"uri://{provider_id}",
            created_snapshot=snapshot,
            last_snapshot=snapshot,
            deleted_snapshot=None,
        )
        return cls(asset=asset, snapshot=snapshot, provider=provider)

    async def upsert_file_paths(
        self, provider_id: int, snapshot_id: int, paths: Iterable[str]
    ) -> set[MetadataKey]:
        provider = await self._ensure_provider(provider_id)
        snapshot = await self._ensure_snapshot(
            provider=provider, snapshot_id=snapshot_id
        )
        metas = [
            md(
                provider.id,
                FILE_PATH,
                path,
                asset=self.asset,
                snapshot=snapshot,
            )
            for path in paths
        ]
        return await self.asset.upsert(snapshot=snapshot, metadata=metas)

    async def fetch_file_path_rows(self) -> list[Metadata]:
        return (
            await Metadata.filter(
                asset=self.asset, metadata_key_id=get_metadata_id(FILE_PATH)
            )
            .order_by("id")
            .all()
        )

    async def seed_initial_file_paths(
        self, provider_id: int, snapshot_id: int, paths: Iterable[str]
    ) -> None:
        """Insert initial file path metadata rows for this asset.

        entries are tuples of (provider_id, snapshot_id, path).
        Providers/snapshots are created if they do not already exist.
        """

        records: list[Metadata] = []
        provider = await self._ensure_provider(provider_id)
        snapshot = await self._ensure_snapshot(
            provider=provider, snapshot_id=snapshot_id
        )
        for path in paths:
            records.append(
                md(
                    provider.id,
                    FILE_PATH,
                    path,
                    asset=self.asset,
                    snapshot=snapshot,
                )
            )

        if records:
            await Metadata.bulk_create(records)


@pytest_asyncio.fixture
async def upsert_ctx() -> AsyncGenerator[UpsertFixture, None]:
    await _init_db()
    ctx = await UpsertFixture.create()
    try:
        yield ctx
    finally:
        await _teardown_db()


@pytest.mark.asyncio
async def test_upsert_adds_first_metadata_value(ctx: UpsertFixture):
    changes = await ctx.upsert_file_paths(
        provider_id=0, snapshot_id=1, paths=["/tmp/file1"]
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_file_path_rows()
    assert len(rows) == 1
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].snapshot_id == 1  # type: ignore


@pytest.mark.asyncio
async def test_upsert_doesnt_add_duplicate(ctx: UpsertFixture):
    await ctx.seed_initial_file_paths(
        provider_id=0, snapshot_id=1, paths=["/tmp/file1"]
    )

    changes = await ctx.upsert_file_paths(
        provider_id=0, snapshot_id=2, paths=["/tmp/file1"]
    )

    assert not changes
    rows = await ctx.fetch_file_path_rows()
    assert len(rows) == 1
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].snapshot_id == 1  # type: ignore


@pytest.mark.asyncio
async def test_upsert_different_value_adds_second_value(ctx: UpsertFixture):
    await ctx.seed_initial_file_paths(
        provider_id=0, snapshot_id=1, paths=["/tmp/file1"]
    )

    changes = await ctx.upsert_file_paths(
        provider_id=1, snapshot_id=2, paths=["/tmp/file2"]
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_file_path_rows()
    assert len(rows) == 2
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].snapshot_id == 1  # type: ignore
    assert rows[1].value_text == "/tmp/file2"
    assert rows[1].removed is False
    assert rows[1].snapshot_id == 2  # type: ignore


@pytest.mark.asyncio
async def test_upsert_multiple_values_adds_only_new(ctx: UpsertFixture):
    await ctx.seed_initial_file_paths(
        provider_id=0, snapshot_id=1, paths=["/tmp/file1"]
    )
    await ctx.seed_initial_file_paths(
        provider_id=0, snapshot_id=2, paths=["/tmp/file2"]
    )

    changes = await ctx.upsert_file_paths(
        provider_id=0, snapshot_id=3, paths=["/tmp/file2", "/tmp/file3"]
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_file_path_rows()
    assert len(rows) == 3
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].snapshot_id == 1  # type: ignore
    assert rows[1].value_text == "/tmp/file2"
    assert rows[1].removed is False
    assert rows[1].snapshot_id == 2  # type: ignore
    assert rows[2].value_text == "/tmp/file3"
    assert rows[2].removed is False
    assert rows[2].snapshot_id == 3  # type: ignore
