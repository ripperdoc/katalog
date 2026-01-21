from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, AsyncGenerator, Sequence

import pytest_asyncio
from tortoise import Tortoise

from katalog.constants.metadata import MetadataKey, get_metadata_id
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Actor,
    ActorType,
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
    actor: Actor

    @staticmethod
    async def _ensure_actor(actor_id: int) -> Actor:
        actor, _ = await Actor.get_or_create(
            id=actor_id,
            defaults={
                "name": f"actor-{actor_id}",
                "plugin_id": f"plugin-{actor_id}",
                "type": ActorType.SOURCE,
            },
        )
        return actor

    @staticmethod
    async def _ensure_changeset(*, actor: Actor, changeset_id: int) -> Changeset:
        changeset, _ = await Changeset.get_or_create(
            id=changeset_id,
            defaults={
                "actor": actor,
                "status": OpStatus.COMPLETED,
                "started_at": datetime.now(UTC),
                "completed_at": datetime.now(UTC),
            },
        )
        return changeset

    @classmethod
    async def create(
        cls, *, actor_id: int = 0, changeset_id: int = 0
    ) -> "UpsertFixture":
        actor = await cls._ensure_actor(actor_id)
        changeset = await cls._ensure_changeset(actor=actor, changeset_id=changeset_id)
        asset = Asset(
            external_id=f"canonical-{actor_id}",
            canonical_uri=f"uri://{actor_id}",
        )
        await asset.save_record(changeset=changeset, actor=actor)
        return cls(asset=asset, changeset=changeset, actor=actor)

    async def upsert(
        self, *, actor_id: int, changeset_id: int, metas: Sequence[Metadata]
    ) -> set[MetadataKey]:
        actor = await self._ensure_actor(actor_id)
        changeset = await self._ensure_changeset(actor=actor, changeset_id=changeset_id)
        for m in metas:
            m.actor = actor
            m.changeset = changeset
            m.asset = self.asset
        await self.asset.save_record(changeset=changeset, actor=actor)
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
        self, actor_id: int, changeset_id: int, metas: Sequence[Metadata]
    ) -> None:
        records: list[Metadata] = []
        actor = await self._ensure_actor(actor_id)
        changeset = await self._ensure_changeset(actor=actor, changeset_id=changeset_id)
        for m in metas:
            m.actor = actor
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
