from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from katalog.constants.metadata import MetadataKey
from katalog.models import (
    Asset,
    Metadata,
    OpStatus,
    Actor,
    ActorType,
    make_metadata,
)
from katalog.db.metadata import sync_config_db
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo


async def init_db() -> None:
    await sync_config_db()


@dataclass
class PipelineFixture:
    actor: Actor
    changeset: "Changeset"
    asset: Asset
    @classmethod
    async def create(cls) -> "PipelineFixture":
        actor_db = get_actor_repo()
        changeset_db = get_changeset_repo()
        asset_db = get_asset_repo()
        actor = await actor_db.create(
            name="source-actor",
            plugin_id="plugin.source",
            type=ActorType.SOURCE,
        )
        changeset = await changeset_db.create_auto(status=OpStatus.IN_PROGRESS)
        await changeset_db.add_actors(changeset, [actor])
        asset = Asset(
            namespace="test",
            external_id="asset-1",
            canonical_uri="file:///asset-1",
            actor_id=actor.id,
        )
        await asset_db.save_record(asset, changeset=changeset, actor=actor)
        return cls(actor=actor, changeset=changeset, asset=asset)

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
            actor_id=self.actor.id,
            removed=removed,
            asset=self.asset,
            changeset=self.changeset,
        )
        return md

    async def load_metadata(self) -> list[Metadata]:
        asset_db = get_asset_repo()
        return list(await asset_db.load_metadata(self.asset, include_removed=True))


if TYPE_CHECKING:
    from katalog.models import Changeset
