from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, TYPE_CHECKING
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from katalog.constants.metadata import MetadataKey

if TYPE_CHECKING:
    from katalog.models.metadata import Metadata, MetadataChanges


class DataReader(ABC):
    path: str | None = None

    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    canonical_asset_id: int | None = None
    namespace: str
    external_id: str
    canonical_uri: str
    actor_id: int | None = None

    _metadata_cache: list[Metadata] | None = None

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        """
        Resolve a FileReader for this asset using current metadata for the given key.
        """
        from loguru import logger

        from katalog.plugins.registry import get_actor_instance
        from katalog.sources.base import SourcePlugin

        current = changes.current()
        entries = current.get(key, [])
        if not entries:
            return None
        if len(entries) > 1:
            logger.warning(
                "Multiple metadata entries for {key} on asset {asset_id}; using newest",
                key=key,
                asset_id=self.id,
            )
        entry = entries[0]
        actor_id = entry.actor_id
        if actor_id is None:
            return None

        try:
            plugin = await get_actor_instance(actor_id)
        except Exception:
            logger.exception(
                "Failed to resolve plugin for actor {actor_id}", actor_id=actor_id
            )
            return None

        if not isinstance(plugin, SourcePlugin):
            logger.warning(
                "Actor {actor_id} is not a SourcePlugin; cannot read data",
                actor_id=actor_id,
            )
            return None

        try:
            return await plugin.get_data_reader(key, changes)
        except Exception:
            logger.exception(
                "Source plugin {actor_id} failed to provide file reader",
                actor_id=actor_id,
            )
            return None



class CollectionRefreshMode(str, Enum):
    LIVE = "live"
    ON_DEMAND = "on_demand"


class AssetCollection(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    name: str
    description: str | None = None
    source: dict[str, Any] | None = None
    membership_key_id: int | None = None
    asset_count: int = Field(default=0, validation_alias="item_count")
    refresh_mode: CollectionRefreshMode = CollectionRefreshMode.ON_DEMAND
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_serializer("refresh_mode")
    def _serialize_refresh_mode(self, value: CollectionRefreshMode) -> str:
        return value.value if isinstance(value, CollectionRefreshMode) else str(value)
