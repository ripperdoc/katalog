from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Sequence, TYPE_CHECKING, cast

from tortoise import Tortoise
from tortoise.fields import (
    CharEnumField,
    CharField,
    DatetimeField,
    ForeignKeyField,
    JSONField,
    IntField,
    TextField,
    RESTRICT,
)
from tortoise.models import Model

from katalog.constants.metadata import (
    ASSET_LOST,
    MetadataType,
    get_metadata_id,
    MetadataKey,
)


class DataReader(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(Model):
    id = IntField(pk=True)
    external_id = CharField(max_length=255, unique=True)
    canonical_uri = CharField(max_length=1024, unique=False)
    _metadata_cache: list["Metadata"] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": int(self.id),
            "external_id": self.external_id,
            "canonical_uri": self.canonical_uri,
        }

    async def save_record(
        self,
        changeset: "Changeset",
        actor: Actor | None = None,
    ) -> bool:
        """Persist the asset row, reusing an existing canonical asset when present.

        Returns:
            True if the asset was newly created in the DB, otherwise False.
        """

        if actor is None:
            raise ValueError("actor must be supplied to save_record")

        was_created = False
        if self.id is None:
            existing = await Asset.get_or_none(external_id=self.external_id)
            if existing:
                self.id = existing.id
                self._saved_in_db = True
                # Keep the first-seen canonical_uri; do not overwrite on merge.
                self.canonical_uri = existing.canonical_uri
            else:
                was_created = True
        await self.save()
        return was_created

    async def load_metadata(self) -> Sequence["Metadata"]:
        """Fetch and cache metadata rows for this asset."""
        if self._metadata_cache is not None:
            return self._metadata_cache
        await self.fetch_related("metadata")
        self._metadata_cache = list(getattr(self, "metadata", []))
        return self._metadata_cache

    async def get_data_reader(
        self, key: MetadataKey, changes: "MetadataChanges"
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

        params = entry.value if isinstance(entry.value, dict) else None
        try:
            return cast(DataReader | None, plugin.get_data_reader(self, params=params))
        except Exception:
            logger.exception(
                "Source plugin {actor_id} failed to provide file reader",
                actor_id=actor_id,
            )
            return None

    @classmethod
    async def mark_unseen_as_lost(
        cls,
        *,
        changeset: "Changeset",
        actor_ids: Sequence[int],
        seen_asset_ids: Sequence[int] | None = None,
    ) -> int:
        """
        Mark assets from the given actors as lost if they were not touched by this changeset.
        Returns the number of affected rows (metadata rows written).
        """
        if not actor_ids:
            return 0

        from .metadata import Metadata

        conn = Tortoise.get_connection("default")
        metadata_table = Metadata._meta.db_table
        affected = 0
        seen_set = {int(a) for a in (seen_asset_ids or [])}

        for pid in actor_ids:
            seen_clause = ""
            seen_params: list[int] = []
            if seen_set:
                placeholders = ", ".join("?" for _ in seen_set)
                seen_clause = f"AND asset_id NOT IN ({placeholders})"
                seen_params = list(seen_set)

            rows = await conn.execute_query_dict(
                f"""
                SELECT DISTINCT asset_id
                FROM {metadata_table}
                WHERE actor_id = ?
                  {seen_clause}
                """,
                [pid, *seen_params],
            )
            asset_ids = [int(r["asset_id"]) for r in rows]
            if not asset_ids:
                continue

            lost_key_id = get_metadata_id(ASSET_LOST)
            now_rows = []
            for aid in asset_ids:
                md = Metadata(
                    asset_id=aid,
                    actor_id=pid,
                    changeset_id=changeset.id,
                    metadata_key_id=lost_key_id,
                    value_type=MetadataType.INT,
                    value_int=1,
                    removed=False,
                )
                now_rows.append(md)

            await Metadata.bulk_create(now_rows)
            affected += len(now_rows)

        return affected

    class Meta(Model.Meta):
        # No Asset indexes yet; list_assets_for_view() is driven by metadata queries.
        indexes = ()


class CollectionRefreshMode(str, Enum):
    LIVE = "live"
    ON_DEMAND = "on_demand"


class AssetCollection(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    description = TextField(null=True)
    source = JSONField(null=True)  # opaque JSON describing query/view used to create
    membership_key = ForeignKeyField(
        "models.MetadataRegistry", on_delete=RESTRICT, null=True
    )
    membership_key_id: int | None
    item_count = IntField(default=0)
    refresh_mode = CharEnumField(
        CollectionRefreshMode, default=CollectionRefreshMode.ON_DEMAND
    )
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    def to_dict(self, *, asset_count: int | None = None) -> dict[str, Any]:
        resolved_count = asset_count if asset_count is not None else self.item_count
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "asset_count": resolved_count,
            "source": self.source,
            "membership_key_id": self.membership_key_id,
            "refresh_mode": self.refresh_mode.value
            if isinstance(self.refresh_mode, CollectionRefreshMode)
            else str(self.refresh_mode),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


# Type-checking imports
if TYPE_CHECKING:
    from .core import Actor, Changeset
    from .metadata import Metadata, MetadataChanges
