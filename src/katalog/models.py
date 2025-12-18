from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum, IntEnum
from time import time

from typing import Any, Mapping, Sequence

from tortoise.transactions import in_transaction
from tortoise.fields import (
    CASCADE,
    IntEnumField,
    CharEnumField,
    BigIntField,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    JSONField,
    IntField,
    BooleanField,
    TextField,
    RESTRICT,
    SET_NULL,
)
from tortoise.models import Model

from katalog.config import config_file
from katalog.metadata import (
    MetadataKey,
    MetadataScalar,
    MetadataType,
    ensure_value_type,
    get_metadata_def,
    get_metadata_def_by_registry_id,
    get_metadata_id,
)
from katalog.utils.utils import fqn

"""Data usage notes
- The target profile for this system is to handle metadata for 1 million files. Actual file contents is not to be stored in the DB.
- This implies
- ~1 million Asset records
- ~30 million Metadata records (assuming an average of 30 metadata entries per asset). 
Metadata will mostly be shorter text and date values, but some fields may grow pretty large, such as text contents, summaries, etc.
- 10 to 100 Providers
- As data changes over time, snapshots will be created, increasing the number of Metadata rows per asset. 
On the other hand, users will be encouraged to purge snapshots regularly.
"""


class OpStatus(Enum):
    IN_PROGRESS = "in_progress"
    PARTIAL = "partial"
    COMPLETED = "completed"
    CANCELED = "canceled"
    SKIPPED = "skipped"
    ERROR = "error"


class ProviderType(IntEnum):
    SOURCE = 0
    PROCESSOR = 1
    ANALYZER = 2
    EDITOR = 3
    EXPORTER = 4


class Provider(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    plugin_id = CharField(max_length=255, null=True)
    class_path = CharField(max_length=1024, null=True)
    config = JSONField(null=True)
    type = IntEnumField(ProviderType)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    @classmethod
    async def sync_db(cls, id: int, name: str) -> None:
        for section, ptype in (
            ("sources", ProviderType.SOURCE),
            ("processors", ProviderType.PROCESSOR),
            ("analyzers", ProviderType.ANALYZER),
        ):
            for entry in (config_file or {}).get(section, []) or []:
                entry_name = entry.get("name") or entry.get("class_path")
                if not entry_name:
                    continue
                if await cls.get_or_none(name=entry_name):
                    continue
                await cls.create(
                    name=entry_name,
                    type=ptype,
                    plugin_id=entry.get("plugin_id"),
                    class_path=entry.get("class_path"),
                    config=dict(entry),
                )


class Snapshot(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(
        fqn(Provider), related_name="snapshots", on_delete=CASCADE
    )
    started_at = DatetimeField(default=lambda: datetime.now(UTC))
    completed_at = DatetimeField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    metadata = JSONField(null=True)

    @classmethod
    async def begin(
        cls,
        provider: Provider | int,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: int | None = None,
    ) -> "Snapshot":
        provider_id = provider.id if isinstance(provider, Provider) else provider
        snapshot_id = snapshot_id or int(time())
        if await cls.get_or_none(id=snapshot_id):
            raise ValueError(f"Snapshot with id {snapshot_id} already exists")
        return await cls.create(
            id=snapshot_id,
            provider_id=provider_id,
            status=status,
            metadata=dict(metadata) if metadata else None,
        )

    async def finalize(
        self, *, status: OpStatus, stats: SnapshotStats | None = None
    ) -> None:
        completed_at = datetime.now(UTC)
        provider_id = self.provider
        metadata_payload: dict[str, Any] | None = None
        if stats is not None or self.metadata is not None:
            metadata_payload = dict(self.metadata or {})
            if stats is not None:
                metadata_payload["stats"] = stats.to_dict()
            self.metadata = metadata_payload

        async with in_transaction():
            update_fields = ["completed_at", "status"]
            if metadata_payload is not None:
                update_fields.append("metadata")
            self.status = status
            self.completed_at = completed_at
            await self.save(update_fields=update_fields)

            await Asset.filter(
                provider_id=provider_id,
                deleted_snapshot_id__isnull=True,
                last_snapshot_id__lt=self.id,
            ).update(deleted_snapshot_id=self.id)

            await Provider.filter(id=provider_id).update(updated_at=completed_at)

    class Meta(Model.Meta):
        indexes = (("provider", "started_at"),)


@dataclass(slots=True)
class SnapshotStats:
    assets_seen: int = 0
    assets_changed: int = 0
    assets_added: int = 0
    assets_modified: int = 0
    assets_deleted: int = 0
    assets_ignored: int = 0
    assets_processed: int = 0

    metadata_values_affected: int = 0
    metadata_values_added: int = 0
    metadata_values_removed: int = 0

    relations_affected: int = 0
    relations_added: int = 0
    relations_removed: int = 0

    processings_started: int = 0
    processings_completed: int = 0
    processings_partial: int = 0
    processings_cancelled: int = 0
    processings_skipped: int = 0
    processings_error: int = 0

    _changed_assets: set[int] = field(default_factory=set, init=False, repr=False)
    _added_assets: set[int] = field(default_factory=set, init=False, repr=False)
    _modified_assets: set[int] = field(default_factory=set, init=False, repr=False)

    def record_asset_change(self, asset_id: int, *, added: bool) -> None:
        if added:
            if asset_id not in self._added_assets:
                self.assets_added += 1
                self._added_assets.add(asset_id)
        else:
            if (
                asset_id not in self._added_assets
                and asset_id not in self._modified_assets
            ):
                self.assets_modified += 1
                self._modified_assets.add(asset_id)
        if asset_id not in self._changed_assets:
            self.assets_changed += 1
            self._changed_assets.add(asset_id)

    def record_metadata_diff(self, added: int, removed: int) -> None:
        if not added and not removed:
            return
        self.metadata_values_added += added
        self.metadata_values_removed += removed
        self.metadata_values_affected += added + removed

    def record_relationship_diff(self, added: int, removed: int) -> None:
        if not added and not removed:
            return
        self.relations_added += added
        self.relations_removed += removed
        self.relations_affected += added + removed

    def to_dict(self) -> dict[str, Any]:
        assets_not_changed = max(
            self.assets_seen - self.assets_changed - self.assets_ignored, 0
        )
        assets_not_processed = max(
            self.assets_seen - self.assets_processed - self.assets_ignored, 0
        )
        return {
            "assets": {
                "seen": self.assets_seen,
                "changed": {
                    "total": self.assets_changed,
                    "added": self.assets_added,
                    "modified": self.assets_modified,
                    "deleted": self.assets_deleted,
                },
                "not_changed": assets_not_changed,
                "ignored": self.assets_ignored,
                "processed": {
                    "processed": self.assets_processed,
                    "not_processed": assets_not_processed,
                },
            },
            "metadata": {
                "values_affected": self.metadata_values_affected,
                "added": self.metadata_values_added,
                "removed": self.metadata_values_removed,
            },
            "relationships": {
                "affected": self.relations_affected,
                "added": self.relations_added,
                "removed": self.relations_removed,
            },
            "processors": {
                "started": self.processings_started,
                "completed": self.processings_completed,
                "partial": self.processings_partial,
                "cancelled": self.processings_cancelled,
                "skipped": self.processings_skipped,
                "error": self.processings_error,
            },
        }


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(fqn(Provider), related_name="assets", on_delete=CASCADE)
    canonical_id = CharField(max_length=255, unique=True)
    canonical_uri = CharField(max_length=1024, unique=True)
    created_snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="created_assets", on_delete=RESTRICT
    )
    last_snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="last_assets", on_delete=RESTRICT
    )
    deleted_snapshot = ForeignKeyField(
        fqn(Snapshot),
        related_name="deleted_assets",
        null=True,
        on_delete=SET_NULL,
    )
    _data_accessor: FileAccessor | None = None

    @property
    def data(self) -> FileAccessor | None:
        return self._data_accessor

    def attach_accessor(self, accessor: FileAccessor | None) -> None:
        self._data_accessor = accessor

    async def upsert(
        self,
        snapshot: "Snapshot",
        metadata: Sequence["Metadata"] | None = None,
        stats: SnapshotStats | None = None,
    ) -> set[MetadataKey]:
        """Persist this asset and apply metadata changes for the snapshot.

        Returns a set of metadata keys that changed.
        """

        # Ensure the asset itself is saved first so we have an id for FKs.
        await self.save()

        # No metadata to process: nothing changed.
        if not metadata:
            return set()

        await self.load_metadata()
        existing_metadata: list[Metadata] = getattr(self, "metadata")

        existing_index: set[tuple[int, int, Any]] = set()
        for md in existing_metadata:
            existing_index.add((int(md.metadata_key_id), int(md.provider_id), md.value))  # type: ignore

        to_create: list[Metadata] = []
        changed_keys: set[MetadataKey] = set()

        for md in metadata:
            md.asset = self
            md.snapshot = snapshot
            key = (int(md.metadata_key_id), int(md.provider_id), md.value)  # type: ignore
            if key in existing_index:
                continue
            to_create.append(md)
            existing_index.add(key)
            changed_keys.add(get_metadata_def_by_registry_id(key[0]).key)

        if to_create:
            await Metadata.bulk_create(to_create)

        return changed_keys

    async def load_metadata(self) -> Sequence["Metadata"]:
        """Fetch and cache metadata rows for this asset."""
        await self.fetch_related("metadata")
        return getattr(self, "metadata", [])

    class Meta(Model.Meta):
        indexes = (("provider", "last_snapshot"),)


class MetadataRegistry(Model):
    id = IntField(pk=True)
    # Owner/defining plugin id (same identifier format as Provider.plugin_id)
    plugin_id = CharField(max_length=255)
    # Canonical, globally unique key string (recommended: namespaced, e.g. "plugin_id:key").
    key = CharField(max_length=512)
    value_type = IntEnumField(MetadataType)
    title = CharField(max_length=255, default="")
    description = TextField(default="")
    width = IntField(null=True)

    class Meta(Model.Meta):
        unique_together = ("plugin_id", "key")


class Metadata(Model):
    id = IntField(pk=True)
    asset = ForeignKeyField(fqn(Asset), related_name="metadata", on_delete=CASCADE)
    provider = ForeignKeyField(
        fqn(Provider), related_name="metadata_entries", on_delete=CASCADE
    )
    snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="metadata_entries", on_delete=CASCADE
    )
    metadata_key = ForeignKeyField(
        fqn(MetadataRegistry), related_name="metadata_entries", on_delete=RESTRICT
    )
    value_type = IntEnumField(MetadataType)
    value_text = TextField(null=True)
    value_int = BigIntField(null=True)
    value_real = FloatField(null=True)
    value_datetime = DatetimeField(null=True)
    value_json = JSONField(null=True)
    value_relation = ForeignKeyField(fqn(Asset), null=True, on_delete=CASCADE)
    removed = BooleanField(default=False)
    # Null means no confidence score, which can be assumed to be 1.0
    confidence = FloatField(null=True)

    class Meta(Model.Meta):
        indexes = [
            "metadata_key",
            "value_type",
        ]
        # unique_together = ("asset", "provider", "snapshot", "metadata_key")

    @property
    def key(self) -> "MetadataKey":
        """Metadata key as the typed `MetadataKey` (no DB fetch).

        Uses the startup-synced in-memory registry mapping from integer id -> key.
        """

        registry_id = getattr(self, "metadata_key_id", None)
        if registry_id is None:
            raise RuntimeError("metadata_key_id is missing on this Metadata instance")
        return get_metadata_def_by_registry_id(int(registry_id)).key

    @property
    def value(self) -> "MetadataScalar":
        """Return the stored value as a Python scalar (no DB fetch)."""

        # Prefer the declared type for speed/clarity.
        if self.value_type == MetadataType.STRING:
            return self.value_text
        if self.value_type == MetadataType.INT:
            return self.value_int
        if self.value_type == MetadataType.FLOAT:
            return self.value_real
        if self.value_type == MetadataType.DATETIME:
            return self.value_datetime
        if self.value_type == MetadataType.JSON:
            return self.value_json
        if self.value_type == MetadataType.RELATION:
            return self.value_relation
        else:
            raise ValueError(f"Unsupported metadata value_type {self.value_type}")

    @classmethod
    async def for_asset(
        cls,
        asset: Asset | int,
        *,
        include_removed: bool = False,
    ) -> Sequence["Metadata"]:
        asset_id = asset.id if isinstance(asset, Asset) else int(asset)
        query = cls.filter(asset_id=asset_id)
        if not include_removed:
            query = query.filter(removed=False)
        return await query.order_by("metadata_key_id", "id")


def make_metadata(*args, **kwargs) -> Metadata:
    """Create a Metadata instance, ensuring the value type matches the key definition."""
    # Signature is intentionally flexible because call sites differ (sources/processors/tests).
    # Preferred usage:
    #   make_metadata(provider_id, key, value, asset_id=..., snapshot_id=...)
    provider_id: int | None = None
    key: MetadataKey | None = None
    value: MetadataScalar | None = None

    if len(args) >= 1:
        provider_id = args[0]
    if len(args) >= 2:
        key = args[1]
    if len(args) >= 3:
        value = args[2]
    if len(args) > 3:
        raise TypeError(
            "make_metadata(provider_id, key, value, ...) takes at most 3 positional arguments"
        )

    provider_id = kwargs.pop("provider_id", provider_id)
    key = kwargs.pop("key", key)
    value = kwargs.pop("value", value)

    asset = kwargs.pop("asset", None)
    asset_id = kwargs.pop("asset_id", None)
    snapshot = kwargs.pop("snapshot", None)
    snapshot_id = kwargs.pop("snapshot_id", None)
    removed = kwargs.pop("removed", False)
    confidence = kwargs.pop("confidence", None)

    if kwargs:
        unknown = ", ".join(sorted(kwargs.keys()))
        raise TypeError(f"Unknown make_metadata() kwargs: {unknown}")

    if provider_id is None:
        raise TypeError("make_metadata requires provider_id")
    if key is None:
        raise TypeError("make_metadata requires key")
    if value is None:
        raise TypeError("make_metadata requires value")

    definition = get_metadata_def(key)
    ensure_value_type(definition.value_type, value)

    entry = Metadata(
        provider_id=int(provider_id),
        metadata_key_id=get_metadata_id(key),
        value_type=definition.value_type,
        removed=bool(removed),
        confidence=confidence,
    )
    if asset is not None:
        entry.asset = asset
    elif asset_id is not None:
        entry.asset_id = int(asset_id)

    if snapshot is not None:
        entry.snapshot = snapshot
    elif snapshot_id is not None:
        entry.snapshot_id = int(snapshot_id)

    if definition.value_type == MetadataType.STRING:
        entry.value_text = str(value)
    elif definition.value_type == MetadataType.INT:
        # bool is rejected by _ensure_value_type
        entry.value_int = int(value)  # type: ignore[arg-type]
    elif definition.value_type == MetadataType.FLOAT:
        entry.value_real = float(value)  # type: ignore[arg-type]
    elif definition.value_type == MetadataType.DATETIME:
        entry.value_datetime = value  # type: ignore[assignment]
    elif definition.value_type == MetadataType.JSON:
        entry.value_json = value  # type: ignore[assignment]
    else:  # pragma: no cover
        raise ValueError(f"Unsupported metadata value type {definition.value_type}")

    return entry


def current_metadata(metadata: Sequence[Metadata]):
    """Get the current metadata entries by key from a list of Metadata.
    This means any metadata entry that hasn't been removed OR"""
    result: dict[MetadataKey, list[Metadata]] = {}
    return result
