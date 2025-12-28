from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Task
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum, IntEnum
from time import time

from typing import Any, Mapping, Sequence

from loguru import logger
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
    ForeignKeyRelation,
    JSONField,
    IntField,
    BooleanField,
    TextField,
    RESTRICT,
    SET_NULL,
)
from tortoise.models import Model

from katalog.metadata import (
    MetadataKey,
    MetadataScalar,
    MetadataType,
    get_metadata_def_by_id,
    get_metadata_def_by_key,
    get_metadata_id,
)
from katalog.utils.utils import orm

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
    plugin_id = CharField(max_length=1024, null=True)
    config = JSONField(null=True)
    type = IntEnumField(ProviderType)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)


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


DEFAULT_TASK_CONCURRENCY = 10


class Snapshot(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(
        orm(Provider), related_name="snapshots", on_delete=CASCADE, null=True
    )
    note = CharField(max_length=512, null=True)
    started_at = DatetimeField(default=lambda: datetime.now(UTC))
    completed_at = DatetimeField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    metadata = JSONField(null=True)

    # Local fields not persisted to DB
    stats: SnapshotStats
    tasks: list[Task]
    # Control concurrency of snapshot (processor) tasks
    semaphore: asyncio.Semaphore

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._init_runtime_state()

    def _init_runtime_state(self) -> None:
        self.stats = SnapshotStats()
        self.tasks = []
        self.semaphore = asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)

    @classmethod
    async def find_partial_resume_point(cls, *, provider: Provider) -> "Snapshot | None":
        """
        Return the most recent PARTIAL snapshot that occurred after the latest
        COMPLETED snapshot for this provider. If no COMPLETED snapshot exists,
        return None (treat as full scan).
        """
        snapshots = list(
            await cls.filter(provider=provider).order_by(
                "-completed_at", "-started_at"
            )
        )
        latest_full = next(
            (s for s in snapshots if s.status == OpStatus.COMPLETED), None
        )
        if latest_full is None:
            return None
        full_mark = latest_full.completed_at or latest_full.started_at
        for snap in snapshots:
            if snap.status != OpStatus.PARTIAL:
                continue
            snap_mark = snap.completed_at or snap.started_at
            if full_mark is None or (snap_mark and snap_mark >= full_mark):
                return snap
        return None

    @classmethod
    async def begin(
        cls,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
        provider: Provider | None = None,
        snapshot_id: int | None = None,
        note: str | None = None,
    ) -> "Snapshot":
        snapshot_id = snapshot_id or int(time())
        if await cls.get_or_none(id=snapshot_id):
            raise ValueError(f"Snapshot with id {snapshot_id} already exists")
        return await cls.create(
            id=snapshot_id,
            provider=provider,
            status=status,
            note=note,
            metadata=dict(metadata) if metadata else None,
        )

    async def finalize(self, *, status: OpStatus) -> None:
        completed_at = datetime.now(UTC)
        metadata_payload: dict[str, Any] | None = None

        if self.tasks:
            await drain_tasks(self.tasks)

        if self.stats is not None or self.metadata is not None:
            metadata_payload = dict(self.metadata or {})
            if self.stats is not None:
                metadata_payload["stats"] = self.stats.to_dict()
            self.metadata = metadata_payload

        async with in_transaction():
            update_fields = ["completed_at", "status"]
            if self.note is not None:
                update_fields.append("note")
            if metadata_payload is not None:
                update_fields.append("metadata")
            self.status = status
            self.completed_at = completed_at
            await self.save(update_fields=update_fields)

    @classmethod
    @asynccontextmanager
    async def context(
        cls,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
        provider: Provider | None = None,
        snapshot_id: int | None = None,
        note: str | None = None,
        success_status: OpStatus = OpStatus.COMPLETED,
        error_status: OpStatus = OpStatus.ERROR,
    ):
        """
        Async context manager for Snapshot lifecycle.

        Usage:
            async with Snapshot.context(provider=..., metadata=...) as snap:
                # do work, snap is a Snapshot instance
        On normal exit -> finalize with success_status.
        On CancelledError -> finalize with CANCELED and re-raise.
        On other exceptions -> finalize with error_status and re-raise.
        """
        snapshot = await cls.begin(
            status=status,
            metadata=metadata,
            provider=provider,
            snapshot_id=snapshot_id,
            note=note,
        )
        try:
            yield snapshot
        except asyncio.CancelledError:
            # best-effort finalize as cancelled, then re-raise
            try:
                await snapshot.finalize(status=OpStatus.CANCELED)
            except Exception as exc:  # keep exception simple and log
                logger.opt(exception=exc).error(
                    "Failed to finalize snapshot after cancellation"
                )
            raise
        except Exception:
            # error path: finalize as error (or custom error_status), then re-raise
            try:
                await snapshot.finalize(status=error_status)
            except Exception as exc:
                logger.opt(exception=exc).error(
                    "Failed to finalize snapshot after error"
                )
            raise
        else:
            # normal completion
            try:
                await snapshot.finalize(status=success_status)
            except Exception as exc:
                # If finalization fails on success, log and re-raise so caller is aware
                logger.opt(exception=exc).error(
                    "Failed to finalize snapshot after success"
                )
                raise

    class Meta(Model.Meta):
        indexes = (("provider", "started_at"),)


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(orm(Provider), related_name="assets", on_delete=CASCADE)
    canonical_id = CharField(max_length=255, unique=True)
    canonical_uri = CharField(max_length=1024, unique=True)
    created_snapshot = ForeignKeyField(
        orm(Snapshot), related_name="created_assets", on_delete=RESTRICT
    )
    last_snapshot = ForeignKeyField(
        orm(Snapshot), related_name="last_assets", on_delete=RESTRICT
    )
    deleted_snapshot = ForeignKeyField(
        orm(Snapshot),
        related_name="deleted_assets",
        null=True,
        on_delete=SET_NULL,
    )
    # Just for fixing type errors, these are populated via ForeignKeyField
    created_snapshot_id: int
    last_snapshot_id: int
    deleted_snapshot_id: int | None
    _data_accessor: FileAccessor | None = None
    _metadata_cache: list["Metadata"] | None = None

    @property
    def data(self) -> FileAccessor | None:
        return self._data_accessor

    def attach_accessor(self, accessor: FileAccessor | None) -> None:
        self._data_accessor = accessor

    async def save_record(self, snapshot: "Snapshot") -> None:
        """Persist the asset row, reusing an existing canonical asset when present."""
        if self.id is None:
            existing = await Asset.get_or_none(canonical_id=self.canonical_id)
            if existing:
                self.id = existing.id
                self._saved_in_db = True
                self.created_snapshot_id = existing.created_snapshot_id
                self.provider_id = existing.provider_id
                self.canonical_uri = existing.canonical_uri
        if getattr(self, "created_snapshot_id", None) is None:
            self.created_snapshot = snapshot
        self.last_snapshot = snapshot
        self.deleted_snapshot = None
        await self.save()

    async def load_metadata(self) -> Sequence["Metadata"]:
        """Fetch and cache metadata rows for this asset."""
        if self._metadata_cache is not None:
            return self._metadata_cache
        await self.fetch_related("metadata")
        self._metadata_cache = list(getattr(self, "metadata", []))
        return self._metadata_cache

    @classmethod
    async def mark_unseen_as_deleted(
        cls, *, snapshot: "Snapshot", provider_ids: Sequence[int]
    ) -> int:
        """
        Mark assets from the given providers as deleted if they were not touched by this snapshot.
        Returns the number of affected rows.
        """
        if not provider_ids:
            return 0
        provider_ids = list(provider_ids)
        updated = (
            await cls.filter(
                provider_id__in=provider_ids,
                deleted_snapshot_id__isnull=True,
            )
            .exclude(last_snapshot_id=snapshot.id)
            .update(deleted_snapshot_id=snapshot.id)
        )
        return updated

    class Meta(Model.Meta):
        indexes = (("provider", "last_snapshot"),)


class MetadataRegistry(Model):
    id = IntField(pk=True)
    # Owner/defining plugin id (import path to the plugin class)
    plugin_id = CharField(max_length=1024)
    key = CharField(max_length=512)
    value_type = IntEnumField(MetadataType)
    title = CharField(max_length=255, default="")
    description = TextField(default="")
    width = IntField(null=True)

    class Meta(Model.Meta):
        unique_together = ("plugin_id", "key")


class Metadata(Model):
    id = IntField(pk=True)
    asset = ForeignKeyField(orm(Asset), related_name="metadata", on_delete=CASCADE)
    provider: ForeignKeyRelation[Provider] = ForeignKeyField(
        orm(Provider), related_name="metadata_entries", on_delete=CASCADE
    )
    snapshot = ForeignKeyField(
        orm(Snapshot), related_name="metadata_entries", on_delete=CASCADE
    )
    metadata_key = ForeignKeyField(
        orm(MetadataRegistry), related_name="metadata_entries", on_delete=RESTRICT
    )
    # Just for fixing type errors, these are populated via ForeignKeyField
    provider_id: int
    snapshot_id: int
    metadata_key_id: int

    value_type = IntEnumField(MetadataType)
    value_text = TextField(null=True)
    value_int = BigIntField(null=True)
    value_real = FloatField(null=True)
    value_datetime = DatetimeField(null=True)
    value_json = JSONField(null=True)
    value_relation = ForeignKeyField(orm(Asset), null=True, on_delete=CASCADE)
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
        return get_metadata_def_by_id(int(registry_id)).key

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

    def set_value(self, value: Any) -> None:
        if self.value_type == MetadataType.STRING:
            self.value_text = str(value)
        elif self.value_type == MetadataType.INT:
            self.value_int = int(value)
        elif self.value_type == MetadataType.FLOAT:
            self.value_real = float(value)
        elif self.value_type == MetadataType.DATETIME:
            if value is None:
                self.value_datetime = None
            else:
                if not isinstance(value, datetime):
                    raise ValueError(
                        f"Expected datetime for MetadataType.DATETIME, got {type(value)}"
                    )
                if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                    raise ValueError(
                        "value_datetime must be timezone-aware (e.g. UTC). "
                        "Provide an aware datetime."
                    )
                self.value_datetime = value
        elif self.value_type == MetadataType.JSON:
            self.value_json = value
        elif self.value_type == MetadataType.RELATION:
            self.value_relation_id = int(value)
        else:
            raise ValueError(
                f"Unsupported value to set '{value}' of type '{type(value)} for Metadata of type {self.value_type}"
            )

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


def make_metadata(
    key: MetadataKey,
    value: MetadataScalar | None = None,
    provider_id: int | None = None,
    removed: bool = False,
    confidence: float | None = None,
    *,
    asset: Asset | None = None,
    asset_id: int | None = None,
    snapshot: Snapshot | None = None,
    snapshot_id: int | None = None,
    metadata_id: int | None = None,  # Only used for testing or bypassing
) -> Metadata:
    """Create a Metadata instance, ensuring the value type matches the key definition."""
    definition = get_metadata_def_by_key(key)

    md = Metadata(
        metadata_key_id=get_metadata_id(key) if metadata_id is None else metadata_id,
        value_type=definition.value_type,
        removed=removed,
        confidence=confidence,
    )
    md.set_value(value)
    if provider_id is not None:
        md.provider_id = provider_id
    if asset is not None:
        md.asset = asset
    elif asset_id is not None:
        md.asset_id = asset_id
    if snapshot is not None:
        md.snapshot = snapshot
    elif snapshot_id is not None:
        md.snapshot_id = snapshot_id

    return md


class MetadataChangeSet:
    """Track metadata state for an asset during processing (loaded + staged changes)."""

    def __init__(
        self,
        loaded: Sequence[Metadata],
        staged: Sequence[Metadata] | None = None,
    ) -> None:
        self._loaded = list(loaded)
        self._staged: list[Metadata] = list(staged or [])
        self._cache_current: dict[int | None, dict[MetadataKey, list[Metadata]]] = {}
        self._cache_changed: dict[int | None, set[MetadataKey]] = {}

    @staticmethod
    def _current_metadata(
        metadata: Sequence[Metadata] | None = None,
        provider_id: int | None = None,
    ) -> dict[MetadataKey, list[Metadata]]:
        """Get current metadata entries by key from a list of Metadata."""
        if not metadata:
            return {}

        ordered = sorted(
            metadata,
            # Metadata should always have a snapshot id, if not we assume 0 which
            # means "oldest"
            key=lambda m: m.snapshot_id if m.snapshot_id is not None else 0,
            reverse=True,
        )

        result: dict[MetadataKey, list[Metadata]] = {}
        seen_values: dict[MetadataKey, set[Any]] = {}
        for entry in ordered:
            if provider_id is not None and int(entry.provider_id) != int(provider_id):
                continue
            key = entry.key
            seen_for_key = seen_values.setdefault(key, set())
            value = entry.value
            if value in seen_for_key:
                continue
            seen_for_key.add(value)
            if entry.removed:
                continue
            result.setdefault(key, []).append(entry)
        return result

    def add(self, metadata: Sequence[Metadata]) -> None:
        """Stage new metadata (including removals)."""
        self._staged.extend(metadata)
        self._cache_current.clear()
        self._cache_changed.clear()

    def current(
        self, provider_id: int | None = None
    ) -> dict[MetadataKey, list[Metadata]]:
        """Return current metadata by key, combining loaded and staged."""
        if provider_id in self._cache_current:
            return self._cache_current[provider_id]
        combined = list(self._loaded) + list(self._staged)
        current = self._current_metadata(combined, provider_id)
        self._cache_current[provider_id] = current
        return current

    def changed_keys(self, provider_id: int | None = None) -> set[MetadataKey]:
        """Return keys whose current values differ from the loaded baseline."""
        if provider_id in self._cache_changed:
            return self._cache_changed[provider_id]
        baseline = self._current_metadata(self._loaded, provider_id)
        current = self.current(provider_id)
        changed: set[MetadataKey] = set()
        for key in set(baseline.keys()) | set(current.keys()):
            base_values = {md.value for md in baseline.get(key, [])}
            curr_values = {md.value for md in current.get(key, [])}
            if base_values != curr_values:
                changed.add(key)
        self._cache_changed[provider_id] = changed
        return changed

    def has(self, key: MetadataKey, provider_id: int | None = None) -> bool:
        return key in self.current(provider_id)

    def pending_entries(self) -> list[Metadata]:
        """Metadata added during processing that should be persisted."""
        return list(self._staged)

    def all_entries(self) -> list[Metadata]:
        """Loaded + staged metadata."""
        return list(self._loaded) + list(self._staged)

    async def persist(
        self,
        asset: Asset,
        snapshot: Snapshot,
    ) -> set[MetadataKey]:
        """Persist staged metadata entries from a change set for the given asset."""
        staged = self.pending_entries()
        if not staged:
            return set()

        existing_metadata = await asset.load_metadata()
        existing_index: set[tuple[int, int, Any]] = {
            (int(md.metadata_key_id), int(md.provider_id), md.value)
            for md in existing_metadata
        }

        to_create: list[Metadata] = []
        changed_keys: set[MetadataKey] = set()

        for md in staged:
            md.asset = asset
            md.asset_id = asset.id
            md.snapshot = snapshot
            md.snapshot_id = snapshot.id
            if md.provider is None and md.provider_id is None:
                raise ValueError("Metadata provider_id is not set for persistence")

            key = (int(md.metadata_key_id), int(md.provider_id), md.value)
            if key in existing_index:
                continue
            to_create.append(md)
            existing_index.add(key)
            changed_keys.add(get_metadata_def_by_id(key[0]).key)

        if to_create:
            await Metadata.bulk_create(to_create)
            if asset._metadata_cache is not None:
                asset._metadata_cache.extend(to_create)

        return changed_keys


async def drain_tasks(tasks: list[asyncio.Task[Any]]) -> tuple[int, int]:
    if not tasks:
        return 0, 0
    results = await asyncio.gather(*tasks, return_exceptions=True)
    modified = 0
    failures = 0
    for result in results:
        if isinstance(result, Exception):
            logger.opt(exception=result).error("Processor task failed")
            failures += 1
            continue
        if result:
            modified += 1
    tasks.clear()
    return modified, failures
