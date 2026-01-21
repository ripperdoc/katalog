from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Task
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, UTC
from enum import Enum, IntEnum
import json
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
from tortoise import Tortoise

from katalog.metadata import (
    MetadataKey,
    MetadataScalar,
    MetadataType,
    get_metadata_def_by_id,
    get_metadata_def_by_key,
    get_metadata_id,
    ASSET_LOST,
)
from katalog.utils.utils import orm

"""Data usage notes
- The target profile for this system is to handle metadata for 1 million files. Actual file contents is not to be stored in the DB.
- This implies
- ~1 million Asset records
- ~30 million Metadata records (assuming an average of 30 metadata entries per asset). 
Metadata will mostly be shorter text and date values, but some fields may grow pretty large, such as text contents, summaries, etc.
- 10 to 100 Actors
- As data changes over time, changesets will be created, increasing the number of Metadata rows per asset. 
On the other hand, users will be encouraged to purge changesets regularly.
"""


class OpStatus(Enum):
    IN_PROGRESS = "in_progress"
    PARTIAL = "partial"
    COMPLETED = "completed"
    CANCELED = "canceled"
    SKIPPED = "skipped"
    ERROR = "error"


class ActorType(IntEnum):
    SOURCE = 0
    PROCESSOR = 1
    ANALYZER = 2
    EDITOR = 3
    EXPORTER = 4


class Actor(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    plugin_id = CharField(max_length=1024, null=True)
    config = JSONField(null=True)
    config_toml = TextField(null=True)
    type = IntEnumField(ActorType)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.name if isinstance(self.type, ActorType) else self.type,
            "plugin_id": self.plugin_id,
            "config": self.config,
            "config_toml": getattr(self, "config_toml", None),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass(slots=True)
class ChangesetStats:
    # Total assets encountered/accessed during scan (saved + ignored)
    assets_seen: int = 0
    assets_saved: int = 0  # Assets yielded and saved/processed by the pipeline
    assets_ignored: int = 0  # Skipped during scan (e.g. filtered by actor settings)

    assets_changed: int = 0  # Assets that had metadata changes
    assets_added: int = 0  # New assets seen for the first time
    assets_lost: int = 0  # Assets marked as deleted (not seen in scan)
    assets_processed: int = 0  # Assets that had processors run on them

    metadata_values_changed: int = 0  # Total metadata values added or removed
    metadata_values_added: int = 0  # Metadata values added
    metadata_values_removed: int = 0  # Metadata values removed
    # TODO how to correctly count unique keys affected across
    # a changeset when we persist paralell async operations?
    # metadata_keys_affected: int = 0

    processings_started: int = 0  # Total processing operations started
    processings_completed: int = 0  # Total processing operations completed successfully
    processings_partial: int = 0  # Total processing operations completed partially
    processings_cancelled: int = 0  # Total processing operations cancelled
    processings_skipped: int = 0  # Total processing operations skipped
    processings_error: int = 0  # Total processing operations failed with error

    # def validate(self) -> None:
    #     assert self.ass

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_TASK_CONCURRENCY = 10


class Changeset(Model):
    id = IntField(pk=True)
    actor = ForeignKeyField(
        orm(Actor), related_name="changesets", on_delete=CASCADE, null=True
    )
    note = CharField(max_length=512, null=True)
    started_at = DatetimeField(default=lambda: datetime.now(UTC))
    completed_at = DatetimeField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    metadata = JSONField(null=True)

    # Local fields not persisted to DB
    stats: ChangesetStats
    tasks: list[Task]
    # Control concurrency of changeset (processor) tasks
    semaphore: asyncio.Semaphore
    # Just for type checking
    actor_id: int

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._init_runtime_state()

    def _init_runtime_state(self) -> None:
        # Safe defaults so instances materialized from the ORM always have runtime fields.
        self.stats = getattr(self, "stats", ChangesetStats())
        self.tasks = getattr(self, "tasks", [])
        self.semaphore = getattr(
            self, "semaphore", asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)
        )

    def to_dict(self) -> dict:
        # Note needs to have been fetched related 'actor' beforehand
        actor = getattr(self, "actor", None)
        return {
            "id": self.id,
            "actor_id": self.actor_id,
            "actor_name": actor.name if actor else None,
            "note": self.note,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "status": self.status.value
            if isinstance(self.status, OpStatus)
            else str(self.status),
            "metadata": self.metadata,
        }

    @classmethod
    async def find_partial_resume_point(cls, *, actor: Actor) -> "Changeset | None":
        """
        Return the most recent PARTIAL changeset that occurred after the latest
        COMPLETED changeset for this actor. If no COMPLETED changeset exists,
        return None (treat as full scan).
        """
        changesets = list(
            await cls.filter(actor=actor).order_by("-completed_at", "-started_at")
        )
        last_full = None
        last_partial = None
        for s in reversed(changesets):
            if s.status == OpStatus.COMPLETED:
                last_full = s
                break
            elif last_partial is None and s.status == OpStatus.PARTIAL:
                last_partial = s

        if last_full is None:
            return None

        if last_partial is not None:
            return last_partial
        else:
            return last_full

    @classmethod
    async def begin(
        cls,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
        actor: Actor | None = None,
        changeset_id: int | None = None,
        note: str | None = None,
    ) -> "Changeset":
        # Prevent concurrent in-progress changesets (scans or edits).
        existing_in_progress = await cls.get_or_none(status=OpStatus.IN_PROGRESS)
        if existing_in_progress is not None:
            raise ValueError(
                f"Changeset {existing_in_progress.id} is already in progress; finish or cancel it first"
            )
        changeset_id = changeset_id or int(time())
        if await cls.get_or_none(id=changeset_id):
            raise ValueError(f"Changeset with id {changeset_id} already exists")
        return await cls.create(
            id=changeset_id,
            actor=actor,
            status=status,
            note=note,
            metadata=dict(metadata) if metadata else None,
        )

    async def finalize(self, *, status: OpStatus) -> None:
        # Ensure runtime fields exist even if object was loaded from DB outside __init__ context.
        if not hasattr(self, "tasks"):
            self._init_runtime_state()

        completed_at = datetime.now(UTC)
        metadata_payload: dict[str, Any] | None = None

        if self.tasks:
            logger.info(
                "Draining {count} processor tasks before finalizing changeset {changeset_id}",
                count=len(self.tasks),
                changeset_id=self.id,
            )
            modified, failures = await drain_tasks(self.tasks)
            logger.info(
                "Finished draining processor tasks for changeset {changeset_id} (modified={modified}, failures={failures})",
                changeset_id=self.id,
                modified=modified,
                failures=failures,
            )

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
        actor: Actor | None = None,
        changeset_id: int | None = None,
        note: str | None = None,
        success_status: OpStatus = OpStatus.COMPLETED,
        error_status: OpStatus = OpStatus.ERROR,
    ):
        """
        Async context manager for Changeset lifecycle.

        Usage:
            async with Changeset.context(actor=..., metadata=...) as snap:
                # do work, snap is a Changeset instance
        On normal exit -> finalize with success_status.
        On CancelledError -> finalize with CANCELED and re-raise.
        On other exceptions -> finalize with error_status and re-raise.
        """
        changeset = await cls.begin(
            status=status,
            metadata=metadata,
            actor=actor,
            changeset_id=changeset_id,
            note=note,
        )
        try:
            yield changeset
        except asyncio.CancelledError:
            # best-effort finalize as cancelled, then re-raise
            try:
                await changeset.finalize(status=OpStatus.CANCELED)
            except Exception as exc:  # keep exception simple and log
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after cancellation"
                )
            raise
        except Exception:
            # error path: finalize as error (or custom error_status), then re-raise
            try:
                await changeset.finalize(status=error_status)
            except Exception as exc:
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after error"
                )
            raise
        else:
            # normal completion
            try:
                await changeset.finalize(status=success_status)
            except Exception as exc:
                # If finalization fails on success, log and re-raise so caller is aware
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after success"
                )
                raise

    class Meta(Model.Meta):
        indexes = (("actor", "started_at"),)


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(Model):
    id = IntField(pk=True)
    external_id = CharField(max_length=255, unique=True)
    canonical_uri = CharField(max_length=1024, unique=False)
    _data_accessor: FileAccessor | None = None
    _metadata_cache: list["Metadata"] | None = None

    @property
    def data(self) -> FileAccessor | None:
        return self._data_accessor

    def attach_accessor(self, accessor: FileAccessor | None) -> None:
        self._data_accessor = accessor

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

        actor = actor or getattr(changeset, "actor", None)
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
        indexes = ()


class CollectionRefreshMode(str, Enum):
    LIVE = "live"
    ON_DEMAND = "on_demand"


class AssetCollection(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    description = TextField(null=True)
    source = JSONField(null=True)  # opaque JSON describing query/view used to create
    refresh_mode = CharEnumField(
        CollectionRefreshMode, default=CollectionRefreshMode.ON_DEMAND
    )
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    def to_dict(self, *, asset_count: int | None = None) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "asset_count": asset_count,
            "source": self.source,
            "refresh_mode": self.refresh_mode.value
            if isinstance(self.refresh_mode, CollectionRefreshMode)
            else str(self.refresh_mode),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class CollectionItem(Model):
    collection = ForeignKeyField(
        orm(AssetCollection), related_name="items", on_delete=CASCADE
    )
    asset = ForeignKeyField(orm(Asset), related_name="collections", on_delete=CASCADE)
    added_at = DatetimeField(auto_now_add=True)

    class Meta(Model.Meta):
        unique_together = (("collection", "asset"),)
        indexes = (("collection", "asset"), ("asset",))


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
    actor: ForeignKeyRelation[Actor] = ForeignKeyField(
        orm(Actor), related_name="metadata_entries", on_delete=CASCADE
    )
    changeset = ForeignKeyField(
        orm(Changeset), related_name="metadata_entries", on_delete=CASCADE
    )
    metadata_key = ForeignKeyField(
        orm(MetadataRegistry), related_name="metadata_entries", on_delete=RESTRICT
    )
    # Just for fixing type errors, these are populated via ForeignKeyField
    asset_id: int
    actor_id: int
    changeset_id: int
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
        # unique_together = ("asset", "actor", "changeset", "metadata_key")

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

    def to_dict(self) -> dict[str, Any]:
        value: Any
        if self.value_type == MetadataType.STRING:
            value = self.value_text
        elif self.value_type == MetadataType.INT:
            value = self.value_int
        elif self.value_type == MetadataType.FLOAT:
            value = self.value_real
        elif self.value_type == MetadataType.DATETIME:
            value = self.value_datetime.isoformat() if self.value_datetime else None
        elif self.value_type == MetadataType.JSON:
            value = self.value_json
        elif self.value_type == MetadataType.RELATION:
            value = self.value_relation_id
        else:
            value = None

        return {
            "id": int(self.id),
            "asset_id": int(self.asset_id),
            "actor_id": int(self.actor_id),
            "changeset_id": int(self.changeset_id),
            "metadata_key_id": int(self.metadata_key_id),
            "key": str(self.key),
            "value_type": self.value_type.name,
            "value": value,
            "removed": bool(self.removed),
            "confidence": float(self.confidence)
            if self.confidence is not None
            else None,
        }

    def set_value(self, value: Any) -> None:
        if value is None:
            self.value_text = None
            self.value_int = None
            self.value_real = None
            self.value_datetime = None
            self.value_json = None
            self.value_relation_id = None
            return
        if self.value_type == MetadataType.STRING:
            self.value_text = str(value)
        elif self.value_type == MetadataType.INT:
            self.value_int = int(value)
        elif self.value_type == MetadataType.FLOAT:
            self.value_real = float(value)
        elif self.value_type == MetadataType.DATETIME:
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
            if value is not None:
                try:
                    # Validate that the value is actually JSON-serializable.
                    # We use a stable encoding to avoid surprising behavior across runs.
                    self._stable_json_dumps(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Value for JSON metadata must be JSON-serializable, got {type(value)}: {exc}"
                    ) from exc
            self.value_json = value
        elif self.value_type == MetadataType.RELATION:
            self.value_relation_id = int(value)
        else:
            raise ValueError(
                f"Unsupported value to set '{value}' of type '{type(value)} for Metadata of type {self.value_type}"
            )

    def __str__(self) -> str:
        return f"Metadata('{self.key}'='{self.value}', id={self.id}, actor={self.actor_id}, removed={self.removed})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def _stable_json_dumps(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    def fingerprint(self) -> Any:
        """Return a hashable, stable representation of this metadata value.

        Used for change detection and duplicate prevention.

        Important: for JSON values we compare a stable JSON encoding rather than Python object
        identity.
        """

        value: Any = self.value

        if self.value_type == MetadataType.JSON:
            if value is None:
                return None
            return self._stable_json_dumps(value)

        if self.value_type == MetadataType.DATETIME:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.isoformat()
            return str(value)

        if self.value_type == MetadataType.RELATION:
            relation_id = getattr(self, "value_relation_id", None)
            if relation_id is not None:
                return int(relation_id)
            relation = getattr(self, "value_relation", None)
            if relation is not None:
                relation_pk = getattr(relation, "id", None)
                if relation_pk is not None:
                    return int(relation_pk)
            if value is None:
                return None
            value_id = getattr(value, "id", None)
            if value_id is not None:
                return int(value_id)
            return int(value)

        return value

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
    actor_id: int | None = None,
    removed: bool = False,
    confidence: float | None = None,
    *,
    asset: Asset | None = None,
    asset_id: int | None = None,
    changeset: Changeset | None = None,
    changeset_id: int | None = None,
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
    if actor_id is not None:
        md.actor_id = actor_id
    if asset is not None:
        md.asset = asset
    elif asset_id is not None:
        md.asset_id = asset_id
    if changeset is not None:
        md.changeset = changeset
    elif changeset_id is not None:
        md.changeset_id = changeset_id

    return md


@dataclass(slots=True)
class MetadataChangeSet:
    """Track metadata state for an asset during processing (loaded + staged changes)."""

    loaded: Sequence[Metadata]
    staged: Sequence[Metadata] | None = None

    # Internal runtime fields (not part of the generated init)
    _loaded: list[Metadata] = field(init=False)
    _staged: list[Metadata] = field(init=False)
    _cache_current: dict[int | None, dict[MetadataKey, list[Metadata]]] = field(
        default_factory=dict, init=False
    )
    _cache_changed: dict[int | None, set[MetadataKey]] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        self._loaded = list(self.loaded)
        self._staged = list(self.staged or [])
        self._cache_current = {}
        self._cache_changed = {}

    @staticmethod
    def _current_metadata(
        metadata: Sequence[Metadata] | None = None,
        actor_id: int | None = None,
    ) -> dict[MetadataKey, list[Metadata]]:
        """Get current metadata entries by key from a list of Metadata."""
        if not metadata:
            return {}

        ordered = sorted(
            metadata,
            # Metadata should always have a changeset id, if not we assume 0 which
            # means "oldest"
            key=lambda m: m.changeset_id if m.changeset_id is not None else 0,
            reverse=True,
        )

        result: dict[MetadataKey, list[Metadata]] = {}
        seen_values: dict[MetadataKey, set[Any]] = {}
        for entry in ordered:
            if actor_id is not None and int(entry.actor_id) != int(actor_id):
                continue
            key = entry.key
            seen_for_key = seen_values.setdefault(key, set())
            value_key = entry.fingerprint()
            # None means "no value" and should not be treated as a stored metadata value.
            # Historically we may have persisted NULL-value rows; ignore those for the
            # purpose of computing current state.
            if value_key is None:
                continue
            if value_key in seen_for_key:
                continue
            seen_for_key.add(value_key)
            if entry.removed:
                continue
            result.setdefault(key, []).append(entry)
        return result

    def add(self, metadata: Sequence[Metadata]) -> None:
        """Stage new metadata (including removals)."""
        self._staged.extend(metadata)
        self._cache_current.clear()
        self._cache_changed.clear()

    def current(self, actor_id: int | None = None) -> dict[MetadataKey, list[Metadata]]:
        """Return current metadata by key, combining loaded and staged."""
        if actor_id in self._cache_current:
            return self._cache_current[actor_id]
        combined = list(self._loaded) + list(self._staged)
        current = self._current_metadata(combined, actor_id)
        self._cache_current[actor_id] = current
        return current

    def changed_keys(self, actor_id: int | None = None) -> set[MetadataKey]:
        """Return keys whose current values differ from the loaded baseline."""
        if actor_id in self._cache_changed:
            return self._cache_changed[actor_id]
        baseline = self._current_metadata(self._loaded, actor_id)
        current = self.current(actor_id)
        changed: set[MetadataKey] = set()
        for key in set(baseline.keys()) | set(current.keys()):
            base_values = {md.fingerprint() for md in baseline.get(key, [])}
            curr_values = {md.fingerprint() for md in current.get(key, [])}
            if base_values != curr_values:
                changed.add(key)
        self._cache_changed[actor_id] = changed
        return changed

    def has(self, key: MetadataKey, actor_id: int | None = None) -> bool:
        return key in self.current(actor_id)

    def pending_entries(self) -> list[Metadata]:
        """Metadata added during processing that should be persisted."""
        return list(self._staged)

    def all_entries(self) -> list[Metadata]:
        """Loaded + staged metadata."""
        return list(self._loaded) + list(self._staged)

    # NOTE (future refactor idea):
    # - A staged value=None means "clear_key" for that (actor_id, metadata_key) and should NOT
    #   be persisted as a NULL-value metadata row.
    # - Clear must remain append-only/undoable: we express it by writing removed=True rows for each
    #   currently-active value (per-value tombstones), not by destructive deletes.
    # - Missing keys are unchanged; only explicitly staged None triggers clear.
    # - Reads like current() intentionally hide removed rows; persistence needs latest *state* per
    #   (metadata_key_id, actor_id, value) (incl removed bit) to dedupe correctly and support
    #   add -> remove -> add over time. Ordering must be newest-first (changeset_id/id).
    # - A cleaner rewrite could factor a shared latest() helper and derive current() from it;
    #   schema-level "clear all" tombstones would reduce writes but complicate queries.
    async def persist(
        self,
        asset: Asset,
        changeset: Changeset,
    ) -> set[MetadataKey]:
        """Persist staged metadata entries from a change set for the given asset."""
        staged = self.pending_entries()
        if not staged:
            return set()

        existing_metadata = await asset.load_metadata()
        # Deduplication is based on latest state (not "ever seen"), so we can support:
        # add -> remove -> add (same value) across changesets.
        ordered_existing = sorted(
            existing_metadata,
            key=lambda m: (
                m.changeset_id if m.changeset_id is not None else 0,
                m.id if m.id is not None else 0,
            ),
            reverse=True,
        )
        latest_states: dict[tuple[int, int, Any], bool] = {}
        for entry in ordered_existing:
            value_key = entry.fingerprint()
            if value_key is None:
                continue
            state_key = (
                int(entry.metadata_key_id),
                int(entry.actor_id),
                value_key,
            )
            if state_key in latest_states:
                continue
            latest_states[state_key] = bool(entry.removed)

        to_create: list[Metadata] = []
        changed_keys: set[MetadataKey] = set()

        # A staged entry with value=None (and removed=False) is an explicit instruction to clear
        # all current values for (metadata_key_id, actor_id).
        clear_groups: set[tuple[int, int]] = set()
        for md in staged:
            if md.actor_id is None:
                raise ValueError("Metadata actor_id is not set for persistence")
            group_key = (int(md.metadata_key_id), int(md.actor_id))
            if md.fingerprint() is None and not md.removed:
                clear_groups.add(group_key)

        # Apply clears first.
        if clear_groups:
            existing_current_by_actor: dict[int, dict[MetadataKey, list[Metadata]]] = {}
            for metadata_key_id, actor_id in clear_groups:
                if actor_id not in existing_current_by_actor:
                    existing_current_by_actor[actor_id] = self._current_metadata(
                        existing_metadata, actor_id
                    )

                key = get_metadata_def_by_id(int(metadata_key_id)).key
                existing_current = existing_current_by_actor[actor_id].get(key, [])

                for existing_entry in existing_current:
                    if existing_entry.value_type == MetadataType.RELATION:
                        existing_value = getattr(
                            existing_entry, "value_relation_id", None
                        )
                    else:
                        existing_value = existing_entry.value

                    if existing_value is None:
                        continue

                    removal = make_metadata(
                        key,
                        existing_value,
                        actor_id=actor_id,
                        removed=True,
                    )
                    removal.asset = asset
                    removal.asset_id = asset.id
                    removal.changeset = changeset
                    removal.changeset_id = changeset.id

                    value_key = removal.fingerprint()
                    if value_key is None:
                        continue

                    state_key = (int(metadata_key_id), int(actor_id), value_key)
                    if latest_states.get(state_key) is True:
                        continue

                    to_create.append(removal)
                    latest_states[state_key] = True
                    changed_keys.add(key)

        # Apply normal staged entries.
        for md in staged:
            md.asset = asset
            md.asset_id = asset.id
            md.changeset = changeset
            md.changeset_id = changeset.id
            if md.actor is None and md.actor_id is None:
                raise ValueError("Metadata actor_id is not set for persistence")

            value_key = md.fingerprint()

            # Never persist NULL-value rows.
            if value_key is None:
                if md.removed:
                    raise ValueError(
                        "Removal rows must include a concrete value; use value=None (removed=False) to clear all values"
                    )
                continue

            state_key = (int(md.metadata_key_id), int(md.actor_id), value_key)
            if latest_states.get(state_key) == bool(md.removed):
                continue

            to_create.append(md)
            latest_states[state_key] = bool(md.removed)
            changed_keys.add(get_metadata_def_by_id(int(md.metadata_key_id)).key)

        if to_create:
            removed = sum(1 for md in to_create if md.removed is True)
            changeset.stats.metadata_values_added += len(to_create) - removed
            changeset.stats.metadata_values_removed += removed
            changeset.stats.metadata_values_changed += len(to_create)
            await Metadata.bulk_create(to_create)
            if asset._metadata_cache is not None:
                asset._metadata_cache.extend(to_create)

            # Update full-text search index for this asset based on current metadata.
            try:
                combined = list(existing_metadata) + list(to_create)
                current = self._current_metadata(combined)
                parts: list[str] = []
                for entries in current.values():
                    for md in entries:
                        if (
                            md.value_type == MetadataType.STRING
                            and md.value_text is not None
                        ):
                            parts.append(md.value_text)
                        elif (
                            md.value_type == MetadataType.INT
                            and md.value_int is not None
                        ):
                            parts.append(str(md.value_int))
                        elif (
                            md.value_type == MetadataType.FLOAT
                            and md.value_real is not None
                        ):
                            parts.append(str(md.value_real))
                        elif (
                            md.value_type == MetadataType.JSON
                            and md.value_json is not None
                        ):
                            parts.append(json.dumps(md.value_json, ensure_ascii=False))

                doc = "\n".join(parts)
                conn = Tortoise.get_connection("default")
                # Virtual tables don't support UPSERT; replace by delete+insert.
                await conn.execute_query(
                    "DELETE FROM asset_search WHERE rowid = ?", [asset.id]
                )
                await conn.execute_query(
                    "INSERT INTO asset_search(rowid, doc) VALUES(?, ?)",
                    [asset.id, doc],
                )
            except Exception as exc:
                logger.opt(exception=exc).warning(
                    f"Failed to update asset_search index for asset_id={asset.id}"
                )
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
