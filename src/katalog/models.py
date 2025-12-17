from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, UTC
from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Mapping, NewType, Sequence

from tortoise import Tortoise
from tortoise.transactions import in_transaction
from tortoise.fields import (
    BigIntField,
    BooleanField,
    CASCADE,
    IntEnumField,
    CharEnumField,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    JSONField,
    IntField,
    RESTRICT,
    SET_NULL,
    TextField,
)
from tortoise.models import Model

from katalog.config import config_file

"""Data usage notes
- The target profile for this system is to handle metadata for 1 million files. Actual file contents is not to be stored in the DB.
- This implies
- ~1 million Asset records
- ~30 million Metadata records (assuming an average of 30 metadata entries per asset). 
Metadata will mostly be shorter text and date values, but some fields may grow pretty large, such as text contents, summaries, etc.
- ~5 million AssetRelationship records (assuming an average of 5 relationships per asset)
- 10 to 100 Providers
- As data changes over time, snapshots will be created, increasing the number of Metadata and AssetRelationship rows per asset. 
On the other hand, users will be encouraged to purge snapshots regularly.
"""


def fqn(cls: type) -> str:
    # return f"{cls.__module__}.{cls.__qualname__}"
    return f"models.{cls.__qualname__}"


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
    ) -> "Snapshot":
        provider_id = provider.id if isinstance(provider, Provider) else provider
        return await cls.create(
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

    class Meta(Model.Meta):
        indexes = (("provider", "last_snapshot"),)


class MetadataType(IntEnum):
    STRING = 0
    INT = 1
    FLOAT = 2
    DATETIME = 3
    JSON = 4


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
    removed = BooleanField(default=False)
    # Null means no confidence score, which can be assumed to be 1.0
    confidence = FloatField(null=True)

    class Meta(Model.Meta):
        indexes = ("metadata_key", "value_type")
        unique_together = ("asset", "provider", "snapshot", "metadata_key")

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

        # Fallback for unexpected values.
        if self.value_text is not None:
            return self.value_text
        if self.value_int is not None:
            return self.value_int
        if self.value_real is not None:
            return self.value_real
        if self.value_datetime is not None:
            return self.value_datetime
        if self.value_json is not None:
            return self.value_json
        return None

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


class AssetRelationship(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(
        fqn(Provider), related_name="relationships", on_delete=CASCADE
    )
    from_asset = ForeignKeyField(
        fqn(Asset), related_name="outgoing_relationships", on_delete=CASCADE
    )
    to_asset = ForeignKeyField(
        fqn(Asset), related_name="incoming_relationships", on_delete=CASCADE
    )
    relationship_type = CharField(max_length=255)
    snapshot = ForeignKeyField(fqn(Snapshot), on_delete=CASCADE)
    removed = BooleanField(default=False)
    confidence = FloatField(null=True)
    description = TextField(null=True)

    class Meta(Model.Meta):
        indexes = (("provider", "from_asset", "to_asset", "relationship_type"),)


async def setup(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite://{db_path}"
    # When executed via `python -m katalog.models_tortoise`, Python runs the code as
    # `__main__`, so the model classes live in that module.
    await Tortoise.init(db_url=db_url, modules={"models": [__name__]})
    await Tortoise.generate_schemas()
    await sync_metadata_registry()
    return db_path


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


MetadataScalar = (
    str | int | float | bool | datetime | Mapping[str, Any] | list[Any] | None
)
MetadataKey = NewType("MetadataKey", str)


CORE_PLUGIN_ID = "katalog"


@dataclass(frozen=True)
class MetadataDef:
    plugin_id: str
    key: MetadataKey
    registry_id: int | None
    value_type: MetadataType
    title: str = ""
    description: str = ""
    width: int | None = None  # For UI display purposes


# Central registry of built-in keys
METADATA_REGISTRY: dict[MetadataKey, MetadataDef] = {}

# Fast lookup from DB integer id -> definition. Populated by `sync_metadata_registry()`.
METADATA_REGISTRY_BY_ID: dict[int, MetadataDef] = {}


def define_metadata_key(
    name: str,
    value_type: MetadataType,
    title: str = "",
    description: str = "",
    width: int | None = None,
    *,
    plugin_id: str = CORE_PLUGIN_ID,
) -> MetadataKey:
    key = MetadataKey(name)
    METADATA_REGISTRY[key] = MetadataDef(
        plugin_id, key, None, value_type, title, description, width
    )
    return key


async def sync_metadata_registry() -> None:
    """Ensure DB registry contains the import-time declared keys.

    DB remains the source of truth: this only INSERTs missing keys; it does not
    update existing rows.
    """

    # Insert missing rows, then record the integer IDs locally for fast queries.
    for meta_key, definition in list(METADATA_REGISTRY.items()):
        row, _created = await MetadataRegistry.get_or_create(
            plugin_id=definition.plugin_id,
            key=str(definition.key),
            defaults={
                "value_type": definition.value_type,
                "title": definition.title,
                "description": definition.description,
                "width": definition.width,
            },
        )
        METADATA_REGISTRY[meta_key] = MetadataDef(
            plugin_id=definition.plugin_id,
            key=definition.key,
            registry_id=row.id,
            value_type=definition.value_type,
            title=definition.title,
            description=definition.description,
            width=definition.width,
        )

    # Rebuild the reverse mapping (id -> definition) for O(1) lookups.
    # Read the DB registry once at startup so `Metadata.key` never needs to touch
    # `MetadataRegistry` at runtime (and so it still works for keys that exist in
    # the DB but weren't imported/defined in this process).
    METADATA_REGISTRY_BY_ID.clear()
    for row in await MetadataRegistry.all():
        METADATA_REGISTRY_BY_ID[int(row.id)] = MetadataDef(
            plugin_id=row.plugin_id,
            key=MetadataKey(row.key),
            registry_id=int(row.id),
            value_type=row.value_type,
            title=row.title,
            description=row.description,
            width=row.width,
        )


def get_metadata_registry_id(key: MetadataKey) -> int:
    definition = get_metadata_def(key)
    if definition.registry_id is None:
        raise RuntimeError(
            f"Metadata key {key!s} has no registry_id; did you call setup()/sync_metadata_registry()?"
        )
    return definition.registry_id


def get_metadata_def_by_registry_id(registry_id: int) -> MetadataDef:
    try:
        return METADATA_REGISTRY_BY_ID[registry_id]
    except KeyError:  # pragma: no cover
        raise KeyError(
            f"Unknown metadata registry_id={registry_id}. "
            "Did you import all plugins and call setup()/sync_metadata_registry()?"
        )


def get_metadata_schema(key: MetadataKey) -> dict:
    definition = METADATA_REGISTRY.get(key)
    if definition is None:
        return {}
    else:
        return asdict(definition)


def get_metadata_def(key: MetadataKey) -> MetadataDef:
    try:
        return METADATA_REGISTRY[key]
    except KeyError:  # pragma: no cover
        raise ValueError(f"Unknown metadata key {key!s}")


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
    _ensure_value_type(definition.value_type, value)

    entry = Metadata(
        provider_id=int(provider_id),
        metadata_key_id=get_metadata_registry_id(key),
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


def _ensure_value_type(expected: MetadataType, value: MetadataScalar) -> None:
    if expected == MetadataType.STRING and isinstance(value, str):
        return
    if (
        expected == MetadataType.INT
        and isinstance(value, int)
        and not isinstance(value, bool)
    ):
        return
    if (
        expected == MetadataType.FLOAT
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
    ):
        return
    if expected == MetadataType.DATETIME and isinstance(value, datetime):
        return
    if expected == MetadataType.JSON:  # accept Mapping/list primitives
        return
    raise TypeError(f"Expected {expected}, got {type(value).__name__}")


# Special keys to signal changes
DATA_KEY = define_metadata_key("data", MetadataType.INT)
FILE_RECORD_KEY = define_metadata_key("asset", MetadataType.INT)

# Built-in metadata
FILE_ABSOLUTE_PATH = define_metadata_key(
    "file/absolute_path", MetadataType.STRING, "Absolute path"
)
FILE_DESCRIPTION = define_metadata_key(
    "file/description", MetadataType.STRING, "Description"
)
FILE_ID_PATH = define_metadata_key("file/id_path", MetadataType.STRING)
FILE_LAST_MODIFYING_USER = define_metadata_key(
    "file/last_modifying_user", MetadataType.STRING, "Last modifying user"
)
FILE_NAME = define_metadata_key("file/filename", MetadataType.STRING, "Filename")
FILE_ORIGINAL_NAME = define_metadata_key(
    "file/original_filename", MetadataType.STRING, "Original filename"
)
FILE_PATH = define_metadata_key("file/path", MetadataType.STRING, "Path")
FILE_QUOTA_BYTES_USED = define_metadata_key(
    "file/quota_bytes_used", MetadataType.INT, "Quota bytes used"
)

ACCESS_OWNER = define_metadata_key("access/owner", MetadataType.STRING, "Owner")
ACCESS_SHARED = define_metadata_key(
    "access/shared", MetadataType.INT, "Shared", width=100
)
ACCESS_SHARED_WITH = define_metadata_key(
    "access/shared_with", MetadataType.STRING, "Shared with"
)
ACCESS_SHARING_USER = define_metadata_key(
    "access/sharing_user", MetadataType.JSON, "Sharing user"
)

FILE_SIZE = define_metadata_key(
    "file/size", MetadataType.INT, "Size (bytes)", width=120
)
FILE_VERSION = define_metadata_key("file/version", MetadataType.INT, "Version")
FLAG_HIDDEN = define_metadata_key("flag/hidden", MetadataType.INT, "Hidden", width=100)
HASH_MD5 = define_metadata_key("hash/md5", MetadataType.STRING, "MD5 Hash")
MIME_TYPE = define_metadata_key("mime/type", MetadataType.STRING, "MIME Type")
STARRED = define_metadata_key("file/starred", MetadataType.INT, "Starred", width=100)
TIME_CREATED = define_metadata_key("time/created", MetadataType.DATETIME, "Created")
TIME_MODIFIED = define_metadata_key("time/modified", MetadataType.DATETIME, "Modified")
TIME_MODIFIED_BY_ME = define_metadata_key(
    "time/modified_by_me", MetadataType.DATETIME, "Modified by me"
)
TIME_SHARED_WITH_ME = define_metadata_key(
    "time/shared_with_me", MetadataType.DATETIME, "Shared with me"
)
TIME_TRASHED = define_metadata_key("time/trashed", MetadataType.DATETIME, "Trashed")
TIME_VIEWED_BY_ME = define_metadata_key(
    "time/viewed_by_me", MetadataType.DATETIME, "Viewed by me"
)
WARNING_NAME_READABILITY = define_metadata_key(
    "warning/name_readability", MetadataType.JSON
)


# version_of
# variant_of

# source_status: found, error, missing, new, deleted, moved
# flag_review, flag_delete, flag_favorite, flag_hide

# Content fingerprints
# Used for similarity and deduplication
# MinHash (LSH), SimHash for text
# pHash, aHash, dHash for images
# Chromaprint / AcoustID, dejavu for audio
# ssdeep, tlsh, sdhash for general binary content

# Filename (derived?)
# Extension (derived?)
# Parent folder (derived)
# Tags
# Access time (st_atime, in most filesystems)
# Birth time (st_birthtime, in APFS, NTFS, FAT)
# Downloaded time: MacOS extended attributes

# Document related metadata
# original_uri: str | None = None
# download_uri: str | None = None # If given, a special URL that can be used to download the document but not used as ID
# uri: str
# title: str | None = None
# summary: str | None = None
# description: str | None = None
# byline: str | None = None
# lang: str | None = None
# authors: list[str] = []
# keywords: list[str] = []
# characters: int = 0 - generated


# Extended attributes
# Access using Python xattr library
# Available mostly in MacOS and Linux. Namespaces like user., system., security.
# Example: user.downloaded, system.metadata, security.label
# macOS (APFS, HFS+)
# com.apple.quarantine — Quarantine flag for downloaded files
# com.apple.metadata:kMDItemWhereFroms — Download source URLs (plist)
# com.apple.metadata:kMDItemDownloadedDate — Downloaded date (plist)
# com.apple.metadata:_kMDItemUserTags — Finder tags
# com.apple.FinderInfo — Finder metadata
# com.apple.ResourceFork — Classic Mac resource fork
# com.apple.lastuseddate#PS — Last used date (plist)

# Linux (ext4, XFS, etc.)
# user.comment — User comment
# user.xdg.origin.url — Download source URL (used by some apps)

# Image metadata standards
# EXIF: Exchangeable Image File Format (JPEG, TIFF, some PNG, WebP)
# IPTC: International Press Telecommunications Council (news/photo metadata, often embedded in JPEG)
# XMP: Extensible Metadata Platform (Adobe, can be embedded in many formats, including JPEG, TIFF, PNG, PDF)
# JFIF: JPEG File Interchange Format (basic metadata for JPEG)

# Audio metadata standards
# ID3: Used in MP3 files (ID3v1, ID3v2) for title, artist, album, etc.
# Vorbis Comments: Used in OGG, FLAC, Opus, and others
# APE tags: Used in Monkey’s Audio and some other formats
# RIFF INFO: Used in WAV and AVI files

# Video metadata standards
# RIFF INFO: Used in AVI, WAV
# QuickTime/MP4 atoms: Metadata in MOV/MP4 files
# Matroska tags: Used in MKV files
# XMP: Can be embedded in some video formats

# Documents
# PDF:
# Document Info Dictionary: Title, Author, Subject, etc.
# XMP: Embedded for richer metadata
# Microsoft Office (DOCX, XLSX, PPTX):
# Core Properties: Title, Author, Created, Modified, etc. (stored as XML in the ZIP container)
# Custom Properties: User-defined fields
# OpenDocument (ODT, ODS, ODP):
# Meta.xml: Contains document metadata
# EPUB:
# OPF file: Metadata in XML
# Plain text/Markdown:
# Sometimes a YAML front matter block is used for metadata

# Sidecar files:
# .xmp files (for images, video, audio)
# .cue files (for audio CDs)

# Websites, HTML
# HTML meta tags: <meta name="description" content="...">
# Open Graph tags: <meta property="og:title" content="...">
# Dublin Core: <meta name="DC.title" content="...">
# RDFa: <div vocab="http://schema.org/" typeof="Article">
# JSON-LD: <script type="application/ld+json">{"@context": "http://schema.org", "@type": "Article", "headline": "..."}</script>

# Tools/Libraries for Reading Metadata:

# Images: Pillow, piexif, exiftool, pyexiv2
# Audio: mutagen, eyed3, tinytag
# Video: ffmpeg, hachoir, mediainfo
# PDF: PyPDF2, pdfminer, exiftool
# Office: python-docx, python-pptx, openpyxl, olefile
# General: exiftool (command-line, supports almost everything)
