from __future__ import annotations

from datetime import datetime
from abc import ABC, abstractmethod
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Literal, Mapping, NewType


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


MetadataScalar = (
    str | int | float | bool | datetime | Mapping[str, Any] | list[Any] | None
)
MetadataType = Literal["string", "int", "float", "datetime", "json"]
MetadataKey = NewType("MetadataKey", str)


@dataclass(frozen=True)
class MetadataDef:
    key: MetadataKey
    value_type: MetadataType  # e.g. "TEXT", "INTEGER", "JSONB", etc.
    title: str = ""
    description: str = ""
    width: int | None = None  # For UI display purposes


# Central registry of built-in keys
METADATA_REGISTRY: dict[MetadataKey, MetadataDef] = {}


def define_metadata_key(
    name: str,
    value_type: MetadataType,
    title: str = "",
    description: str = "",
    width: int | None = None,
) -> MetadataKey:
    key = MetadataKey(name)
    METADATA_REGISTRY[key] = MetadataDef(key, value_type, title, description, width)
    return key


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


def _ensure_value_type(expected: MetadataType, value: MetadataScalar) -> None:
    if expected == "string" and isinstance(value, str):
        return
    if expected == "int" and isinstance(value, int) and not isinstance(value, bool):
        return
    if (
        expected == "float"
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
    ):
        return
    if expected == "datetime" and isinstance(value, datetime):
        return
    if expected == "json":  # accept Mapping/list primitives
        return
    raise TypeError(f"Expected {expected}, got {type(value).__name__}")


# Special keys to signal changes
DATA_KEY = define_metadata_key("data", "int")
FILE_RECORD_KEY = define_metadata_key("asset", "int")

# Built-in metadata
FILE_ABSOLUTE_PATH = define_metadata_key(
    "file/absolute_path", "string", "Absolute path"
)
FILE_DESCRIPTION = define_metadata_key("file/description", "string", "Description")
FILE_ID_PATH = define_metadata_key("file/id_path", "string")
FILE_LAST_MODIFYING_USER = define_metadata_key(
    "file/last_modifying_user", "json", "Last modifying user"
)
FILE_NAME = define_metadata_key("file/filename", "string", "Filename")
FILE_ORIGINAL_NAME = define_metadata_key(
    "file/original_filename", "string", "Original filename"
)
FILE_PATH = define_metadata_key("file/path", "string", "Path")
FILE_QUOTA_BYTES_USED = define_metadata_key(
    "file/quota_bytes_used", "int", "Quota bytes used"
)

ACCESS_OWNER = define_metadata_key("access/owner", "string", "Owner")
ACCESS_SHARED = define_metadata_key("access/shared", "int", "Shared", width=100)
ACCESS_SHARED_WITH = define_metadata_key("access/shared_with", "string", "Shared with")
ACCESS_SHARING_USER = define_metadata_key("access/sharing_user", "json", "Sharing user")

FILE_SIZE = define_metadata_key("file/size", "int", "Size (bytes)", width=120)
FILE_VERSION = define_metadata_key("file/version", "int", "Version")
FLAG_HIDDEN = define_metadata_key("flag/hidden", "int", "Hidden", width=100)
HASH_MD5 = define_metadata_key("hash/md5", "string", "MD5 Hash")
MIME_TYPE = define_metadata_key("mime/type", "string", "MIME Type")
STARRED = define_metadata_key("file/starred", "int", "Starred", width=100)
TIME_CREATED = define_metadata_key("time/created", "datetime", "Created")
TIME_MODIFIED = define_metadata_key("time/modified", "datetime", "Modified")
TIME_MODIFIED_BY_ME = define_metadata_key(
    "time/modified_by_me", "datetime", "Modified by me"
)
TIME_SHARED_WITH_ME = define_metadata_key(
    "time/shared_with_me", "datetime", "Shared with me"
)
TIME_TRASHED = define_metadata_key("time/trashed", "datetime", "Trashed")
TIME_VIEWED_BY_ME = define_metadata_key("time/viewed_by_me", "datetime", "Viewed by me")
WARNING_NAME_READABILITY = define_metadata_key("warning/name_readability", "json")


@dataclass(slots=True)
class Metadata:
    provider_id: str | None
    key: MetadataKey
    value: MetadataScalar
    value_type: MetadataType
    confidence: float = 1.0
    id: int | None = None
    asset_id: str | None = None
    snapshot_id: int | None = None
    removed: bool = False

    def as_sql_columns(self) -> dict[str, Any]:
        """Return a dict matching metadata columns for insertion."""
        column_map = {
            "value_text": None,
            "value_int": None,
            "value_real": None,
            "value_datetime": None,
            "value_json": None,
        }
        if self.value_type == "string":
            column_map["value_text"] = str(self.value)  # type: ignore
        elif self.value_type == "int":
            column_map["value_int"] = int(self.value)  # type: ignore
        elif self.value_type == "float":
            column_map["value_real"] = float(self.value)  # type: ignore
        elif self.value_type == "datetime":
            if isinstance(self.value, datetime):
                column_map["value_datetime"] = self.value.isoformat()  # type: ignore
            else:
                column_map["value_datetime"] = str(self.value)  # type: ignore
        elif self.value_type == "json":
            column_map["value_json"] = self.value  # type: ignore
        else:
            raise ValueError(f"Unsupported value_type {self.value_type}")
        return column_map

    @classmethod
    def from_sql_row(cls, row: Mapping[str, Any]) -> Metadata:
        """Instantiate Metadata from a metadata SELECT row."""

        if row.get("value_text") is not None:
            value: Any = row["value_text"]
        elif row.get("value_int") is not None:
            value = row["value_int"]
        elif row.get("value_real") is not None:
            value = row["value_real"]
        elif row.get("value_datetime") is not None:
            value = row["value_datetime"]
        elif row.get("value_json") is not None:
            try:
                value = json.loads(row["value_json"])
            except (json.JSONDecodeError, TypeError):
                value = row["value_json"]
        else:
            value = None

        confidence = row.get("confidence", 1.0)
        if confidence is None:
            confidence = 1.0

        removed_raw = row.get("removed", 0)
        return cls(
            id=row.get("id"),
            asset_id=row.get("asset_id"),
            provider_id=row.get("provider_id"),
            snapshot_id=row.get("snapshot_id"),
            key=MetadataKey(row["metadata_key"]),
            value=value,
            value_type=row["value_type"],
            confidence=float(confidence),
            removed=bool(removed_raw),
        )

    @classmethod
    def list_to_dict_by_key(cls, entries: Iterable[Metadata]) -> dict[str, list]:
        """Convert a list of Metadata entries to a dict keyed by metadata_key."""
        result: dict[str, list] = {}
        for entry in entries:
            result.setdefault(str(entry.key), []).append(entry)
        return result

    def to_json(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of this metadata entry."""

        return {
            "id": self.id,
            "asset_id": self.asset_id,
            "provider_id": self.provider_id,
            "snapshot_id": self.snapshot_id,
            "metadata_key": str(self.key),
            "value_type": self.value_type,
            "value": self.value,
            "confidence": self.confidence,
            "removed": self.removed,
        }


def make_metadata(
    provider_id: str,
    key: MetadataKey,
    value: MetadataScalar,
    *,
    confidence: float = 1.0,
):
    metadata_def = get_metadata_def(key)
    # Treat None as a request to clear the metadata key rather than an actual value.
    if value is not None:
        _ensure_value_type(metadata_def.value_type, value)
    return Metadata(
        provider_id=provider_id,
        key=key,
        value=value,
        value_type=metadata_def.value_type,
        confidence=confidence,
    )


@dataclass(slots=True)
class AssetRecord:
    id: str
    provider_id: str
    canonical_uri: str
    created_snapshot_id: int | None = None
    last_snapshot_id: int | None = None
    deleted_snapshot_id: int | None = None
    _data_accessor: FileAccessor | None = field(init=False, repr=False, default=None)

    @property
    def data(self) -> FileAccessor | None:
        return self._data_accessor

    def attach_accessor(self, accessor: FileAccessor | None) -> None:
        self._data_accessor = accessor


@dataclass(slots=True)
class AssetRelationship:
    id: int
    provider_id: str
    from_id: str
    to_id: str
    relationship_type: str
    snapshot_id: int
    removed: bool
    confidence: float | None
    description: str | None


@dataclass(slots=True)
class Snapshot:
    id: int
    provider_id: str
    started_at: datetime
    status: str
    completed_at: datetime | None = None
    metadata: dict[str, Any] | None = None


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

    _changed_assets: set[str] = field(default_factory=set, init=False, repr=False)
    _added_assets: set[str] = field(default_factory=set, init=False, repr=False)
    _modified_assets: set[str] = field(default_factory=set, init=False, repr=False)

    def record_asset_change(self, asset_id: str, *, added: bool) -> None:
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
