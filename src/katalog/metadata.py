from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, NewType
from katalog.models import MetadataType


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


def define_metadata(
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


def ensure_value_type(expected: MetadataType, value: MetadataScalar) -> None:
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
DATA_KEY = define_metadata("data", MetadataType.INT)
FILE_RECORD_KEY = define_metadata("asset", MetadataType.INT)

# Built-in metadata
FILE_ABSOLUTE_PATH = define_metadata(
    "file/absolute_path", MetadataType.STRING, "Absolute path"
)
FILE_DESCRIPTION = define_metadata(
    "file/description", MetadataType.STRING, "Description"
)
FILE_ID_PATH = define_metadata("file/id_path", MetadataType.STRING)
FILE_LAST_MODIFYING_USER = define_metadata(
    "file/last_modifying_user", MetadataType.STRING, "Last modifying user"
)
FILE_NAME = define_metadata("file/filename", MetadataType.STRING, "Filename")
FILE_ORIGINAL_NAME = define_metadata(
    "file/original_filename", MetadataType.STRING, "Original filename"
)
FILE_PATH = define_metadata("file/path", MetadataType.STRING, "Path")
FILE_QUOTA_BYTES_USED = define_metadata(
    "file/quota_bytes_used", MetadataType.INT, "Quota bytes used"
)

ACCESS_OWNER = define_metadata("access/owner", MetadataType.STRING, "Owner")
ACCESS_SHARED = define_metadata("access/shared", MetadataType.INT, "Shared", width=100)
ACCESS_SHARED_WITH = define_metadata(
    "access/shared_with", MetadataType.STRING, "Shared with"
)
ACCESS_SHARING_USER = define_metadata(
    "access/sharing_user", MetadataType.JSON, "Sharing user"
)

FILE_SIZE = define_metadata("file/size", MetadataType.INT, "Size (bytes)", width=120)
FILE_VERSION = define_metadata("file/version", MetadataType.INT, "Version")
FLAG_HIDDEN = define_metadata("flag/hidden", MetadataType.INT, "Hidden", width=100)
HASH_MD5 = define_metadata("hash/md5", MetadataType.STRING, "MD5 Hash")
MIME_TYPE = define_metadata("mime/type", MetadataType.STRING, "MIME Type")
STARRED = define_metadata("file/starred", MetadataType.INT, "Starred", width=100)
TIME_CREATED = define_metadata("time/created", MetadataType.DATETIME, "Created")
TIME_MODIFIED = define_metadata("time/modified", MetadataType.DATETIME, "Modified")
TIME_MODIFIED_BY_ME = define_metadata(
    "time/modified_by_me", MetadataType.DATETIME, "Modified by me"
)
TIME_SHARED_WITH_ME = define_metadata(
    "time/shared_with_me", MetadataType.DATETIME, "Shared with me"
)
TIME_TRASHED = define_metadata("time/trashed", MetadataType.DATETIME, "Trashed")
TIME_VIEWED_BY_ME = define_metadata(
    "time/viewed_by_me", MetadataType.DATETIME, "Viewed by me"
)
WARNING_NAME_READABILITY = define_metadata(
    "warning/name_readability", MetadataType.JSON
)

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
