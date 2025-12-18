from dataclasses import asdict, dataclass
from datetime import datetime
from enum import IntEnum
from typing import Any, Mapping, NewType


MetadataScalar = (
    str | int | float | bool | datetime | Mapping[str, Any] | list[Any] | None
)
MetadataKey = NewType("MetadataKey", str)

# Plugin IDs are normally fully qualified Python class paths
# like "katalog.processors.mime_type.MimeTypeProcessor"
# But for built-in/core metadata, we use a special ID.
CORE_PLUGIN_PATH = "katalog.metadata"


class MetadataType(IntEnum):
    STRING = 0
    INT = 1
    FLOAT = 2
    DATETIME = 3
    JSON = 4
    RELATION = 5


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
    plugin_id: str = CORE_PLUGIN_PATH,
) -> MetadataKey:
    key = MetadataKey(name)
    METADATA_REGISTRY[key] = MetadataDef(
        plugin_id, key, None, value_type, title, description, width
    )
    return key


def get_metadata_id(key: MetadataKey) -> int:
    definition = get_metadata_def_by_key(key)
    if definition.registry_id is None:
        raise RuntimeError(
            f"Metadata key {key!s} has no registry_id; did you call setup()/sync_metadata_registry()?"
        )
    return definition.registry_id


def get_metadata_def_by_id(registry_id: int) -> MetadataDef:
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


def get_metadata_def_by_key(key: MetadataKey) -> MetadataDef:
    try:
        return METADATA_REGISTRY[key]
    except KeyError:  # pragma: no cover
        raise ValueError(f"Unknown metadata key {key!s}")


# Main columns for flat current table
# FILE_NAME, sort, filter
# FILE_TYPE, sort, filter
# FILE_PATH, sort, filter (pick latest path)
# FILE_SIZE, sort, filter
# TIME_CREATED, sort, filter
# TIME_MODIFIED, sort, filter
# ACCESS_OWNER, sort, filter
# ACCESS_SHARED_WITH ??
# HASH_MD5 (sort filter not necessary)
# Provider
# Added/seen
# Provider URI
# Provider ID
# Combined col for Warnings
# Combined col for other metadata fields
# Combined col for relations


# Special keys to signal changes
DATA_KEY = define_metadata("data", MetadataType.INT)
FILE_RECORD_KEY = define_metadata("asset", MetadataType.INT)

# Built-in metadata
FILE_ID_PATH = define_metadata("file/id_path", MetadataType.STRING)
FILE_NAME = define_metadata("file/filename", MetadataType.STRING, "Filename")
FILE_ORIGINAL_NAME = define_metadata(
    "file/original_filename", MetadataType.STRING, "Original filename"
)
FILE_PATH = define_metadata("file/path", MetadataType.STRING, "Path")
FILE_QUOTA_BYTES_USED = define_metadata(
    "file/quota_bytes_used", MetadataType.INT, "Quota bytes used"
)
FILE_TYPE = define_metadata("file/type", MetadataType.STRING, "MIME Type")
FILE_EXTENSION = define_metadata(
    "file/extension", MetadataType.STRING, "File extension"
)
FILE_SIZE = define_metadata("file/size", MetadataType.INT, "Size (bytes)", width=120)
FILE_VERSION = define_metadata("file/version", MetadataType.INT, "Version")
FILE_DOWNLOAD_URI = define_metadata(
    "file/download_uri", MetadataType.STRING, "Download URI"
)  # e.g. from Linux user.xdg.origin.url or macOS com.apple.metadata:kMDItemWhereFroms
FILE_URI = define_metadata("file/uri", MetadataType.STRING, "URI")
FILE_TITLE = define_metadata(
    "file/title", MetadataType.STRING, "Title"
)  # E.g. IPTC headline field
FILE_DESCRIPTION = define_metadata(
    "file/description", MetadataType.STRING, "Description"
)  # E.g. IPTC caption/description field, HTML Meta description
FILE_TAGS = define_metadata(
    "file/tags", MetadataType.JSON, "Tags"
)  # E.g. from MaxOS xattr
FILE_COMMENT = define_metadata(
    "file/comment", MetadataType.STRING, "Comment"
)  # E.g. from Linux xattr user.comment


ACCESS_OWNER = define_metadata("access/owner", MetadataType.STRING, "Owner")
ACCESS_SHARED_WITH = define_metadata(
    "access/shared_with", MetadataType.STRING, "Shared with"
)
ACCESS_SHARING_USER = define_metadata(
    "access/sharing_user", MetadataType.JSON, "Sharing user"
)
ACCESS_LAST_MODIFYING_USER = define_metadata(
    "access/last_modifying_user", MetadataType.STRING, "Last modifying user"
)

TIME_CREATED = define_metadata("time/created", MetadataType.DATETIME, "Created")
TIME_MODIFIED = define_metadata("time/modified", MetadataType.DATETIME, "Modified")
TIME_MODIFIED_BY_ME = define_metadata(
    "time/modified_by_me", MetadataType.DATETIME, "Modified by me"
)
TIME_SHARED_WITH_ME = define_metadata(
    "time/shared_with_me", MetadataType.DATETIME, "Shared with me"
)
TIME_TRASHED = define_metadata("time/trashed", MetadataType.DATETIME, "Trashed")
TIME_ACCESSED = define_metadata("time/accessed", MetadataType.DATETIME, "Last accessed")
TIME_ACCESSED_BY_ME = define_metadata(
    "time/accessed", MetadataType.DATETIME, "Accessed by me"
)
TIME_DOWNLOADED = define_metadata(
    "time/downloaded", MetadataType.DATETIME, "Downloaded"
)  # E.g. From MacOS xattr com.apple.metadata:kMDItemDownloadedDate
TIME_BIRTHTIME = define_metadata(
    "time/birthtime", MetadataType.DATETIME, "Birth/creation time (fs)"
)  # E.g. from fs stat birthtime on macOS/Windows or from EXIF original date

REL_CHILD_OF = define_metadata(
    "relationship/child_of", MetadataType.RELATION, "Child of"
)
REL_PARENT_OF = define_metadata(
    "relationship/parent_of", MetadataType.RELATION, "Parent of"
)
REL_PART_OF = define_metadata("relationship/part_of", MetadataType.RELATION, "Part of")
REL_DERIVED_FROM = define_metadata(
    "relationship/derived_from", MetadataType.RELATION, "Derived from"
)
REL_VERSION_OF = define_metadata(
    "relationship/version_of", MetadataType.RELATION, "Version of"
)
REL_SIMILAR_TO = define_metadata(
    "relationship/similar_to", MetadataType.RELATION, "Similar to"
)
REL_DUPLICATE_OF = define_metadata(
    "relationship/duplicate_of", MetadataType.RELATION, "Duplicate of"
)
REL_LINK_TO = define_metadata("relationship/link_to", MetadataType.RELATION, "Link to")

WARNING_NAME_READABILITY = define_metadata(
    "warning/name_readability", MetadataType.JSON
)
WARNING_NAME_CONVENTIONS = define_metadata(
    "warning/name_conventions", MetadataType.JSON
)

FLAG_FAVORITE = define_metadata(
    "flag/starred", MetadataType.INT, "Favorited", width=100
)
FLAG_HIDDEN = define_metadata("flag/hidden", MetadataType.INT, "Hidden", width=100)
FLAG_REVIEW = define_metadata("flag/review", MetadataType.INT, "Review", width=100)
FLAG_REJECTED = define_metadata(
    "flag/rejected", MetadataType.INT, "Rejecedt", width=100
)
FLAG_SHARED = define_metadata("flag/shared", MetadataType.INT, "Shared", width=100)
FLAG_TRASHED = define_metadata("flag/trashed", MetadataType.INT, "Trashed", width=100)


# Content fingerprints (used for similarity / deduplication)
# Hashes often represented as strings; some fingerprints are lists/maps
HASH_MD5 = define_metadata("hash/md5", MetadataType.STRING, "MD5 Hash")
HASH_SHA1 = define_metadata("hash/sha1", MetadataType.STRING, "SHA1 Hash")
HASH_MINHASH = define_metadata(
    "fingerprint/minhash", MetadataType.JSON, "MinHash fingerprint"
)
HASH_SIMHASH = define_metadata(
    "fingerprint/simhash", MetadataType.STRING, "SimHash (text)"
)
HASH_PHASH = define_metadata(
    "fingerprint/phash", MetadataType.STRING, "Perceptual hash (images)"
)
HASH_AHASH = define_metadata(
    "fingerprint/ahash", MetadataType.STRING, "Average hash (images)"
)
HASH_DHASH = define_metadata(
    "fingerprint/dhash", MetadataType.STRING, "Difference hash (images)"
)
HASH_AUDIO_CHROMAPRINT = define_metadata(
    "fingerprint/chromaprint", MetadataType.STRING, "Chromaprint (audio)"
)
HASH_SSDEEP = define_metadata(
    "fingerprint/ssdeep", MetadataType.STRING, "ssdeep fuzzy hash"
)

# Document related metadata
DOC_SUMMARY = define_metadata(
    "document/summary", MetadataType.STRING, "Document summary"
)
DOC_BYLINE = define_metadata(
    "document/byline", MetadataType.STRING, "Byline / author string"
)
DOC_LANG = define_metadata("document/lang", MetadataType.STRING, "Document language")
DOC_AUTHOR = define_metadata("document/author", MetadataType.STRING, "Document author")
DOC_KEYWORD = define_metadata(
    "document/keyword", MetadataType.STRING, "Document keyword"
)  # e.g. from PDF metadata or HTML meta keywords
DOC_CATEGORY = define_metadata(
    "document/category", MetadataType.STRING, "Document category"
)  # e.g. from PDF metadata or OpenGraph type
DOC_CHARS = define_metadata("document/chars", MetadataType.INT, "Character count")
DOC_WORDS = define_metadata("document/words", MetadataType.INT, "Word count")
DOC_PAGES = define_metadata("document/pages", MetadataType.INT, "Page count")


# EXIF common fields (also available inside `image/exif` container)
IMAGE_CAMERA_MAKE = define_metadata(
    "image/camera_make", MetadataType.STRING, "Camera maker"
)
# e.g. "Canon"
IMAGE_CAMERA_MODEL = define_metadata(
    "image/camera_model", MetadataType.STRING, "Camera model"
)  # e.g. "Canon EOS 5D Mark IV"

IMAGE_ORIENTATION = define_metadata(
    "image/orientation", MetadataType.INT, "Orientation flag"
)  # e.g. 1..8 from EXIF spec

IMAGE_FOCAL_LENGTH = define_metadata(
    "image/focal_length", MetadataType.FLOAT, "Focal length (mm)"
)  # e.g. 35.0

IMAGE_APERTURE = define_metadata(
    "image/aperture", MetadataType.FLOAT, "Aperture (f-number)"
)
# e.g. 2.8
IMAGE_ISO = define_metadata("image/iso", MetadataType.INT, "ISO speed")
# e.g. 100
IMAGE_GPS_LATITUDE = define_metadata(
    "image/gps_latitude", MetadataType.FLOAT, "GPS latitude (decimal)"
)
# e.g. 51.5074
IMAGE_GPS_LONGITUDE = define_metadata(
    "image/gps_longitude", MetadataType.FLOAT, "GPS longitude (decimal)"
)
# e.g. -0.1278


# ID3 / audio tag scalars (also inside `audio/tags` container)
# Track title uses FILE_TITLE key

AUDIO_ARTIST = define_metadata("audio/artist", MetadataType.STRING, "Artist")
# e.g. "Artist Name"
AUDIO_ALBUM = define_metadata("audio/album", MetadataType.STRING, "Album")
# e.g. "Album Title"
AUDIO_TRACK = define_metadata("audio/track", MetadataType.INT, "Track number")
# e.g. 3
AUDIO_GENRE = define_metadata("audio/genre", MetadataType.STRING, "Genre")
# e.g. "Rock"
AUDIO_YEAR = define_metadata("audio/year", MetadataType.INT, "Year")
# e.g. 1999

# e.g. ["katalog","photos"]
# OG_IMAGE = define_metadata("og/image", MetadataType.STRING, "OpenGraph image URL")
# # e.g. "https://example.com/cover.jpg"

# # e.g. "Jane Doe"
# MF_EXCERPT = define_metadata(
#     "frontmatter/excerpt", MetadataType.STRING, "Front matter excerpt"
# )
# e.g. "Short summary..."


# Metadata standards to expand into multiple fields
# sidecar XMP
# sidecar CUE (audio track markers)
# ID3 Vorbis
# Video quicktime atoms
# Schema.org (JSON-LD)

# Tools/Libraries for Reading Metadata:

# Images: Pillow, piexif, exiftool, pyexiv2
# Audio: mutagen, eyed3, tinytag
# Video: ffmpeg, hachoir, mediainfo
# PDF: PyPDF2, pdfminer, exiftool
# Office: python-docx, python-pptx, openpyxl, olefile
# General: exiftool (command-line, supports almost everything)
