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

# Content fingerprints (used for similarity / deduplication)
# Hashes often represented as strings; some fingerprints are lists/maps
FINGERPRINT_MINHASH = define_metadata(
    "fingerprint/minhash", MetadataType.JSON, "MinHash fingerprint"
)
FINGERPRINT_SIMHASH = define_metadata(
    "fingerprint/simhash", MetadataType.STRING, "SimHash (text)"
)
FINGERPRINT_PHASH = define_metadata(
    "fingerprint/phash", MetadataType.STRING, "Perceptual hash (images)"
)
FINGERPRINT_AHASH = define_metadata(
    "fingerprint/ahash", MetadataType.STRING, "Average hash (images)"
)
FINGERPRINT_DHASH = define_metadata(
    "fingerprint/dhash", MetadataType.STRING, "Difference hash (images)"
)
FINGERPRINT_AUDIO_CHROMAPRINT = define_metadata(
    "fingerprint/chromaprint", MetadataType.STRING, "Chromaprint (audio)"
)
FINGERPRINT_SSDEEP = define_metadata(
    "fingerprint/ssdeep", MetadataType.STRING, "ssdeep fuzzy hash"
)

# Derived / filesystem values
FILE_EXTENSION = define_metadata(
    "file/extension", MetadataType.STRING, "File extension"
)
FILE_PARENT = define_metadata("file/parent", MetadataType.STRING, "Parent folder path")
FILE_ATIME = define_metadata("file/atime", MetadataType.DATETIME, "Access time")
FILE_BIRTHTIME = define_metadata(
    "file/birthtime", MetadataType.DATETIME, "Birth/creation time (fs)"
)

# Downloaded time: MacOS extended attributes

# Tags and simple collections
TAGS = define_metadata("file/tags", MetadataType.JSON, "Tags")

# Document related metadata
DOC_ORIGINAL_URI = define_metadata(
    "document/original_uri", MetadataType.STRING, "Original document URI"
)
DOC_DOWNLOAD_URI = define_metadata(
    "document/download_uri", MetadataType.STRING, "Download URI"
)
DOC_URI = define_metadata("document/uri", MetadataType.STRING, "Document canonical URI")
DOC_TITLE = define_metadata("document/title", MetadataType.STRING, "Document title")
DOC_SUMMARY = define_metadata(
    "document/summary", MetadataType.STRING, "Document summary"
)
DOC_BYLINE = define_metadata(
    "document/byline", MetadataType.STRING, "Byline / author string"
)
DOC_LANG = define_metadata("document/lang", MetadataType.STRING, "Document language")
DOC_AUTHOR = define_metadata("document/author", MetadataType.JSON, "Document author")
DOC_KEYWORD = define_metadata(
    "document/keywords", MetadataType.JSON, "Document keyword"
)
DOC_CHARACTERS = define_metadata("document/chars", MetadataType.INT, "Character count")
DOC_WORDS = define_metadata("document/words", MetadataType.INT, "Word count")
DOC_PAGES = define_metadata("document/pages", MetadataType.INT, "Page count")

# Image / Audio / Video specific metadata containers (structured JSON)
IMAGE_EXIF = define_metadata("image/exif", MetadataType.JSON, "Image EXIF metadata")
IMAGE_IPTC = define_metadata("image/iptc", MetadataType.JSON, "Image IPTC metadata")
IMAGE_XMP = define_metadata("image/xmp", MetadataType.JSON, "Image XMP metadata")

AUDIO_TAGS = define_metadata("audio/tags", MetadataType.JSON, "Audio tags (ID3/Vorbis)")
VIDEO_ATOMS = define_metadata(
    "video/atoms", MetadataType.JSON, "QuickTime/MP4 atoms or similar"
)

# Sidecar / external metadata
SIDECAR_XMP = define_metadata("sidecar/xmp", MetadataType.JSON, "Sidecar XMP data")
SIDECAR_CUE = define_metadata(
    "sidecar/cue", MetadataType.JSON, "Sidecar CUE data (audio)"
)

# Extended attributes (OS-specific). These are intentionally generic JSON
# because xattrs can contain platform-specific formats (plist, binary).
# Examples: macOS `com.apple.metadata:kMDItemWhereFroms`, `com.apple.metadata:_kMDItemUserTags`.
XATTR = define_metadata("xattr/all", MetadataType.JSON, "All extended attributes")
XATTR_DOWNLOADED_DATE = define_metadata(
    "xattr/downloaded_date",
    MetadataType.DATETIME,
    "Downloaded date from xattr (if available)",
)
XATTR_FINDER_TAGS = define_metadata(
    "xattr/finder_tags", MetadataType.JSON, "Finder tags / user tags from xattr"
)

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

# --- More specific fields derived from standards/tools ---
# Examples are shown inline (truncated) to illustrate typical values.

# EXIF common fields (also available inside `image/exif` container)
IMAGE_EXIF_MAKE = define_metadata(
    "image/exif/make", MetadataType.STRING, "Camera maker"
)
# e.g. "Canon"
IMAGE_EXIF_MODEL = define_metadata(
    "image/exif/model", MetadataType.STRING, "Camera model"
)
# e.g. "Canon EOS 5D Mark IV"
IMAGE_EXIF_DATETIME_ORIGINAL = define_metadata(
    "image/exif/datetime_original", MetadataType.DATETIME, "Original capture time"
)
# e.g. 2021-07-14T12:34:56
IMAGE_EXIF_ORIENTATION = define_metadata(
    "image/exif/orientation", MetadataType.INT, "Orientation flag"
)
# e.g. 1..8
IMAGE_EXIF_FOCAL_LENGTH = define_metadata(
    "image/exif/focal_length", MetadataType.FLOAT, "Focal length (mm)"
)
# e.g. 35.0
IMAGE_EXIF_APERTURE = define_metadata(
    "image/exif/aperture", MetadataType.FLOAT, "Aperture (f-number)"
)
# e.g. 2.8
IMAGE_EXIF_ISO = define_metadata("image/exif/iso", MetadataType.INT, "ISO speed")
# e.g. 100
IMAGE_GPS_LATITUDE = define_metadata(
    "image/exif/gps_latitude", MetadataType.FLOAT, "GPS latitude (decimal)"
)
# e.g. 51.5074
IMAGE_GPS_LONGITUDE = define_metadata(
    "image/exif/gps_longitude", MetadataType.FLOAT, "GPS longitude (decimal)"
)
# e.g. -0.1278

# IPTC common fields (also inside `image/iptc`)
IMAGE_IPTC_HEADLINE = define_metadata(
    "image/iptc/headline", MetadataType.STRING, "IPTC headline"
)
# e.g. "Protest March Downtown"
IMAGE_IPTC_CAPTION = define_metadata(
    "image/iptc/caption", MetadataType.STRING, "IPTC caption/description"
)
# e.g. "Crowds gathered in central plaza..."

# ID3 / audio tag scalars (also inside `audio/tags` container)
AUDIO_TITLE = define_metadata("audio/title", MetadataType.STRING, "Track title")
# e.g. "Song Name"
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

# HTML meta / OpenGraph / Schema.org
HTML_META_DESCRIPTION = define_metadata(
    "html/meta_description", MetadataType.STRING, "HTML meta description"
)
# e.g. "A short summary of the page"
HTML_META_KEYWORDS = define_metadata(
    "html/meta_keywords", MetadataType.JSON, "HTML meta keywords"
)
# e.g. ["katalog","photos"]
OG_TITLE = define_metadata("og/title", MetadataType.STRING, "OpenGraph title")
# e.g. "Article Title"
OG_DESCRIPTION = define_metadata(
    "og/description", MetadataType.STRING, "OpenGraph description"
)
# e.g. "Summary for social cards"
OG_TYPE = define_metadata("og/type", MetadataType.STRING, "OpenGraph type")
# e.g. "article"
OG_IMAGE = define_metadata("og/image", MetadataType.STRING, "OpenGraph image URL")
# e.g. "https://example.com/cover.jpg"
OG_URL = define_metadata("og/url", MetadataType.STRING, "OpenGraph canonical URL")
# e.g. "https://example.com/article/1"
SCHEMA_ORG = define_metadata(
    "schemaorg/jsonld", MetadataType.JSON, "Schema.org JSON-LD"
)
# e.g. {"@context":..., "@type":"Article", ...}

# Markdown front matter (YAML/ TOML) common fields
MF_TITLE = define_metadata(
    "frontmatter/title", MetadataType.STRING, "Front matter title"
)
# e.g. "My Post"
MF_DATE = define_metadata(
    "frontmatter/date", MetadataType.DATETIME, "Front matter date"
)
# e.g. 2022-01-01
MF_TAGS = define_metadata("frontmatter/tags", MetadataType.JSON, "Front matter tags")
# e.g. ["notes","work"]
MF_CATEGORIES = define_metadata(
    "frontmatter/categories", MetadataType.JSON, "Front matter categories"
)
# e.g. ["blog"]
MF_AUTHOR = define_metadata(
    "frontmatter/author", MetadataType.STRING, "Front matter author"
)
# e.g. "Jane Doe"
MF_EXCERPT = define_metadata(
    "frontmatter/excerpt", MetadataType.STRING, "Front matter excerpt"
)
# e.g. "Short summary..."

# Office / ODT metadata
OFFICE_CORE_PROPERTIES = define_metadata(
    "office/core_properties", MetadataType.JSON, "Office core properties (docx/odt)"
)
# e.g. {"creator":"Alice","created":"2020-01-01T...",...}
OFFICE_CUSTOM_PROPERTIES = define_metadata(
    "office/custom_properties", MetadataType.JSON, "Office custom properties"
)
# e.g. {"Project":"Katalog","Reviewed":True}
ODT_EDITOR = define_metadata("odt/editor", MetadataType.STRING, "ODT last editor")
# e.g. "libreoffice 7.0"


# Tools/Libraries for Reading Metadata:

# Images: Pillow, piexif, exiftool, pyexiv2
# Audio: mutagen, eyed3, tinytag
# Video: ffmpeg, hachoir, mediainfo
# PDF: PyPDF2, pdfminer, exiftool
# Office: python-docx, python-pptx, openpyxl, olefile
# General: exiftool (command-line, supports almost everything)
