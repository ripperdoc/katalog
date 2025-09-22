import datetime as _dt
from abc import ABC, abstractmethod
from typing import Optional

from sqlalchemy import JSON
from sqlmodel import Column, Field, SQLModel


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class FileRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    path: str  # Must be normalized file path
    other_paths: list[str] | None = Field(default=None, sa_column=Column(JSON))
    source: str  # Named source, foreign key to a source table if needed
    filename: Optional[str] = None
    size: Optional[int] = None
    modified_at: Optional[_dt.datetime] = None  # Unix timestamp
    created_at: Optional[_dt.datetime] = None  # Unix timestamp
    scanned_at: Optional[_dt.datetime] = None
    found_at: Optional[_dt.datetime] = None  # When the file was found in the source
    lost_at: Optional[_dt.datetime] = (
        None  # When the file was lost from the source, e.g. deleted
    )
    error_message: Optional[str] = None
    mime_type: str | None = None
    # TODO other content hashes can be sha1 and sha256, usefuf if several cloud services prefer that
    md5: str | None = None
    is_virtual: bool = (
        False  # If True, this record is virtual and does not have a real file on disk
    )

    @property
    def data(self) -> FileAccessor | None:
        return getattr(self, "_data_accessor", None)

    # asset_id: foreign key to an asset table. Many files record can point to the same asset.
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


class ProcessorResult(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    file_id: int = Field(foreign_key="filerecord.id", index=True)
    processor_id: str
    cache_key: str
    ran_at: _dt.datetime = Field(default_factory=_dt.datetime.utcnow)
