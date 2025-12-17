import os
from pathlib import Path
from typing import Any, AsyncIterator, Dict
from urllib.parse import unquote, urlparse

if os.name == "nt":
    import ctypes

from loguru import logger

from katalog.db import Snapshot
from katalog.sources.base import AssetRecordResult, ScanResult, SourcePlugin
from katalog.models import (
    FILE_ABSOLUTE_PATH,
    FILE_SIZE,
    FLAG_HIDDEN,
    TIME_CREATED,
    TIME_MODIFIED,
    FileAccessor,
    Asset,
)
from katalog.utils.utils import timestamp_to_utc


class FilesystemAccessor(FileAccessor):
    """
    Accessor for reading files from the local file system.
    """

    def __init__(self, path: str):
        self.path = path

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache=False
    ) -> bytes:
        """
        Read bytes from the file at the specified offset and length.
        """
        with open(self.path, "rb") as f:
            f.seek(offset)
            return f.read(length) if length is not None else f.read()


class FilesystemClient(SourcePlugin):
    """
    Client for accessing and listing files in a local file system source.
    """

    PLUGIN_ID = "dev.katalog.client.filesystem"

    def __init__(self, id: str, root_path: str, max_files: int = 500, **kwargs):
        self.id = id
        self.root_path = root_path
        self.max_files = max_files

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Local file system client",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def get_accessor(self, asset: Asset) -> Any:
        """Return an accessor keyed off the canonical absolute path."""
        if not asset.canonical_uri:
            return None
        return FilesystemAccessor(_canonical_uri_to_path(asset.canonical_uri))

    def can_connect(self, uri: str) -> bool:
        return os.path.exists(uri) and os.path.isdir(uri)

    async def scan(self, *, since_snapshot: Snapshot | None = None) -> ScanResult:
        """
        Recursively scan the directory and yield AssetRecord objects.
        """

        async def inner():
            count = 0
            for dirpath, dirnames, filenames in os.walk(self.root_path):
                for filename in filenames:
                    if count >= self.max_files:
                        logger.info(
                            f"Reached max_files limit of {self.max_files}, stopping scan."
                        )
                        # TODO mark scan result accordingly
                        return
                    full_path = os.path.join(dirpath, filename)
                    try:
                        stat = os.stat(full_path)
                        modified = timestamp_to_utc(stat.st_mtime)
                        created = timestamp_to_utc(stat.st_ctime)
                        inode = getattr(stat, "st_ino", None)
                        device = getattr(stat, "st_dev", None)
                        if inode and device:
                            # POSIX st_ino/st_dev survive renames on macOS/Linux; Windows often reports 0 so we fall back to the path identifier there.
                            asset_id = f"inode:{device}:{inode}"
                        else:
                            asset_id = f"path:{full_path}"

                        abs_path = Path(full_path).resolve()
                        asset = Asset(
                            id=asset_id,
                            provider_id=self.id,
                            canonical_uri=abs_path.as_uri(),
                        )
                        result = AssetRecordResult(asset=asset)
                        result.add_metadata(self.id, FILE_ABSOLUTE_PATH, str(abs_path))
                        result.add_metadata(self.id, TIME_MODIFIED, modified)
                        result.add_metadata(self.id, TIME_CREATED, created)
                        result.add_metadata(self.id, FILE_SIZE, int(stat.st_size))
                        result.add_metadata(
                            self.id, FLAG_HIDDEN, 1 if _looks_hidden(abs_path) else 0
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to stat {full_path} for source {self.id}: {e}"
                        )
                        continue
                    yield result
                    count += 1

        return ScanResult(iterator=inner())


def _looks_hidden(path: Path) -> bool:
    """Return True when the path appears to be hidden on the current platform."""
    dotted_component = any(
        part.startswith(".") for part in path.parts if part not in {"", ".", ".."}
    )
    if dotted_component:
        return True
    if os.name == "nt":
        FILE_ATTRIBUTE_HIDDEN = 0x02
        try:
            attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))  # type: ignore[attr-defined]
        except Exception:
            return False
        if attrs == -1:
            return False
        return bool(attrs & FILE_ATTRIBUTE_HIDDEN)
    return False


def _canonical_uri_to_path(uri: str) -> str:
    """Convert a stored canonical URI to a local filesystem path."""

    if not uri:
        return uri
    if uri.startswith("file://"):
        parsed = urlparse(uri)
        if parsed.netloc not in ("", "localhost"):
            raise ValueError(f"Unsupported file URI host '{parsed.netloc}'")
        path = unquote(parsed.path or "")
        if os.name == "nt" and path.startswith("/"):
            # Drop the leading slash so paths like /C:/foo become C:/foo
            path = path.lstrip("/")
        return path or uri
    return uri
