import os
from pathlib import Path
from typing import Any, AsyncIterator, Dict

if os.name == "nt":
    import ctypes

from loguru import logger

from katalog.clients.base import SourceClient
from katalog.models import (
    FILE_ABSOLUTE_PATH,
    FILE_SIZE,
    FLAG_HIDDEN,
    TIME_CREATED,
    TIME_MODIFIED,
    FileAccessor,
    FileRecord,
    Metadata,
    make_metadata,
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


class FilesystemClient(SourceClient):
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

    def get_accessor(self, record: FileRecord) -> Any:
        """Return an accessor keyed off the canonical absolute path."""
        if not record.canonical_uri:
            return None
        return FilesystemAccessor(record.canonical_uri)

    def can_connect(self, uri: str) -> bool:
        return os.path.exists(uri) and os.path.isdir(uri)

    async def scan(self) -> AsyncIterator[tuple[FileRecord, list[Metadata]]]:
        """
        Recursively scan the directory and yield FileRecord objects.
        """
        count = 0
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                if count >= self.max_files:
                    logger.info(
                        "Reached max_files limit of {}, stopping scan.", self.max_files
                    )
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
                        record_id = f"inode:{device}:{inode}"
                    else:
                        record_id = f"path:{full_path}"

                    abs_path = Path(full_path).resolve()
                    record = FileRecord(
                        id=record_id,
                        source_id=self.id,
                        canonical_uri=abs_path.as_uri(),
                    )
                    metadata = list()
                    metadata.append(
                        make_metadata(self.PLUGIN_ID, FILE_ABSOLUTE_PATH, str(abs_path))
                    )
                    if modified:
                        metadata.append(
                            make_metadata(self.PLUGIN_ID, TIME_MODIFIED, modified)
                        )
                    if created:
                        metadata.append(
                            make_metadata(self.PLUGIN_ID, TIME_CREATED, created)
                        )
                    metadata.append(
                        make_metadata(self.PLUGIN_ID, FILE_SIZE, int(stat.st_size))
                    )
                    if _looks_hidden(abs_path):
                        metadata.append(make_metadata(self.PLUGIN_ID, FLAG_HIDDEN, 1))
                except Exception as e:
                    logger.warning(
                        "Failed to stat {} for source {}: {}",
                        full_path,
                        self.id,
                        e,
                    )
                    continue
                yield record, metadata
                count += 1


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
