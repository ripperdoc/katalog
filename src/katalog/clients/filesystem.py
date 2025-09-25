import os
from typing import Any, AsyncIterator, Dict

from loguru import logger

from katalog.clients.base import SourceClient
from katalog.models import FileAccessor, FileRecord
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

    def __init__(self, id: str, root_path: str, **kwargs):
        self.id = id
        self.root_path = root_path

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

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Recursively scan the directory and yield FileRecord objects.
        """
        count = 0
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
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
                    abs_path = os.path.abspath(full_path)
                    record = FileRecord(
                        id=record_id,
                        source_id=self.id,
                        canonical_uri=abs_path,
                    )
                    record.add_metadata(
                        "file/absolute_path",
                        self.PLUGIN_ID,
                        abs_path,
                        "string",
                    )
                    if modified:
                        record.add_metadata(
                            "time/modified",
                            self.PLUGIN_ID,
                            modified,
                            "datetime",
                        )
                    if created:
                        record.add_metadata(
                            "time/created",
                            self.PLUGIN_ID,
                            created,
                            "datetime",
                        )
                    record.add_metadata(
                        "file/size",
                        self.PLUGIN_ID,
                        int(stat.st_size),
                        "int",
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to stat %s for source %s: %s",
                        full_path,
                        self.id,
                        e,
                    )
                    continue
                yield record
                count += 1
                if count > 100:
                    return
