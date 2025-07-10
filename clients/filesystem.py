import datetime
import os
from typing import Any, AsyncIterator, Dict

from clients.base import SourceClient
from models import FileAccessor, FileRecord
from utils import timestamp_to_utc

class FilesystemAccessor(FileAccessor):
    """
    Accessor for reading files from the local file system.
    """
    def __init__(self, path: str):
        self.path = path

    async def read(self, offset: int = 0, length: int | None = None, no_cache = False) -> bytes:
        """
        Read bytes from the file at the specified offset and length.
        """
        with open(self.path, 'rb') as f:
            f.seek(offset)
            return f.read(length) if length is not None else f.read()

class FilesystemClient(SourceClient):
    """
    Client for accessing and listing files in a local file system source.
    """
    def __init__(self, id: str, root_path: str, **kwargs):
        self.id = id
        self.root_path = root_path

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Local file system client",
            "author": "Katalog Team",
            "version": "0.1"
        }

    def get_accessor(self, record: FileRecord) -> Any:
        """
        Returns a FilesystemAccessor for the file represented by the FileRecord.
        """
        if not record.path:
            return None
        return FilesystemAccessor(record.path)

    def can_connect(self, uri: str) -> bool:
        return os.path.exists(uri) and os.path.isdir(uri)

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Recursively scan the directory and yield FileRecord objects.
        """
        now = datetime.datetime.utcnow()
        count = 0
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    stat = os.stat(full_path)
                    record = FileRecord(
                        path=full_path,
                        source=self.id,
                        size=stat.st_size,
                        modified_at=timestamp_to_utc(stat.st_mtime),
                        created_at=timestamp_to_utc(stat.st_ctime),
                        scanned_at=now,
                    )
                except Exception as e:
                    record = FileRecord(
                        path=full_path,
                        source=self.id,
                        error_message=str(e),
                        scanned_at=now
                    )
                yield record
                count += 1
                if count > 100:
                    return
