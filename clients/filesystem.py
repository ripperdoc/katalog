import datetime
import os
from typing import Any, AsyncIterator, Dict, Iterator

from clients.base import SourceClient
from models import FileRecord
from utils import timestamp_to_utc


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

    def can_connect(self, uri: str) -> bool:
        return os.path.exists(uri) and os.path.isdir(uri)

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Recursively scan the directory and yield FileRecord objects.
        """
        now = datetime.datetime.utcnow()
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    stat = os.stat(full_path)
                    record = FileRecord(
                        path=full_path,
                        source=self.id,
                        size=stat.st_size,
                        modified=timestamp_to_utc(stat.st_mtime),
                        created=timestamp_to_utc(stat.st_ctime),
                        scanned_at=now
                    )
                except Exception as e:
                    record = FileRecord(
                        path=full_path,
                        source=self.id,
                        error_message=str(e),
                        scanned_at=now
                    )
                yield record
