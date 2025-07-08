import os
from typing import Dict, Any, Iterator
import datetime

class FilesystemClient:
    """
    Client for accessing and listing files in a local file system source.
    """
    def __init__(self, root_path: str):
        self.root_path = root_path

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Local file system client",
            "author": "Katalog Team",
            "version": "0.1"
        }

    def can_connect(self, uri: str) -> bool:
        return os.path.exists(uri) and os.path.isdir(uri)

    def scan(self, source_name: str, timestamp_to_utc) -> Iterator[object]:
        """
        Recursively scan the directory and yield FileRecord objects.
        """
        from models import FileRecord  # Local import to avoid circular import
        now = datetime.datetime.utcnow()
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    stat = os.stat(full_path)
                    record = FileRecord(
                        path=full_path,
                        source=source_name,
                        size=stat.st_size,
                        mtime=timestamp_to_utc(stat.st_mtime),
                        ctime=timestamp_to_utc(stat.st_ctime),
                        scanned_at=now
                    )
                except Exception as e:
                    record = FileRecord(
                        path=full_path,
                        source=source_name,
                        error_message=str(e),
                        scanned_at=now
                    )
                yield record
