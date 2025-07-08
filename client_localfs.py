import os
from typing import Dict, Any, Iterator

class LocalFSClient:
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

    def scan(self) -> Iterator[Dict[str, Any]]:
        """
        Recursively scan the directory and yield file info dicts.
        """
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    stat = os.stat(full_path)
                    yield {
                        "path": full_path,
                        "size": stat.st_size,
                        "mtime": stat.st_mtime,
                        "ctime": stat.st_ctime,
                        "is_file": True
                    }
                except Exception as e:
                    yield {
                        "path": full_path,
                        "error": str(e)
                    }
