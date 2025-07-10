from typing import Any, AsyncIterator, Iterator

from models import FileRecord


class SourceClient:
    """
    Client for accessing and listing files in some file repository.
    """

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the client."""
        raise NotImplementedError()
    
    def get_accessor(self, record: FileRecord) -> Any:
        """
        Returns an accessor for the file represented by the FileRecord.
        This is used to read file data.
        """
        raise NotImplementedError()

    def can_connect(self, uri: str) -> bool:
        """Check if the client can connect to the given URI."""
        raise NotImplementedError()

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Scan the source and yields FileRecord objects.
        """
        if False:
            yield  # This makes it an async generator
