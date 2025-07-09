from typing import Any, AsyncIterator, Iterator

from models import FileRecord


class SourceClient:
    """
    Client for accessing and listing files in some file repository.
    """

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the client."""
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
