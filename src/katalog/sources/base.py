from typing import Any, AsyncIterator

from katalog.models import AssetRecord, Metadata


class SourcePlugin:
    """
    Client for accessing and listing files in some file repository.
    """

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the client."""
        raise NotImplementedError()

    def get_accessor(self, record: AssetRecord) -> Any:
        """
        Returns an accessor for the file represented by the AssetRecord.
        This is used to read file data.
        """
        raise NotImplementedError()

    def can_connect(self, uri: str) -> bool:
        """Check if the client can connect to the given URI."""
        raise NotImplementedError()

    async def scan(self) -> AsyncIterator[tuple[AssetRecord, list[Metadata]]]:
        """
        Scan the source and yield AssetRecord objects with their metadata payloads.
        """
        if False:
            yield  # This makes it an async generator
