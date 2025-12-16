from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Collection

from katalog.db import Snapshot
from katalog.models import (
    AssetRecord,
    AssetRelationship,
    Metadata,
    MetadataKey,
    MetadataScalar,
    make_metadata,
)


@dataclass(slots=True)
class AssetRecordResult:
    asset: AssetRecord
    metadata: list[Metadata] = field(default_factory=list)
    relationships: list[AssetRelationship] = field(default_factory=list)

    def add_metadata(
        self, plugin_id: str, metadata_key: MetadataKey, value: MetadataScalar
    ):
        self.metadata.append(make_metadata(plugin_id, metadata_key, value))

    def add_metadata_set(
        self,
        plugin_id: str,
        metadata_key: MetadataKey,
        value: Collection[MetadataScalar],
    ):
        for v in value:
            self.metadata.append(make_metadata(plugin_id, metadata_key, v))


class ScanStatus(Enum):
    IN_PROGRESS = "in_progress"
    PARTIAL = "partial"
    FULL = "full"
    CANCELED = "canceled"
    ERROR = "error"


@dataclass(slots=True)
class ScanResult:
    iterator: AsyncIterator[AssetRecordResult]
    status: ScanStatus = ScanStatus.IN_PROGRESS


class SourcePlugin:
    """
    Source plugin for accessing and listing assets in some asset or file repository.
    """

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the plugin."""
        raise NotImplementedError()

    def get_accessor(self, record: AssetRecord) -> Any:
        """
        Returns an accessor for the file data represented by the AssetRecord.
        This is used to read file data.
        """
        raise NotImplementedError()

    def can_connect(self, uri: str) -> bool:
        """Check if the client can connect to the given URI."""
        raise NotImplementedError()

    async def scan(self, *, since_snapshot: Snapshot | None = None) -> ScanResult:
        """
        Scan the source, return a ScanResult with a status flag (that will be updated) and
        an async iterator that yields AssetRecordResult objects with their assets and metadata to persist.
        """
        raise NotImplementedError()
