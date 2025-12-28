from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Collection, cast

from katalog.models import (
    Asset,
    Metadata,
    MetadataKey,
    MetadataScalar,
    OpStatus,
    Provider,
    Snapshot,
    make_metadata,
)
from katalog.plugins.base import PluginBase
from katalog.utils.utils import import_plugin_class


@dataclass(slots=True)
class AssetScanResult:
    asset: Asset
    provider: Provider
    metadata: list[Metadata] = field(default_factory=list)

    def add_metadata(self, metadata_key: MetadataKey, value: MetadataScalar) -> None:
        self.metadata.append(make_metadata(metadata_key, value, self.provider.id))

    def add_metadata_set(
        self,
        metadata_key: MetadataKey,
        value: Collection[MetadataScalar],
    ) -> None:
        for v in value:
            self.metadata.append(make_metadata(metadata_key, v, self.provider.id))


@dataclass(slots=True)
class ScanResult:
    iterator: AsyncIterator[AssetScanResult]
    status: OpStatus = OpStatus.IN_PROGRESS


class SourcePlugin(PluginBase):
    """
    Source plugin for accessing and listing assets in some asset or file repository.
    """

    def __init__(self, provider: Provider, **kwargs: Any) -> None:
        super().__init__(provider, **kwargs)

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the plugin."""
        raise NotImplementedError()

    def get_accessor(self, asset: Asset) -> Any:
        """
        Returns an accessor for the file data represented by the Asset.
        This is used to read file data.
        """
        raise NotImplementedError()

    def can_connect(self, uri: str) -> bool:
        """Check if the client can connect to the given URI."""
        raise NotImplementedError()

    async def scan(self) -> ScanResult:
        """
        Scan the source, return a ScanResult with a status flag (that will be updated)
        and an async iterator that yields AssetScanResult objects with their assets and
        metadata to persist.
        """
        raise NotImplementedError()


def make_source_instance(source_record: Provider) -> SourcePlugin:
    SourceClass = cast(type[SourcePlugin], import_plugin_class(source_record.plugin_id))
    return SourceClass(provider=source_record, **(source_record.config or {}))
