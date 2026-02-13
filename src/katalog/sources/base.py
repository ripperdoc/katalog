from typing import Any, AsyncIterator, Collection, cast

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from katalog.models import (
    Asset,
    Metadata,
    MetadataChanges,
    MetadataKey,
    MetadataScalar,
    OpStatus,
    Actor,
    make_metadata,
)
from katalog.plugins.base import PluginBase
from katalog.plugins.registry import get_actor_instance


class AssetScanResult(BaseModel):
    asset: Asset
    actor: Actor
    metadata: list[Metadata] = Field(default_factory=list)

    def set_metadata(self, metadata_key: MetadataKey, value: MetadataScalar) -> None:
        """Sets e.g. replaces the metadata value on this actor for the given key with a scalar value."""
        self.metadata.append(make_metadata(metadata_key, value, self.actor.id))

    def set_metadata_list(
        self,
        metadata_key: MetadataKey,
        value: Collection[MetadataScalar],
    ) -> None:
        """Sets e.g. replaces the metadata value on this actor for the given key with a collection value."""
        for v in value:
            self.metadata.append(make_metadata(metadata_key, v, self.actor.id))


class ScanResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    iterator: AsyncIterator[AssetScanResult]
    status: OpStatus = OpStatus.IN_PROGRESS
    ignored: int = 0

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)


class SourcePlugin(PluginBase):
    """
    Source plugin for accessing and listing assets in some asset or file repository.
    """

    plugin_id: str = "katalog.sources.base.SourcePlugin"

    def __init__(self, actor: Actor, **kwargs: Any) -> None:
        super().__init__(actor, **kwargs)

    async def is_ready(self) -> tuple[bool, str | None]:
        """Return whether the source can execute in the current environment."""
        return True, None

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the plugin."""
        raise NotImplementedError()

    def authorize(self, **kwargs) -> str:
        """
        Perform any authentication steps or callback required for this source.
        Returns an authorization URL to redirect the user to, if applicable.
        """
        raise NotImplementedError()

    def get_data_reader(
        self, asset: Asset, params: dict[str, Any] | None = None
    ) -> Any:
        """Return a FileReader for the given asset (or None if not available)."""
        raise NotImplementedError()

    def get_namespace(self) -> str:
        """Return the namespace to use for external_id uniqueness."""
        return self.plugin_id

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

    def can_recurse(self, changes: MetadataChanges) -> int:
        """Return a score (>0) when this source can recurse into the given asset state."""
        _ = changes
        return 0

    async def scan_from_asset(self, changes: MetadataChanges) -> ScanResult:
        """Recursively scan from an already discovered asset."""
        _ = changes
        raise NotImplementedError()


async def make_source_instance(source_record: Actor) -> SourcePlugin:
    return cast(SourcePlugin, await get_actor_instance(source_record))
