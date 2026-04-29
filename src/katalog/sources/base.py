from dataclasses import dataclass
from typing import Any, AsyncIterator, Collection, cast

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from katalog.models import (
    Asset,
    DataReader,
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
from katalog.workflows.contracts import (
    RecursionSeed,
    SourceAssetPayload,
    SourceBatch,
)


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


class ScanBatch(BaseModel):
    """One pull response from a source scan."""

    items: list[AssetScanResult] = Field(default_factory=list)
    cursor: str | None = None
    status: OpStatus = OpStatus.IN_PROGRESS
    ignored: int = 0

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)


@dataclass
class _ScanSession:
    """Internal cursor session around an async iterator-based scan."""

    iterator: AsyncIterator[AssetScanResult]
    status_ref: ScanResult


class SourcePlugin(PluginBase):
    """
    Source plugin for accessing and listing assets in some asset or file repository.
    """

    plugin_id: str = "katalog.sources.base.SourcePlugin"

    def __init__(self, actor: Actor, **kwargs: Any) -> None:
        super().__init__(actor, **kwargs)
        self._next_scan_cursor = 0
        self._scan_sessions: dict[str, _ScanSession] = {}
        self._next_recursive_cursor = 0
        self._recursive_scan_sessions: dict[str, _ScanSession] = {}

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

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        """Return a DataReader for the given metadata key and asset changes."""
        raise NotImplementedError()

    def get_namespace(self) -> str:
        """Return the namespace to use for external_id uniqueness."""
        return self.plugin_id

    def can_scan_uri(self, uri: str) -> bool:
        """Return whether this source can scan a root URI."""
        raise NotImplementedError()

    async def scan(self) -> ScanResult:
        """
        Scan the source, return a ScanResult with a status flag (that will be updated)
        and an async iterator that yields AssetScanResult objects with their assets and
        metadata to persist.
        """
        raise NotImplementedError()

    async def pull_scan_batch(
        self,
        *,
        cursor: str | None,
        batch_size: int,
    ) -> ScanBatch:
        """Pull the next scan batch by cursor.

        If `cursor` is None, a new scan session is started.
        """
        session_cursor, session = await self._get_or_create_scan_session(cursor)
        return await self._pull_from_session(
            sessions=self._scan_sessions,
            session_cursor=session_cursor,
            session=session,
            batch_size=batch_size,
        )

    async def scan_batches(
        self,
        *,
        batch_size: int,
    ) -> AsyncIterator[list[AssetScanResult]]:
        """Yield scan results in bounded batches.

        Default implementation adapts `pull_scan_batch()` for transition.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        cursor: str | None = None
        while True:
            response = await self.pull_scan_batch(cursor=cursor, batch_size=batch_size)
            if response.items:
                yield response.items
            if response.cursor is None:
                break
            cursor = response.cursor

    async def produce_batches(
        self,
        *,
        batch_size: int,
    ) -> AsyncIterator[SourceBatch]:
        """Produce source batches in workflow runtime format."""
        async for batch in self.scan_batches(batch_size=batch_size):
            items: list[SourceAssetPayload] = []
            for item in batch:
                actor_id = item.actor.id
                if actor_id is None:
                    raise ValueError("AssetScanResult.actor.id is required")
                items.append(
                    SourceAssetPayload(
                        asset=item.asset,
                        actor_id=int(actor_id),
                        metadata=list(item.metadata),
                    )
                )
            yield SourceBatch(items=items, status=OpStatus.IN_PROGRESS)

    def can_scan_asset(self, changes: MetadataChanges) -> int:
        """Return a score (>0) when this source can scan from the given asset state."""
        _ = changes
        return 0

    async def scan_from_asset(self, changes: MetadataChanges) -> ScanResult:
        """Recursively scan from an already discovered asset."""
        _ = changes
        raise NotImplementedError()

    async def pull_recursive_batch(
        self,
        *,
        seed: RecursionSeed | None,
        cursor: str | None,
        batch_size: int,
    ) -> ScanBatch:
        """Pull the next recursive scan batch by cursor.

        If `cursor` is None, `seed` is required to start a new recursive session.
        """
        session_cursor, session = await self._get_or_create_recursive_session(seed, cursor)
        return await self._pull_from_session(
            sessions=self._recursive_scan_sessions,
            session_cursor=session_cursor,
            session=session,
            batch_size=batch_size,
        )

    async def scan_from_asset_batches(
        self,
        changes: MetadataChanges,
        *,
        batch_size: int,
    ) -> AsyncIterator[list[AssetScanResult]]:
        """Yield recursive scan results in bounded batches.

        Default implementation adapts `pull_recursive_batch()` for transition.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        cursor: str | None = None
        seed = RecursionSeed(actor_id=int(self.actor.id or 0), changes=changes, depth=0)
        while True:
            response = await self.pull_recursive_batch(
                seed=seed if cursor is None else None,
                cursor=cursor,
                batch_size=batch_size,
            )
            if response.items:
                yield response.items
            if response.cursor is None:
                break
            cursor = response.cursor

    async def produce_recursive_batches(
        self,
        seed: RecursionSeed,
        *,
        batch_size: int,
    ) -> AsyncIterator[SourceBatch]:
        """Produce recursive source batches in workflow runtime format."""
        async for batch in self.scan_from_asset_batches(
            seed.changes,
            batch_size=batch_size,
        ):
            items: list[SourceAssetPayload] = []
            for item in batch:
                actor_id = item.actor.id
                if actor_id is None:
                    raise ValueError("AssetScanResult.actor.id is required")
                items.append(
                    SourceAssetPayload(
                        asset=item.asset,
                        actor_id=int(actor_id),
                        metadata=list(item.metadata),
                    )
                )
            yield SourceBatch(items=items, status=OpStatus.IN_PROGRESS)

    async def _get_or_create_scan_session(
        self,
        cursor: str | None,
    ) -> tuple[str, _ScanSession]:
        """Resolve an active scan session or lazily start one from `scan()`."""
        if cursor is not None:
            session = self._scan_sessions.get(cursor)
            if session is None:
                raise ValueError(f"Invalid scan cursor: {cursor}")
            return cursor, session

        scan_result = await self.scan()
        self._next_scan_cursor += 1
        next_cursor = f"scan:{self._next_scan_cursor}"
        session = _ScanSession(iterator=scan_result.iterator, status_ref=scan_result)
        self._scan_sessions[next_cursor] = session
        return next_cursor, session

    async def _get_or_create_recursive_session(
        self,
        seed: RecursionSeed | None,
        cursor: str | None,
    ) -> tuple[str, _ScanSession]:
        """Resolve or start a recursive scan session for pull-based recursion."""
        if cursor is not None:
            session = self._recursive_scan_sessions.get(cursor)
            if session is None:
                raise ValueError(f"Invalid recursive scan cursor: {cursor}")
            return cursor, session

        if seed is None:
            raise ValueError("seed is required when cursor is None for recursive pull")
        scan_result = await self.scan_from_asset(seed.changes)
        self._next_recursive_cursor += 1
        next_cursor = f"recurse:{self._next_recursive_cursor}"
        session = _ScanSession(iterator=scan_result.iterator, status_ref=scan_result)
        self._recursive_scan_sessions[next_cursor] = session
        return next_cursor, session

    async def _pull_from_session(
        self,
        *,
        sessions: dict[str, _ScanSession],
        session_cursor: str,
        session: _ScanSession,
        batch_size: int,
    ) -> ScanBatch:
        """Read up to `batch_size` items from a cursor session and return next cursor state."""
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        items: list[AssetScanResult] = []
        exhausted = False
        while len(items) < batch_size:
            try:
                item = await anext(session.iterator)
            except StopAsyncIteration:
                exhausted = True
                break
            items.append(item)

        if exhausted:
            sessions.pop(session_cursor, None)
            return ScanBatch(
                items=items,
                cursor=None,
                status=session.status_ref.status,
                ignored=session.status_ref.ignored,
            )

        return ScanBatch(
            items=items,
            cursor=session_cursor,
            status=OpStatus.IN_PROGRESS,
            ignored=0,
        )


async def make_source_instance(source_record: Actor) -> SourcePlugin:
    return cast(SourcePlugin, await get_actor_instance(source_record))
