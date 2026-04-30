from __future__ import annotations

from typing import FrozenSet

from katalog.constants.metadata import (
    ASSET_SEARCH_DOC,
    INTERNAL_FTS_REINDEX,
    METADATA_REGISTRY,
    MetadataKey,
    MetadataType,
)
from katalog.db.fts import get_fts_repo
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult


class FullTextSearchIndexProcessor(Processor):
    plugin_id = "katalog.processors.search_index.FullTextSearchIndexProcessor"
    execution_mode = "io"  # Not really io, but it gives better DB performance

    def __init__(self, actor, **config):
        super().__init__(actor, **config)
        self._searchable_keys = self._resolve_searchable_keys()

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return frozenset(MetadataKey(key) for key in self._searchable_keys)

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return frozenset({INTERNAL_FTS_REINDEX})

    async def is_ready(self) -> tuple[bool, str | None]:
        repo = get_fts_repo()
        return await repo.is_ready()

    def should_run(self, changes: MetadataChanges) -> bool:
        changed = changes.changed_keys()
        for key in self.dependencies:
            if key in changed:
                return True
        return False

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        asset = changes.asset
        if asset is None:
            return ProcessorResult(
                status=OpStatus.ERROR, message="MetadataChanges.asset is missing"
            )
        if asset.id is None:
            return ProcessorResult(status=OpStatus.ERROR, message="Asset id is missing")
        if self.actor.id is None:
            return ProcessorResult(status=OpStatus.ERROR, message="Actor id is missing")
        return ProcessorResult(
            metadata=[
                make_metadata(
                    INTERNAL_FTS_REINDEX,
                    1,
                    actor_id=int(self.actor.id),
                )
            ],
            message="Queued FTS reindex",
        )

    def _resolve_searchable_keys(self) -> set[str]:
        keys: set[str] = set()
        for key, definition in METADATA_REGISTRY.items():
            key_str = str(key)
            if key == ASSET_SEARCH_DOC or key_str.startswith("asset/"):
                continue
            if definition.searchable is not None:
                if definition.searchable:
                    keys.add(key_str)
                continue
            if definition.value_type in {MetadataType.STRING, MetadataType.JSON}:
                keys.add(key_str)
        return keys
