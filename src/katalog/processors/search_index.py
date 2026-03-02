from __future__ import annotations

from typing import FrozenSet

from katalog.constants.metadata import (
    ASSET_SEARCH_DOC,
    METADATA_REGISTRY,
    MetadataKey,
    MetadataType,
    get_metadata_id,
)
from katalog.db.fts import FtsPoint, get_fts_repo
from katalog.models import MetadataChanges, OpStatus
from katalog.processors.base import Processor, ProcessorResult


class FullTextSearchIndexProcessor(Processor):
    plugin_id = "katalog.processors.search_index.FullTextSearchIndexProcessor"
    execution_mode = "cpu"

    def __init__(self, actor, **config):
        super().__init__(actor, **config)
        self._searchable_keys = self._resolve_searchable_keys()

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return frozenset(MetadataKey(key) for key in self._searchable_keys)

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return frozenset()

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

        current = changes.current()
        points: list[FtsPoint] = []
        indexed_key_ids: list[int] = []
        for key in self._searchable_keys:
            metadata_key_id = int(get_metadata_id(MetadataKey(key)))
            indexed_key_ids.append(metadata_key_id)
            entries = current.get(key) or []
            for entry in entries:
                if entry.id is None:
                    continue
                value = entry.value
                if value is None:
                    continue
                text = value.isoformat() if hasattr(value, "isoformat") else str(value)
                cleaned = text.strip()
                if not cleaned:
                    continue
                points.append(FtsPoint(metadata_id=int(entry.id), text=cleaned))

        repo = get_fts_repo()
        indexed = await repo.upsert_asset_points(
            asset_id=int(asset.id),
            actor_id=int(self.actor.id),
            metadata_key_ids=indexed_key_ids,
            points=points,
        )
        return ProcessorResult(message=f"Indexed {indexed} metadata values")

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
