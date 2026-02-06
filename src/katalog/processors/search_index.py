from __future__ import annotations

from typing import FrozenSet

from katalog.constants.metadata import (
    ASSET_CANONICAL_URI,
    ASSET_EXTERNAL_ID,
    ASSET_NAMESPACE,
    ASSET_SEARCH_DOC,
    METADATA_REGISTRY,
    MetadataKey,
    MetadataType,
    get_metadata_def_by_key,
    get_metadata_id,
)
from katalog.models import Asset, Metadata, MetadataChanges
from katalog.processors.base import Processor, ProcessorResult


class SearchIndexProcessor(Processor):
    plugin_id = "katalog.processors.search_index.SearchIndexProcessor"
    execution_mode = "cpu"

    def __init__(self, actor, **config):
        super().__init__(actor, **config)
        self._searchable_keys = self._resolve_searchable_keys()

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return frozenset(MetadataKey(key) for key in self._searchable_keys)

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return frozenset({ASSET_SEARCH_DOC})

    def should_run(self, asset: Asset, changes: MetadataChanges) -> bool:
        changed = changes.changed_keys()
        for key in self.dependencies:
            if key in changed:
                return True
        return False

    async def run(self, asset: Asset, changes: MetadataChanges) -> ProcessorResult:
        parts: list[str] = []
        if str(ASSET_NAMESPACE) in self._searchable_keys and asset.namespace:
            parts.append(str(asset.namespace))
        if str(ASSET_EXTERNAL_ID) in self._searchable_keys and asset.external_id:
            parts.append(str(asset.external_id))
        if str(ASSET_CANONICAL_URI) in self._searchable_keys and asset.canonical_uri:
            parts.append(str(asset.canonical_uri))

        current = changes.current()
        for key in self._searchable_keys:
            if key in {str(ASSET_NAMESPACE), str(ASSET_EXTERNAL_ID), str(ASSET_CANONICAL_URI)}:
                continue
            entries = current.get(key) or []
            for entry in entries:
                value = entry.value
                if value is None:
                    continue
                if hasattr(value, "isoformat"):
                    parts.append(value.isoformat())
                else:
                    parts.append(str(value))

        doc = " ".join(parts)
        metadata_key_id = get_metadata_id(ASSET_SEARCH_DOC)
        definition = get_metadata_def_by_key(ASSET_SEARCH_DOC)
        metadata = Metadata(
            metadata_key_id=metadata_key_id,
            value_type=definition.value_type,
            actor_id=self.actor.id,
            removed=False,
        )
        metadata.set_value(doc)
        return ProcessorResult(metadata=[metadata])

    def _resolve_searchable_keys(self) -> set[str]:
        keys: set[str] = set()
        for key, definition in METADATA_REGISTRY.items():
            if key == ASSET_SEARCH_DOC:
                continue
            if definition.searchable is not None:
                if definition.searchable:
                    keys.add(str(key))
                continue
            if definition.value_type in {MetadataType.STRING, MetadataType.JSON}:
                keys.add(str(key))
        return keys
