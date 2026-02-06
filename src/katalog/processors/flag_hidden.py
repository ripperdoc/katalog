from __future__ import annotations

from typing import Iterable

from katalog.constants.metadata import FILE_NAME, FILE_PATH, FILE_TYPE, FLAG_HIDDEN
from katalog.models import Asset, MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult
from katalog.utils.hidden import should_hide_path


class HiddenFlagProcessor(Processor):
    """Flag assets that should be hidden from analysis based on path heuristics.

    Clears the hidden flag when the asset no longer matches the rules.
    """

    plugin_id = "katalog.processors.flag_hidden.HiddenFlagProcessor"
    title = "Hidden files"
    description = (
        "Flag files and folders that are likely hidden or irrelevant; clears when no longer hidden."
    )
    execution_mode = "cpu"
    _dependencies = frozenset({FILE_PATH, FILE_NAME, FILE_TYPE})
    _outputs = frozenset({FLAG_HIDDEN})

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def outputs(self):
        return self._outputs

    def should_run(self, asset: Asset, changes: MetadataChanges) -> bool:
        changed_keys = changes.changed_keys()
        if FLAG_HIDDEN not in changes.current():
            return True
        return bool(changed_keys & self._dependencies)

    async def run(self, asset: Asset, changes: MetadataChanges) -> ProcessorResult:
        paths = list(self._candidate_paths(changes))
        if not paths:
            return ProcessorResult(status=OpStatus.SKIPPED, message="No path found")

        mime_type = self._first_value(changes, FILE_TYPE)
        hidden = any(should_hide_path(path, mime_type) for path in paths)
        metadata = make_metadata(FLAG_HIDDEN, int(hidden), self.actor.id)
        return ProcessorResult(metadata=[metadata])

    def _candidate_paths(self, changes: MetadataChanges) -> Iterable[str]:
        for value in self._values_for_key(changes, FILE_PATH):
            yield value
        for value in self._values_for_key(changes, FILE_NAME):
            yield value

    def _values_for_key(
        self, changes: MetadataChanges, key
    ) -> Iterable[str]:
        for entry in changes.current().get(key, []):
            value = entry.value
            if isinstance(value, str) and value.strip():
                yield value

    def _first_value(self, changes: MetadataChanges, key) -> str | None:
        for entry in changes.current().get(key, []):
            value = entry.value
            if isinstance(value, str) and value.strip():
                return value
        return None
