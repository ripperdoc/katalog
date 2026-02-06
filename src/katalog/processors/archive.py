from __future__ import annotations


from katalog.processors.base import Processor
from katalog.constants.metadata import FILE_TYPE


class ArchiveProcess(Processor):
    """Checks if a file is an archive and if so, scans it to emit "virtual" file records."""

    execution_mode = "cpu"
    _dependencies = frozenset({FILE_TYPE})
    _outputs = frozenset()  # TODO

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def outputs(self):
        return self._outputs

    # def should_run(self, record: Asset, prev_cache: str | None) -> bool:
    #     return record.actor_id == "downloads" and prev_cache != self.cache_key(record)

    # async def run(self, record: Asset) -> Asset:
    #     return await super().run(record)
