from __future__ import annotations

from typing import Any

from katalog.processors.base import Processor
from katalog.metadata import FILE_TYPE


class ArchiveProcess(Processor):
    """Checks if a file is an archive and if so, scans it to emit "virtual" file records."""

    dependencies = frozenset({FILE_TYPE})
    outputs = frozenset({})  # TODO

    # def should_run(self, record: AssetRecord, prev_cache: str | None) -> bool:
    #     return record.provider_id == "downloads" and prev_cache != self.cache_key(record)

    # async def run(self, record: AssetRecord) -> AssetRecord:
    #     return await super().run(record)
