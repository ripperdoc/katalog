from __future__ import annotations

from typing import Any

from katalog.db import Database
from katalog.processors.base import Processor
from katalog.models import MIME_TYPE


class ArchiveProcess(Processor):
    """Checks if a file is an archive and if so, scans it to emit "virtual" file records."""

    dependencies = frozenset({MIME_TYPE})
    outputs = frozenset({})  # TODO

    def __init__(self, *, database: Database | None = None, **_: Any) -> None:
        self.database = database

    # def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
    #     return record.source_id == "downloads" and prev_cache != self.cache_key(record)

    # async def run(self, record: FileRecord) -> FileRecord:
    #     return await super().run(record)
