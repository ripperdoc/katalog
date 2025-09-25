from katalog.processors.base import Processor
from katalog.models import FileRecord


class ArchiveProcess(Processor):
    """Checks if a file is an archive and if so, scans it to emit "virtual" file records."""

    dependencies = frozenset({"mime_type"})
    outputs = frozenset({"_files_"})

    # def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
    #     return record.source_id == "downloads" and prev_cache != self.cache_key(record)

    # async def run(self, record: FileRecord) -> FileRecord:
    #     return await super().run(record)
