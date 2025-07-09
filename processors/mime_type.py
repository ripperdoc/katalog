import magic
from processors.base import Processor
from models import FileRecord

class MimeTypeProcessor(Processor):
    dependencies = frozenset({"size", "mtime"})
    outputs      = frozenset({"mime_type"})

    def cache_key(self, record: FileRecord) -> str:
        # include file size and mtime (if any) so changes trigger a rerun
        size = record.size or 0
        mtime_str = record.mtime.isoformat() if record.mtime else ""
        return f"{size}-{mtime_str}-v1"

    def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
        return prev_cache != self.cache_key(record)

    def run(self, record: FileRecord) -> FileRecord:
        m = magic.Magic(mime=True)
        mt = m.from_file(record.path)
        return record.model_copy(update={"mime_type": mt})
