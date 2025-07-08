import magic
from processors.base import Processor
from models import FileRecord

class MimeTypeProcessor:
    name = "mime_type"
    # depends on file size and mtime to detect changes
    dependencies: list[str] = ["size", "mtime"]
    # produces the mime_type field
    outputs: list[str] = ["mime_type"]

    def cache_key(self, record: FileRecord) -> str:
        # include file size and mtime (if any) so changes trigger a rerun
        size = record.size or 0
        mtime_str = record.mtime.isoformat() if record.mtime else ""
        return f"{size}-{mtime_str}-v1"

    def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
        return prev_cache != self.cache_key(record)

    def run(self, record: FileRecord) -> dict:
        m = magic.Magic(mime=True)
        mt = m.from_file(record.path)
        return {"mime_type": mt}
