import hashlib
from processors.base import Processor
from models import FileRecord

class MD5HashProcessor(Processor):
    dependencies = frozenset() # No dependencies, runs on any record
    outputs      = frozenset({"md5"})

    def cache_key(self, record: FileRecord) -> str:
        size = record.size or 0
        mtime_str = record.mtime.isoformat() if record.mtime else ""
        return f"{size}-{mtime_str}-v1"

    def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
        # Only run if md5 is missing or cache key changed
        if record.md5 is None:
            return True
        return prev_cache != self.cache_key(record)

    def run(self, record: FileRecord) -> FileRecord:
        hash_md5 = hashlib.md5()
        with open(record.path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return record.model_copy(update={"md5": hash_md5.hexdigest()})
