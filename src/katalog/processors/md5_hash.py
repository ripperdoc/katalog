import hashlib
from katalog.processors.base import Processor
from katalog.models import FileRecord


class MD5HashProcessor(Processor):
    dependencies = frozenset()  # No dependencies, runs on any record
    outputs = frozenset({"checksum_md5"})

    def cache_key(self, record: FileRecord) -> str:
        size = record.size_bytes or 0
        mtime_str = record.mtime.isoformat() if record.mtime else ""
        return f"{size}-{mtime_str}-v1"

    def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
        # Only run if md5 is missing or cache key changed
        if record.checksum_md5 is None and record.data:
            return True
        return prev_cache != self.cache_key(record)

    async def run(self, record: FileRecord) -> FileRecord:
        d = record.data
        if d is None:
            raise ValueError("FileRecord does not have a data accessor")
        hash_md5 = hashlib.md5()

        offset = 0
        chunk_size = 8192

        while True:
            chunk = await d.read(offset, chunk_size)
            if not chunk:
                break
            hash_md5.update(chunk)
            offset += len(chunk)
        record.checksum_md5 = hash_md5.hexdigest()
        return record
