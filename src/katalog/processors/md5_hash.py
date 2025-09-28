import hashlib
from katalog.processors.base import Processor
from katalog.models import FileRecord


class MD5HashProcessor(Processor):
    dependencies = frozenset()  # No dependencies, runs on any record
    outputs = frozenset({"checksum_md5"})

    def should_run(self, record: FileRecord, changes: set[str] | None) -> bool:
        # If the SourceClient provides a hash, don't add this processor to the list, as it would not be
        # able to tell that the hash is already present.
        if record.checksum_md5 is None and record.data:
            return True
        return prev_cache != self.cache_key(record)

    async def run(self, record: FileRecord, changes: set[str] | None) -> FileRecord:
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
