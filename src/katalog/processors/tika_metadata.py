from katalog.processors.base import Processor
from katalog.models import FileRecord

# Run Tika Client https://pypi.org/project/tika-client/


class TikaMetadata(Processor):
    """Detects mime_type and other metadata about a file using Tika."""

    dependencies = frozenset()  # Any file change
    outputs = frozenset({"mime_type"})

    # def cache_key(self, record: FileRecord) -> str:
    #     md5 = record.md5 or ""
    #     return f"{md5}-v1"

    # def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
    #     return record.source == "downloads" and prev_cache != self.cache_key(record)

    # async def run(self, record: FileRecord) -> FileRecord:
    #     # TODO, some services report application/octet-stream but there is a better mime type to find
    #     # So we should probably re-check octet-stream
    #     # Reads the first 2048 bytes of a file
    #     if not record.data:
    #         raise ValueError("FileRecord does not have a data accessor")
    #     m = magic.Magic(mime=True)
    #     buf = await record.data.read(0, 2048, no_cache=True)
    #     mt = m.from_buffer(buf)
    #     return record.model_copy(update={"mime_type": mt})
