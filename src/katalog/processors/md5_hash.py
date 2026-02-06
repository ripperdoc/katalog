from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

from katalog.constants.metadata import (
    DATA_FILE_READER,
    DATA_KEY,
    FILE_SIZE,
    HASH_MD5,
    TIME_MODIFIED,
)
from katalog.processors.base import Processor, ProcessorResult
from katalog.models import Asset, make_metadata, MetadataChanges, OpStatus
from pydantic import BaseModel, ConfigDict, Field


class MD5HashProcessor(Processor):
    plugin_id = "katalog.processors.md5_hash.MD5HashProcessor"
    title = "MD5 hash"
    description = "Compute md5 checksum for assets."
    execution_mode = "cpu"
    _dependencies = frozenset({DATA_KEY, FILE_SIZE, TIME_MODIFIED})
    _outputs = frozenset({HASH_MD5})

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        chunk_size: int = Field(
            default=8192,
            gt=0,
            description="Bytes per read when hashing (tunes IO behavior)",
        )

    config_model = ConfigModel

    def __init__(self, actor, **config):
        self.config = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def outputs(self):
        return self._outputs

    def should_run(self, asset: Asset, changes: MetadataChanges) -> bool:
        changed_keys = changes.changed_keys()
        if HASH_MD5 in changed_keys:
            return False
        if DATA_KEY in changed_keys:
            return True
        if FILE_SIZE in changed_keys or TIME_MODIFIED in changed_keys:
            return True
        if HASH_MD5 not in changes.current():
            return True
        return False

    async def run(self, asset: Asset, changes: MetadataChanges) -> ProcessorResult:
        reader = await asset.get_data_reader(DATA_FILE_READER, changes)
        if reader is None:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Asset does not have a data accessor"
            )

        # If the accessor exposes a local path, hash it in a thread to leverage GIL release.
        if hasattr(reader, "path") and reader.path is not None:
            digest = await asyncio.to_thread(
                _hash_file_path, Path(reader.path), self.config.chunk_size
            )
        else:
            digest = await _hash_stream_async(reader, self.config.chunk_size)

        return ProcessorResult(
            metadata=[make_metadata(HASH_MD5, digest, self.actor.id)]
        )


def _hash_file_path(path: Path, chunk_size: int) -> str:
    hash_md5 = hashlib.md5()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


async def _hash_stream_async(accessor, chunk_size: int) -> str:
    hash_md5 = hashlib.md5()
    offset = 0
    while True:
        chunk = await accessor.read(offset, chunk_size)
        if not chunk:
            break
        hash_md5.update(chunk)
        offset += len(chunk)

    return hash_md5.hexdigest()
