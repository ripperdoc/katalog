from __future__ import annotations

import magic

from katalog.metadata import FILE_TYPE
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    file_data_changed,
    file_data_change_dependencies,
)
from katalog.models import Asset, MetadataChangeSet, make_metadata
from katalog.models import OpStatus

# NOTE, useful info about magic detection and licensing:
# https://github.com/withzombies/tika-magic
# Run Tika Client https://pypi.org/project/tika-client/


class MimeTypeProcessor(Processor):
    plugin_id = "katalog.processors.mime_type.MimeTypeProcessor"
    title = "MIME type"
    description = "Detect MIME type from file bytes using libmagic."
    dependencies = file_data_change_dependencies
    outputs = frozenset({FILE_TYPE})

    def should_run(self, asset: Asset, change_set: MetadataChangeSet) -> bool:
        # TODO, some services report application/octet-stream but there is probably a better mime type to find
        # Is there a logic where we can check for that, without having to recheck every time?
        return file_data_changed(self, asset, change_set)

    async def run(self, asset: Asset, change_set: MetadataChangeSet) -> ProcessorResult:
        # So we should probably re-check octet-stream
        # Reads the first 2048 bytes of a file
        if not asset.data:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Asset does not have a data accessor"
            )
        m = magic.Magic(mime=True)
        buf = await asset.data.read(0, 2048, no_cache=True)
        mt = m.from_buffer(buf)
        return ProcessorResult(
            metadata=[make_metadata(FILE_TYPE, mt, self.provider.id)]
        )
