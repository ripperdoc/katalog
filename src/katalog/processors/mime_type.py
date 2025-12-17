from __future__ import annotations

import magic

from katalog.metadata import MIME_TYPE
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    file_data_changed,
    file_data_change_dependencies,
)
from katalog.models import Asset, make_metadata

# NOTE, useful info about magic detection and licensing:
# https://github.com/withzombies/tika-magic
# Run Tika Client https://pypi.org/project/tika-client/


class MimeTypeProcessor(Processor):
    PLUGIN_ID = "dev.katalog.processor.mime_type"
    dependencies = file_data_change_dependencies
    outputs = frozenset({MIME_TYPE})

    def should_run(
        self,
        asset: Asset,
        changes: set[str] | None,
    ) -> bool:
        # TODO, some services report application/octet-stream but there is probably a better mime type to find
        # Is there a logic where we can check for that, without having to recheck every time?
        return file_data_changed(self, asset, changes)

    async def run(self, asset: Asset, changes: set[str] | None) -> ProcessorResult:
        # So we should probably re-check octet-stream
        # Reads the first 2048 bytes of a file
        if not asset.data:
            raise ValueError("AssetRecord does not have a data accessor")
        m = magic.Magic(mime=True)
        buf = await asset.data.read(0, 2048, no_cache=True)
        mt = m.from_buffer(buf)
        provider_id = getattr(self, "provider_id", asset.provider_id)
        return ProcessorResult(metadata=[make_metadata(provider_id, MIME_TYPE, mt)])
