from __future__ import annotations

import magic
from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import FILE_TYPE, DATA_FILE_READER
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    file_data_changed,
    file_data_change_dependencies,
)
from katalog.models import Asset, MetadataChanges, make_metadata
from katalog.models import OpStatus

# NOTE, useful info about magic detection and licensing:
# https://github.com/withzombies/tika-magic
# Run Tika Client https://pypi.org/project/tika-client/


class MimeTypeProcessor(Processor):
    plugin_id = "katalog.processors.mime_type.MimeTypeProcessor"
    title = "MIME type"
    description = "Detect MIME type from file bytes using libmagic."
    execution_mode = "cpu"
    _dependencies = file_data_change_dependencies
    _outputs = frozenset({FILE_TYPE})

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        probe_length: int = Field(
            default=2048,
            gt=0,
            description="Number of bytes to inspect from start of file",
        )
        run_on_octet_stream: bool = Field(
            default=False,
            description="If true, re-check when all existing MIME types are application/octet-stream.",
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
        if file_data_changed(self, asset, changes):
            return True

        if changes.entries_for_key(FILE_TYPE, self.actor.id):
            return False

        current_types = [
            value
            for value in changes.values_for_key(FILE_TYPE)
            if isinstance(value, str) and value.strip()
        ]
        if not current_types:
            return True
        if not self.config.run_on_octet_stream:
            return False
        return all(value == "application/octet-stream" for value in current_types)

    async def run(self, asset: Asset, changes: MetadataChanges) -> ProcessorResult:
        # So we should probably re-check octet-stream
        # Reads the first 2048 bytes of a file
        reader = await asset.get_data_reader(DATA_FILE_READER, changes)
        if not reader:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Asset does not have a data accessor"
            )
        m = magic.Magic(mime=True)
        buf = await reader.read(0, self.config.probe_length, no_cache=True)
        mt = m.from_buffer(buf)
        return ProcessorResult(metadata=[make_metadata(FILE_TYPE, mt, self.actor.id)])
