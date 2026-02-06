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
        # TODO, some services report application/octet-stream but there is probably a better mime type to find
        # Is there a logic where we can check for that, without having to recheck every time?
        if FILE_TYPE not in changes.current():
            return True
        return file_data_changed(self, asset, changes)

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
