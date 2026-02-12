from __future__ import annotations

from typing import FrozenSet

from pydantic import BaseModel, ConfigDict, Field, field_validator

from katalog.constants.metadata import FILE_PATH, MetadataKey
from katalog.models import Asset, MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult
from katalog.processors.path_template import (
    CompiledTemplate,
    TemplateError,
    compile_template,
    evaluate_template,
)


class PathRenamerProcessor(Processor):
    """Compute and persist a new file/path value from a configured metadata template."""

    plugin_id = "katalog.processors.path_renamer.PathRenamerProcessor"
    title = "Path renamer"
    description = "Build file/path from a metadata template."
    execution_mode = "cpu"
    _outputs = frozenset({FILE_PATH})

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        template: str = Field(
            min_length=1,
            description="Path template with metadata placeholders, e.g. {time.modified:year}/{file.filename}",
        )

        @field_validator("template")
        @classmethod
        def _validate_template(cls, value: str) -> str:
            compile_template(value)
            return value

    config_model = ConfigModel

    def __init__(self, actor, **config):
        self.config = self.config_model.model_validate(config or {})
        # Compile eagerly to validate format + metadata keys at actor init time.
        self._compiled_template: CompiledTemplate = compile_template(
            self.config.template
        )
        self._dependencies = frozenset(self._compiled_template.keys)
        super().__init__(actor, **config)

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return self._outputs

    def should_run(self, asset: Asset, changes: MetadataChanges) -> bool:
        if not changes.entries_for_key(FILE_PATH, self.actor.id):
            return True
        return changes.changed_since_actor(
            self.dependencies,
            actor_id=int(self.actor.id or 0),
            actor_outputs=set(self.outputs),
        )

    async def run(self, asset: Asset, changes: MetadataChanges) -> ProcessorResult:
        current = changes.current()

        def _resolve(key: MetadataKey) -> object:
            return current.get(key, [])

        try:
            new_path = evaluate_template(self._compiled_template, resolver=_resolve)
        except TemplateError as exc:
            return ProcessorResult(status=OpStatus.SKIPPED, message=str(exc))

        if not new_path.strip():
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Template resolved to empty path"
            )

        return ProcessorResult(
            metadata=[make_metadata(FILE_PATH, new_path, self.actor.id)]
        )
