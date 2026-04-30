from __future__ import annotations

from typing import Any, FrozenSet

from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import (
    DOC_CHUNK_TEXT,
    DOC_TEXT,
    FILE_DESCRIPTION,
    FILE_NAME,
    FILE_TITLE,
    INTERNAL_VECTOR_REINDEX,
    MetadataKey,
    VECTOR_INDEXED_COUNT,
)
from katalog.db.vectors import get_vector_repo
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult
from katalog.vectors.embedding import DEFAULT_EMBEDDING_MODEL, embed_text_kreuzberg


class KreuzbergVectorIndexProcessor(Processor):
    plugin_id = "katalog.processors.vector_index.KreuzbergVectorIndexProcessor"
    title = "Kreuzberg vector index"
    description = "Index selected string metadata values via Kreuzberg embeddings into sqlite-vec."
    execution_mode = "cpu"

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        dimension: int = Field(default=64, gt=0)
        embedding_model: str = Field(default=DEFAULT_EMBEDDING_MODEL)
        embedding_backend: str = Field(default="preset")
        embedding_batch_size: int = Field(default=32, gt=0)
        embedding_normalize: bool = Field(default=True)
        metadata_keys: list[str] = Field(
            default_factory=lambda: [
                str(DOC_CHUNK_TEXT),
                str(DOC_TEXT),
                str(FILE_TITLE),
                str(FILE_NAME),
                str(FILE_DESCRIPTION),
            ]
        )
        min_text_length: int = Field(default=3, ge=0)
        max_points: int = Field(default=500, gt=0)

    config_model = ConfigModel

    def __init__(self, actor, **config):
        self.config = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self._dependencies = frozenset(
            MetadataKey(key) for key in self.config.metadata_keys
        )

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return frozenset({INTERNAL_VECTOR_REINDEX, VECTOR_INDEXED_COUNT})

    async def is_ready(self) -> tuple[bool, str | None]:
        repo = get_vector_repo()
        ready, reason = await repo.is_ready()
        if not ready:
            return ready, reason
        try:
            await embed_text_kreuzberg(
                "ready",
                model=self.config.embedding_model,
                backend=str(self.config.embedding_backend),
                normalize=bool(self.config.embedding_normalize),
                batch_size=int(self.config.embedding_batch_size),
                dim=self.config.dimension,
            )
        except Exception as exc:  # noqa: BLE001
            return False, f"embedding model is not ready: {exc}"
        return True, None

    def should_run(self, changes: MetadataChanges) -> bool:
        changed = changes.changed_keys()
        if any(key in changed for key in self.dependencies):
            return True
        current = changes.current()
        return VECTOR_INDEXED_COUNT not in current

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        asset = changes.asset
        if asset is None or asset.id is None:
            return ProcessorResult(
                status=OpStatus.ERROR, message="MetadataChanges.asset is missing"
            )
        if self.actor.id is None:
            return ProcessorResult(status=OpStatus.ERROR, message="Actor id is missing")

        return ProcessorResult(
            metadata=[
                make_metadata(INTERNAL_VECTOR_REINDEX, 1, self.actor.id),
            ],
            message="Queued vector reindex",
        )
