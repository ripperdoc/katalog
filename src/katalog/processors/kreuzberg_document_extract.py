from __future__ import annotations

from pathlib import Path
from typing import Any, FrozenSet

from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import (
    DATA_FILE_READER,
    DATA_KEY,
    DOC_CHARS,
    DOC_CHUNK_COUNT,
    DOC_CHUNK_TEXT,
    DOC_LANG,
    DOC_PAGES,
    DOC_TEXT,
    DOC_WORDS,
    FILE_SIZE,
    FILE_TYPE,
    TIME_MODIFIED,
    MetadataKey,
)
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult


class KreuzbergDocumentExtractProcessor(Processor):
    plugin_id = "katalog.processors.kreuzberg_document_extract.KreuzbergDocumentExtractProcessor"
    title = "Kreuzberg document extract"
    description = "Extract text, metadata, chunks and optional embeddings using kreuzberg."
    execution_mode = "cpu"
    _dependencies = frozenset({DATA_KEY, FILE_SIZE, FILE_TYPE, TIME_MODIFIED})
    _outputs = frozenset(
        {
            DOC_TEXT,
            DOC_LANG,
            DOC_CHARS,
            DOC_WORDS,
            DOC_PAGES,
            DOC_CHUNK_COUNT,
            DOC_CHUNK_TEXT,
        }
    )

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        enable_chunking: bool = Field(
            default=True, description="Generate chunked text output."
        )
        enable_embeddings: bool = Field(
            default=False,
            description="Generate chunk embeddings (requires embedding model download).",
        )
        embedding_model: str = Field(
            default="sentence-transformers/all-MiniLM-L6-v2",
            description="Embedding model preset name for kreuzberg.",
        )
        embedding_batch_size: int = Field(default=32, gt=0)
        embedding_normalize: bool = Field(default=True)

    config_model = ConfigModel

    def __init__(self, actor, **config):
        self.config = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return self._outputs

    async def is_ready(self) -> tuple[bool, str | None]:
        try:
            import kreuzberg  # noqa: F401
        except Exception as exc:  # noqa: BLE001
            return False, f"kreuzberg import failed: {exc}"
        return True, None

    def should_run(self, changes: MetadataChanges) -> bool:
        mime_type = self._resolve_mime_type(changes)
        if mime_type and not _is_supported_mime(mime_type):
            return False
        changed_keys = changes.changed_keys()
        if DATA_KEY in changed_keys or FILE_SIZE in changed_keys:
            return True
        if FILE_TYPE in changed_keys or TIME_MODIFIED in changed_keys:
            return True
        current = changes.current()
        for key in self.outputs:
            if key not in current:
                return True
        return False

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        from kreuzberg import (
            ChunkingConfig,
            EmbeddingConfig,
            EmbeddingModelType,
            ExtractionConfig,
            extract_bytes,
            extract_file,
        )

        asset = changes.asset
        if asset is None:
            return ProcessorResult(
                status=OpStatus.ERROR, message="MetadataChanges.asset is missing"
            )
        reader = await asset.get_data_reader(DATA_FILE_READER, changes)
        if reader is None:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Asset does not have a data accessor"
            )

        mime_type = self._resolve_mime_type(changes)
        if mime_type and not _is_supported_mime(mime_type):
            return ProcessorResult(
                status=OpStatus.SKIPPED,
                message=f"Unsupported mime type for kreuzberg: {mime_type}",
            )
        extraction_config = self._build_extraction_config(
            ChunkingConfig=ChunkingConfig,
            EmbeddingConfig=EmbeddingConfig,
            EmbeddingModelType=EmbeddingModelType,
            ExtractionConfig=ExtractionConfig,
        )
        try:
            if getattr(reader, "path", None):
                result = await extract_file(
                    Path(reader.path),
                    mime_type=mime_type,
                    config=extraction_config,
                )
            else:
                data = await reader.read()
                if not data:
                    return ProcessorResult(
                        status=OpStatus.SKIPPED,
                        message="Asset data reader returned empty content",
                    )
                result = await extract_bytes(
                    data, mime_type=mime_type, config=extraction_config
                )
        except Exception as exc:  # noqa: BLE001
            if "ValidationError" in str(exc):
                return ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message=f"Kreuzberg validation failed: {exc}",
                )
            raise

        metadata = self._build_metadata_payload(result)
        return ProcessorResult(metadata=metadata)

    def _build_extraction_config(
        self,
        *,
        ChunkingConfig,
        EmbeddingConfig,
        EmbeddingModelType,
        ExtractionConfig,
    ):
        chunking_config = None
        if self.config.enable_chunking:
            embedding_config = None
            if self.config.enable_embeddings:
                embedding_config = EmbeddingConfig(
                    model=EmbeddingModelType.preset(self.config.embedding_model),
                    batch_size=self.config.embedding_batch_size,
                    normalize=self.config.embedding_normalize,
                )
            # Use Kreuzberg's default chunking strategy/limits.
            chunking_config = ChunkingConfig(embedding=embedding_config)
        return ExtractionConfig(chunking=chunking_config)

    def _build_metadata_payload(self, extraction_result) -> list:
        payload: list = []
        text_content = extraction_result.content
        if isinstance(text_content, str) and text_content:
            payload.append(make_metadata(DOC_TEXT, text_content, self.actor.id))

        detected_lang = extraction_result.get_detected_language()
        if isinstance(detected_lang, str) and detected_lang:
            payload.append(make_metadata(DOC_LANG, detected_lang, self.actor.id))

        raw_meta = extraction_result.metadata
        if isinstance(raw_meta, dict):
            chars = raw_meta.get("character_count")
            words = raw_meta.get("word_count")
            pages = raw_meta.get("page_count")
            if isinstance(chars, int):
                payload.append(make_metadata(DOC_CHARS, chars, self.actor.id))
            if isinstance(words, int):
                payload.append(make_metadata(DOC_WORDS, words, self.actor.id))
            if isinstance(pages, int):
                payload.append(make_metadata(DOC_PAGES, pages, self.actor.id))

        page_count = extraction_result.get_page_count()
        if isinstance(page_count, int) and page_count > 0:
            payload.append(make_metadata(DOC_PAGES, page_count, self.actor.id))

        chunk_count = extraction_result.get_chunk_count()
        chunks = extraction_result.chunks or []
        chunk_texts = self._collect_chunk_texts(chunks)
        if chunk_texts:
            payload.append(
                make_metadata(DOC_CHUNK_COUNT, len(chunk_texts), self.actor.id)
            )
            for chunk_text in chunk_texts:
                payload.append(make_metadata(DOC_CHUNK_TEXT, chunk_text, self.actor.id))
        elif isinstance(chunk_count, int):
            payload.append(make_metadata(DOC_CHUNK_COUNT, chunk_count, self.actor.id))
        return payload

    def _collect_chunk_texts(self, chunks: list[Any]) -> list[str]:
        values: list[str] = []
        for chunk in chunks:
            chunk_text = None
            for attr in ("text", "content"):
                candidate = getattr(chunk, attr, None)
                if isinstance(candidate, str) and candidate.strip():
                    chunk_text = candidate.strip()
                    break
            if chunk_text:
                values.append(chunk_text)
        return values

    @staticmethod
    def _resolve_mime_type(changes: MetadataChanges) -> str | None:
        entries = changes.current().get(FILE_TYPE, [])
        for entry in entries:
            value = entry.value
            if isinstance(value, str) and value:
                return value
        return None


def _is_supported_mime(mime_type: str) -> bool:
    if mime_type.startswith("text/"):
        return True
    if mime_type.startswith("image/"):
        return True
    supported_exact = {
        "application/pdf",
        "application/msword",
        "application/rtf",
        "application/json",
        "application/xml",
        "application/vnd.oasis.opendocument.text",
    }
    if mime_type in supported_exact:
        return True
    supported_prefixes = (
        "application/vnd.openxmlformats-officedocument",
    )
    return any(mime_type.startswith(prefix) for prefix in supported_prefixes)

