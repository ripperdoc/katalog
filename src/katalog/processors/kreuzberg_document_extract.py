from __future__ import annotations

from pathlib import Path
from typing import Any, FrozenSet, Mapping, Protocol, Sequence

from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import (
    ACCESS_LAST_MODIFIED_BY,
    FILE_CHILD_COUNT,
    ARCHIVE_FORMAT,
    ARCHIVE_UNCOMPRESSED_SIZE,
    AUDIO_ALBUM,
    AUDIO_ARTIST,
    AUDIO_GENRE,
    AUDIO_YEAR,
    DATA_FILE_READER,
    DATA_KEY,
    DOC_AUTHOR,
    DOC_CATEGORY,
    DOC_CHARS,
    DOC_CHUNK_COUNT,
    DOC_CHUNK_TEXT,
    ACCESS_CREATED_BY,
    DOC_KEYWORD,
    DOC_LANG,
    DOC_LINES,
    DOC_PAGES,
    DOC_SUMMARY,
    DOC_TEXT,
    DOC_WORDS,
    FILE_VERSION,
    FILE_DESCRIPTION,
    FILE_SIZE,
    FILE_TITLE,
    FILE_TYPE,
    IMAGE_APERTURE,
    IMAGE_CAMERA_MAKE,
    IMAGE_CAMERA_MODEL,
    IMAGE_FOCAL_LENGTH,
    IMAGE_GPS_LATITUDE,
    IMAGE_GPS_LONGITUDE,
    IMAGE_HEIGHT,
    IMAGE_ISO,
    IMAGE_ORIENTATION,
    IMAGE_WIDTH,
    PDF_ENCRYPTED,
    PDF_PRODUCER,
    PDF_VERSION,
    TIME_CREATED,
    TIME_MODIFIED,
    MetadataKey,
)
from katalog.models import MetadataChanges, OpStatus
from katalog.processors.base import Processor, ProcessorResult
from katalog.utils.utils import parse_datetime_utc
from kreuzberg import (
    ChunkingConfig,
    EmbeddingConfig,
    EmbeddingModelType,
    ExtractionConfig,
    LanguageDetectionConfig,
    ValidationError,
    detect_mime_type_from_bytes,
    extract_bytes,
    extract_file,
)


# Note: We lack proper typing from Kreuzberg, so we define some minimal protocols here to help with type checking and code clarity.
class Chunk(Protocol):
    content: str


class ExtractionResult(Protocol):
    content: str
    mime_type: str
    detected_languages: list[str] | None
    chunks: list[Chunk] | None
    output_format: str | None
    result_format: str | None

    def get_page_count(self) -> int: ...
    def get_chunk_count(self) -> int: ...
    def get_detected_language(self) -> str | None: ...
    def get_metadata_field(self, field_name: str) -> Any | None: ...


class KreuzbergDocumentExtractProcessor(Processor):
    plugin_id = "katalog.processors.kreuzberg_document_extract.KreuzbergDocumentExtractProcessor"
    title = "Kreuzberg document extract"
    description = (
        "Extract text, metadata, chunks and optional embeddings using kreuzberg."
    )
    execution_mode = "io"
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
            DOC_KEYWORD,
            FILE_TITLE,
            FILE_VERSION,
            FILE_DESCRIPTION,
            DOC_AUTHOR,
            DOC_CATEGORY,
            DOC_SUMMARY,
            DOC_LINES,
            ACCESS_CREATED_BY,
            TIME_CREATED,
            ACCESS_LAST_MODIFIED_BY,
            AUDIO_ARTIST,
            AUDIO_ALBUM,
            AUDIO_YEAR,
            AUDIO_GENRE,
            ARCHIVE_FORMAT,
            FILE_CHILD_COUNT,
            ARCHIVE_UNCOMPRESSED_SIZE,
            IMAGE_CAMERA_MAKE,
            IMAGE_CAMERA_MODEL,
            IMAGE_ORIENTATION,
            IMAGE_FOCAL_LENGTH,
            IMAGE_APERTURE,
            IMAGE_ISO,
            IMAGE_GPS_LATITUDE,
            IMAGE_GPS_LONGITUDE,
            IMAGE_WIDTH,
            IMAGE_HEIGHT,
            PDF_VERSION,
            PDF_PRODUCER,
            PDF_ENCRYPTED,
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
        self.extraction_config = self._build_extraction_config(self.config)
        super().__init__(actor, **config)

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return self._outputs

    async def is_ready(self) -> tuple[bool, str | None]:
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

        doc: ExtractionResult
        reader_path = reader.path
        try:
            if reader_path:
                doc = await extract_file(
                    Path(reader_path),
                    mime_type=mime_type,
                    config=self.extraction_config,
                )
            else:
                data = await reader.read()
                if not data:
                    return ProcessorResult(
                        status=OpStatus.SKIPPED,
                        message="Asset data reader returned empty content",
                    )
                inferred_mime_type = mime_type or detect_mime_type_from_bytes(data)
                if not inferred_mime_type:
                    return ProcessorResult(
                        status=OpStatus.SKIPPED,
                        message="Could not infer mime type for byte extraction",
                    )
                if not _is_supported_mime(inferred_mime_type):
                    return ProcessorResult(
                        status=OpStatus.SKIPPED,
                        message=f"Unsupported mime type for kreuzberg: {inferred_mime_type}",
                    )
                doc = await extract_bytes(
                    data,
                    mime_type=inferred_mime_type,
                    config=self.extraction_config,
                )
        except ValidationError as exc:
            return ProcessorResult(
                status=OpStatus.SKIPPED,
                message=f"Kreuzberg validation failed: {exc}",
            )

        return self._build_processor_result(doc)

    def _build_extraction_config(
        self,
        config: ConfigModel,
    ):
        chunking_config = None
        if config.enable_chunking:
            embedding_config = None
            if config.enable_embeddings:
                embedding_config = EmbeddingConfig(
                    model=EmbeddingModelType.preset(config.embedding_model),
                    batch_size=config.embedding_batch_size,
                    normalize=config.embedding_normalize,
                )
            # Use Kreuzberg's default chunking strategy/limits.
            chunking_config = ChunkingConfig(embedding=embedding_config)
        extraction = ExtractionConfig(
            chunking=chunking_config,
            language_detection=LanguageDetectionConfig(enabled=True),
        )

        # Use KeywordConfig
        return extraction

    def _build_processor_result(self, doc: ExtractionResult) -> ProcessorResult:
        result = ProcessorResult(actor_id=self.actor.id)

        format_type = _first_metadata_field(doc, "format_type")

        result.set_metadata(DOC_TEXT, _normalize_text(doc.content))

        languages = _unique_strings(
            [
                *_to_string_list(doc.detected_languages),
                *_to_string_list(doc.get_detected_language()),
                *_to_string_list(_first_metadata_field(doc, "language")),
            ]
        )
        result.set_metadata_list(DOC_LANG, languages)

        pages = doc.get_page_count()
        if pages <= 0:
            pages = (
                _to_int(
                    _first_metadata_field(
                        doc, "page_count", "slide_count", "sheet_count"
                    )
                )
                or 0
            )
        result.set_metadata(DOC_PAGES, pages if pages > 0 else None)

        result.set_metadata(FILE_TYPE, doc.mime_type)

        result.set_metadata(FILE_TITLE, _to_str(_first_metadata_field(doc, "title")))
        result.set_metadata(
            FILE_DESCRIPTION,
            _to_str(_first_metadata_field(doc, "subject", "description")),
        )
        result.set_metadata(
            DOC_SUMMARY,
            _to_str(_first_metadata_field(doc, "abstract_text", "summary")),
        )
        result.set_metadata(
            DOC_CATEGORY, _to_str(_first_metadata_field(doc, "category"))
        )
        result.set_metadata(
            FILE_VERSION,
            _to_str(_first_metadata_field(doc, "document_version")),
        )

        authors = _to_string_list(_first_metadata_field(doc, "authors"))
        byline = _to_str(_first_metadata_field(doc, "author"))
        if byline and byline not in authors:
            authors.append(byline)
        result.set_metadata_list(DOC_AUTHOR, authors)

        created_at = parse_datetime_utc(
            _to_str(_first_metadata_field(doc, "created_at", "creation_date")),
            strict=False,
        )
        modified_at = parse_datetime_utc(
            _to_str(_first_metadata_field(doc, "modified_at", "modification_date")),
            strict=False,
        )
        result.set_metadata(TIME_CREATED, created_at)
        result.set_metadata(TIME_MODIFIED, modified_at)

        result.set_metadata(
            ACCESS_CREATED_BY,
            _to_str(_first_metadata_field(doc, "created_by", "creator")),
        )
        result.set_metadata(
            ACCESS_LAST_MODIFIED_BY,
            _to_str(_first_metadata_field(doc, "modified_by")),
        )

        result.set_metadata(
            DOC_CHARS, _to_int(_first_metadata_field(doc, "character_count"))
        )
        result.set_metadata(
            DOC_WORDS, _to_int(_first_metadata_field(doc, "word_count"))
        )
        result.set_metadata(
            DOC_LINES, _to_int(_first_metadata_field(doc, "line_count"))
        )

        keywords = _unique_strings(
            [
                *_to_string_list(_first_metadata_field(doc, "keywords")),
                *_to_string_list(_first_metadata_field(doc, "tags")),
            ]
        )
        result.set_metadata_list(DOC_KEYWORD, keywords)

        chunk_texts = [_normalize_text(chunk.content) for chunk in (doc.chunks or [])]
        chunk_texts = [chunk_text for chunk_text in chunk_texts if chunk_text]
        chunk_count = doc.get_chunk_count()
        if chunk_count <= 0 and chunk_texts:
            chunk_count = len(chunk_texts)
        result.set_metadata(DOC_CHUNK_COUNT, chunk_count if chunk_count > 0 else None)
        result.set_metadata_list(DOC_CHUNK_TEXT, chunk_texts)

        result.set_metadata(
            AUDIO_ARTIST,
            _to_str(_first_metadata_field(doc, "artist", "audio_artist")),
        )
        result.set_metadata(
            AUDIO_ALBUM,
            _to_str(_first_metadata_field(doc, "album", "audio_album")),
        )
        result.set_metadata(
            AUDIO_YEAR,
            _to_int(_first_metadata_field(doc, "year", "audio_year")),
        )
        result.set_metadata(
            AUDIO_GENRE,
            _to_str(_first_metadata_field(doc, "genre", "audio_genre")),
        )

        if format_type == "archive":
            result.set_metadata(
                ARCHIVE_FORMAT, _to_str(_first_metadata_field(doc, "format"))
            )
            result.set_metadata(
                FILE_CHILD_COUNT,
                _to_int(_first_metadata_field(doc, "file_count")),
            )
            result.set_metadata(
                ARCHIVE_UNCOMPRESSED_SIZE,
                _to_int(_first_metadata_field(doc, "total_size")),
            )

        if format_type == "image":
            result.set_metadata(
                IMAGE_WIDTH, _to_int(_first_metadata_field(doc, "width"))
            )
            result.set_metadata(
                IMAGE_HEIGHT, _to_int(_first_metadata_field(doc, "height"))
            )

            exif = _to_mapping(_first_metadata_field(doc, "exif"))
            if exif:
                result.set_metadata(
                    IMAGE_CAMERA_MAKE,
                    _to_str(
                        _first_mapping_value(
                            exif,
                            "Make",
                            "camera_make",
                            "cameraMake",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_CAMERA_MODEL,
                    _to_str(
                        _first_mapping_value(
                            exif,
                            "Model",
                            "camera_model",
                            "cameraModel",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_ORIENTATION,
                    _to_int(_first_mapping_value(exif, "Orientation", "orientation")),
                )
                result.set_metadata(
                    IMAGE_FOCAL_LENGTH,
                    _to_float(
                        _first_mapping_value(
                            exif,
                            "FocalLength",
                            "focal_length",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_APERTURE,
                    _to_float(
                        _first_mapping_value(
                            exif,
                            "FNumber",
                            "ApertureValue",
                            "aperture",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_ISO,
                    _to_int(
                        _first_mapping_value(
                            exif,
                            "ISO",
                            "ISOSpeedRatings",
                            "PhotographicSensitivity",
                            "iso",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_GPS_LATITUDE,
                    _to_float(
                        _first_mapping_value(
                            exif,
                            "GPSLatitude",
                            "gps_latitude",
                        )
                    ),
                )
                result.set_metadata(
                    IMAGE_GPS_LONGITUDE,
                    _to_float(
                        _first_mapping_value(
                            exif,
                            "GPSLongitude",
                            "gps_longitude",
                        )
                    ),
                )

        # Full office properties are mapped to other metadata fields already
        # if format_type == "docx":
        #     core_props = _first_metadata_field(doc, "core_properties")
        #     app_props = _first_metadata_field(doc, "app_properties")
        #     custom_props = _first_metadata_field(doc, "custom_properties")

        # A bit too detailed for now
        # if format_type == "html":
        #     result.set_metadata(
        #         HTML_CANONICAL_URL,
        #         _to_str(_first_metadata_field(doc, "canonical_url", "canonical")),
        #     )
        #     result.set_metadata(
        #         HTML_BASE_HREF,
        #         _to_str(_first_metadata_field(doc, "base_href")),
        #     )
        #     result.set_metadata(
        #         HTML_TEXT_DIRECTION,
        #         _to_str(_first_metadata_field(doc, "text_direction")),
        #     )
        #     result.set_metadata(
        #         HTML_OPEN_GRAPH,
        #         _to_mapping(_first_metadata_field(doc, "open_graph")) or None,
        #     )
        #     result.set_metadata(
        #         HTML_TWITTER_CARD,
        #         _to_mapping(_first_metadata_field(doc, "twitter_card")) or None,
        #     )
        #     result.set_metadata(
        #         HTML_META_TAGS,
        #         _to_mapping(_first_metadata_field(doc, "meta_tags")) or None,
        #     )

        if format_type == "pdf":
            result.set_metadata(
                PDF_VERSION, _to_str(_first_metadata_field(doc, "pdf_version"))
            )
            result.set_metadata(
                PDF_PRODUCER, _to_str(_first_metadata_field(doc, "producer"))
            )
            is_encrypted = _to_bool(_first_metadata_field(doc, "is_encrypted"))
            result.set_metadata(
                PDF_ENCRYPTED, None if is_encrypted is None else int(is_encrypted)
            )

        return result

    @staticmethod
    def _resolve_mime_type(changes: MetadataChanges) -> str | None:
        value = changes.latest_value(FILE_TYPE, value_type=str)
        if value:
            return value
        return None


def _first_metadata_field(doc: ExtractionResult, *field_names: str) -> Any | None:
    for field_name in field_names:
        value = doc.get_metadata_field(field_name)
        if value is not None:
            return value
    return None


def _normalize_text(value: Any) -> str | None:
    text = _to_str(value)
    if text is None:
        return None
    return text.strip() or None


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"0", "false", "no"}:
            return False
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if "/" in raw and raw.count("/") == 1:
            numerator, denominator = raw.split("/")
            try:
                return float(numerator.strip()) / float(denominator.strip())
            except (TypeError, ValueError, ZeroDivisionError):
                return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _to_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _unique_strings(
            [part.strip() for part in value.split(",") if part.strip()]
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        output: list[str] = []
        for item in value:
            as_str = _to_str(item)
            if as_str:
                output.append(as_str)
        return _unique_strings(output)
    as_str = _to_str(value)
    return [as_str] if as_str else []


def _unique_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def _to_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _first_mapping_value(mapping: Mapping[str, Any], *keys: str) -> Any | None:
    lower_map = {str(key).lower(): value for key, value in mapping.items()}
    for key in keys:
        value = lower_map.get(key.lower())
        if value is not None:
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
    supported_prefixes = ("application/vnd.openxmlformats-officedocument",)
    return any(mime_type.startswith(prefix) for prefix in supported_prefixes)
