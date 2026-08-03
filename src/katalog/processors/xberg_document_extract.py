from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, FrozenSet, Protocol, cast

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field
from xberg import (
    ArchiveMetadata,
    ChunkingConfig,
    ExcelMetadata,
    ExtractedDocument,
    ExtractInput,
    ExtractionConfig,
    ImageMetadata,
    LanguageDetectionConfig,
    PdfMetadata,
    PptxMetadata,
    TextMetadata,
    UnsupportedFormatError,
    ValidationError,
    extract,
    extract_batch,
)

from katalog.constants.metadata import (
    ACCESS_CREATED_BY,
    ACCESS_LAST_MODIFIED_BY,
    ARCHIVE_FORMAT,
    ARCHIVE_UNCOMPRESSED_SIZE,
    DATA_FILE_READER,
    DATA_KEY,
    DOC_AUTHOR,
    DOC_CATEGORY,
    DOC_CHARS,
    DOC_CHUNK_COUNT,
    DOC_CHUNK_TEXT,
    DOC_KEYWORD,
    DOC_LANG,
    DOC_LINES,
    DOC_PAGES,
    DOC_SUMMARY,
    DOC_TEXT,
    DOC_WORDS,
    FILE_CHILD_COUNT,
    FILE_DESCRIPTION,
    FILE_SIZE,
    FILE_TITLE,
    FILE_TYPE,
    FILE_VERSION,
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


# xberg's FormatMetadata is a tagged union whose per-format variant accessors
# (pdf/text/image/...) are dynamic pyo3 attributes not declared in its type
# stubs, so we describe the ones we read here.
class _FormatMetadataVariants(Protocol):
    format_type: str
    pdf: PdfMetadata | None
    pptx: PptxMetadata | None
    excel: ExcelMetadata | None
    text: TextMetadata | None
    archive: ArchiveMetadata | None
    image: ImageMetadata | None


class XbergDocumentExtractProcessor(Processor):
    plugin_id = (
        "katalog.processors.xberg_document_extract.XbergDocumentExtractProcessor"
    )
    title = "Xberg document extract"
    description = "Extract text, metadata and chunks using xberg."
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
        use_batch: bool = Field(
            default=True,
            description="Use Xberg batch extraction APIs when processing a workflow batch.",
        )

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
        return (await self.run_batch([changes]))[0]

    async def run_batch(
        self,
        changes_batch: list[MetadataChanges],
    ) -> list[ProcessorResult]:
        if not changes_batch:
            return []
        if not self.config.use_batch:
            return [await self._run_single(changes) for changes in changes_batch]
        results: list[ProcessorResult | None] = [None] * len(changes_batch)
        jobs: list[tuple[int, ExtractInput]] = []

        for idx, changes in enumerate(changes_batch):
            asset = changes.asset
            if asset is None:
                results[idx] = ProcessorResult(
                    status=OpStatus.ERROR, message="MetadataChanges.asset is missing"
                )
                continue
            reader = await changes.get_data_reader(DATA_FILE_READER)
            if reader is None:
                results[idx] = ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message="Asset does not have a data accessor",
                )
                continue
            mime_type = self._resolve_mime_type(changes)
            if mime_type and not _is_supported_mime(mime_type):
                results[idx] = ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message=f"Unsupported mime type for xberg: {mime_type}",
                )
                continue
            reader_path = reader.path
            if reader_path:
                jobs.append(
                    (
                        idx,
                        ExtractInput(
                            kind="uri", uri=str(reader_path), mime_type=mime_type
                        ),
                    )
                )
                continue
            data = await reader.read()
            if not data:
                results[idx] = ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message="Asset data reader returned empty content",
                )
                continue
            # Mime detection moved inside xberg; a known type is passed as a hint
            # and unsupported/undetectable content surfaces as a per-input error.
            jobs.append(
                (idx, ExtractInput(kind="bytes", bytes=data, mime_type=mime_type))
            )

        await self._run_extraction_jobs(jobs=jobs, results=results)

        finalized: list[ProcessorResult] = []
        for result in results:
            if result is None:
                finalized.append(
                    ProcessorResult(
                        status=OpStatus.ERROR, message="Missing extraction result"
                    )
                )
            else:
                finalized.append(result)
        return finalized

    async def _run_single(self, changes: MetadataChanges) -> ProcessorResult:
        asset = changes.asset
        if asset is None:
            return ProcessorResult(
                status=OpStatus.ERROR, message="MetadataChanges.asset is missing"
            )
        reader = await changes.get_data_reader(DATA_FILE_READER)
        if reader is None:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Asset does not have a data accessor"
            )

        mime_type = self._resolve_mime_type(changes)
        if mime_type and not _is_supported_mime(mime_type):
            return ProcessorResult(
                status=OpStatus.SKIPPED,
                message=f"Unsupported mime type for xberg: {mime_type}",
            )

        reader_path = reader.path
        if reader_path:
            job_input = ExtractInput(
                kind="uri", uri=str(reader_path), mime_type=mime_type
            )
        else:
            data = await reader.read()
            if not data:
                return ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message="Asset data reader returned empty content",
                )
            job_input = ExtractInput(kind="bytes", bytes=data, mime_type=mime_type)
        return await self._extract_one(job_input)

    async def _run_extraction_jobs(
        self,
        *,
        jobs: list[tuple[int, ExtractInput]],
        results: list[ProcessorResult | None],
    ) -> None:
        if not jobs:
            return
        inputs = [job_input for _idx, job_input in jobs]
        try:
            extraction = await extract_batch(inputs, config=self.extraction_config)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Xberg extract_batch failed for {count} inputs, falling back to single extraction: {error}",
                count=len(jobs),
                error=str(exc),
            )
            await self._run_jobs_singly(jobs=jobs, results=results)
            return
        # Successful documents come back in input order; failures are reported
        # separately with the position of the input that caused them.
        errors_by_input = {error.index: error for error in extraction.errors}
        if len(extraction.results) + len(errors_by_input) != len(jobs):
            logger.warning(
                "Xberg extract_batch returned {result_count} results and {error_count} errors for {count} inputs, falling back to single extraction",
                result_count=len(extraction.results),
                error_count=len(errors_by_input),
                count=len(jobs),
            )
            await self._run_jobs_singly(jobs=jobs, results=results)
            return
        docs = iter(extraction.results)
        for position, (idx, _job_input) in enumerate(jobs):
            error = errors_by_input.get(position)
            if error is not None:
                status = (
                    OpStatus.SKIPPED
                    if error.error_type == "unsupported_format"
                    else OpStatus.ERROR
                )
                results[idx] = ProcessorResult(
                    status=status,
                    message=f"Xberg extraction failed: {error.message}",
                )
                continue
            results[idx] = self._build_processor_result(next(docs))

    async def _run_jobs_singly(
        self,
        *,
        jobs: list[tuple[int, ExtractInput]],
        results: list[ProcessorResult | None],
    ) -> None:
        for idx, job_input in jobs:
            results[idx] = await self._extract_one(job_input)

    async def _extract_one(self, job_input: ExtractInput) -> ProcessorResult:
        try:
            extraction = await extract(input=job_input, config=self.extraction_config)
        except ValidationError as exc:
            return ProcessorResult(
                status=OpStatus.SKIPPED,
                message=f"Xberg validation failed: {exc}",
            )
        except Exception as exc:  # noqa: BLE001
            if _is_unsupported_format_error(exc):
                return ProcessorResult(
                    status=OpStatus.SKIPPED,
                    message=f"Unsupported format for xberg: {exc}",
                )
            return ProcessorResult(
                status=OpStatus.ERROR,
                message=f"Xberg extraction failed: {exc}",
            )
        if not extraction.results:
            message = "; ".join(error.message for error in extraction.errors)
            return ProcessorResult(
                status=OpStatus.ERROR,
                message=message or "Xberg returned no extraction result",
            )
        return self._build_processor_result(extraction.results[0])

    def _build_extraction_config(
        self,
        config: ConfigModel,
    ) -> ExtractionConfig:
        chunking_config = None
        if config.enable_chunking:
            # Use Xberg's default chunking strategy/limits.
            chunking_config = ChunkingConfig()
        extraction = ExtractionConfig(
            chunking=chunking_config,
            language_detection=LanguageDetectionConfig(enabled=True),
        )

        # Use KeywordConfig
        return extraction

    def _build_processor_result(self, doc: ExtractedDocument) -> ProcessorResult:
        result = ProcessorResult(actor_id=self.actor.id)

        md = doc.metadata
        # FormatMetadata is a tagged union: exactly one variant accessor
        # (pdf/image/archive/text/...) is non-None per document.
        format_md = cast("_FormatMetadataVariants | None", md.format)
        pdf_md = format_md.pdf if format_md is not None else None

        result.set_metadata(DOC_TEXT, _normalize_text(doc.content))

        languages = _unique_strings(
            [
                *_to_string_list(doc.detected_languages),
                *_to_string_list(md.language),
            ]
        )
        result.set_metadata_list(DOC_LANG, languages)

        pages = doc.counts.pages
        if pages <= 0 and format_md is not None:
            pptx_md = format_md.pptx
            excel_md = format_md.excel
            if pdf_md is not None and pdf_md.page_count:
                pages = pdf_md.page_count
            elif pptx_md is not None and pptx_md.slide_count:
                pages = pptx_md.slide_count
            elif excel_md is not None and excel_md.sheet_count:
                pages = excel_md.sheet_count
        result.set_metadata(DOC_PAGES, pages if pages > 0 else None)

        result.set_metadata(FILE_TYPE, doc.mime_type)

        result.set_metadata(FILE_TITLE, _to_str(md.title))
        result.set_metadata(FILE_DESCRIPTION, _to_str(md.subject))
        result.set_metadata(DOC_SUMMARY, _to_str(md.abstract_text))
        result.set_metadata(DOC_CATEGORY, _to_str(md.category))
        result.set_metadata(FILE_VERSION, _to_str(md.document_version))

        result.set_metadata_list(DOC_AUTHOR, _to_string_list(md.authors))

        created_at = parse_datetime_utc(_to_str(md.created_at), strict=False)
        modified_at = parse_datetime_utc(_to_str(md.modified_at), strict=False)
        result.set_metadata(TIME_CREATED, created_at)
        result.set_metadata(TIME_MODIFIED, modified_at)

        result.set_metadata(ACCESS_CREATED_BY, _to_str(md.created_by))
        result.set_metadata(ACCESS_LAST_MODIFIED_BY, _to_str(md.modified_by))

        text_md = format_md.text if format_md is not None else None
        result.set_metadata(
            DOC_CHARS, _to_int(text_md.character_count) if text_md else None
        )
        result.set_metadata(DOC_WORDS, _to_int(text_md.word_count) if text_md else None)
        result.set_metadata(DOC_LINES, _to_int(text_md.line_count) if text_md else None)

        keywords = _unique_strings(
            [
                *_to_string_list(md.keywords),
                *_to_string_list(md.tags),
            ]
        )
        result.set_metadata_list(DOC_KEYWORD, keywords)

        chunk_texts = [_normalize_text(chunk.content) for chunk in (doc.chunks or [])]
        chunk_texts = [chunk_text for chunk_text in chunk_texts if chunk_text]
        chunk_count = len(doc.chunks or [])
        if chunk_count <= 0 and chunk_texts:
            chunk_count = len(chunk_texts)
        result.set_metadata(DOC_CHUNK_COUNT, chunk_count if chunk_count > 0 else None)
        result.set_metadata_list(DOC_CHUNK_TEXT, chunk_texts)

        # Audio tag metadata (artist/album/year/genre) was dropped in xberg;
        # AudioMetadata now only carries technical fields (codec, duration, ...).

        archive_md = format_md.archive if format_md is not None else None
        if archive_md is not None:
            result.set_metadata(ARCHIVE_FORMAT, _to_str(archive_md.format))
            result.set_metadata(FILE_CHILD_COUNT, _to_int(archive_md.file_count))
            result.set_metadata(
                ARCHIVE_UNCOMPRESSED_SIZE, _to_int(archive_md.total_size)
            )

        image_md = format_md.image if format_md is not None else None
        if image_md is not None:
            result.set_metadata(IMAGE_WIDTH, _to_int(image_md.width))
            result.set_metadata(IMAGE_HEIGHT, _to_int(image_md.height))

            exif = image_md.exif
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
        # docx_md = format_md.docx if format_md is not None else None
        # if docx_md is not None:
        #     core_props = docx_md.core_properties
        #     app_props = docx_md.app_properties
        #     custom_props = docx_md.custom_properties

        # A bit too detailed for now
        # html_md = format_md.html if format_md is not None else None
        # if html_md is not None:
        #     result.set_metadata(HTML_CANONICAL_URL, _to_str(html_md.canonical_url))
        #     result.set_metadata(HTML_BASE_HREF, _to_str(html_md.base_href))
        #     result.set_metadata(HTML_TEXT_DIRECTION, _to_str(html_md.text_direction))
        #     result.set_metadata(HTML_OPEN_GRAPH, html_md.open_graph or None)
        #     result.set_metadata(HTML_TWITTER_CARD, html_md.twitter_card or None)
        #     result.set_metadata(HTML_META_TAGS, html_md.meta_tags or None)

        if pdf_md is not None:
            result.set_metadata(PDF_VERSION, _to_str(pdf_md.pdf_version))
            result.set_metadata(PDF_PRODUCER, _to_str(pdf_md.producer))
            is_encrypted = _to_bool(pdf_md.is_encrypted)
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


def _is_unsupported_format_error(exc: Exception) -> bool:
    if isinstance(exc, UnsupportedFormatError):
        return True
    # The xberg pyo3 bindings surface some core errors as plain RuntimeError
    # instead of the typed exception classes.
    return isinstance(exc, RuntimeError) and "Unsupported format" in str(exc)


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
