from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, AsyncIterator
from urllib.parse import quote

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from katalog.constants.metadata import (
    ASSET_ACTOR_ID,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    FILE_PATH,
    MetadataKey,
    MetadataType,
    define_metadata,
)
from katalog.models import Actor, Asset, DataReader, MetadataChanges, OpStatus
from katalog.models.views import ColumnSpec, ViewSpec
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.utils import parse_datetime_utc

TABULAR_ROW_NUMBER = define_metadata(
    "tabular/row_number",
    MetadataType.INT,
    "Row number",
    "1-based row number in the tabular source.",
)


def _normalize_column_name(value: str) -> str:
    return value.strip().lower()


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return cleaned or "column"


class TabularColumnMapping(BaseModel):
    """Mapping from a source column to one metadata key."""

    model_config = ConfigDict(extra="ignore")

    column: str = Field(..., description="Header name to map from.")
    key: str | None = Field(
        default=None,
        description=(
            "Metadata key (category/property). If omitted, generated as "
            "`tabular/<normalized_column_name>`."
        ),
    )
    value_type: MetadataType = Field(
        default=MetadataType.STRING,
        description="Metadata value type for this column.",
    )
    title: str = Field(default="", description="Metadata title shown in UIs.")
    description: str = Field(default="", description="Metadata description.")
    width: int | None = Field(default=None, description="Optional UI column width.")
    searchable: bool | None = Field(
        default=None,
        description="Whether this metadata should be searchable.",
    )
    delimiter: str | None = Field(
        default=None,
        description=(
            "Optional split delimiter for string cells. "
            "When set, a cell can emit multiple metadata values."
        ),
    )

    @field_validator("column", mode="before")
    @classmethod
    def _validate_column(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("column is required")
        return text

    @field_validator("key", mode="before")
    @classmethod
    def _normalize_key(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator("value_type", mode="before")
    @classmethod
    def _normalize_value_type(cls, value: Any) -> MetadataType:
        if isinstance(value, MetadataType):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if not text:
                return MetadataType.STRING
            if text.isdigit():
                return MetadataType(int(text))
            try:
                return MetadataType[text.upper()]
            except KeyError as exc:
                raise ValueError(
                    f"Unknown value_type '{value}'. Expected one of: "
                    f"{', '.join(item.name.lower() for item in MetadataType)}"
                ) from exc
        return MetadataType(value)

    @field_validator("delimiter", mode="before")
    @classmethod
    def _normalize_delimiter(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        if not text:
            return None
        return text


class TabularSourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    namespace: str = Field(
        default="tabular",
        description="Asset namespace for external_id uniqueness.",
    )
    id_column: str = Field(
        ...,
        description="Header name used as stable external asset ID.",
    )
    header_row: int = Field(
        default=1,
        ge=1,
        description="1-based row number containing column headers.",
    )
    max_rows: int = Field(
        default=0,
        ge=0,
        description="Maximum number of emitted data rows (0 means no limit).",
    )
    column_mappings: list[TabularColumnMapping] = Field(
        default_factory=list,
        description="Column-to-metadata mappings.",
    )

    @field_validator("namespace", "id_column", mode="before")
    @classmethod
    def _normalize_required_text(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("value cannot be empty")
        return text

    @model_validator(mode="after")
    def _validate_uniqueness(self) -> "TabularSourceConfig":
        seen_columns: set[str] = set()
        seen_keys: set[str] = set()
        for mapping in self.column_mappings:
            column_key = _normalize_column_name(mapping.column)
            if column_key in seen_columns:
                raise ValueError(f"Duplicate column mapping for '{mapping.column}'")
            seen_columns.add(column_key)

            key = str(TabularSource.metadata_key_for_mapping(mapping))
            if key in seen_keys:
                raise ValueError(
                    f"Duplicate metadata key in column mappings: '{key}'"
                )
            seen_keys.add(key)
        return self


@dataclass(frozen=True)
class TabularRawRow:
    row_number: int
    values: list[Any]


@dataclass(frozen=True)
class _ResolvedColumnMapping:
    index: int
    config: TabularColumnMapping
    metadata_key: MetadataKey


class TabularSource(SourcePlugin):
    """
    Shared source logic for table-like datasets (CSV, Sheets, Excel).

    Subclasses only need to provide row transport/parsing via `iter_raw_rows()`
    and a source URI string.

    Future work:
    - Detect schema drift across scans (renamed/added/removed columns) and summarize in scan output.
    - Add explicit data window controls (for example `end_row`) to exclude footer rows.
    - Add coercion diagnostics per column (dropped values due to type mismatch).
    """

    plugin_id = "katalog.sources.tabular.TabularSource"
    title = "Tabular source"
    description = "Base source for tabular datasets."
    config_model = TabularSourceConfig

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.namespace = cfg.namespace
        self.id_column = cfg.id_column
        self.header_row = int(cfg.header_row)
        self.max_rows = int(cfg.max_rows)
        self.column_mappings = list(cfg.column_mappings)

    @classmethod
    def metadata_definitions_from_config(
        cls, config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        cfg = cls.config_model.model_validate(config or {})
        definitions: list[dict[str, Any]] = []
        for mapping in cfg.column_mappings:
            metadata_key = cls.metadata_key_for_mapping(mapping)
            definitions.append(
                {
                    "key": str(metadata_key),
                    "value_type": mapping.value_type,
                    "title": mapping.title or mapping.column,
                    "description": mapping.description
                    or f"Column '{mapping.column}' from tabular source.",
                    "width": mapping.width,
                    "searchable": mapping.searchable,
                }
            )
        return definitions

    @staticmethod
    def metadata_key_for_mapping(mapping: TabularColumnMapping) -> MetadataKey:
        key_raw = mapping.key or f"tabular/{_slugify(mapping.column)}"
        key = MetadataKey(str(key_raw).strip())
        if "/" not in str(key):
            raise ValueError(
                f"Invalid metadata key '{key}'. Expected format '<category>/<property>'."
            )
        return key

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "Tabular source",
            "version": "0.1",
        }

    def view_definitions(self) -> list[ViewSpec | dict[str, Any]]:
        actor_id = self.actor.id
        if actor_id is None:
            return []

        mapped_keys: list[MetadataKey] = []
        seen: set[str] = set()
        for mapping in self.column_mappings:
            key = self.metadata_key_for_mapping(mapping)
            key_str = str(key)
            if key_str in seen:
                continue
            seen.add(key_str)
            mapped_keys.append(key)

        columns: list[ColumnSpec] = [
            ColumnSpec.from_metadata(ASSET_ID, hidden=True, sortable=True, width=80),
            ColumnSpec.from_metadata(
                ASSET_ACTOR_ID, hidden=True, sortable=True, filterable=True, width=120
            ),
            ColumnSpec.from_metadata(ASSET_EXTERNAL_ID, filterable=True, searchable=True),
            ColumnSpec.from_metadata(TABULAR_ROW_NUMBER, sortable=True, filterable=True),
        ]
        for key in mapped_keys:
            columns.append(
                ColumnSpec.from_metadata(
                    key,
                    filterable=True,
                    searchable=True,
                )
            )

        return [
            ViewSpec(
                id="table",
                name="Table",
                columns=columns,
                default_sort=[(str(TABULAR_ROW_NUMBER), "asc")],
                default_columns=None,
            )
        ]

    def authorize(self, **kwargs) -> str:
        _ = kwargs
        return ""

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        _ = key, changes
        return None

    def get_namespace(self) -> str:
        return self.namespace

    async def iter_raw_rows(self) -> AsyncIterator[TabularRawRow]:
        raise NotImplementedError()

    def source_uri(self) -> str:
        raise NotImplementedError()

    def source_debug_location(self) -> str:
        try:
            uri = self.source_uri()
        except Exception:
            uri = ""
        return uri or f"namespace={self.get_namespace()}"

    def canonical_row_uri(self, external_id: str, row_number: int) -> str:
        source = self.source_uri()
        if source:
            return f"{source}#row={row_number}&id={quote(external_id, safe='')}"
        return f"tabular://{self.get_namespace()}/{quote(external_id, safe='')}"

    def row_path_value(self, row_number: int) -> str | None:
        """Return optional FILE_PATH metadata value for this source row."""
        return f"row:{row_number}"

    async def scan(self) -> ScanResult:
        status = OpStatus.IN_PROGRESS
        ignored = 0

        async def iterator() -> AsyncIterator[AssetScanResult]:
            nonlocal status, ignored
            headers: list[str] | None = None
            column_index_by_name: dict[str, int] = {}
            id_column_index: int | None = None
            resolved_mappings: list[_ResolvedColumnMapping] = []
            emitted = 0

            async for raw_row in self.iter_raw_rows():
                row_number = int(raw_row.row_number)
                values = list(raw_row.values)

                if row_number < self.header_row:
                    continue

                if row_number == self.header_row:
                    headers = self._build_headers(values)
                    column_index_by_name = self._build_column_index(headers)
                    id_column_index = column_index_by_name.get(
                        _normalize_column_name(self.id_column)
                    )
                    if id_column_index is None:
                        header_preview = ", ".join(headers[:10]) if headers else "(empty)"
                        if headers and len(headers) > 10:
                            header_preview = f"{header_preview}, ..."
                        raise ValueError(
                            f"id_column '{self.id_column}' not found in header row {self.header_row} "
                            f"for {self.source_debug_location()}. Header columns: {header_preview}"
                        )
                    resolved_mappings = self._resolve_mappings(
                        column_index_by_name=column_index_by_name,
                        headers=headers,
                    )
                    continue

                if headers is None or id_column_index is None:
                    continue
                if self._is_empty_row(values):
                    ignored += 1
                    continue

                external_id = self._normalize_external_id(
                    self._cell_value(values, id_column_index)
                )
                if not external_id:
                    ignored += 1
                    continue

                asset = Asset(
                    external_id=external_id,
                    namespace=self.get_namespace(),
                    canonical_uri=self.canonical_row_uri(external_id, row_number),
                    actor_id=self.actor.id,
                )
                result = AssetScanResult(asset=asset, actor=self.actor)
                row_path = self.row_path_value(row_number)
                if row_path:
                    result.set_metadata(FILE_PATH, row_path)
                result.set_metadata(TABULAR_ROW_NUMBER, row_number)

                for mapping in resolved_mappings:
                    raw_value = self._cell_value(values, mapping.index)
                    self._emit_mapped_value(
                        result=result,
                        mapping=mapping.config,
                        metadata_key=mapping.metadata_key,
                        raw_value=raw_value,
                    )

                emitted += 1
                yield result

                if self.max_rows and emitted >= self.max_rows:
                    status = OpStatus.PARTIAL
                    break

            if status == OpStatus.IN_PROGRESS:
                status = OpStatus.COMPLETED
            scan_result.status = status
            scan_result.ignored = ignored

        scan_result = ScanResult(iterator=iterator(), status=status, ignored=ignored)
        return scan_result

    def _resolve_mappings(
        self,
        column_index_by_name: dict[str, int],
        headers: list[str],
    ) -> list[_ResolvedColumnMapping]:
        resolved: list[_ResolvedColumnMapping] = []
        missing_columns: list[str] = []
        for mapping in self.column_mappings:
            index = column_index_by_name.get(_normalize_column_name(mapping.column))
            if index is None:
                missing_columns.append(mapping.column)
                continue
            resolved.append(
                _ResolvedColumnMapping(
                    index=index,
                    config=mapping,
                    metadata_key=self.metadata_key_for_mapping(mapping),
                )
            )
        if missing_columns:
            raise ValueError(
                "Configured tabular column mappings were not found in source header. "
                f"Missing: {sorted(set(missing_columns))}. "
                f"Available: {headers}"
            )
        return resolved

    @staticmethod
    def _build_headers(values: list[Any]) -> list[str]:
        headers: list[str] = []
        for idx, value in enumerate(values):
            text = str(value or "").strip()
            if not text:
                text = f"column_{idx + 1}"
            headers.append(text)
        return headers

    @staticmethod
    def _build_column_index(headers: list[str]) -> dict[str, int]:
        index: dict[str, int] = {}
        for position, header in enumerate(headers):
            key = _normalize_column_name(header)
            if key in index:
                continue
            index[key] = position
        return index

    @staticmethod
    def _cell_value(values: list[Any], index: int) -> Any:
        if index < 0 or index >= len(values):
            return None
        return values[index]

    @staticmethod
    def _is_empty_row(values: list[Any]) -> bool:
        for value in values:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return False
        return True

    @staticmethod
    def _normalize_external_id(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return text

    def _emit_mapped_value(
        self,
        *,
        result: AssetScanResult,
        mapping: TabularColumnMapping,
        metadata_key: MetadataKey,
        raw_value: Any,
    ) -> None:
        values = self._split_cell_values(mapping=mapping, raw_value=raw_value)
        coerced_values: list[Any] = []
        for value in values:
            coerced = self._coerce_value(value_type=mapping.value_type, value=value)
            if coerced is None:
                continue
            coerced_values.append(coerced)
        if not coerced_values:
            return
        if len(coerced_values) == 1:
            result.set_metadata(metadata_key, coerced_values[0])
        else:
            result.set_metadata_list(metadata_key, coerced_values)

    @staticmethod
    def _split_cell_values(
        *,
        mapping: TabularColumnMapping,
        raw_value: Any,
    ) -> list[Any]:
        if raw_value is None:
            return []

        if isinstance(raw_value, list):
            return list(raw_value)

        if mapping.delimiter and isinstance(raw_value, str):
            parts = [part.strip() for part in raw_value.split(mapping.delimiter)]
            return [part for part in parts if part]

        return [raw_value]

    @staticmethod
    def _coerce_value(*, value_type: MetadataType, value: Any) -> Any | None:
        if value is None:
            return None

        if value_type == MetadataType.STRING:
            text = str(value).strip()
            return text or None

        if value_type == MetadataType.INT:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            text = str(value).strip()
            if not text:
                return None
            try:
                return int(text)
            except ValueError:
                try:
                    return int(float(text))
                except ValueError:
                    return None

        if value_type == MetadataType.FLOAT:
            if isinstance(value, float):
                return value
            text = str(value).strip()
            if not text:
                return None
            try:
                return float(text)
            except ValueError:
                return None

        if value_type == MetadataType.DATETIME:
            return parse_datetime_utc(value)

        if value_type == MetadataType.JSON:
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                if text.startswith("{") or text.startswith("["):
                    try:
                        return json.loads(text)
                    except Exception:
                        return text
                return text
            return value

        if value_type in {MetadataType.RELATION, MetadataType.COLLECTION}:
            text = str(value).strip()
            if not text:
                return None
            try:
                return int(text)
            except ValueError:
                return None

        return None
