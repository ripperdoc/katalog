from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from katalog.config import current_workspace
from katalog.constants.metadata import (
    FILE_URI,
    METADATA_REGISTRY,
    SOURCE_JSON_RECORD,
    MetadataKey,
    MetadataType,
    get_metadata_def_by_key,
)
from katalog.models import Actor, Asset, DataReader, MetadataChanges, OpStatus
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.url import canonicalize_web_url
from katalog.utils.utils import parse_datetime_utc


class JsonListSource(SourcePlugin):
    """Source that emits URL assets from a JSON file and optional mapped metadata."""

    plugin_id = "katalog.sources.json_list.JsonListSource"
    title = "JSON document list"
    description = "Emit web assets from a JSON file with URL records and optional metadata mappings."

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        namespace: str = Field(default="web")
        json_file: str = Field(
            description="Workspace-relative or absolute JSON file path containing records."
        )
        records_field: str | None = Field(
            default=None,
            description="Optional top-level field containing the record list when root JSON is an object.",
        )
        records_are_map: bool = Field(
            default=False,
            description=(
                "Treat records_field as an object map where each key is used as external_id "
                "and each value is the record payload."
            ),
        )
        url_field: str = Field(
            default="url",
            description="Field path used as URL, supports dotted object paths.",
        )
        metadata_mappings: dict[str, str] = Field(
            default_factory=dict,
            description="Map JSON field paths to known metadata keys, e.g. {'title': 'file/title'}.",
        )
        emit_record_json: bool = Field(
            default=False,
            description="Emit the full source record as JSON metadata.",
        )
        record_metadata_key: str = Field(
            default=str(SOURCE_JSON_RECORD),
            description="Metadata key used for full source record payload.",
        )
        max_records: int = Field(default=0, ge=0, description="0 means no limit")

        @model_validator(mode="after")
        def _validate_config(self) -> "JsonListSource.ConfigModel":
            if not self.json_file.strip():
                raise ValueError("json_file is required")
            if not self.url_field.strip():
                raise ValueError("url_field is required")
            self.json_file = self.json_file.strip()
            self.url_field = self.url_field.strip()
            if self.records_field is not None:
                self.records_field = self.records_field.strip() or None
            if self.emit_record_json:
                self.record_metadata_key = str(
                    _resolve_metadata_key(self.record_metadata_key)
                )
            normalized_mappings: dict[str, str] = {}
            for field_path, metadata_key in self.metadata_mappings.items():
                if not field_path.strip():
                    raise ValueError("metadata_mappings cannot contain empty field paths")
                normalized_mappings[field_path.strip()] = str(
                    _resolve_metadata_key(metadata_key)
                )
            self.metadata_mappings = normalized_mappings
            return self

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.namespace = cfg.namespace
        self.json_file = cfg.json_file
        self.records_field = cfg.records_field
        self.records_are_map = cfg.records_are_map
        self.url_field = cfg.url_field
        self.metadata_mappings = dict(cfg.metadata_mappings)
        self.emit_record_json = cfg.emit_record_json
        self.record_metadata_key = MetadataKey(cfg.record_metadata_key)
        self.max_records = cfg.max_records
        self._mapping_defs = {
            field_path: get_metadata_def_by_key(MetadataKey(key))
            for field_path, key in self.metadata_mappings.items()
        }

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "JSON document list source",
            "version": "0.1",
        }

    def authorize(self, **kwargs) -> str:
        _ = kwargs
        return ""

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        _ = key, changes
        return None

    def can_scan_uri(self, uri: str) -> bool:
        _ = uri
        return True

    def get_namespace(self) -> str:
        return self.namespace

    async def scan(self) -> ScanResult:
        records = self._load_records()
        if self.max_records > 0:
            records = records[: self.max_records]

        valid_rows: list[tuple[str, str, dict[str, Any]]] = []
        ignored = 0
        for external_id, row in records:
            if not isinstance(row, dict):
                ignored += 1
                continue
            url_candidate = _read_path(row, self.url_field)
            if not isinstance(url_candidate, str):
                ignored += 1
                continue
            url = canonicalize_web_url(url_candidate)
            if not (url.startswith("http://") or url.startswith("https://")):
                ignored += 1
                continue
            resolved_external_id = (
                str(external_id).strip() if external_id is not None else ""
            ) or url
            valid_rows.append((resolved_external_id, url, row))

        async def iterator():
            for external_id, url, row in valid_rows:
                asset = Asset(
                    namespace=self.namespace,
                    external_id=external_id,
                    canonical_uri=url,
                    actor_id=self.actor.id,
                )
                result = AssetScanResult(asset=asset, actor=self.actor)
                result.set_metadata(FILE_URI, url)
                if self.emit_record_json:
                    result.set_metadata(self.record_metadata_key, row)
                self._emit_mapped_metadata(result, row)
                yield result

        return ScanResult(iterator=iterator(), status=OpStatus.COMPLETED, ignored=ignored)

    def _emit_mapped_metadata(self, result: AssetScanResult, row: dict[str, Any]) -> None:
        for field_path, metadata_def in self._mapping_defs.items():
            value = _read_path(row, field_path)
            if value is None:
                continue
            _set_mapped_metadata(
                result=result,
                metadata_key=metadata_def.key,
                value_type=metadata_def.value_type,
                value=value,
            )

    def _load_records(self) -> list[tuple[str | None, Any]]:
        json_path = self._resolve_json_file_path()
        if not json_path.exists():
            raise FileNotFoundError(f"JSON source file not found: {json_path}")

        payload = json.loads(json_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [(None, row) for row in payload]
        if isinstance(payload, dict):
            if self.records_field:
                records = payload.get(self.records_field)
                if isinstance(records, list):
                    return [(None, row) for row in records]
                if self.records_are_map and isinstance(records, dict):
                    return [(str(key), value) for key, value in records.items()]
                expected = "list or object map" if self.records_are_map else "list"
                raise ValueError(
                    f"JSON records_field '{self.records_field}' is not a {expected} in {json_path}"
                )
            if isinstance(payload.get("records"), list):
                return [(None, row) for row in payload["records"]]
            if isinstance(payload.get("documents"), list):
                return [(None, row) for row in payload["documents"]]
            return [(None, payload)]
        raise ValueError(
            f"JSON source root must be an array or object, got {type(payload).__name__}"
        )

    def _resolve_json_file_path(self) -> Path:
        path = Path(self.json_file).expanduser()
        if path.is_absolute():
            return path.resolve()
        try:
            workspace = current_workspace()
        except RuntimeError:
            workspace = Path.cwd()
        return (workspace / path).resolve()


def _resolve_metadata_key(raw_key: str) -> MetadataKey:
    key = MetadataKey(raw_key.strip())
    if key not in METADATA_REGISTRY:
        raise ValueError(f"Unknown metadata key '{raw_key}'")
    get_metadata_def_by_key(key)
    return key


def _read_path(record: dict[str, Any], field_path: str) -> Any:
    current: Any = record
    for segment in field_path.split("."):
        key = segment.strip()
        if not key:
            return None
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _set_mapped_metadata(
    *,
    result: AssetScanResult,
    metadata_key: MetadataKey,
    value_type: MetadataType,
    value: Any,
) -> None:
    if value_type == MetadataType.JSON:
        result.set_metadata(metadata_key, value)
        return

    values = value if isinstance(value, list) else [value]
    scalar_values: list[Any] = []
    for item in values:
        coerced = _coerce_scalar(value_type=value_type, value=item)
        if coerced is None:
            continue
        scalar_values.append(coerced)
    if not scalar_values:
        return
    if len(scalar_values) == 1:
        result.set_metadata(metadata_key, scalar_values[0])
    else:
        result.set_metadata_list(metadata_key, scalar_values)


def _coerce_scalar(*, value_type: MetadataType, value: Any) -> Any | None:
    if value is None:
        return None
    if value_type == MetadataType.STRING:
        return str(value)
    if value_type == MetadataType.INT:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if value_type == MetadataType.FLOAT:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if value_type == MetadataType.DATETIME:
        return parse_datetime_utc(value)
    if value_type in {MetadataType.RELATION, MetadataType.COLLECTION}:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None
