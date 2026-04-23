from __future__ import annotations

import asyncio
import csv
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import unquote, urlparse

from pydantic import Field, field_validator

from katalog.config import current_workspace
from katalog.models import Actor
from katalog.sources.tabular import TabularRawRow, TabularSource, TabularSourceConfig


class CsvSourceConfig(TabularSourceConfig):
    csv_file: str = Field(
        ...,
        description="Workspace-relative or absolute path to CSV/TSV file.",
    )
    delimiter: str | None = Field(
        default=None,
        description="Optional delimiter override. Defaults to ',' (or tab for .tsv).",
    )
    quotechar: str = Field(default='"', description="CSV quote character.")
    encoding: str = Field(default="utf-8-sig", description="File encoding.")

    @field_validator("csv_file", mode="before")
    @classmethod
    def _normalize_csv_file(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("csv_file is required")
        return text

    @field_validator("delimiter", mode="before")
    @classmethod
    def _normalize_delimiter(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        if not text:
            return None
        if len(text) != 1:
            raise ValueError("delimiter must be a single character")
        return text

    @field_validator("quotechar", mode="before")
    @classmethod
    def _normalize_quotechar(cls, value: Any) -> str:
        text = str(value or "")
        if len(text) != 1:
            raise ValueError("quotechar must be a single character")
        return text


class CsvSource(TabularSource):
    plugin_id = "katalog.sources.csv.CsvSource"
    title = "CSV table"
    description = "Read tabular rows from a CSV/TSV file."
    config_model = CsvSourceConfig

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.csv_file = cfg.csv_file
        self.encoding = cfg.encoding
        self.quotechar = cfg.quotechar
        self.csv_path = self._resolve_csv_path(cfg.csv_file)
        self.delimiter = cfg.delimiter or (
            "\t" if self.csv_path.suffix == ".tsv" else ","
        )

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "CSV table source",
            "version": "0.1",
        }

    def source_uri(self) -> str:
        return self.csv_path.as_uri()

    def row_path_value(self, row_number: int) -> str | None:
        _ = row_number
        # CSV rows do not have a stable path-like identity, so omit FILE_PATH.
        return None

    def can_scan_uri(self, uri: str) -> bool:
        path = self._path_from_scan_uri(uri)
        return path.exists() and path.is_file()

    async def iter_raw_rows(self) -> AsyncIterator[TabularRawRow]:
        rows = await asyncio.to_thread(self._read_rows_sync)
        for row in rows:
            yield row

    def _read_rows_sync(self) -> list[TabularRawRow]:
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV source file not found: {self.csv_path}")

        rows: list[TabularRawRow] = []
        with self.csv_path.open(
            "r",
            encoding=self.encoding,
            newline="",
        ) as handle:
            reader = csv.reader(
                handle,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )
            for idx, values in enumerate(reader, start=1):
                rows.append(TabularRawRow(row_number=idx, values=list(values)))
        return rows

    @staticmethod
    def _resolve_csv_path(raw_path: str) -> Path:
        path = Path(raw_path).expanduser()
        if path.is_absolute():
            return path.resolve()
        try:
            workspace = current_workspace()
        except RuntimeError:
            workspace = Path.cwd()
        return (workspace / path).resolve()

    @staticmethod
    def _path_from_scan_uri(uri: str) -> Path:
        text = str(uri or "").strip()
        if text.startswith("file://"):
            parsed = urlparse(text)
            return Path(unquote(parsed.path)).expanduser()
        return Path(text).expanduser()
