from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from katalog.db import Database
from katalog.models import (
    FILE_NAME,
    WARNING_NAME_READABILITY,
    AssetRecord,
    Metadata,
    make_metadata,
)
from katalog.processors.base import Processor, ProcessorResult, ProcessorStatus


class NameReadabilityProcessor(Processor):
    """Flags filenames that look auto-generated or otherwise unreadable."""

    PLUGIN_ID = "dev.katalog.processor.name_readability"
    dependencies = frozenset({FILE_NAME})
    outputs = frozenset({WARNING_NAME_READABILITY})

    _HEXISH_ID = re.compile(r"^[0-9a-f]{12,}$", re.IGNORECASE)
    _GUID = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    _DIGIT_RUN = re.compile(r"\d{5,}")

    def __init__(self, *, database: Database | None = None, **_: Any) -> None:
        self.database = database

    def should_run(
        self,
        record: AssetRecord,
        changes: set[str] | None,
        database: Database | None = None,
    ) -> bool:
        if changes and FILE_NAME in changes:
            return True
        db = database or self.database
        if not db:
            return False
        existing = db.get_metadata_for_file(
            record.id,
            provider_id=record.provider_id,
            metadata_key=WARNING_NAME_READABILITY,
        )
        return not existing

    async def run(
        self, record: AssetRecord, changes: set[str] | None
    ) -> ProcessorResult:
        name = self._resolve_file_name(record)
        if not name:
            return ProcessorResult(
                status=ProcessorStatus.SKIPPED, message="No filename found"
            )
        analysis = self._analyze_name(name)
        signals = analysis["signals"]
        if not signals:
            return ProcessorResult(
                status=ProcessorStatus.COMPLETED, message="No signals from analysis"
            )
        confidence = min(0.9, 0.4 + 0.1 * len(signals))
        provider_id = getattr(self, "provider_id", record.provider_id)
        metadata = make_metadata(
            provider_id,
            WARNING_NAME_READABILITY,
            analysis,
            confidence=confidence,
        )
        return ProcessorResult(metadata=[metadata])

    def _resolve_file_name(self, record: AssetRecord) -> str | None:
        if self.database:
            entries = self.database.get_metadata_for_file(
                record.id,
                provider_id=record.provider_id,
                metadata_key=FILE_NAME,
            )
            if entries:
                latest = entries[0].value
                if isinstance(latest, str) and latest:
                    return latest
        return Path(record.canonical_uri).name or record.canonical_uri

    def _analyze_name(self, filename: str) -> dict[str, Any]:
        stem = Path(filename).stem or filename
        normalized = stem.strip()
        signals: list[str] = []
        if not normalized:
            return {"filename": filename, "stem": normalized, "signals": [], "score": 0}

        letters = [ch for ch in normalized if ch.isalpha()]
        digits = sum(ch.isdigit() for ch in normalized)
        length = len(normalized)

        if normalized[0].islower():
            signals.append("starts_with_lowercase")
        if "_" in normalized:
            signals.append("contains_underscore")
        if re.search(r"[^\w\s.-]", normalized):
            signals.append("contains_special_characters")
        if not any(sep in normalized for sep in (" ", "-", "_")) and length >= 8:
            signals.append("single_word")
        if digits and (
            digits / max(1, length) >= 0.4 or self._DIGIT_RUN.search(normalized)
        ):
            signals.append("digit_heavy")
        if self._HEXISH_ID.fullmatch(normalized) or self._GUID.fullmatch(normalized):
            signals.append("identifier_pattern")
        if length >= 8 and letters and not any(ch.lower() in "aeiou" for ch in letters):
            signals.append("missing_vowels")

        score = len(signals)
        return {
            "filename": filename,
            "stem": normalized,
            "signals": signals,
            "score": score,
        }
