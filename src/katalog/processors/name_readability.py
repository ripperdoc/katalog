from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import FILE_NAME, WARNING_NAME_READABILITY
from katalog.models import (
    Asset,
    OpStatus,
    make_metadata,
)
from katalog.processors.base import Processor, ProcessorResult


class NameReadabilityProcessor(Processor):
    """Flags filenames that look auto-generated or otherwise unreadable."""

    plugin_id = "katalog.processors.name_readability.NameReadabilityProcessor"
    title = "Name readability"
    description = "Flag filenames that look auto-generated or hard to read."
    _dependencies = frozenset({FILE_NAME})
    _outputs = frozenset({WARNING_NAME_READABILITY})

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        min_length: int = Field(
            default=5,
            ge=1,
            description="Minimum stem length to analyze; shorter names are skipped",
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

    _HEXISH_ID = re.compile(r"^[0-9a-f]{12,}$", re.IGNORECASE)
    _GUID = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    _DIGIT_RUN = re.compile(r"\d{5,}")

    def should_run(
        self,
        asset: Asset,
        changes,
    ) -> bool:
        changed_keys = changes.changed_keys()
        if FILE_NAME in changed_keys:
            return True
        return False

    async def run(self, asset: Asset, changes) -> ProcessorResult:
        name = self._resolve_file_name(asset)
        if not name:
            return ProcessorResult(status=OpStatus.SKIPPED, message="No filename found")
        if len(Path(name).stem or name) < self.config.min_length:
            return ProcessorResult(status=OpStatus.SKIPPED, message="Name too short")

        analysis = self._analyze_name(name)
        signals = analysis["signals"]
        if not signals:
            return ProcessorResult(
                status=OpStatus.COMPLETED, message="No signals from analysis"
            )
        confidence = min(0.9, 0.4 + 0.1 * len(signals))
        metadata = make_metadata(
            WARNING_NAME_READABILITY,
            analysis,
            self.actor.id,
            confidence=confidence,
        )
        return ProcessorResult(metadata=[metadata])

    def _resolve_file_name(self, asset: Asset) -> str | None:
        if self.database:
            entries = self.database.get_metadata_for_file(
                asset.id,
                actor_id=self.actor.id,
                metadata_key=FILE_NAME,
            )
            if entries:
                latest = entries[0].value
                if isinstance(latest, str) and latest:
                    return latest
        return Path(asset.canonical_uri).name or asset.canonical_uri

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
