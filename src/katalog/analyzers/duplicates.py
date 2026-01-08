from __future__ import annotations

from collections import defaultdict
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from katalog.analyzers.base import (
    Analyzer,
    AnalyzerIssue,
    AnalyzerResult,
    FileGroupFinding,
    RelationshipRecord,
)
from katalog.models import Metadata, Provider, Snapshot
from katalog.metadata import HASH_MD5


class ExactDuplicateAnalyzer(Analyzer):
    """Groups files that share the same MD5 hash."""

    plugin_id = "katalog.analyzers.duplicates.ExactDuplicateAnalyzer"
    title = "Exact duplicates"
    description = "Group files that share identical MD5 checksums."
    dependencies = frozenset({HASH_MD5})
    outputs = frozenset()

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        max_groups: int = Field(
            default=5000,
            gt=0,
            description="Hard cap on number of duplicate groups emitted to avoid unbounded memory",
        )

    config_model = ConfigModel

    def __init__(self, provider: Provider, **config: Any) -> None:
        self.config = self.config_model.model_validate(config or {})
        super().__init__(provider, **config)

    def should_run(self, *, snapshot: Snapshot) -> bool:  # noqa: D401
        """Currently always runs; future versions may add change detection."""

        return True

    async def run(self, *, snapshot: Snapshot) -> AnalyzerResult:
        provider_id = getattr(self, "provider_id", snapshot.provider_id)
        latest_hash_rows = database.get_latest_metadata_by_key(HASH_MD5)
        per_file_hashes: dict[str, set[str]] = defaultdict(set)
        per_file_providers: dict[str, str] = {}
        issues: list[AnalyzerIssue] = []

        for file_id, metadata in latest_hash_rows:
            hash_value = self._extract_hash(metadata)
            if hash_value is None:
                continue
            per_file_hashes[file_id].add(hash_value)
            per_file_providers[file_id] = metadata.provider_id or ""

        confirmed_hashes: dict[str, str] = {}
        for file_id, hash_values in per_file_hashes.items():
            if len(hash_values) == 1:
                confirmed_hashes[file_id] = next(iter(hash_values))
                continue
            conflict = sorted(hash_values)
            message = f"Multiple current hash/md5 values for file {file_id}: {', '.join(conflict)}"
            logger.error(message)
            issues.append(
                AnalyzerIssue(
                    level="error",
                    message=message,
                    file_ids=[file_id],
                    extra={"hashes": conflict},
                )
            )

        groups: list[FileGroupFinding] = []
        relationships: list[RelationshipRecord] = []
        groups_by_hash: dict[str, list[str]] = defaultdict(list)

        for file_id, hash_value in confirmed_hashes.items():
            groups_by_hash[hash_value].append(file_id)

        for hash_value, file_ids in groups_by_hash.items():
            if len(file_ids) < 2:
                continue
            if len(groups) >= self.config.max_groups:
                logger.warning("Duplicate groups capped at max_groups=%s", self.config.max_groups)
                break
            sorted_members = sorted(file_ids)
            member_providers = sorted(
                {
                    provider_id
                    for provider_id in (
                        per_file_providers.get(fid) for fid in sorted_members
                    )
                    if provider_id
                }
            )
            groups.append(
                FileGroupFinding(
                    kind="exact_duplicate",
                    label=hash_value,
                    file_ids=list(sorted_members),
                    attributes={
                        "hash": hash_value,
                        "file_count": len(sorted_members),
                        "provider_ids": member_providers,
                    },
                )
            )
            anchor = sorted_members[0]
            for other in sorted_members[1:]:
                relationships.append(
                    RelationshipRecord(
                        from_id=anchor,
                        to_id=other,
                        relationship_type="exact_duplicate",
                        provider_id=provider_id,
                        confidence=1.0,
                        description=f"Exact duplicate group for hash {hash_value}",
                        attributes={"hash": hash_value},
                    )
                )

        return AnalyzerResult(
            metadata=[],
            relationships=relationships,
            groups=groups,
            issues=issues,
        )

    @staticmethod
    def _extract_hash(metadata: Metadata) -> str | None:
        if metadata.value is None:
            return None
        text = str(metadata.value).strip()
        return text.lower() or None
