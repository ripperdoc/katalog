from __future__ import annotations

from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field
from tortoise import Tortoise

from katalog.analyzers.base import (
    Analyzer,
    AnalyzerIssue,
    AnalyzerResult,
    FileGroupFinding,
)
from katalog.models import Metadata, Provider, Changeset
from katalog.metadata import HASH_MD5, get_metadata_id


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

    def should_run(self, *, changeset: Changeset) -> bool:  # noqa: D401
        """Currently always runs; future versions may add change detection."""

        return True

    async def run(self, *, changeset: Changeset) -> AnalyzerResult:
        """Find duplicate assets by MD5 using SQL-only grouping."""

        md5_registry_id = get_metadata_id(HASH_MD5)
        max_groups = int(self.config.max_groups)
        conn = Tortoise.get_connection("default")

        # Step 1: detect assets with conflicting current MD5 values (different providers disagree).
        conflict_sql = """
        WITH latest_md5 AS (
            SELECT
                m.asset_id,
                m.provider_id,
                lower(trim(m.value_text)) AS md5,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id, m.provider_id
                    ORDER BY m.changeset_id DESC
                ) AS rn
            FROM metadata AS m
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        ),
        current_md5 AS (
            SELECT asset_id, provider_id, md5
            FROM latest_md5
            WHERE rn = 1 AND md5 != ''
        )
        SELECT asset_id, GROUP_CONCAT(DISTINCT md5) AS hashes
        FROM current_md5
        GROUP BY asset_id
        HAVING COUNT(DISTINCT md5) > 1
        """

        conflict_rows = await conn.execute_query_dict(conflict_sql, [md5_registry_id])

        issues: list[AnalyzerIssue] = []
        for row in conflict_rows:
            hashes = sorted((row.get("hashes") or "").split(","))
            message = (
                "Multiple current hash/md5 values for asset "
                f"{row['asset_id']}: {', '.join(hashes)}"
            )
            logger.error(message)
            issues.append(
                AnalyzerIssue(
                    level="error",
                    message=message,
                    file_ids=[str(row["asset_id"])],
                    extra={"hashes": hashes},
                )
            )

        # Step 2: group remaining assets by MD5 entirely in SQL, returning only duplicate sets.
        group_sql = """
        WITH latest_md5 AS (
            SELECT
                m.asset_id,
                m.provider_id,
                lower(trim(m.value_text)) AS md5,
                ROW_NUMBER() OVER (
                    PARTITION BY m.asset_id, m.provider_id
                    ORDER BY m.changeset_id DESC
                ) AS rn
            FROM metadata AS m
            WHERE m.metadata_key_id = ?
              AND m.removed = 0
              AND m.value_text IS NOT NULL
        ),
        current_md5 AS (
            SELECT asset_id, provider_id, md5
            FROM latest_md5
            WHERE rn = 1 AND md5 != ''
        ),
        deduped AS (
            SELECT asset_id, provider_id, md5
            FROM current_md5
            WHERE asset_id NOT IN (
                SELECT asset_id FROM (
                    SELECT asset_id
                    FROM current_md5
                    GROUP BY asset_id
                    HAVING COUNT(DISTINCT md5) > 1
                )
            )
        )
        SELECT
            md5,
            COUNT(*) AS file_count,
            GROUP_CONCAT(DISTINCT asset_id) AS asset_ids,
            GROUP_CONCAT(DISTINCT provider_id) AS provider_ids
        FROM deduped
        GROUP BY md5
        HAVING COUNT(*) > 1
        ORDER BY file_count DESC, md5
        LIMIT ?
        """

        rows = await conn.execute_query_dict(group_sql, [md5_registry_id, max_groups])

        groups: list[FileGroupFinding] = []
        for row in rows:
            asset_ids = sorted(
                int(a) for a in (row.get("asset_ids") or "").split(",") if a
            )
            provider_ids = sorted(
                {int(p) for p in (row.get("provider_ids") or "").split(",") if p}
            )
            md5_value = row.get("md5") or ""
            groups.append(
                FileGroupFinding(
                    kind="exact_duplicate",
                    label=md5_value,
                    file_ids=[str(a) for a in asset_ids],
                    attributes={
                        "hash": md5_value,
                        "file_count": int(row.get("file_count") or 0),
                        "asset_ids": asset_ids,
                        "provider_ids": provider_ids,
                    },
                )
            )

        # Relationships can be derived later when we decide how to persist duplicate groups.
        return AnalyzerResult(
            metadata=[],
            relationships=[],
            groups=groups,
            issues=issues,
        )

    @staticmethod
    def _extract_hash(metadata: Metadata) -> str | None:
        if metadata.value is None:
            return None
        text = str(metadata.value).strip()
        return text.lower() or None
