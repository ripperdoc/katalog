from __future__ import annotations

from typing import Any

from loguru import logger

from katalog.analyzers.base import Analyzer, AnalyzerIssue, AnalyzerResult, AnalyzerScope
from katalog.constants.metadata import (
    FILE_NAME,
    FILE_PATH,
    REL_LINK_TO,
    SIDECAR_TARGET_NAME,
    SIDECAR_TYPE,
    get_metadata_id,
)
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import select
from katalog.db.sqlspec.tables import ASSET_TABLE, METADATA_TABLE
from katalog.models import Changeset, make_metadata


class SidecarLinksAnalyzer(Analyzer):
    plugin_id = "katalog.analyzers.sidecar_links.SidecarLinksAnalyzer"
    title = "Sidecar linker"
    description = "Link sidecar assets to their target assets."
    output_kind = "sidecar_links"
    supports_single_asset = False

    def should_run(self, *, changeset: Changeset) -> bool:
        _ = changeset
        return True

    async def run(
        self, *, changeset: Changeset, scope: AnalyzerScope
    ) -> AnalyzerResult:
        if scope.kind == "asset":
            raise ValueError("SidecarLinksAnalyzer does not support single-asset scope")

        if self.actor.id is None:
            raise ValueError("Analyzer actor id is missing")

        sidecar_rows = await self._load_sidecar_rows()
        if not sidecar_rows:
            return AnalyzerResult(output={"linked": 0, "unresolved": 0})

        targets = await self._load_target_rows()
        target_by_key: dict[str, int] = {}
        for row in targets:
            asset_id = int(row["asset_id"])
            for key in _candidate_keys(path=row.get("path"), name=row.get("name")):
                target_by_key.setdefault(key, asset_id)

        metadata = []
        unresolved: list[AnalyzerIssue] = []
        linked = 0
        for row in sidecar_rows:
            sidecar_asset_id = int(row["asset_id"])
            target_name = str(row.get("target_name") or "").strip()
            found_target_id: int | None = None
            for key in _candidate_keys(path=target_name, name=target_name):
                match = target_by_key.get(key)
                if match is not None and match != sidecar_asset_id:
                    found_target_id = match
                    break
            if found_target_id is None:
                unresolved.append(
                    AnalyzerIssue(
                        level="warning",
                        message=f"Unresolved sidecar target: {target_name or 'unknown'}",
                        extra={"sidecar_asset_id": sidecar_asset_id},
                    )
                )
                continue

            linked += 1
            metadata.append(
                make_metadata(
                    REL_LINK_TO,
                    found_target_id,
                    actor_id=int(self.actor.id),
                    asset_id=sidecar_asset_id,
                )
            )

        logger.info(
            "Sidecar linker done linked={linked} unresolved={unresolved}",
            linked=linked,
            unresolved=len(unresolved),
        )
        return AnalyzerResult(
            metadata=metadata,
            issues=unresolved,
            output={"linked": linked, "unresolved": len(unresolved)},
        )

    async def _load_sidecar_rows(self) -> list[dict[str, Any]]:
        type_key_id = int(get_metadata_id(SIDECAR_TYPE))
        target_key_id = int(get_metadata_id(SIDECAR_TARGET_NAME))
        name_key_id = int(get_metadata_id(FILE_NAME))
        path_key_id = int(get_metadata_id(FILE_PATH))
        sql = f"""
            WITH latest AS (
                SELECT
                    m.asset_id,
                    m.metadata_key_id,
                    m.value_type,
                    m.value_text,
                    m.value_int,
                    m.value_real,
                    m.value_datetime,
                    m.value_json,
                    m.value_relation_id,
                    m.value_collection_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id, m.metadata_key_id
                        ORDER BY m.changeset_id DESC, m.id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                WHERE m.metadata_key_id IN (?, ?, ?, ?)
                  AND m.removed = 0
            )
            SELECT
                t.asset_id,
                t.value_text AS sidecar_type,
                tn.value_text AS target_name,
                fn.value_text AS name,
                fp.value_text AS path
            FROM latest t
            JOIN latest tn
              ON tn.asset_id = t.asset_id
             AND tn.metadata_key_id = ?
             AND tn.rn = 1
            LEFT JOIN latest fn
              ON fn.asset_id = t.asset_id
             AND fn.metadata_key_id = ?
             AND fn.rn = 1
            LEFT JOIN latest fp
              ON fp.asset_id = t.asset_id
             AND fp.metadata_key_id = ?
             AND fp.rn = 1
            WHERE t.metadata_key_id = ?
              AND t.rn = 1
        """
        params = [
            type_key_id,
            target_key_id,
            name_key_id,
            path_key_id,
            target_key_id,
            name_key_id,
            path_key_id,
            type_key_id,
        ]
        async with session_scope(analysis=True) as session:
            rows = await select(session, sql, params)
        return [dict(row) for row in rows]

    async def _load_target_rows(self) -> list[dict[str, Any]]:
        name_key_id = int(get_metadata_id(FILE_NAME))
        path_key_id = int(get_metadata_id(FILE_PATH))
        type_key_id = int(get_metadata_id(SIDECAR_TYPE))
        sql = f"""
            WITH latest AS (
                SELECT
                    m.asset_id,
                    m.metadata_key_id,
                    m.value_type,
                    m.value_text,
                    m.value_int,
                    m.value_real,
                    m.value_datetime,
                    m.value_json,
                    m.value_relation_id,
                    m.value_collection_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.asset_id, m.metadata_key_id
                        ORDER BY m.changeset_id DESC, m.id DESC
                    ) AS rn
                FROM {METADATA_TABLE} m
                WHERE m.metadata_key_id IN (?, ?, ?)
            )
            SELECT
                a.id AS asset_id,
                fn.value_text AS name,
                fp.value_text AS path
            FROM {ASSET_TABLE} a
            LEFT JOIN latest fn
              ON fn.asset_id = a.id
             AND fn.metadata_key_id = ?
             AND fn.rn = 1
             AND fn.value_text IS NOT NULL
            LEFT JOIN latest fp
              ON fp.asset_id = a.id
             AND fp.metadata_key_id = ?
             AND fp.rn = 1
             AND fp.value_text IS NOT NULL
            LEFT JOIN latest st
              ON st.asset_id = a.id
             AND st.metadata_key_id = ?
             AND st.rn = 1
             AND st.value_text IS NOT NULL
            WHERE st.value_text IS NULL
        """
        params = [
            name_key_id,
            path_key_id,
            type_key_id,
            name_key_id,
            path_key_id,
            type_key_id,
        ]
        async with session_scope(analysis=True) as session:
            rows = await select(session, sql, params)
        return [dict(row) for row in rows]


def _candidate_keys(*, path: str | None, name: str | None) -> list[str]:
    items: list[str] = []
    if isinstance(path, str) and path.strip():
        value = path.strip().replace("\\", "/")
        items.append(value.lower())
        items.append(value.split("/")[-1].lower())
    if isinstance(name, str) and name.strip():
        items.append(name.strip().lower())
    out: list[str] = []
    for item in items:
        if item not in out:
            out.append(item)
    return out
