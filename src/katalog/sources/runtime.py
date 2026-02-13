from __future__ import annotations

import time
from typing import cast

from loguru import logger

from katalog.constants.metadata import ASSET_LOST, DATA_FILE_READER
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.sql_helpers import select
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.models import (
    Actor,
    ActorType,
    Changeset,
    ChangesetStats,
    Metadata,
    MetadataChanges,
    make_metadata,
)
from katalog.models.core import OpStatus
from katalog.plugins.registry import get_actor_instance
from katalog.processors.runtime import process_asset, sort_processors
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin


async def run_sources(
    *,
    sources: list[Actor],
    changeset: Changeset,
    run_processors: bool = True,
    max_recursion_depth: int = 2,
) -> OpStatus:
    """Run source scans, optionally recurse into discovered assets, and persist results."""

    if run_processors:
        processor_pipeline, _processor_actors = await sort_processors()
    else:
        processor_pipeline = []
    has_processors = bool(processor_pipeline)

    final_status = OpStatus.COMPLETED
    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats

    scan_started = time.perf_counter()
    asset_repo = get_asset_repo()
    metadata_repo = get_metadata_repo()

    source_db = get_actor_repo()
    all_source_actors = await source_db.list_rows(
        order_by="id", type=ActorType.SOURCE, disabled=False
    )
    actor_by_id: dict[int, Actor] = {}
    plugin_by_actor_id: dict[int, SourcePlugin] = {}
    actor_has_metadata: dict[int, bool] = {}
    for source_actor in all_source_actors:
        if source_actor.id is None:
            continue
        actor_id = int(source_actor.id)
        actor_by_id[actor_id] = source_actor
        source_plugin = cast(SourcePlugin, await get_actor_instance(source_actor))
        ready, reason = await source_plugin.is_ready()
        if not ready:
            detail = reason or "unknown reason"
            raise RuntimeError(
                f"Source {source_actor.name} ({source_actor.plugin_id}) is not ready: {detail}"
            )
        plugin_by_actor_id[actor_id] = source_plugin
        async with session_scope() as session:
            rows = await select(
                session,
                f"SELECT 1 FROM {METADATA_TABLE} WHERE actor_id = ? LIMIT 1",
                [actor_id],
            )
            actor_has_metadata[actor_id] = bool(rows)

    seen_assets_by_actor: dict[int, set[int]] = {}
    recursion_visited: set[tuple[int, str, str]] = set()

    def _pick_recursive_source(changes: MetadataChanges) -> tuple[Actor, SourcePlugin] | None:
        candidates: list[tuple[int, int]] = []
        for actor_id, plugin in plugin_by_actor_id.items():
            score = int(plugin.can_recurse(changes) or 0)
            if score <= 0:
                continue
            candidates.append((score, actor_id))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        selected_actor_id = candidates[0][1]
        return actor_by_id[selected_actor_id], plugin_by_actor_id[selected_actor_id]

    async def _persist_scan_result(
        source_actor: Actor,
        result: AssetScanResult,
    ) -> tuple[list[Metadata], MetadataChanges]:
        if source_actor.id is None:
            raise ValueError("Source actor id is missing")

        stats.assets_seen += 1
        stats.assets_saved += 1

        was_created = await asset_repo.save_record(
            result.asset, changeset=changeset, actor=source_actor
        )
        if result.asset.id is not None:
            seen_assets_by_actor.setdefault(int(source_actor.id), set()).add(
                int(result.asset.id)
            )

        if was_created:
            stats.assets_added += 1
            result.asset._metadata_cache = []
            loaded_metadata: list[Metadata] = []
        else:
            loaded_metadata = list(
                await asset_repo.load_metadata(result.asset, include_removed=True)
            )

        staged_metadata = result.metadata + [
            make_metadata(ASSET_LOST, None, actor_id=result.actor.id),
            make_metadata(DATA_FILE_READER, {}, actor_id=result.actor.id),
        ]
        changes = MetadataChanges(
            asset=result.asset,
            loaded=loaded_metadata,
            staged=staged_metadata,
        )

        if has_processors:
            changeset.enqueue(
                process_asset(
                    changeset=changeset,
                    pipeline=processor_pipeline,
                    changes=changes,
                )
            )
        else:
            changed_keys = await metadata_repo.persist_changes(changes, changeset=changeset)
            if changed_keys:
                stats.assets_changed += 1

        return loaded_metadata, changes

    async def _scan_branch(
        *,
        source_actor: Actor,
        source_plugin: SourcePlugin,
        depth: int,
        seed_changes: MetadataChanges | None = None,
    ) -> OpStatus:
        if depth > max_recursion_depth:
            return OpStatus.COMPLETED

        if seed_changes is None:
            scan_result: ScanResult = await source_plugin.scan()
        else:
            scan_result = await source_plugin.scan_from_asset(seed_changes)

        if scan_result.ignored:
            stats.assets_seen += int(scan_result.ignored)
            stats.assets_ignored += int(scan_result.ignored)

        async for result in scan_result.iterator:
            loaded_metadata, _persisted_changes = await _persist_scan_result(
                source_actor, result
            )

            if depth >= max_recursion_depth:
                continue

            recurse_changes = MetadataChanges(
                asset=result.asset,
                loaded=loaded_metadata,
                staged=result.metadata,
            )
            picked = _pick_recursive_source(recurse_changes)
            if picked is None:
                continue
            recurse_actor, recurse_plugin = picked
            if recurse_actor.id is None:
                continue
            recurse_key = (
                int(recurse_actor.id),
                str(result.asset.namespace),
                str(result.asset.external_id),
            )
            if recurse_key in recursion_visited:
                continue
            recursion_visited.add(recurse_key)
            await _scan_branch(
                source_actor=recurse_actor,
                source_plugin=recurse_plugin,
                depth=depth + 1,
                seed_changes=recurse_changes,
            )

        return scan_result.status

    for source in sources:
        if source.id is None:
            raise ValueError("Source actor is missing id")
        if source.type != ActorType.SOURCE:
            logger.warning("Skipping actor {} ({}): not a source", source.id, source.name)
            continue
        if source.disabled:
            logger.info(
                "Skipping actor {actor_id}:{actor_name} (disabled)",
                actor_id=source.id,
                actor_name=source.name,
            )
            continue

        source_plugin = plugin_by_actor_id.get(int(source.id))
        if source_plugin is None:
            source_plugin = cast(SourcePlugin, await get_actor_instance(source))
        status = await _scan_branch(
            source_actor=source,
            source_plugin=source_plugin,
            depth=0,
            seed_changes=None,
        )
        if len(sources) == 1:
            final_status = status

    for actor_id, seen_asset_ids in seen_assets_by_actor.items():
        if not actor_has_metadata.get(actor_id):
            continue
        lost_count = await asset_repo.mark_unseen_as_lost(
            changeset=changeset,
            actor_ids=[actor_id],
            seen_asset_ids=list(seen_asset_ids),
        )
        if lost_count:
            stats.assets_lost += lost_count
            stats.assets_changed += lost_count

    scan_finished = time.perf_counter()
    data_payload = dict(changeset.data or {})
    data_payload["scan_metrics"] = {
        "scan_seconds": scan_finished - scan_started,
        "assets_seen": stats.assets_seen,
        "assets_saved": stats.assets_saved,
        "assets_added": stats.assets_added,
        "assets_changed": stats.assets_changed,
        "assets_ignored": stats.assets_ignored,
        "assets_lost": stats.assets_lost,
    }
    changeset.data = data_payload

    return final_status
