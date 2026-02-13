from __future__ import annotations
from loguru import logger
import time
from typing import cast
from katalog.db.sqlspec.sql_helpers import execute, select
from katalog.db.sqlspec import session_scope
from katalog.db.sqlspec.tables import METADATA_TABLE

from katalog.models import (
    MetadataChanges,
    Metadata,
    make_metadata,
    Actor,
    ActorType,
    Changeset,
    ChangesetStats,
)
from katalog.models.core import OpStatus
from katalog.processors.runtime import process_asset, sort_processors

from katalog.sources.base import AssetScanResult, SourcePlugin
from katalog.plugins.registry import get_actor_instance
from katalog.constants.metadata import ASSET_LOST
from katalog.constants.metadata import DATA_FILE_READER
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.runtime.batch import get_batch_size, iter_batches_async


async def _flush_scan_only_batch(
    *,
    batch: list[AssetScanResult],
    changeset: Changeset,
    seen_assets: set[int],
) -> None:
    if not batch:
        return
    async with session_scope() as session:
        await execute(session, "BEGIN")
        try:
            all_metadata: list[Metadata] = []
            for item in batch:
                if changeset.stats is None:
                    changeset.stats = ChangesetStats()
                stats = changeset.stats
                # Ensure asset row exists and changeset markers are updated.
                db = get_asset_repo()
                was_created = await db.save_record(
                    item.asset, changeset=changeset, actor=item.actor, session=session
                )
                if item.asset.id is not None:
                    seen_assets.add(int(item.asset.id))
                if was_created:
                    stats.assets_added += 1
                    # Newly created assets cannot have existing metadata.
                    item.asset._metadata_cache = []
                    loaded_metadata: list[Metadata] = []
                else:
                    loaded_metadata = list(
                        await db.load_metadata(
                            item.asset, include_removed=True, session=session
                        )
                    )

                item.metadata.append(
                    make_metadata(ASSET_LOST, None, actor_id=item.actor.id)
                )
                changes = MetadataChanges(
                    asset=item.asset, loaded=loaded_metadata, staged=item.metadata
                )
                to_create, _changed = changes.prepare_persist(
                    changeset=changeset,
                    existing_metadata=loaded_metadata,
                )
                if to_create:
                    stats.assets_changed += 1
                if to_create:
                    all_metadata.extend(to_create)

            if all_metadata:
                md_db = get_metadata_repo()
                await md_db.bulk_create(all_metadata, session=session)
            await execute(session, "COMMIT")
        except Exception:
            await execute(session, "ROLLBACK")
            raise
    batch.clear()


def get_source_plugin(actor_id: int) -> SourcePlugin:
    """Retrieve a source actor by its ID, ensuring it is of type SOURCE."""
    raise NotImplementedError("get_source_actor is not yet implemented")
    # actor = Actor.get_or_none(id=actor_id)
    # if not actor or actor.type != ActorType.SOURCE:
    #     raise ValueError(f"Actor with ID {actor_id} not found or not a source")
    # return actor


async def run_sources(
    *,
    sources: list[Actor],
    changeset: Changeset,
    run_processors: bool = True,
) -> OpStatus:
    """Run a scan + processor pipeline for a single source and finalize its changeset."""

    if run_processors:
        processor_pipeline, processor_actors = await sort_processors()
    else:
        processor_pipeline, processor_actors = [], []
    has_processors = bool(processor_pipeline)

    # For scan-only runs (no processors), we can safely batch persistence into fewer commits.
    # This tends to be a big speed-up with SQLite/aiosqlite.
    tx_chunk_size = get_batch_size()
    log_every_assets = 5000

    final_status = OpStatus.COMPLETED

    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats

    persist_time_s = 0.0
    persist_batches = 0
    first_persist_start: float | None = None
    scan_started = time.perf_counter()
    scan_iter_finished: float | None = None

    for source in sources:
        if source.id is None:
            raise ValueError("Source actor is missing id")
        if source.type != ActorType.SOURCE:
            logger.warning(f"Skipping actor {source.id} ({source.name}): not a source")
            continue
        if source.disabled:
            logger.info(
                "Skipping actor {actor_id}:{actor_name} (disabled)",
                actor_id=source.id,
                actor_name=source.name,
            )
            continue

        source_plugin = cast(SourcePlugin, await get_actor_instance(source))
        existing_actor_metadata = False
        async with session_scope() as session:
            rows = await select(
                session,
                f"SELECT 1 FROM {METADATA_TABLE} WHERE actor_id = ? LIMIT 1",
                [int(source.id)],
            )
            existing_actor_metadata = bool(rows)
        scan_result = await source_plugin.scan()

        persisted_assets = 0
        seen_assets: set[int] = set()

        if has_processors:
            async for result in scan_result.iterator:
                stats.assets_seen += 1
                stats.assets_saved += 1
                db = get_asset_repo()
                was_created = await db.save_record(
                    result.asset, changeset=changeset, actor=source
                )
                if was_created:
                    stats.assets_added += 1
                    result.asset._metadata_cache = []
                    loaded_metadata = []
                else:
                    loaded_metadata = await db.load_metadata(result.asset)
                changes = MetadataChanges(
                    asset=result.asset,
                    loaded=loaded_metadata,
                    staged=result.metadata
                    + [
                        make_metadata(ASSET_LOST, None, actor_id=result.actor.id),
                        make_metadata(DATA_FILE_READER, {}, actor_id=result.actor.id),
                    ],
                )
                changeset.enqueue(
                    process_asset(
                        changeset=changeset,
                        pipeline=processor_pipeline,
                        changes=changes,
                    )
                )
                if result.asset.id is not None:
                    seen_assets.add(int(result.asset.id))
        else:
            async for batch in iter_batches_async(
                scan_result.iterator, tx_chunk_size
            ):
                for result in batch:
                    stats.assets_seen += 1
                    stats.assets_saved += 1
                pending = list(batch)
                batch_size = len(pending)
                if batch_size:
                    if first_persist_start is None:
                        first_persist_start = time.perf_counter()
                    persist_started = time.perf_counter()
                    await _flush_scan_only_batch(
                        batch=pending, changeset=changeset, seen_assets=seen_assets
                    )
                    persist_time_s += time.perf_counter() - persist_started
                    persist_batches += 1
                    persisted_assets += batch_size

                    if persisted_assets % log_every_assets == 0:
                        logger.info(
                            "Persisted {persisted_assets} scan results for {source} (changed={changed}, added={added})",
                            persisted_assets=persisted_assets,
                            source=f"{source.id}:{source.name}",
                            changed=stats.assets_changed,
                            added=stats.assets_added,
                        )

        if not has_processors:
            scan_iter_finished = time.perf_counter()
            logger.info(
                "Finished persisting scan results for {source} (persisted={persisted}, changed={changed}, added={added})",
                source=f"{source.id}:{source.name}",
                persisted=persisted_assets,
                changed=stats.assets_changed,
                added=stats.assets_added,
            )
        else:
            # In processor mode, DB activity continues in background tasks after the scan iterator ends.
            if changeset.tasks:
                logger.info(
                    "Scan finished for {source}; {tasks} processor tasks queued",
                    source=f"{source.id}:{source.name}",
                    tasks=len(changeset.tasks),
                )
        if existing_actor_metadata:
            db = get_asset_repo()
            lost_count = await db.mark_unseen_as_lost(
                changeset=changeset,
                actor_ids=[int(source.id)],
                seen_asset_ids=list(seen_assets),
            )
            if lost_count:
                stats.assets_lost += lost_count
                stats.assets_changed += lost_count
        ignored = scan_result.ignored
        if ignored:
            stats.assets_seen += ignored
            stats.assets_ignored += ignored
        if len(sources) == 1:
            # Assume the changeset status is that of the single source
            final_status = scan_result.status

        if not has_processors:
            scan_finished = time.perf_counter()
            persist_first_delay_s = (
                first_persist_start - scan_started
                if first_persist_start is not None
                else None
            )
            persist_after_scan_s = (
                scan_finished - scan_iter_finished
                if scan_iter_finished is not None
                else None
            )
            data_payload = dict(changeset.data or {})
            data_payload["scan_metrics"] = {
                "scan_seconds": scan_finished - scan_started,
                "persist_seconds": persist_time_s,
                "persist_batches": persist_batches,
                "persist_first_delay_seconds": persist_first_delay_s,
                "persist_after_scan_seconds": persist_after_scan_s,
                "assets_seen": stats.assets_seen,
                "assets_saved": stats.assets_saved,
                "assets_added": stats.assets_added,
                "assets_changed": stats.assets_changed,
            }
            changeset.data = data_payload

    return final_status
