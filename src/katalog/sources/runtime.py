from __future__ import annotations
from loguru import logger
from typing import cast
from katalog.db.sqlspec.sql_helpers import execute
from katalog.db.sqlspec import session_scope

from katalog.models import (
    MetadataChanges,
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


async def _persist_scan_only_item(
    *,
    item: AssetScanResult,
    changeset: Changeset,
    seen_assets: set[int],
    session,
) -> None:
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
        loaded_metadata = []
    else:
        loaded_metadata = await db.load_metadata(
            item.asset, include_removed=True, session=session
        )

    # Mark asset as seen in this changeset for this actor.
    item.metadata.append(make_metadata(ASSET_LOST, None, actor_id=item.actor.id))

    changes = MetadataChanges(loaded=loaded_metadata, staged=item.metadata)
    md_db = get_metadata_repo()
    changes = await md_db.persist_changes(
        changes, asset=item.asset, changeset=changeset, session=session
    )
    if changes:
        stats.assets_changed += 1


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
            for item in batch:
                await _persist_scan_only_item(
                    item=item,
                    changeset=changeset,
                    seen_assets=seen_assets,
                    session=session,
                )
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
) -> OpStatus:
    """Run a scan + processor pipeline for a single source and finalize its changeset."""

    processor_pipeline, processor_actors = await sort_processors()
    has_processors = bool(processor_pipeline)

    # For scan-only runs (no processors), we can safely batch persistence into fewer commits.
    # This tends to be a big speed-up with SQLite/aiosqlite.
    tx_chunk_size = 500
    log_every_assets = 5000

    final_status = OpStatus.COMPLETED

    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats

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
        scan_result = await source_plugin.scan()

        persisted_assets = 0
        seen_assets: set[int] = set()

        pending: list[AssetScanResult] = []

        async for result in scan_result.iterator:
            stats.assets_seen += 1
            stats.assets_saved += 1

            if has_processors:
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
                    loaded=loaded_metadata,
                    staged=result.metadata
                    + [
                        make_metadata(ASSET_LOST, None, actor_id=result.actor.id),
                        make_metadata(DATA_FILE_READER, {}, actor_id=result.actor.id),
                    ],
                )
                # Enqueue asset for processing, which will also persist the metadata
                changeset.enqueue(
                    process_asset(
                        asset=result.asset,
                        changeset=changeset,
                        pipeline=processor_pipeline,
                        changes=changes,
                    )
                )
                if result.asset.id is not None:
                    seen_assets.add(int(result.asset.id))
            else:
                pending.append(result)
                if len(pending) >= tx_chunk_size:
                    batch_size = len(pending)
                    await _flush_scan_only_batch(
                        batch=pending, changeset=changeset, seen_assets=seen_assets
                    )
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
            batch_size = len(pending)
            await _flush_scan_only_batch(
                batch=pending, changeset=changeset, seen_assets=seen_assets
            )
            persisted_assets += batch_size

            # Final progress log for scan-only mode.
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

    return final_status
