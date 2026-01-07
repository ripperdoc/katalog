from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from loguru import logger
from tortoise.transactions import in_transaction

from katalog.models import (
    Asset,
    MetadataChangeSet,
    Provider,
    ProviderType,
    Snapshot,
)
from katalog.processors.runtime import enqueue_asset_processing, sort_processors

from katalog.sources.base import AssetScanResult, SourcePlugin, make_source_instance


async def _persist_scan_only_item(
    *,
    item: AssetScanResult,
    snapshot: Snapshot,
) -> None:
    # Ensure asset row exists and snapshot markers are updated.
    was_created = await item.asset.save_record(snapshot=snapshot)
    if was_created:
        snapshot.stats.assets_added += 1
        # Newly created assets cannot have existing metadata.
        item.asset._metadata_cache = []
        loaded_metadata = []
    else:
        loaded_metadata = await item.asset.load_metadata()

    change_set = MetadataChangeSet(loaded=loaded_metadata, staged=item.metadata)
    changes = await change_set.persist(asset=item.asset, snapshot=snapshot)
    if changes:
        snapshot.stats.assets_changed += 1


async def _flush_scan_only_batch(
    *,
    batch: list[AssetScanResult],
    snapshot: Snapshot,
) -> None:
    if not batch:
        return
    async with in_transaction():
        for item in batch:
            await _persist_scan_only_item(item=item, snapshot=snapshot)
    batch.clear()


def get_source_plugin(provider_id: int) -> SourcePlugin:
    """Retrieve a source provider by its ID, ensuring it is of type SOURCE."""
    raise NotImplementedError("get_source_provider is not yet implemented")
    # provider = Provider.get_or_none(id=provider_id)
    # if not provider or provider.type != ProviderType.SOURCE:
    #     raise ValueError(f"Provider with ID {provider_id} not found or not a source")
    # return provider


async def run_sources(
    *,
    sources: list[Provider],
    snapshot: Snapshot,
    is_cancelled: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    """Run a scan + processor pipeline for a single source and finalize its snapshot."""

    processor_pipeline = await sort_processors()
    has_processors = bool(processor_pipeline)

    # For scan-only runs (no processors), we can safely batch persistence into fewer commits.
    # This tends to be a big speed-up with SQLite/aiosqlite.
    tx_chunk_size = 500
    log_every_assets = 5000

    async def _should_cancel() -> bool:
        if is_cancelled is None:
            return False
        try:
            result = is_cancelled()
            if asyncio.iscoroutine(result):
                return await result
            return bool(result)
        except Exception:
            logger.exception("Cancellation predicate failed; continuing scan")
            return False

    for source in sources:
        if source.type != ProviderType.SOURCE:
            logger.warning(
                f"Skipping provider {source.id} ({source.name}): not a source"
            )
            continue
        if await _should_cancel():
            raise asyncio.CancelledError()
        source_plugin = make_source_instance(source)
        scan_result = await source_plugin.scan()

        persisted_assets = 0

        pending: list[AssetScanResult] = []

        async for result in scan_result.iterator:
            if await _should_cancel():
                raise asyncio.CancelledError()
            snapshot.stats.assets_seen += 1
            snapshot.stats.assets_saved += 1

            if has_processors:
                was_created = await result.asset.save_record(snapshot=snapshot)
                if was_created:
                    snapshot.stats.assets_added += 1
                    result.asset._metadata_cache = []
                    loaded_metadata = []
                else:
                    loaded_metadata = await result.asset.load_metadata()
                change_set = MetadataChangeSet(
                    loaded=loaded_metadata, staged=result.metadata
                )
                # Enqueue asset for processing, which will also persist the metadata
                await enqueue_asset_processing(
                    asset=result.asset,
                    snapshot=snapshot,
                    stages=processor_pipeline,
                    change_set=change_set,
                )
            else:
                pending.append(result)
                if len(pending) >= tx_chunk_size:
                    batch_size = len(pending)
                    await _flush_scan_only_batch(batch=pending, snapshot=snapshot)
                    persisted_assets += batch_size

                    if persisted_assets % log_every_assets == 0:
                        logger.info(
                            "Persisted {persisted_assets} scan results for {source} (changed={changed}, added={added})",
                            persisted_assets=persisted_assets,
                            source=f"{source.id}:{source.name}",
                            changed=snapshot.stats.assets_changed,
                            added=snapshot.stats.assets_added,
                        )

        if not has_processors:
            batch_size = len(pending)
            await _flush_scan_only_batch(batch=pending, snapshot=snapshot)
            persisted_assets += batch_size

            # Final progress log for scan-only mode.
            logger.info(
                "Finished persisting scan results for {source} (persisted={persisted}, changed={changed}, added={added})",
                source=f"{source.id}:{source.name}",
                persisted=persisted_assets,
                changed=snapshot.stats.assets_changed,
                added=snapshot.stats.assets_added,
            )
        else:
            # In processor mode, DB activity continues in background tasks after the scan iterator ends.
            if snapshot.tasks:
                logger.info(
                    "Scan finished for {source}; {tasks} processor tasks queued",
                    source=f"{source.id}:{source.name}",
                    tasks=len(snapshot.tasks),
                )
        deleted_count = await Asset.mark_unseen_as_deleted(
            snapshot=snapshot, provider_ids=[source.id]
        )
        if deleted_count:
            snapshot.stats.assets_deleted += deleted_count
            snapshot.stats.assets_changed += deleted_count
        ignored = scan_result.ignored
        if ignored:
            snapshot.stats.assets_seen += ignored
            snapshot.stats.assets_ignored += ignored
        if len(sources) == 1:
            # Assume the snapshot status is that of the single source
            # TODO this is currently overwritten by Snapshot.finalize
            snapshot.status = scan_result.status
