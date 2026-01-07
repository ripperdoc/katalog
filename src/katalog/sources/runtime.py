from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from loguru import logger

from katalog.models import (
    Asset,
    MetadataChangeSet,
    Provider,
    ProviderType,
    Snapshot,
)
from katalog.processors.runtime import enqueue_asset_processing, sort_processors

from katalog.sources.base import SourcePlugin, make_source_instance


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
        async for result in scan_result.iterator:
            if await _should_cancel():
                raise asyncio.CancelledError()
            snapshot.stats.assets_seen += 1
            snapshot.stats.assets_saved += 1
            # Ensure asset row exists and snapshot markers are updated.
            await result.asset.save_record(snapshot=snapshot)

            loaded_metadata = await result.asset.load_metadata()
            change_set = MetadataChangeSet(
                loaded=loaded_metadata, staged=result.metadata
            )
            if processor_pipeline:
                # Enqueue asset for processing, which will also persist the metadata
                await enqueue_asset_processing(
                    asset=result.asset,
                    snapshot=snapshot,
                    stages=processor_pipeline,
                    change_set=change_set,
                )
            else:
                # Save only metadata from the source scan.
                changes = await change_set.persist(
                    asset=result.asset, snapshot=snapshot
                )
                if changes:
                    snapshot.stats.assets_changed += 1
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
