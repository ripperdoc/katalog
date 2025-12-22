from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional

from loguru import logger

from katalog.models import OpStatus, Provider, ProviderType, Snapshot
from katalog.processors.runtime import (
    DEFAULT_PROCESSOR_CONCURRENCY,
    enqueue_asset_processing,
)
from katalog.sources.base import make_source_instance


async def run_source_snapshot(
    *,
    source_record: Provider,
    processor_pipeline,
    since_snapshot: Snapshot | None = None,
    max_concurrency: int = DEFAULT_PROCESSOR_CONCURRENCY,
    is_cancelled: Callable[[], Awaitable[bool]] | None = None,
) -> Snapshot:
    """Run a scan + processor pipeline for a single source and finalize its snapshot."""
    if source_record.type != ProviderType.SOURCE:
        raise ValueError(f"Provider {source_record.id} is not a source")

    source_plugin = make_source_instance(source_record)
    snapshot = await Snapshot.begin(source_record)
    processor_semaphore = asyncio.Semaphore(max_concurrency)
    scan_result = None
    status: OpStatus = OpStatus.ERROR
    cancelled = False
    try:
        scan_result = await source_plugin.scan(since_snapshot=since_snapshot)
        async for result in scan_result.iterator:
            snapshot.stats.assets_seen += 1
            changes = await result.asset.upsert(
                snapshot=snapshot, metadata=result.metadata
            )

            if processor_pipeline:
                snapshot.stats.assets_processed += 1
                await enqueue_asset_processing(
                    asset=result.asset,
                    snapshot=snapshot,
                    stages=processor_pipeline,
                    tasks=snapshot.tasks,
                    semaphore=processor_semaphore,
                    initial_changes=changes,
                )
            if is_cancelled and await is_cancelled():
                cancelled = True
                break
        status = scan_result.status if scan_result else OpStatus.ERROR
    except asyncio.CancelledError:
        logger.info(f"Snapshot {snapshot.id} for source {source_record} canceled by client (CancelledError)")
        cancelled = True
        raise
    except Exception:
        raise
    finally:
        if cancelled:
            logger.info(
                f"Snapshot {snapshot.id} for source {source_record} canceled by client"
            )
            status = OpStatus.CANCELED
            for task in snapshot.tasks:
                task.cancel()
        await snapshot.finalize(status=status)
    return snapshot
