from fastapi import APIRouter, Query

from katalog.analyzers.base import AnalyzerScope
from katalog.analyzers.runtime import do_run_analyzer
from katalog.models import Actor, ActorType, Changeset, OpStatus
from katalog.db.asset_collections import get_asset_collection_repo
from katalog.db.assets import get_asset_repo
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.processors.runtime import do_run_processors, sort_processors
from katalog.sources.runtime import run_sources

from katalog.api.state import RUNNING_CHANGESETS
from katalog.api.helpers import ApiError

router = APIRouter()


async def run_source(source_id: int) -> Changeset:
    """Scan a single source and run processors for changed assets."""

    db = get_actor_repo()
    source = await db.get_or_none(id=source_id, type=ActorType.SOURCE)
    if source is None:
        raise ApiError(status_code=404, detail="Source not found")
    if source.disabled:
        raise ApiError(status_code=409, detail="Source is disabled")

    # Single-source scans map 1:1 to a changeset. Processor actors may still participate downstream.
    sources = [source]

    db = get_changeset_repo()
    changeset = await db.begin(
        message="Source scan", actors=sources, status=OpStatus.IN_PROGRESS
    )

    RUNNING_CHANGESETS[changeset.id] = changeset
    changeset.start_operation(lambda: run_sources(changeset=changeset, sources=sources))

    return changeset


async def run_processors(
    processor_ids: list[int] | None,
    asset_ids: list[int] | None,
) -> Changeset:
    processor_pipeline, processor_actors = await sort_processors(processor_ids)
    if not processor_pipeline:
        raise ApiError(status_code=400, detail="No processor actors configured")

    if asset_ids:
        db = get_asset_repo()
        assets = await db.list_rows(id__in=sorted(asset_ids))
    else:
        db = get_asset_repo()
        assets = await db.list_rows()
    if asset_ids and len(assets) != len(asset_ids):
        raise ApiError(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise ApiError(status_code=404, detail="No assets found to process")

    db = get_changeset_repo()
    changeset = await db.begin(
        message="Processor run", actors=processor_actors, status=OpStatus.IN_PROGRESS
    )
    RUNNING_CHANGESETS[changeset.id] = changeset

    changeset.start_operation(
        lambda: do_run_processors(
            changeset=changeset, assets=assets, pipeline=processor_pipeline
        )
    )

    return changeset


async def run_analyzer(
    analyzer_id: str,
    *,
    asset_id: int | None = None,
    collection_id: int | None = None,
) -> Changeset:
    """Run a specific analyzer actor id, return started changeset."""

    try:
        target_id = int(analyzer_id)
    except ValueError:
        raise ApiError(
            status_code=400,
            detail="analyzer_id must be an integer actor id",
        )

    db = get_actor_repo()
    actor = await db.get_or_none(id=target_id, type=ActorType.ANALYZER)
    if actor is None or actor.disabled:
        raise ApiError(status_code=404, detail="Analyzer actor not found or disabled")

    if asset_id is not None and collection_id is not None:
        raise ApiError(
            status_code=400,
            detail="asset_id and collection_id cannot be provided together",
        )

    if asset_id is not None:
        db = get_asset_repo()
        asset = await db.get_or_none(id=asset_id)
        if asset is None:
            raise ApiError(status_code=404, detail="Asset not found")
        scope = AnalyzerScope.asset(asset_id=int(asset_id))
    elif collection_id is not None:
        db = get_asset_collection_repo()
        collection = await db.get_or_none(id=collection_id)
        if collection is None:
            raise ApiError(status_code=404, detail="Collection not found")
        if collection.membership_key_id is None:
            raise ApiError(
                status_code=400,
                detail="Collection missing membership_key_id",
            )
        scope = AnalyzerScope.collection(
            int(collection_id), key_id=int(collection.membership_key_id)
        )
    else:
        scope = AnalyzerScope.all()
    db = get_changeset_repo()
    changeset = await db.begin(
        message=f"Run analyzer {actor.name or actor.id}",
        actors=[actor],
        status=OpStatus.IN_PROGRESS,
    )
    RUNNING_CHANGESETS[changeset.id] = changeset

    changeset.start_operation(
        lambda: do_run_analyzer(actor, changeset=changeset, scope=scope)
    )
    return changeset


@router.post("/sources/{source_id}/run")
async def run_source_rest(source_id: int):
    return await run_source(source_id)


@router.post("/processors/run")
async def run_processors_rest(
    processor_ids: list[int] | None = Query(None),
    asset_ids: list[int] | None = Query(None),
):
    return await run_processors(processor_ids=processor_ids, asset_ids=asset_ids)


@router.post("/analyzers/{analyzer_id}/run")
async def run_analyzer_rest(
    analyzer_id: str,
    asset_id: int | None = Query(None),
    collection_id: int | None = Query(None),
):
    return await run_analyzer(
        analyzer_id,
        asset_id=asset_id,
        collection_id=collection_id,
    )
