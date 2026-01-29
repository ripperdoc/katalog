from fastapi import APIRouter, Query

from katalog.analyzers.base import AnalyzerScope
from katalog.analyzers.runtime import run_analyzer
from katalog.models import Actor, ActorType, Asset, AssetCollection, Changeset, OpStatus
from katalog.processors.runtime import run_processors, sort_processors
from katalog.sources.runtime import run_sources

from katalog.api.state import RUNNING_CHANGESETS
from katalog.api.helpers import ApiError

router = APIRouter()


async def run_sources_api(source_id: int) -> dict:
    """Scan a single source and run processors for changed assets."""

    source = await Actor.get_or_none(id=source_id, type=ActorType.SOURCE)
    if source is None:
        raise ApiError(status_code=404, detail="Source not found")
    if source.disabled:
        raise ApiError(status_code=409, detail="Source is disabled")

    # Single-source scans map 1:1 to a changeset. Processor actors may still participate downstream.
    sources = [source]

    changeset = await Changeset.begin(
        message="Source scan", actors=sources, status=OpStatus.IN_PROGRESS
    )

    RUNNING_CHANGESETS[changeset.id] = changeset
    changeset.start_operation(lambda: run_sources(changeset=changeset, sources=sources))

    return changeset.to_dict()


async def run_processors_api(
    processor_ids: list[int] | None,
    asset_ids: list[int] | None,
) -> dict:
    processor_pipeline, processor_actors = await sort_processors(processor_ids)
    if not processor_pipeline:
        raise ApiError(status_code=400, detail="No processor actors configured")

    assets_query = Asset.all()
    if asset_ids:
        assets_query = assets_query.filter(id__in=sorted(asset_ids))
    assets = await assets_query
    if asset_ids and len(assets) != len(asset_ids):
        raise ApiError(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise ApiError(status_code=404, detail="No assets found to process")

    changeset = await Changeset.begin(
        message="Processor run", actors=processor_actors, status=OpStatus.IN_PROGRESS
    )
    RUNNING_CHANGESETS[changeset.id] = changeset

    changeset.start_operation(
        lambda: run_processors(
            changeset=changeset, assets=assets, pipeline=processor_pipeline
        )
    )

    return changeset.to_dict()


async def run_analyzer_api(
    analyzer_id: str,
    *,
    asset_id: int | None = None,
    collection_id: int | None = None,
) -> dict:
    """Run a specific analyzer actor id, return started changeset."""

    try:
        target_id = int(analyzer_id)
    except ValueError:
        raise ApiError(
            status_code=400,
            detail="analyzer_id must be an integer actor id",
        )

    actor = await Actor.get_or_none(id=target_id, type=ActorType.ANALYZER)
    if actor is None or actor.disabled:
        raise ApiError(status_code=404, detail="Analyzer actor not found or disabled")

    if asset_id is not None and collection_id is not None:
        raise ApiError(
            status_code=400,
            detail="asset_id and collection_id cannot be provided together",
        )

    if asset_id is not None:
        asset = await Asset.get_or_none(id=asset_id)
        if asset is None:
            raise ApiError(status_code=404, detail="Asset not found")
        scope = AnalyzerScope.asset(asset_id=int(asset_id))
    elif collection_id is not None:
        collection = await AssetCollection.get_or_none(id=collection_id)
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
    changeset = await Changeset.begin(
        message=f"Run analyzer {actor.name or actor.id}",
        actors=[actor],
        status=OpStatus.IN_PROGRESS,
    )
    RUNNING_CHANGESETS[changeset.id] = changeset

    changeset.start_operation(
        lambda: run_analyzer(actor, changeset=changeset, scope=scope)
    )
    return changeset.to_dict()


@router.post("/sources/{source_id}/run")
async def do_run_sources(source_id: int):
    return await run_sources_api(source_id)


@router.post("/processors/run")
async def do_run_processors(
    processor_ids: list[int] | None = Query(None),
    asset_ids: list[int] | None = Query(None),
):
    return await run_processors_api(processor_ids=processor_ids, asset_ids=asset_ids)


@router.post("/analyzers/{analyzer_id}/run")
async def do_run_analyzers(
    analyzer_id: str,
    asset_id: int | None = Query(None),
    collection_id: int | None = Query(None),
):
    return await run_analyzer_api(
        analyzer_id,
        asset_id=asset_id,
        collection_id=collection_id,
    )
