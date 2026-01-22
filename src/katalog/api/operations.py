from fastapi import APIRouter, HTTPException, Query

from katalog.analyzers.runtime import run_analyzer
from katalog.models import Actor, ActorType, Asset, Changeset, OpStatus
from katalog.processors.runtime import run_processors, sort_processors
from katalog.sources.runtime import run_sources

from katalog.api.state import RUNNING_CHANGESETS

router = APIRouter()


@router.post("/sources/run")
async def do_run_sources(ids: list[int] | None = Query(None)):
    """Scan selected or all sources and run processors for changed assets."""
    target_ids = set(ids or [])

    if target_ids:
        sources = await Actor.filter(
            type=ActorType.SOURCE, id__in=sorted(target_ids)
        ).order_by("id")
        if len(sources) != len(target_ids):
            raise HTTPException(status_code=404, detail="One or more sources not found")
    else:
        sources = await Actor.filter(type=ActorType.SOURCE).order_by("id")

    if not sources:
        raise HTTPException(status_code=404, detail="No sources configured")
    enabled_sources = [s for s in sources if not getattr(s, "disabled", False)]
    changeset = await Changeset.begin(
        message="Source scan", actors=enabled_sources, status=OpStatus.IN_PROGRESS
    )

    RUNNING_CHANGESETS[changeset.id] = changeset
    changeset.start_operation(lambda: run_sources(changeset=changeset, sources=sources))

    return changeset.to_dict()


@router.post("/processors/run")
async def do_run_processors(
    processor_ids: list[int] | None = Query(None),
    asset_ids: list[int] | None = Query(None),
):
    processor_pipeline, processor_actors = await sort_processors(processor_ids)
    if not processor_pipeline:
        raise HTTPException(status_code=400, detail="No processor actors configured")

    assets_query = Asset.all()
    if asset_ids:
        assets_query = assets_query.filter(id__in=sorted(asset_ids))
    assets = await assets_query
    if asset_ids and len(assets) != len(asset_ids):
        raise HTTPException(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found to process")

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


@router.post("/analyzers/{analyzer_id}/run")
async def do_run_analyzers(analyzer_id: str):
    """Run a specific analyzer actor id, return started changeset."""

    try:
        target_id = int(analyzer_id)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="analyzer_id must be an integer actor id",
        )

    actor = await Actor.get_or_none(id=target_id, type=ActorType.ANALYZER)
    if actor is None or actor.disabled:
        raise HTTPException(
            status_code=404, detail="Analyzer actor not found or disabled"
        )
    changeset = await Changeset.begin(
        message=f"Run analyzer {actor.name or actor.id}",
        actors=[actor],
        status=OpStatus.IN_PROGRESS,
    )
    RUNNING_CHANGESETS[changeset.id] = changeset

    changeset.start_operation(lambda: run_analyzer(actor, changeset=changeset))
    return changeset.to_dict()
