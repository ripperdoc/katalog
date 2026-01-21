import asyncio
import traceback

from fastapi import APIRouter, HTTPException, Query
from loguru import logger

from katalog.analyzers.runtime import run_analyzers
from katalog.models import Actor, ActorType, Asset, Changeset, OpStatus
from katalog.processors.runtime import run_processors, sort_processors
from katalog.sources.runtime import run_sources
from katalog.utils.changeset_events import ChangesetRunState

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
    actor_for_changeset = None
    if len(sources) == 1:
        actor_for_changeset = sources[0]
    changeset = await Changeset.begin(
        actor=actor_for_changeset, status=OpStatus.IN_PROGRESS
    )

    cancel_event = asyncio.Event()
    done_event = asyncio.Event()

    async def is_cancelled() -> bool:
        return cancel_event.is_set()

    async def runner():
        try:
            with logger.contextualize(changeset_id=changeset.id):
                logger.info(f"Starting scan for changeset {changeset.id}")
                await run_sources(
                    changeset=changeset,
                    sources=sources,
                    is_cancelled=is_cancelled,
                )
            await changeset.finalize(status=OpStatus.COMPLETED)
        except asyncio.CancelledError:
            with logger.contextualize(changeset_id=changeset.id):
                logger.info(f"Cancelled changeset {changeset.id}")
            try:
                await changeset.finalize(status=OpStatus.CANCELED)
            finally:
                raise
        except Exception as exc:
            with logger.contextualize(changeset_id=changeset.id):
                logger.exception(f"Changeset {changeset.id} failed: {exc}")
            try:
                tb = traceback.format_exc()
                meta = dict(changeset.metadata or {})
                meta["error_message"] = str(exc)
                meta["error_traceback"] = tb
                changeset.metadata = meta
            except Exception:
                # Best-effort; don't block finalization on metadata failure.
                pass
            await changeset.finalize(status=OpStatus.ERROR)
        finally:
            done_event.set()
            RUNNING_CHANGESETS.pop(changeset.id, None)

    task = asyncio.create_task(runner())
    state = ChangesetRunState(
        changeset=changeset, task=task, cancel_event=cancel_event, done_event=done_event
    )
    task.add_done_callback(lambda _: done_event.set())
    RUNNING_CHANGESETS[changeset.id] = state

    return changeset.to_dict()


@router.post("/processors/run")
async def do_run_processor(ids: list[int] | None = Query(None)):
    processor_pipeline = await sort_processors()
    if not processor_pipeline:
        raise HTTPException(status_code=400, detail="No processor actors configured")

    target_ids = set(ids or [])

    assets_query = Asset.all()
    if target_ids:
        assets_query = assets_query.filter(id__in=sorted(target_ids))
    assets = await assets_query
    if target_ids and len(assets) != len(target_ids):
        raise HTTPException(
            status_code=404, detail="One or more asset ids not found or deleted"
        )
    if not assets:
        raise HTTPException(status_code=404, detail="No assets found to process")

    async with Changeset.context() as changeset:
        return await run_processors(changeset=changeset, assets=assets)
    return changeset


@router.post("/analyzers/{analyzer_id}/run")
async def do_run_analyzers(analyzer_id: str):
    """Run all analyzers or a specific analyzer actor id."""

    target_ids: list[int] | None
    if analyzer_id.lower() == "all":
        target_ids = None
    else:
        try:
            target_ids = [int(analyzer_id)]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="analyzer_id must be an integer actor id or 'all'",
            )

    try:
        results = await run_analyzers(target_ids)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("Analyzer execution failed")
        raise HTTPException(status_code=500, detail=str(exc))

    return {"analyzers": results}
