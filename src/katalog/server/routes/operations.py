from fastapi import APIRouter, Query

from katalog.api.operations import run_analyzer, run_processors, run_source

router = APIRouter()


@router.post("/sources/{source_id}/run")
async def run_source_rest(
    source_id: int, run_processors: bool = Query(True)
):
    return await run_source(source_id, run_processors=run_processors)


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
