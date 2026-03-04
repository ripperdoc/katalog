from fastapi import APIRouter, Request

from katalog.api.metadata import (
    list_metadata,
    metadata_registry,
    metadata_schema_editable,
)
from katalog.models.query import AssetQuery

router = APIRouter()


@router.get("/metadata/schema/editable")
async def metadata_schema_editable_rest():
    return await metadata_schema_editable()


@router.get("/metadata/registry")
async def metadata_registry_rest():
    return await metadata_registry()


@router.post("/metadata/search")
async def list_metadata_rest(request: Request):
    payload = await request.json()
    query = AssetQuery.model_validate(payload)
    return await list_metadata(query)
