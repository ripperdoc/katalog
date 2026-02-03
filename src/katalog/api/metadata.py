from fastapi import APIRouter

from katalog.constants.metadata import (
    MetadataDef,
    editable_metadata_schema,
    METADATA_REGISTRY_BY_ID,
)
from katalog.models.query import EditableMetadataSchemaResponse

router = APIRouter()


async def metadata_schema_editable() -> EditableMetadataSchemaResponse:
    """Return JSON schema for editable metadata (non-asset/ keys)."""
    schema, ui_schema = editable_metadata_schema()
    return EditableMetadataSchemaResponse(schema=schema, uiSchema=ui_schema)


async def metadata_registry() -> dict[str, dict[int, MetadataDef]]:
    """Return metadata registry keyed by registry id."""
    return {"registry": METADATA_REGISTRY_BY_ID}


@router.get("/metadata/schema/editable")
async def metadata_schema_editable_rest():
    return await metadata_schema_editable()


@router.get("/metadata/registry")
async def metadata_registry_rest():
    return await metadata_registry()
