from fastapi import APIRouter

from katalog.constants.metadata import editable_metadata_schema, METADATA_REGISTRY_BY_ID

router = APIRouter()


@router.get("/metadata/schema/editable")
async def metadata_schema_editable():
    """Return JSON schema for editable metadata (non-asset/ keys)."""
    schema, ui_schema = editable_metadata_schema()
    return {"schema": schema, "uiSchema": ui_schema}


@router.get("/metadata/registry")
async def metadata_registry():
    """Return metadata registry keyed by registry id."""
    return {
        "registry": {
            key: value.to_dict() for key, value in METADATA_REGISTRY_BY_ID.items()
        }
    }
