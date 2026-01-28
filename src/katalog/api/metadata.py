from fastapi import APIRouter

from katalog.constants.metadata import editable_metadata_schema, METADATA_REGISTRY_BY_ID

router = APIRouter()


async def metadata_schema_editable_api() -> dict[str, object]:
    """Return JSON schema for editable metadata (non-asset/ keys)."""
    schema, ui_schema = editable_metadata_schema()
    return {"schema": schema, "uiSchema": ui_schema}


async def metadata_registry_api() -> dict[str, object]:
    """Return metadata registry keyed by registry id."""
    return {
        "registry": {
            key: value.to_dict() for key, value in METADATA_REGISTRY_BY_ID.items()
        }
    }


@router.get("/metadata/schema/editable")
async def metadata_schema_editable():
    return await metadata_schema_editable_api()


@router.get("/metadata/registry")
async def metadata_registry():
    return await metadata_registry_api()
