from fastapi import APIRouter

from katalog.api.plugins import get_plugin_config_schema, list_plugins

router = APIRouter()


@router.get("/plugins")
async def list_plugins_rest():
    plugins = await list_plugins()
    return {"plugins": plugins}


@router.get("/plugins/{plugin_id}/config/schema")
async def get_plugin_config_schema_rest(plugin_id: str):
    return await get_plugin_config_schema(plugin_id)
