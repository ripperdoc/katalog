from fastapi import APIRouter

from katalog.plugins.registry import refresh_plugins

from katalog.api.helpers import config_schema_for_plugin

router = APIRouter()


async def list_plugins_api() -> dict:
    plugins = [p.to_dict() for p in refresh_plugins().values()]
    return {"plugins": plugins}


async def get_plugin_config_schema_api(plugin_id: str) -> dict:
    return config_schema_for_plugin(plugin_id)


@router.get("/plugins")
async def list_plugins_endpoint():
    return await list_plugins_api()


@router.get("/plugins/{plugin_id}/config/schema")
async def get_plugin_config_schema(plugin_id: str):
    return await get_plugin_config_schema_api(plugin_id)
