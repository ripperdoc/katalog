from fastapi import APIRouter

from katalog.plugins.registry import refresh_plugins

from katalog.api.helpers import config_schema_for_plugin

router = APIRouter()


@router.get("/plugins")
async def list_plugins_endpoint():
    plugins = [p.to_dict() for p in refresh_plugins().values()]
    return {"plugins": plugins}


@router.get("/plugins/{plugin_id}/config/schema")
async def get_plugin_config_schema(plugin_id: str):
    return config_schema_for_plugin(plugin_id)
