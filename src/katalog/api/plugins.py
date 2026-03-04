from katalog.plugins.registry import refresh_plugins

from katalog.api.helpers import config_schema_for_plugin


async def list_plugins() -> list:
    """List discovered plugin specifications."""
    return list(refresh_plugins().values())


async def get_plugin_config_schema(plugin_id: str) -> dict:
    """Return config schema for one plugin id."""
    return config_schema_for_plugin(plugin_id)
