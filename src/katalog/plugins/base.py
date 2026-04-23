from __future__ import annotations

from typing import Any

from katalog.models import Actor
from katalog.constants.metadata import MetadataDef


class PluginBase:
    """Common base for all plugins, carrying the bound Actor instance."""

    def __init__(self, actor: Actor, **_: Any) -> None:
        self.actor = actor

    @classmethod
    def metadata_definitions_from_config(
        cls, config: dict[str, Any]
    ) -> list[MetadataDef | dict[str, Any]]:
        """Return optional metadata definitions declared by plugin configuration."""
        _ = config
        return []

    async def close(self) -> None:
        """Close any resources held by the plugin."""
        pass
