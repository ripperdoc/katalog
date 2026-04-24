from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING

from katalog.models import Actor
from katalog.constants.metadata import MetadataDef

if TYPE_CHECKING:
    from katalog.models.views import ViewSpec


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

    @classmethod
    def view_definitions_from_config(
        cls, config: dict[str, Any]
    ) -> list["ViewSpec" | dict[str, Any]]:
        """Return optional runtime view definitions declared by plugin configuration."""
        _ = config
        return []

    def view_definitions(self) -> list["ViewSpec" | dict[str, Any]]:
        """Return runtime view definitions for this actor instance."""
        return self.view_definitions_from_config(self.actor.config or {})

    async def close(self) -> None:
        """Close any resources held by the plugin."""
        pass
