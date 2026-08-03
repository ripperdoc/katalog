from __future__ import annotations

from typing import TYPE_CHECKING, Any

from katalog.constants.metadata import MetadataDef
from katalog.models import Actor

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

    async def is_ready(self) -> tuple[bool, str | None]:
        """Return whether the plugin can execute in the current environment.

        This check is run once per actor per run, not per asset.
        """
        return True, None

    async def close(self) -> None:
        """Close any resources held by the plugin."""
        pass
