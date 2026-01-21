from __future__ import annotations

from typing import Any

from katalog.models import Actor


class PluginBase:
    """Common base for all plugins, carrying the bound Actor instance."""

    def __init__(self, actor: Actor, **_: Any) -> None:
        self.actor = actor

    async def close(self) -> None:
        """Close any resources held by the plugin."""
        pass
