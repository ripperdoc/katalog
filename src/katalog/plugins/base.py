from __future__ import annotations

from typing import Any

from katalog.models import Provider


class PluginBase:
    """Common base for all plugins, carrying the bound Provider instance."""

    def __init__(self, provider: Provider, **_: Any) -> None:
        self.provider = provider

    async def close(self) -> None:
        """Close any resources held by the plugin."""
        pass
