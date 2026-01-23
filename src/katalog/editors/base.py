from __future__ import annotations

from typing import Any

from katalog.plugins.base import PluginBase


class EditorPlugin(PluginBase):
    """
    Editor plugin for manual updates that do not support scanning.
    """

    def __init__(self, actor, **kwargs: Any) -> None:
        super().__init__(actor, **kwargs)

    def get_info(self) -> dict[str, Any]:
        """Returns metadata about the plugin."""
        raise NotImplementedError()
