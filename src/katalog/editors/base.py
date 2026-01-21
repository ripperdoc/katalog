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

    def authorize(self, **kwargs) -> str:
        """
        Perform any authentication steps or callback required for this editor.
        Returns an authorization URL to redirect the user to, if applicable.
        """
        raise NotImplementedError()

    def get_accessor(self, asset):
        """
        Returns an accessor for the file data represented by the Asset.
        This is used to read file data.
        """
        raise NotImplementedError()

    def can_connect(self, uri: str) -> bool:
        """Check if the editor can connect to the given URI."""
        raise NotImplementedError()
