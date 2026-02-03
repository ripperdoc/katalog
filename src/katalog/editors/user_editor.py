from __future__ import annotations

from katalog.models import Actor
from katalog.db.actors import get_actor_repo
from katalog.editors.base import EditorPlugin
from katalog.models.core import ActorType


class UserEditor(EditorPlugin):
    """Default editor used for manual edits."""

    plugin_id = "katalog.editors.user_editor.UserEditor"
    title = "User Editor"
    description = "Default editor for manual edits"


async def ensure_user_editor() -> Actor:
    """Return the first Actor configured with the UserEditor plugin."""
    db = get_actor_repo()
    actor = await db.get_or_none(plugin_id=UserEditor.plugin_id)
    if actor is None:
        base_name = "User Editor"
        name = base_name
        suffix = 1
        while await db.get_or_none(name=name):
            suffix += 1
            name = f"{base_name} ({suffix})"

        actor = await db.create(
            name=name,
            plugin_id=UserEditor.plugin_id,
            type=ActorType.EDITOR,
        )
    return actor
