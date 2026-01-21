from __future__ import annotations

from katalog.models import Actor
from katalog.sources.base import ScanResult, SourcePlugin
from katalog.models import OpStatus


class UserEditorSource(SourcePlugin):
    """Pseudo source used for manual edits.

    It does not implement scanning; it merely exists as an actor identity so
    manual edits can be recorded as coming from an actor.
    """

    plugin_id = "katalog.sources.user_editor.UserEditorSource"
    title = "User Editor"
    description = "Manual edits actor"

    async def scan(self) -> ScanResult:
        # Manual editor cannot scan; return empty iterator and skipped status.
        async def _empty():
            if False:
                yield  # pragma: no cover

        return ScanResult(iterator=_empty(), status=OpStatus.SKIPPED, ignored=0)


# Convenience factory used in code/tests
def ensure_user_editor_actor(name: str = "Manual edits") -> Actor:
    from katalog.models import ActorType

    existing = Actor.get_or_none(name=name)
    if existing:
        return existing  # type: ignore
    # NOTE: caller should await; left sync for brevity; actual creation is async elsewhere.
    raise RuntimeError(
        "ensure_user_editor_actor must be awaited via caller-specific logic"
    )
