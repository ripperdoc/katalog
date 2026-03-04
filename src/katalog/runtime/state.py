from __future__ import annotations

from katalog.config import current_app_context


def get_event_manager():
    """Return the shared changeset event manager."""
    return current_app_context().event_manager


def get_running_changesets():
    """Return the map of currently running changesets."""
    return current_app_context().running_changesets
