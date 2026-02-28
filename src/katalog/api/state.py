from __future__ import annotations

from katalog.config import current_app_context


def get_event_manager():
    return current_app_context().event_manager


def get_running_changesets():
    return current_app_context().running_changesets
