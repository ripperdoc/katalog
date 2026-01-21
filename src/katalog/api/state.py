from katalog.utils.changeset_events import ChangesetEventManager, ChangesetRunState

event_manager = ChangesetEventManager()

RUNNING_CHANGESETS: dict[int, ChangesetRunState] = {}
