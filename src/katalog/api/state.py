from katalog.utils.changeset_events import ChangesetEventManager
from katalog.models import Changeset

event_manager = ChangesetEventManager()

RUNNING_CHANGESETS: dict[int, Changeset] = {}
