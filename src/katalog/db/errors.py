from __future__ import annotations


class ChangesetInProgressError(ValueError):
    """Raised when trying to create a changeset while another is in progress."""

    def __init__(self, changeset_id: int):
        self.changeset_id = int(changeset_id)
        super().__init__(
            f"Changeset {self.changeset_id} is already in progress; finish or cancel it first"
        )
