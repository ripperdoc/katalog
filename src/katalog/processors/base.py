from typing import ClassVar, FrozenSet, Protocol, runtime_checkable
from katalog.models import (
    DATA_KEY,
    HASH_MD5,
    TIME_MODIFIED,
    FileRecord,
    MetadataKey,
    Metadata,
)
from katalog.db import Database


@runtime_checkable
class Processor(Protocol):
    """
    Defines the interface for a metadata processor.
    """

    PLUGIN_ID: ClassVar[str]

    # List of meteadata keys that this processor consumes
    dependencies: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    # List of meteadata keys that this processor changes/produces
    outputs: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        plugin_id = getattr(cls, "PLUGIN_ID", None)
        if not isinstance(plugin_id, str) or not plugin_id.strip():
            raise TypeError(
                f"Processor subclass {cls.__name__} must define a non-empty PLUGIN_ID"
            )
        cls.PLUGIN_ID = plugin_id

        # Coerce to frozenset
        deps = cls.dependencies
        if not isinstance(deps, frozenset):
            deps = frozenset(deps)
        outs = cls.outputs
        if not isinstance(outs, frozenset):
            outs = frozenset(outs)
        cls.dependencies, cls.outputs = deps, outs

    def should_run(
        self,
        record: FileRecord,
        changes: set[str] | None,
        database: Database | None = None,
    ) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    async def run(self, record: FileRecord, changes: set[str] | None) -> list[Metadata]:
        """Run the processor logic and return Metadata entries to persist for the file."""
        raise NotImplementedError()


def file_data_changed(
    self, record: FileRecord, changes: set[str] | None, allow_weak_check: bool = True
) -> bool:
    """Helper to determine if data or relevant fields have changed. If allow_weak_check is True, also assume data has changed if TIME_MODIFIED has changed."""
    # TODO more hash types to check?
    return changes is not None and (
        DATA_KEY in changes
        or HASH_MD5 in changes
        or (allow_weak_check and TIME_MODIFIED in changes)
    )


file_data_change_dependencies = frozenset({DATA_KEY, HASH_MD5, TIME_MODIFIED})
