from typing import ClassVar, FrozenSet, Protocol, runtime_checkable
from katalog.models import FileRecord


@runtime_checkable
class Processor(Protocol):
    """
    Defines the interface for a metadata processor.
    """

    # List of FileRecord field names this processor consumes
    dependencies: ClassVar[FrozenSet[str]] = frozenset()
    outputs: ClassVar[FrozenSet[str]] = frozenset()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # 1) coerce to frozenset
        deps = cls.dependencies
        if not isinstance(deps, frozenset):
            deps = frozenset(deps)
        outs = cls.outputs
        if not isinstance(outs, frozenset):
            outs = frozenset(outs)
        cls.dependencies, cls.outputs = deps, outs

    def should_run(self, record: FileRecord, changes: set[str] | None) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    async def run(self, record: FileRecord, changes: set[str] | None) -> FileRecord:
        """
        Run the processor logic and return a dict of fields to update on FileRecord or additional output.
        """
        raise NotImplementedError()
