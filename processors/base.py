from typing import Protocol, runtime_checkable
from models import FileRecord

@runtime_checkable
class Processor(Protocol):
    """
    Defines the interface for a metadata processor.
    """
    name: str
    # List of FileRecord field names this processor consumes
    dependencies: list[str]
    # List of FileRecord field names this processor produces
    outputs: list[str]

    def should_run(self, record: FileRecord, prev_cache: str | None) -> bool:
        """Return True if the processor needs to run based on record and previous cache key."""
        ...

    def cache_key(self, record: FileRecord) -> str:
        """Return a reproducible key of the inputs to decide if re-run is needed."""
        ...

    def run(self, record: FileRecord) -> dict:
        """
        Run the processor logic and return a dict of fields to update on FileRecord or additional output.
        """
        ...
