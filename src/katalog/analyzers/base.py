from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, FrozenSet, Protocol, runtime_checkable

from katalog.db import Database, Snapshot
from katalog.models import Metadata, MetadataKey


@dataclass(slots=True)
class AnalyzerIssue:
    """Represents an unexpected state detected by an analyzer."""

    level: str
    message: str
    file_ids: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FileGroupFinding:
    """Describes a group of related files discovered by an analyzer."""

    kind: str
    label: str
    file_ids: list[str]
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RelationshipRecord:
    """Records a relationship edge that can be persisted to file_relationships."""

    from_file_id: str
    to_file_id: str
    relationship_type: str
    plugin_id: str
    confidence: float | None = None
    description: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AnalyzerResult:
    """Container for the analyzer outputs."""

    metadata: list[Metadata] = field(default_factory=list)
    relationships: list[RelationshipRecord] = field(default_factory=list)
    groups: list[FileGroupFinding] = field(default_factory=list)
    issues: list[AnalyzerIssue] = field(default_factory=list)


@runtime_checkable
class Analyzer(Protocol):
    """Interface for analyzers that operate on the full dataset after a snapshot."""

    PLUGIN_ID: ClassVar[str]

    # Metadata keys that must exist before this analyzer can run
    dependencies: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    # Metadata keys that this analyzer may write or update
    outputs: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        plugin_id = getattr(cls, "PLUGIN_ID", None)
        if not isinstance(plugin_id, str) or not plugin_id.strip():
            raise TypeError(
                f"Analyzer subclass {cls.__name__} must define a non-empty PLUGIN_ID"
            )
        cls.PLUGIN_ID = plugin_id

        deps = cls.dependencies
        if not isinstance(deps, frozenset):
            deps = frozenset(deps)
        outs = cls.outputs
        if not isinstance(outs, frozenset):
            outs = frozenset(outs)
        cls.dependencies, cls.outputs = deps, outs

    def should_run(self, *, snapshot: Snapshot, database: Database) -> bool:
        """Return True if the analyzer needs to execute for the given snapshot."""

        raise NotImplementedError()

    async def run(self, *, snapshot: Snapshot, database: Database) -> AnalyzerResult:
        """Execute the analyzer and return the metadata mutations to persist."""

        raise NotImplementedError()
