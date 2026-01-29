from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar, FrozenSet, cast

from katalog.models import Metadata, MetadataKey, Actor, Changeset
from katalog.plugins.base import PluginBase
from katalog.plugins.registry import get_actor_instance


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
    """Records a relationship edge that can be persisted to asset_relationships."""

    from_id: str
    to_id: str
    relationship_type: str
    actor_id: str | None = None
    confidence: float | None = None
    description: str | None = None
    removed: bool = False
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AnalyzerResult:
    """Container for the analyzer outputs."""

    metadata: list[Metadata] = field(default_factory=list)
    relationships: list[RelationshipRecord] = field(default_factory=list)
    groups: list[FileGroupFinding] = field(default_factory=list)
    issues: list[AnalyzerIssue] = field(default_factory=list)
    output: dict[str, Any] | None = None

    def to_dict(self) -> dict:
        return {
            "metadata": [asdict(m) for m in self.metadata],
            "relationships": [asdict(r) for r in self.relationships],
            "groups": [asdict(g) for g in self.groups],
            "issues": [asdict(i) for i in self.issues],
            "output": self.output,
        }


@dataclass(slots=True)
class AnalyzerScope:
    """Scope for analyzer operations."""

    kind: str
    asset_id: int | None = None
    collection_id: int | None = None
    collection_key_id: int | None = None

    @classmethod
    def all(cls) -> "AnalyzerScope":
        return cls(kind="all")

    @classmethod
    def asset(cls, asset_id: int) -> "AnalyzerScope":
        return cls(kind="asset", asset_id=asset_id)

    @classmethod
    def collection(cls, collection_id: int, *, key_id: int) -> "AnalyzerScope":
        return cls(
            kind="collection", collection_id=collection_id, collection_key_id=key_id
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"kind": self.kind}
        if self.asset_id is not None:
            payload["asset_id"] = self.asset_id
        if self.collection_id is not None:
            payload["collection_id"] = self.collection_id
        return payload


class Analyzer(PluginBase, ABC):
    """Interface for analyzers that operate on the full dataset after a changeset."""

    # Metadata keys that must exist before this analyzer can run
    dependencies: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    # Metadata keys that this analyzer may write or update
    outputs: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    # Output classification for changeset payloads
    output_kind: ClassVar[str | None] = None

    supports_all: ClassVar[bool] = True
    supports_single_asset: ClassVar[bool] = True
    supports_collection: ClassVar[bool] = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        deps = cls.dependencies
        if not isinstance(deps, frozenset):
            deps = frozenset(deps)
        outs = cls.outputs
        if not isinstance(outs, frozenset):
            outs = frozenset(outs)
        cls.dependencies, cls.outputs = deps, outs

    @abstractmethod
    def should_run(self, *, changeset: Changeset) -> bool:
        """Return True if the analyzer needs to execute for the given changeset."""

    @abstractmethod
    async def run(
        self, *, changeset: Changeset, scope: AnalyzerScope
    ) -> AnalyzerResult:
        """Execute the analyzer and return the metadata mutations to persist."""


async def make_analyzer_instance(analyzer_record: Actor) -> Analyzer:
    return cast(Analyzer, await get_actor_instance(analyzer_record))
