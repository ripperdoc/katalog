from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, FrozenSet, cast

from katalog.models import (
    Asset,
    MetadataKey,
    Metadata,
    OpStatus,
    Provider,
)
from katalog.metadata import DATA_KEY, HASH_MD5, TIME_MODIFIED
from katalog.plugins.base import PluginBase

from katalog.utils.utils import import_plugin_class


@dataclass(slots=True)
class ProcessorResult:
    metadata: list[Metadata] = field(default_factory=list)
    assets: list[Asset] = field(default_factory=list)
    status: OpStatus = OpStatus.COMPLETED
    message: str | None = None


class Processor(PluginBase, ABC):
    """
    Defines the interface for a metadata processor.
    """

    # List of metadata keys that this processor consumes
    dependencies: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    # List of metadata keys that this processor changes/produces
    outputs: ClassVar[FrozenSet[MetadataKey]] = frozenset()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Coerce to frozenset for consistency
        deps = cls.dependencies
        if not isinstance(deps, frozenset):
            deps = frozenset(deps)
        outs = cls.outputs
        if not isinstance(outs, frozenset):
            outs = frozenset(outs)
        cls.dependencies, cls.outputs = deps, outs

    @abstractmethod
    def should_run(
        self,
        asset: Asset,
        changes: set[str] | None,
    ) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    @abstractmethod
    async def run(self, asset: Asset, changes: set[str] | None) -> ProcessorResult:
        """Run the processor logic and return a result class with changes to persist."""
        raise NotImplementedError()


def file_data_changed(
    self, asset: Asset, changes: set[str] | None, allow_weak_check: bool = True
) -> bool:
    """Helper to determine if data or relevant fields have changed. If allow_weak_check is True, also assume data has changed if TIME_MODIFIED has changed."""
    # TODO more hash types to check?
    return changes is not None and (
        DATA_KEY in changes
        or HASH_MD5 in changes
        or (allow_weak_check and TIME_MODIFIED in changes)
    )


file_data_change_dependencies = frozenset({DATA_KEY, HASH_MD5, TIME_MODIFIED})


def make_processor_instance(processor_record: Provider) -> Processor:
    ProcessorClass = cast(
        type[Processor], import_plugin_class(processor_record.plugin_id)
    )
    return ProcessorClass(provider=processor_record, **(processor_record.config or {}))
