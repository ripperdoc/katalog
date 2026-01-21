from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import FrozenSet, cast

from katalog.models import (
    Asset,
    MetadataKey,
    Metadata,
    OpStatus,
    Actor,
    MetadataChangeSet,
)
from katalog.constants.metadata import DATA_KEY, HASH_MD5, TIME_MODIFIED
from katalog.plugins.base import PluginBase

from katalog.plugins.registry import get_plugin_class


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

    @property
    @abstractmethod
    def dependencies(self) -> FrozenSet[MetadataKey]:
        """Return dependencies for this processor instance."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def outputs(self) -> FrozenSet[MetadataKey]:
        """Return outputs for this processor instance."""
        raise NotImplementedError()

    @abstractmethod
    def should_run(
        self,
        asset: Asset,
        change_set: MetadataChangeSet,
    ) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    @abstractmethod
    async def run(
        self,
        asset: Asset,
        change_set: MetadataChangeSet,
    ) -> ProcessorResult:
        """Run the processor logic and return a result class with changes to persist."""
        raise NotImplementedError()


def file_data_changed(
    self,
    asset: Asset,
    change_set: MetadataChangeSet,
    allow_weak_check: bool = True,
) -> bool:
    """Helper to determine if data or relevant fields have changed. If allow_weak_check is True, also assume data has changed if TIME_MODIFIED has changed."""
    changes = change_set.changed_keys()
    return (
        DATA_KEY in changes
        or HASH_MD5 in changes
        or (allow_weak_check and TIME_MODIFIED in changes)
    )


file_data_change_dependencies = frozenset({DATA_KEY, HASH_MD5, TIME_MODIFIED})


def make_processor_instance(processor_record: Actor) -> Processor:
    ProcessorClass = cast(type[Processor], get_plugin_class(processor_record.plugin_id))
    return ProcessorClass(actor=processor_record, **(processor_record.config or {}))
