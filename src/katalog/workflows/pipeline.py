from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import AsyncIterator, Protocol, Sequence, cast

from loguru import logger

from katalog.constants.metadata import ASSET_LOST
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import (
    Actor,
    ActorType,
    Changeset,
    ChangesetStats,
    Metadata,
    MetadataChanges,
    make_metadata,
)
from katalog.models.core import OpStatus
from katalog.plugins.registry import get_actor_instance
from katalog.processors.base import Processor
from katalog.processors.executors import ProcessorExecutorBundle
from katalog.processors.runtime import process_asset_collect
from katalog.runtime.batch import get_batch_size
from katalog.sources.base import SourcePlugin
from katalog.workflows.contracts import (
    RecursionSeed,
    SourceBatch,
    StageBatchEnvelope,
    WorkflowInputSpec,
    WorkflowSourceActorsInput,
)

ProcessorStage = Sequence[Processor]
ProcessorPipeline = Sequence[ProcessorStage]


@dataclass(frozen=True)
class WorkflowPipelineSettings:
    """Tunable knobs for outer pipeline behavior and recursion limits."""

    batch_size: int = field(default_factory=get_batch_size)
    max_inflight_load: int = 1
    max_inflight_process: int = 1
    max_inflight_persist: int = 1
    max_recursion_depth: int = 2


@dataclass
class WorkflowPipelineState:
    """Mutable run state shared by loading and finalization logic."""

    next_batch_id: int = 1
    recursion_queue: deque[RecursionSeed] = field(default_factory=deque)
    seen_assets_by_actor: dict[int, set[int]] = field(default_factory=dict)
    actor_has_seen_rows: dict[int, bool] = field(default_factory=dict)


@dataclass
class LoadedBatch:
    """Hydrated in-memory batch passed between load/process/persist stages."""

    batch_id: int
    changes_list: list[MetadataChanges]
    existing_metadata_by_asset: dict[int, list[Metadata]]


class LoadStage(Protocol):
    """Stage protocol for producing hydrated workflow batches."""

    async def produce(self, *, workflow_input: WorkflowInputSpec) -> AsyncIterator[LoadedBatch]: ...
    async def finalize(self) -> None: ...


class ProcessStage(Protocol):
    """Stage protocol for mutating batches through processor pipelines."""

    async def process(self, batch: LoadedBatch) -> LoadedBatch: ...


class PersistStage(Protocol):
    """Stage protocol for writing batch deltas to durable storage."""

    async def persist(self, batch: LoadedBatch) -> None: ...


class SourceLoadStage:
    """Load stage that scans source actors, hydrates metadata, and tracks missing assets."""

    def __init__(
        self,
        *,
        changeset: Changeset,
        source_actors: Sequence[Actor],
        settings: WorkflowPipelineSettings,
        missing_assets_policy: str,
    ) -> None:
        self.changeset = changeset
        self.source_actors = [a for a in source_actors if a.id is not None and not a.disabled]
        self.settings = settings
        self.missing_assets_policy = missing_assets_policy
        self.state = WorkflowPipelineState()
        self.asset_repo = get_asset_repo()
        self.metadata_repo = get_metadata_repo()
        self._actors_by_id: dict[int, Actor] = {}
        self._plugins_by_actor_id: dict[int, SourcePlugin] = {}

    async def _prepare_sources(self) -> None:
        """Resolve and readiness-check all source plugins participating in this run."""
        for actor in self.source_actors:
            if actor.id is None or actor.type != ActorType.SOURCE:
                continue
            actor_id = int(actor.id)
            self._actors_by_id[actor_id] = actor
            plugin = cast(SourcePlugin, await get_actor_instance(actor))
            ready, reason = await plugin.is_ready()
            if not ready:
                detail = reason or "unknown reason"
                raise RuntimeError(
                    f"Source {actor.name} ({actor.plugin_id}) is not ready: {detail}"
                )
            self._plugins_by_actor_id[actor_id] = plugin
            self.state.seen_assets_by_actor.setdefault(actor_id, set())
            self.state.actor_has_seen_rows.setdefault(actor_id, False)

    def _pick_recursive_source(self, changes: MetadataChanges) -> tuple[Actor, SourcePlugin] | None:
        """Pick the highest-scoring source that can recurse from current asset state."""
        candidates: list[tuple[int, int]] = []
        for actor_id, plugin in self._plugins_by_actor_id.items():
            score = int(plugin.can_scan_asset(changes) or 0)
            if score > 0:
                candidates.append((score, actor_id))
        if not candidates:
            return None
        candidates.sort(reverse=True)
        picked_actor_id = candidates[0][1]
        return self._actors_by_id[picked_actor_id], self._plugins_by_actor_id[picked_actor_id]

    async def _hydrate_source_batch(
        self,
        *,
        source_actor: Actor,
        source_batch: SourceBatch,
        depth: int,
    ) -> LoadedBatch:
        """Persist/find asset rows and build `MetadataChanges` inputs for processing."""
        stats = self.changeset.stats
        if stats is None:
            stats = ChangesetStats()
            self.changeset.stats = stats

        changes_list: list[MetadataChanges] = []
        existing_by_asset: dict[int, list[Metadata]] = {}

        for payload in source_batch.items:
            stats.assets_seen += 1
            stats.assets_saved += 1

            was_created = await self.asset_repo.save_record(
                payload.asset,
                changeset=self.changeset,
                actor=source_actor,
            )
            if was_created:
                stats.assets_added += 1
                loaded_metadata: list[Metadata] = []
            else:
                loaded_metadata = list(
                    await self.asset_repo.load_metadata(payload.asset, include_removed=True)
                )

            actor_id = int(source_actor.id or 0)
            staged_metadata = list(payload.metadata)
            staged_metadata.append(make_metadata_lost(actor_id))
            changes = MetadataChanges(
                asset=payload.asset,
                loaded=loaded_metadata,
                staged=staged_metadata,
            )
            changes_list.append(changes)

            asset_id = payload.asset.id
            if asset_id is not None:
                existing_by_asset[int(asset_id)] = loaded_metadata
                self.state.seen_assets_by_actor.setdefault(actor_id, set()).add(int(asset_id))
                self.state.actor_has_seen_rows[actor_id] = True

            if depth < self.settings.max_recursion_depth:
                picked = self._pick_recursive_source(changes)
                if picked is not None:
                    recurse_actor, _plugin = picked
                    if recurse_actor.id is not None:
                        self.state.recursion_queue.append(
                            RecursionSeed(
                                actor_id=int(recurse_actor.id),
                                changes=changes,
                                depth=depth + 1,
                            )
                        )

        if source_batch.ignored:
            stats.assets_seen += int(source_batch.ignored)
            stats.assets_ignored += int(source_batch.ignored)

        batch_id = self.state.next_batch_id
        self.state.next_batch_id += 1
        return LoadedBatch(
            batch_id=batch_id,
            changes_list=changes_list,
            existing_metadata_by_asset=existing_by_asset,
        )

    async def produce(self, *, workflow_input: WorkflowInputSpec) -> AsyncIterator[LoadedBatch]:
        """Produce initial and recursive source batches as hydrated workflow batches."""
        if not isinstance(workflow_input, WorkflowSourceActorsInput):
            raise NotImplementedError("Only source_actors workflow input is currently implemented.")

        await self._prepare_sources()
        selected_actor_ids = (
            set(int(actor_id) for actor_id in workflow_input.actor_ids)
            if workflow_input.actor_ids
            else None
        )

        for source_actor in self.source_actors:
            source_id = int(source_actor.id or 0)
            if selected_actor_ids is not None and source_id not in selected_actor_ids:
                continue
            plugin = self._plugins_by_actor_id.get(source_id)
            if plugin is None:
                continue
            async for source_batch in plugin.produce_batches(batch_size=self.settings.batch_size):
                yield await self._hydrate_source_batch(
                    source_actor=source_actor,
                    source_batch=source_batch,
                    depth=0,
                )

        while self.state.recursion_queue:
            seed = self.state.recursion_queue.popleft()
            if seed.depth > self.settings.max_recursion_depth:
                continue
            recurse_actor = self._actors_by_id.get(int(seed.actor_id))
            plugin = self._plugins_by_actor_id.get(int(seed.actor_id))
            if recurse_actor is None or plugin is None:
                continue
            async for source_batch in plugin.produce_recursive_batches(
                seed,
                batch_size=self.settings.batch_size,
            ):
                yield await self._hydrate_source_batch(
                    source_actor=recurse_actor,
                    source_batch=source_batch,
                    depth=seed.depth,
                )

    async def finalize(self) -> None:
        """Apply missing-assets policy after scan completion per source actor."""
        stats = self.changeset.stats
        if stats is None:
            stats = ChangesetStats()
            self.changeset.stats = stats
        for actor_id, seen_ids in self.state.seen_assets_by_actor.items():
            if not self.state.actor_has_seen_rows.get(actor_id):
                continue
            if self.missing_assets_policy == "delete":
                deleted = await self.asset_repo.delete_unseen_assets(
                    actor_ids=[actor_id],
                    seen_asset_ids=list(seen_ids),
                )
                if deleted:
                    stats.assets_lost += deleted
                    stats.assets_changed += deleted
            else:
                lost = await self.asset_repo.mark_unseen_as_lost(
                    changeset=self.changeset,
                    actor_ids=[actor_id],
                    seen_asset_ids=list(seen_ids),
                )
                if lost:
                    stats.assets_lost += lost
                    stats.assets_changed += lost


class ProcessorPipelineStage:
    """Process stage that applies the dependency-sorted processor pipeline per asset."""

    def __init__(
        self,
        *,
        changeset: Changeset,
        pipeline: ProcessorPipeline,
        always_process: bool = False,
    ) -> None:
        self.changeset = changeset
        self.pipeline = pipeline
        self.always_process = always_process
        self.executors = ProcessorExecutorBundle()

    async def process(self, batch: LoadedBatch) -> LoadedBatch:
        if not self.pipeline or not batch.changes_list:
            return batch
        tasks = [
            process_asset_collect(
                changeset=self.changeset,
                pipeline=self.pipeline,
                changes=changes,
                executors=self.executors,
                force_run=self.always_process,
            )
            for changes in batch.changes_list
        ]
        batch.changes_list = list(await asyncio.gather(*tasks))
        return batch

    def shutdown(self) -> None:
        self.executors.shutdown(cancelled=False)


class MetadataPersistStage:
    """Persist stage that writes merged metadata changes for each batch."""

    def __init__(self, *, changeset: Changeset) -> None:
        self.changeset = changeset
        self.metadata_repo = get_metadata_repo()

    async def persist(self, batch: LoadedBatch) -> None:
        if not batch.changes_list:
            return
        await self.metadata_repo.persist_changes_batch(
            self.changeset,
            batch.changes_list,
            batch.existing_metadata_by_asset,
        )


class WorkflowPipelineRunner:
    """Modular 3-stage workflow runner: load -> process -> persist."""

    def __init__(
        self,
        *,
        settings: WorkflowPipelineSettings | None = None,
        load_stage_factory=None,
        process_stage_factory=None,
        persist_stage_factory=None,
    ) -> None:
        self.settings = settings or WorkflowPipelineSettings()
        self._load_stage_factory = load_stage_factory or SourceLoadStage
        self._process_stage_factory = process_stage_factory or ProcessorPipelineStage
        self._persist_stage_factory = persist_stage_factory or MetadataPersistStage

    async def run(
        self,
        *,
        changeset: Changeset,
        workflow_input: WorkflowInputSpec,
        source_actors: Sequence[Actor],
        processor_pipeline: ProcessorPipeline,
        missing_assets_policy: str = "lost",
        always_process: bool = False,
    ) -> OpStatus:
        """Execute `load -> process -> persist` with pluggable stage implementations."""
        load_stage: LoadStage = self._load_stage_factory(
            changeset=changeset,
            source_actors=source_actors,
            settings=self.settings,
            missing_assets_policy=missing_assets_policy,
        )
        process_stage: ProcessStage = self._process_stage_factory(
            changeset=changeset,
            pipeline=processor_pipeline,
            always_process=always_process,
        )
        persist_stage: PersistStage = self._persist_stage_factory(
            changeset=changeset,
        )

        try:
            async for loaded_batch in load_stage.produce(workflow_input=workflow_input):
                processed_batch = await process_stage.process(loaded_batch)
                await persist_stage.persist(processed_batch)
            await load_stage.finalize()
            return OpStatus.COMPLETED
        except asyncio.CancelledError:
            logger.warning("Workflow pipeline was cancelled")
            return OpStatus.CANCELED
        except Exception:
            logger.exception("Workflow pipeline failed")
            return OpStatus.ERROR
        finally:
            shutdown = getattr(process_stage, "shutdown", None)
            if callable(shutdown):
                shutdown()


def make_metadata_lost(actor_id: int) -> Metadata:
    return make_metadata(ASSET_LOST, None, actor_id=actor_id)
