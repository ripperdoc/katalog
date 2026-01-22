"""Data usage notes
- The target profile for this system is to handle metadata for 1 million files. Actual file contents is not to be stored in the DB.
- This implies
- ~1 million Asset records
- ~30 million Metadata records (assuming an average of 30 metadata entries per asset).
Metadata will mostly be shorter text and date values, but some fields may grow pretty large, such as text contents, summaries, etc.
- 10 to 100 Actors
- As data changes over time, changesets will be created, increasing the number of Metadata rows per asset.
On the other hand, users will be encouraged to purge changesets regularly.
"""

from __future__ import annotations
import asyncio
from dataclasses import dataclass, asdict
from enum import Enum, IntEnum
from time import time
import traceback
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from loguru import logger
from tortoise.fields import (
    CASCADE,
    IntEnumField,
    CharEnumField,
    CharField,
    DatetimeField,
    ForeignKeyField,
    JSONField,
    IntField,
    TextField,
    BooleanField,
)
from tortoise.models import Model


class OpStatus(Enum):
    IN_PROGRESS = "in_progress"
    PARTIAL = "partial"
    COMPLETED = "completed"
    CANCELED = "canceled"
    SKIPPED = "skipped"
    ERROR = "error"


class ActorType(IntEnum):
    SOURCE = 0
    PROCESSOR = 1
    ANALYZER = 2
    EDITOR = 3
    EXPORTER = 4


class Actor(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    plugin_id = CharField(max_length=1024, null=True)
    config = JSONField(null=True)
    config_toml = TextField(null=True)
    type = IntEnumField(ActorType)
    disabled = BooleanField(default=False)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.name if isinstance(self.type, ActorType) else self.type,
            "plugin_id": self.plugin_id,
            "config": self.config,
            "config_toml": self.config_toml,
            "disabled": bool(self.disabled),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass(slots=True)
class ChangesetStats:
    # Total assets encountered/accessed during scan (saved + ignored)
    assets_seen: int = 0
    assets_saved: int = 0  # Assets yielded and saved/processed by the pipeline
    assets_ignored: int = 0  # Skipped during scan (e.g. filtered by actor settings)

    assets_changed: int = 0  # Assets that had metadata changes
    assets_added: int = 0  # New assets seen for the first time
    assets_lost: int = 0  # Assets marked as deleted (not seen in scan)
    assets_processed: int = 0  # Assets that had processors run on them

    metadata_values_changed: int = 0  # Total metadata values added or removed
    metadata_values_added: int = 0  # Metadata values added
    metadata_values_removed: int = 0  # Metadata values removed
    # TODO how to correctly count unique keys affected across
    # a changeset when we persist paralell async operations?
    # metadata_keys_affected: int = 0

    processings_started: int = 0  # Total processing operations started
    processings_completed: int = 0  # Total processing operations completed successfully
    processings_partial: int = 0  # Total processing operations completed partially
    processings_cancelled: int = 0  # Total processing operations cancelled
    processings_skipped: int = 0  # Total processing operations skipped
    processings_error: int = 0  # Total processing operations failed with error

    # def validate(self) -> None:
    #     assert self.ass

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_TASK_CONCURRENCY = 10


class Changeset(Model):
    # The int timestamp in ms since epoch when the changeset was started
    id = IntField(pk=True)
    message = CharField(max_length=512, null=True)
    running_time_ms = IntField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    data = JSONField(null=True)

    # Local fields not persisted to DB, used for operations
    stats: ChangesetStats
    tasks: list[asyncio.Task]
    semaphore: asyncio.Semaphore
    _tasks_queued: int
    _tasks_running: int
    _tasks_finished: int
    task: asyncio.Task | None
    cancel_event: asyncio.Event | None
    done_event: asyncio.Event | None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._init_runtime_state()

    def _init_runtime_state(self) -> None:
        # Safe defaults so instances materialized from the ORM always have runtime fields.
        self.stats = ChangesetStats()
        self.tasks = []
        self.semaphore = asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)
        self._tasks_queued = 0
        self._tasks_running = 0
        self._tasks_finished = 0
        self.task = None
        self.cancel_event = None
        self.done_event = None

    def to_dict(self) -> dict:
        actor_ids: Sequence[int] | None = None
        cache = getattr(self, "_prefetched_objects_cache", None)
        if cache and "actor_links" in cache:
            actor_ids = [int(link.actor_id) for link in cache["actor_links"]]
        return {
            "id": self.id,
            "actor_ids": actor_ids,
            "message": self.message,
            "running_time_ms": self.running_time_ms,
            "status": self.status.value
            if isinstance(self.status, OpStatus)
            else str(self.status),
            "data": self.data,
        }

    @classmethod
    async def find_partial_resume_point(cls, *, actor: Actor) -> "Changeset | None":
        """
        Return the most recent PARTIAL changeset that occurred after the latest
        COMPLETED changeset for this actor. If no COMPLETED changeset exists,
        return None (treat as full scan).
        """
        changesets = list(await cls.filter(actor_links__actor=actor).order_by("-id"))
        last_full = None
        last_partial = None
        for s in reversed(changesets):
            if s.status == OpStatus.COMPLETED:
                last_full = s
                break
            elif last_partial is None and s.status == OpStatus.PARTIAL:
                last_partial = s

        if last_full is None:
            return None

        if last_partial is not None:
            return last_partial
        else:
            return last_full

    @classmethod
    async def begin(
        cls,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        data: Mapping[str, Any] | None = None,
        actors: Iterable[Actor] | None = None,
        message: str | None = None,
    ) -> "Changeset":
        # Prevent concurrent in-progress changesets (scans or edits).
        existing_in_progress = await cls.get_or_none(status=OpStatus.IN_PROGRESS)
        if existing_in_progress is not None:
            raise ValueError(
                f"Changeset {existing_in_progress.id} is already in progress; finish or cancel it first"
            )
        changeset_id = int(time() * 1000)
        if await cls.get_or_none(id=changeset_id):
            raise ValueError(f"Changeset with id {changeset_id} already exists")
        changeset = await cls.create(
            id=changeset_id,
            status=status,
            message=message,
            data=dict(data) if data else None,
        )
        await changeset.add_actors(actors or [])
        return changeset

    async def add_actors(self, actors: Iterable[Actor]) -> None:
        actor_list = list(actors)
        if not actor_list:
            return
        existing = {
            row["actor_id"]
            for row in await ChangesetActor.filter(changeset=self).values("actor_id")
        }
        payload = [
            ChangesetActor(changeset=self, actor=actor)
            for actor in actor_list
            if actor.id not in existing
        ]
        if payload:
            await ChangesetActor.bulk_create(payload)

    def log_task_progress(self) -> None:
        """
        Emit a structured log message that can be turned into SSE progress events.
        """
        logger.bind(changeset_id=self.id).info(
            "tasks_progress queued={queued} running={running} finished={finished}",
            queued=self._tasks_queued,
            running=self._tasks_running,
            finished=self._tasks_finished,
        )

    def start_operation(
        self,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        on_status_error: OpStatus = OpStatus.ERROR,
    ) -> asyncio.Task:
        """
        Start an operation coroutine for this changeset with shared finalize/error handling.

        - `coro_factory` must be a zero-arg callable returning an awaitable.
        """

        if self.task and not self.task.done():
            raise RuntimeError(
                f"Changeset {self.id} already has a running operation task"
            )
        if self.cancel_event is None:
            self.cancel_event = asyncio.Event()
        if self.done_event is None:
            self.done_event = asyncio.Event()

        context_cm = logger.contextualize(changeset_id=self.id)

        async def runner():
            try:
                with context_cm:
                    await coro_factory()
                await self.finalize(status=OpStatus.COMPLETED)
            except asyncio.CancelledError:
                await self.finalize(status=OpStatus.CANCELED)
                raise
            except Exception as exc:  # noqa: BLE001
                data = dict(self.data or {})
                data["error_message"] = str(exc)
                data["error_traceback"] = traceback.format_exc()
                self.data = data
                await self.finalize(status=on_status_error)
                raise
            finally:
                if self.done_event:
                    self.done_event.set()

        self.task = asyncio.create_task(runner())
        self.task.add_done_callback(lambda _: self.done_event.set())
        return self.task

    def cancel(self) -> None:
        """Signal cancellation and cancel the main task."""
        if self.cancel_event:
            self.cancel_event.set()
        if self.task and not self.task.done():
            self.task.cancel()

    def is_cancelled(self) -> bool:
        """Check if cancellation has been signaled."""
        return self.cancel_event.is_set() if self.cancel_event else False

    async def wait_cancelled(self, timeout: float | None = None) -> None:
        """Best-effort wait for this changeset to finish after cancellation."""
        if not self.done_event:
            return
        try:
            await asyncio.wait_for(self.done_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout waiting for changeset {changeset_id} to finish after cancellation",
                changeset_id=self.id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.opt(exception=exc).warning(
                "Error while waiting for changeset {changeset_id} to finish after cancellation",
                changeset_id=self.id,
            )

    def enqueue(
        self, coro_or_factory: Callable[[], Awaitable[Any]] | Awaitable[Any]
    ) -> asyncio.Task:
        """
        Enqueue a sub-task under this changeset with shared cancellation/concurrency and progress.
        """
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)
        self._tasks_queued += 1
        self.log_task_progress()

        async def runner():
            async with self.semaphore:
                self._tasks_queued -= 1
                self._tasks_running += 1
                self.log_task_progress()
                try:
                    if self.cancel_event and self.cancel_event.is_set():
                        raise asyncio.CancelledError()
                    if asyncio.iscoroutine(coro_or_factory):
                        coro = coro_or_factory
                    else:
                        coro = coro_or_factory()
                    return await coro
                finally:
                    self._tasks_running -= 1
                    self._tasks_finished += 1
                    self.log_task_progress()

        task = asyncio.create_task(runner())
        self.tasks.append(task)
        return task

    async def finalize(self, *, status: OpStatus) -> None:
        # Ensure runtime fields exist even if object was loaded from DB outside __init__ context.
        if not hasattr(self, "tasks"):
            self._init_runtime_state()

        if self.tasks:
            logger.info(
                "Draining {count} processor tasks before finalizing changeset {changeset_id}",
                count=len(self.tasks),
                changeset_id=self.id,
            )
            modified, failures = await drain_tasks(self.tasks)
            logger.info(
                "Finished draining processor tasks for changeset {changeset_id} (modified={modified}, failures={failures})",
                changeset_id=self.id,
                modified=modified,
                failures=failures,
            )

        data_payload: dict[str, Any] | None = None
        if self.stats is not None or self.data is not None:
            data_payload = dict(self.data or {})
            if self.stats is not None:
                data_payload["stats"] = self.stats.to_dict()
            self.data = data_payload

        if self.running_time_ms is None:
            now_ts = time()
            self.running_time_ms = int(now_ts * 1000) - int(self.id)

        update_fields = ["status", "running_time_ms"]
        if data_payload is not None:
            update_fields.append("data")
        self.status = status
        await self.save(update_fields=update_fields)

    class Meta(Model.Meta):
        indexes = ()


class ChangesetActor(Model):
    """Join table for changeset -> actors involved."""

    id = IntField(pk=True)
    changeset = ForeignKeyField(
        "models.Changeset", related_name="actor_links", on_delete=CASCADE
    )
    actor = ForeignKeyField(
        "models.Actor", related_name="changeset_links", on_delete=CASCADE
    )

    class Meta(Model.Meta):
        unique_together = (("changeset", "actor"),)


async def drain_tasks(tasks: list[asyncio.Task[Any]]) -> tuple[int, int]:
    if not tasks:
        return 0, 0
    results = await asyncio.gather(*tasks, return_exceptions=True)
    modified = 0
    failures = 0
    for result in results:
        if isinstance(result, Exception):
            logger.opt(exception=result).error("Processor task failed")
            failures += 1
            continue
        if result:
            modified += 1
    tasks.clear()
    return modified, failures
