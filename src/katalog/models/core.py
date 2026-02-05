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
from enum import Enum, IntEnum
from time import time
from datetime import UTC, datetime
import traceback
from typing import Any, Awaitable, Callable

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_serializer


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


class Actor(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    name: str
    plugin_id: str | None = None
    type: ActorType
    config: dict[str, Any] | None = None
    config_toml: str | None = None
    disabled: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_serializer("type")
    def _serialize_type(self, value: ActorType) -> str:
        return value.name if isinstance(value, ActorType) else str(value)


class ChangesetStats(BaseModel):
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

    processings_started: int = 0  # Total processing operations started
    processings_completed: int = 0  # Total processing operations completed successfully
    processings_partial: int = 0  # Total processing operations completed partially
    processings_cancelled: int = 0  # Total processing operations cancelled
    processings_skipped: int = 0  # Total processing operations skipped
    processings_error: int = 0  # Total processing operations failed with error


DEFAULT_TASK_CONCURRENCY = 10


class Changeset(BaseModel):
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    id: int
    message: str | None = None
    running_time_ms: int | None = None
    status: OpStatus
    data: dict[str, Any] | None = None
    actor_ids: list[int] | None = None

    # Local fields not persisted to DB, used for operations
    stats: ChangesetStats | None = Field(default=None, exclude=True)
    tasks: list[asyncio.Task] | None = Field(default=None, exclude=True)
    semaphore: asyncio.Semaphore | None = Field(default=None, exclude=True)
    _tasks_queued: int = 0
    _tasks_running: int = 0
    _tasks_finished: int = 0
    task: asyncio.Task | None = Field(default=None, exclude=True)
    cancel_event: asyncio.Event | None = Field(default=None, exclude=True)
    done_event: asyncio.Event | None = Field(default=None, exclude=True)

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._init_runtime_state()

    def _init_runtime_state(self) -> None:
        # Safe defaults so instances materialized from the DB always have runtime fields.
        if self.stats is None:
            self.stats = ChangesetStats()
        if self.tasks is None:
            self.tasks = []
        if self.semaphore is None:
            self.semaphore = asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)
        self._tasks_queued = 0
        self._tasks_running = 0
        self._tasks_finished = 0
        self.task = None
        self.cancel_event = None
        self.done_event = None

    def started_at(self) -> datetime:
        """Return the changeset start time derived from its millisecond ID."""
        return datetime.fromtimestamp(self.id / 1000.0, tz=UTC)

    def started_at_iso(self) -> str:
        """Return the changeset start time as an ISO-8601 string."""
        return self.started_at().isoformat()

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
        try:
            from katalog.api.state import event_manager

            event_manager.emit(
                int(self.id),
                "changeset_progress",
                payload={
                    "queued": self._tasks_queued,
                    "running": self._tasks_running,
                    "finished": self._tasks_finished,
                    "kind": None,
                },
            )
        except Exception:
            logger.exception("Failed to emit changeset progress event")

    def start_operation(
        self,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        on_status_error: OpStatus = OpStatus.ERROR,
        success_status: OpStatus = OpStatus.COMPLETED,
    ) -> asyncio.Task:
        """
        Start an operation coroutine for this changeset with shared finalize/error handling.

        - `coro_factory` must be a zero-arg callable returning an awaitable.
        - If the coroutine returns an `OpStatus`, it will be used to finalize the changeset;
          otherwise `success_status` is used.
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
                    try:
                        from katalog.api.state import event_manager

                        event_manager.emit(
                            int(self.id),
                            "changeset_start",
                            payload=self.model_dump(mode="json"),
                        )
                    except Exception:
                        logger.exception("Failed to emit changeset start event")
                    result = await coro_factory()
                final_status = (
                    result if isinstance(result, OpStatus) else success_status
                )
                await self.finalize(status=final_status)
            except asyncio.CancelledError:
                await self.finalize(status=OpStatus.CANCELED)
                raise
            except Exception as exc:  # noqa: BLE001
                with context_cm:
                    logger.exception("Changeset operation failed")
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
        done_event = self.done_event
        if done_event is not None:
            self.task.add_done_callback(lambda _: done_event.set())
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
        semaphore = self.semaphore
        assert semaphore is not None
        self._tasks_queued += 1
        self.log_task_progress()

        async def runner():
            async with semaphore:
                self._tasks_queued -= 1
                self._tasks_running += 1
                self.log_task_progress()
                try:
                    if self.cancel_event and self.cancel_event.is_set():
                        raise asyncio.CancelledError()
                    if callable(coro_or_factory):
                        coro = coro_or_factory()
                    else:
                        coro = coro_or_factory
                    return await coro
                finally:
                    self._tasks_running -= 1
                    self._tasks_finished += 1
                    self.log_task_progress()

        task = asyncio.create_task(runner())
        if self.tasks is None:
            self.tasks = []
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
                data_payload["stats"] = self.stats.model_dump(mode="json")
            self.data = data_payload

        if self.running_time_ms is None:
            now_ts = time()
            self.running_time_ms = int(now_ts * 1000) - int(self.id)

        self.status = status
        from katalog.db.changesets import get_changeset_repo

        db = get_changeset_repo()
        await db.save(self, update_data=data_payload)
        try:
            from katalog.api.state import event_manager

            event_manager.emit(
                int(self.id),
                "changeset_status",
                payload=self.model_dump(mode="json"),
            )
        except Exception:
            logger.exception("Failed to emit changeset status event")


class ChangesetActor(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    changeset_id: int
    actor_id: int


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
