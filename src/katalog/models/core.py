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
from asyncio import Task
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, UTC
from enum import Enum, IntEnum
from time import time
from typing import Any, Mapping

from loguru import logger
from tortoise.transactions import in_transaction
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
            "config_toml": getattr(self, "config_toml", None),
            "disabled": bool(getattr(self, "disabled", False)),
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
    id = IntField(pk=True)
    actor = ForeignKeyField(
        "models.Actor", related_name="changesets", on_delete=CASCADE, null=True
    )
    note = CharField(max_length=512, null=True)
    started_at = DatetimeField(default=lambda: datetime.now(UTC))
    completed_at = DatetimeField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    metadata = JSONField(null=True)

    # Local fields not persisted to DB
    stats: ChangesetStats
    tasks: list[Task]
    # Control concurrency of changeset (processor) tasks
    semaphore: asyncio.Semaphore
    # Just for type checking
    actor_id: int

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._init_runtime_state()

    def _init_runtime_state(self) -> None:
        # Safe defaults so instances materialized from the ORM always have runtime fields.
        self.stats = getattr(self, "stats", ChangesetStats())
        self.tasks = getattr(self, "tasks", [])
        self.semaphore = getattr(
            self, "semaphore", asyncio.Semaphore(DEFAULT_TASK_CONCURRENCY)
        )

    def to_dict(self) -> dict:
        # Note needs to have been fetched related 'actor' beforehand
        actor = getattr(self, "actor", None)
        return {
            "id": self.id,
            "actor_id": self.actor_id,
            "actor_name": actor.name if actor else None,
            "note": self.note,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
            "status": self.status.value
            if isinstance(self.status, OpStatus)
            else str(self.status),
            "metadata": self.metadata,
        }

    @classmethod
    async def find_partial_resume_point(cls, *, actor: Actor) -> "Changeset | None":
        """
        Return the most recent PARTIAL changeset that occurred after the latest
        COMPLETED changeset for this actor. If no COMPLETED changeset exists,
        return None (treat as full scan).
        """
        changesets = list(
            await cls.filter(actor=actor).order_by("-completed_at", "-started_at")
        )
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
        metadata: Mapping[str, Any] | None = None,
        actor: Actor | None = None,
        changeset_id: int | None = None,
        note: str | None = None,
    ) -> "Changeset":
        # Prevent concurrent in-progress changesets (scans or edits).
        existing_in_progress = await cls.get_or_none(status=OpStatus.IN_PROGRESS)
        if existing_in_progress is not None:
            raise ValueError(
                f"Changeset {existing_in_progress.id} is already in progress; finish or cancel it first"
            )
        changeset_id = changeset_id or int(time())
        if await cls.get_or_none(id=changeset_id):
            raise ValueError(f"Changeset with id {changeset_id} already exists")
        return await cls.create(
            id=changeset_id,
            actor=actor,
            status=status,
            note=note,
            metadata=dict(metadata) if metadata else None,
        )

    async def finalize(self, *, status: OpStatus) -> None:
        # Ensure runtime fields exist even if object was loaded from DB outside __init__ context.
        if not hasattr(self, "tasks"):
            self._init_runtime_state()

        completed_at = datetime.now(UTC)
        metadata_payload: dict[str, Any] | None = None

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

        if self.stats is not None or self.metadata is not None:
            metadata_payload = dict(self.metadata or {})
            if self.stats is not None:
                metadata_payload["stats"] = self.stats.to_dict()
            self.metadata = metadata_payload

        async with in_transaction():
            update_fields = ["completed_at", "status"]
            if self.note is not None:
                update_fields.append("note")
            if metadata_payload is not None:
                update_fields.append("metadata")
            self.status = status
            self.completed_at = completed_at
            await self.save(update_fields=update_fields)

    @classmethod
    @asynccontextmanager
    async def context(
        cls,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
        actor: Actor | None = None,
        changeset_id: int | None = None,
        note: str | None = None,
        success_status: OpStatus = OpStatus.COMPLETED,
        error_status: OpStatus = OpStatus.ERROR,
    ):
        """
        Async context manager for Changeset lifecycle.

        Usage:
            async with Changeset.context(actor=..., metadata=...) as snap:
                # do work, snap is a Changeset instance
        On normal exit -> finalize with success_status.
        On CancelledError -> finalize with CANCELED and re-raise.
        On other exceptions -> finalize with error_status and re-raise.
        """
        changeset = await cls.begin(
            status=status,
            metadata=metadata,
            actor=actor,
            changeset_id=changeset_id,
            note=note,
        )
        try:
            yield changeset
        except asyncio.CancelledError:
            # best-effort finalize as cancelled, then re-raise
            try:
                await changeset.finalize(status=OpStatus.CANCELED)
            except Exception as exc:  # keep exception simple and log
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after cancellation"
                )
            raise
        except Exception:
            # error path: finalize as error (or custom error_status), then re-raise
            try:
                await changeset.finalize(status=error_status)
            except Exception as exc:
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after error"
                )
            raise
        else:
            # normal completion
            try:
                await changeset.finalize(status=success_status)
            except Exception as exc:
                # If finalization fails on success, log and re-raise so caller is aware
                logger.opt(exception=exc).error(
                    "Failed to finalize changeset after success"
                )
                raise

    class Meta(Model.Meta):
        indexes = (
            # Used by server.list_changesets(), server.get_actor(), and Changeset.find_partial_resume_point().
            ("actor", "started_at"),
        )


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
