from __future__ import annotations

import contextvars
import os
import time
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, AsyncIterator

from loguru import logger

DEFAULT_PROCESSOR_CONCURRENCY = max(4, (os.cpu_count() or 4))
DEFAULT_THREAD_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY
DEFAULT_PROCESS_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY
PROCESS_EXECUTOR_GRACE_SECONDS = 5.0
PROCESS_EXECUTOR_TERMINATE_SECONDS = 2.0
PROCESS_EXECUTOR_FINAL_JOIN_SECONDS = 1.0
PROCESS_EXECUTOR_JOIN_POLL_SECONDS = 0.05

_THREAD_EXECUTOR: ThreadPoolExecutor | None = None
_PROCESS_EXECUTOR: ProcessPoolExecutor | None = None
_GLOBAL_CPU_PROCESSORS_SEEN: set[str] = set()
_EXECUTOR_SCOPE: contextvars.ContextVar["ExecutorScope | None"] = contextvars.ContextVar(
    "katalog_processor_executor_scope",
    default=None,
)


class ExecutorScope:
    def __init__(self) -> None:
        self.thread_executor: ThreadPoolExecutor | None = None
        self.process_executor: ProcessPoolExecutor | None = None
        self.cpu_processors_seen: set[str] = set()

    def get_thread_executor(self) -> ThreadPoolExecutor:
        if self.thread_executor is None:
            self.thread_executor = ThreadPoolExecutor(
                max_workers=DEFAULT_THREAD_CONCURRENCY
            )
        return self.thread_executor

    def get_process_executor(self) -> ProcessPoolExecutor:
        if self.process_executor is None:
            self.process_executor = ProcessPoolExecutor(
                max_workers=DEFAULT_PROCESS_CONCURRENCY
            )
        return self.process_executor

    def shutdown(self, *, wait: bool = True) -> None:
        if self.thread_executor is not None:
            self.thread_executor.shutdown(wait=wait, cancel_futures=True)
            self.thread_executor = None
        if self.process_executor is not None:
            _shutdown_process_executor(
                self.process_executor,
                wait=wait,
                owner="cli",
                observed_processors=self.cpu_processors_seen,
            )
            self.process_executor = None


@asynccontextmanager
async def processor_executor_scope() -> AsyncIterator[None]:
    """
    Scope processor executors to a bounded context, primarily for one-shot CLI runs.
    """
    current_scope = _EXECUTOR_SCOPE.get()
    if current_scope is not None:
        # Reuse the current owner when lifespans are nested.
        yield
        return

    scope = ExecutorScope()
    token = _EXECUTOR_SCOPE.set(scope)
    try:
        yield
    finally:
        # Always tear down pools before returning control to interpreter shutdown.
        scope.shutdown(wait=True)
        _EXECUTOR_SCOPE.reset(token)


def get_thread_executor() -> ThreadPoolExecutor:
    scoped = _EXECUTOR_SCOPE.get()
    if scoped is not None:
        return scoped.get_thread_executor()
    global _THREAD_EXECUTOR
    if _THREAD_EXECUTOR is None:
        _THREAD_EXECUTOR = ThreadPoolExecutor(max_workers=DEFAULT_THREAD_CONCURRENCY)
    return _THREAD_EXECUTOR


def get_process_executor() -> ProcessPoolExecutor:
    scoped = _EXECUTOR_SCOPE.get()
    if scoped is not None:
        return scoped.get_process_executor()
    global _PROCESS_EXECUTOR
    if _PROCESS_EXECUTOR is None:
        _PROCESS_EXECUTOR = ProcessPoolExecutor(max_workers=DEFAULT_PROCESS_CONCURRENCY)
    return _PROCESS_EXECUTOR


def shutdown_executors(*, wait: bool = True) -> None:
    """Shutdown shared process-wide executors."""
    global _THREAD_EXECUTOR, _PROCESS_EXECUTOR

    if _THREAD_EXECUTOR is not None:
        _THREAD_EXECUTOR.shutdown(wait=wait, cancel_futures=True)
        _THREAD_EXECUTOR = None

    if _PROCESS_EXECUTOR is not None:
        _shutdown_process_executor(
            _PROCESS_EXECUTOR,
            wait=wait,
            owner="process",
            observed_processors=_GLOBAL_CPU_PROCESSORS_SEEN,
        )
        _PROCESS_EXECUTOR = None
    _GLOBAL_CPU_PROCESSORS_SEEN.clear()


def record_cpu_processor(plugin_id: str | None) -> None:
    if not plugin_id:
        return
    scoped = _EXECUTOR_SCOPE.get()
    if scoped is not None:
        scoped.cpu_processors_seen.add(plugin_id)
        return
    _GLOBAL_CPU_PROCESSORS_SEEN.add(plugin_id)


def _shutdown_process_executor(
    executor: ProcessPoolExecutor,
    *,
    wait: bool,
    owner: str,
    observed_processors: set[str],
) -> None:
    workers = _snapshot_worker_processes(executor)
    executor.shutdown(wait=False, cancel_futures=True)
    if not wait:
        return

    alive = _wait_for_workers_exit(workers, timeout_seconds=PROCESS_EXECUTOR_GRACE_SECONDS)
    if not alive:
        return

    logger.warning(
        "ProcessPoolExecutor did not shut down cleanly in {owner} mode after {seconds:.2f}s; terminating workers pids={pids} processors={processors}. "
        "A processor likely left background threads running and should be fixed.",
        owner=owner,
        seconds=PROCESS_EXECUTOR_GRACE_SECONDS,
        pids=_worker_pids(alive),
        processors=sorted(observed_processors),
    )
    _stop_workers(alive, signal="terminate")
    alive = _wait_for_workers_exit(
        alive, timeout_seconds=PROCESS_EXECUTOR_TERMINATE_SECONDS
    )
    if alive:
        logger.warning(
            "ProcessPoolExecutor workers ignored terminate; killing workers pids={pids}",
            pids=_worker_pids(alive),
        )
        _stop_workers(alive, signal="kill")
        alive = _wait_for_workers_exit(
            alive, timeout_seconds=PROCESS_EXECUTOR_FINAL_JOIN_SECONDS
        )
        if alive:
            logger.warning(
                "ProcessPoolExecutor workers still alive after forced kill pids={pids}",
                pids=_worker_pids(alive),
            )


def _snapshot_worker_processes(executor: ProcessPoolExecutor) -> list[Any]:
    processes = getattr(executor, "_processes", None)
    if not isinstance(processes, dict):
        return []
    workers: list[Any] = []
    for process in processes.values():
        if process is not None:
            workers.append(process)
    return workers


def _wait_for_workers_exit(workers: list[Any], *, timeout_seconds: float) -> list[Any]:
    alive = _alive_workers(workers)
    if not alive or timeout_seconds <= 0:
        return alive
    deadline = time.monotonic() + timeout_seconds
    while alive and time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        join_timeout = min(PROCESS_EXECUTOR_JOIN_POLL_SECONDS, max(0.0, remaining))
        for process in alive:
            try:
                process.join(timeout=join_timeout)
            except Exception:
                continue
        alive = _alive_workers(workers)
    return alive


def _alive_workers(workers: list[Any]) -> list[Any]:
    alive: list[Any] = []
    for process in workers:
        try:
            if process.is_alive():
                alive.append(process)
        except Exception:
            continue
    return alive


def _stop_workers(workers: list[Any], *, signal: str) -> None:
    for process in workers:
        try:
            if signal == "kill":
                process.kill()
            else:
                process.terminate()
        except Exception:
            continue


def _worker_pids(workers: list[Any]) -> list[int]:
    pids = {int(process.pid) for process in workers if getattr(process, "pid", None)}
    return sorted(pids)
