from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any

from loguru import logger

DEFAULT_PROCESSOR_CONCURRENCY = max(4, (os.cpu_count() or 4))
DEFAULT_THREAD_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY
DEFAULT_PROCESS_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY
PROCESS_EXECUTOR_SHUTDOWN_GRACE_SECONDS = 5.0
PROCESS_EXECUTOR_CANCEL_GRACE_SECONDS = 0.5
PROCESS_EXECUTOR_TERMINATE_SECONDS = 2.0
PROCESS_EXECUTOR_FINAL_KILL_WAIT_SECONDS = 1.0
PROCESS_EXECUTOR_JOIN_POLL_SECONDS = 0.05


class ProcessorExecutorBundle:
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

    def record_cpu_processor(self, plugin_id: str | None) -> None:
        if plugin_id:
            self.cpu_processors_seen.add(plugin_id)

    def shutdown(self, *, cancelled: bool) -> None:
        if self.thread_executor is not None:
            self.thread_executor.shutdown(
                wait=not cancelled,
                cancel_futures=True,
            )
            self.thread_executor = None
        if self.process_executor is not None:
            _shutdown_process_executor(
                self.process_executor,
                cancelled=cancelled,
                observed_processors=self.cpu_processors_seen,
            )
            self.process_executor = None
        self.cpu_processors_seen.clear()


def _shutdown_process_executor(
    executor: ProcessPoolExecutor,
    *,
    cancelled: bool,
    observed_processors: set[str],
) -> None:
    workers = _snapshot_worker_processes(executor)
    executor.shutdown(wait=False, cancel_futures=True)
    grace_seconds = (
        PROCESS_EXECUTOR_CANCEL_GRACE_SECONDS
        if cancelled
        else PROCESS_EXECUTOR_SHUTDOWN_GRACE_SECONDS
    )
    alive = _wait_for_workers_exit(workers, timeout_seconds=grace_seconds)
    if not alive:
        return

    phase = "cancelled" if cancelled else "normal"
    logger.warning(
        "ProcessPoolExecutor did not shut down cleanly during {phase} shutdown after {seconds:.2f}s; "
        "terminating workers pids={pids} processors={processors}. A processor likely left background threads "
        "running and should be fixed.",
        phase=phase,
        seconds=grace_seconds,
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
            alive, timeout_seconds=PROCESS_EXECUTOR_FINAL_KILL_WAIT_SECONDS
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
    return [process for process in processes.values() if process is not None]


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
