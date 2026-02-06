# Performance Design: Scans + Processors

This document describes how `katalog` should behave when scanning sources, computing metadata
changes, running processors, and persisting results. The goal is to make performance characteristics
explicit so we can reason about scalability and where bottlenecks live.

## Goals

- Stream assets from sources without unbounded memory growth.
- Process assets in parallel where safe.
- Avoid database roundtrip bottlenecks (read + write).
- Scale across CPU cores for CPU-bound processors.
- Keep behavior consistent across client types (CLI, API, UI).

## Pipeline Overview

1. **Source scan**
   - Remote APIs or local filesystem emit assets and metadata.
   - Assets are staged in batches for persistence.
2. **Persist scan results**
   - Asset rows and metadata are written to the DB.
   - Changesets track all writes for undo/history.
3. **Processor pipeline**
   - For changed assets, run a sequence of processors.
   - Each processor may emit new metadata.
4. **Persist processor results**
   - Processor output is written to the DB, also via changesets.

## Parallelism Model

The conceptual model is "each asset can be processed independently." In theory, with enough cores,
we could process N assets concurrently.

In practice, two things limit that:

- **Database bottlenecks**
  - We must read existing metadata to decide what changed.
  - We must write metadata back to the DB.
  - These are shared resources, and many small roundtrips dominate time.

- **Python concurrency model**
  - `asyncio` provides concurrency for IO but does **not** provide CPU parallelism.
  - CPU-bound processors still run under the GIL unless moved to separate processes.

## Current Bottlenecks

- Per-asset DB reads (metadata fetch) and writes.
- CPU-bound processors running in the event loop (single-core).
- Lack of backpressure on source scans (can create huge queues).

## Design Principles

- **Stream, don’t accumulate**: keep in-memory working sets bounded.
- **Batch DB operations**: amortize roundtrip overhead.
- **Separate IO and CPU execution**:
  - IO processors should run in asyncio (concurrency).
  - CPU processors should run in worker processes (parallelism).
- **Stable interfaces**: client layers should call API functions, not DB internals.

## Proposed Execution Strategy

### Scans

- Maintain a bounded queue between scan and persistence.
- Persist in batches (e.g., 1k-5k assets per transaction).
- Avoid doing additional work during scan (e.g., FTS) unless it’s part of processors.

### Processors

- Classify processors by execution mode:
  - `io`: keep current async behavior.
  - `cpu`: run in a process pool.
  - `thread`: optional for libraries that block but aren’t CPU-heavy.
- Use separate concurrency limits per mode.

#### Processor Staging and Dependencies

Processors declare:

- **Dependencies**: metadata keys required to run.
- **Outputs**: metadata keys produced.

We build a dependency graph and then compute **stages** (topological layers):

- Processors in the same stage have no dependencies on each other and can run concurrently.
- Later stages depend on outputs from earlier stages and must run after them.

Execution model:

1. For each asset, iterate stages in order.
2. Within a stage, run all processors concurrently (per execution mode).
3. Collect stage outputs, merge into `MetadataChanges`, then proceed to next stage.

This keeps correctness (dependencies satisfied) while maximizing concurrency.

#### Caching Strategy

We want to minimize DB roundtrips and redundant computation:

- **Metadata read cache**:
  - Fetch metadata once per asset per processor run.
  - Reuse in-memory `MetadataChanges` across stages for the same asset.

- **Batch metadata prefetch**:
  - When processing a list of assets, load metadata in bulk for N assets at a time.
  - Reduces per-asset DB overhead.

- **Processor-level caches** (optional):
  - Processors may cache expensive lookups (e.g., file type maps, regexes).
  - Caches must be local to the worker (process/thread) to avoid contention.

- **Search indexing**:
  - Use a dedicated processor emitting `asset/search_doc`.
  - Persist via special-case handling in metadata persistence.

### Persistence

- Bulk insert metadata where possible.
- Use minimal per-asset reads.
- For special cases (e.g., search indexing), allow a pseudo-metadata path.

## Metrics We Track

- Total time (changeset running time).
- Scan time vs. persist time (scan-only runs).
- Memory usage (RSS).
- Per-stage throughput (assets/sec).

## Current Processor Flow (As Implemented)

This describes the current processor execution path as of the latest runtime refactor.

### Batch-Level Flow

1. `do_run_processors()` computes `batch_size` and iterates assets in batches (asset ids, provided
   assets list, or DB paging).
2. For each batch, `_process_batch()`:
   - Loads **all metadata for the batch** in one query via
     `metadata_repo.for_assets(..., include_removed=True)`.
   - For each asset, builds `MetadataChanges(loaded=...)` and schedules
     `process_asset_collect()` via `changeset.enqueue(...)`.
   - Awaits all asset tasks with `asyncio.gather`.
   - Persists **all staged changes for the batch** via
     `metadata_repo.persist_changes_batch(...)`.

### Per-Asset Flow (Multi-Stage Pipeline)

1. `process_asset_collect()` calls `_run_pipeline()`.
2. `_run_pipeline()` iterates **stages in dependency order**.
3. For each stage:
   - Evaluates `should_run()` for each processor.
   - Runs all eligible processors **concurrently** per asset using execution mode:
     - `io`: direct async run
     - `thread`: `ThreadPoolExecutor`
     - `cpu`: `ProcessPoolExecutor` (with JSON/Pydantic payloads)
   - Collects stage outputs and updates the in-memory `MetadataChanges`.
4. After the last stage, the accumulated `MetadataChanges` is returned for persistence.

### Serialization Details

CPU-mode processors are executed in a worker process and the payloads are serialized with
Pydantic JSON dumps (`model_dump(mode="json")`) and restored with `model_validate(...)`.

## Proposal: Improved Batch Ordering (Optional)

The current flow is correct and safe, but it creates some avoidable overhead:

- **Context switching**: per-asset `asyncio.gather` per stage multiplies task scheduling overhead.
- **Serialization cost**: CPU-mode processors serialize/deserialise per asset per processor.
- **Cache locality**: processors operate per asset rather than per batch, reducing locality of
  hot code/data in worker processes.

### Proposed Alternative Ordering

Maintain the same staging semantics but run **stage-by-stage across the batch**:

1. For each stage:
   - Dispatch all processors in the stage across **all assets in the batch**.
   - Update per-asset `MetadataChanges` in memory.
2. Persist all changes once after the last stage (same as today).

### Expected Benefits

- Reduced event-loop task churn by batching work at stage granularity.
- Better cache locality inside CPU workers (processes) and lower serialization overhead.
- Still respects dependencies (stage ordering is unchanged).

### Tradeoffs

- More complex orchestration (needs per-stage batch scheduling).
- Some memory pressure (must hold `MetadataChanges` for the batch across stages).

## Open Questions

- How to batch metadata reads for processor runs efficiently?
- How to prioritize DB writes vs. processor throughput?
- What is the correct default batch size for different workloads?
