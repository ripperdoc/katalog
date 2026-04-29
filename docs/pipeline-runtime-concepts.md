# Pipeline Runtime PRD (MVP)

Katalog must process large numbers of assets on a local machine with predictable memory use and good
throughput. This PRD defines the MVP runtime and workflow contracts.

## Problem Statement

Current runtimes are split by actor type and user-facing operations are inconsistent. We need one
workflow-centered runtime model that is simple, shareable, and performant for local SQLite-backed
workspaces.

## Goals

- Make workflows the single user-facing way to run source + processor operations.
- Keep memory bounded through batch-based execution.
- Avoid unnecessary processor work through dependency-aware skipping.
- Persist changes efficiently to SQLite.
- Keep behavior consistent across API, CLI, and UI.

## Non-Goals (MVP)

- Global analyzers across the full dataset.
- Strong ordering guarantees between batches or assets.
- Full resume/replay orchestration semantics.
- Defining processor internal concurrency implementation details.

## Workflow Model

### Workflow identity and format

- A workflow is defined by a workspace TOML file or a `WorkflowSpec` in code.
- Workflow ID is the file name (or explicit ID for `WorkflowSpec`).
- Workflow metadata includes `name` and optional `description`.

### Workflow input selectors

A workflow can produce its input asset stream from:

- One or more source actors.
- All assets in the workspace.
- A saved collection.
- An explicit set of asset IDs.

At run start, the user may override input selectors. If not overridden, workflow defaults are used.

### Workflow sync

Before execution, the runtime performs additive actor sync:

- Match workflow actors against DB actors by `identity_key`.
- Create missing actors.
- Update matched actor config from workflow definition.

Each run records the workflow reference in changeset metadata.

### Workflow lifecycle test cases

The workflow lifecycle must be validated with deterministic tests (preferably in-memory workspaces)
that cover:

1. First-time sync creates actors.
- Given an empty workspace actor table and a valid workflow, sync creates all workflow actors.

2. Re-sync is idempotent for unchanged workflows.
- Re-running sync with the same workflow does not create duplicate actors and does not change actor
  count.

3. Selective actor update.
- If only one actor definition changes (for example config or disabled flag), sync updates that
  actor and leaves non-changed actors untouched.

4. Identity key change creates a new actor.
- If a workflow actor changes `identity_key`, sync creates a new actor row and does not mutate the
  old actor identity.

5. Workflow discovery/listing.
- Workflow listing returns all discoverable workflow files in a workspace with correct status
  information.

6. `WorkflowSpec` parity with TOML workflows.
- The runtime accepts `WorkflowSpec` input and applies the same lifecycle/sync semantics as for
  file-based workflows.

7. Validation errors for invalid workflow definitions.
- Invalid TOML or invalid payloads fail fast with clear errors (unknown plugin id, bad policy
  values, malformed actor entries, duplicate identity keys, invalid config schema).

### Workflow execution test cases

The workflow execution model must be validated with deterministic tests for core behavior and
targeted integration tests for runtime/performance risks.

1. Basic end-to-end run completes.
- Given one source and one processor with valid config, workflow run finishes as completed.
- One changeset is created and workflow reference metadata is stored.

2. Dependency staging order is respected.
- Given processors A and B where B depends on A output, B runs only after A output is merged into
  the in-memory batch state.

3. Coarse skip behavior in default mode.
- If dependencies have not changed and processor `Actor.updated_at` has not advanced, processor is
  skipped.
- Skip counts increase and run counts do not.

4. `always_process` policy forces execution.
- With `policy.always_process = true`, processors run even when skip logic would normally skip.

5. Start-time `always_process` override precedence.
- Start option overrides workflow policy in both directions:
  - policy false + start true => run
  - policy true + start false => respect skip

6. Missing assets policy: `lost`.
- Unseen assets are marked lost (not deleted) and stats reflect lost counts.

7. Missing assets policy: `delete`.
- Unseen assets are deleted and stats reflect deletion/lost semantics.

8. Multi-batch execution correctness.
- Source emits multiple batches; all batches are processed and persisted without drops.

9. Processor failure handling.
- If a processor fails for one or more assets, run status and changeset error metadata follow the
  defined failure semantics.

10. Workflow cancellation behavior.
- Canceling an in-progress run leads to final canceled/partial status and no orphan in-progress
  changeset remains.

11. Run provenance completeness.
- Changeset metadata includes workflow identity (id/name/file), effective `always_process`, and
  runtime stats payload.

12. `WorkflowSpec` parity with TOML.
- Equivalent definitions in TOML and `WorkflowSpec` produce equivalent sync and execution behavior.

13. Recursive source flow (if recursion is enabled).
- Recursive seeds produce recursive batches and respect recursion depth limits.

14. Source-only workflows.
- Workflow without processors still completes and persists source metadata correctly.

15. Selector-only processing mode (when supported).
- Workflows using non-source selectors (all assets / collection / asset ids) run processors without
  requiring a source scan stage.

16. Bounded memory under larger synthetic runs.
- With fixed batch size, memory usage stays within expected bounds as asset count scales.

17. SQLite contention resilience (integration).
- Under higher concurrency and index writes, runtime avoids uncontrolled lock/pool failures and
  fails with diagnosable errors when it cannot proceed.

18. CLI synchronous run semantics.
- `workflows run` waits for completion and returns final status only after run termination.

19. REST async start semantics.
- REST start returns quickly with run reference; run continues in background and reaches terminal
  status via polling/events.

20. Invalid execution inputs fail fast.
- Unknown plugins, invalid config/schema, duplicate identity keys, and invalid policy values fail
  before run start, without leaking in-progress changesets.

## Runtime Execution Model

### Outer pipeline stages

Each workflow run executes in three stages:

1. Loading: read one batch of assets into memory.
2. Processing: run processor pipeline on the batch.
3. Persisting: write deltas for the batch to SQLite.

The runtime allows up to one batch in flight per stage.

### Batching and memory

- Batch size is configurable per workflow.
- Memory usage must be bounded by batch size and per-stage queue capacity.
- DB reads/writes are logically set-based per batch and may use bounded internal chunks.

### Processor contract

Each processor must:

- Accept a batch of assets.
- Declare `depends_on` metadata keys.
- Declare `outputs` metadata keys.
- Emit deltas only (not full asset rewrites).
- A processor is instantiated before or at the start of the workflow, this allows it to cache data
  between batches if it wants to

Each processor may define processor-specific concurrency settings.

### Data access strategy

- There is no dedicated downloading stage in MVP.
- Binary data should be fetched lazily on access through existing data-reader metadata contracts.
- This keeps the runtime simple and avoids unnecessary prefetch/download work.

### Processor skip contract (coarse invalidation)

For each asset and processor, runtime skips execution only when both are true:

- None of the processor dependency keys changed since the processor last successful output.
- `Actor.updated_at` for that processor actor has not changed since the processor last successful
  output.

This is intentionally coarse and may reprocess more than strictly necessary.

### Dependency staging

- Runtime builds a dependency graph from processor `outputs -> depends_on`.
- Processors run in topological stages.
- Deltas from stage N are merged into the in-memory working set before stage N+1 starts.

## Data Mutability Model

- Loaded asset snapshots are treated as read-only inputs.
- Per-asset working state is represented as `MetadataChanges` deltas.
- Persisting stage writes merged deltas only.

## Failure and Changeset Semantics

- Processors may retry transient internal failures.
- If a batch fails anywhere in the outer pipeline, it's discarded and the workflow should drain
  remaining work (e.g. if it was still persisting a batch, let it finish)
- Workflow run persists to one shared changeset.
- Aborted runs mark the changeset as partial.

## Ordering Semantics

- Correctness must not depend on batch order or in-batch asset order.
- Runtime may choose any order, but will likely just take them in the order received

## Observability Requirements

Runtime must expose:

- Per-batch start/end logs with timing.
- Per-batch asset counts and delta counts.
- Skip/run counts per processor.
- Progress by completed batches.
- Estimated total progress only when input cardinality is known.

Above follows the current logic for changeset stats, it's just that we can be less granular in
logging and progress.

## API/CLI/UI Requirements

- API endpoint to list workflows and read one workflow definition.
- API should include actor sync status for each workflow.
- CLI runs workflows by workflow ID/name through the same API path.
- UI lists workflows as first-class run targets.
- UI should allow running a workflow from asset table selection or collection context.
- Actor-specific run entry points are deprecated in UI for MVP workflow mode.

## Compatibility Policy

- Backward compatibility for runtime APIs is not required for this transition.
- DB schema compatibility is required. This PRD must not require changing existing SQLite schema.
- Existing data in `assets`, `metadata`, `actors`, and `changesets` must remain readable.
- Workflow run provenance should use existing extensible fields (for example changeset metadata
  payload) instead of new schema.

## Transition Plan (Current Codebase -> MVP PRD)

### Major code changes expected

1. Unify runtimes behind one workflow runner.
- Replace actor-specific run paths with one workflow execution path.
- Keep wrappers thin in CLI/API/UI so they all call the same workflow runner.

2. Simplify plugin abstract classes.
- Source-side plugin contract should focus on producing batches of assets/metadata inputs.
- Processor contract should be batch-first, with required declarations for `depends_on` and
  `outputs`.
- Remove or deprecate contracts that imply separate run models per actor type for this MVP.

3. Add dependency-stage planner.
- Build processor graph from `outputs -> depends_on`.
- Execute processors by topological stages per batch.

4. Introduce coarse skip implementation.
- Track dependency freshness per asset+processor output.
- Use `Actor.updated_at` as the only actor-level invalidation signal.

5. Normalize persistence path.
- Ensure all workflow writes flow through one set-based persistence path that supports bounded
  chunking internally.
- Keep single changeset semantics for a workflow run.

6. Align user-facing entry points.
- API: add workflow list/read/start as first-class operations.
- CLI: start by workflow ID/name.
- UI: promote workflows as run targets; remove actor-specific run controls in MVP mode.

### Rollout steps

1. Implement workflow runner and dependency-stage planner behind feature flag or internal toggle.
2. Migrate CLI/API to call workflow runner while keeping behavior parity where practical.
3. Update UI run flows to workflow-first.
4. Remove old actor-specific runtime paths after new path is stable.

### Explicitly out of transition scope

- DB schema migrations.
- Global analyzer orchestration.
- Resume/replay runtime redesign.

## Acceptance Criteria

1. A workflow file can be discovered, listed, and executed from API, CLI, and UI.
2. Actor sync maps workflow actors by `identity_key` and updates config before run.
3. Runtime executes the three-stage outer pipeline with bounded in-flight batches.
4. Processor staging honors declared dependencies.
5. Skip behavior uses dependency freshness plus `Actor.updated_at`.
6. Persisting writes batch deltas correctly when using internal chunking.
7. Batch failure marks run partial and does not report completed status.
8. One run produces one changeset with workflow reference metadata.
9. Progress and batch-level logs are available during run.

## Open Questions (Post-MVP)

- How global analyzers should integrate with this batch model.
