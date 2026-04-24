# Asset Identity and Merge PRD

## Status

Draft.

## Summary

Katalog can now scan file-like sources and row-like tabular sources. This makes identity more
important: the system must distinguish a stable record from one source from the real-world asset,
product, work, or file that may be described by multiple source records.

The product direction is to keep ingestion stable and provenance-preserving, then add explicit tools
for manual and query-driven entity resolution. Sources should be able to rescan without creating
duplicate records. Multiple sources should be able to contribute metadata to one effective asset
when the user chooses to merge them.

## Problem

Users combine asset data from local filesystems, Google Drive, CSV files, Google Sheets, URLs, and
other future sources. Each source can describe the same real-world thing with different identifiers
and different metadata quality.

Examples:

- A product table has a barcode and product name.
- A second sheet has product names but no barcode.
- A Drive folder has product images with filenames that partly match product names.
- A CSV export has stale or incorrect barcodes.

The current model upserts source records by `assets.namespace + assets.external_id` and can merge
records by setting `assets.canonical_asset_id`. The target terminology should rename
`external_id` to `source_id`: the source id is how the source identifies the record in its own
system, while `assets.id` is how Katalog identifies the record in its database. The existing model
is a useful base, but source plugins need a clearer identity contract and users need a simple path
from grouped candidate views to merge actions and combined export.

## Goals

- Scanning the same source repeatedly should update existing source records whenever the source
  provides stable identity.
- Source records without global identifiers must still be stored and remain usable.
- Multiple source records can be merged into one effective asset without losing provenance.
- Strong identifiers such as barcode, SKU, ISBN, canonical URL, or content hash can be used in
  grouped views that help users find records to merge.
- Users and AI agents can merge, unmerge, and explain merge decisions.
- Views and exports can show combined metadata with provenance.

## Non-Goals

- Do not build a full MDM platform in the first iteration.
- Do not require every source to provide a global identifier.
- Do not overwrite source metadata during merges.
- Do not push identity changes back to external systems in this PRD.
- Do not require backwards compatibility for experimental APIs until the implementation is agreed.

## Current Model

`assets.id` is the workspace-local integer row id for one source record.

`assets.namespace + assets.external_id` is the current source-record identity used for upserts. The
target schema name should be `assets.namespace + assets.source_id`.

Examples:

- Google Drive: namespace `gdrive`, source id is the Drive file id.
- Filesystem: namespace based on filesystem device where possible, source id prefers inode.
- Tabular sources: namespace from source config, source id from configured `id_column`.

`assets.canonical_asset_id` is an existing merge primitive. Query paths use
`COALESCE(canonical_asset_id, id)` to treat merged source records as one effective asset.

Metadata rows carry actor provenance, changeset history, and value confidence. This already supports
multiple sources disagreeing about the same metadata key.

## Key Concepts

### Source Record

A source record is one thing emitted by one source scan: a file, row, object, URL, or similar
record. It must have source-local stable identity whenever possible.

### Effective Asset

An effective asset is what users see after merges. In the current implementation, this is
represented by the canonical row id: `COALESCE(assets.canonical_asset_id, assets.id)`.

### Identifier Claim

An identifier claim is metadata asserting that a source record has an identifier such as barcode,
SKU, ISBN, URL, content hash, or vendor id. Identifier claims are not primary keys. They are
evidence for merge decisions.

## User Stories

- As a user, I can scan the same CSV or Google Sheet repeatedly without duplicated rows when I have
  configured a stable row id.
- As a user, I can ingest a table even when some rows lack barcode or SKU.
- As a user, I can see records that are not merged into any other effective asset.
- As a user, I can group records by trusted identifiers and decide which records to merge.
- As a user, I can undo an incorrect merge.
- As a user, I can export a combined table of effective assets with provenance-aware values.
- As an AI agent, I can inspect grouped candidate duplicates and call normal merge or metadata-edit
  APIs when instructed.

## Requirements

### R1: Source Identity Contract

Every source plugin must document how it constructs `namespace` and source id. In the current schema
this value is stored as `assets.external_id`; in the target schema it should be renamed to
`assets.source_id`.

For tabular sources:

- `id_column` remains the recommended stable identity input.
- Support for composite row identity should be added, for example `id_columns = ["Brand", "SKU"]`.
- If no stable id is configured, the source may use a temporary row identity but must mark the scan
  as unstable in changeset data and emit a warning.
- Default tabular namespaces should be actor/source-specific to avoid accidental cross-source
  upserts.

Acceptance criteria:

- Two unrelated sheets with the same row ids do not collide by default.
- Reordering rows does not create new assets when a stable id column is configured.
- Scans without a stable id show a clear warning in changeset data and UI/API responses.

### R2: Identifier Metadata

Add built-in metadata definitions for common identity claims. Prefer standards-based identifiers
where possible, but keep local/vendor identifiers separate from global product identifiers.

- `identity/gtin`
- `identity/content_hash`
- `identity/source_record_id`
- `identity/sku`
- `identity/vendor_item_id`
- `identity/isbn`

`identity/gtin` should be the preferred field for globally registered trade item identifiers,
including GTIN-8, GTIN-12/UPC, GTIN-13/EAN, GTIN-14, and ISBN-13 values when the source treats the
book as a trade item. `identity/sku` should remain separate because SKUs are usually seller-local or
system-local identifiers, not globally unique product ids. `identity/isbn` can remain as a
book-specific identifier when book-domain semantics matter or when sources provide ISBN-10 values.

Identifier values must be normalized conservatively per type. For example, GTIN normalization may
strip whitespace and separators, but should not silently rewrite invalid check digits or infer a
different identifier system without preserving the source value.

Do not add `identity/canonical_url` in the first iteration. URLs used to locate source records
should be modeled as source locators (`assets.canonical_uri` or source metadata), while source
identity is represented by `namespace + source_id`. If a URL is itself the source's stable
identifier, the source may use the canonicalized URL as `source_id`. Domain-level URL identifiers
can be introduced later with clearer semantics, for example `web/canonical_url` for a canonical URL
declared by web content or `identity/official_url` for a user/domain asserted entity URL.

Acceptance criteria:

- Sources and column mappings can emit identifier metadata.
- Identifier metadata remains actor-scoped and changeset-versioned like all other metadata.
- Identifier claims can be searched, filtered, grouped, and used by candidate views.

### R3: Merge and Unmerge API

Add API-layer functions for merge operations:

- `merge_assets(canonical_asset_id: int, member_asset_ids: list[int], changeset_id?: int)`
- `unmerge_assets(member_asset_ids: list[int], changeset_id?: int)`
- `get_asset_merge_group(asset_id: int)`

The first iteration can update `assets.canonical_asset_id` directly. It should create or reuse a
manual changeset where possible, and it must avoid cycles.

Acceptance criteria:

- Merging two assets collapses them in list views.
- Original source rows remain queryable by direct id.
- Unmerge restores separate rows.
- Merge attempts that would create cycles or merge missing asset ids fail clearly.

## Source Plugin Identity Review

This section defines the first identity hardening work. The goal is stable source-record upserts,
not global entity matching.

### Filesystem

Current behavior:

- Namespace is `fs:<st_dev>` when the root path can be statted, otherwise `fs:<actor_id>`.
- Source id is `inode:<st_ino>` when available, otherwise `path:<scan_path>`.
- Canonical URI is the absolute file URI.

Assessment:

- Good default for POSIX filesystems because inode identity survives renames within the same
  filesystem.
- Path fallback is weaker and will create a new source record on rename.
- If a file is renamed but keeps the inode, current upsert behavior may keep the original
  `assets.canonical_uri`, which can break data readers.
- Inode reuse after deletion is possible, so source metadata such as size, modified time, and path
  should remain visible when reviewing suspicious changes.

Required changes:

- Update `assets.canonical_uri` on source-record upsert when the source emits a new URI for the same
  `namespace + source_id`.
- Document that path fallback is unstable and should produce a warning in scan data.
- Consider adding platform-specific stable ids for Windows file index and macOS volume identity
  later.

### Google Drive

Current behavior:

- Namespace is `gdrive`.
- Source id is the Google Drive file id.
- Canonical URI is the Drive web view link or file/folder URL.

Assessment:

- Good default. Drive file ids are the right source-record identity and survive renames and moves.
- Using a global `gdrive` namespace is acceptable because Drive ids are globally unique enough for
  this purpose, and the same Drive file seen by multiple actors should normally be one source record
  with metadata provenance from multiple actors.

Required changes:

- Update `assets.canonical_uri` on upsert if Drive returns a changed web view link.
- No namespace change needed.

### Google Cloud Storage

Current behavior:

- Namespace is `gcs:<bucket>`.
- Source id is the object name.
- Canonical URI is the object URI.

Assessment:

- Good default for mutable object identity: overwriting an object at the same name updates the same
  source record.
- Object rename is copy/delete in GCS, so a rename creates a new source record, which is expected.
- Some users may later want immutable identity by object generation, but that is a different mode.

Required changes:

- No immediate identity change.
- Consider a future `identity_mode = "object_name" | "generation"` option.

### CSV

Current behavior:

- Inherits tabular source behavior.
- Namespace defaults to `tabular`.
- Source id is the configured `id_column`.
- Canonical URI includes file URI, row number, and id.

Assessment:

- Repeated scans are stable only when `id_column` is truly durable.
- Default namespace `tabular` can collide across unrelated CSV and Sheets actors that share row ids.
- Canonical URI includes row number, so row reordering should update `canonical_uri` for the same
  source id.

Required changes:

- Change default namespace to actor/source-specific, for example `csv:<actor_id>` or a stable source
  fingerprint.
- Add optional composite identity columns.
- Update `assets.canonical_uri` on upsert.
- Add warnings when configured id values are missing or duplicated within one scan.

### Google Sheets

Current behavior:

- Inherits tabular source behavior.
- Namespace defaults to `tabular`.
- Source id is the configured `id_column`.
- Canonical URI includes spreadsheet URL, row number, and id.

Assessment:

- Same tabular strengths and weaknesses as CSV.
- Spreadsheet id and worksheet/range should be part of the default namespace unless the user
  explicitly opts into a shared namespace.

Required changes:

- Change default namespace to include actor id or spreadsheet id plus worksheet/range.
- Add optional composite identity columns.
- Update `assets.canonical_uri` on upsert.
- Add warnings when configured id values are missing or duplicated within one scan.

### URL List

Current behavior:

- Namespace defaults to `web`.
- Source id is the canonicalized URL.
- Canonical URI is the canonicalized URL.

Assessment:

- Good default when the URL itself is the source record.
- Multiple URL-list actors emitting the same URL collapse to one source record, which is usually
  useful.
- If a URL list is meant to represent row-specific records with additional list-local metadata, this
  source is too simple; JSON or tabular sources are better.

Required changes:

- No immediate identity change.
- Document that URL identity is global by default.

### JSON Document List

Current behavior:

- Namespace defaults to `web`.
- Source id is the map key when `records_are_map` is used, otherwise the canonicalized URL.
- Canonical URI is the canonicalized URL.

Assessment:

- URL fallback is good for web documents.
- Map-key identity with namespace `web` is risky because local keys can collide with unrelated JSON
  sources or even URL strings.

Required changes:

- If `records_are_map` is true, default namespace should be actor/source-specific unless configured.
- Consider an explicit `id_field` for JSON records, separate from `url_field`.
- Keep URL fallback in `web` namespace only when URL is the actual source-record identity.

### HTTP URL Recursive Metadata Source

Current behavior:

- Recursive source keeps the seed asset namespace and source id.
- It updates metadata such as final URL, content type, size, and modified time.

Assessment:

- Correct: this source enriches an existing URL-like asset rather than defining a new source-record
  identity.

Required changes:

- Update `assets.canonical_uri` on upsert when HTTP resolution discovers a final canonical URL.
- Ensure redirects are treated as locator metadata, not source-record id changes.

### Fake Assets

Current behavior:

- Namespace is configurable.
- Source id is deterministic from actor id and generated index.

Assessment:

- Good for tests and demos.

Required changes:

- None.

## Data Model Changes

Recommended incremental path:

1. Keep `assets` as the source-record table.
2. Keep `assets.canonical_asset_id` for applied merges.
3. Add metadata keys for identifier claims.
4. Rename `assets.external_id` to `assets.source_id` with a migration script.
5. Rename related model/API/UI labels from "external id" to "source id" while preserving backwards
   compatibility aliases where needed during migration.

## API Surface

Suggested API-layer functions:

- `merge_assets(payload)`
- `unmerge_assets(payload)`
- `get_asset_merge_group(asset_id)`
- `list_grouped_assets(group_by, query)` improvements needed for candidate match views

CLI, HTTP, and MCP wrappers should remain in sync for read/write operations that represent the same
API surface.

## UI Requirements

Initial UI can be simple:

- Show merge status in asset detail: unmerged, canonical, or member of merge group.
- Show all source records in a merge group.
- Show grouped candidate views such as duplicate content hashes or shared identifiers.
- Allow side-by-side comparison of assets within a group.
- Add export action for current query/view.

## Migration and Compatibility

Existing assets remain valid.

Migration steps:

1. Add new metadata definitions.
2. Add source identity warnings and namespace hardening.
3. Add a database migration script that renames `assets.external_id` to `assets.source_id` and
   recreates the unique index/constraint on `(namespace, source_id)`.
4. Migrate code, API schemas, CLI output, UI labels, and docs from "external id" to "source id".
5. Do not rewrite existing `assets.namespace` values automatically.
6. For tabular actors with namespace `tabular`, warn that future scans may need actor-specific
   namespace configuration to avoid cross-source collisions.

## Open Questions

- Should accepted merge operations be represented only by `assets.canonical_asset_id`, or also as
  relationship metadata for changeset audit?
- What is the minimum UI needed to make review safe?
- Should tabular sources allow missing ids by default, or require an explicit `unstable_identity`
  opt-in?
- Should existing tabular actors with namespace `tabular` be migrated automatically, or only warned?
- Should filesystem namespace be actor-specific by default, or should overlapping filesystem actors
  intentionally collapse the same inode into one source record?

## Rollout Plan

1. Documentation and terminology alignment.
2. Source identity hardening, especially tabular namespaces and canonical URI updates.
3. Identifier metadata definitions.
4. Manual merge/unmerge API.
5. Candidate grouped views.
6. Combined export.

## Success Metrics

- Repeated scans of stable sources do not increase asset count unexpectedly.
- Users can explain why two source records were merged.
- Users can recover from incorrect merges.
- Exported effective-asset tables include combined source metadata without losing provenance.
- Large workspaces can show candidate groups without all-pairs scans.
