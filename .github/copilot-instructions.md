# Katalog Copilot Instructions

## Workspace & Runtime

- Always run inside a workspace folder that contains `katalog.toml`, credentials, and the SQLite DB;
  set `KATALOG_WORKSPACE` or invoke `python -m katalog.cli /path/to/workspace` so
  `src/katalog/config.py` resolves the correct location.
- The CLI (`src/katalog/cli.py`) bootstraps uvicorn with autoreload and injects `<repo>/src` onto
  `sys.path`; prefer running it over calling uvicorn directly to avoid import issues.
- Server startup (`src/katalog/server.py`) is side-effectful: it creates/initializes `katalog.db`,
  loads `katalog.toml`, populates the `sources` table, and caches source clients in-process—avoid
  importing this module in tests unless the workspace env vars are set.
- FastAPI endpoints: `POST /snapshot/{source_id}` scans a source and writes rows via
  `Database.upsert_file_record`, `GET /files/{source_id}?view=flat|complete` reads metadata via
  `Database.list_records_with_metadata`.
- Reuse the HTTP file `tests/scan_local_list.http` to exercise these endpoints without wiring a full
  UI.

## Configuration & Sources

- Workspace config lives in `katalog.toml`; each `[[sources]]` entry must define a unique `id` and
  `class` (either absolute like `katalog.clients.filesystem.FilesystemClient` or relative such as
  `clients.filesystem.FilesystemClient`, which `katalog.utils.utils.import_client_class`
  auto-prefixes).
- `server._ensure_sources_registered` stores source metadata in SQLite using the plugin’s
  `PLUGIN_ID` (falls back to module path); keep IDs stable because `file_records` keys embed the
  source id.
- Workspace folders such as `hg_workspace/` include `credentials.json`/`token.json`; Google Drive
  tokens are persisted next to `katalog.db`, so new cloud connectors should also read/write secrets
  relative to `WORKSPACE`.
- Sample filesystem source (`src/katalog/clients/filesystem.py`) demonstrates emitting metadata
  (`file/absolute_path`, `time/modified`, etc.) via `FileRecord.add_metadata`; follow the
  `<category>/<property>` naming convention from `DESIGN.md`.
- Google Drive client (`src/katalog/clients/googledrive.py`) is asynchronous, paginates via Google
  APIs, and caches folder metadata in `*_folder_cache.pkl`; treat similar long-running clients as
  async generators that occasionally `await asyncio.sleep(0)` to yield control.

## Database & Metadata

- Persistence is centralized in `src/katalog/db.py`; `Database` wraps a single sqlite3 connection
  guarded by a `Lock`, so always go through its methods instead of writing SQL elsewhere.
- Schema uses an EAV table (`metadata_entries`) with one-hot typed columns;
  `MetadataValue.as_sql_columns` handles coercion, so emit metadata through
  `FileRecord.add_metadata` and let `Database._insert_metadata` serialize it.
- Snapshots are millisecond timestamps (`_generate_snapshot_id`) and control soft deletes:
  `finalize_snapshot` sets `deleted_snapshot_id` for records not seen in the latest run—never mutate
  those columns manually.
- Metadata lookups default to a “flat” view that collapses duplicate IDs; pass `view="complete"`
  when callers need provenance (`plugin_id`, `confidence`).

## Plugin & Processor Patterns

- All source connectors inherit from `SourceClient` (`src/katalog/clients/base.py`) and must
  implement `scan()` as an async iterator returning fully-populated `FileRecord` objects with stable
  `record.id` values; include a deterministic ID derived from provider-native keys (see inode/device
  example in `FilesystemClient`).
- Attach data accessors via `record.attach_accessor(client.get_accessor(record))` when processors
  need file bytes; `populate_accessor` in `src/katalog/utils/utils.py` provides a helper once you
  have a `source_id → client` map.
- Metadata processors live in `src/katalog/processors/` and implement the `Processor` Protocol
  (`dependencies`/`outputs` are validated against `FileRecord` fields). `utils.sort_processors`
  performs a topological order so producers run before consumers—keep `outputs` accurate to avoid
  dependency cycles.
- Processors such as `MimeTypeProcessor` assume upstream fields (`checksum_md5`) are set; when
  adding FileRecord attributes (e.g., `size_bytes`, `mtime`), update `katalog/models.py` so
  validators accept them.
- Cache behavior: each processor should expose `cache_key()` and `should_run()` to skip redundant
  work; reference `processors/md5_hash.py` for a complete implementation.

## Development Workflow

- Install dependencies via `uv pip install -e .` (or `pip install -e .`); run formatting/tests
  manually since no automation is wired yet.
- When debugging scans, enable `loguru` debug logging or add contextual
  logs—`server.snapshot_source` already logs processed counts per run.
- For schema changes, modify `SCHEMA_STATEMENTS` in `src/katalog/db.py` and run the CLI once to
  auto-apply (there’s no migration runner yet), keeping backwards compatibility in mind.
- Keep instructions in `DESIGN.md` aligned with implementation when introducing new concepts
  (assets, processors) so future agents understand naming and relationships.
