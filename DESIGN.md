# DESIGN

Make sure you start by reading `README.md` as it describes from a user's perspective what this app
is for.

## Design Principles

Here are some overall design principles `katalog` should follow:

- **Shoulder of giants**. There are already so many great tools and libraries. Try to build on
  what's battle tested, don't attempt to recreate the wheel.

- **Pluggable**. There are so many flavors of data sources and metadata processors, an easily
  pluggable system makes it easy for anyone to contribute and tweak it for their needs. Because the
  community can add new plugins, it means things like database tables need to be flexible to new
  data types coming from new plugins.

- **Modular**. A direct consequence of both above principles is that it must be easy to link
  together existing functionality but in different languages, formats and executables, and that it
  must be easy to add functionality following well defined interfaces. Modularity also reduces the
  risk of getting stuck in the wrong solutions as the tool develops. Finally, it is easier to
  develop in small teams and with AI when things can be solved in smaller contexts and single
  modules than across a complex system.

- **Simplicity**. Easier said than done, but the core building blocks of the design should be
  simple. Re-use wellknown concepts where possible, and try to boil things down to the essence.

- **Your data**. Convenience and business logic could imply that `katalog` should be a cloud-hosted
  SaaS service. And it may be offered like that, but a core principle must be that it's your data.
  Only then can you trust it to know about all your assets and files. Therefore it must be able to
  run it on a local machine or self-host it in a network you control.

## Core Concepts

### "Works"

A creative work (in Swedish, 'ett verk') is a document, a photo, a video; any piece of content
created by a human (or "creative agent"). It may exist in multiple versions, variants and in
multiple files, across multiple locations that we can call "assets" (think "files"). It's in the
nature of a work that it cannot be automatically and uniquely separated from other works. This is
because it's a matter of a definition or context when a work can be seen as a copy of another or be
seen as a separate (but perhaps similar) work. Also, works may contain or be contained in other
works, such as an image or a paragraph of text.

It's a central problem to solve in `katalog` how to manage works and not just a mass of files. The
naive starting point is simply that each file is a unique work, and even at that point `katalog` can
be very useful. But the longer term value comes when `katalog` can create relationships between
multiple files as either versions or variants of the same work. In this way, a work has a
one-to-many relation to assets, and metadata on the set of associated files tell a story about the
asset - where it exists, who created it, when and how.

### Asset

A recorded asset is typically a reference to a file, or something file-like. It is one instance of
digital data. By it's nature, it's a digital changeset in time of a "work", e.g that represents a
version or variant of the work. An asset can be located within a source using an identifier, often a
URI, and it may also exist in one (or more) path-like hierarchies. It comes with a set of metadata
that can tell us how and when the asset was created and modified, and much more.

Whenever we deal with data in `katalog` it's usually in the form of assets. Metadata is associated
with a asset, and relationships between assets define things like folders, clusters, duplicates,
variants, etc.

### Metadata

Each file record can have a large amount of metadata, some common among all files, some specific to
certain types of files or sources. Our system needs to handle the fact that there might be multiple
opinions for a certain piece of metadata that may not be in agreement, such as when the file was
last modified, which can be given differently by file system and file header metadata. We need to
track these different opinions of the same property and asset. The system needs to heuristically
pick one as the current "candidate" (in e.g. file listings), and when not easily resolved, a human
or smart agent could make an informed decision or provide new information using some metadata
setting tools.

Metadata also follows some kind of ontology, e.g. there is a reference catalogue of metadata that a
file record can have, and what such metadata actually means (e.g. size in bytes, created date). We
cannot however hardcode all metadata, but need to support plugins and users adding additional
metadata, with their own definitions.

### Relationships

When we say two files reference the same asset, are versions or duplicates of eachother, we are
talking about a relationship between two file records. Here are some of the relationships we want to
be able to track:

- **Version**: A set of files are changesets in time over the same asset, and could be seen as an
  ordered (linked) list or even a graph, as a version may split into multiple branches.
- **Variant**: Typically a variant is a file that is a derivative or can be generated from a
  `master` file, such thumbnail. In this way, a variant is typically connected to a single
  `version`. Variants can also be discovered by finding that while the content is not the same, it
  is very similar.
- **Directory**: A directory can be seen as (and represented as) a file record without any data of
  it's own, but other files are part of it. Other ways to represent essentially the same is a ZIP
  archive, a file bundle or image files attached inside a document file.
- **Duplicate**: A special case of version is when two or more file records have identical contents
  to eachother.
- **Link**: A file that is a link to another file (depends on SourceActor if this is seen as just
  one record or multiple records)
- **Asset**: Back to our original definition, essentially we could cluster all files that are
  variants, versions and duplicates of eachother and consider them the same asset. But probably we
  don't consider files in a folder to be the same asset because they are in the same folder.

Relationships are rarely exact. Several files may be similar but a human might deem some of them not
part of the same group. Different analysis plugins might come with different proposed clusterings.
We might want to click on one file record and see all associated files and their relation, or we
might want to browse all "groups" and see the files considered part of them, like a file browser or
a version tree.

### Actors and Plugins

An actor in our system is the instance of some piece of code that provides data into the system, so
that all data can be given "provenance". In basically all cases, an actor instance is implemented
with a plugin, even if that plugin just conveys data from a user into the system. The reason actors
are separate is because there may be multiple instances of an actor using the same plugin (e.g.
multiple Google Drives), and there might also in the future exist actors that aren't linked to a
plugin.

#### Sources

A source plugin is piece of code that reads assets from some repository, maybe by scanning a local
folder (or complete filesystem) or a remote filesystem, or it may be an API to a cloud storage or
CMS. Ultimately, it lists "file-like" bytestreams with associated metadata, in a flat or folder-type
structure. The settings on a source will control how files are accessed and catalogued from within
it. The whole user journey of `katalog` starts with the user adding a first Adapter, e.g. their
"Document" folder.

Whenever we scan a source, we create a changeset which adds new assets and/or metadata to the
system.

## Processors

A processor is a plugin which operates on a assets emitted by a source changeset. A process runs on
each record that has been added or updated, which allows them to run in paralell. In order to run
them efficiently, they have a `should_run` function which can look at what data has been updated and
decide whether it should run or not. It relies on the source to correctly tell it what has been
updated.

When the processor has run, which may take some time, it outputs a list of new metadata that can be
saved to the database using a new changeset.

Processors can be seen as "map" functions in a map-reduce paradigm.

Example processors: calculate file hash, flag a file based on naming patterns, extract binary inline
metadata, generate a summary.

## Analyzers

*NOTE: Analyzers are still WIP and their purpose and API may change*s

An analyzer is like a processor but for a whole set of assets - two all the way up to all assets
together. This allows them to compare assets against each other and create relationships between
them, as well as suggest batch updates. The output of an analyzer can be large amount of changes
across many assets, and should either be possible to review before applying, or it should be
appended as a changeset that isn't yet considered canonical, e.g. fully undoable.

Note that if we want an analyzer to apply something back to the source, e.g. things get more
complicated and there may not be possible to fully capture the proposed edit.

Example analyzers: mark duplicate files, find similar files, suggest re-organizations.

## Architecture

### Workspace model

A given user may have multiple workspaces. A workspace represents a separate database and all
related caches, such as downloaded files. Assets are only tracked and de-duplicated within a
workspace. A workspace can be backed up and moved around, and it contains all caches.

### Database model

We have several difficult requirements that means we need to carefully choose the right database and
database model:

- Handle 1M files with 30 pieces of metadata each, being able to insert/upsert maybe 500k per scan
  without causing large delays
- Be as space efficient as possible
- Handle an extensible number of metadata keys, atleast 30-50
- Handle multiple opinions from multiple actors on every metadata, on every asset
- Full version history on all metadata, enabling full undo, restore and editing of history
- Support full sorting, filtering and grouping on all metadata fields
- Support full text search and vector search (using necessary plugins)
- Respond to queries quickly, at least below 200ms
- With all of above, keep the schema and queries simple to create and understand, and make it easy
  to create plugins and parse the database with other tools

### Identifiers

`asset_id` is a string ID that is a workspace unique UUID that is assigned once the system, or a
human wants to define a tracked asset. That asset id is referred to by one or more file records to
link them together.

`actor_id` is a workspace unique readable string ID for the instance of a source along with it's
settings. It's not the same as the `plugin_id`, e.g. one workspace can have multiple Google Drive's
setup.

`plugin_id` is a Python package path that by definition has to be unique in the current imported
code. It should be globally unique but can't be enforced without a plugin registry.

`metadata_key` is a globally unique identifier for a type of metadata. It has the format
`<category>/<property>`, e.g. `time/created_at`. This allows us to easily filter metadata, e.g. all
from a specific plugin, all relating to `time` or all relating to a specific concept such as
`time/created_at`.

### Changesets

By considering asset and metadata tables as "append-only" EAV, and connecting each change to a
changeset id, we get the ability to undo any change and edit history after the fact. This makes it
very safe to do new scans and manual edits.

However there are a few important notes about how this works:

#### Metadata changesets

Take a metadata value like `access/owner`. It might seem at first a scalar, singular value. But in
fact it can have multiple values. On one hand, we may have multiple sources (`actor_id`s) that give
different values for `access/owner`. On top of that, some APIs might return multiple `access/owner`
for the same asset. As our metadata structure should allow for maximum modularity, we can never
assume a value is singular.

So over time, `access/owner` might both change a singular value to a different one, or we might add
an aditional value in addition to the previous, and we might remove a value but keeping the other.
And if we step back in time, we should always represent the value as it was from the actor at that
time.

If we have new a value but there was nothing before from that `actor_id`, we simple add a new row.

If we have a new value but it's different than the previous one for that `actor_id`, we add a new
row and the default queries will only show the most recent (maximum) changeset e.g. row for this
value.

If have a new value, from a different `actor_id`, we add it as a new row and a query might show both
values (but from different actors).

If we remove a value that was previously there, from same `actor_id`, we add a row with same value
as before but `removed=1`.

If the actor at some time later suddenly provides multiple values (e.g. an array) we compare the
values with the most recent changeset for same actor, and any values added are given a new row, any
values the same are ignored and any values removed are added as a row with `removed=1`.

Note, this approach does not allow us to store duplicate values, which would in theory be possible
in a list. E.g it's more correct that say that we store "sets" of metadata values. This would be a
very minor problem, and the escape hatch is to store it all as JSON.

#### Relationship changesets

Relationships are modeled as a metadata with a relation type. Therefore the same rules that apply
for metadata changesets apply here.

#### Removing changesets

Removing the last changeset, e.g. undo, is as straightforward as deleting the changeset and letting
the cascade delete all related rows.

To restore to a point in history, it just means deleting every changeset from now back to that point
in time.

The history can also be edited by removing changesets in the middle.

Finally, we may want remove some changesets to save space. The naive approach of removing the X
oldest changesets will not work, because as we record only changes the first changeset will contain
most data and subsequent ones will only contain changes. We could concatenate changesets, but that
effectively only saves space if values have been changing back and forth many times, to only contain
the last written value, e.g. a type of compression.

#### Changeset stats

A changeset can modify several things: assets, metadata associated with assets and relationships
between assets. It's very useful to gather some structured statistics for each changeset, which can
be logged or displayed in UI. Each stat is a number, and some of those numbers can be broken down
into smaller numbers, allowing us to render it as e.g. a chart. It's important that when a number is
broken down, it creates a complete breakdown, e.g. we need to return all parts not just some. Here
is the current assumption:

- Changeset stats
  - Assets seen
    - Assets changed
      - Breakdown A
        - Assets added
        - Assets modified
        - Assets deleted
      - Breakdown B
        - Assets processed
        - Assets not processed
    - Assets not changed
    - Assets ignored (e.g. through blacklisting)
  - Subassets seen (assets found within other assets)
    - Ass
    - Subassets ignored
  - Processings started
    - Partial
    - Completed
    - Cancelled
    - Skipped
    - Error
  - Metadata values affected
    - Metadata values added
    - Metadata values removed
    - Metadata values changed
  - Relations affected
    - Relations added
    - Relations removed
    - Relations changed

### Operations, e.g. running changesets

Whenever we begin a changeset, we are starting a long-running operation, and we treat the Changeset
class as also representing an ongoing or completed operation. These operations can be:

- Scanning one or more sources, and also processing them
- Running a list of processors against a list of assets
- Running an analyzer on a list of assets
- Starting a series of manual edits and then marking as done

There are some important requirements to list for these operations.

- Changesets are identified with an integer ID that is the current timestamp in milliseconds since
  epoch (1970)
- A Changeset can have a message that describes the change to humans (e.g. commit message)
- A Changeset has a 1-to-many relationship with Actors, e.g. one Changeset can include changes from
  one or many Actors
- A Changeset can contain unstructured data such a stats or side-effect outputs
- Most other entities have a relation to a Changeset to allow us to delete those changes along with
  the Changeset, and to be able to show all data changed by a Changeset
- Whenever we start an operation, we should return the in-progress Changeset ID and object to the
  client / UI, rather than wait for the Changeset to complete.
- In general, a Changeset operation includes many smaller running tasks that are sequential or
  paralell. These tasks should wherever possible persist their state to the database as they finish.
  There should be a simple interface for plugins to add tasks to the Changeset.
- The changeset should track planned, ongoing and finished tasks (e.g. asyncio.Task). Regularly the
  Changeset can emit an event that contains updated counts for these, so that a client can show a
  progress bar (Note all tasks may not be known upfront, so it would be a moving progress target)
- The client can call a streaming endpoint based on the Changeset ID to receive events in the shape
  of log lines and/or progress updates
- A Changeset cannot be started if the previous one is still in-progress, e.g. there is a global
  lock of just one operation at a time.
- A Changeset cannot be created if the previous one has the same ID, e.g. same timestamp
- Any other requirements that the operation requires should if possible be validated before starting
  the Changeset, so that we can return an error without creating a new Changeset
- An in-progress Changeset can be cancelled by calling that endpoint from the client. This should
  attempt to stop all work as soon as possible, while still allowing for smaller ongoing tasks to
  complete and allowing for cleanup. Any changes already persisted remain and are still associated
  with the changeset.
- A Changeset can also be discarded, this completely removes it and all data associated with it.
- Manual edits (e.g. editing metadata, removing items from a collection) must use the `User Editor`
  actor and create an in-progress changeset with `data.manual = true`. UI actions that perform
  manual edits should start or reuse the active manual changeset, apply edits against it, and allow
  users to Finish (complete) or Cancel (undo by deleting) the changeset.
- Manual edit actions should log progress messages (e.g. number of assets affected) using the
  changeset event stream so the floating changeset bar can show activity.

### Data views

Changesetting deals with how we add data, but an even more important activity is to allow the local
Web UI to read the data in the database. We strive for a UI that feels like a desktop UI, that can
handle very large amount of files with ease, and also gives a lot of flexibility to the user to
search, sort, filter and query this data. This is not so easy, because the way we structure the data
for easy changesetting and metadata flexibility also makes it a lot harder to read data in a
performant way.

The requirements from a UI perspective are:

- Can view all Assets combined with their current Metadata in a data table
  - Current metadata can mean different things, but for simplicity, it's the latest non-removed row
    for that metadata key
- The table can handle 1 million assets and several million metadata while still being very
  responsive (through virtualization, e.g. not trying to render all data together)
- The user can see a quick summary such as number of assets, metadata, etc in the current selected
  data
- The user can search as they type through the data and see the table adapt in realtime
  - At least the data visible in the table (as text), but even better through all current metadata
- The user can sort by most columns
- The user can create filters
- An advanced user could potentially write SQL queries (instead of trying to build too advanced
  filtering UI)
- The user can switch between different prepared views, that shows a subset of metadata and maybe
  also renders it differently

To achieve this, there are a few important technical choices to make:

- The data table component uses virtualization and has controls to both sort/filter locally but
  probably more importantly to the server
- That we have an efficient API to get results between table and server, including the flexibility
  for search, column sort and custom filters
- That the API also tells the table about the schema of the data, e.g. which metadata columns to
  render and how to render them (e.g. type, descriptions, etc)
- That the server can serve this very quickly using SQLite. We need to consider:
  - Where to use or not use TortoiseORM
  - What actions we take in SQL, and what we do in code
  - What to index on
  - Can we generate faster de-normalized tables and read from them?

### Asset collections

Users often want to keep a set of assets around, not just rerun a query. To keep this simple and
avoid overlapping concepts, we use a single persisted type:

- **AssetCollection**
  - Fields: `id`, `name`, `description?`, `asset_ids` (ordered), `source` (optional JSON),
    `refresh_mode` (`live` or `on_demand`), `created_at`, `updated_at`.
  - `source` JSON can store the query that produced the collection, the view/widget suggestion used
    when it was saved, and provenance metadata such as client or timestamp. We do not persist
    queries or views as first-class tables yet; they stay opaque in this JSON.
  - “Save search” in the UI creates an AssetCollection from the current result set and stores the
    query/view in `source` for future refresh.
  - Opening a collection: if `refresh_mode = live`, rerun `source.query` and rebuild the ordered
    `asset_ids` in memory (persist only when user explicitly saves the refresh); if `on_demand`,
    keep stored `asset_ids` until the user clicks Refresh.
  - Presentation: the UI may override `source.view` at render time to switch widgets; this does not
    change the stored collection unless saved again.

### Main server

The main module is implemented as a FastAPI server. It initializes the system, manages actors and
provides an HTTP API for actions such as scanning all or specific sources. The server tracks
asynchronous scan jobs using a simple job system and offers endpoints to retrieve job status and
results. As a scan progresses, it collects file information and can trigger processors on each
scanned file.

A CLI or a web app can connect to the FastAPI server to act as a UI.

## FAQ

### If two sources report files with the same MD5 hash, can we detect that via SQL?

Yes. Every processor that emits checksums writes them as metadata rows (e.g.
`metadata_key = "core/checksum/md5"`, `value_type = 'string'`, value stored in `value_text`).
Because `metadata` keeps the `asset_id`, we can join back to `assets` and spot duplicates across
sources with a single query:

```sql
SELECT me.value_text AS md5, GROUP_CONCAT(fr.id) AS asset_ids
FROM metadata me
JOIN assets fr ON fr.id = me.asset_id
WHERE me.metadata_key = 'core/checksum/md5'
GROUP BY me.value_text
HAVING COUNT(DISTINCT fr.actor_id) > 1;
```

This produces every MD5 value seen in multiple sources together with the affected file records,
allowing the deduper to merge them under a single `asset_id` or prompt a review workflow.

### Can we ingest file records before deciding which asset they belong to?

Yes. Both `assets.asset_id` and `metadata.asset_id` are nullable, so scanners can persist
discoveries immediately, even when we have not yet created or linked a canonical asset. The workflow
is typically:

1. Source plugin inserts a `assets` row with `asset_id = NULL` plus whatever metadata the actor
   offers (URIs, hashes, timestamps).
2. Processors emit metadata rows that still point at the `asset_id` (and leave `asset_id = NULL`).
3. When a deduper decides that the file should join (or create) an asset, it issues an `UPDATE` to
   set `asset_id` on both the `assets` row and any metadata rows referencing it.

Because of this, scanning is never blocked on asset decisions, and asset creation can be an async or
human-in-the-loop step performed later.

### What happens when a source stops returning a previously seen file?

`assets` keeps `last_seen_at` and `lost_at` timestamps, so every scan simply updates `last_seen_at`
for the rows it touched. After a crawl, any file whose `last_seen_at` predates the scan window is
implicitly missing from the source. We keep that row (and all related metadata) while marking it as
a soft delete by setting `lost_at = CURRENT_TIMESTAMP`. Queries that only care about live files
filter on `lost_at IS NULL`, while history/audit views can still surface the full record. Because
the row stays around, users can later “forget” it entirely (hard delete) or reconcile it if the file
reappears in a future scan.

### Can we leverage sources that provide incremental change feeds?

Yes. The schema keeps `first_seen_at`/`last_seen_at` plus actor identifiers (`actor_file_id`,
`canonical_uri`). When a connector supports “changes since T”, we store the checkpoint timestamp in
the `actors` table (inside `config` or a dedicated column) and ask the source only for files whose
modification time is newer than that value. The scanner then:

1. Upserts rows for returned files, bumping `last_seen_at` and updating any metadata that changed.
2. Leaves untouched rows whose `last_seen_at` already exceeds the incremental window, so they are
   implicitly up-to-date without re-fetching.
3. After the incremental pass, any row with `last_seen_at < last_checkpoint` is a candidate for soft
   deletion, identical to the full-scan logic.

Because the tables already expose the timestamps and uniqueness constraints we need, comparing the
incremental payload to existing `assets` becomes an O(changes) operation—no full rescan is required.

# Developing plugins

This section summarizes how to build and ship a plugin so that `katalog` can discover and run it.

## Registration and discovery

- Implement a subclass of `SourcePlugin`, `Processor`, or `Analyzer` from `katalog.plugins.*` and
  add `plugin_id` (package path), `title`, `description`, and optional `version` as properties on
  the class.
- Keep the constructor signature `__init__(actor, **config)`; `actor.config` is passed as the
  `config` dict when the plugin is instantiated.
- Publish the class through a Python entry point in your `pyproject.toml`. The groups the runtime
  scans are:
  - `[project.entry-points."katalog.source"]` for sources
  - `[project.entry-points."katalog.processor"]` for processors
  - `[project.entry-points."katalog.analyzer"]` for analyzers
- After `uv pip install -e .` in katalog (or installing the plugin package), plugins are picked up
  automatically via `importlib.metadata.entry_points`. Use
  `katalog.plugins.registry.refresh_plugins()` to reload without restarting a process.

## Plugin configuration

Each plugin should accept their Actor object from database to their `__init__` method. From that
actor object, it can read the config dict. The recommendation is to have a Pydantic model which
represents accepted configuration, and add it as class property e.g. `<PluginClass>.ConfigModel`.

Then in `__init__`, just validate the actor config and save the instantiated ConfigModel to `self`.

Keep keys snake_case, keep types descriptive (not necessarily Python types), and make `description`
succinct. The runtime can later render this metadata in UIs or validate configs without changing
plugin code.

## General plugin principles

- Try to make plugins focused on one thing and use limited dependencies
- We encourage to re-use dependencies from core `katalog` or our `utils` package.
- If your plugin need to persist files, call `actor_path` from `config.py` to ensure a subfolder
  that won't conflict with other actors.

## Sources

- Should not attempt to look up Assets from database, can just create a new Asset and it will be
  fixed when the changeset is saved.
- Must return correct scan results based on if the scan was cancelled, completed, etc
- Sources are not currently meant to know anything about the Asset and it's metadat from earlier
  scans. We also assume any metadata set from a scan are intended to replace any previous value from
  the same actor id.
- Should set the `data/file_reader` metadata key to signal that the file data can be read (through
  the Source). Value should be an empty object, or special arguments needed to find/locate the file
  data.

## Processors

- Correctly declare `dependencies`/`outputs` (processors), otherwise the processor may be skipped or
  cause unnecessary work. The dependencies/outputs should be conservative, e.g. if there is any
  chance that we depend on or change a field, it should be mentioned. If it should always be run,
  give an empty `depencencies`
- Strive for `run` be a pure functions that can be paralellized. We cannot block access to the
  database through TortoiseORM it should not be done
- `should_run` should be quick and optimistic, it may return false positives (e.g. said should run
  but wasn't necessary), but not false negatives (e.g. said shouldn't run but should've). Sometimes
  only the full algorithm in run can determine if a run was necessary or not
- If `run` cannot execute due to missing prerequisites (e.g. no data accessor, missing required
  metadata, unsupported file type), it should return `ProcessorResult(status=SKIPPED, message=...)`.
- A processor may return `COMPLETED` with an empty `metadata` list to mean “ran but produced no
  changes”.
- Exceptions from `run` should be reserved for unexpected failures; the runtime treats those as
  `ERROR` and logs them.
