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
digital data. By it's nature, it's a digital snapshot in time of a "work", e.g that represents a
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

- **Version**: A set of files are snapshots in time over the same asset, and could be seen as an
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
- **Link**: A file that is a link to another file (depends on SourceProvider if this is seen as just
  one record or multiple records)
- **Asset**: Back to our original definition, essentially we could cluster all files that are
  variants, versions and duplicates of eachother and consider them the same asset. But probably we
  don't consider files in a folder to be the same asset because they are in the same folder.

Relationships are rarely exact. Several files may be similar but a human might deem some of them not
part of the same group. Different analysis plugins might come with different proposed clusterings.
We might want to click on one file record and see all associated files and their relation, or we
might want to browse all "groups" and see the files considered part of them, like a file browser or
a version tree.

### Providers and Plugins

A provider in our system is the instance of some piece of code that provides data into the system,
so that all data can be given "provenance". In basically all cases, a provider instance is
implemented with a plugin, even if that plugin just conveys data from a user into the system. The
reason providers are separate is because there may be multiple instances of a provider using the
same plugin (e.g. multiple Google Drives), and there might also in the future exist providers that
aren't linked to a plugin.

#### Sources

A source plugin is piece of code that reads assets from some repository, maybe by scanning a local
folder (or complete filesystem) or a remote filesystem, or it may be an API to a cloud storage or
CMS. Ultimately, it lists "file-like" bytestreams with associated metadata, in a flat or folder-type
structure. The settings on a source will control how files are accessed and catalogued from within
it. The whole user journey of `katalog` starts with the user adding a first Adapter, e.g. their
"Document" folder.

Whenever we scan a source, we create a snapshot which adds new assets and/or metadata to the system.

## Processors

A processor is a plugin which operates on a assets emitted by a source snapshot. A process runs on
each record that has been added or updated, which allows them to run in paralell. In order to run
them efficiently, they have a `should_run` function which can look at what data has been updated and
decide whether it should run or not. It relies on the source to correctly tell it what has been
updated.

When the processor has run, which may take some time, it outputs a list of new metadata that can be
saved to the database using a new snapshot.

Example processors: calculate file hash, flag a file based on naming patterns, extract binary inline
metadata, generate a summary.

## Analyzers

An analyzer is like a processor but for a set of assets - two all the way up to all assets together.
This allows them to compare assets against each other and create relationships between them, as well
as suggest batch updates. The output of an analyzer can be large amount of changes across many
assets, and should either be possible to review before applying, or it should be appended as a
snapshot that isn't yet considered canonical, e.g. fully undoable.

Note that if we want an analyzer to apply something back to the source, e.g. things get more
complicated and there may not be possible to fully capture the proposed edit.

Example analyzers: mark duplicate files, find similar files, suggest re-organizations.

## Architecture

### Workspace model

A given user may have multiple workspaces. A workspace represents both a separate database and a
separate sets of configured sources and settings (`katalog.toml`). Assets are only tracked and
de-duplicated within a workspace. A workspace can be backed up and moved around, and it contains all
caches.

### Database model

See `db.py` for current database model. In general, we have one "narrow" table for assets and
another "narrow" table for metadata, where each metadata value is linked to a snapshot and a asset.
This makes it an Entity-Attribute-Value (EAV) type model. These tables should allow us to develop
new functionality over time without modifying table structures.

### Identifiers

`asset_id` is a string ID that is a workspace unique UUID that is assigned once the system, or a
human wants to define a tracked asset. That asset id is referred to by one or more file records to
link them together.

`provider_id` is a workspace unique readable string ID for the instance of a source along with it's
settings. It's not the same as the `plugin_id`, e.g. one workspace can have multiple Google Drive's
setup.

`plugin_id` is globally unique reversed DNS identifier for a plugin, e.g the implementation of a
source, e.g. `se.helmgast.gdrive`. This allows multiple plugins even for the same API or cloud
provider to co-exist.

`metadata_key` is a globally unique identifier for a type of metadata. It has the format
`<category>/<property>`, e.g. `time/created_at`. This allows us to easily filter metadata, e.g. all
from a specific plugin, all relating to `time` or all relating to a specific concept such as
`time/created_at`.

### Snapshots

By considering asset and metadata tables as "append-only" EAV, and connecting each change to a
snapshot id, we get the ability to undo any change and edit history after the fact. This makes it
very safe to do new scans and manual edits.

However there are a few important notes about how this works:

#### Metadata snapshots

Take a metadata value like `access/owner`. It might seem at first a scalar, singular value. But in
fact it can have multiple values. On one hand, we may have multiple sources (`provider_id`s) that
give different values for `access/owner`. On top of that, some APIs might return multiple
`access/owner` for the same asset. As our metadata structure should allow for maximum modularity, we
can never assume a value is singular.

So over time, `access/owner` might both change a singular value to a different one, or we might add
an aditional value in addition to the previous, and we might remove a value but keeping the other.
And if we step back in time, we should always represent the value as it was from the provider at
that time.

If we have new a value but there was nothing before from that `provider_id`, we simple add a new
row.

If we have a new value but it's different than the previous one for that `provider_id`, we add a new
row and the default queries will only show the most recent (maximum) snapshot e.g. row for this
value.

If have a new value, from a different `provider_id`, we add it as a new row and a query might show
both values (but from different providers).

If we remove a value that was previously there, from same `provider_id`, we add a row with same
value as before but `removed=1`.

If the provider at some time later suddenly provides multiple values (e.g. an array) we compare the
values with the most recent snapshot for same provider, and any values added are given a new row,
any values the same are ignored and any values removed are added as a row with `removed=1`.

Note, this approach does not allow us to store duplicate values, which would in theory be possible
in a list. E.g it's more correct that say that we store "sets" of metadata values. This would be a
very minor problem, and the escape hatch is to store it all as JSON.

#### Relationship snapshots

Relationships between assets are also saved by snapshot to allow us to walk back as needed. But the
nature of a relationship is a little bit different.

If we want to add a relationship A-(type)->B, and there was no such relation before for that
provider, we add a row.

If we want to then add a relationship A-(othertype)->B, it is also added.

If we analyze again and want to set the same relationship again, with the same provider, we should
detect that they are the same and not add a new row.

If we want to set that there is NO relationshop A-(type)->B, and it existed already for that
provider, we add a new row which sets this relationship as `removed=1`. However, a typical analyzer
would not explicitly say which files are _not_ related in a way, just say that somme files may be
related. Therefore it would be unusual to remove a relationship.

#### Removing snapshots

Removing the last snapshot, e.g. undo, is as straightforward as deleting the snapshot and letting
the cascade delete all related rows.

To restore to a point in history, it just means deleting every snapshot from now back to that point
in time.

The history can also be edited by removing snapshots in the middle.

Finally, we may want remove some snapshots to save space. The naive approach of removing the X
oldest snapshots will not work, because as we record only changes the first snapshot will contain
most data and subsequent ones will only contain changes. We could concatenate snapshots, but that
effectively only saves space if values have been changing back and forth many times, to only contain
the last written value, e.g. a type of compression.

### Main server

The main module is implemented as a FastAPI server. It initializes the system, manages providers and
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
HAVING COUNT(DISTINCT fr.provider_id) > 1;
```

This produces every MD5 value seen in multiple sources together with the affected file records,
allowing the deduper to merge them under a single `asset_id` or prompt a review workflow.

### Can we ingest file records before deciding which asset they belong to?

Yes. Both `assets.asset_id` and `metadata.asset_id` are nullable, so scanners can persist
discoveries immediately, even when we have not yet created or linked a canonical asset. The workflow
is typically:

1. Source plugin inserts a `assets` row with `asset_id = NULL` plus whatever metadata the provider
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

Yes. The schema keeps `first_seen_at`/`last_seen_at` plus provider identifiers (`provider_file_id`,
`canonical_uri`). When a connector supports “changes since T”, we store the checkpoint timestamp in
the `providers` table (inside `config` or a dedicated column) and ask the source only for files
whose modification time is newer than that value. The scanner then:

1. Upserts rows for returned files, bumping `last_seen_at` and updating any metadata that changed.
2. Leaves untouched rows whose `last_seen_at` already exceeds the incremental window, so they are
   implicitly up-to-date without re-fetching.
3. After the incremental pass, any row with `last_seen_at < last_checkpoint` is a candidate for soft
   deletion, identical to the full-scan logic.

Because the tables already expose the timestamps and uniqueness constraints we need, comparing the
incremental payload to existing `assets` becomes an O(changes) operation—no full rescan is required.
