# Performance

Katalog needs to be a performant tool. It's a core design principle, and it's important for it to be
a tool that feels easy to use.

Being performant while at the same time handling large amount of data is far from trivial.

## Usage model

Before we talk about performance, we need to define the model of usage, as solutions can look very
different depending on the scale.

- Single-user, local browser app (at least for now)
- Scan multiple local and remote data sources. We don't want to cap at some maximum size, but here
  is a realistic upper bound:
  - 1 million assets
  - 30 points of metadata per asset
  - 1 to 100 snapshots. The main way a user can control size on disk is to remove or compress
    historical snapshots.
  - File data is NOT stored read or stored by default, but a on-disk cache capped at a maximum size
    of e.g 5GB could be used
- When scanning remote sources, we may be limited by page sizes, e.g. we might need to scan up to 1M
  assets via a REST API that only gives 100 per page.

### Query model

For database design, we specifically need to look at the type of queries to expect. Here is a
non-exhaustive list:

- List all assets as flat, paged table
  - Filter by multiple condition (with typical filtering operators)
  - Sort by multiple columns
- List all assets in grouped-by sections
  - One level grouping is typical
  - In some cases, more nested grouping to present e.g. a folder system. However, that would need
    special handling
- List all Assets and Metadata affected by a Snapshot

## Performance targets

- The UI should in general be Desktop-like in responsiveness (less than 200ms)
  - Each page query should be <200 ms. Exception is for more rare and advanced queries.
- The time to scan a provider is unbounded, but the faster the better and users would prefer to
  finish within minutes, not hours
- Memory usage should be kept in check, aim for less than 2GB
- Database on-disk storage is unbounded, but a user wouldn't expect databases above 1GB

## Real-world challenging example

In the current system, we have a database with real world data, 584,112 assets with 10,885,468
metadata. It took 30 mins to scan from Google Drive using 10 concurrent fetcher threads, and it took
another few minutes to persist to database. The database is at 2GB. This is a fresh database, so no
history.
