# Database

## Requirements:

- Handle 1M file records with 30 pieces of metadata each, being able to insert/upsert maybe 500k per
  scan without causing large delays
- Be as space efficient as possible
- Handle an extensible number of metadata keys, including plugins adding new keys without
  migrations.
- Handle multiple opinions from multiple providers on every metadata, on every asset
- Full version history on all metadata, enabling full undo, restore and editing of history
- Support full sorting, filtering and grouping on all metadata fields
- Support full text search and vector search (using necessary plugins and/or separate tables)
- Respond to queries quickly, preferrably below 200ms
- With all of above, keep the schema and queries simple to create and understand, and make it easy
  to create plugins and parse the database with other tools

## Current database design:

- Defined in TortoiseORM in models.py
- Asset table mainly tracking external asset IDs
- All keys converted to integers to reduce space in Metadata table, but this causes some extra
  complexity to lookup names from integers
- AssetState table, tracking when and how assets have been seen
- Metadata table, EAV style append-only that allows single or multiple values for each key, provider
  and snapshot. Tombstone logic to delete specific values.
- EAV possibly creates a lot of repetition
- The logic for what new EAV rows to store requires Python code in MetadataChangeSet, it's fairly
  complicated

### Current issues:

1. Complicated database design, not easy to analyze manually, write queries for or export
2. Filtering, sorting and grouping by Metadata values not yet implemented and seem to require
   complicated queries and/or extra tables to not be very slow
3. TortoiseORM _possibly_ creating overhead

## Proposed new design:

- All data in a main Asset table
- All metadata stored in a JSON field of that table
- Queries done with `json_extract`, adding indices for hot paths

### Benefits:

1. Much simpler database design, close to NoSQL
2. Simpler code to ingest and query data
3. Allow full expressivity of JSON for metadata values

### Downsides:

1. Size may increase and we need to try and keep JSON keys compact
2. Filtering inside JSON without using index is slow (several seconds on 500k asset table), maybe
   EAV would be faster or equally slow, although more complicated?
3. JSON canny contain foreign keys, meaning we can't dynamically track relationships in metadata,
   which EAV can do out of the box

## Implementation plan

Here is a plan for how to move into the new design.

- Replace Asset, AssetState and Metadata database models with a unified Asset model/table
  - Some metadata fields should get their top-level column, the rest are supported inside JSON
  - Top-level columns, accepting only single scalar values: `file/name`, `file/path` `file/size`,
    `file/type`, `time/created`, `time/modified`
  - We should offer a method that combines the top-level columns and the JSON back into a dict so we
    can treat them as if the were all in the same dict
- The change means that we can have multiple asset rows for the same `external_id`, either rows that
  are from different providers or have been tombstoned
- MetadataChangeSet needs to change. It's methods should still take a provider_id to determine which
  "view" of metadata to return. When we save metadata, we still want to avoid writing new rows if
  the row is the same as before, this can maybe be handled in SQL rather than code.
- See if we can speed up data ingestion through batching updates in sources/runtime.py
- The MetadataDef need to include a json_key which should be a shortened but readable version that
  we write into database. The MetadataDef also needs to keep a flag for whether the metadata is
  written to a top-level column or not.
- We need to update `queries.py` to instead run on the new model. We should support sorting,
  filtering, grouping by and free text search for all metadata columns
- We don't need any backward compatibility or database migration, remove anything that is no longer
  supported
- Update all affected tests
- The goal of this change is simplicity. Try to keep all changes simple, not over-engineered, and
  not prematurely optimized.
