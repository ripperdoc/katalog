# Filter Support (Current)

This describes the filter behavior currently implemented in Katalog query paths (`/api/assets`, `/api/collections/{id}/assets`, and corresponding MCP tools).

## Syntax

- Each filter is a string: `<key> <operator> <value>`
- REST: repeat `filters` query param for multiple filters.
- Multiple filters are combined with **AND** only (no OR/grouping).

Examples:

- `filters=asset/actor_id equals 3`
- `filters=product/sku_id equals 1-56504-793-1`

## Notes

- `isEmpty` and `isNotEmpty` still require a value placeholder because parser format is fixed (example: `file/filename isEmpty _`).
- `in`, `notIn`, `between`, `notBetween` use comma-separated values in the `<value>` part.

## Asset Keys

Supported asset keys:

- `asset/id`
- `asset/actor_id`
- `asset/namespace`
- `asset/external_id`
- `asset/canonical_uri`

Supported operators:

- For all asset keys:
  - `equals`, `notEquals`
  - `greaterThan`, `lessThan`, `greaterThanOrEqual`, `lessThanOrEqual`
  - `in`, `notIn`
  - `between`, `notBetween`
  - `isEmpty`, `isNotEmpty`
- For string asset keys only (`namespace`, `external_id`, `canonical_uri`):
  - `contains`, `notContains`, `startsWith`, `endsWith`

## Metadata Keys

Any registered metadata key can be filtered.

### Operators currently executed by SQL path

- Generic:
  - `equals`, `notEquals`
  - `greaterThan`, `lessThan`, `greaterThanOrEqual`, `lessThanOrEqual`
  - `between`, `notBetween`
  - `isEmpty`, `isNotEmpty`
- String-like metadata (`STRING`, `JSON`):
  - `contains`, `notContains`, `startsWith`, `endsWith`
- Collection metadata (`COLLECTION`) special case:
  - `equals`, `in`, `notIn` (latest-state aware)

### Important current limitations

- No OR support.
- `in`/`notIn` are **not** generally supported for all metadata types (only special-cased for `COLLECTION`).
- `before`/`after` are not currently executed by the SQL filter engine.

