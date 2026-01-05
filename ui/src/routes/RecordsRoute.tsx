import { useCallback, useEffect, useState } from "react";
import { fetchViewAssets } from "../api/client";
import type { Asset, MetadataValueEntry, ColumnDefinition, ViewAssetsResponse } from "../types/api";
import {
  SimpleTable,
  HeaderObject,
  ValueGetterProps,
  ValueFormatterProps,
  ColumnType,
} from "simple-table-core";
import type { TableFilterState } from "simple-table-core";
import "simple-table-core/styles.css";

const DEFAULT_VIEW_ID = "default";

const valueGetter = (props: ValueGetterProps) => {
  const value = props.row[props.accessor];
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    const entry = value as MetadataValueEntry;
    const inner = entry.value;
    if (inner === null || inner === undefined) {
      return "";
    }
    if (typeof inner === "object") {
      return JSON.stringify(inner);
    }
    return inner;
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return value;
};

const valueFormatter = (props: ValueFormatterProps) => {
  const raw = valueGetter(props);
  if (raw === null || raw === undefined) {
    return "";
  }
  if (typeof raw === "object") {
    return JSON.stringify(raw);
  }
  return `${raw}`;
};

const getSimpleTableType = (metadataType?: number): ColumnType => {
  if (metadataType === 1 || metadataType === 2) {
    return "number";
  } else if (metadataType === 3) {
    return "date";
  }
  return "string";
};

const normalizeWidth = (width: number | null): string | number => {
  if (width === null || width === undefined) {
    return "1fr";
  } else {
    return width;
  }
};

const normalizeSort = (
  sortArg: unknown
): { accessor: string; direction: "asc" | "desc" } | null => {
  if (!sortArg) {
    return null;
  }
  if (
    typeof sortArg === "object" &&
    sortArg !== null &&
    "key" in (sortArg as Record<string, unknown>)
  ) {
    const sortObj = sortArg as { key?: { accessor?: unknown }; direction?: unknown };
    const accessor = sortObj.key?.accessor ? String(sortObj.key.accessor) : "";
    const direction = sortObj.direction === "desc" ? "desc" : "asc";
    if (!accessor) {
      return null;
    }
    return { accessor, direction };
  }
  const accessor = String(sortArg);
  if (!accessor) {
    return null;
  }
  return { accessor, direction: "asc" };
};

const buildHeadersFromSchema = (schema: ColumnDefinition[]): HeaderObject[] => {
  return schema.map((column) => ({
    accessor: column.id,
    label: column.title || column.id,
    width: normalizeWidth(column.width),
    type: getSimpleTableType(column.value_type),
    isSortable: Boolean(column.sortable),
    filterable: Boolean(column.filterable),
    valueGetter,
    valueFormatter,
  }));
};

function RecordsRoute() {
  const [records, setRecords] = useState<Asset[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [seenHeaders, setSeenHeaders] = useState<HeaderObject[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [pagination, setPagination] = useState<{ limit: number; page: number }>({
    limit: 200,
    page: 1,
  });
  const [total, setTotal] = useState<number | null>(null);
  const [sort, setSort] = useState<{ accessor: string; direction: "asc" | "desc" } | null>(null);
  const [filters, setFilters] = useState<TableFilterState>({});
  const [durationMs, setDurationMs] = useState<number | null>(null);

  const loadPage = useCallback(
    async (
      page: number,
      limitOverride?: number,
      sortOverride?: { accessor: string; direction: "asc" | "desc" } | null,
      filtersOverride?: TableFilterState,
      searchOverride?: string
    ) => {
      const limit = limitOverride ?? pagination.limit;
      const effectiveSort = sortOverride ?? sort;
      const effectiveFilters = filtersOverride ?? filters;
      const effectiveSearch = (searchOverride ?? searchQuery).trim();
      setLoading(true);
      setError(null);
      try {
        const offset = (page - 1) * limit;
        const sortParam =
          effectiveSort && effectiveSort.accessor && effectiveSort.direction
            ? `${effectiveSort.accessor}:${effectiveSort.direction}`
            : undefined;
        const filterParams = new URLSearchParams();
        Object.values(effectiveFilters || {}).forEach((filter) => {
          filterParams.append("filters", JSON.stringify(filter));
        });
        const response: ViewAssetsResponse = await fetchViewAssets(DEFAULT_VIEW_ID, {
          offset,
          limit,
          sort: sortParam,
          filters: filterParams.getAll("filters"),
          search: effectiveSearch.length > 0 ? effectiveSearch : undefined,
        });
        const fetchedRecords = response.items ?? [];
        setRecords(fetchedRecords);
        const schema = response.schema ?? [];
        setSeenHeaders(buildHeadersFromSchema(schema));
        setPagination({
          limit,
          page,
        });
        setTotal(response.stats?.total ?? null);
        console.log(response.stats);
        setDurationMs(response.stats?.duration_ms ?? null);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setRecords([]);
      } finally {
        setLoading(false);
      }
    },
    [pagination.limit, sort, filters, searchQuery]
  );

  useEffect(() => {
    void loadPage(1);
    // We intentionally only load once per mount or when the view changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      void loadPage(1, undefined, sort, filters, searchQuery);
    }, 250);
    return () => window.clearTimeout(handle);
  }, [searchQuery, loadPage, sort, filters]);

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Records</h2>
          <p>
            Displaying records from view “{DEFAULT_VIEW_ID}”, total {total}.
          </p>
        </div>
        <button type="button" onClick={() => void loadPage(1)} disabled={loading}>
          {loading ? "Loading..." : "Reload"}
        </button>
      </header>
      <div className="search-bar">
        <input
          type="search"
          placeholder="Search records…"
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
          onKeyDown={(event) => event.stopPropagation()}
          aria-label="Search records"
        />
      </div>
      {error && <p className="error">{error}</p>}
      {!error && !loading && records.length === 0 && (
        <div className="empty-state">No records available.</div>
      )}
      <SimpleTable
        defaultHeaders={seenHeaders}
        rows={records}
        height={"75vh"}
        selectableCells={true}
        rowIdAccessor="asset/id"
        columnResizing={true}
        shouldPaginate={true}
        rowsPerPage={pagination.limit}
        serverSidePagination={true}
        totalRowCount={total ?? records.length}
        onPageChange={(page) => {
          if (page === pagination.page) {
            return;
          }
          void loadPage(page);
        }}
        isLoading={loading}
        externalSortHandling={true}
        externalFilterHandling={true}
        onSortChange={(sortArg) => {
          const normalized = normalizeSort(sortArg);
          if (!normalized) {
            setSort(null);
            void loadPage(1, undefined, null, filters);
            return;
          }
          setSort(normalized);
          void loadPage(1, undefined, normalized, filters);
        }}
        onFilterChange={(state) => {
          const nextFilters = state || {};
          const currentSig = JSON.stringify(filters || {});
          const nextSig = JSON.stringify(nextFilters || {});
          if (currentSig === nextSig) {
            return;
          }
          setFilters(nextFilters);
          void loadPage(1, undefined, sort, nextFilters);
        }}
      />
      {durationMs !== null && (
        <div
          style={{ marginTop: "0.5rem", fontStyle: "italic", fontSize: "0.9rem", color: "#555" }}
        >
          Query time: {durationMs} ms
        </div>
      )}
    </section>
  );
}

export default RecordsRoute;
