import { ReactNode, useCallback, useEffect, useRef, useState } from "react";
import {
  CellClickProps,
  ColumnType,
  HeaderObject,
  SimpleTable,
  TableFilterState,
  ValueFormatterProps,
  ValueGetterProps,
} from "simple-table-core";
import { Asset, ColumnDefinition, MetadataValueEntry, ViewAssetsResponse } from "../types/api";
import "simple-table-core/styles.css";
import AssetCell from "./AssetCell";
import ExternalIdCell from "./ExternalIdCell";
import TableFooter from "./TableFooter";

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

const formatBytes = (value: unknown): string => {
  const numericValue = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numericValue)) {
    return value === null || value === undefined ? "" : String(value);
  }
  if (numericValue === 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const absValue = Math.abs(numericValue);
  const unitIndex = Math.min(
    Math.floor(Math.log(absValue) / Math.log(1024)),
    units.length - 1,
  );
  const scaled = numericValue / 1024 ** unitIndex;
  const formatter = new Intl.NumberFormat(undefined, {
    maximumFractionDigits: scaled >= 10 || unitIndex === 0 ? 0 : 1,
  });
  return `${formatter.format(scaled)} ${units[unitIndex]}`;
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
    return 200;
    return "1fr";
  } else {
    return width;
  }
};

const normalizeSort = (
  sortArg: unknown,
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

export const buildHeadersFromSchema = (schema: ColumnDefinition[]): HeaderObject[] => {
  const assetIdKey = "asset/id";
  const externalIdKey = "asset/external_id";

  const rv = schema
    .filter((column) => !column.hidden)
    .map((column, index) => ({
      accessor: column.id,
      label: column.title || column.id,
      // pinned: index < 2 ? "left" : undefined,
      width: normalizeWidth(column.width),
      type: getSimpleTableType(column.value_type),
      isSortable: Boolean(column.sortable),
      filterable: Boolean(column.filterable),
      valueGetter,
      valueFormatter:
        column.id === "file/size"
          ? (props: ValueFormatterProps) => formatBytes(valueGetter(props))
          : valueFormatter,
      cellRenderer:
        column.id === externalIdKey
          ? ExternalIdCell
          : column.id === assetIdKey
            ? AssetCell
            : undefined,
    }));
  return rv;
};

type FetchArgs = {
  offset: number;
  limit: number;
  sort?: string | undefined;
  filters?: string[] | undefined;
  search?: string | undefined;
};

type FetchPage = (args: FetchArgs) => Promise<ViewAssetsResponse>;

interface AssetTableProps {
  title: string;
  subtitle?: string;
  fetchPage: FetchPage;
  defaultLimit?: number;
  onRowClick?: (assetId: number) => void;
  onLoadComplete?: (payload: { response: ViewAssetsResponse; params: FetchArgs }) => void;
  onSelectionChange?: (selectedAssetIds: Set<number>) => void;
  actions?: ReactNode;
  searchPlaceholder?: string;
}

const AssetTable = ({
  title,
  subtitle,
  fetchPage,
  defaultLimit = 200,
  onRowClick,
  onLoadComplete,
  onSelectionChange,
  actions,
  searchPlaceholder = "Search…",
}: AssetTableProps) => {
  const [records, setRecords] = useState<Asset[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [headers, setHeaders] = useState<HeaderObject[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [pagination, setPagination] = useState<{ limit: number; page: number }>({
    limit: defaultLimit,
    page: 1,
  });
  const [total, setTotal] = useState<number | null>(null);
  const [sort, setSort] = useState<{ accessor: string; direction: "asc" | "desc" } | null>(null);
  const [filters, setFilters] = useState<TableFilterState>({});
  const [durationMs, setDurationMs] = useState<number | null>(null);
  const [selectedAssetIds, setSelectedAssetIds] = useState<Set<number>>(new Set());
  const searchRef = useRef("");
  const lastRequestRef = useRef<string | null>(null);

  useEffect(() => {
    searchRef.current = searchQuery;
  }, [searchQuery]);

  const loadPage = useCallback(
    async (
      page: number,
      limitOverride?: number,
      sortOverride?: { accessor: string; direction: "asc" | "desc" } | null,
      filtersOverride?: TableFilterState,
      searchOverride?: string,
      forceReload: boolean = false,
    ) => {
      const limit = limitOverride ?? pagination.limit;
      const effectiveSort = sortOverride ?? sort;
      const effectiveFilters = filtersOverride ?? filters;
      const effectiveSearch = (searchOverride ?? searchRef.current).trim();
      const sortParam =
        effectiveSort && effectiveSort.accessor && effectiveSort.direction
          ? `${effectiveSort.accessor}:${effectiveSort.direction}`
          : undefined;
      const filterParams = new URLSearchParams();
      Object.values(effectiveFilters || {}).forEach((filter) => {
        filterParams.append("filters", JSON.stringify(filter));
      });
      const searchParam = effectiveSearch.length > 0 ? effectiveSearch : undefined;
      const requestKey = JSON.stringify({
        page,
        limit,
        sort: sortParam,
        filters: filterParams.getAll("filters"),
        search: searchParam,
      });
      if (!forceReload && lastRequestRef.current === requestKey) {
        return;
      }
      lastRequestRef.current = requestKey;
      setLoading(true);
      setError(null);
      try {
        const response = await fetchPage({
          offset: (page - 1) * limit,
          limit,
          sort: sortParam,
          filters: filterParams.getAll("filters"),
          search: searchParam,
        });
        const fetchedRecords = (response.items ?? []).map((item) => ({
          ...item,
          id: item["asset/id"] ?? item.id,
        }));
        setRecords(fetchedRecords);
        setHeaders(buildHeadersFromSchema(response.schema ?? []));
        setPagination({
          limit,
          page,
        });
        setTotal(response.stats?.total ?? null);
        setDurationMs(response.stats?.duration_ms ?? null);
        onLoadComplete?.({
          response,
          params: {
            offset: (page - 1) * limit,
            limit,
            sort: sortParam,
            filters: filterParams.getAll("filters"),
            search: effectiveSearch.length > 0 ? effectiveSearch : undefined,
          },
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setRecords([]);
      } finally {
        setLoading(false);
      }
    },
    [pagination.limit, sort, filters, fetchPage, onLoadComplete],
  );

  useEffect(() => {
    void loadPage(1);
  }, [loadPage]);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      void loadPage(1, undefined, sort, filters, searchQuery);
    }, 1000);
    return () => window.clearTimeout(handle);
  }, [searchQuery, loadPage, sort, filters]);

  return (
    <section className="panel">
      {/* <header className="panel-header">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
        <div className="panel-actions">
          {actions}
          <button
            className="btn-primary"
            type="button"
            onClick={() => void loadPage(1, undefined, sort, filters, searchQuery, true)}
            disabled={loading}
          >
            {loading ? "Loading..." : "Reload"}
          </button>
        </div>
      </header>
      <div className="search-bar">
        <input
          type="search"
          placeholder={searchPlaceholder}
          value={searchQuery}
          onChange={(event) => setSearchQuery(event.target.value)}
          onKeyDown={(event) => event.stopPropagation()}
          aria-label="Search"
        />
      </div>
      {error && <p className="error">{error}</p>} */}

      <div className="table-container">
        <SimpleTable
          defaultHeaders={headers}
          rows={records}
          height="100%"
          autoExpandColumns={false}
          selectableCells={true}
          columnResizing={true}
          shouldPaginate={true}
          rowsPerPage={pagination.limit}
          serverSidePagination={true}
          enableRowSelection={true}
          totalRowCount={total ?? records.length}
          footerRenderer={(props) => (
            <TableFooter
              {...props}
              queryTimeMs={durationMs}
              selectedCount={selectedAssetIds.size}
            />
          )}
          onPageChange={(page) => {
            if (page === pagination.page) {
              return;
            }
            void loadPage(page, undefined, sort, filters, searchQuery);
          }}
          isLoading={loading}
          onRowSelectionChange={({ selectedRows }) => {
            const nextSelected = new Set<number>();
            selectedRows.forEach((rowId) => {
              const assetId = Number(rowId);
              if (!Number.isNaN(assetId)) {
                nextSelected.add(assetId);
              }
            });
            setSelectedAssetIds(nextSelected);
            onSelectionChange?.(nextSelected);
          }}
          // externalSortHandling={true}
          // externalFilterHandling={true}
          // onSortChange={(sortArg) => {
          //   const normalized = normalizeSort(sortArg);
          //   if (!normalized) {
          //     setSort(null);
          //     void loadPage(1, undefined, null, filters);
          //     return;
          //   }
          //   setSort(normalized);
          //   void loadPage(1, undefined, normalized, filters);
          // }}
          // onFilterChange={(state) => {
          //   const nextFilters = state || {};
          //   const currentSig = JSON.stringify(filters || {});
          //   const nextSig = JSON.stringify(nextFilters || {});
          //   if (currentSig === nextSig) {
          //     return;
          //   }
          //   setFilters(nextFilters);
          //   void loadPage(1, undefined, sort, nextFilters);
          // }}
        />
      </div>
    </section>
  );
};

export default AssetTable;
