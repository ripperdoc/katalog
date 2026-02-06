import { ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ColumnType,
  FilterCondition,
  HeaderObject,
  SimpleTable,
  TableFilterState,
  ValueFormatterProps,
  ValueGetterProps,
} from "simple-table-core";
import { Asset, ColumnDefinition, MetadataValueEntry, ViewAssetsResponse } from "../types/api";
import "simple-table-core/styles.css";
import AssetCell from "./AssetCell";
import { ActorCellPure } from "./ActorCell";
import ExternalIdCell from "./ExternalIdCell";
import { makeFlagCell, ThumbnailCell } from "./FlagCell";
import TableFooter from "./TableFooter";
import { useRegistry } from "../utils/registry";

const ASSET_ID_KEY = "asset/id";
const ACTOR_ID_KEY = "asset/actor_id";
const EXTERNAL_ID_KEY = "asset/external_id";
const FILE_THUMBNAIL_URI_KEY = "file/thumbnail_link";
const FLAG_FAVORITE_KEY = "flag/starred";
const FLAG_HIDDEN_KEY = "flag/hidden";
const FLAG_REVIEW_KEY = "flag/review";
const FLAG_REJECTED_KEY = "flag/rejected";

const favoriteFlagCell = makeFlagCell({
  label: "Favorite",
  iconOn: "star",
  iconOff: "star_outline",
  onColor: "#f59e0b",
  offColor: "#94a3b8",
});
const hiddenFlagCell = makeFlagCell({
  label: "Hidden",
  iconOn: "visibility_off",
  iconOff: "visibility",
  onColor: "#0f172a",
  offColor: "#94a3b8",
});
const reviewFlagCell = makeFlagCell({
  label: "Review",
  iconOn: "rate_review",
  iconOff: "rate_review",
  onColor: "#2563eb",
  offColor: "#94a3b8",
});
const rejectedFlagCell = makeFlagCell({
  label: "Rejected",
  iconOn: "block",
  iconOff: "block",
  onColor: "#dc2626",
  offColor: "#94a3b8",
});

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
  const unitIndex = Math.min(Math.floor(Math.log(absValue) / Math.log(1024)), units.length - 1);
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

const normalizeFilterValue = (value: unknown): string => {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
};

const serializeFilter = (filter: FilterCondition): string | null => {
  const accessor = filter.accessor ? String(filter.accessor) : "";
  const operator = filter.operator ? String(filter.operator) : "";
  if (!accessor || !operator) {
    return null;
  }
  if (operator === "isEmpty" || operator === "isNotEmpty") {
    return `${accessor} ${operator} true`;
  }
  if (operator === "between" || operator === "notBetween" || operator === "in" || operator === "notIn") {
    const rawValues = filter.values ?? (filter.value !== undefined ? [filter.value] : []);
    const values = rawValues.map(normalizeFilterValue).filter((value) => value.length > 0);
    if (values.length === 0) {
      return null;
    }
    return `${accessor} ${operator} ${values.join(",")}`;
  }
  if (filter.value === undefined || filter.value === null) {
    return null;
  }
  return `${accessor} ${operator} ${normalizeFilterValue(filter.value)}`;
};

const cellRenderersById: Record<string, HeaderObject["cellRenderer"]> = {
  [EXTERNAL_ID_KEY]: ExternalIdCell,
  [ASSET_ID_KEY]: AssetCell,
  [FLAG_FAVORITE_KEY]: favoriteFlagCell,
  [FLAG_HIDDEN_KEY]: hiddenFlagCell,
  [FLAG_REVIEW_KEY]: reviewFlagCell,
  [FLAG_REJECTED_KEY]: rejectedFlagCell,
  [FILE_THUMBNAIL_URI_KEY]: ThumbnailCell,
};

const headerLabelById: Record<string, string> = {
  [FLAG_FAVORITE_KEY]: "",
  [FLAG_HIDDEN_KEY]: "",
  [FLAG_REVIEW_KEY]: "",
  [FLAG_REJECTED_KEY]: "",
  [FILE_THUMBNAIL_URI_KEY]: "",
};

const valueFormatterById: Record<string, HeaderObject["valueFormatter"]> = {
  "file/size": (props: ValueFormatterProps) => formatBytes(valueGetter(props)),
};

export const buildHeadersFromSchema = (schema: ColumnDefinition[]): HeaderObject[] => {
  const rv = schema
    .filter((column) => !column.hidden)
    .map((column, index) => ({
      accessor: column.id,
      label: headerLabelById[column.id] ?? (column.title || column.id),
      // pinned: index < 2 ? "left" : undefined,
      width: normalizeWidth(column.width),
      type: getSimpleTableType(column.value_type),
      isSortable: Boolean(column.sortable),
      filterable: Boolean(column.filterable),
      valueGetter,
      valueFormatter: valueFormatterById[column.id] ?? valueFormatter,
      cellRenderer: cellRenderersById[column.id],
    }));
  return rv;
};

type FetchArgs = {
  offset: number;
  limit: number;
  sort?: [string, "asc" | "desc"][] | undefined;
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
  const { data: registryData } = useRegistry();
  const [records, setRecords] = useState<Asset[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [schema, setSchema] = useState<ColumnDefinition[]>([]);
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
  const [useServerFilters, setUseServerFilters] = useState<boolean>(true);
  const [useServerSort, setUseServerSort] = useState<boolean>(true);
  const searchRef = useRef("");
  const lastRequestRef = useRef<string | null>(null);

  useEffect(() => {
    searchRef.current = searchQuery;
  }, [searchQuery]);

  const headers = useMemo<HeaderObject[]>(() => {
    if (!schema.length) {
      return [];
    }
    const baseHeaders = buildHeadersFromSchema(schema);
    return baseHeaders.map((header) =>
      header.accessor === ACTOR_ID_KEY
        ? {
            ...header,
            cellRenderer: (props) => (
              <ActorCellPure {...props} actorsById={registryData?.actorsById} />
            ),
          }
        : header,
    );
  }, [schema, registryData]);

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
        useServerSort && effectiveSort && effectiveSort.accessor && effectiveSort.direction
          ? ([[effectiveSort.accessor, effectiveSort.direction]] as [
              string,
              "asc" | "desc",
            ][])
          : undefined;
      const filterParams: string[] = [];
      if (useServerFilters) {
        Object.values(effectiveFilters || {}).forEach((filter) => {
          const normalized = serializeFilter(filter);
          if (normalized) {
            filterParams.push(normalized);
          }
        });
      }
      const searchParam = effectiveSearch.length > 0 ? effectiveSearch : undefined;
      const requestKey = JSON.stringify({
        page,
        limit,
        sort: sortParam,
        filters: filterParams,
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
          filters: filterParams,
          search: searchParam,
        });
        const fetchedRecords = (response.items ?? []).map((item) => ({
          ...item,
          id: item["asset/id"] ?? item.id,
        }));
        setRecords(fetchedRecords);
        setSchema(response.schema ?? []);
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
            filters: filterParams,
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
    [
      pagination.limit,
      sort,
      filters,
      fetchPage,
      onLoadComplete,
      useServerFilters,
      useServerSort,
    ],
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
      <header className="panel-header">
        <div>
          <h2>{title}</h2>
          {subtitle && <p>{subtitle}</p>}
        </div>
        <div className="panel-actions">
          {actions}
          <label className="toggle-inline">
            <input
              type="checkbox"
              checked={useServerFilters}
              onChange={(event) => setUseServerFilters(event.target.checked)}
            />
            Server filters
          </label>
          <label className="toggle-inline">
            <input
              type="checkbox"
              checked={useServerSort}
              onChange={(event) => setUseServerSort(event.target.checked)}
            />
            Server sort
          </label>
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
      {error && <p className="error">{error}</p>}

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
          externalSortHandling={useServerSort}
          externalFilterHandling={useServerFilters}
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
          onRowSelectionChange={({ row, isSelected, selectedRows }) => {
            const assetId = Number(row["asset/id"]);
            const nextSelected = new Set<number>(selectedAssetIds);
            if (Number.isNaN(assetId)) return;
            if (isSelected) {
              nextSelected.add(assetId);
            } else {
              nextSelected.delete(assetId);
            }
            setSelectedAssetIds(nextSelected);
            onSelectionChange?.(nextSelected);
          }}
          onSortChange={(sortArg) => {
            const normalized = normalizeSort(sortArg);
            if (!normalized) {
              setSort(null);
              if (useServerSort) {
                void loadPage(1, undefined, null, filters);
              }
              return;
            }
            setSort(normalized);
            if (useServerSort) {
              void loadPage(1, undefined, normalized, filters);
            }
          }}
          onFilterChange={(state) => {
            const nextFilters = state || {};
            const currentSig = JSON.stringify(filters || {});
            const nextSig = JSON.stringify(nextFilters || {});
            if (currentSig === nextSig) {
              return;
            }
            setFilters(nextFilters);
            if (useServerFilters) {
              void loadPage(1, undefined, sort, nextFilters);
            }
          }}
        />
      </div>
    </section>
  );
};

export default AssetTable;
