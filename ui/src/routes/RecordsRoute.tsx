import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchViewAssets } from "../api/client";
import type { Asset, MetadataValueEntry, ColumnDefinition, ViewAssetsResponse } from "../types/api";
import {
  SimpleTable,
  HeaderObject,
  ValueGetterProps,
  ValueFormatterProps,
  ColumnType,
} from "simple-table-core";
import "simple-table-core/styles.css";

const DEFAULT_VIEW_ID = "default";

const valueGetter = (props: ValueGetterProps) => {
  const value = props.row[props.accessor];
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    const entry = value as MetadataValueEntry;
    return entry.value;
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

const collectSearchableParts = (value: unknown, parts: string[]) => {
  if (value === null || value === undefined) {
    return;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    parts.push(String(value));
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectSearchableParts(item, parts));
    return;
  }
  if (typeof value === "object") {
    const recordValue = value as Record<string, unknown>;
    if ("value" in recordValue) {
      collectSearchableParts(recordValue.value, parts);
    }
    Object.values(recordValue).forEach((child) => collectSearchableParts(child, parts));
  }
};

const buildSearchString = (record: Asset): string => {
  const parts: string[] = [];
  Object.entries(record).forEach(([key, value]) => {
    parts.push(key);
    collectSearchableParts(value, parts);
  });
  return parts.map((part) => part.toLowerCase()).join(" ");
};

function RecordsRoute() {
  const [records, setRecords] = useState<Asset[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [seenHeaders, setSeenHeaders] = useState<HeaderObject[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [pagination, setPagination] = useState<{ offset: number; limit: number; page: number }>({
    offset: 0,
    limit: 200,
    page: 1,
  });
  const [total, setTotal] = useState<number | null>(null);
  const indexedRecords = useMemo(
    () => records.map((record) => ({ record, haystack: buildSearchString(record) })),
    [records]
  );

  const filteredRecords = useMemo(() => {
    const trimmed = searchQuery.trim().toLowerCase();
    if (!trimmed) {
      return indexedRecords.map(({ record }) => record);
    }
    return indexedRecords
      .filter(({ haystack }) => haystack.includes(trimmed))
      .map(({ record }) => record);
  }, [indexedRecords, searchQuery]);

  const loadPage = useCallback(
    async (page: number, limitOverride?: number) => {
      const limit = limitOverride ?? pagination.limit;
      setLoading(true);
      setError(null);
      try {
        const offset = (page - 1) * limit;
        const response: ViewAssetsResponse = await fetchViewAssets(DEFAULT_VIEW_ID, {
          offset,
          limit,
        });
        const fetchedRecords = response.items ?? [];
        setRecords(fetchedRecords);
        const schema = response.schema ?? [];
        setSeenHeaders(buildHeadersFromSchema(schema));
        setPagination({
          offset,
          limit,
          page,
        });
        setTotal(response.stats?.total ?? null);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setRecords([]);
      } finally {
        setLoading(false);
      }
    },
    [pagination.limit]
  );

  useEffect(() => {
    void loadPage(1);
    // We intentionally only load once per mount or when the view changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Records</h2>
          <p>Displaying records from view “{DEFAULT_VIEW_ID}”.</p>
        </div>
        <button type="button" onClick={() => void loadPage(true)} disabled={loading}>
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
      {!error && !loading && records.length > 0 && filteredRecords.length === 0 && (
        <div className="empty-state">No records match your search.</div>
      )}
      <SimpleTable
        defaultHeaders={seenHeaders}
        rows={filteredRecords}
        height={"75vh"}
        selectableCells={true}
        rowIdAccessor="asset/id"
        columnResizing={true}
        serverSidePagination={true}
        rowsPerPage={pagination.limit}
        totalRows={total ?? records.length}
        currentPage={pagination.page}
        onPageChange={(page) => void loadPage(page)}
        onNextPage={() => void loadPage(pagination.page + 1)}
        onUserPageChange={(page) => void loadPage(page)}
      />
    </section>
  );
}

export default RecordsRoute;
