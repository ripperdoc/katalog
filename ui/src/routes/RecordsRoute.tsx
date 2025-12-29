import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchAssets } from "../api/client";
import type { AssetResponse, Asset, MetadataValueEntry } from "../types/api";
import {
  SimpleTable,
  HeaderObject,
  ValueGetterProps,
  ValueFormatterProps,
  ColumnType,
} from "simple-table-core";
import "simple-table-core/styles.css";

const headers: HeaderObject[] = [
  // Fixed width in pixels
  {
    accessor: "id",
    label: "ID",
    width: "90px",
    type: "number",
    isSortable: true,
    filterable: true,
  },
  {
    accessor: "canonical_id",
    label: "Canonical ID",
    width: "1fr",
    type: "string",
    isSortable: true,
    filterable: true,
  },
  {
    accessor: "canonical_uri",
    label: "URI",
    width: "2fr",
    type: "string",
    isSortable: true,
    filterable: true,
  },
  {
    accessor: "seen",
    label: "Last Snapshot",
    width: "1fr",
    type: "number",
    isSortable: true,
    filterable: true,
  },
];

const valueGetter = (props: ValueGetterProps) => {
  const metadata = props.row["metadata"];
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return "";
  }
  const meta = (metadata as Record<string, MetadataValueEntry>)[props.accessor];
  if (!meta || typeof meta !== "object") {
    return "";
  }
  return meta.value;
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
  const parts: string[] = [
    record.id,
    record.canonical_id,
    record.canonical_uri,
    record.created,
    record.seen,
    record.deleted,
  ]
    .filter((part) => part !== undefined && part !== null)
    .map((part) => String(part));

  if (record.metadata && typeof record.metadata === "object") {
    Object.entries(record.metadata).forEach(([key, value]) => {
      parts.push(key);
      collectSearchableParts(value, parts);
    });
  }

  return parts.map((part) => part.toLowerCase()).join(" ");
};

function RecordsRoute() {
  const [records, setRecords] = useState<Asset[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [seenHeaders, setSeenHeaders] = useState<HeaderObject[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
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

  const loadRecords = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response: AssetResponse = await fetchAssets();
      const fetchedRecords = response.assets ?? [];
      setRecords(fetchedRecords);
      const schema = response.schema ?? {};
      setSeenHeaders(
        Object.keys(schema).map((key) => ({
          accessor: key,
          label: schema[key]?.title || key,
          width: schema[key]?.width || "100px",
          type: getSimpleTableType(schema[key]?.value_type),
          isSortable: true,
          filterable: true,
          valueGetter,
          valueFormatter,
        }))
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setRecords([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadRecords();
  }, [loadRecords]);

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Records</h2>
          <p>Displaying every file record returned by the backend.</p>
        </div>
        <button type="button" onClick={loadRecords} disabled={loading}>
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
        defaultHeaders={[...headers, ...seenHeaders]}
        rows={filteredRecords}
        height={"75vh"}
        selectableCells={true}
        rowIdAccessor="id"
        rowsPerPage={20}
        columnResizing={true}
      />
    </section>
  );
}

export default RecordsRoute;
