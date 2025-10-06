import { useCallback, useEffect, useState } from "react";
import { fetchRecords } from "../api/client";
import type { FileRecordResponse, FileRecord, MetadataEntry } from "../types/api";
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
  { accessor: "id", label: "ID", width: "1fr", type: "string", isSortable: true, filterable: true },
  {
    accessor: "source_id",
    label: "Source",
    width: "90px",
    type: "string",
    isSortable: true,
    filterable: true,
  },
];

const valueGetter = (props: ValueGetterProps) => {
  const metadata = props.row["metadata"];
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return "";
  }
  const meta = (metadata as Record<string, MetadataEntry[]>)[props.accessor];
  if (!meta || !Array.isArray(meta)) {
    return "";
  }
  // const value = meta.map((entry) => JSON.stringify(entry.value)).join(", ");
  const value = meta[0]?.value;
  return value;
};

const valueFormatter = (props: ValueFormatterProps) => {
  const metadata = props.row["metadata"];
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return "";
  }
  const meta = (metadata as Record<string, MetadataEntry[]>)[props.accessor];
  if (!meta || !Array.isArray(meta)) {
    return "";
  }
  // const value = meta.map((entry) => JSON.stringify(entry.value)).join(", ");
  const value = `${meta[0]?.value}`;
  return value;
};

const getSimpleTableType = (metadataType?: string): ColumnType => {
  if (metadataType === "int" || metadataType === "float") {
    return "number";
  } else if (metadataType === "datetime") {
    return "date";
  }
  return "string";
};

function RecordsRoute() {
  const [records, setRecords] = useState<FileRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [seenHeaders, setSeenHeaders] = useState<HeaderObject[]>([]);

  const loadRecords = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response: FileRecordResponse = await fetchRecords();
      const fetchedRecords = response.records ?? [];
      setRecords(fetchedRecords);
      const schema = response.schema ?? {};
      setSeenHeaders(
        Object.keys(schema).map((key) => ({
          accessor: key,
          label: schema[key]?.title || key,
          width: schema[key]?.width || "1fr",
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
      {error && <p className="error">{error}</p>}
      {!error && !loading && records.length === 0 && (
        <div className="empty-state">No records available.</div>
      )}
      <SimpleTable
        defaultHeaders={[...headers, ...seenHeaders]}
        rows={records}
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
