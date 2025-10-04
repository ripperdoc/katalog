import { useCallback, useEffect, useState } from "react";
import { fetchRecords } from "../api/client";
import type { FileRecordResponse, FileRecord, MetadataEntry } from "../types/api";
import {
  SimpleTable,
  HeaderObject,
  ValueGetterProps,
  ValueFormatterProps,
} from "simple-table-core";
import "simple-table-core/styles.css";

function RecordsRoute() {
  const [records, setRecords] = useState<FileRecord[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [seenHeaders, setSeenHeaders] = useState<HeaderObject[]>([]);

  const headers: HeaderObject[] = [
    // Fixed width in pixels
    { accessor: "id", label: "ID", width: "1fr", type: "string" },
    { accessor: "source_id", label: "Source", width: "1fr", type: "string" },
  ];

  const valueGetter = (props: ValueGetterProps) => props.accessor;

  const valueFormatter = (props: ValueFormatterProps) => {
    const metadata = props.row["metadata"];
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
      return "";
    }
    const meta = (metadata as Record<string, MetadataEntry[]>)[props.accessor];
    if (!meta || !Array.isArray(meta)) {
      return "";
    }
    const value = meta.map((entry) => JSON.stringify(entry.value)).join(", ");
    return value;
  };

  const loadRecords = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response: FileRecordResponse = await fetchRecords();
      const fetchedRecords = response.records ?? [];
      setRecords(fetchedRecords);
      const metadataCounts = response.stats?.metadata ?? {};
      setSeenHeaders(
        Object.keys(metadataCounts).map((key) => ({
          accessor: key,
          label: key,
          width: "1fr",
          type: "string",
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
      />
    </section>
  );
}

export default RecordsRoute;
