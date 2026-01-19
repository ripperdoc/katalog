import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  cancelSnapshot,
  fetchSnapshot,
  fetchSnapshotChanges,
  deleteSnapshot,
  snapshotEventsUrl,
} from "../api/client";
import type { Snapshot, SnapshotChangeRecord } from "../types/api";
import { HeaderObject, SimpleTable } from "simple-table-core";
import "simple-table-core/styles.css";

function SnapshotDetailRoute() {
  const { snapshotId } = useParams();
  const snapshotIdNum = snapshotId ? Number(snapshotId) : NaN;
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [changesLoading, setChangesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [changesError, setChangesError] = useState<string | null>(null);
  const [changes, setChanges] = useState<SnapshotChangeRecord[]>([]);
  const [changesTotal, setChangesTotal] = useState<number | null>(null);
  const [changesPage, setChangesPage] = useState<number>(1);
  const changesLimit = 200;
  const streamRef = useRef<EventSource | null>(null);

  const isRunning = useMemo(
    () => snapshot?.status === "in_progress",
    [snapshot?.status]
  );

  const loadSnapshot = useCallback(async () => {
    if (!snapshotIdNum || Number.isNaN(snapshotIdNum)) {
      setError("Invalid snapshot id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchSnapshot(snapshotIdNum);
      setSnapshot(response.snapshot);
      setLogs(response.logs ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setSnapshot(null);
      setLogs([]);
    } finally {
      setLoading(false);
    }
  }, [snapshotIdNum]);

  useEffect(() => {
    void loadSnapshot();
  }, [loadSnapshot]);

  const loadChanges = useCallback(
    async (page: number = 1) => {
      if (!snapshotIdNum || Number.isNaN(snapshotIdNum)) {
        setChangesError("Invalid snapshot id");
        return;
      }
      setChangesLoading(true);
      setChangesError(null);
      try {
        const offset = (page - 1) * changesLimit;
        const response = await fetchSnapshotChanges(snapshotIdNum, {
          offset,
          limit: changesLimit,
        });
        setChanges(response.items ?? []);
        setChangesTotal(response.stats?.total ?? null);
        setChangesPage(page);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setChangesError(message);
        setChanges([]);
        setChangesTotal(null);
      } finally {
        setChangesLoading(false);
      }
    },
    [snapshotIdNum, changesLimit]
  );

  useEffect(() => {
    void loadChanges(1);
  }, [loadChanges]);

  useEffect(() => {
    if (!snapshotIdNum || Number.isNaN(snapshotIdNum)) {
      return;
    }
    const url = snapshotEventsUrl(snapshotIdNum);
    const source = new EventSource(url);
    streamRef.current = source;

    const handleLog = (event: MessageEvent) => {
      const message = typeof event.data === "string" ? event.data : JSON.stringify(event.data);
      setLogs((prev) => (prev.includes(message) ? prev : [...prev, message]));
    };

    const handleSnapshot = (event: MessageEvent) => {
      try {
        const payload = JSON.parse(event.data) as Snapshot;
        setSnapshot(payload);
        if (payload.status !== "in_progress") {
          source.close();
          streamRef.current = null;
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    };

    source.addEventListener("log", handleLog);
    source.addEventListener("snapshot", handleSnapshot);
    source.onerror = () => {
      setError((prev) => prev ?? "Stream disconnected");
      // If already finished, close to avoid reconnect loops.
      if (!isRunning) {
        source.close();
        streamRef.current = null;
      }
    };

    return () => {
      source.removeEventListener("log", handleLog);
      source.removeEventListener("snapshot", handleSnapshot);
      source.close();
      streamRef.current = null;
    };
  }, [snapshotIdNum, isRunning]);

  const requestCancel = async () => {
    if (!snapshotIdNum || Number.isNaN(snapshotIdNum)) {
      setError("Invalid snapshot id");
      return;
    }
    setCancelling(true);
    setError(null);
    try {
      await cancelSnapshot(snapshotIdNum);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setCancelling(false);
    }
  };

  const requestDelete = async () => {
    if (!snapshotIdNum || Number.isNaN(snapshotIdNum)) {
      setError("Invalid snapshot id");
      return;
    }
    setDeleting(true);
    setError(null);
    try {
      await deleteSnapshot(snapshotIdNum);
      // After delete, clear UI state.
      setSnapshot(null);
      setLogs([]);
      setChanges([]);
      setChangesTotal(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setDeleting(false);
    }
  };

  const changeHeaders: HeaderObject[] = [
    { accessor: "id", label: "ID", width: 80, type: "number" },
    { accessor: "asset_id", label: "Asset", width: 120, type: "number" },
    { accessor: "provider_id", label: "Provider", width: 120, type: "number" },
    { accessor: "metadata_key", label: "Metadata Key", width: "2fr", type: "string" },
    {
      accessor: "value",
      label: "Value",
      width: "2fr",
      type: "string",
      valueFormatter: ({ row }) => {
        const value = row["value"];
        if (value === null || value === undefined) {
          return "";
        }
        if (typeof value === "object") {
          try {
            return JSON.stringify(value);
          } catch {
            return String(value);
          }
        }
        return String(value);
      },
    },
    {
      accessor: "removed",
      label: "Removed",
      width: 100,
      type: "string",
      valueFormatter: ({ row }) => (row["removed"] ? "yes" : "no"),
    },
  ];

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Snapshot #{snapshotId}</h2>
          <p>Live view of scan progress and logs.</p>
        </div>
        <div className="button-row">
          <Link to="/snapshots" className="link-button">
            Back
          </Link>
          {isRunning && (
            <button type="button" onClick={requestCancel} disabled={cancelling}>
              {cancelling ? "Cancelling..." : "Cancel"}
            </button>
          )}
          {!isRunning && snapshot && (
            <button
              type="button"
              onClick={requestDelete}
              disabled={deleting}
              className="danger"
              title="Delete this snapshot and undo its changes"
            >
              {deleting ? "Deleting..." : "Delete snapshot"}
            </button>
          )}
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      {snapshot && (
        <div className="record-list">
          <div className="file-card">
            <h3>Snapshot JSON</h3>
            <pre>{JSON.stringify(snapshot, null, 2)}</pre>
          </div>
          <div className="file-card">
            <h3>Logs</h3>
            <pre className="log-stream">{logs.join("\n")}</pre>
          </div>
          <div className="file-card" style={{ width: "100%" }}>
            <div className="panel-header" style={{ padding: 0, marginBottom: "0.5rem" }}>
              <div>
                <h3>Changes</h3>
                {!changesLoading && (
                  <p>
                    Total {changesTotal ?? changes.length}, page {changesPage}
                  </p>
                )}
              </div>
              <div className="panel-actions">
                <button
                  type="button"
                  onClick={() => void loadChanges(changesPage)}
                  disabled={changesLoading}
                >
                  {changesLoading ? "Loading..." : "Reload"}
                </button>
              </div>
            </div>
            {changesError && <p className="error">{changesError}</p>}
            {!changesError && !changesLoading && changes.length === 0 && (
              <div className="empty-state">No changes in this snapshot.</div>
            )}
            <SimpleTable
              defaultHeaders={changeHeaders}
              rows={changes}
              height={"60vh"}
              selectableCells={true}
              rowIdAccessor="id"
              shouldPaginate={true}
              rowsPerPage={changesLimit}
              serverSidePagination={true}
              totalRowCount={changesTotal ?? changes.length}
              onPageChange={(page) => {
                if (page === changesPage) {
                  return;
                }
                void loadChanges(page);
              }}
              isLoading={changesLoading}
            />
          </div>
        </div>
      )}
      {!snapshot && !loading && !error && <div className="empty-state">Snapshot not found.</div>}
    </section>
  );
}

export default SnapshotDetailRoute;
