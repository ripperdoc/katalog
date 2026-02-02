import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import {
  cancelChangeset,
  fetchChangeset,
  fetchChangesetChanges,
  deleteChangeset,
  changesetEventsUrl,
} from "../api/client";
import type { Changeset, ChangesetChangeRecord } from "../types/api";
import AppHeader from "../components/AppHeader";
import { HeaderObject, SimpleTable } from "simple-table-core";
import "simple-table-core/styles.css";

function ChangesetDetailRoute() {
  const { changesetId } = useParams();
  const changesetIdNum = changesetId ? Number(changesetId) : NaN;
  const [changeset, setChangeset] = useState<Changeset | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [changesLoading, setChangesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [changesError, setChangesError] = useState<string | null>(null);
  const [changes, setChanges] = useState<ChangesetChangeRecord[]>([]);
  const [changesTotal, setChangesTotal] = useState<number | null>(null);
  const [changesPage, setChangesPage] = useState<number>(1);
  const changesLimit = 200;
  const streamRef = useRef<EventSource | null>(null);

  const isRunning = useMemo(
    () => changeset?.status === "in_progress" && changeset?.running !== false,
    [changeset?.status, changeset?.running],
  );

  const loadChangeset = useCallback(async () => {
    if (!changesetIdNum || Number.isNaN(changesetIdNum)) {
      setError("Invalid changeset id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchChangeset(changesetIdNum);
      setChangeset(response.changeset);
      setLogs(response.logs ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setChangeset(null);
      setLogs([]);
    } finally {
      setLoading(false);
    }
  }, [changesetIdNum]);

  useEffect(() => {
    void loadChangeset();
  }, [loadChangeset]);

  const loadChanges = useCallback(
    async (page: number = 1) => {
      if (!changesetIdNum || Number.isNaN(changesetIdNum)) {
        setChangesError("Invalid changeset id");
        return;
      }
      setChangesLoading(true);
      setChangesError(null);
      try {
        const offset = (page - 1) * changesLimit;
        const response = await fetchChangesetChanges(changesetIdNum, {
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
    [changesetIdNum, changesLimit],
  );

  useEffect(() => {
    void loadChanges(1);
  }, [loadChanges]);

  useEffect(() => {
    if (!changesetIdNum || Number.isNaN(changesetIdNum)) {
      return;
    }
    const url = changesetEventsUrl(changesetIdNum);
    const source = new EventSource(url);
    streamRef.current = source;

    const handleLog = (event: MessageEvent) => {
      const message = typeof event.data === "string" ? event.data : JSON.stringify(event.data);
      setLogs((prev) => (prev.includes(message) ? prev : [...prev, message]));
    };

    const handleChangeset = (event: MessageEvent) => {
      try {
        const payload = JSON.parse(event.data) as Changeset;
        setChangeset(payload);
        if (payload.status !== "in_progress") {
          source.close();
          streamRef.current = null;
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    };

    source.addEventListener("log", handleLog);
    source.addEventListener("changeset", handleChangeset);
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
      source.removeEventListener("changeset", handleChangeset);
      source.close();
      streamRef.current = null;
    };
  }, [changesetIdNum, isRunning]);

  const requestCancel = async () => {
    if (!changesetIdNum || Number.isNaN(changesetIdNum)) {
      setError("Invalid changeset id");
      return;
    }
    setCancelling(true);
    setError(null);
    try {
      const res = await cancelChangeset(changesetIdNum);
      if (res.changeset) {
        setChangeset(res.changeset);
      } else {
        await loadChangeset();
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setCancelling(false);
    }
  };

  const requestDelete = async () => {
    if (!changesetIdNum || Number.isNaN(changesetIdNum)) {
      setError("Invalid changeset id");
      return;
    }
    setDeleting(true);
    setError(null);
    try {
      await deleteChangeset(changesetIdNum);
      // After delete, clear UI state.
      setChangeset(null);
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
    { accessor: "actor_id", label: "Actor", width: 120, type: "number" },
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
    <>
      <AppHeader breadcrumbLabel={changesetId ? `Changeset ${changesetId}` : null}>
        <div className="button-row">
          {isRunning && (
            <button
              className="app-btn btn-primary"
              type="button"
              onClick={requestCancel}
              disabled={cancelling}
            >
              {cancelling ? "Cancelling..." : "Cancel"}
            </button>
          )}
          {changeset && (
            <button
              type="button"
              onClick={requestDelete}
              disabled={deleting}
              className="app-btn danger"
              title="Delete this changeset and undo its changes"
            >
              {deleting ? "Deleting..." : "Discard changeset"}
            </button>
          )}
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {changeset && (
            <div className="record-list">
              <div className="file-card">
                <h3>Changeset JSON</h3>
                <pre>{JSON.stringify(changeset, null, 2)}</pre>
              </div>
              <div className="file-card">
                <h3>Logs</h3>
                <pre className="log-stream">{logs.join("\n")}</pre>
              </div>
              <div className="file-card" style={{ width: "100%" }}>
                <div className="panel-header" style={{ padding: 0, marginBottom: "0.5rem" }}>
                  <div>
                    <h3>Changes</h3>
                  </div>
                  <div className="panel-actions">
                    <button
                      type="button"
                      className="app-btn btn-primary"
                      onClick={() => void loadChanges(changesPage)}
                      disabled={changesLoading}
                    >
                      {changesLoading ? "Loading..." : "Reload"}
                    </button>
                  </div>
                </div>
                {changesError && <p className="error">{changesError}</p>}
                <SimpleTable
                  defaultHeaders={changeHeaders}
                  rows={changes as unknown as Record<string, any>[]}
                  height={"60vh"}
                  selectableCells={true}
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
          {!changeset && !loading && !error && (
            <div className="empty-state">Changeset not found.</div>
          )}
        </section>
      </main>
    </>
  );
}

export default ChangesetDetailRoute;
