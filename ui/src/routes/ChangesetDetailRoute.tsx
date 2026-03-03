import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import { cancelChangeset, fetchChangeset, fetchChangesetChanges, deleteChangeset } from "../api/client";
import type { Changeset, ChangesetChangeRecord, ChangesetEvent } from "../types/api";
import AppHeader from "../components/AppHeader";
import { HeaderObject, SimpleTable } from "simple-table-core";
import "simple-table-core/styles.css";
import { subscribeChangesetEvents } from "../utils/changesetEvents";
import { ActorCellPure } from "../components/ActorCell";
import AssetCell from "../components/AssetCell";
import ChangesetCell from "../components/ChangesetCell";
import { useRegistry } from "../utils/registry";

type ParsedChangesetRef =
  | { kind: "single"; id: number }
  | { kind: "range"; from: number; to: number }
  | { kind: "invalid"; reason: string };

function formatTableValue(value: unknown): string {
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
}

function parseChangesetRef(value: string | undefined): ParsedChangesetRef {
  if (!value || value.trim().length === 0) {
    return { kind: "invalid", reason: "Missing changeset id." };
  }
  const singleMatch = value.match(/^(\d+)$/);
  if (singleMatch) {
    return { kind: "single", id: Number(singleMatch[1]) };
  }

  const rangeMatch = value.match(/^(\d+)\.\.(\d+)$/);
  if (!rangeMatch) {
    return {
      kind: "invalid",
      reason: `Invalid changeset reference '${value}'. Use <id> or <from>..<to>.`,
    };
  }
  const from = Number(rangeMatch[1]);
  const to = Number(rangeMatch[2]);
  if (from > to) {
    return {
      kind: "invalid",
      reason: "Invalid range: <from> must be <= <to>.",
    };
  }
  return { kind: "range", from, to };
}

function ChangesetDetailRoute() {
  const { changesetRef } = useParams();
  const parsedRef = useMemo(() => parseChangesetRef(changesetRef), [changesetRef]);
  const isSingle = parsedRef.kind === "single";
  const isRange = parsedRef.kind === "range";
  const primaryChangesetId =
    parsedRef.kind === "single"
      ? parsedRef.id
      : parsedRef.kind === "range"
        ? parsedRef.from
        : NaN;
  const fromChangesetId = parsedRef.kind === "range" ? parsedRef.from : undefined;
  const toChangesetId = parsedRef.kind === "range" ? parsedRef.to : undefined;

  const [changeset, setChangeset] = useState<Changeset | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [changesLoading, setChangesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [changesError, setChangesError] = useState<string | null>(null);
  const [changesView, setChangesView] = useState<"raw" | "diff">("raw");
  const [rawChanges, setRawChanges] = useState<ChangesetChangeRecord[]>([]);
  const [diffRows, setDiffRows] = useState<Record<string, unknown>[]>([]);
  const [changesWarnings, setChangesWarnings] = useState<string[]>([]);
  const [changesTotal, setChangesTotal] = useState<number | null>(null);
  const [changesPage, setChangesPage] = useState<number>(1);
  const changesLimit = 200;
  const { data: registryData } = useRegistry();

  const statusRef = useRef<Changeset["status"] | null>(null);
  const streamCleanupRef = useRef<(() => void) | null>(null);

  const stopStream = useCallback(() => {
    if (streamCleanupRef.current) {
      streamCleanupRef.current();
      streamCleanupRef.current = null;
    }
  }, []);

  const loadChangeset = useCallback(async () => {
    if (parsedRef.kind === "invalid") {
      setError(parsedRef.reason);
      setChangeset(null);
      setLogs([]);
      stopStream();
      return;
    }
    if (parsedRef.kind === "range") {
      setError(null);
      setChangeset(null);
      setLogs([]);
      stopStream();
      return;
    }
    if (!primaryChangesetId || Number.isNaN(primaryChangesetId)) {
      setError("Invalid changeset id");
      setChangeset(null);
      setLogs([]);
      stopStream();
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const response = await fetchChangeset(primaryChangesetId);
      setChangeset(response.changeset);
      statusRef.current = response.changeset.status;
      if (response.changeset.status !== "in_progress") {
        stopStream();
      }
      const logLines =
        response.logs
          ?.filter((evt) => evt.event === "log")
          .map((evt) => {
            const message = evt.payload?.["message"];
            return typeof message === "string" ? message : JSON.stringify(evt.payload);
          }) ?? [];
      setLogs(logLines);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setChangeset(null);
      setLogs([]);
    } finally {
      setLoading(false);
    }
  }, [parsedRef, primaryChangesetId, stopStream]);

  useEffect(() => {
    void loadChangeset();
  }, [loadChangeset]);

  const loadChanges = useCallback(
    async (page: number = 1) => {
      if (parsedRef.kind === "invalid") {
        setChangesError(parsedRef.reason);
        setRawChanges([]);
        setDiffRows([]);
        setChangesWarnings([]);
        setChangesTotal(null);
        return;
      }
      if (!primaryChangesetId || Number.isNaN(primaryChangesetId)) {
        setChangesError("Invalid changeset id");
        setRawChanges([]);
        setDiffRows([]);
        setChangesWarnings([]);
        setChangesTotal(null);
        return;
      }

      setChangesLoading(true);
      setChangesError(null);
      try {
        const offset = (page - 1) * changesLimit;
        const response = await fetchChangesetChanges(primaryChangesetId, {
          view: changesView,
          offset,
          limit: changesLimit,
          fromChangesetId,
          toChangesetId,
        });
        if (response.mode === "diff") {
          setDiffRows(response.items ?? []);
          setRawChanges([]);
          setChangesWarnings(response.warnings ?? []);
        } else {
          setRawChanges(response.items ?? []);
          setDiffRows([]);
          setChangesWarnings(response.warnings ?? []);
        }
        setChangesTotal(response.stats?.total ?? null);
        setChangesPage(page);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setChangesError(message);
        setRawChanges([]);
        setDiffRows([]);
        setChangesWarnings([]);
        setChangesTotal(null);
      } finally {
        setChangesLoading(false);
      }
    },
    [changesLimit, changesView, fromChangesetId, parsedRef, primaryChangesetId, toChangesetId],
  );

  useEffect(() => {
    void loadChanges(1);
  }, [loadChanges]);

  useEffect(() => {
    if (!isSingle || !primaryChangesetId || Number.isNaN(primaryChangesetId)) {
      stopStream();
      return;
    }
    if (changeset?.status !== "in_progress") {
      stopStream();
      return;
    }
    if (streamCleanupRef.current) {
      return;
    }

    const cleanup = subscribeChangesetEvents(
      primaryChangesetId,
      (payload: ChangesetEvent) => {
        if (payload.event === "log") {
          const message = payload.payload?.["message"];
          const line = typeof message === "string" ? message : JSON.stringify(payload.payload);
          setLogs((prev) => (prev.includes(line) ? prev : [...prev, line]));
          return;
        }
        if (payload.event === "changeset_status" || payload.event === "changeset_start") {
          const next = payload.payload as unknown as Changeset;
          setChangeset((prev) => {
            if (
              prev &&
              prev.id === next.id &&
              prev.status === next.status &&
              prev.running_time_ms === next.running_time_ms &&
              prev.message === next.message
            ) {
              return prev;
            }
            return next;
          });
          statusRef.current = next.status;
          if (next.status !== "in_progress") {
            stopStream();
          }
        }
      },
      () => {
        if (statusRef.current === "in_progress") {
          setError((prev) => prev ?? "Stream disconnected");
        }
      },
    );
    streamCleanupRef.current = cleanup;

    return () => {
      if (streamCleanupRef.current === cleanup) {
        cleanup();
        streamCleanupRef.current = null;
      } else {
        cleanup();
      }
    };
  }, [changeset?.status, isSingle, primaryChangesetId, stopStream]);

  const requestCancel = async () => {
    if (!isSingle || !primaryChangesetId || Number.isNaN(primaryChangesetId)) {
      setError("Cancel is only available for a single changeset.");
      return;
    }
    setCancelling(true);
    setError(null);
    try {
      const res = await cancelChangeset(primaryChangesetId);
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
    if (!isSingle || !primaryChangesetId || Number.isNaN(primaryChangesetId)) {
      setError("Discard is only available for a single changeset.");
      return;
    }
    setDeleting(true);
    setError(null);
    try {
      await deleteChangeset(primaryChangesetId);
      // After delete, clear UI state.
      setChangeset(null);
      setLogs([]);
      setRawChanges([]);
      setDiffRows([]);
      setChangesWarnings([]);
      setChangesTotal(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setDeleting(false);
    }
  };

  const rawHeaders: HeaderObject[] = [
    { accessor: "id", label: "ID", width: 80, type: "number" },
    ...(isRange
      ? [
          {
            accessor: "changeset_id",
            label: "Changeset",
            width: 120,
            type: "number" as const,
            cellRenderer: ChangesetCell,
          },
        ]
      : []),
    { accessor: "asset_id", label: "Asset", width: 120, type: "number", cellRenderer: AssetCell },
    {
      accessor: "actor_id",
      label: "Actor",
      width: 120,
      type: "number",
      cellRenderer: (props) => (
        <ActorCellPure {...props} actorsById={registryData?.actorsById} />
      ),
    },
    { accessor: "metadata_key", label: "Metadata Key", width: "2fr", type: "string" },
    {
      accessor: "value",
      label: "Value",
      width: "2fr",
      type: "string",
      valueFormatter: ({ row }) => formatTableValue(row["value"]),
    },
    {
      accessor: "removed",
      label: "Removed",
      width: 100,
      type: "string",
      valueFormatter: ({ row }) => (row["removed"] ? "yes" : "no"),
    },
  ];

  const diffHeaders: HeaderObject[] = [
    {
      accessor: "id",
      label: "ID",
      width: "1.4fr",
      type: "string",
    },
    {
      accessor: "asset_id",
      label: "Asset",
      width: 120,
      type: "number",
      cellRenderer: AssetCell,
    },
    {
      accessor: "metadata_key",
      label: "Metadata Key",
      width: "1.8fr",
      type: "string",
    },
    {
      accessor: "actor_id",
      label: "Actor",
      width: 120,
      type: "number",
      cellRenderer: (props) => (
        <ActorCellPure {...props} actorsById={registryData?.actorsById} />
      ),
    },
    {
      accessor: "before",
      label: "Previous Value",
      width: "2fr",
      type: "string",
      valueFormatter: ({ row }) => formatTableValue(row["before"]),
    },
    {
      accessor: "after",
      label: "New Value",
      width: "2fr",
      type: "string",
      valueFormatter: ({ row }) => formatTableValue(row["after"]),
    },
    {
      accessor: "change_type",
      label: "Change",
      width: 120,
      type: "string",
    },
  ];

  const isRunning =
    isSingle && changeset?.status === "in_progress" && changeset?.running !== false;
  const currentHeaders = changesView === "diff" ? diffHeaders : rawHeaders;
  const currentRows =
    changesView === "diff"
      ? diffRows
      : (rawChanges as unknown as Record<string, unknown>[]);
  const breadcrumbLabel =
    parsedRef.kind === "range"
      ? `Changesets ${parsedRef.from}..${parsedRef.to}`
      : parsedRef.kind === "single"
        ? `Changeset ${parsedRef.id}`
        : null;
  const showActionButtons = isSingle && changeset !== null;
  const showChangesTable = isRange || changeset !== null;
  const showChangesetCards = isSingle && changeset !== null;

  return (
    <>
      <AppHeader breadcrumbLabel={breadcrumbLabel}>
        <div className="panel-actions">
          {showActionButtons && isRunning && (
            <button
              className="app-btn danger"
              type="button"
              onClick={requestCancel}
              disabled={cancelling}
            >
              {cancelling ? "Cancelling..." : "Cancel"}
            </button>
          )}
          {showActionButtons && (
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
          {showChangesTable && (
            <div className="record-list">
              {showChangesetCards && (
                <>
                  <div className="file-card">
                    <h3>Changeset JSON</h3>
                    <pre>{JSON.stringify(changeset, null, 2)}</pre>
                  </div>
                  <div className="file-card">
                    <h3>Logs</h3>
                    <pre className="log-stream">{logs.join("\n")}</pre>
                  </div>
                </>
              )}
              <div className="file-card" style={{ width: "100%" }}>
                <div className="panel-header" style={{ padding: 0, marginBottom: "0.5rem" }}>
                  <div>
                    <h3>Changes</h3>
                  </div>
                </div>
                <div className="table-toolbar">
                  <button
                    type="button"
                    className={`button-toggle ${changesView === "raw" ? "is-active" : ""}`}
                    aria-pressed={changesView === "raw"}
                    onClick={() => setChangesView("raw")}
                  >
                    Raw
                  </button>
                  <button
                    type="button"
                    className={`button-toggle ${changesView === "diff" ? "is-active" : ""}`}
                    aria-pressed={changesView === "diff"}
                    onClick={() => setChangesView("diff")}
                  >
                    Diff
                  </button>
                </div>
                {isRange && (
                  <div className="note">
                    Range: {fromChangesetId}..{toChangesetId}
                  </div>
                )}
                {changesWarnings.length > 0 && (
                  <div className="note">{changesWarnings.join(" ")}</div>
                )}
                {!changesLoading && !changesError && currentRows.length === 0 && (
                  <div className="note">
                    {changesView === "diff"
                      ? "No metadata diff rows for this changeset range."
                      : "No metadata rows were recorded for this changeset range."}
                  </div>
                )}
                {changesError && <p className="error">{changesError}</p>}
                <SimpleTable
                  defaultHeaders={currentHeaders}
                  rows={currentRows as Record<string, any>[]}
                  height={"60vh"}
                  selectableCells={true}
                  shouldPaginate={true}
                  rowsPerPage={changesLimit}
                  serverSidePagination={true}
                  totalRowCount={changesTotal ?? currentRows.length}
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
          {!isRange && !changeset && !loading && !error && (
            <div className="empty-state">Changeset not found.</div>
          )}
        </section>
      </main>
    </>
  );
}

export default ChangesetDetailRoute;
