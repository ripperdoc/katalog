import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { cancelSnapshot, fetchSnapshot, snapshotEventsUrl } from "../api/client";
import type { Snapshot } from "../types/api";

function SnapshotDetailRoute() {
  const { snapshotId } = useParams();
  const snapshotIdNum = snapshotId ? Number(snapshotId) : NaN;
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
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
        </div>
      )}
      {!snapshot && !loading && !error && <div className="empty-state">Snapshot not found.</div>}
    </section>
  );
}

export default SnapshotDetailRoute;
