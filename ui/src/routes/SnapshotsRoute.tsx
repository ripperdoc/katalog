import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchSnapshots } from "../api/client";
import type { Snapshot } from "../types/api";

function SnapshotsRoute() {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadSnapshots = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchSnapshots();
      setSnapshots(response.snapshots ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setSnapshots([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadSnapshots();
  }, [loadSnapshots]);

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>History</h2>
          <p>Changes made to the data.</p>
        </div>
        <button type="button" onClick={() => loadSnapshots()} disabled={loading}>
          {loading ? "Loading..." : "Refresh"}
        </button>
      </header>
      {error && <p className="error">{error}</p>}
      <div className="record-list">
        {snapshots.map((snap) => (
          <div key={snap.id} className="file-card">
            <div className="status-bar">
              <Link to={`/snapshots/${snap.id}`}>Snapshot #{snap.id}</Link>
              <span>{snap.status}</span>
            </div>
            <p>Provider: {snap.provider_name ?? snap.provider_id ?? "n/a"}</p>
            <small>
              Started: {snap.started_at ?? "unknown"} | Completed: {snap.completed_at ?? "n/a"}
            </small>
          </div>
        ))}
        {!loading && snapshots.length === 0 && (
          <div className="empty-state">No snapshots found.</div>
        )}
      </div>
    </section>
  );
}

export default SnapshotsRoute;
