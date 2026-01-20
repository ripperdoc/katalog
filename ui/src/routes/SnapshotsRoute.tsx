import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchSnapshots } from "../api/client";
import AppHeader from "../components/AppHeader";
import type { Snapshot } from "../types/api";

function SnapshotsRoute() {
  const navigate = useNavigate();
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
    <>
      <AppHeader>
        <div>
          <h2>History</h2>
          <p>Changes made to the data.</p>
        </div>
        <div className="panel-actions">
          <button
            type="button"
            className="app-btn btn-primary"
            onClick={() => loadSnapshots()}
            disabled={loading}
          >
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          <div className="record-list">
            {snapshots.map((snap) => (
              <div
                key={snap.id}
                className="file-card"
                onClick={() => navigate(`/snapshots/${snap.id}`)}
                style={{ cursor: "pointer" }}
              >
                <div className="status-bar">
                  <span>Snapshot #{snap.id}</span>
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
      </main>
    </>
  );
}

export default SnapshotsRoute;
