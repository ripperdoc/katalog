import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchChangesets } from "../api/client";
import AppHeader from "../components/AppHeader";
import type { Changeset } from "../types/api";

function ChangesetsRoute() {
  const navigate = useNavigate();
  const [changesets, setChangesets] = useState<Changeset[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadChangesets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchChangesets();
      setChangesets(response.changesets ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setChangesets([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadChangesets();
  }, [loadChangesets]);

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
            onClick={() => loadChangesets()}
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
            {changesets.map((snap) => (
              <div
                key={snap.id}
                className="file-card"
                onClick={() => navigate(`/changesets/${snap.id}`)}
                style={{ cursor: "pointer" }}
              >
                <div className="status-bar">
                  <span>Changeset #{snap.id}</span>
                  <span>{snap.status}</span>
                </div>
                <p>Provider: {snap.provider_name ?? snap.provider_id ?? "n/a"}</p>
                <small>
                  Started: {snap.started_at ?? "unknown"} | Completed: {snap.completed_at ?? "n/a"}
                </small>
              </div>
            ))}
            {!loading && changesets.length === 0 && (
              <div className="empty-state">No changesets found.</div>
            )}
          </div>
        </section>
      </main>
    </>
  );
}

export default ChangesetsRoute;
