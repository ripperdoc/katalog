import { useCallback, useEffect, useState } from "react";

import { fetchWorkspaceStats } from "../api/client";
import AppHeader from "../components/AppHeader";
import type { WorkspaceStatsResponse } from "../types/api";

function StatsRoute() {
  const [stats, setStats] = useState<WorkspaceStatsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await fetchWorkspaceStats();
      setStats(payload);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadStats();
  }, [loadStats]);

  return (
    <>
      <AppHeader breadcrumbLabel="Stats">
        <div className="panel-actions">
          <button className="app-btn" type="button" onClick={() => void loadStats()} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {loading && !stats && <div className="empty-state">Loading stats...</div>}
          {stats && (
            <div className="record-list">
              <div className="file-card">
                <h3>Workspace Stats JSON</h3>
                <pre>{JSON.stringify(stats, null, 2)}</pre>
              </div>
            </div>
          )}
          {!stats && !loading && !error && <div className="empty-state">No stats available.</div>}
        </section>
      </main>
    </>
  );
}

export default StatsRoute;
