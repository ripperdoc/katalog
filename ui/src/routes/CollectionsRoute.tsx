import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { fetchCollections } from "../api/client";
import type { AssetCollection } from "../types/api";

function CollectionsRoute() {
  const navigate = useNavigate();
  const [collections, setCollections] = useState<AssetCollection[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadCollections = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchCollections();
      setCollections(response.collections || []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadCollections();
  }, [loadCollections]);

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Collections</h2>
          <p>All saved collections. Click one to open details.</p>
        </div>
        <button type="button" onClick={() => void loadCollections()} disabled={loading}>
          {loading ? "Loading..." : "Reload"}
        </button>
      </header>

      {error && <p className="error">{error}</p>}
      {!error && collections.length === 0 && !loading && (
        <div className="empty-state">No collections yet.</div>
      )}

      {collections.length > 0 && (
        <div className="table-responsive">
          <table className="collections-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Assets</th>
                <th>Mode</th>
                <th>Created</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {collections.map((col) => (
                <tr key={col.id} onClick={() => navigate(`/collections/${col.id}`)}>
                  <td>{col.name}</td>
                  <td>{col.asset_count ?? 0}</td>
                  <td>{col.refresh_mode ?? "on_demand"}</td>
                  <td>{col.created_at ?? "n/a"}</td>
                  <td>{col.updated_at ?? "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default CollectionsRoute;
