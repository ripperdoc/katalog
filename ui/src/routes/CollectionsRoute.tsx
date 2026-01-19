import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { fetchCollections } from "../api/client";
import ListTable, { ListColumn } from "../components/ListTable";
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

  const columns: ListColumn<AssetCollection>[] = [
    { accessor: "name", label: "Name" },
    { accessor: "asset_count", label: "Assets", width: 100 },
    { accessor: "refresh_mode", label: "Mode", width: 120 },
    { accessor: "created_at", label: "Created", width: 180 },
    { accessor: "updated_at", label: "Updated", width: 180 },
  ];

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
      <ListTable
        items={collections}
        columns={columns}
        loading={loading}
        emptyMessage="No collections yet."
        onRowClick={(col) => navigate(`/collections/${col.id}`)}
      />
    </section>
  );
}

export default CollectionsRoute;
