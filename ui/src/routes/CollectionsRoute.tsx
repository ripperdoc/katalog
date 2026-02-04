import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { HeaderObject, SimpleTable, type Row } from "simple-table-core";
import { fetchCollections } from "../api/client";
import AppHeader from "../components/AppHeader";
import type { AssetCollection } from "../types/api";
import "simple-table-core/styles.css";

function CollectionsRoute() {
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

  const headers: HeaderObject[] = useMemo(
    () => [
      {
        accessor: "id",
        label: "ID",
        width: 90,
        type: "number",
        cellRenderer: ({ value }) => {
          const id = typeof value === "number" ? value : Number(value);
          if (!id || Number.isNaN(id)) {
            return <span>{String(value ?? "")}</span>;
          }
          return <Link to={`/collections/${id}`}>{String(id)}</Link>;
        },
      },
      { accessor: "name", label: "Name", width: "1.5fr", type: "string" },
      { accessor: "asset_count", label: "Assets", width: 110, type: "number" },
      { accessor: "refresh_mode", label: "Mode", width: 140, type: "string" },
      { accessor: "created_at", label: "Created", width: 200, type: "date" },
      { accessor: "updated_at", label: "Updated", width: 200, type: "date" },
    ],
    [],
  );

  const rows = useMemo(() => collections.map((col) => ({ ...col }) as Row), [collections]);

  return (
    <>
      <AppHeader />
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          <div className="table-container">
            <SimpleTable
              defaultHeaders={headers}
              rows={rows}
              height="60vh"
              selectableCells={true}
              shouldPaginate={false}
              isLoading={loading}
            />
          </div>
        </section>
      </main>
    </>
  );
}

export default CollectionsRoute;
