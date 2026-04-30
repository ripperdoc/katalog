import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactHeaderObject,
  SimpleTable,
  type CellRendererProps,
  type Row,
} from "@simple-table/react";
import { fetchCollections } from "../api/client";
import AppHeader from "../components/AppHeader";
import AppLink from "../components/AppLink";
import { simpleTableLegacyAppearance } from "../components/simpleTableAppearance";
import type { AssetCollection } from "../types/api";
import "@simple-table/react/styles.css";

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

  const headers: ReactHeaderObject[] = useMemo(
    () => [
      {
        accessor: "id",
        label: "ID",
        width: 90,
        type: "number",
        cellRenderer: ({ value }: CellRendererProps) => {
          const id = typeof value === "number" ? value : Number(value);
          if (!id || Number.isNaN(id)) {
            return <span>{String(value ?? "")}</span>;
          }
          return <AppLink to={`/collections/${id}`}>{String(id)}</AppLink>;
        },
      },
      {
        accessor: "name",
        label: "Name",
        width: "1.5fr",
        type: "string",
        cellRenderer: (props: CellRendererProps) => {
          const idValue = props.row?.id;
          const id = typeof idValue === "number" ? idValue : Number(idValue);
          const label = String(props.value ?? "");
          if (!id || Number.isNaN(id)) {
            return <span>{label}</span>;
          }
          return <AppLink to={`/collections/${id}`}>{label}</AppLink>;
        },
      },
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
              {...simpleTableLegacyAppearance}
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
