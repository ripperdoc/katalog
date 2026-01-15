import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import DataTable from "../components/DataTable";
import { fetchCollection, fetchCollectionAssets } from "../api/client";
import type { AssetCollection, ViewAssetsResponse } from "../types/api";

const DEFAULT_VIEW_ID = "default";

function CollectionDetailRoute() {
  const { collectionId } = useParams();
  const navigate = useNavigate();
  const [collection, setCollection] = useState<AssetCollection | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      if (!collectionId) {
        return;
      }
      const idNum = Number(collectionId);
      if (!Number.isFinite(idNum)) {
        setError("Invalid collection id");
        return;
      }
      try {
        const res = await fetchCollection(idNum);
        setCollection(res.collection);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
      }
    };
    void load();
  }, [collectionId]);

  const fetchPage = useCallback(
    ({
      offset,
      limit,
      sort,
      filters,
      search,
    }: {
      offset: number;
      limit: number;
      sort?: string;
      filters?: string[];
      search?: string;
    }) => {
      if (!collectionId) {
        return Promise.resolve({
          items: [],
          schema: [],
          stats: { returned: 0, total: 0, duration_ms: 0 },
          pagination: { offset, limit },
        });
      }
      return fetchCollectionAssets(Number(collectionId), {
        viewId: DEFAULT_VIEW_ID,
        offset,
        limit,
        sort,
        filters,
        search,
      });
    },
    [collectionId]
  );

  if (error) {
    return (
      <section className="panel">
        <p className="error">Failed to load collection: {error}</p>
      </section>
    );
  }

  if (!collection) {
    return (
      <section className="panel">
        <p>Loading collection…</p>
      </section>
    );
  }

  return (
    <DataTable
      title={collection.name}
      subtitle={
        collection.description
          ? collection.description
          : `Collection #${collection.id} · ${collection.asset_count ?? 0} assets`
      }
      fetchPage={fetchPage}
      searchPlaceholder="Search within collection…"
      actions={
        <button type="button" onClick={() => navigate("/collections")}>
          Back to collections
        </button>
      }
    />
  );
}

export default CollectionDetailRoute;
