import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import AssetTable from "../components/AssetTable";
import AppHeader from "../components/AppHeader";
import { fetchCollection, fetchCollectionAssets, updateCollection } from "../api/client";
import type { AssetCollection, ViewAssetsResponse } from "../types/api";

const DEFAULT_VIEW_ID = "default";

function CollectionDetailRoute() {
  const { collectionId } = useParams();
  const navigate = useNavigate();
  const [collection, setCollection] = useState<AssetCollection | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [nameDraft, setNameDraft] = useState<string>("");
  const [descDraft, setDescDraft] = useState<string | undefined>(undefined);

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
        setNameDraft(res.collection.name);
        setDescDraft(res.collection.description ?? "");
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
      }
    };
    void load();
  }, [collectionId]);

  const handleSaveMeta = useCallback(async () => {
    if (!collection) {
      return;
    }
    setSaving(true);
    try {
      const res = await updateCollection(collection.id, {
        name: nameDraft.trim() || collection.name,
        description: descDraft === undefined ? collection.description : descDraft,
      });
      setCollection(res.collection);
      setNameDraft(res.collection.name);
      setDescDraft(res.collection.description ?? "");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to update collection: ${message}`);
    } finally {
      setSaving(false);
    }
  }, [collection, nameDraft, descDraft]);

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
    [collectionId],
  );

  if (error) {
    return (
      <>
        <AppHeader>
          <div>
            <h2>Collections</h2>
            <p>Collection detail.</p>
          </div>
        </AppHeader>
        <main className="app-main">
          <section className="panel">
            <p className="error">Failed to load collection: {error}</p>
          </section>
        </main>
      </>
    );
  }

  if (!collection) {
    return (
      <>
        <AppHeader>
          <div>
            <h2>Collections</h2>
            <p>Loading collection…</p>
          </div>
        </AppHeader>
        <main className="app-main">
          <section className="panel">
            <p>Loading collection…</p>
          </section>
        </main>
      </>
    );
  }

  return (
    <>
      <AppHeader>
        <div className="collection-header-fields">
          <input
            className="collection-title-input"
            type="text"
            value={nameDraft}
            onChange={(e) => setNameDraft(e.target.value)}
            placeholder="Collection name"
            aria-label="Collection name"
          />
          <textarea
            className="collection-subtitle-input"
            value={descDraft ?? ""}
            onChange={(e) => setDescDraft(e.target.value)}
            placeholder="Optional description"
            rows={1}
            aria-label="Collection description"
          />
        </div>
        <div className="panel-actions">
          <button className="btn-primary" type="button" onClick={() => navigate("/collections")}>
            Back to collections
          </button>
          <button
            className="btn-primary"
            type="button"
            onClick={() => void handleSaveMeta()}
            disabled={saving}
          >
            {saving ? "Saving…" : "Save"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main app-main--locked">
        <section className="panel collection-detail">
          <div className="collection-assets">
            <AssetTable
              title="Assets"
              fetchPage={fetchPage}
              searchPlaceholder="Search within collection…"
            />
          </div>
        </section>
      </main>
    </>
  );
}

export default CollectionDetailRoute;
