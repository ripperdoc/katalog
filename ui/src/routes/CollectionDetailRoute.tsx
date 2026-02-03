import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import AssetTable from "../components/AssetTable";
import ActorList from "../components/ActorList";
import AppHeader from "../components/AppHeader";
import Sidebar from "../components/Sidebar";
import {
  deleteCollection,
  fetchActors,
  fetchCollection,
  fetchCollectionAssets,
  removeCollectionAssets,
  runAnalyzer,
  startManualChangeset,
  updateCollection,
} from "../api/client";
import type { Actor, AssetCollection, ViewAssetsResponse } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

const DEFAULT_VIEW_ID = "default";

function CollectionDetailRoute() {
  const { collectionId } = useParams();
  const navigate = useNavigate();
  const [collection, setCollection] = useState<AssetCollection | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [showAnalyzerSidebar, setShowAnalyzerSidebar] = useState(false);
  const [analyzers, setAnalyzers] = useState<Actor[]>([]);
  const [analyzersLoading, setAnalyzersLoading] = useState(false);
  const [analyzersError, setAnalyzersError] = useState<string | null>(null);
  const [runningAnalyzerId, setRunningAnalyzerId] = useState<number | null>(null);
  const [nameDraft, setNameDraft] = useState<string>("");
  const [descDraft, setDescDraft] = useState<string | undefined>(undefined);
  const [selectedAssetIds, setSelectedAssetIds] = useState<Set<number>>(new Set());
  const [removing, setRemoving] = useState(false);
  const [tableReloadKey, setTableReloadKey] = useState(0);
  const { active, startTracking } = useChangesetProgress();

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

  const loadAnalyzers = useCallback(async () => {
    setAnalyzersLoading(true);
    setAnalyzersError(null);
    try {
      const response = await fetchActors();
      const list = (response.actors ?? []).filter((actor) => actor.type === "ANALYZER");
      setAnalyzers(list);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setAnalyzersError(message);
      setAnalyzers([]);
    } finally {
      setAnalyzersLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!showAnalyzerSidebar) {
      return;
    }
    void loadAnalyzers();
  }, [showAnalyzerSidebar, loadAnalyzers]);

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

  const handleRemoveAssets = useCallback(async () => {
    if (!collection) {
      return;
    }
    const assetIds = Array.from(selectedAssetIds);
    if (assetIds.length === 0) {
      return;
    }
    const current = active[0];
    const currentIsManual = Boolean(current?.data && current.data["manual"]);
    if (current && !currentIsManual) {
      window.alert("A changeset is already running. Finish or cancel it before editing.");
      return;
    }

    setRemoving(true);
    try {
      let changesetId = current?.id;
      if (!changesetId) {
        const created = await startManualChangeset();
        startTracking(created);
        changesetId = created.id;
      }
      await removeCollectionAssets(collection.id, {
        asset_ids: assetIds,
        changeset_id: changesetId,
      });
      setSelectedAssetIds(new Set());
      setTableReloadKey((prev) => prev + 1);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to remove assets: ${message}`);
    } finally {
      setRemoving(false);
    }
  }, [collection, selectedAssetIds, active, startTracking]);

  const handleDelete = useCallback(async () => {
    if (!collection) {
      return;
    }
    if (
      !window.confirm(
        `Delete collection "${collection.name}"? This will remove all collection memberships.`,
      )
    ) {
      return;
    }
    setDeleting(true);
    try {
      await deleteCollection(collection.id);
      navigate("/collections");
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to delete collection: ${message}`);
    } finally {
      setDeleting(false);
    }
  }, [collection, navigate]);

  const handleRunAnalyzer = useCallback(
    async (actor: Actor) => {
      if (!collection) {
        return;
      }
      setRunningAnalyzerId(actor.id);
      setAnalyzersError(null);
      try {
        const changeset = await runAnalyzer(actor.id, { collectionId: collection.id });
        startTracking(changeset);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setAnalyzersError(message);
      } finally {
        setRunningAnalyzerId(null);
      }
    },
    [collection, startTracking],
  );

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
        <AppHeader breadcrumbLabel={collectionId ? `Collection ${collectionId}` : null}>
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
        <AppHeader breadcrumbLabel={collectionId ? `Collection ${collectionId}` : null}>
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
      <AppHeader
        breadcrumbLabel={collection?.name || (collectionId ? `Collection ${collectionId}` : null)}
      >
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
          {selectedAssetIds.size > 0 && (
            <button
              className="app-btn danger"
              type="button"
              onClick={() => void handleRemoveAssets()}
              disabled={removing || saving || deleting}
            >
              {removing
                ? "Removing…"
                : `Remove ${selectedAssetIds.size.toLocaleString()} assets`}
            </button>
          )}
          <button
            className="btn-primary"
            type="button"
            onClick={() => setShowAnalyzerSidebar(true)}
          >
            Run analyzer
          </button>
          <button
            className="app-btn danger"
            type="button"
            onClick={() => void handleDelete()}
            disabled={deleting}
          >
            {deleting ? "Deleting…" : "Delete"}
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
              key={tableReloadKey}
              title="Assets"
              fetchPage={fetchPage}
              searchPlaceholder="Search within collection…"
              onSelectionChange={setSelectedAssetIds}
            />
          </div>
        </section>
      </main>
      <Sidebar
        isOpen={showAnalyzerSidebar}
        title="Run analyzer"
        subtitle="Select an analyzer actor to run on this collection."
        onClose={() => setShowAnalyzerSidebar(false)}
      >
        {analyzersError && <p className="error">{analyzersError}</p>}
        <ActorList
          actors={analyzers}
          typeLabel="Analyzers"
          runningId={runningAnalyzerId}
          loading={analyzersLoading}
          showEdit={false}
          showToggle={false}
          showRun={true}
          runDisabled={runningAnalyzerId !== null}
          runContextLabel={collection.name}
          onRun={handleRunAnalyzer}
          emptyLabel="No analyzers configured."
        />
      </Sidebar>
    </>
  );
}

export default CollectionDetailRoute;
