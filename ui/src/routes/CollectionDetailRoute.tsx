import { useCallback, useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import AssetTable from "../components/AssetTable";
import AppHeader from "../components/AppHeader";
import {
  deleteCollection,
  fetchCollection,
  fetchCollectionAssets,
  fetchWorkflows,
  removeCollectionAssets,
  startWorkflow,
  startManualChangeset,
  updateCollection,
} from "../api/client";
import type { AssetCollection, ViewAssetsResponse, WorkflowSummary } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

const DEFAULT_VIEW_ID = "default";

function CollectionDetailRoute() {
  const { collectionId } = useParams();
  const navigate = useNavigate();
  const [collection, setCollection] = useState<AssetCollection | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [runningWorkflowName, setRunningWorkflowName] = useState<string | null>(null);
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [workflowMenuOpen, setWorkflowMenuOpen] = useState(false);
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

  useEffect(() => {
    const loadWorkflows = async () => {
      try {
        const response = await fetchWorkflows();
        setWorkflows((response.workflows ?? []).filter((workflow) => workflow.status !== "invalid"));
      } catch {
        setWorkflows([]);
      }
    };
    void loadWorkflows();
  }, []);

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

  const handleRunWorkflowForCollection = useCallback(async (workflowName: string) => {
    if (!collection) {
      return;
    }
    if (workflows.length === 0) {
      window.alert("No runnable workflows available.");
      return;
    }
    const selectedWorkflow = workflows.find((workflow) => workflow.file_name === workflowName);
    if (!selectedWorkflow) {
      window.alert("Unknown workflow name.");
      return;
    }
    setRunningWorkflowName(workflowName);
    setWorkflowMenuOpen(false);
    try {
      const response = await startWorkflow(workflowName, {
        input: {
          kind: "collection",
          collection_id: collection.id,
        },
      });
      if (response.changeset) {
        startTracking(response.changeset);
        navigate(`/changesets/${response.changeset.id}`);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to run workflow: ${message}`);
    } finally {
      setRunningWorkflowName(null);
    }
  }, [collection, navigate, startTracking, workflows]);

  useEffect(() => {
    if (!workflowMenuOpen) {
      return;
    }
    const handlePointerDown = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest(".workflow-run-dropdown")) {
        return;
      }
      setWorkflowMenuOpen(false);
    };
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setWorkflowMenuOpen(false);
      }
    };
    window.addEventListener("mousedown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [workflowMenuOpen]);

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
      sort?: [string, "asc" | "desc"][];
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
          <div className="collection-dropdown workflow-run-dropdown">
            <button
              className="app-btn btn-action collection-dropdown-trigger"
              type="button"
              onClick={() => setWorkflowMenuOpen((open) => !open)}
              disabled={runningWorkflowName !== null || workflows.length === 0}
            >
              {runningWorkflowName ? "Running workflow..." : "Run workflow on collection"}
            </button>
            {workflowMenuOpen ? (
              <div className="collection-dropdown-menu">
                {workflows.map((workflow) => (
                  <button
                    key={workflow.file_name}
                    type="button"
                    className="app-btn btn-action collection-dropdown-item"
                    onClick={() => void handleRunWorkflowForCollection(workflow.file_name)}
                  >
                    {workflow.status === "not-synced"
                      ? `Sync and run · ${workflow.name}`
                      : `Run · ${workflow.name}`}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
          <button
            className="app-btn danger"
            type="button"
            onClick={() => void handleDelete()}
            disabled={deleting}
          >
            {deleting ? "Deleting…" : "Delete"}
          </button>
          <button
            className="app-btn btn-save"
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
    </>
  );
}

export default CollectionDetailRoute;
