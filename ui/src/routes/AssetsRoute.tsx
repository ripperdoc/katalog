import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import AssetTable from "../components/AssetTable";
import AppHeader from "../components/AppHeader";
import {
  addCollectionAssets,
  createCollection,
  fetchAssets,
  fetchCollections,
  fetchMetadataSearch,
  fetchViews,
  fetchWorkflows,
  startWorkflow,
} from "../api/client";
import type { AssetCollection, ViewAssetsResponse, ViewSpec, WorkflowSummary } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";
import { useStringSearchParamState } from "../utils/useStringSearchParamState";

const DEFAULT_VIEW_ID = "default";

function AssetsRoute() {
  const navigate = useNavigate();
  const [views, setViews] = useState<ViewSpec[]>([]);
  const [collections, setCollections] = useState<AssetCollection[]>([]);
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [selectedViewId, setSelectedViewId] = useStringSearchParamState("view", DEFAULT_VIEW_ID);
  const [viewsLoaded, setViewsLoaded] = useState(false);
  const [lastResponse, setLastResponse] = useState<ViewAssetsResponse | null>(null);
  const [lastParams, setLastParams] = useState<{
    offset: number;
    limit: number;
    sort?: [string, "asc" | "desc"][];
    filters?: string[];
    search?: string;
    searchMode?: "fts" | "semantic" | "hybrid";
    searchMinScore?: number;
    searchIncludeMatches?: boolean;
  } | null>(null);
  const [saving, setSaving] = useState(false);
  const [selectedAssetIds, setSelectedAssetIds] = useState<Set<number>>(new Set());
  const [resultType, setResultType] = useState<"assets" | "metadata">("assets");
  const [collectionMenuOpen, setCollectionMenuOpen] = useState(false);
  const [workflowMenuOpen, setWorkflowMenuOpen] = useState(false);
  const collectionMenuRef = useRef<HTMLDivElement | null>(null);
  const workflowMenuRef = useRef<HTMLDivElement | null>(null);
  const { startTracking } = useChangesetProgress();

  const fetchPage = useCallback(
    ({
      offset,
      limit,
      sort,
      filters,
      search,
      searchMode,
      searchMinScore,
      searchIncludeMatches,
    }: {
      offset: number;
      limit: number;
      sort?: [string, "asc" | "desc"][];
      filters?: string[];
      search?: string;
      searchMode?: "fts" | "semantic" | "hybrid";
      searchMinScore?: number;
      searchIncludeMatches?: boolean;
    }) =>
      fetchAssets(selectedViewId, {
        offset,
        limit,
        sort,
        filters,
        search,
        searchMode,
        searchMinScore,
        searchIncludeMatches,
      }),
    [selectedViewId],
  );

  const handleRowClick = useCallback(
    (assetId: number) => {
      navigate(`/assets/${assetId}`);
    },
    [navigate],
  );

  const fetchMetadataPage = useCallback(
    ({
      offset,
      limit,
      filters,
      search,
      searchMode,
      searchMinScore,
    }: {
      offset: number;
      limit: number;
      filters?: string[];
      search?: string;
      searchMode?: "fts" | "semantic" | "hybrid";
      searchMinScore?: number;
    }) =>
      fetchMetadataSearch({
        offset,
        limit,
        filters,
        search,
        searchMode,
        searchMinScore,
      }),
    [],
  );

  const handleLoadComplete = useCallback(
    ({ response, params }: { response: ViewAssetsResponse; params: typeof lastParams }) => {
      setLastResponse(response);
      setLastParams(params);
    },
    [],
  );

  const handleSaveQueryAsCollection = useCallback(async () => {
    const totalCount = lastResponse?.stats?.total ?? lastResponse?.items?.length ?? 0;
    if (totalCount === 0) {
      window.alert("Nothing to save. Run a query first.");
      return;
    }
    const saveAllAllowed = resultType === "assets";
    if (!saveAllAllowed) {
      window.alert("Metadata result mode supports saving selected assets only.");
      return;
    }
    const saveCount = totalCount;
    const defaultName = `Collection ${new Date().toISOString().slice(0, 19)}`;
    if (
      saveCount > 1000 &&
      !window.confirm(`Are you sure you want to save a new collection with ${saveCount} assets?`)
    ) {
      return;
    }
    const name = window.prompt("Name for the new collection", defaultName);
    if (!name) {
      return;
    }

    setSaving(true);
    try {
      const queryParams = {
        sort: lastParams?.sort,
        filters: lastParams?.filters,
        search: lastParams?.search,
        search_mode: lastParams?.searchMode,
        search_min_score: lastParams?.searchMinScore,
        search_include_matches: lastParams?.searchIncludeMatches,
      };
      const source = {
        query: {
          view_id: selectedViewId,
          ...queryParams,
        },
      };
      const response = await createCollection({
        name,
        asset_ids: [],
        source,
      });
      navigate(`/collections/${response.collection.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to save collection: ${message}`);
    } finally {
      setSaving(false);
    }
  }, [lastParams, lastResponse, navigate, resultType, selectedViewId]);

  const handleCreateCollectionFromSelected = useCallback(async () => {
    const selectedIds = Array.from(selectedAssetIds);
    if (selectedIds.length === 0) {
      return;
    }
    const defaultName = `Collection ${new Date().toISOString().slice(0, 19)}`;
    if (
      selectedIds.length > 1000 &&
      !window.confirm(
        `Are you sure you want to save a new collection with ${selectedIds.length.toLocaleString()} assets?`,
      )
    ) {
      return;
    }
    const name = window.prompt("Name for the new collection", defaultName);
    if (!name) {
      return;
    }

    setSaving(true);
    try {
      const response = await createCollection({
        name,
        asset_ids: selectedIds,
        source: null,
      });
      navigate(`/collections/${response.collection.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to save collection: ${message}`);
    } finally {
      setSaving(false);
    }
  }, [navigate, selectedAssetIds]);

  const handleAddToExistingCollection = useCallback(
    async (collectionId: number) => {
      const selectedIds = Array.from(selectedAssetIds);
      if (selectedIds.length === 0) {
        window.alert("Select one or more assets to add to an existing collection.");
        return;
      }

      if (
        selectedIds.length > 1000 &&
        !window.confirm(
          `Are you sure you want to add ${selectedIds.length.toLocaleString()} assets to this collection?`,
        )
      ) {
        return;
      }

      setSaving(true);
      try {
        const result = await addCollectionAssets(collectionId, { asset_ids: selectedIds });
        window.alert(
          `Added ${result.added} assets to collection.${result.skipped > 0 ? ` Skipped ${result.skipped} already-present assets.` : ""}`,
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        window.alert(`Failed to add assets to collection: ${message}`);
      } finally {
        setSaving(false);
      }
    },
    [selectedAssetIds],
  );

  const handleCollectionMenuPick = useCallback(
    async (collectionId: number | null) => {
      setCollectionMenuOpen(false);
      if (collectionId === null) {
        await handleCreateCollectionFromSelected();
        return;
      }
      await handleAddToExistingCollection(collectionId);
    },
    [handleAddToExistingCollection, handleCreateCollectionFromSelected],
  );

  const loadViews = useCallback(async () => {
    setViewsLoaded(false);
    try {
      const [viewsResponse, collectionsResponse] = await Promise.all([
        fetchViews(),
        fetchCollections(),
      ]);
      const all = viewsResponse.views ?? [];
      setViews(all);
      setCollections(collectionsResponse.collections ?? []);
      setSelectedViewId((current) =>
        all.some((view) => view.id === current) ? current : DEFAULT_VIEW_ID,
      );
    } catch {
      setViews([]);
      setCollections([]);
      setSelectedViewId(DEFAULT_VIEW_ID);
    } finally {
      setViewsLoaded(true);
    }
  }, []);

  const loadWorkflows = useCallback(async () => {
    try {
      const response = await fetchWorkflows();
      setWorkflows((response.workflows ?? []).filter((workflow) => workflow.status === "ready"));
    } catch {
      setWorkflows([]);
    }
  }, []);

  useEffect(() => {
    void loadViews();
    void loadWorkflows();
  }, [loadViews, loadWorkflows]);

  const handleRunWorkflowForSelection = useCallback(async (workflowName: string) => {
    const selectedIds = Array.from(selectedAssetIds);
    if (selectedIds.length === 0) {
      window.alert("Select one or more assets first.");
      return;
    }
    if (workflows.length === 0) {
      window.alert("No ready workflows available.");
      return;
    }
    const selectedWorkflow = workflows.find((workflow) => workflow.file_name === workflowName);
    if (!selectedWorkflow) {
      window.alert("Unknown workflow name.");
      return;
    }
    try {
      setWorkflowMenuOpen(false);
      const response = await startWorkflow(workflowName, {
        input: {
          kind: "asset_ids",
          asset_ids: selectedIds,
        },
      });
      if (response.changeset) {
        startTracking(response.changeset);
        navigate(`/changesets/${response.changeset.id}`);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to run workflow: ${message}`);
    }
  }, [navigate, selectedAssetIds, startTracking, workflows]);

  useEffect(() => {
    if (!collectionMenuOpen && !workflowMenuOpen) {
      return;
    }
    const handlePointerDown = (event: MouseEvent) => {
      const collectionMenu = collectionMenuRef.current;
      const workflowMenu = workflowMenuRef.current;
      if (collectionMenu && !collectionMenu.contains(event.target as Node)) {
        setCollectionMenuOpen(false);
      }
      if (workflowMenu && !workflowMenu.contains(event.target as Node)) {
        setWorkflowMenuOpen(false);
      }
    };
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setCollectionMenuOpen(false);
        setWorkflowMenuOpen(false);
      }
    };
    window.addEventListener("mousedown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [collectionMenuOpen, workflowMenuOpen]);

  const viewOptions = useMemo(
    () =>
      views.length > 0
        ? views
        : [
            {
              id: DEFAULT_VIEW_ID,
              name: "Files",
              columns: [],
              default_sort: [],
              default_columns: null,
            } as ViewSpec,
          ],
    [views],
  );

  const selectedCount = selectedAssetIds.size;
  const saveQueryEnabled = selectedCount === 0 && resultType === "assets";
  const addSelectedEnabled = selectedCount > 0;
  const saveAllAllowed = resultType === "assets";
  const addLabel = `Add ${selectedCount.toLocaleString()} to collection`;

  useEffect(() => {
    if (!addSelectedEnabled) {
      setCollectionMenuOpen(false);
      setWorkflowMenuOpen(false);
    }
  }, [addSelectedEnabled]);

  return (
    <>
      <AppHeader>
        <div className="panel-actions">
          <label className="toggle-inline">
            <span style={{ marginRight: 8 }}>View</span>
            <select
              value={selectedViewId}
              onChange={(event) => setSelectedViewId(event.target.value)}
            >
              {viewOptions.map((view) => (
                <option key={view.id} value={view.id}>
                  {view.name}
                </option>
              ))}
            </select>
          </label>
          <button
            className="app-btn btn-save"
            type="button"
            onClick={() => void handleSaveQueryAsCollection()}
            disabled={saving || !saveQueryEnabled || !saveAllAllowed}
          >
            {saving ? "Saving…" : "Save query as collection"}
          </button>
          <div className="collection-dropdown" ref={collectionMenuRef}>
            <button
              className="app-btn btn-save collection-dropdown-trigger"
              type="button"
              onClick={() => setCollectionMenuOpen((open) => !open)}
              disabled={saving || !addSelectedEnabled}
            >
              {saving ? "Saving…" : addLabel}
            </button>
            {collectionMenuOpen && addSelectedEnabled ? (
              <div className="collection-dropdown-menu">
                <button
                  type="button"
                  className="app-btn btn-save collection-dropdown-item"
                  onClick={() => void handleCollectionMenuPick(null)}
                >
                  Create new
                </button>
                {collections.map((collection) => (
                  <button
                    key={collection.id}
                    type="button"
                    className="app-btn btn-save collection-dropdown-item"
                    onClick={() => void handleCollectionMenuPick(collection.id)}
                  >
                    {collection.name}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
          <div className="collection-dropdown" ref={workflowMenuRef}>
            <button
              className="app-btn btn-action collection-dropdown-trigger"
              type="button"
              onClick={() => setWorkflowMenuOpen((open) => !open)}
              disabled={saving || selectedCount === 0 || workflows.length === 0}
            >
              Run workflow on selected
            </button>
            {workflowMenuOpen ? (
              <div className="collection-dropdown-menu">
                {workflows.map((workflow) => (
                  <button
                    key={workflow.file_name}
                    type="button"
                    className="app-btn btn-action collection-dropdown-item"
                    onClick={() => void handleRunWorkflowForSelection(workflow.file_name)}
                  >
                    {workflow.name}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      </AppHeader>
      <main className="app-main app-main--locked">
        {viewsLoaded ? (
          <AssetTable
            key={`assets-view:${selectedViewId}`}
            title="Assets"
            fetchPage={fetchPage}
            fetchMetadataPage={fetchMetadataPage}
            onRowClick={handleRowClick}
            onLoadComplete={handleLoadComplete}
            onSelectionChange={setSelectedAssetIds}
            onResultTypeChange={setResultType}
            searchPlaceholder="Search all assets…"
          />
        ) : null}
      </main>
    </>
  );
}

export default AssetsRoute;
