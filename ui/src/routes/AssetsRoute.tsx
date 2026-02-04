import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import AssetTable from "../components/AssetTable";
import AppHeader from "../components/AppHeader";
import { createCollection, fetchViewAssets } from "../api/client";
import type { ViewAssetsResponse } from "../types/api";

const DEFAULT_VIEW_ID = "default";

function AssetsRoute() {
  const navigate = useNavigate();
  const [lastResponse, setLastResponse] = useState<ViewAssetsResponse | null>(null);
  const [lastParams, setLastParams] = useState<{
    offset: number;
    limit: number;
    sort?: [string, "asc" | "desc"][];
    filters?: string[];
    search?: string;
  } | null>(null);
  const [saving, setSaving] = useState(false);
  const [selectedAssetIds, setSelectedAssetIds] = useState<Set<number>>(new Set());

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
    }) =>
      fetchViewAssets(DEFAULT_VIEW_ID, {
        offset,
        limit,
        sort,
        filters,
        search,
      }),
    [],
  );

  const handleRowClick = useCallback(
    (assetId: number) => {
      navigate(`/assets/${assetId}`);
    },
    [navigate],
  );

  const handleLoadComplete = useCallback(
    ({ response, params }: { response: ViewAssetsResponse; params: typeof lastParams }) => {
      setLastResponse(response);
      setLastParams(params);
    },
    [],
  );

  const handleSaveCollection = useCallback(async () => {
    const totalCount = lastResponse?.stats?.total ?? lastResponse?.items?.length ?? 0;
    const selectedIds = Array.from(selectedAssetIds);
    if (totalCount === 0 && selectedIds.length === 0) {
      window.alert("Nothing to save. Run a query first.");
      return;
    }
    const savingSelected = selectedIds.length > 0;
    const saveCount = savingSelected ? selectedIds.length : totalCount;
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
      if (savingSelected) {
        const response = await createCollection({
          name,
          asset_ids: selectedIds,
          source: null,
        });
        navigate(`/collections/${response.collection.id}`);
        return;
      }

      const queryParams = {
        sort: lastParams?.sort,
        filters: lastParams?.filters,
        search: lastParams?.search,
      };
      const source = {
        query: {
          view_id: DEFAULT_VIEW_ID,
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
  }, [lastParams, lastResponse, navigate, selectedAssetIds]);

  const selectedCount = selectedAssetIds.size;
  const saveLabel =
    selectedCount > 0
      ? `Save ${selectedCount.toLocaleString()} to collection`
      : "Save all to collection";

  return (
    <>
      <AppHeader />
      <main className="app-main app-main--locked">
        <AssetTable
          title="Assets"
          fetchPage={fetchPage}
          onRowClick={handleRowClick}
          onLoadComplete={handleLoadComplete}
          onSelectionChange={setSelectedAssetIds}
          searchPlaceholder="Search all assets…"
          actions={
            <button
              className="btn-primary"
              type="button"
              onClick={() => void handleSaveCollection()}
              disabled={saving}
            >
              {saving ? "Saving…" : saveLabel}
            </button>
          }
        />
      </main>
    </>
  );
}

export default AssetsRoute;
