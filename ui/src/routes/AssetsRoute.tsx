import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import DataTable from "../components/DataTable";
import { createCollection, fetchViewAssets } from "../api/client";
import type { ViewAssetsResponse } from "../types/api";

const DEFAULT_VIEW_ID = "default";

function AssetsRoute() {
  const navigate = useNavigate();
  const [lastResponse, setLastResponse] = useState<ViewAssetsResponse | null>(null);
  const [lastParams, setLastParams] = useState<{
    offset: number;
    limit: number;
    sort?: string;
    filters?: string[];
    search?: string;
  } | null>(null);
  const [saving, setSaving] = useState(false);

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
    }) =>
      fetchViewAssets(DEFAULT_VIEW_ID, {
        offset,
        limit,
        sort,
        filters,
        search,
      }),
    []
  );

  const handleRowClick = useCallback(
    (assetId: number) => {
      navigate(`/assets/${assetId}`);
    },
    [navigate]
  );

  const handleLoadComplete = useCallback(
    ({ response, params }: { response: ViewAssetsResponse; params: typeof lastParams }) => {
      setLastResponse(response);
      setLastParams(params);
    },
    []
  );

  const handleSaveCollection = useCallback(async () => {
    const items = lastResponse?.items ?? [];
    if (items.length === 0) {
      window.alert("Nothing to save. Run a query first.");
      return;
    }
    const assetIds = items
      .map((row) => {
        const raw = (row as Record<string, unknown>)["asset/id"];
        const numeric = typeof raw === "number" ? raw : Number(raw);
        return Number.isFinite(numeric) ? Number(numeric) : null;
      })
      .filter((id): id is number => id !== null);
    if (assetIds.length === 0) {
      window.alert("No asset ids found in the current result.");
      return;
    }
    const defaultName = `Collection ${new Date().toISOString().slice(0, 19)}`;
    if (
      assetIds.length > 1000 &&
      !window.confirm(
        `Are you sure you want to save a new collection with ${assetIds.length} assets?`
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
      const source = {
        query: {
          view_id: DEFAULT_VIEW_ID,
          sort: lastParams?.sort,
          filters: lastParams?.filters,
          search: lastParams?.search,
        },
      };
      const response = await createCollection({
        name,
        asset_ids: assetIds,
        source,
      });
      navigate(`/collections/${response.collection.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      window.alert(`Failed to save collection: ${message}`);
    } finally {
      setSaving(false);
    }
  }, [lastParams, lastResponse, navigate]);

  return (
    <DataTable
      title="Assets"
      subtitle={`Displaying view “${DEFAULT_VIEW_ID}”`}
      fetchPage={fetchPage}
      onRowClick={handleRowClick}
      onLoadComplete={handleLoadComplete}
      searchPlaceholder="Search records…"
      actions={
        <button type="button" onClick={() => void handleSaveCollection()} disabled={saving}>
          {saving ? "Saving…" : "Save as collection"}
        </button>
      }
    />
  );
}

export default AssetsRoute;
