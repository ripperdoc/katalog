import type {
  ViewAssetsResponse,
  ViewListResponse,
  ViewResponse,
  ProviderListResponse,
  PluginListResponse,
  ProviderResponse,
  ProviderCreateResponse,
  ProviderUpdateResponse,
  Snapshot,
  SnapshotListResponse,
  SnapshotResponse,
  DeleteSnapshotResponse,
  SnapshotChangesResponse,
  AssetDetailResponse,
  AssetCollection,
  CollectionListResponse,
  CollectionResponse,
  CollectionUpdateResponse,
} from "../types/api";

const rawBase = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
const API_BASE = rawBase && rawBase.length > 0 ? rawBase : "/api";

async function handleResponse(response: Response) {
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }
  return response.json();
}

export async function fetchViewAssets(
  viewId: string,
  {
    providerId,
    offset = 0,
    limit = 100,
    sort,
    filters,
    search,
  }: {
    providerId?: number;
    offset?: number;
    limit?: number;
    sort?: string | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
  }
): Promise<ViewAssetsResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  if (providerId !== undefined) {
    params.set("provider_id", String(providerId));
  }
  if (sort) {
    params.set("sort", sort);
  }
  if (filters && filters.length > 0) {
    filters.forEach((f) => params.append("filters", f));
  }
  if (search) {
    params.set("search", search);
  }
  const response = await fetch(`${API_BASE}/views/${encodeURIComponent(viewId)}/assets?${params}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchViews(): Promise<ViewListResponse> {
  const response = await fetch(`${API_BASE}/views`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchView(viewId: string): Promise<ViewResponse> {
  const response = await fetch(`${API_BASE}/views/${encodeURIComponent(viewId)}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchProviders(): Promise<ProviderListResponse> {
  const response = await fetch(`${API_BASE}/providers`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchPlugins(): Promise<PluginListResponse> {
  const response = await fetch(`${API_BASE}/plugins`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchProvider(id: number): Promise<ProviderResponse> {
  const response = await fetch(`${API_BASE}/providers/${id}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchProviderConfigSchema(
  id: number
): Promise<{ schema: Record<string, unknown>; value: Record<string, unknown> }> {
  const response = await fetch(`${API_BASE}/providers/${id}/config/schema`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchPluginConfigSchema(
  pluginId: string
): Promise<{ schema: Record<string, unknown> }> {
  const response = await fetch(`${API_BASE}/plugins/${encodeURIComponent(pluginId)}/config/schema`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchSnapshots(): Promise<SnapshotListResponse> {
  const response = await fetch(`${API_BASE}/snapshots`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function createProvider(payload: {
  name: string;
  plugin_id: string;
  config?: Record<string, unknown> | null;
  config_toml?: string;
}): Promise<ProviderCreateResponse> {
  const response = await fetch(`${API_BASE}/providers`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function updateProvider(
  id: number,
  payload: {
    name?: string;
    config?: Record<string, unknown> | null;
    config_toml?: string;
  }
): Promise<ProviderUpdateResponse> {
  const response = await fetch(`${API_BASE}/providers/${id}`, {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function fetchSnapshot(id: number): Promise<SnapshotResponse> {
  const response = await fetch(`${API_BASE}/snapshots/${id}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchSnapshotChanges(
  snapshotId: number,
  {
    offset = 0,
    limit = 200,
  }: {
    offset?: number;
    limit?: number;
  } = {}
): Promise<SnapshotChangesResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  const response = await fetch(
    `${API_BASE}/snapshots/${encodeURIComponent(snapshotId)}/changes?${params.toString()}`,
    {
      headers: { Accept: "application/json" },
    }
  );
  return handleResponse(response);
}

export async function fetchAssetDetail(assetId: number): Promise<AssetDetailResponse> {
  const response = await fetch(`${API_BASE}/assets/${assetId}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchCollections(): Promise<CollectionListResponse> {
  const response = await fetch(`${API_BASE}/collections`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchCollection(
  id: number
): Promise<CollectionResponse> {
  const response = await fetch(`${API_BASE}/collections/${id}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function updateCollection(
  id: number,
  payload: {
    name?: string;
    description?: string | null;
    refresh_mode?: string;
  }
): Promise<CollectionUpdateResponse> {
  const response = await fetch(`${API_BASE}/collections/${id}`, {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function createCollection(payload: {
  name: string;
  description?: string | null;
  asset_ids: number[];
  source?: Record<string, unknown> | null;
  refresh_mode?: string;
}): Promise<{ collection: AssetCollection }> {
  const response = await fetch(`${API_BASE}/collections`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function fetchCollectionAssets(
  collectionId: number,
  {
    viewId = "default",
    offset = 0,
    limit = 100,
    sort,
    filters,
    search,
  }: {
    viewId?: string;
    offset?: number;
    limit?: number;
    sort?: string | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
  }
): Promise<ViewAssetsResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  params.set("view_id", viewId);
  if (sort) {
    params.set("sort", sort);
  }
  if (filters && filters.length > 0) {
    filters.forEach((f) => params.append("filters", f));
  }
  if (search) {
    params.set("search", search);
  }
  const response = await fetch(
    `${API_BASE}/collections/${encodeURIComponent(collectionId)}/assets?${params}`,
    {
      headers: { Accept: "application/json" },
    }
  );
  return handleResponse(response);
}

export async function startScan(providerId?: number): Promise<Snapshot> {
  const search = providerId !== undefined ? `?ids=${encodeURIComponent(providerId)}` : "";
  const response = await fetch(`${API_BASE}/sources/run${search}`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function cancelSnapshot(snapshotId: number): Promise<void> {
  const response = await fetch(`${API_BASE}/snapshots/${snapshotId}/cancel`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  await handleResponse(response);
}

export async function deleteSnapshot(snapshotId: number): Promise<DeleteSnapshotResponse> {
  const response = await fetch(`${API_BASE}/snapshots/${snapshotId}`, {
    method: "DELETE",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function runAllProcessors(): Promise<Snapshot> {
  const response = await fetch(`${API_BASE}/processors/run`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function runAllAnalyzers(): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/analyzers/all/run`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function syncConfig(): Promise<void> {
  const response = await fetch(`${API_BASE}/sync`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  await handleResponse(response);
}

export function snapshotEventsUrl(snapshotId: number): string {
  return `${API_BASE}/snapshots/${snapshotId}/events`;
}
