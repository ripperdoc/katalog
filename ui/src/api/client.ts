import type {
  ViewAssetsResponse,
  ViewListResponse,
  ViewResponse,
  ProviderListResponse,
  ProviderResponse,
  Snapshot,
  SnapshotListResponse,
  SnapshotResponse,
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
  }: { providerId?: number; offset?: number; limit?: number; sort?: string | undefined }
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

export async function fetchProvider(id: number): Promise<ProviderResponse> {
  const response = await fetch(`${API_BASE}/providers/${id}`, {
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

export async function fetchSnapshot(id: number): Promise<SnapshotResponse> {
  const response = await fetch(`${API_BASE}/snapshots/${id}`, {
    headers: { Accept: "application/json" },
  });
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
