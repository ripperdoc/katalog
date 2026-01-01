import type {
  AssetResponse,
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

export async function fetchAssets(providerId?: number): Promise<AssetResponse> {
  const search = providerId !== undefined ? `?provider_id=${encodeURIComponent(providerId)}` : "";
  const url = `${API_BASE}/assets${search}`;
  const response = await fetch(url, {
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
