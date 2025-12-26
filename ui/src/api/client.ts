import type { AssetResponse } from "../types/api";

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
