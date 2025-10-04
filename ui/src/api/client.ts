import type { FileRecordResponse, ViewMode } from "../types/api";

const rawBase = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
const API_BASE = rawBase && rawBase.length > 0 ? rawBase : "/api";

async function handleResponse(response: Response) {
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }
  return response.json();
}

export async function fetchFilesBySource(
  sourceId: string,
  view: ViewMode
): Promise<FileRecordResponse> {
  if (!sourceId) {
    throw new Error("source id is required");
  }
  const url = `${API_BASE}/files/${encodeURIComponent(sourceId)}?view=${view}`;
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchRecords(view: ViewMode = "flat"): Promise<FileRecordResponse> {
  const url = `${API_BASE}/records?view=${view}`;
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}
