import type {
  ViewAssetsResponse,
  ViewListResponse,
  ViewResponse,
  ActorListResponse,
  PluginListResponse,
  ActorResponse,
  ActorCreateResponse,
  ActorUpdateResponse,
  MetadataRegistryResponse,
  Changeset,
  ChangesetListResponse,
  ChangesetResponse,
  DeleteChangesetResponse,
  CancelChangesetResponse,
  ChangesetChangesResponse,
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
    actorId,
    offset = 0,
    limit = 100,
    sort,
    filters,
    search,
  }: {
    actorId?: number;
    offset?: number;
    limit?: number;
    sort?: string | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
  },
): Promise<ViewAssetsResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  if (actorId !== undefined) {
    params.set("actor_id", String(actorId));
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

export async function fetchActors(): Promise<ActorListResponse> {
  const response = await fetch(`${API_BASE}/actors`, {
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

export async function fetchMetadataRegistry(): Promise<MetadataRegistryResponse> {
  const response = await fetch(`${API_BASE}/metadata/registry`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchActor(id: number): Promise<ActorResponse> {
  const response = await fetch(`${API_BASE}/actors/${id}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchActorConfigSchema(
  id: number,
): Promise<{ schema: Record<string, unknown>; value: Record<string, unknown> }> {
  const response = await fetch(`${API_BASE}/actors/${id}/config/schema`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchPluginConfigSchema(
  pluginId: string,
): Promise<{ schema: Record<string, unknown> }> {
  const response = await fetch(
    `${API_BASE}/plugins/${encodeURIComponent(pluginId)}/config/schema`,
    {
      headers: { Accept: "application/json" },
    },
  );
  return handleResponse(response);
}

export async function fetchChangesets(): Promise<ChangesetListResponse> {
  const response = await fetch(`${API_BASE}/changesets`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchActiveChangesets(): Promise<ChangesetListResponse> {
  // Simple client-side filter for now.
  const all = await fetchChangesets();
  return {
    changesets: (all.changesets ?? []).filter((c) => c.status === "in_progress"),
  };
}

export async function createActor(payload: {
  name: string;
  plugin_id: string;
  config?: Record<string, unknown> | null;
  config_toml?: string;
  disabled?: boolean;
}): Promise<ActorCreateResponse> {
  const response = await fetch(`${API_BASE}/actors`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function updateActor(
  id: number,
  payload: {
    name?: string;
    config?: Record<string, unknown> | null;
    config_toml?: string;
    disabled?: boolean;
  },
): Promise<ActorUpdateResponse> {
  const response = await fetch(`${API_BASE}/actors/${id}`, {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function fetchChangeset(id: number): Promise<ChangesetResponse> {
  const response = await fetch(`${API_BASE}/changesets/${id}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchChangesetChanges(
  changesetId: number,
  {
    offset = 0,
    limit = 200,
  }: {
    offset?: number;
    limit?: number;
  } = {},
): Promise<ChangesetChangesResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  const response = await fetch(
    `${API_BASE}/changesets/${encodeURIComponent(changesetId)}/changes?${params.toString()}`,
    {
      headers: { Accept: "application/json" },
    },
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

export async function fetchCollection(id: number): Promise<CollectionResponse> {
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
  },
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
  },
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
    },
  );
  return handleResponse(response);
}

export async function runSources(actorId: number): Promise<Changeset> {
  const response = await fetch(`${API_BASE}/sources/${encodeURIComponent(actorId)}/run`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function runProcessors(): Promise<Changeset> {
  const response = await fetch(`${API_BASE}/processors/run`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function runProcessor(actorId: number): Promise<Changeset> {
  const response = await fetch(
    `${API_BASE}/processors/run?processor_ids=${encodeURIComponent(actorId)}`,
    {
      method: "POST",
      headers: { Accept: "application/json" },
    },
  );
  return handleResponse(response);
}

export async function runAnalyzer(actorId: number): Promise<Record<string, unknown>> {
  const response = await fetch(`${API_BASE}/analyzers/${encodeURIComponent(actorId)}/run`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function cancelChangeset(changesetId: number): Promise<CancelChangesetResponse> {
  const response = await fetch(`${API_BASE}/changesets/${changesetId}/cancel`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function deleteChangeset(changesetId: number): Promise<DeleteChangesetResponse> {
  const response = await fetch(`${API_BASE}/changesets/${changesetId}`, {
    method: "DELETE",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function startManualChangeset(): Promise<Changeset> {
  const response = await fetch(`${API_BASE}/changesets`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function finishChangeset(changesetId: number): Promise<ChangesetResponse> {
  const response = await fetch(`${API_BASE}/changesets/${changesetId}/finish`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchEditableMetadataSchema(): Promise<{
  schema: Record<string, unknown>;
  uiSchema: Record<string, unknown>;
}> {
  const response = await fetch(`${API_BASE}/metadata/schema/editable`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function runAllProcessors(): Promise<Changeset> {
  return runProcessors();
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

export function changesetEventsUrl(changesetId: number): string {
  return `${API_BASE}/changesets/${changesetId}/events`;
}
