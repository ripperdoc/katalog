import type {
  ViewAssetsResponse,
  MetadataSearchResponse,
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
  DeleteCollectionResponse,
  CollectionAddAssetsResponse,
  CollectionRemoveAssetsResponse,
  WorkflowActionResponse,
  WorkflowInputPayload,
  WorkflowListResponse,
  WorkspaceStatsResponse,
} from "../types/api";

const rawBase = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
const API_BASE = rawBase && rawBase.length > 0 ? rawBase : "/api";

type AssetDetailProjectedRow = Record<string, unknown>;

async function handleResponse(response: Response) {
  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Request failed (${response.status}): ${payload}`);
  }
  return response.json();
}

function toNumberOr(value: unknown, fallback: number): number {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : fallback;
}

function toStringOr(value: unknown, fallback: string): string {
  if (typeof value === "string") {
    return value;
  }
  if (value === null || value === undefined) {
    return fallback;
  }
  return String(value);
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeAssetDetail(payload: unknown, fallbackAssetId: number): AssetDetailResponse {
  if (isObjectRecord(payload) && Array.isArray(payload.metadata) && isObjectRecord(payload.asset)) {
    const legacy = payload as { asset: Record<string, unknown>; metadata: unknown[] };
    return {
      asset: {
        id: toNumberOr(legacy.asset.id, fallbackAssetId),
        actor_id: toNumberOr(legacy.asset.actor_id, 0),
        external_id: toStringOr(legacy.asset.external_id, String(fallbackAssetId)),
        canonical_uri: toStringOr(legacy.asset.canonical_uri, ""),
      },
      metadata: legacy.metadata
        .filter(isObjectRecord)
        .map((entry: Record<string, unknown>, index: number) => ({
          id: toNumberOr(entry.id, index),
          asset_id: toNumberOr(entry.asset_id, fallbackAssetId),
          actor_id: toNumberOr(entry.actor_id, 0),
          changeset_id: toNumberOr(entry.changeset_id, 0),
          metadata_key_id: toNumberOr(entry.metadata_key_id, 0),
          key: toStringOr(entry.key, ""),
          value_type: toStringOr(entry.value_type, "STRING"),
          value: (entry.value ?? null) as AssetDetailResponse["metadata"][number]["value"],
          removed: Boolean(entry.removed),
          confidence: typeof entry.confidence === "number" ? entry.confidence : null,
        })),
    };
  }

  const row = isObjectRecord(payload) ? (payload as AssetDetailProjectedRow) : {};
  const assetId = toNumberOr(row["asset/id"], fallbackAssetId);
  const metadataRows: AssetDetailResponse["metadata"] = [];

  Object.entries(row).forEach(([key, value]) => {
    if (key.startsWith("asset/") || !Array.isArray(value)) {
      return;
    }
    value.forEach((entry, index) => {
      if (!isObjectRecord(entry)) {
        return;
      }
      metadataRows.push({
        id: toNumberOr(entry.id, index),
        asset_id: toNumberOr(entry.asset_id, assetId),
        actor_id: toNumberOr(entry.actor_id, 0),
        changeset_id: toNumberOr(entry.changeset_id, 0),
        metadata_key_id: toNumberOr(entry.metadata_key_id, 0),
        key: toStringOr(entry.key, key),
        value_type: toStringOr(entry.value_type, "STRING"),
        value: (entry.value ?? null) as AssetDetailResponse["metadata"][number]["value"],
        removed: Boolean(entry.removed),
        confidence: typeof entry.confidence === "number" ? entry.confidence : null,
      });
    });
  });

  metadataRows.sort((a, b) => {
    if (a.changeset_id !== b.changeset_id) {
      return b.changeset_id - a.changeset_id;
    }
    return b.id - a.id;
  });

  return {
    asset: {
      id: assetId,
      actor_id: toNumberOr(row["asset/actor_id"], 0),
      external_id: toStringOr(row["asset/external_id"], String(assetId)),
      canonical_uri: toStringOr(row["asset/canonical_uri"], ""),
    },
    metadata: metadataRows,
  };
}

export async function fetchAssets(
  viewId: string,
  {
    offset = 0,
    limit = 100,
    sort,
    filters,
    search,
    searchMode,
    searchMinScore,
    searchIncludeMatches,
  }: {
    offset?: number;
    limit?: number;
    sort?: [string, "asc" | "desc"][] | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
    searchMode?: "fts" | "semantic" | "hybrid" | undefined;
    searchMinScore?: number | undefined;
    searchIncludeMatches?: boolean | undefined;
  },
): Promise<ViewAssetsResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  if (sort && sort.length > 0) {
    sort.forEach(([key, direction]) => {
      params.append("sort", `${key}:${direction}`);
    });
  }
  if (filters && filters.length > 0) {
    filters.forEach((f) => params.append("filters", f));
  }
  if (search) {
    params.set("search", search);
  }
  if (searchMode) {
    params.set("search_mode", searchMode);
  }
  if (searchMinScore !== undefined) {
    params.set("search_min_score", String(searchMinScore));
  }
  if (searchIncludeMatches) {
    params.set("search_include_matches", "true");
  }
  params.set("view_id", viewId);
  const response = await fetch(`${API_BASE}/assets?${params}`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function fetchMetadataSearch({
  offset = 0,
  limit = 100,
  filters,
  search,
  searchMode,
  searchMinScore,
}: {
  offset?: number;
  limit?: number;
  filters?: string[] | undefined;
  search?: string | undefined;
  searchMode?: "fts" | "semantic" | "hybrid" | undefined;
  searchMinScore?: number | undefined;
}): Promise<MetadataSearchResponse> {
  const payload: Record<string, unknown> = {
    view_id: "default",
    search_granularity: "metadata",
    offset,
    limit,
  };
  if (filters && filters.length > 0) {
    payload.filters = filters;
  }
  if (search) {
    payload.search = search;
  }
  if (searchMode) {
    payload.search_mode = searchMode;
  }
  if (searchMinScore !== undefined) {
    payload.search_min_score = searchMinScore;
  }

  const response = await fetch(`${API_BASE}/metadata/search`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
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
  identity_key?: string;
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
    identity_key?: string;
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
    view = "raw",
    offset = 0,
    limit = 200,
    fromChangesetId,
    toChangesetId,
    sort,
    filters,
    search,
  }: {
    view?: "raw" | "diff";
    offset?: number;
    limit?: number;
    fromChangesetId?: number;
    toChangesetId?: number;
    sort?: string[] | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
  } = {},
): Promise<ChangesetChangesResponse> {
  const params = new URLSearchParams();
  params.set("view", view);
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  if (fromChangesetId !== undefined) {
    params.set("from_changeset_id", String(fromChangesetId));
  }
  if (toChangesetId !== undefined) {
    params.set("to_changeset_id", String(toChangesetId));
  }
  if (sort && sort.length > 0) {
    sort.forEach((value) => params.append("sort", value));
  }
  if (filters && filters.length > 0) {
    filters.forEach((value) => params.append("filters", value));
  }
  if (search && search.length > 0) {
    params.set("search", search);
  }
  const response = await fetch(
    `${API_BASE}/changesets/${encodeURIComponent(changesetId)}/changes?${params.toString()}`,
    {
      headers: { Accept: "application/json" },
    },
  );
  return handleResponse(response);
}

export async function fetchAssetDetail(assetId: number): Promise<AssetDetailResponse> {
  const params = new URLSearchParams();
  params.set("metadata_aggregation", "object");
  const response = await fetch(`${API_BASE}/assets/${assetId}?${params.toString()}`, {
    headers: { Accept: "application/json" },
  });
  const payload = await handleResponse(response);
  return normalizeAssetDetail(payload, assetId);
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
    sort?: [string, "asc" | "desc"][] | undefined;
    filters?: string[] | undefined;
    search?: string | undefined;
  },
): Promise<ViewAssetsResponse> {
  const params = new URLSearchParams();
  params.set("offset", String(offset));
  params.set("limit", String(limit));
  params.set("view_id", viewId);
  if (sort && sort.length > 0) {
    sort.forEach(([key, direction]) => {
      params.append("sort", `${key}:${direction}`);
    });
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

export async function removeCollectionAssets(
  collectionId: number,
  payload: {
    asset_ids: number[];
    changeset_id: number;
  },
): Promise<CollectionRemoveAssetsResponse> {
  const response = await fetch(`${API_BASE}/collections/${collectionId}/remove`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function addCollectionAssets(
  collectionId: number,
  payload: {
    asset_ids: number[];
  },
): Promise<CollectionAddAssetsResponse> {
  const response = await fetch(`${API_BASE}/collections/${collectionId}/add`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return handleResponse(response);
}

export async function deleteCollection(id: number): Promise<DeleteCollectionResponse> {
  const response = await fetch(`${API_BASE}/collections/${id}`, {
    method: "DELETE",
    headers: { Accept: "application/json" },
  });
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

export async function runAnalyzer(
  actorId: number,
  {
    assetId,
    collectionId,
  }: {
    assetId?: number;
    collectionId?: number;
  } = {},
): Promise<Changeset> {
  const params = new URLSearchParams();
  if (assetId !== undefined) {
    params.set("asset_id", String(assetId));
  }
  if (collectionId !== undefined) {
    params.set("collection_id", String(collectionId));
  }
  const suffix = params.toString();
  const response = await fetch(
    `${API_BASE}/analyzers/${encodeURIComponent(actorId)}/run${suffix ? `?${suffix}` : ""}`,
    {
      method: "POST",
      headers: { Accept: "application/json" },
    },
  );
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

export async function updateChangesetMessage(
  changesetId: number,
  message: string,
): Promise<ChangesetResponse> {
  const response = await fetch(`${API_BASE}/changesets/${changesetId}`, {
    method: "PATCH",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message }),
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

export async function fetchWorkflows(): Promise<WorkflowListResponse> {
  const response = await fetch(`${API_BASE}/workflows`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}

export async function startWorkflow(
  workflowName: string,
  payload?: {
    always_process?: boolean;
    input?: WorkflowInputPayload;
  },
): Promise<WorkflowActionResponse> {
  const response = await fetch(`${API_BASE}/workflows/${encodeURIComponent(workflowName)}/start`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload ?? {}),
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

export async function fetchWorkspaceStats(): Promise<WorkspaceStatsResponse> {
  const response = await fetch(`${API_BASE}/stats`, {
    headers: { Accept: "application/json" },
  });
  return handleResponse(response);
}
