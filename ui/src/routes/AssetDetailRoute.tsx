import { useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import {
  fetchAssetDetail,
  fetchEditableMetadataSchema,
  fetchChangeset,
  deleteChangeset,
  fetchChangesetChanges,
  startManualChangeset,
  finishChangeset as finishChangesetApi,
  createActor,
  fetchActors,
} from "../api/client";
import MetadataTable from "../components/MetadataTable";
import type { AssetDetailResponse, EditableMetadataSchemaResponse, Changeset } from "../types/api";
import { SimpleTable } from "simple-table-core";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

function AssetDetailRoute() {
  const { assetId } = useParams();
  const assetIdNum = assetId ? Number(assetId) : NaN;

  const [asset, setAsset] = useState<AssetDetailResponse | null>(null);
  const [schema, setSchema] = useState<EditableMetadataSchemaResponse | null>(null);
  const [formData, setFormData] = useState<Record<string, unknown>>({});
  const [activeChangeset, setActiveChangeset] = useState<Changeset | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const { startTracking, stopTracking } = useChangesetProgress();

  const load = useCallback(async () => {
    if (!assetIdNum || Number.isNaN(assetIdNum)) {
      setError("Invalid asset id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchAssetDetail(assetIdNum);
      setAsset(response);
      // Prefill form with current manual-actor values if present
      const latest: Record<string, unknown> = {};
      response.metadata.forEach((m) => {
        latest[m.key] = m.value;
      });
      setFormData(latest);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setAsset(null);
    } finally {
      setLoading(false);
    }
  }, [assetIdNum]);

  const loadSchema = useCallback(async () => {
    try {
      const res = await fetchEditableMetadataSchema();
      setSchema(res);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, []);

  useEffect(() => {
    void load();
    void loadSchema();
  }, [load]);

  const startManual = useCallback(async () => {
    setError(null);
    try {
      const snap = await startManualChangeset();
      setActiveChangeset(snap);
      startTracking(snap);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, []);

  const finishChangeset = useCallback(async () => {
    if (!activeChangeset) return;
    setError(null);
    try {
      const refreshed = await finishChangesetApi(activeChangeset.id);
      setActiveChangeset(refreshed.changeset);
      stopTracking(activeChangeset.id);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, [activeChangeset, stopTracking]);

  const discardChangeset = useCallback(async () => {
    if (!activeChangeset) return;
    setError(null);
    try {
      await deleteChangeset(activeChangeset.id);
      setActiveChangeset(null);
      stopTracking(activeChangeset.id);
      await load();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, [activeChangeset, load, stopTracking]);

  const canEdit = useMemo(() => Boolean(schema && activeChangeset), [schema, activeChangeset]);

  const handleSubmit = useCallback(
    async ({ formData: data }: { formData: Record<string, unknown> }) => {
      if (!activeChangeset) {
        setError("Start a manual edit changeset first");
        return;
      }
      if (!assetIdNum || Number.isNaN(assetIdNum)) {
        setError("Invalid asset id");
        return;
      }
      setSaving(true);
      setError(null);
      try {
        await fetch("/api/assets/" + assetIdNum + "/manual-edit", {
          method: "POST",
          headers: { Accept: "application/json", "Content-Type": "application/json" },
          body: JSON.stringify({ changeset_id: activeChangeset.id, metadata: data }),
        }).then((res) => {
          if (!res.ok) throw new Error(res.statusText);
        });
        await load();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
      } finally {
        setSaving(false);
      }
    },
    [activeChangeset, assetIdNum, load],
  );

  const assetLabel = asset?.asset?.external_id
    ? String(asset.asset.external_id)
    : assetId
      ? `Asset ${assetId}`
      : null;

  return (
    <>
      <AppHeader breadcrumbLabel={assetLabel}>
        <div className="button-row">
          {!activeChangeset && (
            <button
              className="btn-primary"
              type="button"
              onClick={() => void startManual()}
              disabled={loading}
            >
              Start editing
            </button>
          )}
          {activeChangeset && (
            <>
              <button type="button" onClick={() => void finishChangeset()} disabled={loading}>
                Finish changes
              </button>
              <button type="button" onClick={() => void discardChangeset()} disabled={loading}>
                Discard changeset
              </button>
            </>
          )}
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}

          {asset && (
            <div className="record-list">
              {/* {schema && (
                <div className="file-card">
                  <h3>Edit metadata</h3>
                  {!activeChangeset && <p className="note">Start editing to enable form.</p>}
                  <Form
                    schema={schema.schema as any}
                    uiSchema={schema.uiSchema as any}
                    formData={formData}
                    onChange={(evt) => setFormData(evt.formData as Record<string, unknown>)}
                    onSubmit={(evt) => void handleSubmit(evt)}
                    validator={validator}
                    disabled={!canEdit || saving}
                  >
                    <div className="button-row">
                      <button type="submit" disabled={!canEdit || saving}>
                        {saving ? "Saving..." : "Save changes"}
                      </button>
                    </div>
                  </Form>
                </div>
              )} */}
              <div className="file-card" style={{ width: "100%" }}>
                <h3>Metadata</h3>
                <MetadataTable metadata={asset.metadata} initialView="flat" />
              </div>
            </div>
          )}

          {!asset && !loading && !error && <div className="empty-state">Asset not found.</div>}
        </section>
      </main>
    </>
  );
}

export default AssetDetailRoute;
