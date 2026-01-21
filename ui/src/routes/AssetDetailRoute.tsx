import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import {
  fetchAssetDetail,
  fetchEditableMetadataSchema,
  fetchSnapshot,
  deleteSnapshot,
  fetchSnapshotChanges,
  startManualSnapshot,
  finishSnapshot as finishSnapshotApi,
  createProvider,
  fetchProviders,
} from "../api/client";
import MetadataTable from "../components/MetadataTable";
import type { AssetDetailResponse, EditableMetadataSchemaResponse, Snapshot } from "../types/api";
import { SimpleTable } from "simple-table-core";

function AssetDetailRoute() {
  const { assetId } = useParams();
  const assetIdNum = assetId ? Number(assetId) : NaN;

  const [asset, setAsset] = useState<AssetDetailResponse | null>(null);
  const [schema, setSchema] = useState<EditableMetadataSchemaResponse | null>(null);
  const [formData, setFormData] = useState<Record<string, unknown>>({});
  const [activeSnapshot, setActiveSnapshot] = useState<Snapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

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
      // Prefill form with current manual-provider values if present
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
      const providers = await fetchProviders();
      const hasManual = providers.providers.find(
        (p) => p.plugin_id === "katalog.sources.user_editor.UserEditorSource",
      );
      if (!hasManual) {
        await createProvider({
          name: "Manual edits",
          plugin_id: "katalog.sources.user_editor.UserEditorSource",
          config: {},
        });
      }
      const snap = await startManualSnapshot();
      setActiveSnapshot(snap);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, []);

  const finishSnapshot = useCallback(async () => {
    if (!activeSnapshot) return;
    setError(null);
    try {
      const refreshed = await finishSnapshotApi(activeSnapshot.id);
      setActiveSnapshot(refreshed.snapshot);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, [activeSnapshot]);

  const discardSnapshot = useCallback(async () => {
    if (!activeSnapshot) return;
    setError(null);
    try {
      await deleteSnapshot(activeSnapshot.id);
      setActiveSnapshot(null);
      await load();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  }, [activeSnapshot, load]);

  const canEdit = useMemo(() => Boolean(schema && activeSnapshot), [schema, activeSnapshot]);

  const handleSubmit = useCallback(
    async ({ formData: data }) => {
      if (!activeSnapshot) {
        setError("Start a manual edit snapshot first");
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
          body: JSON.stringify({ snapshot_id: activeSnapshot.id, metadata: data }),
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
    [activeSnapshot, assetIdNum, load],
  );

  return (
    <>
      <AppHeader>
        <div>
          <h2>Asset #{assetId}</h2>
          <p>View and edit metadata via manual snapshots.</p>
        </div>
        <div className="button-row">
          <Link to="/assets" className="link-button">
            Back
          </Link>
          {!activeSnapshot && (
            <button
              className="btn-primary"
              type="button"
              onClick={() => void startManual()}
              disabled={loading}
            >
              Start editing
            </button>
          )}
          {activeSnapshot && (
            <>
              <button type="button" onClick={() => void finishSnapshot()} disabled={loading}>
                Finish changes
              </button>
              <button type="button" onClick={() => void discardSnapshot()} disabled={loading}>
                Discard snapshot
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
                  {!activeSnapshot && <p className="note">Start editing to enable form.</p>}
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
