import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import {
  fetchProvider,
  startScan,
  updateProvider,
  fetchProviderConfigSchema,
} from "../api/client";
import type { Provider, Snapshot } from "../types/api";

function ProviderDetailRoute() {
  const { providerId } = useParams();
  const navigate = useNavigate();
  const [provider, setProvider] = useState<Provider | null>(null);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [formName, setFormName] = useState<string>("");
  const [configSchema, setConfigSchema] = useState<Record<string, unknown>>({ type: "object" });
  const [configData, setConfigData] = useState<Record<string, unknown>>({});

  const providerIdNum = providerId ? Number(providerId) : NaN;

  const loadProvider = useCallback(async () => {
    if (!providerIdNum || Number.isNaN(providerIdNum)) {
      setError("Invalid provider id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchProvider(providerIdNum);
      setProvider(response.provider);
      setSnapshots(response.snapshots ?? []);
      setFormName(response.provider?.name ?? "");
      setConfigData(response.provider?.config ?? {});
      try {
        const schemaRes = await fetchProviderConfigSchema(providerIdNum);
        setConfigSchema(schemaRes.schema || { type: "object" });
        setConfigData(schemaRes.value || response.provider?.config || {});
      } catch {
        setConfigSchema({ type: "object" });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setProvider(null);
      setSnapshots([]);
    } finally {
      setLoading(false);
    }
  }, [providerIdNum]);

  useEffect(() => {
    void loadProvider();
  }, [loadProvider]);

  const triggerScan = async () => {
    if (!providerIdNum || Number.isNaN(providerIdNum)) {
      setError("Invalid provider id");
      return;
    }
    setScanning(true);
    setError(null);
    try {
      const snapshot = await startScan(providerIdNum);
      navigate(`/snapshots/${snapshot.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setScanning(false);
    }
  };

  const canSave = useMemo(() => Boolean(formName), [formName]);

  const handleSave = async () => {
    if (!providerIdNum || Number.isNaN(providerIdNum)) {
      setError("Invalid provider id");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await updateProvider(providerIdNum, { name: formName, config: configData });
      await loadProvider();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Provider #{providerId}</h2>
          <p>Inspect provider details and snapshots.</p>
        </div>
        <div className="button-row">
          <Link to="/providers" className="link-button">
            Back
          </Link>
          <button type="button" onClick={triggerScan} disabled={scanning || loading}>
            {scanning ? "Starting..." : "Scan"}
          </button>
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      {provider && (
        <div className="record-list">
          <div className="file-card">
            <h3>Provider</h3>
            <label className="form-row">
              <span>Name</span>
              <input
                type="text"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                placeholder="Provider name"
              />
            </label>
            <label className="form-row">
              <span>Config</span>
              <Form
                schema={configSchema as any}
                formData={configData}
                onChange={(evt) => setConfigData(evt.formData as Record<string, unknown>)}
                liveValidate={false}
                validator={validator}
              >
                <div />
              </Form>
            </label>
            <div className="button-row">
              <button type="button" onClick={handleSave} disabled={!canSave || saving}>
                {saving ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
          <div className="file-card">
            <h3>History</h3>
            {snapshots.length === 0 && <div className="empty-state">No snapshots yet.</div>}
            {snapshots.map((snap) => (
              <div key={snap.id} className="status-bar">
                <Link to={`/snapshots/${snap.id}`}>Snapshot #{snap.id}</Link>
                <span>{snap.status}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {!provider && !loading && !error && <div className="empty-state">Provider not found.</div>}
    </section>
  );
}

export default ProviderDetailRoute;
