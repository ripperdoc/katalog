import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import AppHeader from "../components/AppHeader";
import { fetchProvider, startScan, updateProvider, fetchProviderConfigSchema } from "../api/client";
import type { Provider, Changeset } from "../types/api";

function ProviderDetailRoute() {
  const { providerId } = useParams();
  const navigate = useNavigate();
  const [provider, setProvider] = useState<Provider | null>(null);
  const [changesets, setChangesets] = useState<Changeset[]>([]);
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
      setChangesets(response.changesets ?? []);
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
      setChangesets([]);
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
      const changeset = await startScan(providerIdNum);
      navigate(`/changesets/${changeset.id}`);
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
    <>
      <AppHeader>
        <div>
          <h2>Provider #{providerId}</h2>
          <p>Inspect provider details and changesets.</p>
        </div>
        <div className="button-row">
          <Link to="/providers" className="link-button">
            Back
          </Link>
          <button
            className="btn-primary"
            type="button"
            onClick={triggerScan}
            disabled={scanning || loading}
          >
            {scanning ? "Starting..." : "Scan"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
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
                  <button
                    className="btn-primary"
                    type="button"
                    onClick={handleSave}
                    disabled={!canSave || saving}
                  >
                    {saving ? "Saving..." : "Save"}
                  </button>
                </div>
              </div>
              <div className="file-card">
                <h3>History</h3>
                {changesets.length === 0 && <div className="empty-state">No changesets yet.</div>}
                {changesets.map((snap) => (
                  <div key={snap.id} className="status-bar">
                    <Link to={`/changesets/${snap.id}`}>Changeset #{snap.id}</Link>
                    <span>{snap.status}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          {!provider && !loading && !error && (
            <div className="empty-state">Provider not found.</div>
          )}
        </section>
      </main>
    </>
  );
}

export default ProviderDetailRoute;
