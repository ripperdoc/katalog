import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import AppHeader from "../components/AppHeader";
import { fetchActor, startScan, updateActor, fetchActorConfigSchema } from "../api/client";
import type { Actor, Changeset } from "../types/api";

function ActorDetailRoute() {
  const { actorId } = useParams();
  const navigate = useNavigate();
  const [actor, setActor] = useState<Actor | null>(null);
  const [changesets, setChangesets] = useState<Changeset[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [formName, setFormName] = useState<string>("");
  const [configSchema, setConfigSchema] = useState<Record<string, unknown>>({ type: "object" });
  const [configData, setConfigData] = useState<Record<string, unknown>>({});

  const actorIdNum = actorId ? Number(actorId) : NaN;

  const loadActor = useCallback(async () => {
    if (!actorIdNum || Number.isNaN(actorIdNum)) {
      setError("Invalid actor id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchActor(actorIdNum);
      setActor(response.actor);
      setChangesets(response.changesets ?? []);
      setFormName(response.actor?.name ?? "");
      setConfigData(response.actor?.config ?? {});
      try {
        const schemaRes = await fetchActorConfigSchema(actorIdNum);
        setConfigSchema(schemaRes.schema || { type: "object" });
        setConfigData(schemaRes.value || response.actor?.config || {});
      } catch {
        setConfigSchema({ type: "object" });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setActor(null);
      setChangesets([]);
    } finally {
      setLoading(false);
    }
  }, [actorIdNum]);

  useEffect(() => {
    void loadActor();
  }, [loadActor]);

  const triggerScan = async () => {
    if (!actorIdNum || Number.isNaN(actorIdNum)) {
      setError("Invalid actor id");
      return;
    }
    setScanning(true);
    setError(null);
    try {
      const changeset = await startScan(actorIdNum);
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
    if (!actorIdNum || Number.isNaN(actorIdNum)) {
      setError("Invalid actor id");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await updateActor(actorIdNum, { name: formName, config: configData });
      await loadActor();
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
          <h2>Actor #{actorId}</h2>
          <p>Inspect actor details and changesets.</p>
        </div>
        <div className="button-row">
          <Link to="/actors" className="link-button">
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
          {actor && (
            <div className="record-list">
              <div className="file-card">
                <h3>Actor</h3>
                <label className="form-row">
                  <span>Name</span>
                  <input
                    type="text"
                    value={formName}
                    onChange={(e) => setFormName(e.target.value)}
                    placeholder="Actor name"
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
          {!actor && !loading && !error && <div className="empty-state">Actor not found.</div>}
        </section>
      </main>
    </>
  );
}

export default ActorDetailRoute;
