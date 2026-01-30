import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import ActorForm from "../components/ActorForm";
import {
  fetchActor,
  runSources,
  runProcessors,
  runProcessor,
  runAnalyzer,
  updateActor,
  fetchActorConfigSchema,
} from "../api/client";
import type { Actor, Changeset } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

function ActorDetailRoute() {
  const { actorId } = useParams();
  const navigate = useNavigate();
  const [actor, setActor] = useState<Actor | null>(null);
  const [changesets, setChangesets] = useState<Changeset[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [formName, setFormName] = useState<string>("");
  const [configSchema, setConfigSchema] = useState<Record<string, unknown> | null>(null);
  const [configData, setConfigData] = useState<Record<string, unknown>>({});
  const [configToml, setConfigToml] = useState<string>("");
  const { startTracking } = useChangesetProgress();

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
      setConfigToml(response.actor?.config_toml ?? "");
      try {
        const schemaRes = await fetchActorConfigSchema(actorIdNum);
        setConfigSchema(schemaRes.schema ?? null);
        setConfigData(schemaRes.value ?? response.actor?.config ?? {});
      } catch {
        setConfigSchema(null);
        setConfigData(response.actor?.config ?? {});
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
      if (actor?.type === "SOURCE") {
        const changeset = await runSources(actorIdNum);
        startTracking(changeset);
        navigate(`/changesets/${changeset.id}`);
      } else if (actor?.type === "PROCESSOR") {
        const changeset = await runProcessor(actorIdNum);
        startTracking(changeset);
        navigate(`/changesets/${changeset.id}`);
      } else if (actor?.type === "ANALYZER") {
        const changeset = await runAnalyzer(actorIdNum);
        startTracking(changeset);
        navigate(`/changesets/${changeset.id}`);
      }
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
      const payload: {
        name: string;
        config?: Record<string, unknown>;
        config_toml?: string;
      } = { name: formName };

      // Only send the relevant field based on which mode is active
      if (configToml.trim()) {
        payload.config_toml = configToml;
      } else {
        payload.config = configData;
      }

      await updateActor(actorIdNum, payload);
      await loadActor();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  const handleToggleDisabled = async () => {
    if (!actor || !actorIdNum || Number.isNaN(actorIdNum)) {
      setError("Invalid actor id");
      return;
    }
    setToggling(true);
    setError(null);
    try {
      await updateActor(actorIdNum, { disabled: !actor.disabled });
      await loadActor();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setToggling(false);
    }
  };

  return (
    <>
      <AppHeader>
        <div>
          <h2>{formName ? formName : `Actor #${actorId}`}</h2>
          <p>Inspect actor details and changesets.</p>
        </div>
        <div className="button-row">
          <Link to="/actors" className="link-button">
            Back
          </Link>
          {actor && (
            <button
              className="app-btn btn-primary"
              type="button"
              onClick={() => void handleToggleDisabled()}
              disabled={toggling || loading}
            >
              {toggling ? "Updating..." : actor.disabled ? "Enable" : "Disable"}
            </button>
          )}
          {actor?.type !== "EDITOR" && (
            <button
              className="btn-primary"
              type="button"
              onClick={triggerScan}
              disabled={scanning || loading || Boolean(actor?.disabled)}
            >
              {scanning ? "Starting..." : "Scan"}
            </button>
          )}
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {actor && (
            <div className="record-list">
              <div className="file-card">
                <h3>Actor</h3>
                <div className="meta-grid">
                  <div>Created: {actor.created_at ?? "—"}</div>
                  <div>Updated: {actor.updated_at ?? "—"}</div>
                </div>
                <ActorForm
                  isCreating={false}
                  pluginId={actor.plugin_id ?? ""}
                  name={formName}
                  onNameChange={setFormName}
                  schema={configSchema}
                  configData={configData}
                  onConfigChange={setConfigData}
                  configToml={configToml}
                  onConfigTomlChange={setConfigToml}
                  onSubmit={() => void handleSave()}
                  canSubmit={canSave}
                  submitting={saving}
                  submitLabel="Save"
                  submittingLabel="Saving..."
                />
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
