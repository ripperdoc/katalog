import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import ActorList from "../components/ActorList";
import WorkflowList from "../components/WorkflowList";
import {
  fetchPlugins,
  fetchActors,
  fetchWorkflows,
  startWorkflow,
  updateActor,
} from "../api/client";
import type { Actor, PluginSpec, WorkflowSummary } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

function ActorsRoute() {
  const [actors, setActors] = useState<Actor[]>([]);
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runningWorkflow, setRunningWorkflow] = useState<string | null>(null);
  const navigate = useNavigate();
  const didInit = useRef(false);
  const { startTracking } = useChangesetProgress();

  const loadActors = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchActors();
      setActors(response.actors ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setActors([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const loadPlugins = useCallback(async () => {
    try {
      const response = await fetchPlugins();
      setPlugins(response.plugins ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setPlugins([]);
    }
  }, []);

  const loadWorkflows = useCallback(async () => {
    try {
      const response = await fetchWorkflows();
      setWorkflows(response.workflows ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setWorkflows([]);
    }
  }, []);

  useEffect(() => {
    if (didInit.current) {
      return;
    }
    didInit.current = true;
    void loadActors();
    void loadPlugins();
    void loadWorkflows();
  }, [loadActors, loadPlugins, loadWorkflows]);

  const filteredPlugins = useCallback(
    (ptype: "SOURCE" | "PROCESSOR" | "ANALYZER" | "EDITOR") =>
      plugins.filter((p) => p.actor_type === ptype),
    [plugins],
  );

  const grouped = {
    sources: actors.filter((p) => p.type === "SOURCE"),
    processors: actors.filter((p) => p.type === "PROCESSOR"),
    analyzers: actors.filter((p) => p.type === "ANALYZER"),
    editors: actors.filter((p) => p.type === "EDITOR"),
  };

  const toggleDisabled = async (actor: Actor) => {
    setError(null);
    try {
      await updateActor(actor.id, { disabled: !actor.disabled });
      await loadActors();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  };

  const triggerWorkflow = async (workflow: WorkflowSummary) => {
    setError(null);
    setRunningWorkflow(workflow.file_name);
    try {
      const response = await startWorkflow(workflow.file_name);

      await Promise.all([loadActors(), loadWorkflows()]);

      const started = response.changeset;
      if (started) {
        startTracking(started);
        navigate(`/changesets/${started.id}`);
        return;
      }

      const lastChangesetId = response.result?.last_changeset_id;
      if (lastChangesetId) {
        navigate(`/changesets/${lastChangesetId}`);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setRunningWorkflow(null);
    }
  };

  const renderActorGroup = (
    groupKey: "sources" | "processors" | "analyzers" | "editors",
  ) => {
    const typeLabel =
      groupKey === "sources"
        ? "Sources"
        : groupKey === "processors"
          ? "Processors"
          : groupKey === "analyzers"
            ? "Analyzers"
            : "Editors";
    const typeConst =
      groupKey === "sources"
        ? "SOURCE"
        : groupKey === "processors"
          ? "PROCESSOR"
          : groupKey === "analyzers"
            ? "ANALYZER"
            : "EDITOR";
    const list = grouped[groupKey];
    const availablePlugins = filteredPlugins(typeConst);
    return (
      <div key={groupKey} className="subsection">
        <div className="panel-header">
          <h3>{typeLabel}</h3>
          <div className="panel-actions">
            <button
              type="button"
              className="app-btn btn-save"
              onClick={() =>
                navigate(
                  `/actors/new?type=${
                    groupKey === "sources"
                      ? "sources"
                      : groupKey === "processors"
                        ? "processors"
                        : groupKey === "analyzers"
                          ? "analyzers"
                          : "editors"
                  }`,
                )
              }
              disabled={availablePlugins.length === 0}
              title={
                availablePlugins.length === 0 ? "No plugins installed for this type" : undefined
              }
            >
              Add
            </button>
          </div>
        </div>
        <div className="record-list">
          <ActorList
            actors={list}
            typeLabel={typeLabel}
            loading={loading}
            showEdit={true}
            showToggle={true}
            showRun={false}
            showAuthorize={groupKey === "sources"}
            onToggleDisabled={(actor) => void toggleDisabled(actor)}
          />
        </div>
      </div>
    );
  };

  return (
    <>
      <AppHeader />
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          <div className="subsection">
            <div className="panel-header">
              <h3>Workflows</h3>
            </div>
            <div className="record-list">
              <WorkflowList
                workflows={workflows}
                loading={loading}
                runningName={runningWorkflow}
                onRun={(workflow) => void triggerWorkflow(workflow)}
              />
            </div>
          </div>
          {(["sources"] as const).map((groupKey) => renderActorGroup(groupKey))}
          {(["processors", "analyzers", "editors"] as const).map((groupKey) =>
            renderActorGroup(groupKey),
          )}
        </section>
      </main>
    </>
  );
}

export default ActorsRoute;
