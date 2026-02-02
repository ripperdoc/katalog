import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import ActorList from "../components/ActorList";
import {
  fetchPlugins,
  fetchActors,
  runSources,
  runProcessors,
  runProcessor,
  runAnalyzer,
  runAllProcessors,
  updateActor,
} from "../api/client";
import type { Actor, PluginSpec } from "../types/api";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

function ActorsRoute() {
  const [actors, setActors] = useState<Actor[]>([]);
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanningId, setRunningId] = useState<number | null>(null);
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

  useEffect(() => {
    if (didInit.current) {
      return;
    }
    didInit.current = true;
    void loadActors();
    void loadPlugins();
  }, [loadActors, loadPlugins]);

  const triggerRun = async (actor?: Actor, groupKey?: "sources" | "processors" | "analyzers") => {
    const runningId = actor?.id ?? 0;
    setRunningId(runningId);
    setError(null);
    try {
      if (groupKey === "sources") {
        if (!actor) {
          throw new Error("Select a source to run");
        }
        const changeset = await runSources(actor.id);
        startTracking(changeset);
        navigate(`/changesets/${changeset.id}`);
        return;
      }
      if (groupKey === "processors") {
        if (actor?.id) {
          const changeset = await runProcessor(actor.id);
          startTracking(changeset);
          navigate(`/changesets/${changeset.id}`);
          return;
        }
        const changeset = await runProcessors();
        startTracking(changeset);
        navigate(`/changesets/${changeset.id}`);
        return;
      }
      if (groupKey === "analyzers") {
        if (actor?.id) {
          const changeset = await runAnalyzer(actor.id);
          startTracking(changeset);
        } else {
          throw new Error("Select an analyzer to run");
        }
        return;
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setRunningId(null);
    }
  };

  const filteredPlugins = useCallback(
    (ptype: "SOURCE" | "PROCESSOR" | "ANALYZER" | "EDITOR") =>
      plugins.filter((p) => p.type === ptype),
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

  return (
    <>
      <AppHeader />
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {(["sources", "processors", "analyzers", "editors"] as const).map((groupKey) => {
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
            const availablePlugins = filteredPlugins(
              typeConst as "SOURCE" | "PROCESSOR" | "ANALYZER" | "EDITOR",
            );
            const runAllEnabled = groupKey === "processors" || groupKey === "analyzers";
            const hasActors = list.length > 0;
            const requiresActors =
              groupKey === "sources" || groupKey === "processors" || groupKey === "analyzers";

            return (
              <div key={groupKey} className="subsection">
                <div className="panel-header">
                  <h3>{typeLabel}</h3>
                  <div className="panel-actions">
                    {runAllEnabled && (
                      <button
                        type="button"
                        className="app-btn btn-primary"
                        onClick={async () => {
                          setError(null);
                          try {
                            if (groupKey === "processors") {
                              setRunningId(0);
                              const snap = await runAllProcessors();
                              startTracking(snap);
                              navigate(`/changesets/${snap.id}`);
                            }
                          } catch (err) {
                            const message = err instanceof Error ? err.message : String(err);
                            setError(message);
                          } finally {
                            setRunningId(null);
                          }
                        }}
                        disabled={scanningId !== null || loading || (requiresActors && !hasActors)}
                        title={
                          hasActors || !requiresActors
                            ? groupKey === "processors"
                              ? "Run all processors on assets"
                              : "Run all analyzers"
                            : "Add an actor to enable this action"
                        }
                      >
                        {scanningId !== null ? "Starting..." : "Run all"}
                      </button>
                    )}
                    <button
                      type="button"
                      className="app-btn btn-primary"
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
                        availablePlugins.length === 0
                          ? "No plugins installed for this type"
                          : undefined
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
                    runningId={scanningId}
                    loading={loading}
                    showEdit={true}
                    showToggle={true}
                    showRun={groupKey !== "editors"}
                    runDisabled={scanningId !== null}
                    onToggleDisabled={(actor) => void toggleDisabled(actor)}
                    onRun={(actor) => triggerRun(actor, groupKey)}
                  />
                </div>
              </div>
            );
          })}
        </section>
      </main>
    </>
  );
}

export default ActorsRoute;
