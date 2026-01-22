import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { createActor, fetchPlugins, fetchPluginConfigSchema } from "../api/client";
import type { PluginSpec } from "../types/api";
import ActorForm from "../components/ActorForm";

const USER_EDITOR_PLUGIN_ID = "katalog.editors.user_editor.UserEditor";

function ActorCreateRoute() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [pluginId, setPluginId] = useState<string>("");
  const [name, setName] = useState<string>("");
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [configData, setConfigData] = useState<Record<string, unknown>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const typeFilter = useMemo(() => {
    const t = params.get("type");
    if (!t) return null;
    const upper = t.toUpperCase();
    return upper === "SOURCES"
      ? "SOURCE"
      : upper === "PROCESSORS"
        ? "PROCESSOR"
        : upper === "ANALYZERS"
          ? "ANALYZER"
          : upper === "EDITORS"
            ? "EDITOR"
            : null;
  }, [params]);

  const loadPlugins = useCallback(async () => {
    setError(null);
    try {
      const res = await fetchPlugins();
      const list = res.plugins ?? [];
      const filtered = typeFilter ? list.filter((p) => p.type === typeFilter) : list;
      setPlugins(filtered);
      const defaultPlugin =
        filtered.find((p) => p.plugin_id === USER_EDITOR_PLUGIN_ID) || filtered[0];
      if (defaultPlugin) {
        setPluginId(defaultPlugin.plugin_id);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
      setPlugins([]);
    }
  }, [typeFilter]);

  useEffect(() => {
    void loadPlugins();
  }, [loadPlugins]);

  useEffect(() => {
    const loadSchema = async () => {
      if (!pluginId) {
        setSchema(null);
        return;
      }
      try {
        const res = await fetchPluginConfigSchema(pluginId);
        setSchema(res.schema ?? null);
      } catch {
        setSchema(null);
      }
    };
    void loadSchema();
  }, [pluginId]);

  useEffect(() => {
    setConfigData({});
  }, [pluginId]);

  const handleSubmit = async () => {
    if (!pluginId) {
      setError("Select a plugin");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await createActor({
        name: name || pluginId,
        plugin_id: pluginId,
        config: configData,
      });
      navigate(`/actors/${res.actor.id}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Create actor</h2>
          <p>Select a plugin and configure it.</p>
        </div>
        <div className="button-row">
          <Link to="/actors" className="link-button">
            Back
          </Link>
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      <div className="file-card">
        <ActorForm
          isCreating
          plugins={plugins}
          pluginId={pluginId}
          onPluginChange={setPluginId}
          name={name}
          onNameChange={setName}
          schema={schema}
          configData={configData}
          onConfigChange={setConfigData}
          onSubmit={() => void handleSubmit()}
          canSubmit={Boolean(pluginId)}
          submitting={loading}
          submitLabel="Create"
          submittingLabel="Saving..."
        />
      </div>
    </section>
  );
}

export default ActorCreateRoute;
