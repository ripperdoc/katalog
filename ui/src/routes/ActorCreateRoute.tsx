import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { createActor, fetchPlugins, fetchPluginConfigSchema } from "../api/client";
import type { PluginSpec } from "../types/api";
import ActorForm from "../components/ActorForm";
import AppHeader from "../components/AppHeader";

const USER_EDITOR_PLUGIN_ID = "katalog.editors.user_editor.UserEditor";

function ActorCreateRoute() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [pluginId, setPluginId] = useState<string>("");
  const [name, setName] = useState<string>("");
  const [schema, setSchema] = useState<Record<string, unknown> | null>(null);
  const [configData, setConfigData] = useState<Record<string, unknown>>({});
  const [configToml, setConfigToml] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const defaultNameRef = useRef<string>("");

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
      const filtered = typeFilter ? list.filter((p) => p.actor_type === typeFilter) : list;
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
      const nextDefault = pluginId.split(".").pop() || pluginId;
      if (!name || name === defaultNameRef.current) {
        setName(nextDefault);
        defaultNameRef.current = nextDefault;
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
    setConfigToml("");
  }, [pluginId]);

  const handleSubmit = async () => {
    if (!pluginId) {
      setError("Select a plugin");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const payload: {
        name: string;
        plugin_id: string;
        config?: Record<string, unknown>;
        config_toml?: string;
      } = {
        name: name || pluginId,
        plugin_id: pluginId,
      };

      // Only send the relevant field based on which mode is active
      if (configToml.trim()) {
        payload.config_toml = configToml;
      } else {
        payload.config = configData;
      }

      const res = await createActor(payload);
      navigate(`/actors/${res.actor.id}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
      <AppHeader breadcrumbLabel="New Actor">
        <div className="panel-actions">
          <button
            type="button"
            className="app-btn btn-save"
            onClick={() => void handleSubmit()}
            disabled={!pluginId || loading}
          >
            {loading ? "Saving..." : "Create"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
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
              configToml={configToml}
              onConfigTomlChange={setConfigToml}
              onSubmit={() => void handleSubmit()}
              canSubmit={Boolean(pluginId)}
              submitting={loading}
              showSubmit={false}
            />
          </div>
        </section>
      </main>
    </>
  );
}

export default ActorCreateRoute;
