import react from "@vitejs/plugin-react-swc";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "VITE");
  const apiBase = env.VITE_API_BASE_URL || "http://localhost:8000";
  const appVersion = process.env.npm_package_version ?? "dev";

  return {
    plugins: [react()],
    server: {
      port: 5173,
      strictPort: true,
      proxy: {
        "/api": {
          target: apiBase,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ""),
        },
      },
    },
    define: {
      __APP_VERSION__: JSON.stringify(appVersion),
      global: "window", // polyfill for node-targeted libs like @iarna/toml
    },
  };
});
