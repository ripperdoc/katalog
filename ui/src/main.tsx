import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import "./styles.css";
import { ChangesetProgressProvider } from "./contexts/ChangesetProgressContext";
import { useEffect } from "react";
import { fetchActiveChangesets } from "./api/client";
import { useChangesetProgress } from "./contexts/ChangesetProgressContext";

const SeedActiveChangesets = ({ children }: { children: React.ReactNode }) => {
  const { seedActive } = useChangesetProgress();
  useEffect(() => {
    (async () => {
      try {
        const res = await fetchActiveChangesets();
        seedActive(res.changesets ?? []);
      } catch {
        // ignore seed errors, UI can still start tracking when user triggers runs
      }
    })();
  }, [seedActive]);
  return <>{children}</>;
};

// Provide a browser-friendly alias for libraries expecting Node's `global`.
// This is safe in browsers and ignored if already defined.
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
if (typeof window !== "undefined" && typeof (window as any).global === "undefined") {
  (window as any).global = window;
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <BrowserRouter basename={import.meta.env.BASE_URL}>
      <ChangesetProgressProvider>
        <SeedActiveChangesets>
          <App />
        </SeedActiveChangesets>
      </ChangesetProgressProvider>
    </BrowserRouter>
  </React.StrictMode>
);
