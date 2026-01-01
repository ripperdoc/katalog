import { Link, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import RecordsRoute from "./routes/RecordsRoute";
import ProviderDetailRoute from "./routes/ProviderDetailRoute";
import ProvidersRoute from "./routes/ProvidersRoute";
import SnapshotDetailRoute from "./routes/SnapshotDetailRoute";
import SnapshotsRoute from "./routes/SnapshotsRoute";
import { syncConfig } from "./api/client";

function App() {
  return (
    <div className="app-shell">
      <header>
        <h1>Katalog Explorer</h1>
        <p>Inspect scanned sources via the local FastAPI backend.</p>
        <nav className="nav">
          <Link to="/providers">Providers</Link>
          <Link to="/snapshots">Snapshots</Link>
          <Link to="/records">Assets</Link>
          <ReloadButton />
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/providers" element={<ProvidersRoute />} />
          <Route path="/providers/:providerId" element={<ProviderDetailRoute />} />
          <Route path="/snapshots" element={<SnapshotsRoute />} />
          <Route path="/snapshots/:snapshotId" element={<SnapshotDetailRoute />} />
          <Route path="/records" element={<RecordsRoute />} />
          <Route path="*" element={<Navigate to="/providers" replace />} />
        </Routes>
      </main>
      <footer>
        <small>Version {__APP_VERSION__ ?? "dev"}</small>
      </footer>
    </div>
  );
}

export default App;

function ReloadButton() {
  const navigate = useNavigate();
  const handleReload = async () => {
    try {
      await syncConfig();
      navigate("/providers");
    } catch (err) {
      console.error("syncConfig failed:", err);
    }
  };

  return (
    <button onClick={handleReload} title="Reload configuration from server">
      Reload
    </button>
  );
}
