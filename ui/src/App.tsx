import { Link, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import AssetsRoute from "./routes/AssetsRoute";
import AssetDetailRoute from "./routes/AssetDetailRoute";
import ProviderDetailRoute from "./routes/ProviderDetailRoute";
import ProvidersRoute from "./routes/ProvidersRoute";
import SnapshotDetailRoute from "./routes/SnapshotDetailRoute";
import SnapshotsRoute from "./routes/SnapshotsRoute";
import { syncConfig } from "./api/client";

function App() {
  return (
    <div className="app-shell">
      <header>
        <h1>Katalog</h1>
        <p>Bring order to and analyze your digital assets.</p>
        <nav className="nav">
          <Link to="/providers">Providers</Link>
          <Link to="/snapshots">Snapshots</Link>
          <Link to="/assets">Assets</Link>
        </nav>
      </header>
      <main>
        <Routes>
          <Route path="/providers" element={<ProvidersRoute />} />
          <Route path="/providers/:providerId" element={<ProviderDetailRoute />} />
          <Route path="/snapshots" element={<SnapshotsRoute />} />
          <Route path="/snapshots/:snapshotId" element={<SnapshotDetailRoute />} />
          <Route path="/assets" element={<AssetsRoute />} />
          <Route path="/assets/:assetId" element={<AssetDetailRoute />} />
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
