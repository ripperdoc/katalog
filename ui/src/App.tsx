import { Navigate, Route, Routes } from "react-router-dom";
import AssetsRoute from "./routes/AssetsRoute";
import AssetDetailRoute from "./routes/AssetDetailRoute";
import ProviderDetailRoute from "./routes/ProviderDetailRoute";
import ProvidersRoute from "./routes/ProvidersRoute";
import SnapshotDetailRoute from "./routes/SnapshotDetailRoute";
import SnapshotsRoute from "./routes/SnapshotsRoute";
import CollectionsRoute from "./routes/CollectionsRoute";
import CollectionDetailRoute from "./routes/CollectionDetailRoute";

function App() {
  return (
    <div className="app-shell">
      <Routes>
        <Route path="/providers" element={<ProvidersRoute />} />
        <Route path="/providers/:providerId" element={<ProviderDetailRoute />} />
        <Route path="/snapshots" element={<SnapshotsRoute />} />
        <Route path="/snapshots/:snapshotId" element={<SnapshotDetailRoute />} />
        <Route path="/assets" element={<AssetsRoute />} />
        <Route path="/assets/:assetId" element={<AssetDetailRoute />} />
        <Route path="/collections" element={<CollectionsRoute />} />
        <Route path="/collections/:collectionId" element={<CollectionDetailRoute />} />
        <Route path="*" element={<Navigate to="/providers" replace />} />
      </Routes>
    </div>
  );
}

export default App;
