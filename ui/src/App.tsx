import { Navigate, Route, Routes } from "react-router-dom";
import AssetsRoute from "./routes/AssetsRoute";
import AssetDetailRoute from "./routes/AssetDetailRoute";
import ActorDetailRoute from "./routes/ActorDetailRoute";
import ActorsRoute from "./routes/ActorsRoute";
import ActorCreateRoute from "./routes/ActorCreateRoute";
import ChangesetDetailRoute from "./routes/ChangesetDetailRoute";
import ChangesetsRoute from "./routes/ChangesetsRoute";
import CollectionsRoute from "./routes/CollectionsRoute";
import CollectionDetailRoute from "./routes/CollectionDetailRoute";

function App() {
  return (
    <Routes>
      <Route path="/actors" element={<ActorsRoute />} />
      <Route path="/actors/new" element={<ActorCreateRoute />} />
      <Route path="/actors/:actorId" element={<ActorDetailRoute />} />
      <Route path="/changesets" element={<ChangesetsRoute />} />
      <Route path="/changesets/:changesetId" element={<ChangesetDetailRoute />} />
      <Route path="/assets" element={<AssetsRoute />} />
      <Route path="/assets/:assetId" element={<AssetDetailRoute />} />
      <Route path="/collections" element={<CollectionsRoute />} />
      <Route path="/collections/:collectionId" element={<CollectionDetailRoute />} />
      <Route path="*" element={<Navigate to="/actors" replace />} />
    </Routes>
  );
}

export default App;
