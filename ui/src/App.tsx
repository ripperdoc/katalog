import { Navigate, Route, Routes } from "react-router-dom";
import RecordsRoute from "./routes/RecordsRoute";
import Test from "./routes/Test";

function App() {
  return (
    <div className="app-shell">
      <header>
        <h1>Katalog Explorer</h1>
        <p>Inspect scanned sources via the local FastAPI backend.</p>
      </header>
      <main>
        <Routes>
          <Route path="/records" element={<RecordsRoute />} />
          <Route path="/test" element={<Test />} />
          <Route path="*" element={<Navigate to="/records" replace />} />
        </Routes>
      </main>
      <footer>
        <small>Version {__APP_VERSION__ ?? "dev"}</small>
      </footer>
    </div>
  );
}

export default App;
