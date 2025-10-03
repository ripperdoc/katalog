import SourceExplorer from "./components/SourceExplorer";

function App() {
  return (
    <div className="app-shell">
      <header>
        <h1>Katalog Explorer</h1>
        <p>Inspect scanned sources via the local FastAPI backend.</p>
      </header>
      <main>
        <SourceExplorer />
      </main>
      <footer>
        <small>Version {__APP_VERSION__ ?? "dev"}</small>
      </footer>
    </div>
  );
}

export default App;
