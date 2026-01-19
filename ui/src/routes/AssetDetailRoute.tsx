import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import { fetchAssetDetail } from "../api/client";
import type { AssetDetailResponse } from "../types/api";

function AssetDetailRoute() {
  const { assetId } = useParams();
  const assetIdNum = assetId ? Number(assetId) : NaN;

  const [payload, setPayload] = useState<AssetDetailResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!assetIdNum || Number.isNaN(assetIdNum)) {
      setError("Invalid asset id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchAssetDetail(assetIdNum);
      setPayload(response);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setPayload(null);
    } finally {
      setLoading(false);
    }
  }, [assetIdNum]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <>
      <AppHeader>
        <div>
          <h2>Asset #{assetId}</h2>
          <p>Raw JSON response from the backend.</p>
        </div>
        <div className="button-row">
          <Link to="/assets" className="link-button">
            Back
          </Link>
          <button type="button" onClick={() => void load()} disabled={loading}>
            {loading ? "Loading..." : "Reload"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}

          {payload && (
            <div className="record-list">
              <div className="file-card">
                <h3>Asset JSON</h3>
                <pre>{JSON.stringify(payload, null, 2)}</pre>
              </div>
            </div>
          )}

          {!payload && !loading && !error && <div className="empty-state">Asset not found.</div>}
        </section>
      </main>
    </>
  );
}

export default AssetDetailRoute;
