import { useEffect, useMemo, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";
import { buildSkyteIndustryMap, type SkyteIndustriesPayload, type SkyteIndustryRow } from "../lib/skyteRs";

export function useSkyteRsIndustries(enabled: boolean) {
  const [payload, setPayload] = useState<SkyteIndustriesPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) {
      setPayload(null);
      setError(null);
      setLoading(false);
      return;
    }
    let alive = true;
    setLoading(true);
    setError(null);
    void fetch(`${API_BASE_URL}/api/rs/skyte/industries`)
      .then(async (r) => {
        const j = (await r.json()) as SkyteIndustriesPayload;
        if (!r.ok) {
          throw new Error(j.detail || `skyte ${r.status}`);
        }
        return j;
      })
      .then((j) => {
        if (!alive) return;
        setPayload(j);
      })
      .catch((e) => {
        if (!alive) return;
        setPayload(null);
        setError(e instanceof Error ? e.message : "skyte industries failed");
      })
      .finally(() => {
        if (!alive) return;
        setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [enabled]);

  const rows: SkyteIndustryRow[] = useMemo(() => {
    if (!payload?.ok || !Array.isArray(payload.rows)) return [];
    return payload.rows;
  }, [payload]);

  const lookupMap = useMemo(() => buildSkyteIndustryMap(rows), [rows]);

  return { payload, loading, error, lookupMap, rowCount: rows.length };
}
