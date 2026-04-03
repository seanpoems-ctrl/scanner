import { useCallback, useEffect, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

export type RotationHistoryPoint = {
  date: string;
  rs1m?: number | null;
  perf1D?: number | null;
  perf1W?: number | null;
  perf1M?: number | null;
};

export type RotationThemeRow = {
  theme: string;
  sector?: string | null;
  rs1m?: number | null;
  perf1D?: number | null;
  perf1W?: number | null;
  perf1M?: number | null;
  rs_delta?: number | null;
  history: RotationHistoryPoint[];
};

export type RotationPayload = {
  ok?: boolean;
  themes?: RotationThemeRow[];
  generated_at_utc?: string;
};

export function useRotationSnapshot(days: number) {
  const [data, setData] = useState<RotationPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`${API_BASE_URL}/api/rotation/snapshot?days=${days}`);
      if (!r.ok) throw new Error(`rotation ${r.status}`);
      setData(await r.json());
      setLastUpdatedAt(Date.now());
    } catch (e) {
      setData(null);
      setError(e instanceof Error ? e.message : "Failed to load rotation data");
    } finally {
      setLoading(false);
    }
  }, [days]);

  useEffect(() => {
    void load();
  }, [load]);

  return { data, loading, error, reload: load, lastUpdatedAt };
}
