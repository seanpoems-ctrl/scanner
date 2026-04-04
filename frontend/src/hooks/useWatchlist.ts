import { useCallback, useEffect, useMemo, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

export type WatchlistItem = {
  ticker: string;
  theme?: string | null;
  sector?: string | null;
  grade?: string | null;
  note?: string | null;
  added_at_utc?: string | null;
};

export function useWatchlist() {
  const [items, setItems] = useState<WatchlistItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`${API_BASE_URL}/api/watchlist`);
      if (!r.ok) throw new Error(`watchlist ${r.status}`);
      const j = (await r.json()) as { items?: WatchlistItem[] };
      setItems(Array.isArray(j.items) ? j.items : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load watchlist");
      setItems([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  const watchlisted = useMemo(() => new Set(items.map((i) => String(i.ticker || "").toUpperCase()).filter(Boolean)), [items]);

  const addTicker = useCallback(
    async (ticker: string, ctx?: { theme?: string; sector?: string; grade?: string }) => {
      const r = await fetch(`${API_BASE_URL}/api/watchlist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ticker,
          theme: ctx?.theme,
          sector: ctx?.sector,
          grade: ctx?.grade,
        }),
      });
      if (!r.ok) throw new Error(`add ${r.status}`);
      await reload();
      return r.json() as Promise<{ added?: boolean }>;
    },
    [reload]
  );

  const removeTicker = useCallback(
    async (ticker: string) => {
      const r = await fetch(`${API_BASE_URL}/api/watchlist/${encodeURIComponent(ticker)}`, { method: "DELETE" });
      if (!r.ok) throw new Error(`remove ${r.status}`);
      await reload();
      return r.json() as Promise<{ removed?: boolean }>;
    },
    [reload]
  );

  const toggleTicker = useCallback(
    async (ticker: string, ctx?: { theme?: string; sector?: string; grade?: string }) => {
      const u = ticker.trim().toUpperCase();
      if (!u) return;
      const exists = items.some((i) => String(i.ticker || "").toUpperCase() === u);
      if (exists) await removeTicker(u);
      else await addTicker(u, ctx);
    },
    [items, addTicker, removeTicker]
  );

  const updateNote = useCallback(
    async (ticker: string, note: string) => {
      const r = await fetch(`${API_BASE_URL}/api/watchlist/${encodeURIComponent(ticker)}/note`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ note }),
      });
      if (!r.ok) throw new Error(`note ${r.status}`);
      await reload();
    },
    [reload]
  );

  return { items, loading, error, reload, watchlisted, addTicker, removeTicker, toggleTicker, updateNote };
}
