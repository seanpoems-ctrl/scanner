import { useEffect, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

/** Minimal ticker quote for compact UI (watchlist rows, etc.). */
export function useTickerIntel(ticker: string) {
  const [close, setClose] = useState<number | null>(null);
  const [todayPct, setTodayPct] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const t = ticker.trim().toUpperCase();
    if (!t) {
      setClose(null);
      setTodayPct(null);
      return;
    }
    let alive = true;
    setLoading(true);
    fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(t)}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(String(r.status));
        return r.json() as { close?: number | null; today_return_pct?: number };
      })
      .then((d) => {
        if (!alive) return;
        setClose(d.close ?? null);
        const p = d.today_return_pct;
        setTodayPct(typeof p === "number" && Number.isFinite(p) ? p : null);
      })
      .catch(() => {
        if (!alive) return;
        setClose(null);
        setTodayPct(null);
      })
      .finally(() => {
        if (!alive) return;
        setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [ticker]);

  return { close, today_return_pct: todayPct, loading };
}
