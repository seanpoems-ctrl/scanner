import { useCallback, useEffect, useMemo, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

type QuoteMap = Record<string, { close: number | null; today_return_pct: number | null }>;

export function useWatchlistQuotes(tickers: string[], refreshMs: number) {
  const [quotes, setQuotes] = useState<QuoteMap>({});

  const sortedTickers = useMemo(
    () => [...new Set(tickers.map((t) => String(t || "").trim().toUpperCase()).filter(Boolean))].sort(),
    [tickers]
  );

  const fetchQuotes = useCallback(async () => {
    if (!sortedTickers.length) return;
    const slice = sortedTickers.slice(0, 40);
    const results: QuoteMap = {};
    await Promise.allSettled(
      slice.map(async (ticker) => {
        try {
          const r = await fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(ticker)}`);
          if (!r.ok) return;
          const d = (await r.json()) as { close?: number | null; today_return_pct?: number | null };
          results[ticker] = {
            close: d.close ?? null,
            today_return_pct: d.today_return_pct ?? null,
          };
        } catch {
          /* stale quote ok */
        }
      })
    );
    setQuotes((prev) => ({ ...prev, ...results }));
  }, [sortedTickers]);

  useEffect(() => {
    void fetchQuotes();
    const id = window.setInterval(() => void fetchQuotes(), refreshMs);
    return () => window.clearInterval(id);
  }, [fetchQuotes, refreshMs]);

  return quotes;
}
