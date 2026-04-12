import { useCallback, useEffect, useMemo, useState } from "react";
import { API_BASE_URL } from "../lib/apiBase";

type QuoteMap = Record<string, { close: number | null; today_return_pct: number | null }>;

export function useWatchlistQuotes(tickers: string[], refreshMs: number, ibkrLive = false) {
  const [quotes, setQuotes] = useState<QuoteMap>({});

  const sortedTickers = useMemo(
    () => [...new Set(tickers.map((t) => String(t || "").trim().toUpperCase()).filter(Boolean))].sort(),
    [tickers]
  );

  // ── IBKR live path: one batch call for all tickers ──────────────────────
  const fetchIbkrQuotes = useCallback(async () => {
    if (!sortedTickers.length) return;
    const slice = sortedTickers.slice(0, 20); // IBKR endpoint cap
    try {
      const r = await fetch(
        `${API_BASE_URL}/api/ibkr/quotes?symbols=${encodeURIComponent(slice.join(","))}`
      );
      if (!r.ok) return;
      const data = (await r.json()) as {
        ok: boolean;
        live: boolean;
        quotes?: Array<{ ticker: string; last: number | null; change_pct: number | null }>;
      };
      if (!data.ok || !data.live || !data.quotes) return;
      const results: QuoteMap = {};
      for (const q of data.quotes) {
        if (!q.ticker) continue;
        results[q.ticker.toUpperCase()] = {
          close: q.last ?? null,
          today_return_pct: q.change_pct ?? null,
        };
      }
      setQuotes((prev) => ({ ...prev, ...results }));
    } catch {
      /* stale quote ok */
    }
  }, [sortedTickers]);

  // ── Delayed path: per-ticker yfinance calls ──────────────────────────────
  const fetchDelayedQuotes = useCallback(async () => {
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

  const fetchQuotes = ibkrLive ? fetchIbkrQuotes : fetchDelayedQuotes;

  useEffect(() => {
    void fetchQuotes();
    const id = window.setInterval(() => void fetchQuotes(), refreshMs);
    return () => window.clearInterval(id);
  }, [fetchQuotes, refreshMs]);

  return quotes;
}
