import { useCallback, useState } from "react";
import type { WatchlistItem } from "../hooks/useWatchlist";
import { useWatchlistQuotes } from "../hooks/useWatchlistQuotes";
import { fmtPct, pctClass } from "../lib/formatters";

type MarketStatus = {
  session: "closed" | "premarket" | "open" | "post";
};

type Props = {
  onClose: () => void;
  marketStatus: MarketStatus | null;
  items: WatchlistItem[];
  loading: boolean;
  error: string | null;
  onReload: () => void;
  onRemove: (ticker: string) => Promise<void>;
  onUpdateNote: (ticker: string, note: string) => Promise<void>;
  onSelectTicker: (ticker: string) => void;
  ibkrLive?: boolean;
};

function WatchlistRow({
  item,
  close,
  today_return_pct,
  onRemove,
  onUpdateNote,
  onSelectTicker,
}: {
  item: WatchlistItem;
  close: number | null;
  today_return_pct: number | null;
  onRemove: (t: string) => Promise<void>;
  onUpdateNote: (t: string, n: string) => Promise<void>;
  onSelectTicker: (t: string) => void;
}) {
  const t = String(item.ticker || "").toUpperCase();
  const [noteDraft, setNoteDraft] = useState(item.note ?? "");
  const [saving, setSaving] = useState(false);

  const saveNote = useCallback(async () => {
    if (noteDraft === (item.note ?? "")) return;
    setSaving(true);
    try {
      await onUpdateNote(t, noteDraft);
    } finally {
      setSaving(false);
    }
  }, [noteDraft, item.note, onUpdateNote, t]);

  return (
    <li className="rounded-lg border border-terminal-border/70 bg-terminal-bg/40 px-3 py-2.5">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <button
            type="button"
            onClick={() => onSelectTicker(t)}
            className="font-mono text-[13px] font-bold text-accent hover:underline"
          >
            {t}
          </button>
          <p className="mt-0.5 truncate t-micro text-slate-500">
            {[item.theme, item.sector].filter(Boolean).join(" · ") || "—"}
            {item.grade ? ` · Grade ${item.grade}` : ""}
          </p>
          <div className="mt-1.5 flex flex-wrap items-baseline gap-x-2 gap-y-0.5 t-mono text-slate-300">
            <span>{close != null ? `$${close.toFixed(2)}` : "—"}</span>
            <span className={pctClass(today_return_pct ?? 0)}>
              {today_return_pct != null ? fmtPct(today_return_pct, 2) : ""}
            </span>
          </div>
        </div>
        <button
          type="button"
          onClick={() => void onRemove(t)}
          className="shrink-0 rounded-md border border-rose-900/50 bg-rose-950/30 px-2 py-1 text-[11px] font-semibold text-rose-200 hover:border-rose-700 hover:bg-rose-950/50"
        >
          Remove
        </button>
      </div>
      <label className="mt-2 block">
        <span className="sr-only">Note for {t}</span>
        <div className="flex gap-2">
          <input
            value={noteDraft}
            onChange={(e) => setNoteDraft(e.target.value)}
            onBlur={() => void saveNote()}
            placeholder="Note…"
            className="min-w-0 flex-1 rounded-md border border-terminal-border bg-terminal-bg px-2 py-1.5 text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-terminal-elevated"
          />
          {saving ? <span className="t-micro self-center text-slate-500">Saving…</span> : null}
        </div>
      </label>
    </li>
  );
}

export function WatchlistDrawer({
  onClose,
  marketStatus,
  items,
  loading,
  error,
  onReload,
  onRemove,
  onUpdateNote,
  onSelectTicker,
  ibkrLive = false,
}: Props) {
  const marketOpen = marketStatus?.session === "open";
  // When IBKR is live refresh every 30s during market hours, otherwise 60s / 5m
  const refreshMs = ibkrLive
    ? marketOpen ? 30_000 : 60_000
    : marketOpen ? 60_000 : 5 * 60_000;
  const quotes = useWatchlistQuotes(
    items.map((i) => i.ticker),
    refreshMs,
    ibkrLive
  );

  return (
    <aside
      id="watchlist-drawer"
      className="h-full w-[380px] min-w-[320px] max-w-[100vw] shrink-0 border-l border-terminal-border bg-terminal-elevated"
      role="complementary"
      aria-label="Watchlist"
    >
      <div className="flex h-full min-h-0 flex-col">
        <header className="shrink-0 border-b border-terminal-border px-4 py-3">
          <div className="flex items-start justify-between gap-3">
            <div>
              <div className="flex items-center gap-2">
                <h2 className="t-page">Watchlist</h2>
                <span className="t-micro text-slate-500">
                  {ibkrLive ? (
                    <span className="flex items-center gap-1">
                      <span className="inline-block h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
                      {marketOpen ? "IBKR Live · 30s" : "IBKR Live · 60s"}
                    </span>
                  ) : marketOpen ? (
                    <span className="flex items-center gap-1">
                      <span className="inline-block h-1.5 w-1.5 rounded-full bg-amber-400 animate-pulse" />
                      Delayed · 60s
                    </span>
                  ) : (
                    "Delayed · 5m"
                  )}
                </span>
              </div>
              <p className="mt-0.5 t-data text-slate-500">{items.length} ticker{items.length === 1 ? "" : "s"}</p>
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => void onReload()}
                className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-1 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white"
              >
                Refresh
              </button>
              <button
                type="button"
                onClick={onClose}
                className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-1 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white"
                aria-label="Close watchlist"
              >
                Close
              </button>
            </div>
          </div>
          <p className="mt-2 t-micro text-slate-600">Press Esc to close. Click a ticker for full details.</p>
        </header>

        <div className="fintech-scroll min-h-0 flex-1 overflow-auto p-3">
          {loading && !items.length ? (
            <p className="t-data text-slate-500">Loading…</p>
          ) : error ? (
            <p className="text-sm text-rose-300">{error}</p>
          ) : !items.length ? (
            <p className="t-data text-slate-500">No tickers yet. Star names in the scanner or gappers tab.</p>
          ) : (
            <ul className="space-y-2">
              {items.map((item) => {
                const t = String(item.ticker || "").toUpperCase();
                const quote = quotes[t];
                const close = quote?.close ?? null;
                const changePct = quote?.today_return_pct ?? null;
                return (
                  <WatchlistRow
                    key={t}
                    item={item}
                    close={close}
                    today_return_pct={changePct}
                    onRemove={onRemove}
                    onUpdateNote={onUpdateNote}
                    onSelectTicker={onSelectTicker}
                  />
                );
              })}
            </ul>
          )}
        </div>
      </div>
    </aside>
  );
}
