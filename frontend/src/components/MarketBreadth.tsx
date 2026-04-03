import { memo, useCallback, useEffect, useRef, useState } from "react";
import { BarChart3, RefreshCw } from "lucide-react";
import MarketBreadthReport, { type MarketBreadthPayload } from "./MarketBreadthReport";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim() || "http://127.0.0.1:8000";

const MarketBreadth = memo(function MarketBreadth() {
  const [stockbee, setStockbee] = useState<MarketBreadthPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    try {
      const rsb = await fetch(`${API_BASE_URL}/api/market-breadth`, { signal: ctrl.signal });
      try {
        const sb = (await rsb.json()) as MarketBreadthPayload;
        setStockbee(!rsb.ok ? { ...sb, ok: false, detail: sb.detail ?? `HTTP ${rsb.status}` } : sb);
      } catch {
        setStockbee({ ok: false, rows: [], detail: `market-breadth HTTP ${rsb.status}` });
      }
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return;
      setError(e instanceof Error ? e.message : String(e));
      setStockbee(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 30 * 60_000);
    return () => {
      window.clearInterval(id);
      abortRef.current?.abort();
    };
  }, [load]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-accent/15 text-accent">
            <BarChart3 className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <h2 className="text-sm font-semibold text-white">Market breadth</h2>
            <p className="text-[11px] text-slate-500">Stockbee Market Monitor · Finviz theme proxy</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => void load()}
            disabled={loading}
            title="Refresh Stockbee data"
            className="flex items-center gap-1.5 rounded-lg border border-terminal-border bg-terminal-bg px-3 py-1.5 text-[11px] font-semibold text-slate-300 transition-colors hover:border-slate-500 hover:text-white disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} aria-hidden />
            {loading ? "Loading…" : "Refresh"}
          </button>
        </div>
      </div>

      {error ? (
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-[12px] text-rose-300">
          Failed to load market breadth: {error}
        </div>
      ) : null}

      <MarketBreadthReport data={stockbee} />
    </div>
  );
});

export default MarketBreadth;
