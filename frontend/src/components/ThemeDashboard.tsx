import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Info, LayoutGrid, Plus, Search, Sunrise } from "lucide-react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

const API_BASE_URL = "http://127.0.0.1:8000";

type ApiStock = {
  ticker: string;
  name: string;
  adr_pct: number;
  avg_dollar_volume: number;
  market_cap: number;
  close: number;
  ema10: number;
  ema20: number;
  ema50: number;
  ema200: number;
  current_volume: number;
  avg_volume_3m: number;
  volume_buzz_pct: number;
  qualifies_a_plus: boolean;
  qualifies_grade_a: boolean;
  gradeLabel: "A+" | "A" | "-";
  gap_open_pct?: number;
  or_rvol_ratio?: number | null;
  today_return_pct?: number;
  month_return_pct?: number;
  ep_candidate?: boolean;
};

type ApiTheme = {
  theme: string;
  sector?: string;
  relativeStrength1M: number | null;
  perf1D?: number | null;
  perf1W?: number | null;
  perf1M?: number | null;
  perf3M?: number | null;
  perf6M?: number | null;
  relativeStrengthQualifierRatio: number;
  leaders: string[];
  qualifiedCount: number;
  totalCount: number;
  themeDollarVolume: number;
  themePrevDollarVolume: number;
  themeAvg20DollarVolume: number;
  highLiquidity: boolean;
  accumulation: boolean;
  stocks: ApiStock[];
};

type ApiPayload = {
  vix: { symbol: string; close: number; change_pct: number };
  themes: ApiTheme[];
  leaderboardMeta?: { view?: string; source?: string; perfNote?: string; url?: string; urls?: { overview?: string; performance?: string } };
  tape?: { label: string; symbol: string; close: number | null; change_pct: number | null }[];
  marketFlowSummary?: {
    aggregateDollarVolume: number;
    previousAggregateDollarVolume: number;
    aggregateAvg20DollarVolume: number;
    flowTrend: "up" | "down";
    conviction: "high" | "low";
  };
  market_momentum_score?: {
    score: number;
    state: "bullish" | "neutral" | "bearish";
    message: string;
    aPlusCount: number;
    aCount: number;
  };
  polling?: { pollSeconds: number; backoffActive: boolean; retryAfterSeconds: number };
};

type PremarketBrief = {
  generated_at_utc: string | null;
  scheduled_for_et: string | null;
  narrative?: string[];
  sections: { title: string; bullets: string[] }[];
  headlines: { title: string; link: string; pubDate?: string }[];
  source?: { markets?: string; news?: string };
};

type PremarketGappersPayload = {
  source: string;
  market?: string;
  row_count: number;
  rows: Record<string, string | number | null>[];
  fetched_at_utc?: string;
  total_matched_scanner?: number;
  rows_after_avg_dollar_filter?: number;
  filters?: Record<string, number>;
  screener_path?: string;
  short_interest_source?: string | null;
};

type ThemeUniverseSpotlight = {
  label: string;
  slug: string;
  updated_at: string;
  best: { ticker: string; today_return_pct: number }[];
  worst: { ticker: string; today_return_pct: number }[];
};

type TickerIntel = {
  ticker: string;
  name: string;
  close: number | null;
  today_return_pct: number;
  sector: string;
  industry: string;
  sector_etf: string | null;
  theme: string | null;
  subtheme: string | null;
  theme_matches: string[];
};

type TickerSuggestRow = { ticker: string; name: string };

function formatMoney(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(2)}B`;
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
  return `$${value.toFixed(2)}`;
}

function pctClass(value: number): string {
  if (!Number.isFinite(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-400";
  if (value < 0) return "text-rose-400";
  return "text-slate-400";
}

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const x = Number(v);
  return `${x >= 0 ? "+" : ""}${x.toFixed(digits)}%`;
}

function fmtPrice(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const x = Number(v);
  if (x >= 1000) return Math.round(x).toLocaleString();
  if (x >= 100) return x.toFixed(1);
  return x.toFixed(2);
}

function gapTvSymbolOnly(full: string | null | undefined): string {
  if (!full) return "—";
  const s = String(full);
  const i = s.lastIndexOf(":");
  return i >= 0 ? s.slice(i + 1) : s;
}

function gapScanRowSymbol(row: Record<string, string | number | null | undefined>): string {
  const raw = row.ticker ?? row["symbol"];
  return gapTvSymbolOnly(raw != null ? String(raw) : null);
}

function gapRowNum(row: Record<string, string | number | null | undefined>, key: string): number | null {
  const v = row[key];
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : parseFloat(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function gapFmtPctSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(Number(v))) return "—";
  const x = Number(v);
  return `${x >= 0 ? "+" : ""}${x.toFixed(digits)}%`;
}

function gapFmtVolShares(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(Number(n))) return "—";
  const v = Number(n);
  const a = Math.abs(v);
  if (a >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
  if (a >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(v);
}

function gapFmtMktCapLines(mc: number | null | undefined): { main: string; tier: string } {
  if (mc == null || !Number.isFinite(Number(mc)) || Number(mc) <= 0) return { main: "—", tier: "" };
  const dollars = Number(mc);
  const b = dollars / 1e9;
  let tier = "Small";
  if (b >= 10) tier = "Large";
  else if (b >= 2) tier = "Mid";
  const main = b >= 1 ? `${b.toFixed(1)}B` : `${(dollars / 1e6).toFixed(1)}M`;
  return { main, tier };
}

function formatEt(iso?: string | null): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return `${new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }).format(d)} ET`;
  } catch {
    return iso;
  }
}

function numOr0(s: string): number {
  const x = parseFloat(String(s).replace(/,/g, ""));
  return Number.isFinite(x) ? x : 0;
}

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function VixFearGaugeLite({ close, changePct }: { close: number | null | undefined; changePct: number | null | undefined }) {
  const min = 10;
  const max = 40;
  const v = close != null && Number.isFinite(close) ? close : null;
  const t = v == null ? 0 : (clamp(v, min, max) - min) / (max - min);
  const pct = Math.round(t * 100);
  const mood =
    v != null && v < 16
      ? { label: "Complacent", cls: "text-emerald-300" }
      : v != null && v < 22
        ? { label: "Calm", cls: "text-cyan-300" }
        : v != null && v < 28
          ? { label: "Elevated", cls: "text-amber-300" }
          : { label: "Fear", cls: "text-rose-300" };

  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">VIX Fear Gauge</h3>
      </header>
      <div className="px-4 py-3">
        <div className="flex items-baseline justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[10px] text-slate-600">CBOE:VIX</p>
            <p className={`mt-0.5 font-mono text-2xl font-extrabold tabular-nums ${mood.cls}`}>{v == null ? "—" : v.toFixed(2)}</p>
          </div>
          <div className="text-right">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Expected move</p>
            <p className="mt-0.5 font-mono text-sm font-bold tabular-nums text-slate-200">{v == null ? "—" : `±${(v / 16).toFixed(2)}%`}</p>
            <p className={`mt-1 font-mono text-[11px] tabular-nums ${pctClass(changePct ?? 0)}`}>
              {changePct != null && changePct >= 0 ? "+" : ""}
              {changePct == null || !Number.isFinite(changePct) ? "—" : `${changePct.toFixed(2)}%`}
            </p>
          </div>
        </div>
        <div className="mt-3">
          <div className="mb-2 flex items-center justify-between text-[10px] text-slate-600">
            <span>{min}</span>
            <span className={mood.cls}>{mood.label}</span>
            <span>{max}</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800">
            <div
              className="h-full bg-gradient-to-r from-emerald-500 via-cyan-400 via-amber-400 to-rose-500"
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function MarketRegimeCard({
  state,
  message,
}: {
  state: "bullish" | "neutral" | "bearish" | undefined;
  message?: string;
}) {
  const tone =
    state === "bullish"
      ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-100"
      : state === "neutral"
        ? "border-cyan-500/25 bg-cyan-500/10 text-cyan-100"
        : "border-rose-500/25 bg-rose-500/10 text-rose-50";
  const title = state === "bullish" ? "BULLISH" : state === "neutral" ? "NEUTRAL" : state === "bearish" ? "BEARISH" : "—";
  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Market Regime</h3>
      </header>
      <div className="px-4 py-3">
        <div className={`rounded-lg border px-3 py-3 ${tone}`}>
          <p className="text-[10px] font-semibold uppercase tracking-wider opacity-90">{title}</p>
          <p className="mt-1 text-[12px] font-semibold leading-snug">{message ?? "—"}</p>
        </div>
      </div>
    </div>
  );
}

function LiquidityFlowCard({ summary }: { summary: ApiPayload["marketFlowSummary"] | undefined }) {
  const dv = summary?.aggregateDollarVolume ?? null;
  const prev = summary?.previousAggregateDollarVolume ?? null;
  const avg20 = summary?.aggregateAvg20DollarVolume ?? null;
  const trend = summary?.flowTrend ?? null;
  const cls = trend === "up" ? "text-emerald-300" : trend === "down" ? "text-rose-300" : "text-slate-300";
  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Liquidity &amp; Flow</h3>
      </header>
      <div className="grid grid-cols-2 gap-3 px-4 py-3 text-[11px]">
        <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Aggregate $ Vol</p>
          <p className="mt-0.5 font-mono text-sm font-bold tabular-nums text-slate-200">{dv == null ? "—" : formatMoney(dv)}</p>
          <p className="mt-0.5 text-[10px] text-slate-600">
            Prev: <span className="font-mono text-slate-400">{prev == null ? "—" : formatMoney(prev)}</span>
          </p>
        </div>
        <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Trend</p>
          <p className={`mt-0.5 text-sm font-bold ${cls}`}>{trend === "up" ? "Flow improving" : trend === "down" ? "Flow fading" : "—"}</p>
          <p className="mt-0.5 text-[10px] text-slate-600">
            20D avg: <span className="font-mono text-slate-400">{avg20 == null ? "—" : formatMoney(avg20)}</span>
          </p>
        </div>
      </div>
    </div>
  );
}

function RsSnapshot({ rows }: { rows: { theme: string; rs: number }[] }) {
  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">RS Snapshot</h3>
        <p className="mt-0.5 text-[10px] text-slate-600">Top themes by 1M RS</p>
      </header>
      <div className="h-[240px] px-2 py-2">
        {rows.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={rows} layout="vertical" margin={{ top: 6, right: 12, bottom: 6, left: 6 }}>
              <XAxis type="number" domain={["dataMin", "dataMax"]} hide />
              <YAxis type="category" dataKey="theme" width={120} tick={{ fill: "#94a3b8", fontSize: 10 }} />
              <Tooltip
                cursor={{ fill: "rgba(148,163,184,0.08)" }}
                contentStyle={{ background: "#0b1220", border: "1px solid rgba(51,65,85,0.8)", borderRadius: 10 }}
                labelStyle={{ color: "#e2e8f0" }}
                formatter={(value: unknown) => [typeof value === "number" ? value.toFixed(1) : "—", "RS"]}
              />
              <Bar dataKey="rs" fill="#22d3ee" radius={[6, 6, 6, 6]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="px-3 py-10 text-center text-xs text-slate-500">No RS data yet.</div>
        )}
      </div>
    </div>
  );
}

function GapGradeBadge({ grade }: { grade: "A" | "B" | "C" }) {
  const ring =
    grade === "A"
      ? "border-emerald-500/60 bg-emerald-500/20 text-emerald-300"
      : grade === "B"
        ? "border-sky-500/55 bg-sky-500/15 text-sky-200"
        : "border-slate-500/60 bg-slate-800/80 text-slate-300";
  return (
    <span
      className={`inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-[10px] font-bold leading-none ${ring}`}
      title={grade === "A" ? "Scanner A+" : grade === "B" ? "Scanner A" : "Not in scanner audit / ungraded"}
    >
      {grade}
    </span>
  );
}

function useThemesPayload() {
  const [payload, setPayload] = useState<ApiPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState<number>(110_000);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const res = await fetch(`${API_BASE_URL}/api/themes?view=scanner`);
        const retryAfterRaw = res.headers.get("Retry-After");
        if (!res.ok) {
          if (res.status === 429 && retryAfterRaw) {
            const ra = Number(retryAfterRaw);
            if (Number.isFinite(ra) && ra > 0) setPollMs(Math.min(8 * 60_000, Math.max(110_000, Math.round(ra * 1000))));
          }
          throw new Error(`themes ${res.status}`);
        }
        const data = (await res.json()) as ApiPayload;
        if (!active) return;
        setPayload(data);
        setError(null);
        if (data.polling?.pollSeconds) {
          const n = Number(data.polling.pollSeconds);
          if (Number.isFinite(n) && n > 0) setPollMs(Math.min(8 * 60_000, Math.max(110_000, Math.round(n * 1000))));
        }
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : "Failed to load themes");
      }
    }
    void load();
    const id = window.setInterval(() => void load(), pollMs);
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, [pollMs]);

  return { payload, error };
}

function TapeStrip({ tape }: { tape: { label: string; symbol: string; close: number | null; change_pct: number | null }[] | undefined }) {
  if (!tape?.length) return null;
  return (
    <div className="shrink-0 border-b border-terminal-border bg-terminal-bg px-4 py-2">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 text-[11px]">
        {tape.slice(0, 8).map((t) => (
          <div key={`${t.symbol}-${t.label}`} className="flex items-baseline gap-2">
            <span className="font-mono text-slate-400">{t.label}</span>
            <span className="font-mono font-semibold text-slate-200 tabular-nums">{t.close == null ? "—" : fmtPrice(t.close)}</span>
            <span className={`font-mono tabular-nums ${pctClass(t.change_pct ?? 0)}`}>{fmtPct(t.change_pct, 2)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function usePremarketBrief() {
  const [brief, setBrief] = useState<PremarketBrief | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetch(`${API_BASE_URL}/api/news/premarket`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`premarket ${r.status}`);
        return (await r.json()) as PremarketBrief;
      })
      .then((data) => {
        if (!active) return;
        setBrief(data);
      })
      .catch(() => {
        if (!active) return;
        setBrief(null);
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const refresh = useCallback(() => {
    setLoading(true);
    fetch(`${API_BASE_URL}/api/news/premarket/refresh`, { method: "POST" })
      .then(async (r) => {
        if (!r.ok) throw new Error(`premarket refresh ${r.status}`);
        return (await r.json()) as PremarketBrief;
      })
      .then((data) => setBrief(data))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  return { brief, loading, refresh };
}

function usePremarketGappers(filters: {
  min_gap_pct: string;
  min_pm_vol_k: string;
  min_price: string;
  min_avg_vol_10d_k: string;
  min_mkt_cap_b: string;
  min_avg_dollar_vol_m: string;
}) {
  const [data, setData] = useState<PremarketGappersPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadRef = useRef<() => Promise<void>>(async () => {});

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const p = new URLSearchParams();
      p.set("min_gap_pct", String(numOr0(filters.min_gap_pct)));
      p.set("min_pm_vol_k", String(numOr0(filters.min_pm_vol_k)));
      p.set("min_price", String(numOr0(filters.min_price)));
      p.set("min_avg_vol_10d_k", String(numOr0(filters.min_avg_vol_10d_k)));
      p.set("min_mkt_cap_b", String(numOr0(filters.min_mkt_cap_b)));
      p.set("min_avg_dollar_vol_m", String(numOr0(filters.min_avg_dollar_vol_m)));
      p.set("limit", "100");
      const r = await fetch(`${API_BASE_URL}/api/scanner/premarket-gappers?${p.toString()}`);
      if (!r.ok) throw new Error(`gap scan ${r.status}`);
      const payload = (await r.json()) as PremarketGappersPayload;
      setData(payload);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load premarket gap scan");
    } finally {
      setLoading(false);
    }
  }, [filters]);

  loadRef.current = load;

  useEffect(() => {
    void loadRef.current();
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => void loadRef.current(), 60_000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    const t = window.setTimeout(() => void loadRef.current(), 450);
    return () => window.clearTimeout(t);
  }, [filters.min_gap_pct, filters.min_pm_vol_k, filters.min_price, filters.min_avg_vol_10d_k, filters.min_mkt_cap_b, filters.min_avg_dollar_vol_m]);

  return { data, loading, error, reload: load };
}

function useTickerIntel(ticker: string, open: boolean) {
  const [intel, setIntel] = useState<TickerIntel | null>(null);
  const [intelLoading, setIntelLoading] = useState(false);
  const [suggest, setSuggest] = useState<TickerSuggestRow[]>([]);
  const [suggestLoading, setSuggestLoading] = useState(false);

  useEffect(() => {
    const q = ticker.trim();
    if (!q) {
      setIntel(null);
      setIntelLoading(false);
      return;
    }
    const handle = window.setTimeout(() => {
      setIntelLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(q)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-intel ${r.status}`);
          return (await r.json()) as TickerIntel;
        })
        .then((data) => setIntel(data))
        .catch(() => setIntel(null))
        .finally(() => setIntelLoading(false));
    }, 220);
    return () => window.clearTimeout(handle);
  }, [ticker]);

  useEffect(() => {
    if (!open) return;
    const q = ticker.trim();
    if (!q) {
      setSuggest([]);
      setSuggestLoading(false);
      return;
    }
    let active = true;
    const handle = window.setTimeout(() => {
      setSuggestLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-suggest?q=${encodeURIComponent(q)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-suggest ${r.status}`);
          return (await r.json()) as { results: TickerSuggestRow[] };
        })
        .then((data) => {
          if (!active) return;
          setSuggest(Array.isArray(data.results) ? data.results : []);
        })
        .catch(() => {
          if (!active) return;
          setSuggest([]);
        })
        .finally(() => {
          if (!active) return;
          setSuggestLoading(false);
        });
    }, 140);
    return () => {
      active = false;
      window.clearTimeout(handle);
    };
  }, [ticker, open]);

  return { intel, intelLoading, suggest, suggestLoading };
}

function useUniverseSpotlight(label: string | null) {
  const [data, setData] = useState<ThemeUniverseSpotlight | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!label) {
      setData(null);
      setLoading(false);
      return;
    }
    let active = true;
    setLoading(true);
    fetch(`${API_BASE_URL}/api/theme-universe/spotlight?label=${encodeURIComponent(label)}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`spotlight ${r.status}`);
        return (await r.json()) as ThemeUniverseSpotlight;
      })
      .then((d) => {
        if (!active) return;
        setData(d);
      })
      .catch(() => {
        if (!active) return;
        setData(null);
      })
      .finally(() => {
        if (!active) return;
        setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [label]);

  return { data, loading };
}

function ScannerView({
  payload,
  spotlightThemeName,
  setSpotlightThemeName,
  themeQuery,
  setThemeQuery,
}: {
  payload: ApiPayload;
  spotlightThemeName: string | null;
  setSpotlightThemeName: (s: string) => void;
  themeQuery: string;
  setThemeQuery: (s: string) => void;
}) {
  const themes = payload.themes ?? [];
  const sortedThemes = useMemo(() => {
    const score = (t: ApiTheme) => (t.relativeStrength1M ?? t.relativeStrengthQualifierRatio) ?? -Infinity;
    const list = [...themes];
    list.sort((a, b) => score(b) - score(a));
    return list;
  }, [themes]);

  const filteredThemes = useMemo(() => {
    const q = themeQuery.trim().toLowerCase();
    if (!q) return sortedThemes;
    return sortedThemes.filter((t) => t.theme.toLowerCase().includes(q) || (t.sector ?? "").toLowerCase().includes(q));
  }, [sortedThemes, themeQuery]);

  const rsRows = useMemo(() => {
    return sortedThemes
      .map((t) => ({ theme: t.theme, rs: t.relativeStrength1M ?? t.relativeStrengthQualifierRatio }))
      .filter((r) => Number.isFinite(r.rs))
      .slice(0, 18);
  }, [sortedThemes]);

  useEffect(() => {
    if (!spotlightThemeName && sortedThemes[0]?.theme) setSpotlightThemeName(sortedThemes[0].theme);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortedThemes.length]);

  const spotlightTheme = useMemo(() => {
    if (!spotlightThemeName) return filteredThemes[0] ?? sortedThemes[0] ?? null;
    return themes.find((t) => t.theme === spotlightThemeName) ?? filteredThemes[0] ?? sortedThemes[0] ?? null;
  }, [themes, spotlightThemeName, filteredThemes, sortedThemes]);

  const { data: uniSpotlight } = useUniverseSpotlight(spotlightTheme?.theme ?? null);
  const constituents = useMemo(() => {
    const list = spotlightTheme?.stocks ?? [];
    if (!list.length) return [];
    const scored = list
      .map((s) => ({
        ticker: s.ticker,
        close: s.close,
        today: s.today_return_pct ?? null,
        adr: s.adr_pct,
        addv: s.avg_dollar_volume,
        grade: s.gradeLabel,
      }))
      .filter((x) => x.ticker);
    scored.sort((a, b) => {
      const ga = a.grade === "A+" ? 2 : a.grade === "A" ? 1 : 0;
      const gb = b.grade === "A+" ? 2 : b.grade === "A" ? 1 : 0;
      if (gb !== ga) return gb - ga;
      return (b.addv ?? 0) - (a.addv ?? 0);
    });
    return scored.slice(0, 12);
  }, [spotlightTheme]);

  return (
    <div className="flex h-full min-h-0 w-full min-w-0 flex-1 flex-row gap-3 overflow-hidden">
      <div className="flex w-[360px] min-w-[360px] shrink-0 flex-col gap-3 overflow-y-auto pr-1">
        <MarketRegimeCard state={payload.market_momentum_score?.state} message={payload.market_momentum_score?.message} />
        <VixFearGaugeLite close={payload.vix?.close} changePct={payload.vix?.change_pct} />
        <LiquidityFlowCard summary={payload.marketFlowSummary} />
      </div>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 overflow-hidden">
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-3">
          <div className="xl:col-span-2 min-h-0">
            <section className="flex min-h-0 flex-1 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
              <header className="flex flex-wrap items-center justify-between gap-3 border-b border-terminal-border px-4 py-3">
                <div className="min-w-0">
                  <h2 className="truncate text-sm font-semibold text-white">Theme Leaderboard</h2>
                  <p className="truncate text-[11px] text-slate-500">1D / 1W / 1M / 3M / 6M · RS 1M · Qual% · Leaders</p>
                </div>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-2 rounded-md border border-terminal-border bg-terminal-bg px-2 py-2">
                    <Search className="h-4 w-4 text-slate-500" aria-hidden />
                    <input
                      value={themeQuery}
                      onChange={(e) => setThemeQuery(e.target.value)}
                      placeholder="Search themes…"
                      className="w-[200px] bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none"
                    />
                  </div>
                </div>
              </header>
              <div className="fintech-scroll min-h-0 flex-1 overflow-auto">
                <table className="w-full min-w-[980px] border-separate border-spacing-0 text-left text-[11px]">
                  <thead>
                    <tr>
                      {["Theme", "1D", "1W", "1M", "3M", "6M", "RS 1M", "Qual%", "Leaders"].map((h) => (
                        <th
                          key={h}
                          className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500"
                        >
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredThemes.slice(0, 120).map((t) => {
                      const rs = t.relativeStrength1M ?? null;
                      const qRatio = Number.isFinite(t.relativeStrengthQualifierRatio) ? t.relativeStrengthQualifierRatio : null;
                      return (
                        <tr
                          key={`${t.theme}|${t.sector ?? ""}`}
                          className={`cursor-pointer border-b border-terminal-border/60 hover:bg-terminal-elevated/40 ${
                            spotlightTheme?.theme === t.theme ? "bg-terminal-elevated/30" : ""
                          }`}
                          onClick={() => setSpotlightThemeName(t.theme)}
                          title="Click to spotlight"
                        >
                          <td className="px-3 py-2">
                            <div className="min-w-0">
                              <p className="truncate font-medium text-slate-100">{t.theme}</p>
                              <p className="truncate text-[10px] text-slate-600">
                                {t.qualifiedCount}/{t.totalCount} · {t.sector ?? "—"} · {formatMoney(t.themeDollarVolume)}
                              </p>
                            </div>
                          </td>
                          {[t.perf1D, t.perf1W, t.perf1M, t.perf3M, t.perf6M].map((v, i) => (
                            <td
                              key={i}
                              className={`px-3 py-2 text-right font-mono tabular-nums ${
                                v == null || !Number.isFinite(v) ? "text-slate-600" : pctClass(v)
                              }`}
                            >
                              {fmtPct(v, 2)}
                            </td>
                          ))}
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-200">{rs == null || !Number.isFinite(rs) ? "—" : rs.toFixed(1)}</td>
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-200">{qRatio == null ? "—" : `${(qRatio * 100).toFixed(0)}%`}</td>
                          <td className="px-3 py-2 text-right font-mono text-slate-300">{(t.leaders ?? []).slice(0, 4).join(", ") || "—"}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <div className="flex min-h-0 flex-col gap-3">
            <RsSnapshot rows={rsRows} />
            <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
              <header className="border-b border-terminal-border px-4 py-3">
                <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Thematic Spotlight</h3>
                <p className="mt-0.5 truncate text-[10px] text-slate-600">{spotlightTheme?.theme ?? "—"}</p>
              </header>
              <div className="px-4 py-3">
                {!spotlightTheme ? (
                  <p className="text-xs text-slate-500">No theme selected.</p>
                ) : (
                  <>
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <p className="text-[12px] font-semibold text-white">{spotlightTheme.theme}</p>
                      <p className="font-mono text-[11px] text-slate-400 tabular-nums">
                        RS {spotlightTheme.relativeStrength1M == null || !Number.isFinite(spotlightTheme.relativeStrength1M) ? "—" : spotlightTheme.relativeStrength1M.toFixed(1)}
                      </p>
                    </div>
                    <p className="mt-1 text-[10px] text-slate-600">
                      Leaders: <span className="font-mono text-slate-300">{(spotlightTheme.leaders ?? []).slice(0, 6).join(", ") || "—"}</span>
                    </p>
                    <div className="mt-3 grid grid-cols-2 gap-2">
                      {(uniSpotlight?.best ?? []).slice(0, 4).map((x) => (
                        <div key={`b-${x.ticker}`} className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-2.5 py-2">
                          <div className="flex items-baseline justify-between gap-2">
                            <span className="font-mono text-[12px] font-semibold text-accent">{x.ticker}</span>
                            <span className={`font-mono text-[11px] tabular-nums ${pctClass(x.today_return_pct)}`}>
                              {x.today_return_pct >= 0 ? "+" : ""}
                              {x.today_return_pct.toFixed(2)}%
                            </span>
                          </div>
                        </div>
                      ))}
                      {!(uniSpotlight?.best?.length) ? (
                        <div className="col-span-2 rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2 text-xs text-slate-500">
                          No mover data.
                        </div>
                      ) : null}
                    </div>
                  </>
                )}
              </div>
            </div>

            <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
              <header className="border-b border-terminal-border px-4 py-3">
                <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">Leader Constituents</h3>
                <p className="mt-0.5 text-[10px] text-slate-600">Top names (grade + liquidity)</p>
              </header>
              <div className="px-4 py-3">
                {constituents.length ? (
                  <div className="grid grid-cols-2 gap-2">
                    {constituents.map((c) => {
                      const href = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(`NASDAQ:${c.ticker}`)}`;
                      return (
                        <a
                          key={c.ticker}
                          href={href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-2.5 py-2 hover:border-slate-600"
                        >
                          <div className="flex items-baseline justify-between gap-2">
                            <span className="font-mono text-[12px] font-semibold text-accent">{c.ticker}</span>
                            <span className="text-[10px] font-semibold text-slate-500">{c.grade}</span>
                          </div>
                          <div className="mt-1 flex items-baseline justify-between gap-2">
                            <span className="font-mono text-[11px] text-slate-300">{fmtPrice(c.close)}</span>
                            <span className={`font-mono text-[11px] tabular-nums ${pctClass(c.today ?? 0)}`}>{fmtPct(c.today, 2)}</span>
                          </div>
                          <p className="mt-1 text-[9px] text-slate-600">ADDV {formatMoney(c.addv)}</p>
                        </a>
                      );
                    })}
                  </div>
                ) : (
                  <p className="text-xs text-slate-500">No constituents available for this theme.</p>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function PremarketBriefAside({ brief, loading, onGenerate }: { brief: PremarketBrief | null; loading: boolean; onGenerate: () => void }) {
  return (
    <aside className="fintech-scroll flex h-full min-h-0 w-[380px] min-w-[380px] shrink-0 flex-col overflow-y-auto border-r border-terminal-border bg-terminal-bg p-4">
      <section className="flex min-h-0 flex-1 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
        <header className="shrink-0 border-b border-terminal-border px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
          Pre-market brief
        </header>
        <div className="fintech-scroll min-h-0 flex-1 overflow-y-auto p-3">
          {loading ? (
            <p className="text-xs text-slate-500">Loading pre-market brief…</p>
          ) : brief?.narrative?.length || brief?.sections?.length ? (
            <article className="space-y-2 text-[12px] leading-snug text-slate-300">
              <header className="flex items-center justify-between gap-3 rounded-lg border border-terminal-border bg-terminal-bg/50 px-2.5 py-2">
                <div className="min-w-0">
                  <p className="flex items-center gap-1.5 truncate text-[11px] font-semibold uppercase tracking-wider text-slate-300">
                    <Sunrise className="h-4 w-4 shrink-0 text-amber-300" aria-hidden />
                    <span className="truncate">Pre-market brief</span>
                  </p>
                  <p className="truncate text-[10px] text-slate-600">
                    Generated <span className="font-mono text-slate-400">{brief.generated_at_utc ?? "—"}</span>
                  </p>
                </div>
                <div className="shrink-0 rounded-md border border-terminal-border bg-terminal-bg px-2 py-1">
                  <p className="text-[9px] font-semibold uppercase tracking-wider text-slate-500">Release</p>
                  <p className="font-mono text-[11px] text-slate-300">{brief.scheduled_for_et ?? "8:03am ET"}</p>
                </div>
              </header>
              {(brief.narrative ?? []).map((p, i) => (
                <p key={i} className="text-slate-300">
                  {p}
                </p>
              ))}
              {(brief.sections ?? []).map((sec) => (
                <section key={sec.title} className="space-y-1">
                  <h3 className="text-[10px] font-semibold uppercase tracking-wider text-slate-600">{sec.title}</h3>
                  {sec.bullets.map((b, j) => (
                    <p key={`${sec.title}-${j}`} className="text-slate-300">
                      {b}
                    </p>
                  ))}
                </section>
              ))}
            </article>
          ) : (
            <div className="space-y-2">
              <p className="text-xs text-slate-500">
                No pre-market brief yet. Scheduled <span className="text-slate-300">8:03am ET</span>.
              </p>
              <button
                type="button"
                className="rounded-md border border-terminal-border bg-terminal-bg px-3 py-2 text-xs font-semibold text-slate-300 hover:border-accent/40 hover:text-white"
                onClick={onGenerate}
              >
                Generate now
              </button>
            </div>
          )}
        </div>
      </section>
    </aside>
  );
}

function GappersView({
  gappers,
  loading,
  error,
  filters,
  setFilters,
  gapScannerGradeByTicker,
}: {
  gappers: PremarketGappersPayload | null;
  loading: boolean;
  error: string | null;
  filters: {
    min_gap_pct: string;
    min_pm_vol_k: string;
    min_price: string;
    min_avg_vol_10d_k: string;
    min_mkt_cap_b: string;
    min_avg_dollar_vol_m: string;
  };
  setFilters: (next: typeof filters) => void;
  gapScannerGradeByTicker: Map<string, "A" | "B" | "C">;
}) {
  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-terminal-bg p-4">
      <section className="mb-3 w-full shrink-0 rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
        <p className="mb-3 text-[10px] font-semibold uppercase tracking-wider text-amber-200/90">
          Pre-market screener · TradingView scanner (america)
        </p>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {(
            [
              ["Min gap (%)", "min_gap_pct"],
              ["Min PM vol (K)", "min_pm_vol_k"],
              ["Min price ($)", "min_price"],
              ["Min avg vol 10d (K)", "min_avg_vol_10d_k"],
              ["Min mkt cap ($B)", "min_mkt_cap_b"],
              ["Min avg $ vol ($M)", "min_avg_dollar_vol_m"],
            ] as const
          ).map(([label, key]) => (
            <label key={key} className="flex flex-col gap-1">
              <span className="text-[10px] font-semibold uppercase tracking-wide text-amber-200/85">{label}</span>
              <input
                type="text"
                inputMode="decimal"
                value={filters[key]}
                onChange={(e) => setFilters({ ...filters, [key]: e.target.value })}
                className="w-full rounded-md border border-terminal-border bg-terminal-bg px-2 py-1.5 font-mono text-xs text-slate-200 placeholder:text-slate-600 focus:border-accent/50 focus:outline-none"
                placeholder="0"
              />
            </label>
          ))}
        </div>
        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-terminal-border/80 pt-3">
          <button
            type="button"
            onClick={() =>
              setFilters({
                min_gap_pct: "0",
                min_pm_vol_k: "0",
                min_price: "0",
                min_avg_vol_10d_k: "0",
                min_mkt_cap_b: "0",
                min_avg_dollar_vol_m: "0",
              })
            }
            className="rounded-md border border-terminal-border bg-terminal-bg px-3 py-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400 transition-colors hover:border-slate-500 hover:text-slate-200"
          >
            Reset
          </button>
          <p className="text-right text-[10px] leading-relaxed text-slate-500">
            <span className="text-slate-400">Scanned (ET):</span>{" "}
            <span className="font-mono text-slate-400">{formatEt(gappers?.fetched_at_utc)}</span>
            <span className="mx-1.5 text-slate-600">·</span>
            <span>
              {gappers?.row_count ?? 0}
              {gappers?.total_matched_scanner != null ? ` / ${gappers.total_matched_scanner} matched` : ""} shown
            </span>
          </p>
        </div>
      </section>

      <section className="mb-0 flex min-h-0 min-w-0 flex-1 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
        <header className="shrink-0 flex flex-wrap items-center justify-between gap-2 border-b border-terminal-border px-4 py-3">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold text-white">Pre-market Gappers</h2>
            <p className="mt-0.5 max-w-[52rem] text-[10px] leading-snug text-slate-500">
              Sorted by premarket gap (desc). Grade column uses scanner audit (A+/A mapped to A/B).
            </p>
          </div>
        </header>

        <div className="fintech-scroll min-h-0 min-w-0 flex-1 overflow-auto px-2 pb-3 pt-2">
          {loading && !(gappers?.rows && gappers.rows.length > 0) ? (
            <p className="px-2 py-8 text-center text-xs text-slate-500">Loading gap scan…</p>
          ) : error ? (
            <p className="px-2 py-8 text-center text-xs text-rose-400">{error}</p>
          ) : !gappers?.rows?.length ? (
            <p className="px-2 py-8 text-center text-xs text-slate-500">No rows — tighten filters or try again.</p>
          ) : (
            <table className="w-full min-w-[1180px] border-separate border-spacing-0 text-left text-[11px]">
              <thead>
                <tr>
                  {["Ticker", "Premkt %", "Premkt Vol", "Daily %", "ADR%", "MktCap", "Sector", "Industry", "Grade"].map((label) => (
                    <th
                      key={label}
                      className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-2 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500"
                    >
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {gappers.rows.map((row, idx) => {
                  const sym = gapScanRowSymbol(row);
                  const symUs = sym !== "—" ? sym : "";
                  const tvRaw = row.ticker != null && String(row.ticker).trim() !== "" ? String(row.ticker) : symUs ? `NASDAQ:${symUs}` : "";
                  const chartHref = tvRaw ? `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(tvRaw)}` : undefined;
                  const finvizHref = symUs ? `https://finviz.com/quote.ashx?t=${encodeURIComponent(symUs)}` : undefined;
                  const pmGap = gapRowNum(row, "premarket_gap");
                  const pmVol = gapRowNum(row, "premarket_volume");
                  const dailyChg = gapRowNum(row, "change");
                  const adr = gapRowNum(row, "Volatility.D");
                  const mcap = gapRowNum(row, "market_cap_basic");
                  const mcapLines = gapFmtMktCapLines(mcap);
                  const sector = row.sector != null && String(row.sector).trim() ? String(row.sector) : "—";
                  const industry = row.industry != null && String(row.industry).trim() ? String(row.industry) : "—";
                  const grade = symUs ? (gapScannerGradeByTicker.get(symUs.toUpperCase()) ?? "C") : "C";
                  return (
                    <tr
                      key={`${symUs || "row"}-${idx}`}
                      className={`transition-colors hover:bg-terminal-bg/55 ${idx % 2 === 1 ? "bg-terminal-bg/35" : ""}`}
                    >
                      <td className="border-b border-terminal-border/60 px-2 py-2 align-middle text-slate-200">
                        <div className="flex items-center gap-1.5">
                          {chartHref ? (
                            <a
                              href={chartHref}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="rounded p-0.5 text-emerald-400 hover:bg-emerald-500/15"
                              title="Open chart"
                            >
                              <Plus className="h-3.5 w-3.5" strokeWidth={2.5} />
                            </a>
                          ) : (
                            <span className="inline-flex h-5 w-5 items-center justify-center text-slate-600">
                              <Plus className="h-3.5 w-3.5" />
                            </span>
                          )}
                          {finvizHref ? (
                            <a
                              href={finvizHref}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="rounded p-0.5 text-slate-400 hover:bg-slate-700/50 hover:text-white"
                              title="Quote / fundamentals"
                            >
                              <Info className="h-3.5 w-3.5" />
                            </a>
                          ) : (
                            <span className="inline-flex text-slate-600">
                              <Info className="h-3.5 w-3.5" />
                            </span>
                          )}
                          {chartHref ? (
                            <a href={chartHref} target="_blank" rel="noopener noreferrer" className="font-mono font-semibold text-accent hover:underline">
                              {sym}
                            </a>
                          ) : (
                            <span className="font-mono font-semibold text-slate-300">{sym}</span>
                          )}
                        </div>
                      </td>
                      <td className={`border-b border-terminal-border/60 px-2 py-2 text-center tabular-nums ${pctClass(pmGap ?? 0)}`}>
                        {gapFmtPctSigned(pmGap, 2)}
                      </td>
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-center tabular-nums text-slate-300">{gapFmtVolShares(pmVol)}</td>
                      <td className={`border-b border-terminal-border/60 px-2 py-2 text-center tabular-nums ${pctClass(dailyChg ?? 0)}`}>{gapFmtPctSigned(dailyChg, 2)}</td>
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-center tabular-nums text-slate-300">{adr != null ? `${adr.toFixed(1)}%` : "—"}</td>
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-center text-slate-300">
                        <div className="flex flex-col items-center leading-tight">
                          <span className="tabular-nums">{mcapLines.main}</span>
                          {mcapLines.tier ? <span className="text-[9px] font-medium text-slate-500">{mcapLines.tier}</span> : null}
                        </div>
                      </td>
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-slate-300">{sector}</td>
                      <td className="max-w-[10rem] border-b border-terminal-border/60 px-2 py-2 text-slate-400">
                        <span className="line-clamp-2 leading-snug">{industry}</span>
                      </td>
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-center align-middle">
                        <div className="flex justify-center">
                          <GapGradeBadge grade={grade} />
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </div>
  );
}

export function ThemeDashboard() {
  const [tab, setTab] = useState<"scanner" | "gappers">("scanner");
  const { payload, error } = useThemesPayload();
  const { brief, loading: briefLoading, refresh: refreshBrief } = usePremarketBrief();

  const [themeQuery, setThemeQuery] = useState("");
  const [spotlightThemeName, setSpotlightThemeName] = useState<string | null>(null);

  const [tickerQuery, setTickerQuery] = useState("");
  const [tickerIntelOpen, setTickerIntelOpen] = useState(false);
  const { intel, intelLoading, suggest, suggestLoading } = useTickerIntel(tickerQuery, tickerIntelOpen);

  const [gapFilters, setGapFilters] = useState({
    min_gap_pct: "0",
    min_pm_vol_k: "0",
    min_price: "0",
    min_avg_vol_10d_k: "0",
    min_mkt_cap_b: "0",
    min_avg_dollar_vol_m: "0",
  });
  const { data: gappers, loading: gappersLoading, error: gappersError } = usePremarketGappers(gapFilters);

  const gapScannerGradeByTicker = useMemo(() => {
    const m = new Map<string, "A" | "B" | "C">();
    for (const th of payload?.themes ?? []) {
      for (const s of th.stocks ?? []) {
        const tk = String(s.ticker || "").trim().toUpperCase();
        if (!tk) continue;
        let g: "A" | "B" | "C" = "C";
        if (s.gradeLabel === "A+") g = "A";
        else if (s.gradeLabel === "A") g = "B";
        m.set(tk, g);
      }
    }
    return m;
  }, [payload]);

  if (error) {
    return <div className="flex h-full items-center justify-center bg-terminal-bg p-6 text-rose-300">Failed to load dashboard: {error}</div>;
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden bg-terminal-bg text-slate-200">
      <header className="shrink-0 border-b border-terminal-border bg-terminal-elevated px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex min-w-0 items-center gap-2.5">
            <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-accent/15 text-accent">
              <LayoutGrid className="h-4 w-4" aria-hidden />
            </span>
            <div className="min-w-0">
              <h1 className="truncate text-sm font-semibold tracking-tight text-white">Thematic Scanner</h1>
              <p className="truncate text-[11px] text-slate-500">Pure-play clusters · 1M RS · Finviz · yfinance · VIX</p>
            </div>
          </div>
          <div className="flex items-center gap-2" />
        </div>
      </header>

      <TapeStrip tape={payload?.tape} />

      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        <div className="shrink-0 border-b border-terminal-border bg-terminal-bg px-4 py-3">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setTab("scanner")}
              className={`rounded-full border px-4 py-2 text-[11px] font-semibold transition-colors ${
                tab === "scanner" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              Thematic Scanner
            </button>
            <button
              type="button"
              onClick={() => setTab("gappers")}
              className={`rounded-full border px-4 py-2 text-[11px] font-semibold transition-colors ${
                tab === "gappers" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              Pre-Market Gappers
            </button>

            <div className="ml-auto flex items-center gap-2">
              <button
                type="button"
                onClick={() => setTickerIntelOpen((v) => !v)}
                className={`rounded-md border px-3 py-2 text-[11px] font-semibold transition-colors ${
                  tickerIntelOpen ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
                }`}
              >
                Ticker Intel
              </button>
            </div>
          </div>
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-row overflow-x-auto overflow-y-hidden">
          <PremarketBriefAside brief={brief} loading={briefLoading} onGenerate={refreshBrief} />

          <div className="flex h-full min-h-0 min-w-0 flex-1 basis-0 flex-col overflow-hidden bg-terminal-bg p-0">
            {tab === "scanner" ? (
              <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden p-4">
                {tickerIntelOpen ? (
                  <section className="mb-3 w-full shrink-0 rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
                    <header className="mb-2 flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <h2 className="text-sm font-semibold text-white">Ticker Intel</h2>
                        <p className="text-[10px] text-slate-500">Finviz / theme mappings / basic tape</p>
                      </div>
                    </header>
                    <div className="flex flex-wrap items-start gap-3">
                      <div className="w-[260px]">
                        <div className="flex items-center gap-2 rounded-md border border-terminal-border bg-terminal-bg px-2 py-2">
                          <Search className="h-4 w-4 text-slate-500" aria-hidden />
                          <input
                            value={tickerQuery}
                            onChange={(e) => setTickerQuery(e.target.value)}
                            placeholder="NVDA, PLTR, CRWD…"
                            className="w-full bg-transparent font-mono text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none"
                          />
                        </div>
                        {suggestLoading ? <p className="mt-2 text-xs text-slate-500">Searching…</p> : null}
                        {suggest.length ? (
                          <div className="mt-2 max-h-[140px] overflow-auto">
                            <ul className="space-y-1">
                              {suggest.slice(0, 12).map((r) => (
                                <li key={r.ticker}>
                                  <button
                                    type="button"
                                    onClick={() => setTickerQuery(r.ticker)}
                                    className="flex w-full items-center justify-between gap-3 rounded-md border border-terminal-border/60 bg-terminal-bg/40 px-2.5 py-2 text-left hover:border-slate-600 hover:bg-terminal-elevated/30"
                                  >
                                    <span className="font-mono text-[12px] font-semibold text-accent">{r.ticker}</span>
                                    <span className="truncate text-[10px] text-slate-500">{r.name}</span>
                                  </button>
                                </li>
                              ))}
                            </ul>
                          </div>
                        ) : null}
                      </div>

                      <div className="min-w-[280px] flex-1">
                        {intelLoading ? (
                          <p className="text-xs text-slate-500">Loading intel…</p>
                        ) : intel ? (
                          <div className="grid grid-cols-2 gap-3">
                            <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
                              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Name</p>
                              <p className="mt-0.5 truncate text-xs text-slate-200">{intel.name}</p>
                              <p className="mt-1 font-mono text-[11px] text-slate-400">
                                {intel.ticker} · {intel.close == null ? "—" : fmtPrice(intel.close)} · {fmtPct(intel.today_return_pct, 2)}
                              </p>
                            </div>
                            <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
                              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Theme</p>
                              <p className="mt-0.5 truncate text-xs text-slate-200">{intel.theme ?? "—"}</p>
                              <p className="mt-1 text-[11px] text-slate-500">Industry: {intel.industry}</p>
                            </div>
                          </div>
                        ) : (
                          <p className="text-xs text-slate-500">Type a ticker to see intel.</p>
                        )}
                      </div>
                    </div>
                  </section>
                ) : null}

                {payload ? (
                  <ScannerView
                    payload={payload}
                    spotlightThemeName={spotlightThemeName}
                    setSpotlightThemeName={(s) => setSpotlightThemeName(s)}
                    themeQuery={themeQuery}
                    setThemeQuery={setThemeQuery}
                  />
                ) : (
                  <div className="flex h-full items-center justify-center text-slate-500">Loading scanner…</div>
                )}
              </div>
            ) : (
              <GappersView
                gappers={gappers}
                loading={gappersLoading}
                error={gappersError}
                filters={gapFilters}
                setFilters={setGapFilters}
                gapScannerGradeByTicker={gapScannerGradeByTicker}
              />
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

