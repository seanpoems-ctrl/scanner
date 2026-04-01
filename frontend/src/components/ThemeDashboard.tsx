import { Fragment, memo, useCallback, useEffect, useMemo, useRef, useState, type JSX } from "react";
import { AlertTriangle, BarChart2, Info, LayoutGrid, Moon, Plus, Search, Sunrise } from "lucide-react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import MarketBreadth from "./MarketBreadth";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim() || "http://127.0.0.1:8000";
const THEMES_CACHE_KEY = "power-theme:themes-cache:v1";
const MIN_AUTO_REFRESH_MS = 10 * 60_000;

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
  ur_candidate?: boolean;
  pullback_candidate?: boolean;
  setup_tag?: string;
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

function makeFallbackPayload(retryAfterSeconds = 110): ApiPayload {
  return {
    vix: { symbol: "VIX", close: 0, change_pct: 0 },
    themes: [],
    tape: [],
    polling: { pollSeconds: retryAfterSeconds, backoffActive: true, retryAfterSeconds },
    market_momentum_score: {
      score: 0,
      state: "bearish",
      message: "Data source cooling down. Showing fallback snapshot.",
      aPlusCount: 0,
      aCount: 0,
    },
  };
}

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

type EarningsNextRow = { ticker: string; earnings_et_iso: string | null };
type TickerNewsRow = {
  date_utc: string | null;
  published_at_utc: string | null;
  event_type: string;
  title: string;
  link: string | null;
  source: string;
};

type TickerDrawerMeta = {
  reasoning?: string | null;
  sector?: string | null;
  industry?: string | null;
  theme?: string | null;
  category?: string | null;
  grade?: string | null;
};

type MarketStatus = {
  now_et_iso: string;
  is_trading_day: boolean;
  premarket_scan_active: boolean;
  session: "closed" | "premarket" | "open" | "post";
  next_open_et_iso: string | null;
  next_close_et_iso: string | null;
};

function useMarketStatus() {
  const [status, setStatus] = useState<MarketStatus | null>(null);
  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const r = await fetch(`${API_BASE_URL}/api/market/status`);
        if (!r.ok) throw new Error(`market status ${r.status}`);
        const data = (await r.json()) as MarketStatus;
        if (!active) return;
        setStatus(data);
      } catch {
        if (!active) return;
        setStatus(null);
      }
    }
    void load();
    const id = window.setInterval(() => void load(), 60_000);
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, []);
  return status;
}

type ThemeUniverseSpotlight = {
  label: string;
  slug: string;
  updated_at: string;
  best: { ticker: string; today_return_pct: number }[];
  worst: { ticker: string; today_return_pct: number }[];
};

type TickerSuggestRow = { ticker: string; name: string };

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

function gapRowStr(row: Record<string, string | number | null | undefined>, key: string): string | null {
  const v = row[key];
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
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

function formatEtClock(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "—";
  try {
    return `${new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(ms))} ET`;
  } catch {
    return "—";
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
    v != null && v < 12
      ? { label: "Low (< 12)", reminder: "Extreme Complacency", color: "#00e676" }
      : v != null && v < 20
        ? { label: "Mid (12-20)", reminder: "Healthy/Normal", color: "#ffee58" }
        : v != null && v < 30
          ? { label: "High (20-30)", reminder: "Elevated Concern", color: "#ff9100" }
          : { label: "Extreme (30-40+)", reminder: "Extreme Panic", color: "#ff1744" };

  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">VIX Fear Gauge</h3>
      </header>
      <div className="px-4 py-3">
        <div className="flex items-baseline justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[10px] text-slate-600">CBOE:VIX</p>
            <p className="mt-0.5 font-mono text-2xl font-extrabold tabular-nums" style={{ color: mood.color }}>
              {v == null ? "—" : v.toFixed(2)}
            </p>
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
            <span className="font-semibold" style={{ color: mood.color }}>
              {mood.label}
            </span>
            <span>{max}</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800">
            <div
              className="h-full"
              style={{ width: `${pct}%`, background: "linear-gradient(to right, #00e676, #ffee58, #ff9100, #ff1744)" }}
            />
          </div>
          <p className="mt-2 text-[10px] font-medium" style={{ color: mood.color }}>
            {mood.reminder}
          </p>
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

const RsSnapshot = memo(function RsSnapshot({ rows }: { rows: { theme: string; rs: number }[] }) {
  return (
    <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="border-b border-terminal-border px-4 py-3">
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">RS Snapshot</h3>
        <p className="mt-0.5 text-[10px] text-slate-600">Top themes by 1M RS</p>
      </header>
      <div className="h-[240px] w-full px-2 py-2">
        {rows.length ? (
          <ResponsiveContainer width="100%" height={240}>
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
});

function SetupBadge({ tag }: { tag: string | null | undefined }) {
  if (!tag) return <span className="text-slate-600">—</span>;
  const cls =
    tag === "EP"
      ? "border-[#2EE59D]/50 bg-[#2EE59D]/15 text-[#2EE59D]"
      : tag === "U&R"
        ? "border-[#B3FF00]/45 bg-[#B3FF00]/12 text-[#B3FF00]"
        : "border-sky-500/45 bg-sky-500/12 text-sky-300";
  return (
    <span
      className={`inline-flex items-center rounded-md border px-1.5 py-0.5 font-mono text-[9px] font-bold leading-none ${cls}`}
      title={tag === "EP" ? "Episodic Pivot: gap>5%, volume>3× avg, near 52w high" : tag === "U&R" ? "Undercut & Reclaim: price broke below key EMA and reclaimed above it" : "Pullback Buy: Stage-2 uptrend touching 10EMA or 20EMA on orderly volume"}
    >
      {tag}
    </span>
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
  const [payload, setPayload] = useState<ApiPayload | null>(() => {
    try {
      const raw = window.localStorage.getItem(THEMES_CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { payload?: ApiPayload };
      return parsed?.payload ?? null;
    } catch {
      return null;
    }
  });
  const [error, setError] = useState<string | null>(null);
  const [pollMs, setPollMs] = useState<number>(MIN_AUTO_REFRESH_MS);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingSince, setLoadingSince] = useState<number | null>(null);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        if (!active) return;
        setLoading(true);
        setLoadingSince(Date.now());
        const res = await fetch(`${API_BASE_URL}/api/themes?view=scanner`);
        const retryAfterRaw = res.headers.get("Retry-After");
        if (!res.ok) {
          if (res.status === 429 && retryAfterRaw) {
            const ra = Number(retryAfterRaw);
            if (Number.isFinite(ra) && ra > 0) setPollMs(Math.min(8 * 60_000, Math.max(MIN_AUTO_REFRESH_MS, Math.round(ra * 1000))));
          }
          // Don't blank the UI on rate-limit or transient errors.
          if (res.status === 429) {
            if (!active) return;
            setError("themes 429");
            setPayload((prev) => prev ?? makeFallbackPayload(retryAfterRaw ? Number(retryAfterRaw) || 110 : 110));
            return;
          }
          throw new Error(`themes ${res.status}`);
        }
        const data = (await res.json()) as ApiPayload;
        if (!active) return;
        setPayload(data);
        setError(null);
        setLastUpdatedAt(Date.now());
        try {
          window.localStorage.setItem(THEMES_CACHE_KEY, JSON.stringify({ payload: data, savedAt: Date.now() }));
        } catch {
          // ignore storage quota/private-mode errors
        }
        if (data.polling?.pollSeconds) {
          const n = Number(data.polling.pollSeconds);
          if (Number.isFinite(n) && n > 0) setPollMs(Math.min(8 * 60_000, Math.max(MIN_AUTO_REFRESH_MS, Math.round(n * 1000))));
        }
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : "Failed to load themes");
      } finally {
        if (!active) return;
        setLoading(false);
        setLoadingSince(null);
      }
    }
    void load();
    const id = window.setInterval(() => void load(), pollMs);
    return () => {
      active = false;
      window.clearInterval(id);
    };
  }, [pollMs]);

  const reload = useCallback(async () => {
    try {
      setLoading(true);
      setLoadingSince(Date.now());
      const res = await fetch(`${API_BASE_URL}/api/themes?view=scanner`);
      const retryAfterRaw = res.headers.get("Retry-After");
      if (!res.ok) {
        if (res.status === 429 && retryAfterRaw) {
          const ra = Number(retryAfterRaw);
          if (Number.isFinite(ra) && ra > 0) setPollMs(Math.min(8 * 60_000, Math.max(MIN_AUTO_REFRESH_MS, Math.round(ra * 1000))));
        }
        if (res.status === 429) {
          setError("themes 429");
          setPayload((prev) => prev ?? makeFallbackPayload(retryAfterRaw ? Number(retryAfterRaw) || 110 : 110));
          return;
        }
        throw new Error(`themes ${res.status}`);
      }
      const data = (await res.json()) as ApiPayload;
      setPayload(data);
      setError(null);
      setLastUpdatedAt(Date.now());
      try {
        window.localStorage.setItem(THEMES_CACHE_KEY, JSON.stringify({ payload: data, savedAt: Date.now() }));
      } catch {
        // ignore storage quota/private-mode errors
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load themes");
    } finally {
      setLoading(false);
      setLoadingSince(null);
    }
  }, []);

  return { payload, error, reload, lastUpdatedAt, loading, loadingSince, pollMs };
}

function useFdvLeaderboard(view: "themes" | "industry") {
  const cacheKey = `power-theme:finviz-leaderboard:${view}:v1`;
  const [payload, setPayload] = useState<ApiPayload | null>(() => {
    try {
      const raw = window.localStorage.getItem(cacheKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { payload?: ApiPayload };
      return parsed?.payload ?? null;
    } catch {
      return null;
    }
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        setLoading(true);
        const res = await fetch(`${API_BASE_URL}/api/themes?view=${encodeURIComponent(view)}`);
        if (!res.ok) {
          if (res.status === 429) {
            if (!active) return;
            setError("finviz 429");
            return;
          }
          throw new Error(`finviz ${res.status}`);
        }
        const data = (await res.json()) as ApiPayload;
        if (!active) return;
        setPayload(data);
        setError(null);
        try {
          window.localStorage.setItem(cacheKey, JSON.stringify({ payload: data, savedAt: Date.now() }));
        } catch {
          // ignore
        }
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : "Failed to load finviz leaderboard");
      } finally {
        if (!active) return;
        setLoading(false);
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, [view, cacheKey]);

  return { payload, loading, error };
}

function TapeInline({ tape }: { tape: { label: string; symbol: string; close: number | null; change_pct: number | null }[] | undefined }) {
  if (!tape?.length) return null;
  return (
    <div className="flex flex-wrap items-center gap-y-2 text-[11px]">
      {tape.slice(0, 8).map((t, idx) => (
        <Fragment key={`${t.symbol}-${t.label}`}>
          {idx > 0 ? <span className="mx-4 h-8 w-px shrink-0 bg-slate-800/80" aria-hidden /> : null}
          <div className="flex min-w-[72px] flex-col items-center justify-center px-1 leading-tight">
            <span className="font-mono text-[10px] font-semibold text-slate-400">{t.label}</span>
            <div className="mt-0.5 flex items-baseline gap-2">
              <span className="font-mono text-[12px] font-semibold text-slate-200 tabular-nums">{t.close == null ? "—" : fmtPrice(t.close)}</span>
              <span className={`font-mono text-[11px] tabular-nums ${pctClass(t.change_pct ?? 0)}`}>{fmtPct(t.change_pct, 2)}</span>
            </div>
          </div>
        </Fragment>
      ))}
    </div>
  );
}

function useTickerAutocomplete(query: string, open: boolean) {
  const [suggest, setSuggest] = useState<TickerSuggestRow[]>([]);
  const [suggestLoading, setSuggestLoading] = useState(false);
  const [activeTicker, setActiveTicker] = useState<string | null>(null);
  const [intel, setIntel] = useState<TickerIntel | null>(null);
  const [intelLoading, setIntelLoading] = useState(false);

  useEffect(() => {
    if (!open) return;
    const q = query.trim();
    if (!q) {
      setSuggest([]);
      setSuggestLoading(false);
      setActiveTicker(null);
      setIntel(null);
      setIntelLoading(false);
      return;
    }

    let alive = true;
    const t = window.setTimeout(() => {
      setSuggestLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-suggest?q=${encodeURIComponent(q)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-suggest ${r.status}`);
          return (await r.json()) as { results: TickerSuggestRow[] };
        })
        .then((data) => {
          if (!alive) return;
          const rows = Array.isArray(data.results) ? data.results : [];
          setSuggest(rows);
          const next = rows?.[0]?.ticker ? String(rows[0].ticker).toUpperCase() : null;
          setActiveTicker(next);
        })
        .catch(() => {
          if (!alive) return;
          setSuggest([]);
          setActiveTicker(null);
        })
        .finally(() => {
          if (!alive) return;
          setSuggestLoading(false);
        });
    }, 140);

    return () => {
      alive = false;
      window.clearTimeout(t);
    };
  }, [query, open]);

  useEffect(() => {
    if (!open) return;
    const tkr = (activeTicker || "").trim();
    if (!tkr) {
      setIntel(null);
      setIntelLoading(false);
      return;
    }

    let alive = true;
    const t = window.setTimeout(() => {
      setIntelLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(tkr)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-intel ${r.status}`);
          return (await r.json()) as TickerIntel;
        })
        .then((data) => {
          if (!alive) return;
          setIntel(data);
        })
        .catch(() => {
          if (!alive) return;
          setIntel(null);
        })
        .finally(() => {
          if (!alive) return;
          setIntelLoading(false);
        });
    }, 120);

    return () => {
      alive = false;
      window.clearTimeout(t);
    };
  }, [activeTicker, open]);

  return { suggest, suggestLoading, activeTicker, setActiveTicker, intel, intelLoading };
}

function useTickerDrawerData(ticker: string | null) {
  const [intel, setIntel] = useState<TickerIntel | null>(null);
  const [intelLoading, setIntelLoading] = useState(false);
  const [news, setNews] = useState<TickerNewsRow[]>([]);
  const [newsLoading, setNewsLoading] = useState(false);
  const [earnings, setEarnings] = useState<EarningsNextRow | null>(null);
  const [earningsLoading, setEarningsLoading] = useState(false);

  useEffect(() => {
    const t = (ticker || "").trim().toUpperCase();
    if (!t) {
      setIntel(null);
      setNews([]);
      setEarnings(null);
      setIntelLoading(false);
      setNewsLoading(false);
      setEarningsLoading(false);
      return;
    }

    let alive = true;
    setIntelLoading(true);
    fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(t)}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`ticker-intel ${r.status}`);
        return (await r.json()) as TickerIntel;
      })
      .then((d) => {
        if (!alive) return;
        setIntel(d);
      })
      .catch(() => {
        if (!alive) return;
        setIntel(null);
      })
      .finally(() => {
        if (!alive) return;
        setIntelLoading(false);
      });

    setNewsLoading(true);
    fetch(`${API_BASE_URL}/api/ticker/news?ticker=${encodeURIComponent(t)}&days=90`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`ticker-news ${r.status}`);
        return (await r.json()) as { results: TickerNewsRow[] };
      })
      .then((d) => {
        if (!alive) return;
        setNews(Array.isArray(d.results) ? d.results : []);
      })
      .catch(() => {
        if (!alive) return;
        setNews([]);
      })
      .finally(() => {
        if (!alive) return;
        setNewsLoading(false);
      });

    setEarningsLoading(true);
    const p = new URLSearchParams();
    p.append("tickers", t);
    fetch(`${API_BASE_URL}/api/earnings/next?${p.toString()}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`earnings ${r.status}`);
        return (await r.json()) as { results: EarningsNextRow[] };
      })
      .then((d) => {
        if (!alive) return;
        const row = Array.isArray(d.results) ? d.results.find((x) => String(x.ticker || "").toUpperCase() === t) ?? null : null;
        setEarnings(row);
      })
      .catch(() => {
        if (!alive) return;
        setEarnings(null);
      })
      .finally(() => {
        if (!alive) return;
        setEarningsLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [ticker]);

  return { intel, intelLoading, news, newsLoading, earnings, earningsLoading };
}

function TickerDrawer({
  ticker,
  meta,
  onClose,
}: {
  ticker: string;
  meta?: TickerDrawerMeta | null;
  onClose: () => void;
}) {
  const { intel, intelLoading, news, newsLoading, earnings, earningsLoading } = useTickerDrawerData(ticker);

  const changeText = useMemo(() => {
    if (!intel || intel.close == null || !Number.isFinite(intel.today_return_pct)) return "—";
    const close = Number(intel.close);
    const pct = Number(intel.today_return_pct);
    const prev = pct === -100 ? null : close / (1 + pct / 100);
    if (prev == null || !Number.isFinite(prev) || prev === 0) return "—";
    const chg = close - prev;
    const sign = chg >= 0 ? "+" : "";
    return `${sign}${fmtPrice(chg)} (${fmtPct(pct, 2)})`;
  }, [intel]);

  const nextEarningsText = useMemo(() => {
    const iso = earnings?.earnings_et_iso ?? null;
    if (!iso) return "—";
    try {
      const d = new Date(iso);
      return d.toLocaleString("en-US", { timeZone: "America/New_York", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }) + " ET";
    } catch {
      return "—";
    }
  }, [earnings]);

  return (
    <aside className="h-full w-[420px] min-w-[420px] shrink-0 border-l border-terminal-border bg-terminal-elevated">
      <div className="flex h-full min-h-0 flex-col">
        <header className="shrink-0 border-b border-terminal-border px-4 py-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="font-mono text-sm font-semibold text-white">{ticker}</p>
              <p className="mt-0.5 truncate text-[11px] text-slate-400">{intel?.name ?? (intelLoading ? "Loading…" : "—")}</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-1 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white"
            >
              Close
            </button>
          </div>
          <div className="mt-2 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-[11px]">
            <span className="font-mono text-slate-200 tabular-nums">{intel?.close == null ? "—" : fmtPrice(intel.close)}</span>
            <span className={`font-mono tabular-nums ${pctClass(intel?.today_return_pct ?? 0)}`}>{changeText}</span>
            <span className="text-slate-600">·</span>
            <span className="text-slate-400">Next earnings:</span>
            <span className="font-mono text-slate-300 tabular-nums">{earningsLoading ? "Loading…" : nextEarningsText}</span>
          </div>
        </header>

        <div className="fintech-scroll min-h-0 flex-1 overflow-auto p-4">
          {meta?.reasoning ? (
            <section className="rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Reasoning</p>
              <p className="mt-2 text-[11px] leading-relaxed text-slate-200">{meta.reasoning}</p>
            </section>
          ) : null}

          <section className="rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
            <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">Classification</p>
            <div className="mt-2 grid grid-cols-[84px_1fr] gap-x-3 gap-y-1 text-[11px]">
              <p className="text-slate-500">Sector</p>
              <p className="truncate font-semibold text-slate-200">{intel?.sector ?? meta?.sector ?? "—"}</p>
              <p className="text-slate-500">Industry</p>
              <p className="truncate font-semibold text-slate-200">{intel?.industry ?? meta?.industry ?? "—"}</p>
              <p className="text-slate-500">Theme</p>
              <p className="truncate font-semibold text-sky-200">{intel?.theme ?? meta?.theme ?? "—"}</p>
              <p className="text-slate-500">Sub‑Theme</p>
              <p className="truncate font-semibold text-violet-200">{intel?.subtheme ?? meta?.category ?? "—"}</p>
              <p className="text-slate-500">Grade</p>
              <p className="truncate font-semibold text-amber-200">{meta?.grade ?? "—"}</p>
            </div>
          </section>

          <section className="mt-3 rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="flex items-center justify-between gap-2 border-b border-terminal-border px-4 py-3">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">News / events (90D)</p>
              <span className="text-[10px] text-slate-600">{newsLoading ? "Loading…" : `${news.length}`}</span>
            </header>
            <div className="p-2">
              {!newsLoading && !news.length ? (
                <p className="px-2 py-6 text-center text-xs text-slate-500">No recent items returned.</p>
              ) : (
                <ul className="space-y-1">
                  {news.slice(0, 30).map((n, i) => (
                    <li key={`${n.published_at_utc ?? n.date_utc ?? "x"}-${i}`} className="rounded-lg border border-terminal-border/60 bg-terminal-bg/30 px-2.5 py-2">
                      <div className="flex items-baseline justify-between gap-3">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{n.event_type || "News"}</span>
                        <span className="font-mono text-[10px] text-slate-600 tabular-nums">{n.date_utc ?? "—"}</span>
                      </div>
                      {n.link ? (
                        <a href={n.link} target="_blank" rel="noopener noreferrer" className="mt-1 block text-[11px] text-slate-200 hover:underline">
                          {n.title}
                        </a>
                      ) : (
                        <p className="mt-1 text-[11px] text-slate-200">{n.title}</p>
                      )}
                      <p className="mt-1 text-[10px] text-slate-600">{n.source}</p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </section>

          <p className="mt-3 text-[10px] leading-relaxed text-slate-600">
            Note: This drawer currently uses best‑effort Yahoo Finance data (intel/news/earnings). Insider/institution/analyst target changes can be added next.
          </p>
        </div>
      </div>
    </aside>
  );
}

function BreakingRiskBanner({
  state,
  message,
}: {
  state: "bullish" | "neutral" | "bearish" | undefined;
  message: string | undefined;
}) {
  if (!message) return null;
  const ring =
    state === "bearish"
      ? "border-rose-500/25 bg-rose-500/10 text-rose-100"
      : state === "neutral"
        ? "border-amber-500/25 bg-amber-500/10 text-amber-100"
        : "border-emerald-500/25 bg-emerald-500/10 text-emerald-100";
  return (
    <div className={`shrink-0 rounded-xl border px-4 py-2 text-[11px] shadow-sm ${ring}`}>
      <div className="flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 shrink-0" aria-hidden />
        <p className="truncate">
          <span className="font-semibold uppercase tracking-wider">Breaking Risk</span>
          <span className="mx-2 text-white/35">·</span>
          <span className="text-slate-100">{message}</span>
        </p>
      </div>
    </div>
  );
}

// ── Intelligence Brief hook (7-Pillar Gemini-powered) ─────────────────────────

type CatalystRow = {
  catalyst: string;
  event: string;
  impact: string;
  impact_level: "High" | "Medium" | "Low" | string;
};

type IntelBrief = {
  brief_type: "pre" | "post";
  generated_at_utc: string | null;
  gen_time_et: string | null;
  markdown: string | null;
  catalysts: CatalystRow[];
  headlines: { title: string; link: string; pubDate: string }[];
  macro_snapshot: Record<string, { close: number | null; change_pct: number | null }>;
};

function useIntelBrief() {
  const [pre, setPre] = useState<IntelBrief | null>(null);
  const [post, setPost] = useState<IntelBrief | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchBoth = useCallback(async () => {
    try {
      const [rPre, rPost] = await Promise.all([
        fetch(`${API_BASE_URL}/api/intelligence-brief/pre`),
        fetch(`${API_BASE_URL}/api/intelligence-brief/post`),
      ]);
      if (rPre.ok) setPre((await rPre.json()) as IntelBrief);
      if (rPost.ok) setPost((await rPost.json()) as IntelBrief);
    } catch {
      // silent — briefs are best-effort
    }
  }, []);

  useEffect(() => {
    void fetchBoth();
    const id = window.setInterval(() => void fetchBoth(), 5 * 60_000); // poll every 5 min
    return () => window.clearInterval(id);
  }, [fetchBoth]);

  const refresh = useCallback(async (briefType: "pre" | "post") => {
    setLoading(true);
    try {
      await fetch(`${API_BASE_URL}/api/intelligence-brief/${briefType}/refresh`, { method: "POST" });
      // Poll for completion — brief generates in ~5-15s
      for (let i = 0; i < 12; i++) {
        await new Promise((r) => window.setTimeout(r, 5000));
        const r = await fetch(`${API_BASE_URL}/api/intelligence-brief/${briefType}`);
        if (r.ok) {
          const data = (await r.json()) as IntelBrief;
          if (data.markdown) {
            briefType === "pre" ? setPre(data) : setPost(data);
            break;
          }
        }
      }
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }, []);

  return { pre, post, loading, refresh };
}

// ── Intelligence Brief panel (auto-displaying, no Generate button) ─────────────

// ── Catalyst impact badge ──────────────────────────────────────────────────
function ImpactBadge({ level }: { level: string }) {
  const lv = (level || "").toLowerCase();
  // 4-tier: Extreme High | High | Medium | Low
  const styles =
    lv.includes("extreme")
      ? "text-rose-500 font-black border-rose-500/50 bg-rose-500/10"
      : lv === "high"
      ? "text-orange-400 font-bold border-orange-400/50 bg-orange-400/10"
      : lv === "low"
      ? "text-emerald-400 font-semibold border-emerald-400/50 bg-emerald-400/10"
      : "text-amber-200 font-semibold border-amber-200/50 bg-amber-200/10"; // Medium
  return (
    <span
      className={`inline-block rounded border px-1.5 py-0.5 text-[9px] uppercase tracking-widest ${styles}`}
    >
      {level || "Medium"}
    </span>
  );
}

// ── Catalyst structured table ──────────────────────────────────────────────
function CatalystTable({ rows }: { rows: CatalystRow[] }) {
  if (!rows || rows.length === 0) return null;
  return (
    <div className="my-2 overflow-x-auto rounded-lg border border-slate-800/80">
      <table className="w-full text-[11px]">
        <thead>
          <tr className="border-b border-slate-800/60 bg-slate-900/40">
            <th className="px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-widest text-slate-500">
              Catalyst
            </th>
            <th className="px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-widest text-slate-500">
              Data / Event
            </th>
            <th className="px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-widest text-slate-500">
              Market Impact
            </th>
            <th className="px-4 py-2 text-left text-[10px] font-semibold uppercase tracking-widest text-slate-500">
              Level
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr
              key={ri}
              className="border-b border-slate-800/60 transition-colors hover:bg-slate-800/10"
            >
              <td className="px-4 py-3 font-semibold text-slate-100">{row.catalyst}</td>
              <td className="px-4 py-3 text-slate-300">{row.event}</td>
              <td className="px-4 py-3 text-slate-400">{row.impact}</td>
              <td className="px-4 py-3">
                <ImpactBadge level={row.impact_level} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Bold-text inline renderer ──────────────────────────────────────────────
function InlineBold({ text }: { text: string }) {
  const parts = text.split(/\*\*(.+?)\*\*/g);
  return (
    <>
      {parts.map((p, i) =>
        i % 2 === 1 ? <strong key={i} className="font-semibold text-white">{p}</strong> : p
      )}
    </>
  );
}

// ── Markdown renderer (shared between side panel and focus modal) ──────────
function renderBriefMarkdown(brief: IntelBrief): JSX.Element[] {
  // Replace both ```json_catalysts and ```json fences containing the catalyst
  // array with a sentinel so CatalystTable can be injected at that position.
  const cleanedMarkdown = brief.markdown!
    .replace(/```json_catalysts[\s\S]*?```/g, "___CATALYST_TABLE___")
    .replace(/```json\s*\[[\s\S]*?\]\s*```/g, "___CATALYST_TABLE___");

  const lines = cleanedMarkdown.split("\n");
  const rendered: JSX.Element[] = [];
  let tableBuffer: string[] = [];
  let inMdTable = false;

  const flushMarkdownTable = () => {
    if (!tableBuffer.length) return;
    const rows = tableBuffer.filter((l) => l.trim().startsWith("|"));
    if (rows.length >= 2) {
      const headers = rows[0].split("|").filter(Boolean).map((c) => c.trim());
      const bodyRows = rows.slice(2);
      rendered.push(
        <div key={`tbl-${rendered.length}`} className="my-2 overflow-x-auto rounded-lg border border-slate-800/60">
          <table className="w-full text-[10px]">
            <thead>
              <tr className="border-b border-slate-800/60 bg-slate-900/40">
                {headers.map((h) => (
                  <th key={h} className="px-4 py-2 text-left text-[9px] font-semibold uppercase tracking-widest text-slate-500">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {bodyRows.map((row, ri) => {
                const cells = row.split("|").filter(Boolean).map((c) => c.trim());
                return (
                  <tr key={ri} className="border-b border-slate-800/50 hover:bg-slate-800/10">
                    {cells.map((c, ci) => (
                      <td key={ci} className="px-4 py-2.5 text-slate-300"><InlineBold text={c} /></td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      );
    }
    tableBuffer = [];
    inMdTable = false;
  };

  lines.forEach((line, i) => {
    const trimmed = line.trim();

    if (trimmed.startsWith("|")) {
      inMdTable = true;
      tableBuffer.push(trimmed);
      return;
    }
    if (inMdTable) flushMarkdownTable();

    if (trimmed === "___CATALYST_TABLE___") {
      rendered.push(<CatalystTable key={`cat-${i}`} rows={brief.catalysts ?? []} />);
      return;
    }

    if (trimmed.startsWith("## ")) {
      rendered.push(
        <div key={i} className="mt-4 flex items-center gap-2 rounded-lg border border-accent/30 bg-terminal-elevated px-3 py-2">
          <p className="font-mono text-[11px] font-bold text-accent">{trimmed.slice(3)}</p>
        </div>
      );
    } else if (trimmed.startsWith("### ")) {
      rendered.push(
        <h3 key={i} className="mt-4 border-b border-terminal-border/40 pb-1 text-[10px] font-semibold uppercase tracking-widest text-slate-400">
          {trimmed.slice(4)}
        </h3>
      );
    } else if (trimmed.startsWith("> ")) {
      rendered.push(
        <blockquote key={i} className="my-2 rounded-r border-l-2 border-accent/60 bg-terminal-elevated/50 py-2 pl-3 pr-2 italic text-[11px] text-slate-200">
          <InlineBold text={trimmed.slice(2)} />
        </blockquote>
      );
    } else if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
      rendered.push(
        <p key={i} className="flex gap-2 text-[11px] leading-relaxed text-slate-300">
          <span className="mt-0.5 shrink-0 text-slate-600">•</span>
          <span><InlineBold text={trimmed.slice(2)} /></span>
        </p>
      );
    } else if (trimmed === "---") {
      rendered.push(<hr key={i} className="my-3 border-terminal-border/50" />);
    } else if (trimmed.startsWith("```") || trimmed === "") {
      // skip code fences and blank lines
    } else {
      rendered.push(
        <p key={i} className="text-[11px] leading-relaxed text-slate-300">
          <InlineBold text={trimmed} />
        </p>
      );
    }
  });
  if (inMdTable) flushMarkdownTable();
  return rendered;
}

// ── Main panel (side drawer) + Focus Modal ─────────────────────────────────
function IntelBriefPanel({ brief, loading }: { brief: IntelBrief | null; loading: boolean }) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (loading) {
    return (
      <div className="flex flex-col gap-2 p-3">
        <div className="h-3 w-3/4 animate-pulse rounded bg-terminal-border" />
        <div className="h-3 w-full animate-pulse rounded bg-terminal-border" />
        <div className="h-3 w-5/6 animate-pulse rounded bg-terminal-border" />
        <p className="mt-2 text-[10px] text-slate-600">Generating intelligence brief…</p>
      </div>
    );
  }
  if (!brief?.markdown) {
    return (
      <p className="p-3 text-[11px] text-slate-600">
        Brief scheduled for next market session. Will appear automatically at 8:03 AM ET (pre) or 4:55 PM ET (post).
      </p>
    );
  }

  const rendered = renderBriefMarkdown(brief);

  return (
    <>
      {/* Side-panel: compact view with expand button */}
      <article className="space-y-1 p-3">
        {rendered}
        <button
          type="button"
          onClick={() => setIsExpanded(true)}
          className="mt-3 w-full rounded-lg border border-terminal-border/60 bg-terminal-elevated py-1.5 text-[10px] font-semibold uppercase tracking-widest text-slate-500 transition-colors hover:border-accent/40 hover:text-white"
        >
          ↗ Open Full Brief
        </button>
      </article>

      {/* Focus Modal: full-width centered overlay */}
      {isExpanded && (
        <div
          className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/80 backdrop-blur-xl"
          onClick={(e) => { if (e.target === e.currentTarget) setIsExpanded(false); }}
        >
          <div className="relative mx-auto my-8 w-full max-w-3xl rounded-2xl border border-terminal-border bg-terminal-card shadow-2xl">
            {/* Modal header */}
            <div className="sticky top-0 z-10 flex items-center justify-between rounded-t-2xl border-b border-terminal-border bg-terminal-card px-5 py-3">
              <div className="flex items-center gap-2">
                {brief.brief_type === "pre"
                  ? <Sunrise className="h-4 w-4 text-amber-300" aria-hidden />
                  : <Moon className="h-4 w-4 text-sky-300" aria-hidden />}
                <span className="text-[12px] font-bold uppercase tracking-widest text-white">
                  Market Intelligence — {brief.brief_type === "pre" ? "Pre-Market" : "Post-Market"}
                </span>
                {brief.gen_time_et && (
                  <span className="ml-2 font-mono text-[10px] text-accent">
                    Gen {brief.gen_time_et}
                  </span>
                )}
              </div>
              <button
                type="button"
                onClick={() => setIsExpanded(false)}
                className="rounded-full p-1 text-slate-500 hover:bg-terminal-elevated hover:text-white"
                aria-label="Close"
              >
                ✕
              </button>
            </div>
            {/* Modal body */}
            <article className="space-y-1.5 px-5 py-4">
              {rendered}
            </article>
          </div>
        </div>
      )}
    </>
  );
}

// ── Updated MarketBriefCard using Intelligence Brief (no Generate button) ─────

function IntelBriefCard({
  mode,
  onModeChange,
  pre,
  post,
  loading,
  onRefresh,
}: {
  mode: "pre" | "post";
  onModeChange: (m: "pre" | "post") => void;
  pre: IntelBrief | null;
  post: IntelBrief | null;
  loading: boolean;
  onRefresh: (t: "pre" | "post") => void;
}) {
  const brief = mode === "pre" ? pre : post;
  const now_et_h = new Date().toLocaleString("en-US", { timeZone: "America/New_York", hour: "numeric", hour12: false });
  // Auto-select: pre-market until 16:54, post-market from 16:55 ET
  const autoMode: "pre" | "post" = Number(now_et_h) < 17 ? "pre" : "post";

  return (
    <section className="flex min-h-0 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
      <header className="shrink-0 border-b border-terminal-border px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5">
            {mode === "pre"
              ? <Sunrise className="h-3.5 w-3.5 text-amber-300" aria-hidden />
              : <Moon className="h-3.5 w-3.5 text-sky-300" aria-hidden />}
            <span>Market Intelligence</span>
            {brief?.gen_time_et && (
              <span className="ml-1 font-mono text-[9px] text-slate-600">Gen {brief.gen_time_et}</span>
            )}
          </div>
          <div className="flex items-center gap-1.5">
            <div className="flex items-center gap-1 rounded-full border border-terminal-border bg-terminal-bg p-0.5">
              <button type="button" onClick={() => onModeChange("pre")}
                className={`rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors ${mode === "pre" ? "bg-accent/20 text-white" : "text-slate-500 hover:text-white"}`}>
                PRE
              </button>
              <button type="button" onClick={() => onModeChange("post")}
                className={`rounded-full px-2 py-0.5 text-[10px] font-semibold transition-colors ${mode === "post" ? "bg-accent/20 text-white" : "text-slate-500 hover:text-white"}`}>
                POST
              </button>
            </div>
            <button type="button" disabled={loading} title="Regenerate brief"
              onClick={() => onRefresh(mode)}
              className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-0.5 text-[9px] font-semibold text-slate-500 hover:border-accent/30 hover:text-white disabled:opacity-40">
              {loading ? "…" : "↺"}
            </button>
          </div>
        </div>
        {autoMode !== mode && (
          <p className="mt-1 text-[9px] text-slate-600">
            Auto-display: {autoMode === "pre" ? "Pre-Market" : "Post-Market"} · switch above to compare
          </p>
        )}
      </header>
      <div className="fintech-scroll min-h-0 flex-1 overflow-y-auto">
        <IntelBriefPanel brief={brief} loading={loading} />
      </div>
    </section>
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

function usePostmarketBrief() {
  const [brief, setBrief] = useState<PremarketBrief | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetch(`${API_BASE_URL}/api/news/postmarket`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`postmarket ${r.status}`);
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
    fetch(`${API_BASE_URL}/api/news/postmarket/refresh`, { method: "POST" })
      .then(async (r) => {
        if (!r.ok) throw new Error(`postmarket refresh ${r.status}`);
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
  const market = useMarketStatus();

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
    // Auto-scan only between 8:03am ET and market open, on NYSE trading days.
    if (!market?.premarket_scan_active) return;
    const id = window.setInterval(() => void loadRef.current(), 60_000);
    return () => window.clearInterval(id);
  }, [market?.premarket_scan_active]);

  useEffect(() => {
    const t = window.setTimeout(() => void loadRef.current(), 450);
    return () => window.clearTimeout(t);
  }, [filters.min_gap_pct, filters.min_pm_vol_k, filters.min_price, filters.min_avg_vol_10d_k, filters.min_mkt_cap_b, filters.min_avg_dollar_vol_m]);

  return { data, loading, error, reload: load };
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

const ScannerView = memo(function ScannerView({
  payload,
  spotlightThemeName,
  setSpotlightThemeName,
  themeQuery,
  setThemeQuery,
  intelPre,
  intelPost,
  intelLoading,
  onRefreshIntel,
}: {
  payload: ApiPayload;
  spotlightThemeName: string | null;
  setSpotlightThemeName: (s: string) => void;
  themeQuery: string;
  setThemeQuery: (s: string) => void;
  intelPre: IntelBrief | null;
  intelPost: IntelBrief | null;
  intelLoading: boolean;
  onRefreshIntel: (t: "pre" | "post") => void;
}) {
  const [leaderboardMode, setLeaderboardMode] = useState<"themes" | "industry">("themes");
  const now_et_h = new Date().toLocaleString("en-US", { timeZone: "America/New_York", hour: "numeric", hour12: false });
  const [briefMode, setBriefMode] = useState<"pre" | "post">(Number(now_et_h) < 17 ? "pre" : "post");
  const [sortKey, setSortKey] = useState<
    "theme" | "perf1D" | "perf1W" | "perf1M" | "perf3M" | "perf6M" | "rs1m" | "qual" | "leaders" | null
  >(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

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

  const { payload: finvizLeaderboardPayload, loading: finvizLbLoading, error: finvizLbError } = useFdvLeaderboard(leaderboardMode);
  const finvizRows = finvizLeaderboardPayload?.themes ?? [];
  const finvizFilteredRows = useMemo(() => {
    const q = themeQuery.trim().toLowerCase();
    if (!q) return finvizRows;
    return finvizRows.filter((t) => t.theme.toLowerCase().includes(q) || (t.sector ?? "").toLowerCase().includes(q));
  }, [finvizRows, themeQuery]);
  const sortedLeaderboardRows = useMemo(() => {
    if (!sortKey) return finvizFilteredRows;
    const dir = sortDir === "asc" ? 1 : -1;
    const num = (n: number | null | undefined) => (n == null || !Number.isFinite(n) ? Number.NEGATIVE_INFINITY : n);
    const rows = [...finvizFilteredRows];
    rows.sort((a, b) => {
      if (sortKey === "theme") return a.theme.localeCompare(b.theme) * dir;
      if (sortKey === "leaders") {
        const aKey = (a.leaders?.[0] ?? a.leaders?.join(",") ?? "").toString();
        const bKey = (b.leaders?.[0] ?? b.leaders?.join(",") ?? "").toString();
        return aKey.localeCompare(bKey) * dir;
      }
      if (sortKey === "rs1m") return (num(a.relativeStrength1M) - num(b.relativeStrength1M)) * dir;
      if (sortKey === "qual") return (num(a.relativeStrengthQualifierRatio) - num(b.relativeStrengthQualifierRatio)) * dir;
      const av = sortKey === "perf1D" ? a.perf1D : sortKey === "perf1W" ? a.perf1W : sortKey === "perf1M" ? a.perf1M : sortKey === "perf3M" ? a.perf3M : a.perf6M;
      const bv = sortKey === "perf1D" ? b.perf1D : sortKey === "perf1W" ? b.perf1W : sortKey === "perf1M" ? b.perf1M : sortKey === "perf3M" ? b.perf3M : b.perf6M;
      return (num(av) - num(bv)) * dir;
    });
    return rows;
  }, [finvizFilteredRows, sortDir, sortKey]);

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
      {/* Left: Market + Brief + VIX */}
      <div className="fintech-scroll flex w-[360px] min-w-[360px] shrink-0 flex-col gap-3 overflow-y-auto pr-1">
        <MarketRegimeCard state={payload.market_momentum_score?.state} message={payload.market_momentum_score?.message} />
        <IntelBriefCard
          mode={briefMode}
          onModeChange={setBriefMode}
          pre={intelPre}
          post={intelPost}
          loading={intelLoading}
          onRefresh={onRefreshIntel}
        />
        <VixFearGaugeLite close={payload.vix?.close} changePct={payload.vix?.change_pct} />
        <LiquidityFlowCard summary={payload.marketFlowSummary} />
      </div>

      {/* Middle: RS + Spotlight + Constituents */}
      <div className="fintech-scroll flex w-[420px] min-w-[420px] shrink-0 flex-col gap-3 overflow-y-auto pr-1">
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
                    RS{" "}
                    {spotlightTheme.relativeStrength1M == null || !Number.isFinite(spotlightTheme.relativeStrength1M)
                      ? "—"
                      : spotlightTheme.relativeStrength1M.toFixed(1)}
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

      {/* Right: Leaderboard — sticky thead via border-separate + sticky top-0 on th */}
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        <section className="flex min-h-0 flex-1 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
          <header className="flex flex-wrap items-center justify-between gap-3 border-b border-terminal-border px-4 py-3">
            <div className="min-w-0">
              <h2 className="truncate text-sm font-semibold text-white">Leaderboard</h2>
              <p className="truncate text-[11px] text-slate-500">Theme performance + RS</p>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1 rounded-full border border-terminal-border bg-terminal-bg p-1">
                <button
                  type="button"
                  onClick={() => setLeaderboardMode("themes")}
                  className={`rounded-full px-3 py-1.5 text-[11px] font-semibold transition-colors ${
                    leaderboardMode === "themes" ? "bg-accent/20 text-white" : "text-slate-400 hover:text-white"
                  }`}
                >
                  Themes
                </button>
                <button
                  type="button"
                  onClick={() => setLeaderboardMode("industry")}
                  className={`rounded-full px-3 py-1.5 text-[11px] font-semibold transition-colors ${
                    leaderboardMode === "industry" ? "bg-accent/20 text-white" : "text-slate-400 hover:text-white"
                  }`}
                >
                  Industry
                </button>
              </div>
              <div className="flex items-center gap-2 rounded-md border border-terminal-border bg-terminal-bg px-2 py-2">
                <Search className="h-4 w-4 text-slate-500" aria-hidden />
                <input
                  value={themeQuery}
                  onChange={(e) => setThemeQuery(e.target.value)}
                  placeholder="Filter themes…"
                  className="w-[200px] bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none"
                />
              </div>
            </div>
          </header>
          <div className="fintech-scroll min-h-0 flex-1 overflow-auto">
            <table className="w-full min-w-[1020px] border-separate border-spacing-0 text-left text-[11px]">
              <thead>
                <tr>
                  {[
                    { label: "#", key: null as null | "theme" | "perf1D" | "perf1W" | "perf1M" | "perf3M" | "perf6M" | "rs1m" | "qual" },
                    { label: "Theme", key: "theme" as const },
                    { label: "1D", key: "perf1D" as const },
                    { label: "1W", key: "perf1W" as const },
                    { label: "1M", key: "perf1M" as const },
                    { label: "3M", key: "perf3M" as const },
                    { label: "6M", key: "perf6M" as const },
                    { label: "RS 1M", key: "rs1m" as const },
                    { label: "Qual%", key: "qual" as const },
                    { label: "Leaders", key: "leaders" as const },
                  ].map((h) => (
                    <th key={h.label} className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                      {h.key ? (
                        <button
                          type="button"
                          onClick={() => {
                            if (sortKey === h.key) {
                              setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                            } else {
                              setSortKey(h.key);
                              setSortDir(h.key === "theme" ? "asc" : "desc");
                            }
                          }}
                          className="inline-flex items-center gap-1 hover:text-slate-300"
                        >
                          <span>{h.label}</span>
                          <span className={`text-[9px] ${sortKey === h.key ? "text-accent" : "text-slate-600"}`}>
                            {sortKey === h.key ? (sortDir === "asc" ? "▲" : "▼") : "↕"}
                          </span>
                        </button>
                      ) : (
                        h.label
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {finvizLbLoading && !finvizFilteredRows.length ? (
                  <tr>
                    <td colSpan={10} className="px-3 py-6 text-center text-xs text-slate-500">
                      Loading Finviz {leaderboardMode}…
                    </td>
                  </tr>
                ) : finvizLbError && !finvizFilteredRows.length ? (
                  <tr>
                    <td colSpan={10} className="px-3 py-6 text-center text-xs text-rose-300">
                      Failed to load Finviz {leaderboardMode}: {finvizLbError}
                    </td>
                  </tr>
                ) : (
                  sortedLeaderboardRows.slice(0, 160).map((t, idx) => {
                      const displayedCount = Math.min(160, sortedLeaderboardRows.length);
                      const rankNumber = sortDir === "asc" ? displayedCount - idx : idx + 1;
                      const rs = t.relativeStrength1M ?? null;
                      const qRatio = Number.isFinite(t.relativeStrengthQualifierRatio) ? t.relativeStrengthQualifierRatio : null;
                      return (
                        <tr
                          key={`${leaderboardMode}:${t.theme}|${t.sector ?? ""}`}
                          className={`cursor-pointer border-b border-terminal-border/60 hover:bg-terminal-elevated/40 ${
                            spotlightTheme?.theme === t.theme ? "bg-terminal-elevated/30" : ""
                          }`}
                          onClick={() => setSpotlightThemeName(t.theme)}
                          title="Click to spotlight"
                        >
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-500">{rankNumber}</td>
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
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-200">
                            {rs == null || !Number.isFinite(rs) ? "—" : rs.toFixed(1)}
                          </td>
                          <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-200">
                            {qRatio == null ? "—" : `${(qRatio * 100).toFixed(0)}%`}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-slate-300">{(t.leaders ?? []).slice(0, 4).join(", ") || "—"}</td>
                        </tr>
                      );
                    })
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  );
});

// MarketBriefCard removed — superseded by IntelBriefCard (7-Pillar Gemini engine).

const GappersView = memo(function GappersView({
  gappers,
  loading,
  error,
  filters,
  setFilters,
  gapScannerGradeByTicker,
  onSelectTicker,
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
  onSelectTicker: (ticker: string, meta?: TickerDrawerMeta) => void;
}) {
  const [sortKey, setSortKey] = useState<
    "ticker" | "premktPct" | "premktVol" | "dailyPct" | "adr" | "mcap" | "sector" | "industry" | "grade" | "setup"
  >("premktPct");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const sortedRows = useMemo(() => {
    const rows = [...(gappers?.rows ?? [])];

    const gradeScore = (g: string) => (g === "A" ? 3 : g === "B" ? 2 : 1);
    const setupScore = (s: string) => (s === "EP" ? 3 : s === "U&R" ? 2 : s === "Pullback" ? 1 : 0);

    rows.sort((a, b) => {
      const symA = gapScanRowSymbol(a);
      const symB = gapScanRowSymbol(b);
      const symUsA = symA !== "—" ? symA.toUpperCase() : "";
      const symUsB = symB !== "—" ? symB.toUpperCase() : "";

      const pmGapA = gapRowNum(a, "premarket_gap");
      const pmGapB = gapRowNum(b, "premarket_gap");
      const pmVolA = gapRowNum(a, "premarket_volume");
      const pmVolB = gapRowNum(b, "premarket_volume");
      const dailyA = gapRowNum(a, "change");
      const dailyB = gapRowNum(b, "change");
      const adrA = gapRowNum(a, "Volatility.D");
      const adrB = gapRowNum(b, "Volatility.D");
      const mcapA = gapRowNum(a, "market_cap_basic");
      const mcapB = gapRowNum(b, "market_cap_basic");
      const sectorA = a.sector != null && String(a.sector).trim() ? String(a.sector) : "";
      const sectorB = b.sector != null && String(b.sector).trim() ? String(b.sector) : "";
      const industryA = a.industry != null && String(a.industry).trim() ? String(a.industry) : "";
      const industryB = b.industry != null && String(b.industry).trim() ? String(b.industry) : "";
      const gradeA = symUsA ? (gapScannerGradeByTicker.get(symUsA) ?? "C") : "C";
      const gradeB = symUsB ? (gapScannerGradeByTicker.get(symUsB) ?? "C") : "C";
      const setupA = gapRowStr(a, "setup_tag") ?? "";
      const setupB = gapRowStr(b, "setup_tag") ?? "";

      const dir = sortDir === "asc" ? 1 : -1;

      const numCmp = (x: number | null, y: number | null) => {
        const ax = x == null || !Number.isFinite(x) ? -Infinity : x;
        const by = y == null || !Number.isFinite(y) ? -Infinity : y;
        return (ax - by) * dir;
      };

      const strCmp = (x: string, y: string) => x.localeCompare(y) * dir;

      let c = 0;
      if (sortKey === "ticker") c = strCmp(symUsA, symUsB);
      else if (sortKey === "premktPct") c = numCmp(pmGapA, pmGapB);
      else if (sortKey === "premktVol") c = numCmp(pmVolA, pmVolB);
      else if (sortKey === "dailyPct") c = numCmp(dailyA, dailyB);
      else if (sortKey === "adr") c = numCmp(adrA, adrB);
      else if (sortKey === "mcap") c = numCmp(mcapA, mcapB);
      else if (sortKey === "sector") c = strCmp(sectorA, sectorB);
      else if (sortKey === "industry") c = strCmp(industryA, industryB);
      else if (sortKey === "grade") c = (gradeScore(gradeA) - gradeScore(gradeB)) * dir;
      else if (sortKey === "setup") c = (setupScore(setupA) - setupScore(setupB)) * dir;

      if (c !== 0) return c;
      const tie1 = (pmGapA ?? -Infinity) - (pmGapB ?? -Infinity);
      if (tie1 !== 0) return tie1 * -1;
      return symUsA.localeCompare(symUsB);
    });

    return rows;
  }, [gappers?.rows, gapScannerGradeByTicker, sortDir, sortKey]);

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
                  {(
                    [
                      { label: "Ticker", key: "ticker" },
                      { label: "Premkt %", key: "premktPct" },
                      { label: "Premkt Vol", key: "premktVol" },
                      { label: "Daily %", key: "dailyPct" },
                      { label: "ADR%", key: "adr" },
                      { label: "MktCap", key: "mcap" },
                      { label: "Setup", key: "setup" },
                      { label: "Sector", key: "sector" },
                      { label: "Industry", key: "industry" },
                      { label: "Grade", key: "grade" },
                    ] as const
                  ).map((h) => (
                    <th
                      key={h.label}
                      className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-2 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500"
                    >
                      <button
                        type="button"
                        onClick={() => {
                          if (sortKey === h.key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                          else {
                            setSortKey(h.key);
                            setSortDir(h.key === "ticker" || h.key === "sector" || h.key === "industry" ? "asc" : "desc");
                          }
                        }}
                        className="inline-flex items-center gap-1 hover:text-slate-300"
                      >
                        <span>{h.label}</span>
                        <span className={`text-[9px] ${sortKey === h.key ? "text-accent" : "text-slate-600"}`}>
                          {sortKey === h.key ? (sortDir === "asc" ? "▲" : "▼") : "↕"}
                        </span>
                      </button>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, idx) => {
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
                  const setupTag = gapRowStr(row, "setup_tag") ?? "";
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
                            <button
                              type="button"
                              onClick={() =>
                                symUs
                                  ? onSelectTicker(symUs, {
                                      reasoning: gapRowStr(row, "reasoning") ?? gapRowStr(row, "Reasoning"),
                                      sector,
                                      industry,
                                      theme: gapRowStr(row, "theme") ?? gapRowStr(row, "Theme"),
                                      category: gapRowStr(row, "category") ?? gapRowStr(row, "Category"),
                                      grade,
                                    })
                                  : null
                              }
                              className="font-mono font-semibold text-accent hover:underline"
                              title="Open ticker details"
                            >
                              {sym}
                            </button>
                          ) : (
                            <button
                              type="button"
                              onClick={() =>
                                symUs
                                  ? onSelectTicker(symUs, {
                                      reasoning: gapRowStr(row, "reasoning") ?? gapRowStr(row, "Reasoning"),
                                      sector,
                                      industry,
                                      theme: gapRowStr(row, "theme") ?? gapRowStr(row, "Theme"),
                                      category: gapRowStr(row, "category") ?? gapRowStr(row, "Category"),
                                      grade,
                                    })
                                  : null
                              }
                              className="font-mono font-semibold text-slate-300 hover:text-white hover:underline"
                              title="Open ticker details"
                            >
                              {sym}
                            </button>
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
                      <td className="border-b border-terminal-border/60 px-2 py-2 text-center align-middle">
                        <SetupBadge tag={setupTag || null} />
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
});

export function ThemeDashboard() {
  const [tab, setTab] = useState<"scanner" | "gappers" | "breadth">("scanner");
  const [focusTicker, setFocusTicker] = useState<string | null>(null);
  const [focusTickerMeta, setFocusTickerMeta] = useState<TickerDrawerMeta | null>(null);
  const { payload, error, reload: reloadThemes, lastUpdatedAt, loading, loadingSince, pollMs } = useThemesPayload();
  // Legacy RSS-based briefs kept alive for data continuity (not rendered in main UI).
  usePremarketBrief();
  usePostmarketBrief();
  const { pre: intelPre, post: intelPost, loading: intelBriefLoading, refresh: refreshIntel } = useIntelBrief();

  const [etNow, setEtNow] = useState(() => Date.now());

  useEffect(() => {
    const id = window.setInterval(() => setEtNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  const liveTimeText = useMemo(() => {
    try {
      // Use the user's local machine time settings (Windows taskbar time).
      return new Date(etNow).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    } catch {
      return "—";
    }
  }, [etNow]);

  const autoRefreshCountdown = useMemo(() => {
    const base = lastUpdatedAt ?? etNow;
    const nextAt = base + pollMs;
    const ms = Math.max(0, nextAt - etNow);
    const s = Math.floor(ms / 1000);
    const mm = String(Math.floor(s / 60)).padStart(2, "0");
    const ss = String(s % 60).padStart(2, "0");
    return `${mm}:${ss}`;
  }, [lastUpdatedAt, pollMs, etNow]);

  const status = useMemo(() => {
    const now = etNow;
    const base = lastUpdatedAt ?? null;
    const expected = pollMs;
    const staleMs = base == null ? Infinity : now - base;
    const hungMs = loadingSince == null ? 0 : now - loadingSince;
    if (error && !payload) return { label: "DOWN", cls: "bg-rose-500", hint: error };
    if (loading && hungMs > 20_000) return { label: "HUNG", cls: "bg-amber-400", hint: "Fetch running > 20s" };
    if (base == null) return { label: "STARTING", cls: "bg-slate-500", hint: "Waiting for first payload" };
    if (staleMs > Math.max(expected * 2.2, MIN_AUTO_REFRESH_MS * 2.2)) return { label: "DOWN", cls: "bg-rose-500", hint: "Stale payload" };
    if (loading) return { label: "UPDATING", cls: "bg-sky-400", hint: "Fetching new payload" };
    return { label: "OK", cls: "bg-emerald-400", hint: "Fresh payload" };
  }, [etNow, lastUpdatedAt, pollMs, loading, loadingSince, error, payload]);

  const [leaderboardQuery, setLeaderboardQuery] = useState("");
  const [themeSearchQuery, setThemeSearchQuery] = useState("");
  const [spotlightThemeName, setSpotlightThemeName] = useState<string | null>(null);

  const [themeSearchOpen, setThemeSearchOpen] = useState(false);
  const closeThemeSearchRef = useRef<number | null>(null);
  const { suggest, suggestLoading, activeTicker, setActiveTicker, intel, intelLoading } = useTickerAutocomplete(themeSearchQuery, themeSearchOpen);

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

  if (error && !payload) {
    const retryIn = Math.max(0, Math.round(pollMs / 1000));
    return (
      <div className="flex h-full items-center justify-center bg-terminal-bg p-6">
        <div className="rounded-xl border border-terminal-border bg-terminal-card px-5 py-4 text-center">
          <p className="text-sm font-semibold text-rose-300">Failed to load dashboard: {error}</p>
          <p className="mt-1 text-xs text-slate-500">Backing off. Retrying in ~{retryIn}s.</p>
          <button
            type="button"
            onClick={() => void reloadThemes()}
            className="mt-3 rounded-md border border-terminal-border bg-terminal-bg px-3 py-1.5 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white"
          >
            Retry now
          </button>
        </div>
      </div>
    );
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
          <div className="flex min-w-0 flex-1 items-center gap-2">
            <div className="hidden min-w-0 flex-1 items-center justify-center gap-6 lg:flex">
              <div className="min-w-0 overflow-hidden">
                <TapeInline tape={payload?.tape} />
              </div>
            </div>
            <div className="ml-auto hidden items-center gap-3 rounded-xl border border-terminal-border bg-terminal-bg px-3 py-2 sm:flex">
              <div className="flex flex-col leading-tight">
                <span className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                  Live time
                  <span className={`inline-flex h-2 w-2 rounded-full ${status.cls}`} title={status.hint} />
                </span>
                <span className="font-mono text-[12px] font-semibold text-slate-200 tabular-nums">{liveTimeText}</span>
              </div>
              <div className="h-7 w-px bg-terminal-border/80" />
              <div className="flex flex-col leading-tight">
                <span className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                  Auto update
                  <span className={`inline-flex h-2 w-2 rounded-full ${status.cls}`} title={status.hint} />
                </span>
                <span className="font-mono text-[12px] font-semibold text-slate-200 tabular-nums">{autoRefreshCountdown}</span>
              </div>
              <span className="hidden text-[10px] text-slate-600 lg:inline">
                Last: <span className="font-mono">{formatEtClock(lastUpdatedAt)}</span>
              </span>
            </div>
          </div>
        </div>
      </header>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        <div className="shrink-0 border-b border-terminal-border bg-terminal-bg pl-4 pr-[42px] py-3">
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
            <button
              type="button"
              onClick={() => setTab("breadth")}
              className={`flex items-center gap-1.5 rounded-full border px-4 py-2 text-[11px] font-semibold transition-colors ${
                tab === "breadth" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              <BarChart2 className="h-3.5 w-3.5" aria-hidden />
              Market Breath
            </button>

            <div className="relative ml-auto">
              <div className="flex items-center gap-2 rounded-md border border-terminal-border bg-terminal-bg px-2 py-2">
                <Search className="h-4 w-4 text-slate-500" aria-hidden />
                <input
                  value={themeSearchQuery}
                  onChange={(e) => setThemeSearchQuery(e.target.value)}
                  onFocus={() => {
                    if (closeThemeSearchRef.current) window.clearTimeout(closeThemeSearchRef.current);
                    setThemeSearchOpen(true);
                  }}
                  onBlur={() => {
                    if (closeThemeSearchRef.current) window.clearTimeout(closeThemeSearchRef.current);
                    closeThemeSearchRef.current = window.setTimeout(() => setThemeSearchOpen(false), 140);
                  }}
                  placeholder="Search ticker / company…"
                  className="w-[180px] bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none"
                />
                {themeSearchQuery ? (
                  <button
                    type="button"
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => setThemeSearchQuery("")}
                    className="rounded px-1 text-slate-500 hover:text-slate-200"
                    aria-label="Clear search"
                    title="Clear"
                  >
                    ×
                  </button>
                ) : null}
              </div>

              {themeSearchOpen && themeSearchQuery.trim() ? (
                <div className="absolute right-0 top-[44px] z-50 w-[340px] overflow-hidden rounded-xl border border-terminal-border bg-terminal-card shadow-xl">
                  <div className="px-3 py-2">
                    {suggestLoading ? <p className="text-[11px] text-slate-500">Searching…</p> : null}
                    {!suggestLoading && !suggest.length ? <p className="text-[11px] text-slate-500">No matches.</p> : null}
                  </div>

                  {suggest.length ? (
                    <div className="border-t border-terminal-border">
                      <div className="max-h-[108px] overflow-auto px-2 py-2">
                        <div className="grid gap-1">
                          {suggest.slice(0, 6).map((r) => {
                            const isActive = (activeTicker || "").toUpperCase() === String(r.ticker || "").toUpperCase();
                            return (
                              <button
                                key={r.ticker}
                                type="button"
                                onMouseDown={(e) => e.preventDefault()}
                                onMouseEnter={() => setActiveTicker(String(r.ticker || "").toUpperCase())}
                                onClick={() => {
                                  setThemeSearchQuery(String(r.ticker || "").toUpperCase());
                                  setThemeSearchOpen(false);
                                }}
                                className={`flex w-full items-center justify-between gap-3 rounded-lg border px-2.5 py-2 text-left ${
                                  isActive ? "border-accent/40 bg-accent/10" : "border-terminal-border/60 bg-terminal-bg/40 hover:border-slate-600 hover:bg-terminal-elevated/20"
                                }`}
                              >
                                <span className="font-mono text-[12px] font-semibold text-accent">{String(r.ticker || "").toUpperCase()}</span>
                                <span className="truncate text-[10px] text-slate-500">{r.name}</span>
                              </button>
                            );
                          })}
                        </div>
                      </div>

                      <div className="border-t border-terminal-border px-3 py-3">
                        {intelLoading ? (
                          <p className="text-[11px] text-slate-500">Loading…</p>
                        ) : intel ? (
                          <div className="space-y-2">
                            <div>
                              <div className="flex items-baseline justify-between gap-3">
                                <p className="font-mono text-[12px] font-semibold text-white">{intel.ticker}</p>
                                <div className="flex items-baseline gap-3">
                                  <span className="font-mono text-[11px] font-semibold text-slate-200 tabular-nums">{fmtPrice(intel.close)}</span>
                                  <span className={`font-mono text-[11px] font-semibold tabular-nums ${pctClass(intel.today_return_pct ?? 0)}`}>
                                    {(() => {
                                      const close = intel.close ?? null;
                                      const pct = Number.isFinite(intel.today_return_pct) ? Number(intel.today_return_pct) : null;
                                      if (close == null || pct == null) return "—";
                                      const prev = pct === -100 ? null : close / (1 + pct / 100);
                                      if (prev == null || !Number.isFinite(prev) || prev === 0) return "—";
                                      const chg = close - prev;
                                      const sign = chg >= 0 ? "+" : "";
                                      return `${sign}${fmtPrice(chg)}`;
                                    })()}
                                  </span>
                                  <span className={`font-mono text-[11px] tabular-nums ${pctClass(intel.today_return_pct ?? 0)}`}>{fmtPct(intel.today_return_pct, 2)}</span>
                                </div>
                              </div>
                              <p className="mt-0.5 text-[11px] text-slate-400">{intel.name}</p>
                            </div>
                            <div className="h-px bg-terminal-border" />
                            <div className="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 text-[11px]">
                              <p className="text-slate-500">Sector</p>
                              <p className="truncate font-semibold text-slate-200">{intel.sector}</p>
                              <p className="text-slate-500">Industry</p>
                              <p className="truncate font-semibold text-slate-200">{intel.industry}</p>
                              <p className="text-slate-500">Theme</p>
                              <p className="truncate font-semibold text-sky-200">{intel.theme ?? "—"}</p>
                              <p className="text-slate-500">Sub-Theme</p>
                              <p className="truncate font-semibold text-violet-200">{intel.subtheme ?? "—"}</p>
                            </div>
                          </div>
                        ) : (
                          <p className="text-[11px] text-slate-500">Select a ticker to view details.</p>
                        )}
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-row overflow-x-auto overflow-y-hidden">
          <div className="flex h-full min-h-0 min-w-0 flex-1 basis-0 flex-col overflow-hidden bg-terminal-bg p-0">
            {tab === "scanner" ? (
              <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden p-4">
                <BreakingRiskBanner state={payload?.market_momentum_score?.state} message={payload?.market_momentum_score?.message} />

                {payload ? (
                  <ScannerView
                    payload={payload}
                    spotlightThemeName={spotlightThemeName}
                    setSpotlightThemeName={(s) => setSpotlightThemeName(s)}
                    themeQuery={leaderboardQuery}
                    setThemeQuery={setLeaderboardQuery}
                    intelPre={intelPre}
                    intelPost={intelPost}
                    intelLoading={intelBriefLoading}
                    onRefreshIntel={refreshIntel}
                  />
                ) : (
                  <div className="flex h-full items-center justify-center text-slate-500">Loading scanner…</div>
                )}
              </div>
            ) : tab === "breadth" ? (
              <MarketBreadth />
            ) : (
              <GappersView
                gappers={gappers}
                loading={gappersLoading}
                error={gappersError}
                filters={gapFilters}
                setFilters={setGapFilters}
                gapScannerGradeByTicker={gapScannerGradeByTicker}
                onSelectTicker={(t, meta) => {
                  setFocusTicker(t);
                  setFocusTickerMeta(meta ?? null);
                }}
              />
            )}
          </div>
          {focusTicker ? (
            <TickerDrawer
              ticker={focusTicker}
              meta={focusTickerMeta}
              onClose={() => {
                setFocusTicker(null);
                setFocusTickerMeta(null);
              }}
            />
          ) : null}
        </div>
      </div>
    </div>
  );
}

