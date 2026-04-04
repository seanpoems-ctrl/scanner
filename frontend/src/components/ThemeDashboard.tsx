import React, { Fragment, memo, useCallback, useEffect, useMemo, useRef, useState, type JSX } from "react";
import { AlertTriangle, BarChart2, BarChart3, ChevronRight, Info, LayoutGrid, ListPlus, Moon, Plus, Search, Star, Sunrise } from "lucide-react";
import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import MarketBreadth from "./MarketBreadth";
import { useWatchlist } from "../hooks/useWatchlist";
import { useSkyteRsIndustries } from "../hooks/useSkyteRsIndustries";
import { API_BASE_URL } from "../lib/apiBase";
import { lookupSkyteIndustry } from "../lib/skyteRs";
import { formatMoney, fmtPct, fmtPrice, pctClass } from "../lib/formatters";
import { RotationView } from "./RotationView";
import { WatchlistDrawer } from "./WatchlistDrawer";
import { ImpactBadge } from "./ui/ImpactBadge";
import { ErrorBanner } from "./ui/ErrorBanner";
import { EmptyState } from "./ui/EmptyState";
import { PanelLoading, SkeletonRows } from "./ui/SkeletonRows";
import { RefreshRow } from "./ui/RefreshRow";

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
  thematicLabel?: string;
  category?: string;
  adr?: number | null;
  adr_pct?: number | null;
  seed?: boolean;
  relativeStrength1M: number | null;
  perf1D?: number | null;
  perf1W?: number | null;
  perf1M?: number | null;
  perf3M?: number | null;
  perf6M?: number | null;
  perfYTD?: number | null;
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
  /** Finviz Themes map node id; used for `f=themes_{slug}` movers when in Themes leaderboard mode. */
  finvizThemeSlug?: string | null;
};

type ThemeIndustryRow = ApiTheme & {
  category?: string | null;
  adr?: number | null;
  adr_pct?: number | null;
};

const THEME_PARENT_MAP: Record<string, string> = {
  Semiconductors: "Artificial Intelligence",
  "Semiconductor Equipment & Materials": "Artificial Intelligence",
  "Software—Application": "Artificial Intelligence",
  "Software-Application": "Artificial Intelligence",
  "Information Technology Services": "Artificial Intelligence",
  "Communication Equipment": "Artificial Intelligence",
  "Computer Hardware": "Artificial Intelligence",
  "Aerospace & Defense": "Space & Defense",
  "Space - Data Analytics": "Space & Defense",
  "Space - Satellites": "Space & Defense",
  "Defense - Space Tech": "Space & Defense",
  "Commodities - Agri Biofuels": "Energy & Commodities",
  "Commodities - Energy Biofuels": "Energy & Commodities",
  "Commodities - Agri Grains": "Energy & Commodities",
  "Commodities - Agri Fertilizers": "Energy & Commodities",
  "Commodities - Energy Oil": "Energy & Commodities",
  "Energy - Clean Biofuels": "Energy & Commodities",
  "Energy - Base Majors": "Energy & Commodities",
  "Energy - Base Oil Production": "Energy & Commodities",
  "Telecom - SatCom": "Telecom & Connectivity",
  "Telecom-Fiber Optics": "Telecom & Connectivity",
  "Telecom-Infrastructure": "Telecom & Connectivity",
  "Agriculture - Indoor Farming": "Agriculture",
  "Agricultural Operations": "Agriculture",
  "Chemicals-Agricultural": "Agriculture",
  "Biotech - Gene Therapy": "Healthcare & Biotech",
  "Biotech - CRISPR": "Healthcare & Biotech",
  "Medical Devices": "Healthcare & Biotech",
  "Drug Manufacturers": "Healthcare & Biotech",
  "Health Information Services": "Healthcare & Biotech",
  "Fintech - Payments": "Fintech & Finance",
  "Fintech - Lending": "Fintech & Finance",
  "Crypto - Digital Assets": "Fintech & Finance",
  "Capital Markets": "Fintech & Finance",
  Insurance: "Fintech & Finance",
  "Utilities - Renewable": "Utilities & Infrastructure",
  "Utilities - Regulated Gas": "Utilities & Infrastructure",
  "Utilities - Electric Power": "Utilities & Infrastructure",
  "Infrastructure - REITs": "Utilities & Infrastructure",
  "Retail - E-Commerce": "Consumer & Retail",
  "Consumer Discretionary": "Consumer & Retail",
  "Auto Parts": "Consumer & Retail",
  "Mining - Gold/Silver": "Materials & Mining",
  "Chemicals - Specialty": "Materials & Mining",
  "Steel & Metals": "Materials & Mining",
};

/** Finviz themes map uses `Category · Sub` labels; map into rollup parents (no loose "Other" for known rows). */
function finvizThemeLedgerParent(themeName: string): string {
  if (themeName === "Biometrics · Gov Defense") return "Space & Defense";
  if (themeName === "Biometrics · Identity") return "Fintech & Finance";
  if (themeName.startsWith("Biometrics ·")) return "Artificial Intelligence";

  if (themeName === "Robotics · Medical") return "Healthcare & Biotech";
  if (themeName.startsWith("Healthcare ·")) return "Healthcare & Biotech";
  if (themeName.startsWith("Longevity ·")) return "Healthcare & Biotech";
  if (themeName === "Wearables · Medical") return "Healthcare & Biotech";
  if (themeName === "Real Estate · Healthcare") return "Healthcare & Biotech";

  if (themeName.startsWith("Fintech ·")) return "Fintech & Finance";
  if (themeName.startsWith("Blockchain ·")) return "Fintech & Finance";

  if (themeName === "Cloud · Data Centers" || themeName === "Hardware · Data Centers") return "Utilities & Infrastructure";
  if (themeName === "Energy · Base Utilities" || themeName === "Energy · Clean Utilities") return "Utilities & Infrastructure";
  if (themeName === "Transportation · Infrastructure") return "Utilities & Infrastructure";
  if (themeName === "Education · Infrastructure") return "Utilities & Infrastructure";
  if (themeName === "VR / AR · Infrastructure") return "Utilities & Infrastructure";
  if (themeName === "Entertainment · Infrastructure") return "Utilities & Infrastructure";
  if (themeName.startsWith("Real Estate ·")) return "Utilities & Infrastructure";

  if (themeName.startsWith("Consumer ·")) return "Consumer & Retail";
  if (themeName.startsWith("E-commerce ·")) return "Consumer & Retail";
  if (themeName.startsWith("Entertainment ·")) return "Consumer & Retail";
  if (themeName.startsWith("Social ·")) return "Consumer & Retail";
  if (themeName.startsWith("Smart Home ·")) return "Consumer & Retail";
  if (themeName.startsWith("VR / AR ·")) return "Consumer & Retail";
  if (themeName.startsWith("Wearables ·")) return "Consumer & Retail";
  if (themeName.startsWith("EV ·")) return "Consumer & Retail";
  if (themeName.startsWith("Nutrition ·")) return "Consumer & Retail";
  if (themeName.startsWith("Education ·")) return "Consumer & Retail";

  if (themeName.startsWith("Commodities · Metals")) return "Materials & Mining";

  if (themeName === "Environmental · Agriculture") return "Agriculture";
  if (themeName.startsWith("Agriculture ·")) return "Agriculture";
  if (themeName.startsWith("Commodities · Agri")) return "Agriculture";

  if (themeName.startsWith("Commodities · Energy")) return "Energy & Commodities";
  if (themeName.startsWith("Energy ·")) return "Energy & Commodities";

  if (themeName.startsWith("Space ·")) return "Space & Defense";
  if (themeName.startsWith("Defense ·")) return "Space & Defense";
  if (themeName === "Autonomous · Defense") return "Space & Defense";

  if (themeName.startsWith("Telecom ·")) return "Telecom & Connectivity";

  if (themeName.startsWith("AI ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Semiconductors ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Software ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Big Data ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Cloud ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Cybersecurity ·")) return "Artificial Intelligence";
  if (themeName.startsWith("IoT ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Quantum ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Hardware ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Automation ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Robotics ·")) return "Artificial Intelligence";
  if (themeName.startsWith("Autonomous ·")) return "Artificial Intelligence";

  if (themeName.startsWith("Environmental ·")) return "Energy & Commodities";

  if (themeName.startsWith("Nanotech · Medicine")) return "Healthcare & Biotech";
  if (themeName.startsWith("Nanotech · Energy")) return "Energy & Commodities";
  if (themeName.startsWith("Nanotech ·")) return "Materials & Mining";

  if (themeName.startsWith("Transportation ·")) return "Utilities & Infrastructure";

  const dot = themeName.indexOf(" · ");
  if (dot > 0) return themeName.slice(0, dot);
  return "Other";
}

function getParentCategory(themeName: string): string {
  return THEME_PARENT_MAP[themeName] ?? finvizThemeLedgerParent(themeName);
}

function avgFiniteField(rows: ApiTheme[], pick: (r: ApiTheme) => number | null | undefined): number | null {
  const vals = rows.map(pick).filter((x): x is number => x != null && Number.isFinite(x));
  return vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : null;
}

type ThemeParentGroup = {
  parent: string;
  rows: ApiTheme[];
  avgRs: number;
  avgPerf1D: number | null;
  avgPerf1W: number | null;
  avgPerf1M: number | null;
  avgPerf3M: number | null;
  avgPerf6M: number | null;
  sortScore: number;
};

function groupThemesByParent(themes: ApiTheme[]): ThemeParentGroup[] {
  const map = new Map<string, ApiTheme[]>();
  for (const t of themes) {
    const p = getParentCategory(t.theme);
    if (!map.has(p)) map.set(p, []);
    map.get(p)!.push(t);
  }
  return [...map.entries()]
    .map(([parent, rows]) => {
      const rsVals = rows.map((r) => r.relativeStrength1M).filter((x): x is number => x != null && Number.isFinite(x));
      const avgRs = rsVals.length ? rsVals.reduce((s, v) => s + v, 0) / rsVals.length : 0;
      const avgPerf1D = avgFiniteField(rows, (r) => r.perf1D);
      const avgPerf1W = avgFiniteField(rows, (r) => r.perf1W);
      const avgPerf1M = avgFiniteField(rows, (r) => r.perf1M);
      const avgPerf3M = avgFiniteField(rows, (r) => r.perf3M);
      const avgPerf6M = avgFiniteField(rows, (r) => r.perf6M);
      const sortScore = rsVals.length > 0 ? avgRs : (avgPerf1M ?? Number.NEGATIVE_INFINITY);
      return {
        parent,
        rows,
        avgRs,
        avgPerf1D,
        avgPerf1W,
        avgPerf1M,
        avgPerf3M,
        avgPerf6M,
        sortScore,
      };
    })
    .sort((a, b) => b.sortScore - a.sortScore);
}

function rsTextClass(rs: number | null | undefined): string {
  if (rs == null || !Number.isFinite(rs)) return "text-slate-500";
  if (rs >= 80) return "text-emerald-400";
  if (rs >= 50) return "text-amber-400";
  return "text-rose-400";
}

function rsBarColor(rs: number | null | undefined): string {
  if ((rs ?? 0) >= 80) return "#34d399";
  if ((rs ?? 0) >= 50) return "#fbbf24";
  return "#f87171";
}

function getIndustryCategory(row: ThemeIndustryRow): string {
  return (row.category ?? row.thematicLabel ?? row.sector ?? row.theme ?? "Uncategorized").trim() || "Uncategorized";
}

function getIndustryAdr(row: ThemeIndustryRow): number | null {
  const raw = row.adr ?? row.adr_pct;
  if (raw == null || !Number.isFinite(raw)) return null;
  return raw;
}

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

type IndustryMoversRow = { ticker: string; close: number; change_pct: number };

type IndustryMoversPayload = {
  ok: boolean;
  industry: string;
  parent_category?: string | null;
  detail?: string | null;
  top_gainers: IndustryMoversRow[];
  top_losers: IndustryMoversRow[];
  rest: IndustryMoversRow[];
  fetched_at_utc?: string;
};

type LeaderboardSubSpotlight =
  | {
      kind: "industry";
      parent: string;
      theme: string;
      relativeStrength1M: number | null;
      perf1D: number | null;
      perf1M: number | null;
      perfYTD: number | null;
      totalCount: number;
      leaders: string[];
    }
  | {
      kind: "finviz_theme";
      theme: string;
      finvizThemeSlug: string;
      relativeStrength1M: number | null;
      perf1D: number | null;
      perf1M: number | null;
      perfYTD: number | null;
      totalCount: number;
      leaders: string[];
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

/** Banner background for sub-industry spotlight (1D change). */
function subIndustryBannerShellClass(perf1D: number | null | undefined): string {
  if (perf1D == null || !Number.isFinite(perf1D)) {
    return "border-b border-terminal-border bg-terminal-elevated/80";
  }
  if (perf1D > 0) {
    return "border-b border-emerald-800/50 bg-emerald-950/90";
  }
  if (perf1D < 0) {
    return "border-b border-rose-900/50 bg-rose-950/90";
  }
  return "border-b border-terminal-border bg-terminal-elevated/80";
}

function industryThemeBadgeCode(theme: string): string {
  const t = (theme || "").trim();
  if (!t) return "—";
  const parts = t.split(/[\s/&,-]+/).filter(Boolean);
  const initials = parts
    .slice(0, 3)
    .map((p) => p[0])
    .join("")
    .toUpperCase();
  return initials.slice(0, 4) || t.slice(0, 3).toUpperCase();
}

function leaderboardSubSpotlightBadge(sp: LeaderboardSubSpotlight): string {
  if (sp.kind === "finviz_theme") {
    const compact = sp.finvizThemeSlug.replace(/[^a-z0-9]/gi, "");
    return compact.slice(0, 4).toUpperCase() || industryThemeBadgeCode(sp.theme);
  }
  return industryThemeBadgeCode(sp.theme);
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
        <h3 className="t-section">VIX Fear Gauge</h3>
      </header>
      <div className="px-4 py-3">
        <div className="flex items-baseline justify-between gap-3">
          <div className="min-w-0">
            <p className="t-micro">CBOE:VIX</p>
            <p className="mt-0.5 font-mono text-2xl font-extrabold tabular-nums" style={{ color: mood.color }}>
              {v == null ? "—" : v.toFixed(2)}
            </p>
          </div>
          <div className="text-right">
            <p className="t-label">Expected move</p>
            <p className="mt-0.5 t-mono text-slate-200 text-sm font-bold">{v == null ? "—" : `±${(v / 16).toFixed(2)}%`}</p>
            <p className={`mt-1 t-mono ${pctClass(changePct ?? 0)}`}>
              {changePct != null && changePct >= 0 ? "+" : ""}
              {changePct == null || !Number.isFinite(changePct) ? "—" : `${changePct.toFixed(2)}%`}
            </p>
          </div>
        </div>
        <div className="mt-3">
          <div className="mb-2 flex items-center justify-between t-micro">
            <span>{min}</span>
            <span className="font-semibold" style={{ color: mood.color }}>
              {mood.label}
            </span>
            <span>{max}</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-terminal-elevated/40">
            <div
              className="h-full"
              style={{ width: `${pct}%`, background: "linear-gradient(to right, #00e676, #ffee58, #ff9100, #ff1744)" }}
            />
          </div>
          <p className="mt-2 t-micro font-medium" style={{ color: mood.color }}>
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
        <h3 className="t-section">Market Regime</h3>
      </header>
      <div className="px-4 py-3">
        <div className={`rounded-lg border px-3 py-3 ${tone}`}>
          <p className="t-label opacity-90">{title}</p>
          <p className="mt-1 t-data text-[12px] font-semibold leading-snug">{message ?? "—"}</p>
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
        <h3 className="t-section">Liquidity &amp; Flow</h3>
      </header>
      <div className="grid grid-cols-2 gap-3 px-4 py-3 t-data">
        <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
          <p className="t-label">Aggregate $ Vol</p>
          <p className="mt-0.5 t-mono text-slate-200 text-sm font-bold">{dv == null ? "—" : formatMoney(dv)}</p>
          <p className="mt-0.5 t-micro">
            Prev: <span className="t-mono text-slate-400">{prev == null ? "—" : formatMoney(prev)}</span>
          </p>
        </div>
        <div className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2">
          <p className="t-label">Trend</p>
          <p className={`mt-0.5 text-sm font-bold ${cls}`}>{trend === "up" ? "Flow improving" : trend === "down" ? "Flow fading" : "—"}</p>
          <p className="mt-0.5 t-micro">
            20D avg: <span className="t-mono text-slate-400">{avg20 == null ? "—" : formatMoney(avg20)}</span>
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
        <h3 className="t-section">RS Snapshot</h3>
        <p className="mt-0.5 t-micro">Top themes by 1M RS</p>
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
          <EmptyState
            icon={BarChart3}
            title="No RS data yet"
            subtitle="Data loads with the first scanner payload."
          />
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
      className={`inline-flex items-center rounded-md border px-1.5 py-0.5 t-mono font-bold leading-none ${cls}`}
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
        : "border-slate-500/60 bg-terminal-elevated/60 text-slate-300";
  return (
    <span
      className={`inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full border t-micro font-bold leading-none ${ring}`}
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
  const isMountedRef = useRef(true);
  const fetchGenRef = useRef(0);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const fetchThemes = useCallback(async (signal?: AbortSignal) => {
    const gen = ++fetchGenRef.current;
    const gate = () => isMountedRef.current && (signal == null || !signal.aborted);

    try {
      if (!gate()) return;
      setLoading(true);
      setLoadingSince(Date.now());
      const res = await fetch(`${API_BASE_URL}/api/themes?view=scanner`, signal ? { signal } : undefined);
      const retryAfterRaw = res.headers.get("Retry-After");
      if (!res.ok) {
        if (res.status === 429 && retryAfterRaw) {
          const ra = Number(retryAfterRaw);
          if (Number.isFinite(ra) && ra > 0) {
            setPollMs(Math.min(8 * 60_000, Math.max(MIN_AUTO_REFRESH_MS, Math.round(ra * 1000))));
          }
        }
        if (res.status === 429) {
          if (!gate()) return;
          setError("themes 429");
          setPayload((prev) => prev ?? makeFallbackPayload(retryAfterRaw ? Number(retryAfterRaw) || 110 : 110));
          return;
        }
        throw new Error(`themes ${res.status}`);
      }
      const data = (await res.json()) as ApiPayload;
      if (!gate()) return;
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
        if (Number.isFinite(n) && n > 0) {
          setPollMs(Math.min(8 * 60_000, Math.max(MIN_AUTO_REFRESH_MS, Math.round(n * 1000))));
        }
      }
    } catch (e) {
      if (e instanceof Error && e.name === "AbortError") return;
      if (!gate()) return;
      setError(e instanceof Error ? e.message : "Failed to load themes");
    } finally {
      if (gen === fetchGenRef.current) {
        setLoading(false);
        setLoadingSince(null);
      }
    }
  }, []);

  useEffect(() => {
    const abortRef = { current: null as AbortController | null };

    const run = async () => {
      abortRef.current?.abort();
      const ac = new AbortController();
      abortRef.current = ac;
      await fetchThemes(ac.signal);
    };

    void run();
    const id = window.setInterval(() => void run(), pollMs);
    return () => {
      abortRef.current?.abort();
      window.clearInterval(id);
    };
  }, [pollMs, fetchThemes]);

  const reload = useCallback(async () => {
    await fetchThemes();
  }, [fetchThemes]);

  return { payload, error, reload, lastUpdatedAt, loading, loadingSince, pollMs };
}

function useFdvLeaderboard(view: "themes" | "industry") {
  const cacheKey = `power-theme:finviz-leaderboard:${view}:v3`;
  const [payload, setPayload] = useState<ApiPayload | null>(() => {
    try {
      const raw = window.localStorage.getItem(cacheKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { payload?: ApiPayload };
      // Discard cache entries with empty themes so stale blanks don't persist.
      const p = parsed?.payload ?? null;
      if (!p || !p.themes?.length) return null;
      return p;
    } catch {
      return null;
    }
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [retryAt, setRetryAt] = useState(0);

  useEffect(() => {
    let active = true;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    async function load() {
      try {
        setLoading(true);

        // Try static JSON file first (written by local_pusher_static.py via Vercel CDN).
        // This completely bypasses Render's IP blocking issues.
        let data: ApiPayload | null = null;
        try {
          const staticRes = await fetch(`/leaderboard-${view}.json?t=${Date.now()}`);
          if (staticRes.ok) {
            const staticData = (await staticRes.json()) as ApiPayload;
            if (staticData.themes?.length) {
              data = staticData;
            }
          }
        } catch {
          // static file not available — fall through to Render API
        }

        // Fall back to Render API if static file not available or empty.
        if (!data) {
          const res = await fetch(`${API_BASE_URL}/api/themes?view=${encodeURIComponent(view)}`);
          if (!res.ok) {
            if (res.status === 429) {
              if (!active) return;
              setError("finviz 429 — rate limited, retrying in 20 s…");
              retryTimer = setTimeout(() => { if (active) setRetryAt(Date.now()); }, 20_000);
              return;
            }
            throw new Error(`HTTP ${res.status}`);
          }
          data = (await res.json()) as ApiPayload;
        }

        if (!active) return;
        if (!data.themes?.length) {
          setError(null);
          setPayload(data);
          retryTimer = setTimeout(() => { if (active) setRetryAt(Date.now()); }, 15_000);
          return;
        }
        setPayload(data);
        setError(null);
        try {
          window.localStorage.setItem(cacheKey, JSON.stringify({ payload: data, savedAt: Date.now() }));
        } catch {
          // ignore quota errors
        }
      } catch (e) {
        if (!active) return;
        setError(e instanceof Error ? e.message : "Failed to load leaderboard");
        retryTimer = setTimeout(() => { if (active) setRetryAt(Date.now()); }, 20_000);
      } finally {
        if (!active) return;
        setLoading(false);
      }
    }
    void load();
    return () => {
      active = false;
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [view, cacheKey, retryAt]);

  return { payload, loading, error };
}

function TapeInline({ tape }: { tape: { label: string; symbol: string; close: number | null; change_pct: number | null }[] | undefined }) {
  if (!tape?.length) return null;
  return (
    <div className="flex flex-wrap items-center gap-y-2 t-data">
      {tape.slice(0, 8).map((t, idx) => (
        <Fragment key={`${t.symbol}-${t.label}`}>
          {idx > 0 ? <span className="mx-4 h-8 w-px shrink-0 bg-terminal-elevated/60" aria-hidden /> : null}
          <div className="flex min-w-[72px] flex-col items-center justify-center px-1 leading-tight">
            <span className="t-label font-mono">{t.label}</span>
            <div className="mt-0.5 flex items-baseline gap-2">
              <span className="t-mono text-slate-200 text-xs font-semibold">{t.close == null ? "—" : fmtPrice(t.close)}</span>
              <span className={`t-mono ${pctClass(t.change_pct ?? 0)}`}>{fmtPct(t.change_pct, 2)}</span>
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
    <aside
      className="h-full w-[420px] min-w-[420px] shrink-0 border-l border-terminal-border bg-terminal-elevated"
      role="complementary"
      aria-label={`Ticker details for ${ticker}`}
    >
      <div className="flex h-full min-h-0 flex-col">
        <header className="shrink-0 border-b border-terminal-border px-4 py-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="t-page font-mono">{ticker}</p>
              <p className="mt-0.5 truncate t-data text-slate-400">{intel?.name ?? (intelLoading ? "Loading…" : "—")}</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-1 text-xs font-semibold text-slate-300 hover:border-slate-500 hover:text-white"
              aria-label="Close ticker drawer"
            >
              Close
            </button>
          </div>
          <div className="mt-2 flex flex-wrap items-baseline gap-x-3 gap-y-1 t-data">
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
              <p className="t-section">Reasoning</p>
              <p className="mt-2 t-data">{meta.reasoning}</p>
            </section>
          ) : null}

          <section className="rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
            <p className="t-section">Classification</p>
            <div className="mt-2 grid grid-cols-[84px_1fr] gap-x-3 gap-y-1">
              <p className="t-label">Sector</p>
              <p className="truncate t-data font-semibold">{intel?.sector ?? meta?.sector ?? "—"}</p>
              <p className="t-label">Industry</p>
              <p className="truncate t-data font-semibold">{intel?.industry ?? meta?.industry ?? "—"}</p>
              <p className="t-label">Theme</p>
              <p className="truncate t-data font-semibold text-sky-200">{intel?.theme ?? meta?.theme ?? "—"}</p>
              <p className="t-label">Sub‑Theme</p>
              <p className="truncate t-data font-semibold text-violet-200">{intel?.subtheme ?? meta?.category ?? "—"}</p>
              <p className="t-label">Grade</p>
              <p className="truncate t-data font-semibold text-amber-200">{meta?.grade ?? "—"}</p>
            </div>
          </section>

          <section className="mt-3 rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="flex items-center justify-between gap-2 border-b border-terminal-border px-4 py-3">
              <p className="t-section">News / events (90D)</p>
              <span className="t-micro">{newsLoading ? "Loading…" : `${news.length}`}</span>
            </header>
            <div className="p-2">
              {!newsLoading && !news.length ? (
                <p className="px-2 py-6 text-center text-xs text-slate-500">No recent items returned.</p>
              ) : (
                <ul className="space-y-1">
                  {news.slice(0, 30).map((n, i) => (
                    <li key={`${n.published_at_utc ?? n.date_utc ?? "x"}-${i}`} className="rounded-lg border border-terminal-border/60 bg-terminal-bg/30 px-2.5 py-2">
                      <div className="flex items-baseline justify-between gap-3">
                        <span className="t-label">{n.event_type || "News"}</span>
                        <span className="t-mono text-slate-600">{n.date_utc ?? "—"}</span>
                      </div>
                      {n.link ? (
                        <a href={n.link} target="_blank" rel="noopener noreferrer" className="mt-1 block t-data hover:underline">
                          {n.title}
                        </a>
                      ) : (
                        <p className="mt-1 t-data">{n.title}</p>
                      )}
                      <p className="mt-1 t-micro">{n.source}</p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </section>

          <p className="mt-3 t-micro">
            Note: This drawer currently uses best‑effort Yahoo Finance data (intel/news/earnings). Insider/institution/analyst target changes can be added next.
          </p>
          <p className="mt-2 t-micro text-slate-600">Press Esc to close this drawer.</p>
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
    <div className={`shrink-0 rounded-xl border px-4 py-2 t-data shadow-sm ${ring}`}>
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

// ── Catalyst structured table ──────────────────────────────────────────────
// Splits "Bullish. Rest of sentence" → colored first word + dimmer rest
function ImpactText({ text, size }: { text: string; size: string }) {
  const sentimentMatch = text.match(/^(Bullish|Bearish|Mixed|Neutral)\.\s*/i);
  if (!sentimentMatch) {
    return <span className={`text-slate-400 ${size}`}>{text}</span>;
  }
  const sentiment = sentimentMatch[1];
  const rest = text.slice(sentimentMatch[0].length);
  const sentColor =
    sentiment.toLowerCase() === "bullish"
      ? "text-emerald-400 font-bold"
      : sentiment.toLowerCase() === "bearish"
      ? "text-rose-400 font-bold"
      : "text-amber-300 font-bold";
  return (
    <span className={size}>
      <span className={sentColor}>{sentiment}.</span>
      {rest && <span className="text-slate-400"> {rest}</span>}
    </span>
  );
}

function CatalystTable({ rows, isModal = false }: { rows: CatalystRow[]; isModal?: boolean }) {
  if (!rows || rows.length === 0) return null;
  const th = isModal ? "t-label tracking-widest" : "t-micro font-semibold uppercase tracking-widest text-slate-500";
  const td = isModal ? "text-[13px] leading-relaxed text-slate-200" : "t-data";
  return (
    <div className="my-2 overflow-x-auto rounded-lg border border-slate-800/80">
      <table className="w-full">
        <caption className="sr-only">Catalyst events referenced in the intelligence brief.</caption>
        <thead>
          <tr className="border-b border-slate-800/60 bg-terminal-bg/50">
            {["Catalyst", "Data / Event", "Market Impact", "Level"].map((h) => (
              <th key={h} scope="col" className={`px-4 py-2 text-left ${th}`}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} className="border-b border-slate-800/60 transition-colors hover:bg-terminal-bg/30">
              {/* Catalyst — bold white */}
              <td className={`px-4 py-3 font-semibold text-slate-100 ${td}`}>{row.catalyst}</td>
              {/* Event — bold, slightly brighter */}
              <td className={`px-4 py-3 font-bold text-slate-100 ${td}`}>{row.event}</td>
              {/* Impact — sentiment-colored first word + dimmer rest */}
              <td className="px-4 py-3">
                <ImpactText text={row.impact} size={td} />
              </td>
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
// isModal=true applies 20% larger text and tighter leading throughout.
function renderBriefMarkdown(brief: IntelBrief, isModal: boolean = false): JSX.Element[] {
  // Replace both ```json_catalysts and ```json fences containing the catalyst
  // array with a sentinel so CatalystTable can be injected at that position.
  // Scale classes: panel uses compact sizes; modal bumps everything ~20%
  const sz = {
    genHeader:   isModal ? "text-sm font-mono" : "t-mono",
    section:     isModal ? "t-label tracking-widest text-slate-400" : "t-micro font-semibold uppercase tracking-widest text-slate-400",
    body:        isModal ? "text-sm" : "t-data",
    bullet:      isModal ? "text-sm" : "t-data",
    blockquote:  isModal ? "text-sm" : "t-data",
    thCell:      "t-micro font-semibold uppercase tracking-widest text-slate-500",
    tdCell:      isModal ? "text-[12px]" : "t-data",
    leading:     isModal ? "leading-snug"  : "leading-relaxed",
  };

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
          <table className="w-full">
            <caption className="sr-only">Data table from intelligence brief.</caption>
            <thead>
              <tr className="border-b border-slate-800/60 bg-terminal-bg/50">
                {headers.map((h) => (
                  <th key={h} scope="col" className={`px-4 py-2 text-left font-semibold uppercase tracking-widest text-slate-500 ${sz.thCell}`}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {bodyRows.map((row, ri) => {
                const cells = row.split("|").filter(Boolean).map((c) => c.trim());
                return (
                  <tr key={ri} className="border-b border-slate-800/50 hover:bg-terminal-bg/30">
                    {cells.map((c, ci) => (
                      <td key={ci} className={`px-4 py-2.5 text-slate-300 ${sz.tdCell}`}><InlineBold text={c} /></td>
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
      rendered.push(<CatalystTable key={`cat-${i}`} rows={brief.catalysts ?? []} isModal={isModal} />);
      return;
    }

    if (trimmed.startsWith("## ")) {
      // In the side panel the Gen timestamp is already shown in the card header —
      // suppress the duplicate rounded box. In the modal it's useful as a section
      // anchor so render it there only.
      if (isModal) {
        rendered.push(
          <div key={i} className="mt-4 flex items-center gap-2 rounded-lg border border-accent/30 bg-terminal-elevated px-3 py-2">
            <p className={`font-bold text-accent ${sz.genHeader}`}>{trimmed.slice(3)}</p>
          </div>
        );
      }
      // side panel: skip entirely — timestamp lives in the header row
    } else if (trimmed.startsWith("### ")) {
      rendered.push(
        <h3 key={i} className={`mt-4 border-b border-terminal-border/40 pb-1 ${sz.section}`}>
          {trimmed.slice(4)}
        </h3>
      );
    } else if (trimmed.startsWith("> ")) {
      rendered.push(
        <blockquote key={i} className={`my-2 rounded-r border-l-2 border-accent/60 bg-terminal-elevated/50 py-2 pl-3 pr-2 italic text-slate-200 ${sz.blockquote}`}>
          <InlineBold text={trimmed.slice(2)} />
        </blockquote>
      );
    } else if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
      rendered.push(
        <p key={i} className={`flex gap-2 text-slate-300 ${sz.bullet} ${sz.leading}`}>
          <span className="mt-0.5 shrink-0 text-slate-600">•</span>
          <span><InlineBold text={trimmed.slice(2)} /></span>
        </p>
      );
    } else if (trimmed === "---") {
      // Skip the very first horizontal rule (it sits right after ## Gen header
      // and creates a redundant top border before Pillar 1).
      if (rendered.length > 0) {
        rendered.push(<hr key={i} className="my-3 border-terminal-border/50" />);
      }
    } else if (trimmed.startsWith("```") || trimmed === "") {
      // skip code fences and blank lines
    } else {
      rendered.push(
        <p key={i} className={`text-slate-300 ${sz.body} ${sz.leading}`}>
          <InlineBold text={trimmed} />
        </p>
      );
    }
  });
  if (inMdTable) flushMarkdownTable();
  return rendered;
}

// ── Main panel (side drawer) + Focus Modal ─────────────────────────────────
// IntelBriefPanel — compact scrollable side view (OPEN button lives in IntelBriefCard header)
function IntelBriefPanel({
  brief,
  loading,
}: {
  brief: IntelBrief | null;
  loading: boolean;
}) {
  if (loading) {
    return (
      <div className="flex flex-col gap-2 p-3">
        <div className="h-3 w-3/4 animate-pulse rounded bg-terminal-border" />
        <div className="h-3 w-full animate-pulse rounded bg-terminal-border" />
        <div className="h-3 w-5/6 animate-pulse rounded bg-terminal-border" />
        <p className="mt-2 t-micro">Generating intelligence brief…</p>
      </div>
    );
  }
  if (!brief?.markdown) {
    return (
      <p className="p-3 t-data text-slate-600">
        Brief scheduled for next market session. Will appear automatically at 8:03 AM ET (pre) or 4:55 PM ET (post).
      </p>
    );
  }

  const rendered = renderBriefMarkdown(brief, false);

  return (
    <article className="space-y-1 p-3">
      {rendered}
    </article>
  );
}

// ── Updated MarketBriefCard using Intelligence Brief ──────────────────────

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
  const [isExpanded, setIsExpanded] = useState(false);
  const brief = mode === "pre" ? pre : post;
  const now_et_h = new Date().toLocaleString("en-US", { timeZone: "America/New_York", hour: "numeric", hour12: false });
  const autoMode: "pre" | "post" = Number(now_et_h) < 17 ? "pre" : "post";

  useEffect(() => {
    if (!isExpanded) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsExpanded(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isExpanded]);

  return (
    <>
      <section className="flex min-h-0 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
        <header className="shrink-0 border-b border-terminal-border px-3 py-1.5">
          {/* Single horizontal line: icon · title · mode · gen-time | controls */}
          <div className="flex items-center justify-between gap-x-3">
            {/* ── Left: icon + title + mode label + gen timestamp ── */}
            <div className="flex min-w-0 items-center gap-x-1.5 overflow-hidden">
              {mode === "pre"
                ? <Sunrise className="h-3.5 w-3.5 shrink-0 text-amber-300" aria-hidden />
                : <Moon className="h-3.5 w-3.5 shrink-0 text-sky-300" aria-hidden />}
              <span className="shrink-0 t-section">
                Market Intelligence
              </span>
              <span className="shrink-0 t-micro">—</span>
              <span className="shrink-0 t-label">
                {mode === "pre" ? "Pre-Market" : "Post-Market"}
              </span>
              {brief?.gen_time_et && (
                <span className="ml-2 shrink-0 t-mono text-slate-600">
                  Gen {brief.gen_time_et}
                </span>
              )}
            </div>

            {/* ── Right: PRE/POST toggle · OPEN · refresh ── */}
            <div className="flex shrink-0 items-center gap-x-1.5">
              <div className="flex items-center gap-1 rounded-full border border-terminal-border bg-terminal-bg p-0.5">
                <button type="button" onClick={() => onModeChange("pre")}
                  className={`rounded-full px-2 py-0.5 t-micro font-semibold transition-colors ${mode === "pre" ? "bg-accent/20 text-white" : "text-slate-500 hover:text-white"}`}>
                  PRE
                </button>
                <button type="button" onClick={() => onModeChange("post")}
                  className={`rounded-full px-2 py-0.5 t-micro font-semibold transition-colors ${mode === "post" ? "bg-accent/20 text-white" : "text-slate-500 hover:text-white"}`}>
                  POST
                </button>
              </div>
              {/* OPEN — visible border, hover fill */}
              <button
                type="button"
                disabled={!brief?.markdown}
                onClick={() => setIsExpanded(true)}
                aria-haspopup="dialog"
                aria-expanded={isExpanded}
                className="rounded border border-slate-500/60 bg-transparent px-2 py-0.5 t-mono font-semibold text-slate-400 transition-colors hover:border-slate-400 hover:bg-terminal-elevated/40 hover:text-white disabled:cursor-not-allowed disabled:opacity-30"
              >
                OPEN
              </button>
              <button type="button" disabled={loading} title="Regenerate brief"
                onClick={() => onRefresh(mode)}
                className="rounded-md border border-terminal-border bg-terminal-bg px-2 py-0.5 t-micro font-semibold text-slate-500 hover:border-accent/30 hover:text-white disabled:opacity-40">
                {loading ? "…" : "↺"}
              </button>
            </div>
          </div>
          {autoMode !== mode && (
            <p className="mt-0.5 t-micro">
              Auto-display: {autoMode === "pre" ? "Pre-Market" : "Post-Market"} · switch above to compare
            </p>
          )}
        </header>
        <div className="fintech-scroll min-h-0 flex-1 overflow-y-auto">
          <IntelBriefPanel brief={brief} loading={loading} />
        </div>
      </section>

      {/* ── Focus Modal ── */}
      {isExpanded && brief?.markdown && (
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="intel-brief-modal-title"
          className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-terminal-bg/90 backdrop-blur-xl"
          onClick={(e) => { if (e.target === e.currentTarget) setIsExpanded(false); }}
        >
          <div className="relative mx-auto my-8 w-full max-w-3xl rounded-2xl border border-terminal-border bg-terminal-card shadow-2xl">
            {/* Modal header */}
            <div className="sticky top-0 z-10 flex items-center justify-between rounded-t-2xl border-b border-terminal-border bg-terminal-card px-5 py-3">
              <div className="flex items-center gap-2">
                {brief.brief_type === "pre"
                  ? <Sunrise className="h-4 w-4 text-amber-300" aria-hidden />
                  : <Moon className="h-4 w-4 text-sky-300" aria-hidden />}
                <span id="intel-brief-modal-title" className="text-[12px] font-bold uppercase tracking-widest text-white">
                  Market Intelligence — {brief.brief_type === "pre" ? "Pre-Market" : "Post-Market"}
                </span>
                {brief.gen_time_et && (
                  <span className="ml-2 t-mono text-accent">
                    Gen {brief.gen_time_et}
                  </span>
                )}
              </div>
              <button
                type="button"
                onClick={() => setIsExpanded(false)}
                className="rounded-full p-1 text-slate-500 transition-colors hover:bg-terminal-elevated hover:text-white"
                aria-label="Close intelligence brief"
              >
                ✕
              </button>
            </div>
            {/* Modal body — 20% larger base text, tighter leading */}
            <article className="space-y-2 px-6 py-5 text-sm leading-snug">
              {renderBriefMarkdown(brief, true)}
            </article>
            <p className="px-6 pb-5 t-micro text-slate-600">Press Esc to close.</p>
          </div>
        </div>
      )}
    </>
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
    const themeName = (label ?? "").trim();
    fetch(`${API_BASE_URL}/api/theme-universe/spotlight?label=${encodeURIComponent(themeName)}`)
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

function useLeaderboardSubMovers(sel: LeaderboardSubSpotlight | null, enabled: boolean) {
  const [data, setData] = useState<IndustryMoversPayload | null>(null);
  const [loading, setLoading] = useState(false);

  const industryKey = sel?.kind === "industry" ? `${sel.parent}\0${sel.theme}` : "";
  const finvizKey = sel?.kind === "finviz_theme" ? `${sel.finvizThemeSlug}\0${sel.theme}` : "";

  useEffect(() => {
    if (!enabled || !sel) {
      setData(null);
      setLoading(false);
      return;
    }
    let active = true;
    setLoading(true);
    const url =
      sel.kind === "industry"
        ? (() => {
            const p = new URLSearchParams();
            p.set("industry", sel.theme);
            p.set("parent", sel.parent);
            return `${API_BASE_URL}/api/industry/subindustry-movers?${p.toString()}`;
          })()
        : (() => {
            const p = new URLSearchParams();
            p.set("slug", sel.finvizThemeSlug);
            p.set("label", sel.theme);
            return `${API_BASE_URL}/api/themes/finviz-movers?${p.toString()}`;
          })();

    fetch(url)
      .then(async (r) => {
        if (!r.ok) throw new Error(`sub-spotlight movers ${r.status}`);
        return (await r.json()) as IndustryMoversPayload;
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
  }, [enabled, industryKey, finvizKey]);

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
  watchlisted,
  onToggleWatchlist,
  onSelectTicker,
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
  watchlisted: Set<string>;
  onToggleWatchlist: (ticker: string, ctx?: { theme?: string; sector?: string; grade?: string }) => void | Promise<void>;
  onSelectTicker: (ticker: string, meta?: TickerDrawerMeta) => void;
}) {
  const [leaderboardMode, setLeaderboardMode] = useState<"themes" | "industry">("themes");
  const skyteRsEnabled = leaderboardMode === "industry";
  const { lookupMap: skyteIndustryMap, loading: skyteRsLoading } = useSkyteRsIndustries(skyteRsEnabled);
  // drilldownLabel: when set (industry mode only), filter rows to this thematic bucket.
  const [drilldownLabel, setDrilldownLabel] = useState<string | null>(null);
  // expandedTheme: for accordion-style industry grouping
  const [expandedTheme, setExpandedTheme] = useState<string | null>(null);
  const [leaderboardSubSpotlight, setLeaderboardSubSpotlight] = useState<LeaderboardSubSpotlight | null>(null);
  const [expandedParents, setExpandedParents] = useState<Set<string>>(new Set());
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

  // Reset accordion state when switching modes or drill-down changes
  useEffect(() => {
    setExpandedTheme(null);
  }, [leaderboardMode, drilldownLabel]);

  useEffect(() => {
    setLeaderboardSubSpotlight(null);
  }, [leaderboardMode]);

  useEffect(() => {
    setExpandedParents(new Set());
  }, [leaderboardMode]);

  const toggleParent = useCallback((parent: string) => {
    setExpandedParents((prev) => {
      const next = new Set(prev);
      if (next.has(parent)) next.delete(parent);
      else next.add(parent);
      return next;
    });
  }, []);

  const activateThemeLeaderboardRow = useCallback(
    (row: ApiTheme) => {
      setSpotlightThemeName(row.theme);
      const finvizSlug = (row.finvizThemeSlug ?? "").trim();
      if (finvizSlug) {
        setLeaderboardSubSpotlight({
          kind: "finviz_theme",
          theme: row.theme,
          finvizThemeSlug: finvizSlug,
          relativeStrength1M: row.relativeStrength1M,
          perf1D: row.perf1D ?? null,
          perf1M: row.perf1M ?? null,
          perfYTD: row.perfYTD ?? null,
          totalCount: row.totalCount ?? 0,
          leaders: row.leaders ?? [],
        });
      } else {
        setLeaderboardSubSpotlight(null);
      }
    },
    [setSpotlightThemeName]
  );

  const { payload: finvizLeaderboardPayload, loading: finvizLbLoading, error: finvizLbError } = useFdvLeaderboard(leaderboardMode);
  const finvizRows = finvizLeaderboardPayload?.themes ?? [];
  const finvizFilteredRows = useMemo(() => {
    let rows = finvizRows;
    // In industry mode, if a thematic drilldown is active, restrict to that bucket.
    if (leaderboardMode === "industry" && drilldownLabel) {
      rows = rows.filter((t) => (t.thematicLabel ?? "") === drilldownLabel);
    }
    const q = themeQuery.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((t) =>
      t.theme.toLowerCase().includes(q) ||
      (t.sector ?? "").toLowerCase().includes(q) ||
      (t.thematicLabel ?? "").toLowerCase().includes(q) ||
      getIndustryCategory(t).toLowerCase().includes(q)
    );
  }, [finvizRows, themeQuery, leaderboardMode, drilldownLabel]);

  // Group industries by category for accordion view
  const groupedThemes = useMemo(() => {
    if (leaderboardMode !== "industry") return [];
    
    const groups = new Map<string, ThemeIndustryRow[]>();
    
    finvizFilteredRows.forEach((row) => {
      const category = getIndustryCategory(row);
      if (!groups.has(category)) {
        groups.set(category, []);
      }
      groups.get(category)!.push(row);
    });

    // Convert to array and sort each group's industries by the selected column
    const result = Array.from(groups.entries()).map(([category, industries]) => {
      const sortedIndustries = [...industries];
      
      // Sort sub-industries by the selected sort key
      if (sortKey) {
        const dir = sortDir === "asc" ? 1 : -1;
        const num = (n: number | null | undefined) => (n == null || !Number.isFinite(n) ? Number.NEGATIVE_INFINITY : n);
        
        sortedIndustries.sort((a, b) => {
          if (sortKey === "theme") return a.theme.localeCompare(b.theme) * dir;
          if (sortKey === "perf1D") return (num(a.perf1D) - num(b.perf1D)) * dir;
          if (sortKey === "perf1W") return (num(a.perf1W) - num(b.perf1W)) * dir;
          if (sortKey === "perf1M") return (num(a.perf1M) - num(b.perf1M)) * dir;
          if (sortKey === "perf3M") return (num(a.perf3M) - num(b.perf3M)) * dir;
          if (sortKey === "perf6M") return (num(a.perf6M) - num(b.perf6M)) * dir;
          if (sortKey === "rs1m") return (num(a.relativeStrength1M) - num(b.relativeStrength1M)) * dir;
          if (sortKey === "qual") return (num(a.relativeStrengthQualifierRatio) - num(b.relativeStrengthQualifierRatio)) * dir;
          return 0;
        });
      } else {
        // Default sort by 1W performance (descending)
        sortedIndustries.sort((a, b) => {
          const perfA = a.perf1W ?? -Infinity;
          const perfB = b.perf1W ?? -Infinity;
          return perfB - perfA;
        });
      }

      // Calculate aggregate performance for the parent row
      const validIndustries = industries.filter(i => !i.seed);
      const avgPerf1D = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.perf1D ?? 0), 0) / validIndustries.length : null;
      const avgPerf1W = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.perf1W ?? 0), 0) / validIndustries.length : null;
      const avgPerf1M = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.perf1M ?? 0), 0) / validIndustries.length : null;
      const avgPerf3M = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.perf3M ?? 0), 0) / validIndustries.length : null;
      const avgPerf6M = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.perf6M ?? 0), 0) / validIndustries.length : null;
      const avgRS = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.relativeStrength1M ?? 0), 0) / validIndustries.length : null;
      const avgQual = validIndustries.length > 0 ? validIndustries.reduce((sum, i) => sum + (i.relativeStrengthQualifierRatio ?? 0), 0) / validIndustries.length : null;

      return {
        category,
        industries: sortedIndustries,
        avgPerf1D,
        avgPerf1W,
        avgPerf1M,
        avgPerf3M,
        avgPerf6M,
        avgRS,
        avgQual
      };
    });

    // Sort groups by the selected sort key, or default to 1W performance
    const sortByKey = sortKey || "perf1W";
    const dir = sortDir === "asc" ? 1 : -1;
    
    result.sort((a, b) => {
      const getValue = (group: typeof result[0]) => {
        switch (sortByKey) {
          case "theme": return group.category;
          case "perf1D": return group.avgPerf1D ?? -Infinity;
          case "perf1W": return group.avgPerf1W ?? -Infinity;
          case "perf1M": return group.avgPerf1M ?? -Infinity;
          case "perf3M": return group.avgPerf3M ?? -Infinity;
          case "perf6M": return group.avgPerf6M ?? -Infinity;
          case "rs1m": return group.avgRS ?? -Infinity;
          case "qual": return group.avgQual ?? -Infinity;
          default: return group.avgPerf1W ?? -Infinity;
        }
      };
      
      const valueA = getValue(a);
      const valueB = getValue(b);
      
      if (typeof valueA === "string" && typeof valueB === "string") {
        return valueA.localeCompare(valueB) * dir;
      } else {
        return (Number(valueB) - Number(valueA)) * dir;
      }
    });

    // Never render accordion parents with zero children (avoids empty Uncategorized / Discretionary headers)
    return result.filter((g) => g.industries.length > 0);
  }, [finvizFilteredRows, leaderboardMode, sortKey, sortDir]);
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

  const themesLeaderboardGroups = useMemo(() => {
    if (leaderboardMode !== "themes") return [];
    return groupThemesByParent(sortedLeaderboardRows.slice(0, 160));
  }, [leaderboardMode, sortedLeaderboardRows]);

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

  const showLeaderboardSubSpot = leaderboardSubSpotlight !== null;
  const { data: uniSpotlight } = useUniverseSpotlight(
    showLeaderboardSubSpot ? null : spotlightTheme?.theme ?? null
  );
  const { data: industryMovers, loading: industryMoversLoading } = useLeaderboardSubMovers(
    leaderboardSubSpotlight,
    showLeaderboardSubSpot
  );

  const industrySpotlightMoverStats = useMemo(() => {
    if (!industryMovers?.ok) return null;
    const all = [
      ...(industryMovers.top_gainers ?? []),
      ...(industryMovers.top_losers ?? []),
      ...(industryMovers.rest ?? []),
    ];
    const byTicker = new Map<string, IndustryMoversRow>();
    for (const r of all) {
      if (r?.ticker) byTicker.set(r.ticker, r);
    }
    const rows = [...byTicker.values()];
    const n = rows.length;
    const avgPrice = n > 0 ? rows.reduce((s, r) => s + r.close, 0) / n : null;
    return { n, avgPrice, rows };
  }, [industryMovers]);

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
        <div className="overflow-hidden rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
          {leaderboardSubSpotlight ? (
            <>
              <div className={`px-4 pb-3.5 pt-3 ${subIndustryBannerShellClass(leaderboardSubSpotlight.perf1D)}`}>
                <p className="t-micro font-semibold uppercase tracking-widest text-white/55">Thematic spotlight</p>
                <div className="mt-2 flex items-start justify-between gap-2">
                  <div className="flex min-w-0 flex-1 items-start gap-2.5">
                    <LayoutGrid className="mt-0.5 h-4 w-4 shrink-0 text-white/85" aria-hidden />
                    <div className="min-w-0">
                      <p className="truncate text-[13px] font-bold leading-tight text-white">{leaderboardSubSpotlight.theme}</p>
                      <p className="mt-1 flex items-center gap-1 font-mono text-xs font-semibold tabular-nums text-white/95">
                        {leaderboardSubSpotlight.perf1D != null && Number.isFinite(leaderboardSubSpotlight.perf1D) ? (
                          leaderboardSubSpotlight.perf1D > 0 ? (
                            <span className="text-white">▲</span>
                          ) : leaderboardSubSpotlight.perf1D < 0 ? (
                            <span className="text-white">▼</span>
                          ) : null
                        ) : null}
                        <span>
                          {leaderboardSubSpotlight.perf1D != null && Number.isFinite(leaderboardSubSpotlight.perf1D)
                            ? fmtPct(leaderboardSubSpotlight.perf1D, 2)
                            : "—"}
                        </span>
                        <span className="t-micro font-normal text-white/70">1D</span>
                      </p>
                    </div>
                  </div>
                  <span className="shrink-0 rounded-full border border-white/20 bg-terminal-bg/30 px-2.5 py-1 text-center t-micro font-semibold leading-tight text-white/90 backdrop-blur-sm">
                    <span className="font-mono">{leaderboardSubSpotlightBadge(leaderboardSubSpotlight)}</span>
                    <span className="mx-1 text-white/40">·</span>
                    <span>{industrySpotlightMoverStats?.n ?? leaderboardSubSpotlight.totalCount}</span>
                    <span className="text-white/60"> stocks</span>
                  </span>
                </div>
              </div>
              <div className="space-y-3 px-3 py-3">
                <div className="grid grid-cols-3 gap-2">
                  {(
                    [
                      {
                        label: "Avg price",
                        value:
                          industrySpotlightMoverStats?.avgPrice != null &&
                          Number.isFinite(industrySpotlightMoverStats.avgPrice)
                            ? `$${industrySpotlightMoverStats.avgPrice.toFixed(2)}`
                            : "—",
                        valueClass: "text-slate-100",
                      },
                      {
                        label: "Avg 1-month",
                        value:
                          leaderboardSubSpotlight.perf1M != null && Number.isFinite(leaderboardSubSpotlight.perf1M)
                            ? fmtPct(leaderboardSubSpotlight.perf1M, 1)
                            : "—",
                        valueClass:
                          leaderboardSubSpotlight.perf1M != null && Number.isFinite(leaderboardSubSpotlight.perf1M)
                            ? pctClass(leaderboardSubSpotlight.perf1M)
                            : "text-slate-500",
                      },
                      {
                        label: "Avg YTD",
                        value:
                          leaderboardSubSpotlight.perfYTD != null && Number.isFinite(leaderboardSubSpotlight.perfYTD)
                            ? fmtPct(leaderboardSubSpotlight.perfYTD, 1)
                            : "—",
                        valueClass:
                          leaderboardSubSpotlight.perfYTD != null && Number.isFinite(leaderboardSubSpotlight.perfYTD)
                            ? pctClass(leaderboardSubSpotlight.perfYTD)
                            : "text-slate-500",
                      },
                    ] as const
                  ).map((cell) => (
                    <div
                      key={cell.label}
                      className="rounded-lg border border-terminal-border/70 bg-terminal-bg/50 px-2 py-2 text-center"
                    >
                      <p className="t-label">{cell.label}</p>
                      <p className={`mt-1 text-sm font-semibold tabular-nums ${cell.valueClass}`}>{cell.value}</p>
                    </div>
                  ))}
                </div>
                <p className="t-micro text-slate-500">
                  RS{" "}
                  <span className="font-mono text-slate-400">
                    {leaderboardSubSpotlight.relativeStrength1M == null ||
                    !Number.isFinite(leaderboardSubSpotlight.relativeStrength1M)
                      ? "—"
                      : leaderboardSubSpotlight.relativeStrength1M.toFixed(1)}
                  </span>
                  <span className="mx-2 text-slate-700">|</span>
                  Leaders{" "}
                  <span className="font-mono text-slate-400">
                    {leaderboardSubSpotlight.leaders.slice(0, 5).join(", ") || "—"}
                  </span>
                </p>
                {industryMoversLoading ? (
                  <p className="text-xs text-slate-500">Loading movers…</p>
                ) : industryMovers && !industryMovers.ok ? (
                  <p className="text-xs text-rose-300/90">
                    {industryMovers.detail ?? "Could not load movers for this industry."}
                  </p>
                ) : (
                  <>
                    <div>
                      <p className="mb-2 t-label">
                        Today&apos;s movers
                      </p>
                      <div className="space-y-2.5">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="w-14 shrink-0 t-micro font-bold text-emerald-400">▲ Best</span>
                          <div className="flex min-w-0 flex-1 flex-wrap gap-1.5">
                            {(industryMovers?.top_gainers ?? []).map((r) => (
                              <span
                                key={`g-${r.ticker}`}
                                className="inline-flex items-center gap-1 rounded-full border border-emerald-800/50 bg-emerald-950/35 px-2 py-0.5 t-mono font-semibold text-emerald-300"
                              >
                                {r.ticker}
                                <span className={pctClass(r.change_pct)}>
                                  {r.change_pct >= 0 ? "+" : ""}
                                  {r.change_pct.toFixed(1)}%
                                </span>
                              </span>
                            ))}
                            {!(industryMovers?.top_gainers?.length) ? (
                              <span className="t-micro">—</span>
                            ) : null}
                          </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="w-14 shrink-0 t-micro font-bold text-rose-400">▼ Worst</span>
                          <div className="flex min-w-0 flex-1 flex-wrap gap-1.5">
                            {(industryMovers?.top_losers ?? []).map((r) => (
                              <span
                                key={`l-${r.ticker}`}
                                className="inline-flex items-center gap-1 rounded-full border border-rose-900/50 bg-rose-950/35 px-2 py-0.5 t-mono font-semibold text-rose-300"
                              >
                                {r.ticker}
                                <span className={pctClass(r.change_pct)}>
                                  {r.change_pct >= 0 ? "+" : ""}
                                  {r.change_pct.toFixed(1)}%
                                </span>
                              </span>
                            ))}
                            {!(industryMovers?.top_losers?.length) ? (
                              <span className="t-micro">—</span>
                            ) : null}
                          </div>
                        </div>
                      </div>
                    </div>
                    <div>
                      <p className="mb-2 t-label">All stocks</p>
                      <div className="max-h-36 overflow-y-auto rounded-lg border border-terminal-border/60 bg-terminal-bg/40 p-2">
                        {industrySpotlightMoverStats?.rows?.length ? (
                          <div className="flex flex-wrap gap-1.5">
                            {[...industrySpotlightMoverStats.rows]
                              .sort((a, b) => a.ticker.localeCompare(b.ticker))
                              .map((r) => (
                                <span
                                  key={`cloud-${r.ticker}`}
                                  className="rounded-md border border-slate-700/90 bg-terminal-elevated/80 px-2 py-1 t-mono font-medium text-slate-200"
                                >
                                  {r.ticker}
                                </span>
                              ))}
                          </div>
                        ) : (
                          <p className="t-micro">No ticker universe yet.</p>
                        )}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </>
          ) : (
            <>
              <header className="border-b border-terminal-border px-4 py-3">
                <h3 className="t-section">Thematic Spotlight</h3>
                <p className="mt-0.5 truncate t-micro">{spotlightTheme?.theme ?? "—"}</p>
              </header>
              <div className="px-4 py-3">
            {!spotlightTheme && !leaderboardSubSpotlight ? (
              <p className="text-xs text-slate-500">No theme selected.</p>
            ) : spotlightTheme ? (
              <>
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-[12px] font-semibold text-white">{spotlightTheme.theme}</p>
                  <p className="t-mono text-slate-400">
                    RS{" "}
                    {spotlightTheme.relativeStrength1M == null || !Number.isFinite(spotlightTheme.relativeStrength1M)
                      ? "—"
                      : spotlightTheme.relativeStrength1M.toFixed(1)}
                  </p>
                </div>
                <p className="mt-1 t-micro">
                  Leaders: <span className="font-mono text-slate-300">{(spotlightTheme.leaders ?? []).slice(0, 6).join(", ") || "—"}</span>
                </p>
                <div className="mt-3 grid grid-cols-2 gap-2">
                  {(uniSpotlight?.best ?? []).slice(0, 4).map((x) => {
                    const symU = String(x.ticker || "").toUpperCase();
                    const onList = symU && watchlisted.has(symU);
                    return (
                      <div key={`b-${x.ticker}`} className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-2.5 py-2">
                        <div className="flex items-baseline justify-between gap-2">
                          <div className="flex min-w-0 items-center gap-1">
                            <button
                              type="button"
                              onClick={() => onSelectTicker(symU, { theme: spotlightTheme?.theme, sector: spotlightTheme?.sector })}
                              className="truncate font-mono text-[12px] font-semibold text-accent hover:underline"
                            >
                              {x.ticker}
                            </button>
                            <button
                              type="button"
                              title={onList ? "Remove from watchlist" : "Add to watchlist"}
                              aria-label={onList ? "Remove from watchlist" : "Add to watchlist"}
                              onClick={() =>
                                void onToggleWatchlist(symU, {
                                  theme: spotlightTheme?.theme,
                                  sector: spotlightTheme?.sector,
                                })
                              }
                              className="shrink-0 rounded p-0.5 text-slate-500 hover:bg-terminal-elevated/60 hover:text-amber-300"
                            >
                              <Star className={`h-3 w-3 ${onList ? "fill-amber-400 text-amber-400" : ""}`} strokeWidth={2} />
                            </button>
                          </div>
                          <span className={`t-mono ${pctClass(x.today_return_pct)}`}>
                            {x.today_return_pct >= 0 ? "+" : ""}
                            {x.today_return_pct.toFixed(2)}%
                          </span>
                        </div>
                      </div>
                    );
                  })}
                  {!(uniSpotlight?.best?.length) ? (
                    <div className="col-span-2 rounded-lg border border-terminal-border bg-terminal-bg/40 px-3 py-2 text-xs text-slate-500">
                      No mover data.
                    </div>
                  ) : null}
                </div>
              </>
            ) : null}
              </div>
            </>
          )}
        </div>

        <div className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
          <header className="border-b border-terminal-border px-4 py-3">
            <h3 className="t-section">Leader Constituents</h3>
            <p className="mt-0.5 t-micro">Top names (grade + liquidity)</p>
          </header>
          <div className="px-4 py-3">
            {constituents.length ? (
              <div className="grid grid-cols-2 gap-2">
                {constituents.map((c) => {
                  const href = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(`NASDAQ:${c.ticker}`)}`;
                  const symU = String(c.ticker || "").toUpperCase();
                  const onList = watchlisted.has(symU);
                  return (
                    <div
                      key={c.ticker}
                      className="rounded-lg border border-terminal-border bg-terminal-bg/40 px-2.5 py-2 hover:border-slate-600"
                    >
                      <div className="flex items-baseline justify-between gap-1">
                        <button
                          type="button"
                          onClick={() =>
                            onSelectTicker(symU, {
                              theme: spotlightTheme?.theme,
                              sector: spotlightTheme?.sector,
                              grade: c.grade,
                            })
                          }
                          className="truncate font-mono text-[12px] font-semibold text-accent hover:underline"
                        >
                          {c.ticker}
                        </button>
                        <div className="flex shrink-0 items-center gap-0.5">
                          <button
                            type="button"
                            title={onList ? "Remove from watchlist" : "Add to watchlist"}
                            aria-label={onList ? "Remove from watchlist" : "Add to watchlist"}
                            onClick={() =>
                              void onToggleWatchlist(symU, {
                                theme: spotlightTheme?.theme,
                                sector: spotlightTheme?.sector,
                                grade: c.grade,
                              })
                            }
                            className="rounded p-0.5 text-slate-500 hover:bg-terminal-elevated/50 hover:text-amber-300"
                          >
                            <Star className={`h-3.5 w-3.5 ${onList ? "fill-amber-400 text-amber-400" : ""}`} strokeWidth={2} />
                          </button>
                          <a
                            href={href}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="rounded p-0.5 text-emerald-400 hover:bg-emerald-500/15"
                            title="TradingView chart"
                            aria-label={`Open ${c.ticker} on TradingView`}
                          >
                            <BarChart3 className="h-3.5 w-3.5" aria-hidden />
                          </a>
                        </div>
                      </div>
                      <span className="t-label">{c.grade}</span>
                      <div className="mt-1 flex items-baseline justify-between gap-2">
                        <span className="t-mono text-slate-300">{fmtPrice(c.close)}</span>
                        <span className={`t-mono ${pctClass(c.today ?? 0)}`}>{fmtPct(c.today, 2)}</span>
                      </div>
                      <p className="mt-1 t-micro">ADDV {formatMoney(c.addv)}</p>
                    </div>
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
              {/* Breadcrumb: show when drilled into a thematic bucket */}
              {leaderboardMode === "industry" && drilldownLabel ? (
                <p className="flex items-center gap-1 truncate t-data text-slate-500">
                  <button
                    type="button"
                    onClick={() => setDrilldownLabel(null)}
                    className="text-accent hover:underline"
                  >
                    Industry
                  </button>
                  <span className="text-slate-600">›</span>
                  <span className="font-semibold text-slate-300">{drilldownLabel}</span>
                  <span className="ml-1 rounded bg-terminal-elevated px-1 t-micro text-slate-500">
                    {finvizFilteredRows.length} sub-industries
                  </span>
                </p>
              ) : (
                <p className="truncate t-data text-slate-500">
                  {leaderboardMode === "industry"
                    ? "Click industry groups to expand/collapse sub-industries"
                    : "Theme performance + RS"}
                </p>
              )}
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1 rounded-full border border-terminal-border bg-terminal-bg p-1">
                <button
                  type="button"
                  data-e2e="lb-mode-themes"
                  onClick={() => {
                    setLeaderboardMode("themes");
                    setDrilldownLabel(null);
                    setExpandedTheme(null);
                    setLeaderboardSubSpotlight(null);
                  }}
                  className={`rounded-full px-3 py-1.5 t-data font-semibold transition-colors ${
                    leaderboardMode === "themes" ? "bg-accent/20 text-white" : "text-slate-400 hover:text-white"
                  }`}
                >
                  Themes
                </button>
                <button
                  type="button"
                  data-e2e="lb-mode-industry"
                  onClick={() => {
                    setLeaderboardMode("industry");
                    setDrilldownLabel(null);
                    setExpandedTheme(null);
                    setLeaderboardSubSpotlight(null);
                  }}
                  className={`rounded-full px-3 py-1.5 t-data font-semibold transition-colors ${
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
                  className="w-[200px] bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-terminal-card"
                />
              </div>
            </div>
          </header>
          <div className="fintech-scroll min-h-0 flex-1 overflow-auto">
            <table className="w-full min-w-[1020px] border-separate border-spacing-0 text-left t-data">
              <caption className="sr-only">
                Leaderboard of themes or industries with performance and relative strength columns.
              </caption>
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
                    { label: "Log %", key: null },
                    { label: "Qual%", key: "qual" as const },
                    { label: "Leaders", key: "leaders" as const },
                  ].map((h) => (
                    <th key={h.label} scope="col" className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-3 py-2 t-label">
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
                          <span className={`t-micro ${sortKey === h.key ? "text-accent" : "text-slate-600"}`}>
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
                    <td colSpan={11}>
                      <SkeletonRows count={8} />
                    </td>
                  </tr>
                ) : finvizLbError && !finvizFilteredRows.length ? (
                  <tr>
                    <td colSpan={11} className="px-3 py-4">
                      <ErrorBanner
                        title={`Failed to load ${leaderboardMode} data`}
                        detail={finvizLbError}
                      />
                    </td>
                  </tr>
                ) : !finvizLbLoading && sortedLeaderboardRows.length === 0 ? (
                  <tr>
                    <td colSpan={11}>
                      <EmptyState
                        icon={LayoutGrid}
                        title={`No ${leaderboardMode} data yet`}
                        subtitle="Retrying automatically every 15s. Finviz may be rate-limiting the server."
                      />
                    </td>
                  </tr>
                ) : leaderboardMode === "industry" ? (
                  // Accordion-style industry grouping
                  groupedThemes.map((group, groupIdx) => {
                    const isOpen = expandedTheme === group.category;
                    
                    return (
                      <React.Fragment key={`group-${group.category}`}>
                        {/* Parent Category Row */}
                        <tr
                          className="cursor-pointer border-b border-terminal-border/60 bg-terminal-elevated/20 hover:bg-terminal-elevated/40"
                          onClick={() => setExpandedTheme(isOpen ? null : group.category)}
                        >
                          <td className="px-3 py-3 text-right font-mono tabular-nums text-slate-500">{groupIdx + 1}</td>
                          <td className="px-3 py-3">
                            <div className="flex items-center gap-2">
                              <span className={isOpen ? "rotate-90" : ""} style={{ transition: "transform 0.2s" }}>▶</span>
                              <div className="min-w-0">
                                <p className="truncate font-bold text-slate-100">{group.category}</p>
                                <p className="truncate t-micro text-slate-500">
                                  {group.industries.length} industries
                                </p>
                              </div>
                            </div>
                          </td>
                          {/* Performance columns - show averages */}
                          {[group.avgPerf1D, group.avgPerf1W, group.avgPerf1M, group.avgPerf3M, group.avgPerf6M].map((v, i) => (
                            <td
                              key={i}
                              className={`px-3 py-3 text-right font-mono tabular-nums font-bold ${
                                v == null || !Number.isFinite(v) ? "text-slate-600" : pctClass(v)
                              }`}
                            >
                              {fmtPct(v, 1)}
                            </td>
                          ))}
                          <td className="px-3 py-3 text-right font-mono tabular-nums font-bold text-slate-300">
                            {group.avgRS == null || !Number.isFinite(group.avgRS) ? "—" : group.avgRS.toFixed(1)}
                          </td>
                          <td className="px-3 py-3 text-right font-mono tabular-nums font-bold text-slate-600" title="Category rollup — open a sub-industry row for skyte/rs-log match">
                            —
                          </td>
                          <td className="px-3 py-3 text-right font-mono tabular-nums font-bold text-slate-300">
                            {group.avgQual == null ? "—" : `${(group.avgQual * 100).toFixed(0)}%`}
                          </td>
                          <td className="px-3 py-3 text-right font-mono text-slate-600">—</td>
                        </tr>
                        
                        {/* Sub-Industries (when expanded) */}
                        {isOpen && group.industries.map((industry, idx) => {
                          const rs = industry.relativeStrength1M ?? null;
                          const qRatio = Number.isFinite(industry.relativeStrengthQualifierRatio) ? industry.relativeStrengthQualifierRatio : null;
                          const adr = getIndustryAdr(industry);
                          
                          const subSelected =
                            leaderboardSubSpotlight?.kind === "industry" &&
                            leaderboardSubSpotlight.theme === industry.theme &&
                            leaderboardSubSpotlight.parent === group.category;
                          return (
                            <tr
                              key={`industry-${industry.theme}`}
                              className={`cursor-pointer border-b border-terminal-border/60 hover:bg-terminal-elevated/40 ${
                                subSelected ? "bg-accent/15 ring-1 ring-inset ring-accent/35" : ""
                              }`}
                              onClick={(e) => {
                                e.stopPropagation();
                                setSpotlightThemeName(industry.theme);
                                setLeaderboardSubSpotlight({
                                  kind: "industry",
                                  parent: group.category,
                                  theme: industry.theme,
                                  relativeStrength1M: industry.relativeStrength1M,
                                  perf1D: industry.perf1D ?? null,
                                  perf1M: industry.perf1M ?? null,
                                  perfYTD: industry.perfYTD ?? null,
                                  totalCount: industry.totalCount ?? 0,
                                  leaders: industry.leaders ?? [],
                                });
                              }}
                            >
                              <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-600">
                                {groupIdx + 1}.{idx + 1}
                              </td>
                              <td className="px-3 py-2 pl-8">
                                <div className="min-w-0 border-l border-slate-700 pl-4">
                                  <p className={`truncate font-medium ${industry.seed ? "text-slate-500 italic" : "text-slate-200"}`}>
                                    {industry.theme}
                                    {industry.seed && (
                                      <span className="ml-1.5 rounded bg-terminal-elevated/60 px-1 py-px t-mono not-italic text-slate-600">
                                        awaiting data
                                      </span>
                                    )}
                                  </p>
                                  <p className="truncate t-micro">
                                    <span className={industry.perf1W != null ? pctClass(industry.perf1W) : "text-slate-600"}>
                                      {fmtPct(industry.perf1W, 2)}
                                    </span>
                                    {adr != null && ` (${adr.toFixed(1)}%)`}
                                  </p>
                                </div>
                              </td>
                              {[industry.perf1D, industry.perf1W, industry.perf1M, industry.perf3M, industry.perf6M].map((v, i) => (
                                <td
                                  key={i}
                                  className={`px-3 py-2 text-right font-mono tabular-nums ${
                                    v == null || !Number.isFinite(v) ? "text-slate-600" : pctClass(v)
                                  }`}
                                >
                                  {fmtPct(v, 2)}
                                </td>
                              ))}
                              <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-300">
                                {rs == null || !Number.isFinite(rs) ? "—" : rs.toFixed(1)}
                              </td>
                              <td
                                className="px-3 py-2 text-right font-mono tabular-nums text-slate-300"
                                title={(() => {
                                  if (!skyteRsEnabled || skyteRsLoading) return undefined;
                                  const sk = lookupSkyteIndustry(skyteIndustryMap, industry.theme);
                                  return sk ? `skyte/rs-log • RS ${sk.relative_strength.toFixed(1)}` : "No skyte row for this label (name mismatch)";
                                })()}
                              >
                                {!skyteRsEnabled ? "—" : skyteRsLoading ? "…" : (() => {
                                  const sk = lookupSkyteIndustry(skyteIndustryMap, industry.theme);
                                  return sk ? String(sk.percentile) : "—";
                                })()}
                              </td>
                              <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-300">
                                {qRatio == null ? "—" : `${(qRatio * 100).toFixed(0)}%`}
                              </td>
                              <td className="px-3 py-2 text-right font-mono text-slate-400">{(industry.leaders ?? []).slice(0, 4).join(", ") || "—"}</td>
                            </tr>
                          );
                        })}
                      </React.Fragment>
                    );
                  })
                ) : (
                  // Themes mode — parent groups + child rows (Industry mode unchanged above)
                  themesLeaderboardGroups.map(
                    (
                      { parent, rows, avgRs, avgPerf1D, avgPerf1W, avgPerf1M, avgPerf3M, avgPerf6M },
                      idx
                    ) => {
                    const isOpen = expandedParents.has(parent);
                    const rsVals = rows.map((r) => r.relativeStrength1M).filter((x): x is number => x != null && Number.isFinite(x));
                    const hasRsAverage = rsVals.length > 0;
                    const sortedChildren = [...rows].sort((a, b) => {
                      const ar = a.relativeStrength1M;
                      const br = b.relativeStrength1M;
                      const aOk = ar != null && Number.isFinite(ar);
                      const bOk = br != null && Number.isFinite(br);
                      if (aOk && bOk) return br - ar;
                      if (aOk) return -1;
                      if (bOk) return 1;
                      return (b.perf1M ?? Number.NEGATIVE_INFINITY) - (a.perf1M ?? Number.NEGATIVE_INFINITY);
                    });
                    return (
                      <Fragment key={parent}>
                        <tr
                          className={`cursor-pointer select-none border-b border-terminal-border/60 bg-terminal-elevated/20 transition-colors duration-100 hover:bg-terminal-elevated/40 ${
                            isOpen ? "bg-terminal-elevated/30" : ""
                          }`}
                          onClick={() => toggleParent(parent)}
                          role="button"
                          aria-expanded={isOpen}
                        >
                          <td className="px-3 py-2.5 text-right font-mono tabular-nums text-slate-600">{idx + 1}</td>
                          <td className="px-3 py-2.5">
                            <div className="flex min-w-0 items-center gap-2">
                              <ChevronRight
                                size={11}
                                className={`shrink-0 text-slate-500 transition-transform duration-200 ${
                                  isOpen ? "rotate-90 text-cyan-400" : ""
                                }`}
                                aria-hidden
                              />
                              <div className="min-w-0">
                                <p className="truncate text-[12px] font-semibold text-slate-200">{parent}</p>
                                <p className="t-micro text-slate-600">{rows.length} sub-industries</p>
                              </div>
                            </div>
                          </td>
                          {[avgPerf1D, avgPerf1W, avgPerf1M, avgPerf3M, avgPerf6M].map((v, i) => (
                            <td
                              key={i}
                              className={`px-3 py-2.5 text-right font-mono tabular-nums font-bold ${
                                v == null || !Number.isFinite(v) ? "text-slate-600" : pctClass(v)
                              }`}
                            >
                              {fmtPct(v, 2)}
                            </td>
                          ))}
                          <td className="px-3 py-2.5 text-right">
                            <div className="flex items-center justify-end gap-1.5">
                              <div className="h-1 w-6 overflow-hidden rounded-full bg-terminal-elevated/50">
                                <div
                                  className="h-full rounded-full"
                                  style={{
                                    width: `${Math.min(100, Math.max(0, avgRs))}%`,
                                    background: rsBarColor(avgRs),
                                  }}
                                />
                              </div>
                              <span className={`t-mono text-[11px] font-bold tabular-nums ${rsTextClass(hasRsAverage ? avgRs : null)}`}>
                                {hasRsAverage ? avgRs.toFixed(0) : "—"}
                              </span>
                            </div>
                          </td>
                          <td className="px-3 py-2.5 text-right font-mono tabular-nums text-slate-600">—</td>
                          <td className="px-3 py-2.5 text-right font-mono tabular-nums text-slate-600">—</td>
                          <td className="px-3 py-2.5 text-right font-mono text-slate-600">—</td>
                        </tr>

                        {isOpen &&
                          sortedChildren.map((row, cidx) => {
                            const rs = row.relativeStrength1M ?? null;
                            const qRatio = Number.isFinite(row.relativeStrengthQualifierRatio)
                              ? row.relativeStrengthQualifierRatio
                              : null;
                            const themeRowSelected =
                              (leaderboardSubSpotlight?.kind === "finviz_theme" &&
                                leaderboardSubSpotlight.theme === row.theme) ||
                              (!leaderboardSubSpotlight && spotlightTheme?.theme === row.theme);
                            return (
                              <tr
                                key={`${leaderboardMode}:${row.theme}|${row.sector ?? ""}`}
                                tabIndex={0}
                                className={`cursor-pointer border-b border-terminal-border/60 bg-terminal-bg/50 transition-colors duration-100 hover:bg-terminal-elevated/40 ${
                                  themeRowSelected ? "bg-terminal-elevated/30" : ""
                                }`}
                                onClick={() => activateThemeLeaderboardRow(row)}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") activateThemeLeaderboardRow(row);
                                }}
                                title="Click to spotlight"
                              >
                                <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-600">{cidx + 1}</td>
                                <td className="px-3 py-2 pl-8">
                                  <div className="min-w-0 border-l border-slate-700 pl-4">
                                    <p className={`truncate font-medium ${row.seed ? "italic text-slate-500" : "text-slate-100"}`}>
                                      {row.theme}
                                      {row.seed && (
                                        <span className="ml-1.5 rounded bg-terminal-elevated/60 px-1 py-px t-mono not-italic text-slate-600">
                                          awaiting data
                                        </span>
                                      )}
                                    </p>
                                    <p className="flex flex-wrap items-center gap-x-1.5 truncate t-micro">
                                      {row.qualifiedCount}/{row.totalCount} · {row.sector ?? "—"} · {formatMoney(row.themeDollarVolume)}
                                    </p>
                                  </div>
                                </td>
                                {[row.perf1D, row.perf1W, row.perf1M, row.perf3M, row.perf6M].map((v, i) => (
                                  <td
                                    key={i}
                                    className={`px-3 py-2 text-right font-mono tabular-nums ${
                                      v == null || !Number.isFinite(v) ? "text-slate-600" : pctClass(v)
                                    }`}
                                  >
                                    {fmtPct(v, 2)}
                                  </td>
                                ))}
                                <td className="px-3 py-2 text-right">
                                  <div className="flex items-center justify-end gap-1.5">
                                    <div className="h-1 w-5 overflow-hidden rounded-full bg-terminal-elevated/50">
                                      <div
                                        className="h-full rounded-full"
                                        style={{
                                          width: `${Math.min(100, Math.max(0, rs ?? 0))}%`,
                                          background: rsBarColor(rs),
                                        }}
                                      />
                                    </div>
                                    <span className={`t-mono text-[11px] font-bold tabular-nums ${rsTextClass(rs)}`}>
                                      {rs != null ? rs.toFixed(1) : "—"}
                                    </span>
                                  </div>
                                </td>
                                <td
                                  className="px-3 py-2 text-right font-mono tabular-nums text-slate-600"
                                  title="Switch to Industry leaderboard for skyte/rs-log percentile column"
                                >
                                  —
                                </td>
                                <td className="px-3 py-2 text-right font-mono tabular-nums text-slate-200">
                                  {qRatio == null ? "—" : `${(qRatio * 100).toFixed(0)}%`}
                                </td>
                                <td className="px-3 py-2 text-right font-mono text-slate-300">
                                  {(row.leaders ?? []).slice(0, 4).join(", ") || "—"}
                                </td>
                              </tr>
                            );
                          })}
                      </Fragment>
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
  reload,
  filters,
  setFilters,
  gapScannerGradeByTicker,
  onSelectTicker,
  watchlisted,
  onToggleWatchlist,
}: {
  gappers: PremarketGappersPayload | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
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
  watchlisted: Set<string>;
  onToggleWatchlist: (ticker: string, ctx?: { theme?: string; sector?: string; grade?: string }) => void | Promise<void>;
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
        <p className="mb-3 t-label text-amber-200/90">
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
              <span className="t-label text-amber-200/85">{label}</span>
              <input
                type="text"
                inputMode="decimal"
                value={filters[key]}
                onChange={(e) => setFilters({ ...filters, [key]: e.target.value })}
                className="w-full rounded-md border border-terminal-border bg-terminal-bg px-2 py-1.5 font-mono text-xs text-slate-200 placeholder:text-slate-600 focus:border-accent/50 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-terminal-card"
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
            className="rounded-md border border-terminal-border bg-terminal-bg px-3 py-1 t-label text-slate-400 transition-colors hover:border-slate-500 hover:text-slate-200"
          >
            Reset
          </button>
          <p className="text-right t-micro text-slate-500">
            <span className="text-slate-400">Scanned (ET):</span>{" "}
            <span className="t-mono text-slate-400">{formatEt(gappers?.fetched_at_utc)}</span>
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
            <h2 className="t-page">Pre-market Gappers</h2>
            <p className="mt-0.5 max-w-[52rem] t-micro text-slate-500">
              Sorted by premarket gap (desc). Grade column uses scanner audit (A+/A mapped to A/B).
            </p>
          </div>
          <RefreshRow
            onRefresh={() => void reload()}
            loading={loading}
            lastUpdatedAt={gappers?.fetched_at_utc ? new Date(gappers.fetched_at_utc).getTime() : null}
          />
        </header>

        <div className="fintech-scroll min-h-0 min-w-0 flex-1 overflow-auto px-2 pb-3 pt-2">
          {loading && !(gappers?.rows && gappers.rows.length > 0) ? (
            <PanelLoading label="Loading gap scan…" />
          ) : error ? (
            <ErrorBanner title="Gap scan failed" detail={error} onRetry={() => void reload()} />
          ) : !gappers?.rows?.length ? (
            <EmptyState
              icon={Search}
              title="No matching gappers"
              subtitle="Loosen your filters or click Refresh."
            />
          ) : (
            <table className="w-full min-w-[1240px] border-separate border-spacing-0 text-left t-data">
              <caption className="sr-only">Pre-market gap scan results with sortable columns.</caption>
              <thead>
                <tr>
                  {[
                    { label: "Ticker", key: "ticker" as const },
                    { label: "Watch", key: null },
                    { label: "Premkt %", key: "premktPct" as const },
                    { label: "Premkt Vol", key: "premktVol" as const },
                    { label: "Daily %", key: "dailyPct" as const },
                    { label: "ADR%", key: "adr" as const },
                    { label: "MktCap", key: "mcap" as const },
                    { label: "Setup", key: "setup" as const },
                    { label: "Sector", key: "sector" as const },
                    { label: "Industry", key: "industry" as const },
                    { label: "Grade", key: "grade" as const },
                  ].map((h) => (
                    <th
                      key={h.label}
                      scope="col"
                      className="sticky top-0 z-10 whitespace-nowrap border-b border-terminal-border bg-terminal-card px-2 py-2 t-label"
                    >
                      {h.key === null ? (
                        <span>{h.label}</span>
                      ) : (
                        <button
                          type="button"
                          onClick={() => {
                            if (sortKey === h.key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                            else {
                              setSortKey(h.key);
                              setSortDir(h.key === "ticker" || h.key === "sector" || h.key === "industry" ? "asc" : "desc");
                            }
                          }}
                          className="inline-flex w-full items-center gap-1 text-left hover:text-slate-300"
                        >
                          <span>{h.label}</span>
                          <span className={`t-micro ${sortKey === h.key ? "text-accent" : "text-slate-600"}`}>
                            {sortKey === h.key ? (sortDir === "asc" ? "▲" : "▼") : "↕"}
                          </span>
                        </button>
                      )}
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
                              className="rounded p-0.5 text-slate-400 hover:bg-terminal-elevated/40 hover:text-white"
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
                      <td className="border-b border-terminal-border/60 px-1 py-2 text-center align-middle">
                        {symUs ? (
                          <button
                            type="button"
                            title={watchlisted.has(symUs.toUpperCase()) ? "Remove from watchlist" : "Add to watchlist"}
                            aria-label={watchlisted.has(symUs.toUpperCase()) ? "Remove from watchlist" : "Add to watchlist"}
                            onClick={() =>
                              void onToggleWatchlist(symUs.toUpperCase(), {
                                sector: sector === "—" ? undefined : sector,
                                grade,
                              })
                            }
                            className="inline-flex rounded-md p-1 text-slate-500 hover:bg-terminal-elevated/50 hover:text-amber-300"
                          >
                            <Star
                              className={`h-4 w-4 ${watchlisted.has(symUs.toUpperCase()) ? "fill-amber-400 text-amber-400" : ""}`}
                              strokeWidth={2}
                            />
                          </button>
                        ) : (
                          <span className="text-slate-700">—</span>
                        )}
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
                          {mcapLines.tier ? <span className="t-micro font-medium text-slate-500">{mcapLines.tier}</span> : null}
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
  const [tab, setTab] = useState<"scanner" | "gappers" | "breadth" | "rotation">("scanner");
  const [focusTicker, setFocusTicker] = useState<string | null>(null);
  const [focusTickerMeta, setFocusTickerMeta] = useState<TickerDrawerMeta | null>(null);
  const [watchlistOpen, setWatchlistOpen] = useState(false);
  const [watchActionErr, setWatchActionErr] = useState<string | null>(null);
  const {
    items: watchItems,
    loading: watchLoading,
    error: watchLoadError,
    reload: reloadWatchlist,
    watchlisted,
    toggleTicker,
    removeTicker,
    updateNote,
  } = useWatchlist();
  const marketStatus = useMarketStatus();
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
  const {
    data: gappers,
    loading: gappersLoading,
    error: gappersError,
    reload: reloadGappers,
  } = usePremarketGappers(gapFilters);

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

  const handleToggleWatchlist = useCallback(
    async (ticker: string, ctx?: { theme?: string; sector?: string; grade?: string }) => {
      try {
        setWatchActionErr(null);
        await toggleTicker(ticker, ctx);
      } catch (e) {
        setWatchActionErr(e instanceof Error ? e.message : "Watchlist update failed");
      }
    },
    [toggleTicker]
  );

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      if (watchlistOpen) {
        setWatchlistOpen(false);
        e.preventDefault();
        return;
      }
      if (focusTicker) {
        setFocusTicker(null);
        setFocusTickerMeta(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [focusTicker, watchlistOpen]);

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
              <p className="truncate t-data text-slate-500">Pure-play clusters · 1M RS · Finviz · yfinance · VIX</p>
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
                <span className="flex items-center gap-2 t-label">
                  Live time
                  <span className={`inline-flex h-2 w-2 rounded-full ${status.cls}`} title={status.hint} />
                </span>
                <span className="font-mono text-[12px] font-semibold text-slate-200 tabular-nums">{liveTimeText}</span>
              </div>
              <div className="h-7 w-px bg-terminal-border/80" />
              <div className="flex flex-col leading-tight">
                <span className="flex items-center gap-2 t-label">
                  Auto update
                  <span className={`inline-flex h-2 w-2 rounded-full ${status.cls}`} title={status.hint} />
                </span>
                <span className="font-mono text-[12px] font-semibold text-slate-200 tabular-nums">{autoRefreshCountdown}</span>
              </div>
              <span className="hidden t-micro lg:inline">
                Last: <span className="font-mono">{formatEtClock(lastUpdatedAt)}</span>
              </span>
            </div>
          </div>
        </div>
      </header>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        <nav className="shrink-0 border-b border-terminal-border bg-terminal-bg pl-4 pr-[42px] py-3" aria-label="Workspace tabs">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setTab("scanner")}
              aria-pressed={tab === "scanner"}
              className={`rounded-full border px-4 py-2 t-data font-semibold transition-colors ${
                tab === "scanner" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              Thematic Scanner
            </button>
            <button
              type="button"
              onClick={() => setTab("gappers")}
              aria-pressed={tab === "gappers"}
              className={`rounded-full border px-4 py-2 t-data font-semibold transition-colors ${
                tab === "gappers" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              Pre-Market Gappers
            </button>
            <button
              type="button"
              onClick={() => setTab("breadth")}
              aria-pressed={tab === "breadth"}
              className={`flex items-center gap-1.5 rounded-full border px-4 py-2 t-data font-semibold transition-colors ${
                tab === "breadth" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              <BarChart2 className="h-3.5 w-3.5" aria-hidden />
              Market Breadth
            </button>
            <button
              type="button"
              onClick={() => setTab("rotation")}
              aria-pressed={tab === "rotation"}
              className={`flex items-center gap-1.5 rounded-full border px-4 py-2 t-data font-semibold transition-colors ${
                tab === "rotation" ? "border-accent/40 bg-accent/20 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              <BarChart3 className="h-3.5 w-3.5" aria-hidden />
              Rotation
            </button>

            <div className="ml-auto flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setWatchlistOpen((v) => !v)}
              aria-expanded={watchlistOpen}
              aria-controls="watchlist-drawer"
              className={`flex items-center gap-1.5 rounded-full border px-3 py-2 t-data font-semibold transition-colors ${
                watchlistOpen ? "border-accent/40 bg-accent/15 text-white" : "border-terminal-border bg-terminal-bg text-slate-300 hover:border-slate-600 hover:text-white"
              }`}
            >
              <ListPlus className="h-3.5 w-3.5 shrink-0" aria-hidden />
              <span className="hidden sm:inline">Watchlist</span>
              {watchItems.length ? (
                <span className="rounded-full bg-terminal-elevated px-1.5 py-px font-mono text-[10px] text-slate-300">{watchItems.length}</span>
              ) : null}
            </button>

            <div className="relative">
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
                  aria-label="Search ticker or company"
                  autoComplete="off"
                  className="w-[180px] bg-transparent text-xs text-slate-200 placeholder:text-slate-600 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-terminal-bg"
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
                    {suggestLoading ? <p className="t-data text-slate-500">Searching…</p> : null}
                    {!suggestLoading && !suggest.length ? <p className="t-data text-slate-500">No matches.</p> : null}
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
                                <span className="truncate t-micro text-slate-500">{r.name}</span>
                              </button>
                            );
                          })}
                        </div>
                      </div>

                      <div className="border-t border-terminal-border px-3 py-3">
                        {intelLoading ? (
                          <p className="t-data text-slate-500">Loading…</p>
                        ) : intel ? (
                          <div className="space-y-2">
                            <div>
                              <div className="flex items-baseline justify-between gap-3">
                                <p className="font-mono text-[12px] font-semibold text-white">{intel.ticker}</p>
                                <div className="flex items-baseline gap-3">
                                  <span className="t-mono font-semibold text-slate-200">{fmtPrice(intel.close)}</span>
                                  <span className={`t-mono font-semibold ${pctClass(intel.today_return_pct ?? 0)}`}>
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
                                  <span className={`t-mono ${pctClass(intel.today_return_pct ?? 0)}`}>{fmtPct(intel.today_return_pct, 2)}</span>
                                </div>
                              </div>
                              <p className="mt-0.5 t-data text-slate-400">{intel.name}</p>
                            </div>
                            <div className="h-px bg-terminal-border" />
                            <div className="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 t-data">
                              <p className="t-label">Sector</p>
                              <p className="truncate font-semibold text-slate-200">{intel.sector}</p>
                              <p className="t-label">Industry</p>
                              <p className="truncate font-semibold text-slate-200">{intel.industry}</p>
                              <p className="t-label">Theme</p>
                              <p className="truncate font-semibold text-sky-200">{intel.theme ?? "—"}</p>
                              <p className="t-label">Sub-Theme</p>
                              <p className="truncate font-semibold text-violet-200">{intel.subtheme ?? "—"}</p>
                            </div>
                          </div>
                        ) : (
                          <p className="t-data text-slate-500">Select a ticker to view details.</p>
                        )}
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
            </div>
          </div>
        </nav>

        {watchActionErr ? (
          <div className="shrink-0 border-b border-rose-900/40 bg-rose-950/35 px-4 py-2 text-center text-xs font-medium text-rose-200">
            {watchActionErr}
          </div>
        ) : null}

        <main
          id="main-content"
          className="flex min-h-0 min-w-0 flex-1 flex-row overflow-x-auto overflow-y-hidden"
          aria-label="Dashboard workspace"
        >
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
                    watchlisted={watchlisted}
                    onToggleWatchlist={handleToggleWatchlist}
                    onSelectTicker={(t, meta) => {
                      setFocusTicker(t);
                      setFocusTickerMeta(meta ?? null);
                    }}
                  />
                ) : (
                  <div className="flex h-full items-center justify-center text-slate-500">Loading scanner…</div>
                )}
              </div>
            ) : tab === "breadth" ? (
              <MarketBreadth />
            ) : tab === "rotation" ? (
              <RotationView />
            ) : (
              <GappersView
                gappers={gappers}
                loading={gappersLoading}
                error={gappersError}
                reload={reloadGappers}
                filters={gapFilters}
                setFilters={setGapFilters}
                gapScannerGradeByTicker={gapScannerGradeByTicker}
                onSelectTicker={(t, meta) => {
                  setFocusTicker(t);
                  setFocusTickerMeta(meta ?? null);
                }}
                watchlisted={watchlisted}
                onToggleWatchlist={handleToggleWatchlist}
              />
            )}
          </div>
          {watchlistOpen ? (
            <WatchlistDrawer
              onClose={() => setWatchlistOpen(false)}
              marketStatus={marketStatus}
              items={watchItems}
              loading={watchLoading}
              error={watchLoadError}
              onReload={() => void reloadWatchlist()}
              onRemove={async (t) => {
                try {
                  setWatchActionErr(null);
                  await removeTicker(t);
                } catch (e) {
                  setWatchActionErr(e instanceof Error ? e.message : "Remove failed");
                }
              }}
              onUpdateNote={async (t, n) => {
                try {
                  setWatchActionErr(null);
                  await updateNote(t, n);
                } catch (e) {
                  setWatchActionErr(e instanceof Error ? e.message : "Note save failed");
                }
              }}
              onSelectTicker={(t) => {
                setFocusTicker(t);
                setFocusTickerMeta(null);
              }}
            />
          ) : null}
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
        </main>
      </div>
    </div>
  );
}

