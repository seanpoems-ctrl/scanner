import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { Activity, ArrowDown, ArrowUp, ArrowUpDown, Filter, LayoutGrid, LineChart, Search, Sunrise, Zap } from "lucide-react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, XAxis, YAxis } from "recharts";
import { MarketSummary } from "./MarketSummary";

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
  /** Finviz sector row: session / 1D %-style move */
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

type ThemeSortKey =
  | "theme"
  | "perf1d"
  | "perf1w"
  | "perf1m"
  | "perf3m"
  | "perf6m"
  | "rs1m"
  | "rsRatio"
  | "leaders"
  | "themeVol"
  | "stage";

type RsChartRow = {
  theme: string;
  rsDisplay: number;
  isGhost: boolean;
  barFill: "muted" | "neg" | "zero" | "pos";
};

function themeRowKey(t: ApiTheme): string {
  return `${t.theme}|${t.sector ?? ""}|${t.totalCount}`;
}

function normalizeThemeLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function finvizBucketFromThemeLabel(label: string | null | undefined): string {
  if (!label) return "";
  const parts = label.split("·").map((p) => p.trim()).filter(Boolean);
  return parts[0] ?? "";
}

type ThemeSortKey =
  | "theme"
  | "perf1d"
  | "perf1w"
  | "perf1m"
  | "perf3m"
  | "perf6m"
  | "rs1m"
  | "rsRatio"
  | "leaders"
  | "themeVol"
  | "stage";

type RsChartRow = {
  theme: string;
  rsDisplay: number;
  isGhost: boolean;
  barFill: "muted" | "neg" | "zero" | "pos";
};

function themeRowKey(t: ApiTheme): string {
  return `${t.theme}|${t.sector ?? ""}|${t.totalCount}`;
}

function normalizeThemeLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function finvizBucketFromThemeLabel(label: string | null | undefined): string {
  if (!label) return "";
  const parts = label.split("·").map((p) => p.trim()).filter(Boolean);
  return parts[0] ?? "";
}

type ApiPayload = {
  vix: {
    symbol: string;
    close: number;
    change_pct: number;
  };
  themes: ApiTheme[];
  leaderboardMeta?: {
    view?: string;
    source?: string;
    perfNote?: string;
    url?: string;
    urls?: { overview?: string; performance?: string };
  };
  marketFlowSummary: {
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
  polling?: {
    pollSeconds: number;
    backoffActive: boolean;
    retryAfterSeconds: number;
  };
  tape?: { label: string; symbol: string; close: number | null; change_pct: number | null }[];
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

type PremarketBrief = {
  generated_at_utc: string | null;
  scheduled_for_et: string | null;
  narrative?: string[];
  sections: { title: string; bullets: string[] }[];
  headlines: { title: string; link: string; pubDate?: string }[];
  source?: { markets?: string; news?: string };
};

function TradingViewMiniSymbol({ symbol, title }: { symbol: string; title: string }) {
  const containerId = useId().replace(/:/g, "");
  useEffect(() => {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = "";

    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js";
    script.async = true;
    script.innerHTML = JSON.stringify(
      {
        symbol,
        width: "100%",
        height: 112,
        locale: "en",
        dateRange: "3M",
        colorTheme: "dark",
        isTransparent: true,
        autosize: true,
        largeChartUrl: "",
      },
      null,
      0,
    );
    container.appendChild(script);
  }, [containerId, symbol]);

  return (
    <div className="rounded-lg border border-terminal-border bg-terminal-bg/30 p-2">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-500">{title}</p>
      <div id={containerId} className="w-full" />
    </div>
  );
}

function TradingViewMiniSymbol({ symbol, title }: { symbol: string; title: string }) {
  const containerId = useId().replace(/:/g, "");
  useEffect(() => {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = "";

    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js";
    script.async = true;
    script.innerHTML = JSON.stringify(
      {
        symbol,
        width: "100%",
        height: 112,
        locale: "en",
        dateRange: "3M",
        colorTheme: "dark",
        isTransparent: true,
        autosize: true,
        largeChartUrl: "",
      },
      null,
      0,
    );
    container.appendChild(script);
  }, [containerId, symbol]);

  return (
    <div className="rounded-lg border border-terminal-border bg-terminal-bg/30 p-2">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-500">{title}</p>
      <div id={containerId} className="w-full" />
    </div>
  );
}

function highlightBriefText(text: string): React.ReactNode {
  const s = String(text || "");
  const terms = [
    "Risk-on",
    "Risk-off",
    "VIX",
    "US10Y",
    "inflation",
    "rates",
    "rate",
    "yields",
    "yield",
    "credit",
    "HY",
    "CPI",
    "PCE",
    "Fed",
    "earnings",
    "earnings season",
    "AI",
    "energy",
    "oil",
    "FX",
    "breadth",
    "volatility",
    "impact",
    "catalyst",
  ];
  if (!s) return s;
  const termRe = new RegExp(`(${terms.map((t) => t.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")).join("|")})`, "gi");
  const pctRe = /([+-]\s*\d+(?:\.\d+)?\s*%)/g;

  // Split on terms, then within each chunk split on signed % moves.
  const parts = s.split(termRe);
  const out: React.ReactNode[] = [];
  parts.forEach((part, i) => {
    const hit = terms.some((t) => t.toLowerCase() === part.toLowerCase());
    const baseNode: React.ReactNode = hit ? (
      <span key={`t-${i}`} className="font-semibold text-slate-100">
        {part}
      </span>
    ) : (
      <span key={`p-${i}`}>{part}</span>
    );

    // If this part is a term hit, don't try to recolor inside it.
    if (hit) {
      out.push(baseNode);
      return;
    }

    const chunks = String(part).split(pctRe);
    chunks.forEach((c, j) => {
      const m = c.match(/^[+-]\s*\d+(?:\.\d+)?\s*%$/);
      if (!m) {
        if (c) out.push(<span key={`c-${i}-${j}`}>{c}</span>);
        return;
      }
      const n = Number(c.replace("%", "").replace(/\s+/g, ""));
      out.push(
        <span key={`m-${i}-${j}`} className={`font-semibold ${pctClass(n)}`}>
          {c.replace(/\s+/g, "")}
        </span>,
      );
    });
  });

  return out;
}

const API_BASE_URL = "http://127.0.0.1:8000";

const REFRESH_BASE_MS = 110_000;
const REFRESH_MAX_MS = 8 * 60_000;

function tapeValue(close: number | null | undefined): string {
  if (close == null || !Number.isFinite(close)) return "—";
  const v = close;
  if (v >= 1000) return Math.round(v).toLocaleString();
  if (v >= 100) return v.toFixed(1);
  return v.toFixed(2);
}

/** Top N themes by 1M RS (numeric only); used for "Hot" labels on stock cards. */
const HOT_THEME_COUNT = 3;

/** Episodic Pivot: backend sets true when gap-up, OR-RVOL, and RS decile gates all pass. */
function isEpCandidate(stock: ApiStock): boolean {
  return stock.ep_candidate === true;
}

/**
 * High-intensity EP burst (UI + sonar): gap > 5% and OR RVOL > 3×
 * ("300" = 300% of opening-range baseline volume).
 */
function isEpBurst(stock: ApiStock): boolean {
  const gap = stock.gap_open_pct;
  const orv = stock.or_rvol_ratio;
  return gap != null && gap > 5 && orv != null && orv > 3;
}

function playSubtleBuzzPing(audioCtxRef: { current: AudioContext | null }) {
  const AC = window.AudioContext ?? (window as Window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
  if (!AC) return;
  try {
    if (!audioCtxRef.current) {
      audioCtxRef.current = new AC();
    }
    const ctx = audioCtxRef.current;
    if (ctx.state === "suspended") {
      void ctx.resume();
    }
    const t0 = ctx.currentTime;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(920, t0);
    gain.gain.setValueAtTime(0.0001, t0);
    gain.gain.exponentialRampToValueAtTime(0.055, t0 + 0.012);
    gain.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.11);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(t0);
    osc.stop(t0 + 0.13);
  } catch {
    /* autoplay or API blocked */
  }
}

/** Single sonar-style ping when a ticker first hits EP burst gates. */
function playSonarPing(audioCtxRef: { current: AudioContext | null }) {
  const AC = window.AudioContext ?? (window as Window & { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
  if (!AC) return;
  try {
    if (!audioCtxRef.current) {
      audioCtxRef.current = new AC();
    }
    const ctx = audioCtxRef.current;
    if (ctx.state === "suspended") {
      void ctx.resume();
    }
    const t0 = ctx.currentTime;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = "sine";
    osc.frequency.setValueAtTime(720, t0);
    osc.frequency.exponentialRampToValueAtTime(180, t0 + 0.2);
    gain.gain.setValueAtTime(0.0001, t0);
    gain.gain.exponentialRampToValueAtTime(0.07, t0 + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, t0 + 0.28);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(t0);
    osc.stop(t0 + 0.3);
  } catch {
    /* autoplay or API blocked */
  }
}

function stockCardHoverClass(grade: ApiStock["gradeLabel"]): string {
  if (grade === "A+") {
    return "hover:border-grade-aplus/55 hover:shadow-[0_0_16px_-6px_rgba(46,229,157,0.4)]";
  }
  if (grade === "A") {
    return "hover:border-grade-a/55 hover:shadow-[0_0_16px_-6px_rgba(46,191,245,0.38)]";
  }
  return "hover:border-terminal-border";
}

function formatUpdatedClock(d: Date): string {
  return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

/** Body styles inside a terminal card (no outer border — card chrome handles it). */
function trendRibbonSurfaceClass(state: "bullish" | "neutral" | "bearish" | undefined): string {
  if (!state) return "bg-terminal-bg/60 text-slate-300";
  if (state === "bullish") return "bg-grade-aplus/10 text-emerald-100";
  if (state === "neutral") return "bg-grade-a/10 text-sky-100";
  return "bg-rose-500/10 text-rose-100";
}

function formatMoney(value: number): string {
  if (value >= 1_000_000_000) {
    return `$${(value / 1_000_000_000).toFixed(2)}B`;
  }
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(2)}M`;
  }
  return `$${value.toFixed(2)}`;
}

function pctClass(value: number): string {
  if (value > 0) return "text-emerald-400";
  if (value < 0) return "text-rose-400";
  return "text-slate-400";
}

/** Rule-of-thumb: SPX ~1-day expected move from VIX (VIX÷16). */
function vixDailyExpectedMovePct(vix: number): number {
  if (!Number.isFinite(vix) || vix <= 0) return 0;
  return vix / 16;
}

function defaultThemeSortDir(key: ThemeSortKey): "asc" | "desc" {
  if (key === "theme") return "asc";
  return "desc";
}

function cmpNullableNum(a: number | null | undefined, b: number | null | undefined, dir: number): number {
  const na = a != null && Number.isFinite(a) ? a : null;
  const nb = b != null && Number.isFinite(b) ? b : null;
  if (na == null && nb == null) return 0;
  if (na == null) return 1;
  if (nb == null) return -1;
  return dir * (na - nb);
}

function themeStageScore(t: ApiTheme): number {
  const rs = t.relativeStrength1M ?? t.relativeStrengthQualifierRatio;
  const rsNum = typeof rs === "number" && Number.isFinite(rs) ? rs : null;
  return t.accumulation && t.highLiquidity && rsNum != null && rsNum > 0 ? 1 : 0;
}

function leadersRatio(t: ApiTheme): number {
  return t.qualifiedCount / Math.max(t.totalCount, 1);
}

function PerfCell({ v }: { v: number | null | undefined }) {
  if (v == null || !Number.isFinite(v)) {
    return <span className="text-slate-600">—</span>;
  }
  const cls = v >= 0 ? "text-emerald-400" : "text-rose-400";
  return (
    <span className={`font-mono text-xs tabular-nums ${cls}`}>
      {v >= 0 ? "+" : ""}
      {v.toFixed(2)}%
    </span>
  );
}

function SortTh({
  label,
  colKey,
  activeKey,
  dir,
  onSort,
  align = "left",
  thClassName,
}: {
  label: string;
  colKey: ThemeSortKey;
  activeKey: ThemeSortKey;
  dir: "asc" | "desc";
  onSort: (k: ThemeSortKey) => void;
  align?: "left" | "right";
  /** Horizontal padding tweaks (e.g. tighten gap between Theme and first % column). */
  thClassName?: string;
}) {
  const active = activeKey === colKey;
  return (
    <th
      className={`sticky top-0 z-30 whitespace-nowrap border-b border-terminal-border bg-terminal-bg py-2.5 align-bottom ${
        align === "right" ? "text-right" : "text-left"
      } ${thClassName ?? "px-3"}`}
    >
      <button
        type="button"
        title={`Sort by ${label}`}
        aria-label={`Sort by ${label}`}
        onClick={() => onSort(colKey)}
        className={`inline-flex cursor-pointer items-center gap-1 text-[10px] font-semibold uppercase tracking-wider transition-colors ${
          active ? "text-accent" : "text-slate-500 hover:text-slate-300"
        } ${align === "right" ? "w-full justify-end" : ""}`}
      >
        {label}
        {active ? (
          dir === "asc" ? (
            <ArrowUp className="h-3.5 w-3.5 shrink-0" strokeWidth={2.5} aria-hidden />
          ) : (
            <ArrowDown className="h-3.5 w-3.5 shrink-0" strokeWidth={2.5} aria-hidden />
          )
        ) : (
          <ArrowUpDown className="h-3.5 w-3.5 shrink-0 text-slate-500/80" strokeWidth={2.5} aria-hidden />
        )}
      </button>
    </th>
  );
}

/** Semicircular fear gauge: VIX ~10–40 mapped to arc. */
function VixFearGauge({ close, changePct }: { close: number; changePct: number }) {
  const gradId = useId().replace(/:/g, "");
  const min = 10;
  const max = 40;
  const clamped = Math.min(max, Math.max(min, close));
  const t = (clamped - min) / (max - min);

  const cx = 80;
  const cy = 78;
  const r = 58;
  const startAngle = Math.PI;
  const endAngle = 0;
  const needleAngle = startAngle + t * (endAngle - startAngle);
  const nx = cx + (r - 8) * Math.cos(needleAngle);
  const ny = cy - (r - 8) * Math.sin(needleAngle);

  const arcPath = `M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`;

  let mood = "Elevated";
  let moodClass = "text-slate-300";
  let valueClass = "text-slate-100";
  if (clamped < 16) {
    mood = "Complacent";
    moodClass = "text-emerald-400/90";
    valueClass = "text-emerald-300";
  } else if (clamped < 22) {
    mood = "Calm";
    moodClass = "text-cyan-400/90";
    valueClass = "text-cyan-300";
  } else if (clamped < 28) {
    mood = "Elevated";
    moodClass = "text-amber-400/90";
    valueClass = "text-amber-300";
  } else {
    mood = "Fear";
    moodClass = "text-rose-400/90";
    valueClass = "text-amber-300";
  }

  return (
    <div className="flex flex-col items-center justify-center px-1 py-0 text-center">
      <div className="mb-0.5 flex w-full items-center justify-center gap-1.5">
        <Activity className="h-3 w-3 text-slate-600" aria-hidden />
      </div>
      {/* ~15% visual shrink; negative margin recovers layout gap from transform */}
      <div className="origin-top scale-[0.85] max-h-[168px] -mb-5 w-full">
      <svg width="160" height="88" viewBox="0 0 160 88" className="mx-auto block max-h-[75px] w-auto overflow-visible" aria-hidden>
        <defs>
          <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#10b981" stopOpacity="0.9" />
            <stop offset="35%" stopColor="#22d3ee" stopOpacity="0.85" />
            <stop offset="65%" stopColor="#f59e0b" stopOpacity="0.9" />
            <stop offset="100%" stopColor="#f43f5e" stopOpacity="0.95" />
          </linearGradient>
        </defs>
        <path
          d={arcPath}
          fill="none"
          stroke={`url(#${gradId})`}
          strokeWidth="9"
          strokeLinecap="round"
        />
        <line
          x1={cx}
          y1={cy}
          x2={nx}
          y2={ny}
          stroke="#e2e8f0"
          strokeWidth="2.5"
          strokeLinecap="round"
        />
        <circle cx={cx} cy={cy} r={5} fill="#1e293b" stroke="#64748b" strokeWidth="1.5" />
        <text x={cx} y={cy + 22} textAnchor="middle" className="fill-slate-500" style={{ fontSize: 9 }}>
          {min} — {max}
        </text>
      </svg>
      <p className={`font-mono text-2xl font-extrabold leading-tight tracking-tight ${valueClass}`}>
        {Number.isFinite(close) ? close.toFixed(2) : "—"}
      </p>
      <p className={`text-[10px] font-medium leading-tight ${moodClass}`}>{mood}</p>
      <p className={`mt-0.5 font-mono text-[10px] leading-tight ${pctClass(changePct)}`}>
        {changePct >= 0 ? "+" : ""}
        {Number.isFinite(changePct) ? changePct.toFixed(2) : "—"}% session
      </p>
      </div>
    </div>
  );
}

const LONG_SCAN_MS = 10_000;

/** Rule-of-thumb: SPX ~1-day expected move from VIX (VIX÷16). */
function vixDailyExpectedMovePct(vix: number): number {
  if (!Number.isFinite(vix) || vix <= 0) return 0;
  return vix / 16;
}

function defaultThemeSortDir(key: ThemeSortKey): "asc" | "desc" {
  if (key === "theme") return "asc";
  return "desc";
}

function cmpNullableNum(a: number | null | undefined, b: number | null | undefined, dir: number): number {
  const na = a != null && Number.isFinite(a) ? a : null;
  const nb = b != null && Number.isFinite(b) ? b : null;
  if (na == null && nb == null) return 0;
  if (na == null) return 1;
  if (nb == null) return -1;
  return dir * (na - nb);
}

function themeStageScore(t: ApiTheme): number {
  const rs = t.relativeStrength1M ?? t.relativeStrengthQualifierRatio;
  const rsNum = typeof rs === "number" && Number.isFinite(rs) ? rs : null;
  return t.accumulation && t.highLiquidity && rsNum != null && rsNum > 0 ? 1 : 0;
}

function leadersRatio(t: ApiTheme): number {
  return t.qualifiedCount / Math.max(t.totalCount, 1);
}

function PerfCell({ v }: { v: number | null | undefined }) {
  if (v == null || !Number.isFinite(v)) {
    return <span className="text-slate-600">—</span>;
  }
  const cls = v >= 0 ? "text-emerald-400" : "text-rose-400";
  return (
    <span className={`font-mono text-xs tabular-nums ${cls}`}>
      {v >= 0 ? "+" : ""}
      {v.toFixed(2)}%
    </span>
  );
}

function SortTh({
  label,
  colKey,
  activeKey,
  dir,
  onSort,
  align = "left",
  thClassName,
}: {
  label: string;
  colKey: ThemeSortKey;
  activeKey: ThemeSortKey;
  dir: "asc" | "desc";
  onSort: (k: ThemeSortKey) => void;
  align?: "left" | "right";
  /** Horizontal padding tweaks (e.g. tighten gap between Theme and first % column). */
  thClassName?: string;
}) {
  const active = activeKey === colKey;
  return (
    <th
      className={`sticky top-0 z-30 whitespace-nowrap border-b border-terminal-border bg-terminal-bg py-2.5 align-bottom ${
        align === "right" ? "text-right" : "text-left"
      } ${thClassName ?? "px-3"}`}
    >
      <button
        type="button"
        title={`Sort by ${label}`}
        aria-label={`Sort by ${label}`}
        onClick={() => onSort(colKey)}
        className={`inline-flex cursor-pointer items-center gap-1 text-[10px] font-semibold uppercase tracking-wider transition-colors ${
          active ? "text-accent" : "text-slate-500 hover:text-slate-300"
        } ${align === "right" ? "w-full justify-end" : ""}`}
      >
        {label}
        {active ? (
          dir === "asc" ? (
            <ArrowUp className="h-3.5 w-3.5 shrink-0" strokeWidth={2.5} aria-hidden />
          ) : (
            <ArrowDown className="h-3.5 w-3.5 shrink-0" strokeWidth={2.5} aria-hidden />
          )
        ) : (
          <ArrowUpDown className="h-3.5 w-3.5 shrink-0 text-slate-500/80" strokeWidth={2.5} aria-hidden />
        )}
      </button>
    </th>
  );
}

/** Semicircular fear gauge: VIX ~10–40 mapped to arc. */
function VixFearGauge({ close, changePct }: { close: number; changePct: number }) {
  const gradId = useId().replace(/:/g, "");
  const min = 10;
  const max = 40;
  const clamped = Math.min(max, Math.max(min, close));
  const t = (clamped - min) / (max - min);

  const cx = 80;
  const cy = 78;
  const r = 58;
  const startAngle = Math.PI;
  const endAngle = 0;
  const needleAngle = startAngle + t * (endAngle - startAngle);
  const nx = cx + (r - 8) * Math.cos(needleAngle);
  const ny = cy - (r - 8) * Math.sin(needleAngle);

  const arcPath = `M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`;

  let mood = "Elevated";
  let moodClass = "text-slate-300";
  let valueClass = "text-slate-100";
  if (clamped < 16) {
    mood = "Complacent";
    moodClass = "text-emerald-400/90";
    valueClass = "text-emerald-300";
  } else if (clamped < 22) {
    mood = "Calm";
    moodClass = "text-cyan-400/90";
    valueClass = "text-cyan-300";
  } else if (clamped < 28) {
    mood = "Elevated";
    moodClass = "text-amber-400/90";
    valueClass = "text-amber-300";
  } else {
    mood = "Fear";
    moodClass = "text-rose-400/90";
    valueClass = "text-amber-300";
  }

  return (
    <div className="flex flex-col items-center justify-center px-1 py-0 text-center">
      <div className="mb-0.5 flex w-full items-center justify-center gap-1.5">
        <Activity className="h-3.5 w-3.5 text-slate-600" aria-hidden />
      </div>
      {/* ~15% visual shrink; negative margin recovers layout gap from transform */}
      <div className="origin-top scale-[0.85] max-h-[168px] -mb-5 w-full">
      <svg
        width="160"
        height="100"
        viewBox="0 0 160 100"
        className="mx-auto block max-h-[88px] w-auto overflow-visible"
        aria-hidden
      >
        <defs>
          <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#10b981" stopOpacity="0.9" />
            <stop offset="35%" stopColor="#22d3ee" stopOpacity="0.85" />
            <stop offset="65%" stopColor="#f59e0b" stopOpacity="0.9" />
            <stop offset="100%" stopColor="#f43f5e" stopOpacity="0.95" />
          </linearGradient>
        </defs>
        <path
          d={arcPath}
          fill="none"
          stroke={`url(#${gradId})`}
          strokeWidth="9"
          strokeLinecap="round"
        />
        <line
          x1={cx}
          y1={cy}
          x2={nx}
          y2={ny}
          stroke="#e2e8f0"
          strokeWidth="2.5"
          strokeLinecap="round"
        />
        <circle cx={cx} cy={cy} r={5} fill="#1e293b" stroke="#64748b" strokeWidth="1.5" />
        {/* Keep scale inside viewBox so layout height includes it (avoids overlap with VIX value below). */}
        <text
          x={cx}
          y={96}
          textAnchor="middle"
          dominantBaseline="middle"
          className="fill-slate-500"
          style={{ fontSize: 10.8 }}
        >
          {min} — {max}
        </text>
      </svg>
      <p className={`mt-2 font-mono text-[1.8rem] font-extrabold leading-tight tracking-tight ${valueClass}`}>
        {Number.isFinite(close) ? close.toFixed(2) : "—"}
      </p>
      {/* Mood + session % on one line, centered. */}
      <p className="flex w-full flex-wrap items-baseline justify-center gap-x-1.5 text-center text-[0.9rem] font-medium leading-tight">
        <span className={moodClass}>{mood}</span>
        <span className={`font-mono tabular-nums ${pctClass(changePct)}`}>
          {changePct >= 0 ? "+" : ""}
          {Number.isFinite(changePct) ? changePct.toFixed(2) : "—"}%
        </span>
      </p>
      </div>
    </div>
  );
}

const LONG_SCAN_MS = 10_000;

export function ThemeLeaderboard() {
  const [payload, setPayload] = useState<ApiPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [longScan, setLongScan] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [autoRefreshEnabled] = useState(true);
  const [pollMs, setPollMs] = useState<number>(REFRESH_BASE_MS);
  const [nextRefreshAt, setNextRefreshAt] = useState<number | null>(null);
  const [backoffActive, setBackoffActive] = useState(false);
  const [countdownSec, setCountdownSec] = useState<number | null>(null);
  const [vixPanelMinimized, setVixPanelMinimized] = useState(false);
  const [macroGaugesMinimized, setMacroGaugesMinimized] = useState(false);
  const [audioAlertsEnabled, setAudioAlertsEnabled] = useState(false);
  const [workspaceTab, setWorkspaceTab] = useState<"scanner" | "gappers">("scanner");
  const [leaderView, setLeaderView] = useState<"themes" | "industry" | "scanner">("themes");
  const [themeSortKey, setThemeSortKey] = useState<ThemeSortKey>("rs1m");
  const [themeSortDir, setThemeSortDir] = useState<"asc" | "desc">("desc");
  const [rsSnapshotHover, setRsSnapshotHover] = useState<RsChartRow | null>(null);
  const [rsSnapshotHoverIndex, setRsSnapshotHoverIndex] = useState<number | null>(null);
  const [rsSnapshotMinimized, setRsSnapshotMinimized] = useState(false);
  const [spotlightThemeName, setSpotlightThemeName] = useState<string | null>(null);
  const [scannerPayload, setScannerPayload] = useState<ApiPayload | null>(null);
  const [scannerLoading, setScannerLoading] = useState(false);
  const [universeSpotlight, setUniverseSpotlight] = useState<ThemeUniverseSpotlight | null>(null);
  const [tickerQuery, setTickerQuery] = useState("");
  const [tickerIntel, setTickerIntel] = useState<TickerIntel | null>(null);
  const [tickerIntelOpen, setTickerIntelOpen] = useState(false);
  const [tickerIntelLoading, setTickerIntelLoading] = useState(false);
  const [tickerSuggest, setTickerSuggest] = useState<TickerSuggestRow[]>([]);
  const [tickerSuggestLoading, setTickerSuggestLoading] = useState(false);
  const [premarketBrief, setPremarketBrief] = useState<PremarketBrief | null>(null);
  const [premarketLoading, setPremarketLoading] = useState(false);
  const epAudioAnnouncedRef = useRef<Set<string>>(new Set());
  const epBurstAudioAnnouncedRef = useRef<Set<string>>(new Set());
  const audioCtxRef = useRef<AudioContext | null>(null);

  useEffect(() => {
    let active = true;
    const longScanTimer = window.setTimeout(() => {
      if (active) setLongScan(true);
    }, LONG_SCAN_MS);

    async function loadThemes(background: boolean) {
      if (background) {
        setRefreshing(true);
      } else {
        setLoading(true);
      }
      try {
        const response = await fetch(`${API_BASE_URL}/api/themes?view=${leaderView}`);
        const retryAfterRaw = response.headers.get("Retry-After");

        if (!response.ok) {
          // If backend is rate-limited, honor Retry-After and keep auto refresh alive.
          if (response.status === 429 && retryAfterRaw) {
            const ra = Number(retryAfterRaw);
            if (Number.isFinite(ra) && ra > 0) {
              setPollMs(Math.min(REFRESH_MAX_MS, Math.max(REFRESH_BASE_MS, Math.round(ra * 1000))));
            }
          }
          throw new Error(`API request failed with status ${response.status}`);
        }
        const data = (await response.json()) as ApiPayload;
        if (!active) return;
        setPayload(data);
        setLastUpdated(new Date());
        setError(null);
        if (data.polling?.pollSeconds) {
          const n = Number(data.polling.pollSeconds);
          if (Number.isFinite(n) && n > 0) {
            setPollMs(Math.min(REFRESH_MAX_MS, Math.max(REFRESH_BASE_MS, Math.round(n * 1000))));
          }
        }
        setBackoffActive(Boolean(data.polling?.backoffActive));
      } catch (err) {
        if (!active) return;
        const message = err instanceof Error ? err.message : "Unknown API error";
        setError(message);
      } finally {
        if (active) {
          setLoading(false);
          setRefreshing(false);
          setLongScan(false);
        }
      }
    }

    loadThemes(false);
    let intervalId: number | null = null;
    if (autoRefreshEnabled) {
      setNextRefreshAt(Date.now() + pollMs);
      intervalId = window.setInterval(() => {
        setNextRefreshAt(Date.now() + pollMs);
        void loadThemes(true);
      }, pollMs);
    }

    return () => {
      active = false;
      window.clearTimeout(longScanTimer);
      if (intervalId != null) window.clearInterval(intervalId);
    };
  }, [leaderView, autoRefreshEnabled, pollMs]);

  useEffect(() => {
    if (!autoRefreshEnabled || nextRefreshAt == null) {
      setCountdownSec(null);
      return;
    }
    const id = window.setInterval(() => {
      const sec = Math.max(0, Math.ceil((nextRefreshAt - Date.now()) / 1000));
      setCountdownSec(sec);
    }, 250);
    return () => window.clearInterval(id);
  }, [autoRefreshEnabled, nextRefreshAt]);

  const hotThemeNames = useMemo(() => {
    const list = payload?.themes ?? [];
    const scored = list
      .filter((t): t is ApiTheme & { relativeStrength1M: number } => {
        const rs = t.relativeStrength1M;
        return typeof rs === "number" && Number.isFinite(rs);
      })
      .map((t) => ({ theme: t.theme, rs: t.relativeStrength1M }))
      .sort((a, b) => b.rs - a.rs);
    return new Set(scored.slice(0, HOT_THEME_COUNT).map((x) => x.theme));
  }, [payload]);

  const stockGridItems = useMemo(() => {
    const themes = payload?.themes ?? [];
    const items: { theme: string; stock: ApiStock; themeHot: boolean }[] = [];
    for (const row of themes) {
      const themeHot = hotThemeNames.has(row.theme);
      for (const stock of row.stocks) {
        items.push({ theme: row.theme, stock, themeHot });
      }
    }
    return items;
  }, [payload, hotThemeNames]);

  const themeTableRows = useMemo(() => [...(payload?.themes ?? [])], [payload]);

  const sortedThemeRows = useMemo(() => {
    const rows = [...themeTableRows];
    const dir = themeSortDir === "asc" ? 1 : -1;
    rows.sort((a, b) => {
      switch (themeSortKey) {
        case "theme":
          return dir * a.theme.localeCompare(b.theme);
        case "perf1d":
          return cmpNullableNum(a.perf1D, b.perf1D, dir);
        case "perf1w":
          return cmpNullableNum(a.perf1W, b.perf1W, dir);
        case "perf1m":
          return cmpNullableNum(a.perf1M, b.perf1M, dir);
        case "perf3m":
          return cmpNullableNum(a.perf3M, b.perf3M, dir);
        case "perf6m":
          return cmpNullableNum(a.perf6M, b.perf6M, dir);
        case "rs1m":
          return cmpNullableNum(a.relativeStrength1M, b.relativeStrength1M, dir);
        case "rsRatio":
          return dir * (a.relativeStrengthQualifierRatio - b.relativeStrengthQualifierRatio);
        case "leaders":
          return dir * (leadersRatio(a) - leadersRatio(b));
        case "themeVol":
          return dir * (a.themeDollarVolume - b.themeDollarVolume);
        case "stage":
          return dir * (themeStageScore(a) - themeStageScore(b));
        default:
          return 0;
      }
    });
    return rows;
  }, [themeTableRows, themeSortKey, themeSortDir]);

  const chartData = useMemo(() => {
    const source = themeSortKey === "rs1m" ? sortedThemeRows : themeTableRows;
    return source.map((row) => {
      const rs1m = row.relativeStrength1M;
      const hasSector1m = rs1m != null && Number.isFinite(rs1m);
      const fallback = row.relativeStrengthQualifierRatio;
      const rsDisplay = hasSector1m ? rs1m : fallback;
      return {
        theme: row.theme,
        rsDisplay,
        isGhost: !hasSector1m,
        barFill: !hasSector1m ? "muted" : rs1m < 0 ? "neg" : rs1m > 0 ? "pos" : "zero",
      };
    });
  }, [themeSortKey, sortedThemeRows, themeTableRows]);

  /**
   * Y-bounds hug the data so bars use the full plot height (no symmetric ±12-style auto padding).
   * Padding is small (~4% of span) so the scale still breathes without a dead band above/below bars.
   */
  const rsYDomain = useMemo((): [number, number] => {
    const vals = chartData.map((d) => d.rsDisplay).filter((v): v is number => Number.isFinite(v));
    if (vals.length === 0) return [-1, 1];
    const mn = Math.min(...vals);
    const mx = Math.max(...vals);
    const span = mx - mn;
    const pad =
      span > 1e-9
        ? Math.max(span * 0.04, 0.08)
        : Math.max(Math.abs(mn), Math.abs(mx), 0.5) * 0.04;
    if (mn >= 0) return [0, mx + pad];
    if (mx <= 0) return [mn - pad, 0];
    return [mn - pad, mx + pad];
  }, [chartData]);

  useEffect(() => {
    setRsSnapshotHover(null);
    setRsSnapshotHoverIndex(null);
  }, [payload, themeSortKey, themeSortDir]);

  const onThemeSort = useCallback(
    (key: ThemeSortKey) => {
      if (themeSortKey === key) {
        setThemeSortDir((d) => (d === "asc" ? "desc" : "asc"));
      } else {
        setThemeSortKey(key);
        setThemeSortDir(defaultThemeSortDir(key));
      }
    },
    [themeSortKey],
  );

  const finvizLeaderboardOnly = leaderView === "themes" || leaderView === "industry";

  const ensureScannerLoaded = useCallback(async () => {
    if (!finvizLeaderboardOnly) return; // already using ticker audit in Scanner view
    if (scannerLoading) return;
    if (scannerPayload) return;

    setScannerLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/api/themes?view=scanner`);
      if (!response.ok) {
        throw new Error(`Scanner audit request failed with status ${response.status}`);
      }
      const data = (await response.json()) as ApiPayload;
      setScannerPayload(data);
    } catch (err) {
      console.error(err);
    } finally {
      setScannerLoading(false);
    }
  }, [finvizLeaderboardOnly, scannerLoading, scannerPayload]);

  const loadUniverseSpotlight = useCallback(
    async (label: string) => {
      if (!label) return false;
      try {
        const response = await fetch(`${API_BASE_URL}/api/theme-universe/spotlight?label=${encodeURIComponent(label)}`);
        if (!response.ok) {
          throw new Error(`Theme universe spotlight failed with status ${response.status}`);
        }
        const data = (await response.json()) as ThemeUniverseSpotlight;
        setUniverseSpotlight(data);
        const hasMovers = (data.best?.length ?? 0) > 0 || (data.worst?.length ?? 0) > 0;
        return hasMovers;
      } catch (err) {
        console.error(err);
        setUniverseSpotlight(null);
        return false;
      }
    },
    [],
  );

  useEffect(() => {
    const q = tickerQuery.trim();
    if (!q) {
      setTickerIntel(null);
      setTickerIntelLoading(false);
      setTickerSuggest([]);
      setTickerSuggestLoading(false);
      return;
    }
    const handle = window.setTimeout(() => {
      setTickerIntelLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-intel?ticker=${encodeURIComponent(q)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-intel ${r.status}`);
          return (await r.json()) as TickerIntel;
        })
        .then((data) => {
          setTickerIntel(data);
        })
        .catch(() => setTickerIntel(null))
        .finally(() => setTickerIntelLoading(false));
    }, 220);
    return () => window.clearTimeout(handle);
  }, [tickerQuery]);

  useEffect(() => {
    if (!tickerIntelOpen) return;
    const q = tickerQuery.trim();
    if (q.length < 1) {
      setTickerSuggest([]);
      setTickerSuggestLoading(false);
      return;
    }
    let active = true;
    const handle = window.setTimeout(() => {
      setTickerSuggestLoading(true);
      fetch(`${API_BASE_URL}/api/ticker-suggest?q=${encodeURIComponent(q)}`)
        .then(async (r) => {
          if (!r.ok) throw new Error(`ticker-suggest ${r.status}`);
          return (await r.json()) as { results: TickerSuggestRow[] };
        })
        .then((data) => {
          if (!active) return;
          setTickerSuggest(Array.isArray(data.results) ? data.results : []);
        })
        .catch(() => {
          if (!active) return;
          setTickerSuggest([]);
        })
        .finally(() => {
          if (!active) return;
          setTickerSuggestLoading(false);
        });
    }, 140);
    return () => {
      active = false;
      window.clearTimeout(handle);
    };
  }, [tickerQuery, tickerIntelOpen]);

  useEffect(() => {
    let active = true;
    setPremarketLoading(true);
    fetch(`${API_BASE_URL}/api/news/premarket`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`premarket ${r.status}`);
        return (await r.json()) as PremarketBrief;
      })
      .then((data) => {
        if (!active) return;
        setPremarketBrief(data);
      })
      .catch(() => {
        if (!active) return;
        setPremarketBrief(null);
      })
      .finally(() => {
        if (!active) return;
        setPremarketLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (themeTableRows.length === 0) return;
    const exists = spotlightThemeName
      ? themeTableRows.some((t) => t.theme === spotlightThemeName)
      : false;
    if (!exists) {
      setSpotlightThemeName(sortedThemeRows[0]?.theme ?? themeTableRows[0].theme ?? null);
    }
    // Intentionally only reset on payload/view changes; sorting should not steal the user's selection.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [leaderView, payload]);

  const finvizSpotlightTheme = useMemo(() => {
    if (!spotlightThemeName) return sortedThemeRows[0] ?? themeTableRows[0] ?? null;
    return themeTableRows.find((t) => t.theme === spotlightThemeName) ?? null;
  }, [spotlightThemeName, themeTableRows, sortedThemeRows]);

  const scannerSpotlightTheme = useMemo(() => {
    if (!spotlightThemeName || !scannerPayload?.themes) return null;
    const norm = normalizeThemeLabel(spotlightThemeName);
    return scannerPayload.themes.find((t) => normalizeThemeLabel(t.theme) === norm) ?? null;
  }, [scannerPayload, spotlightThemeName]);

  const spotlightTheme = useMemo(() => finvizSpotlightTheme ?? scannerSpotlightTheme ?? null, [finvizSpotlightTheme, scannerSpotlightTheme]);

  const spotlightStocks = useMemo(() => {
    // 1) Exact scanner theme match (best case).
    if (scannerSpotlightTheme?.stocks?.length) return scannerSpotlightTheme.stocks.slice(0, 12);

    // 2) Finviz theme click in Themes/Industry mode: finviz rows have empty `stocks`.
    //    We still want movers, so approximate by bucket -> scanner sector, then fallback to global scanner stocks.
    const name = spotlightThemeName ?? finvizSpotlightTheme?.theme ?? null;
    if (!name || !scannerPayload?.themes?.length) {
      return finvizSpotlightTheme?.stocks?.slice(0, 12) ?? [];
    }

    const parts = name.split("·").map((p) => p.trim()).filter(Boolean);
    const finvizBucket = parts[0] ?? "";
    const finvizBucketNorm = normalizeThemeLabel(finvizBucket);

    const bucketToScannerSector = (bucketNorm: string): string | null => {
      // Broad mapping to yfinance sector labels used by scanner audit.
      if (!bucketNorm) return null;
      if (bucketNorm.includes("energy")) return "Energy";
      if (bucketNorm.includes("consumer")) return "Consumer";
      if (bucketNorm.includes("financial") || bucketNorm.includes("fintech") || bucketNorm.includes("insurance")) return "Financial";
      // Most Finviz buckets here are effectively Technology for our scanner dataset.
      if (
        bucketNorm.includes("hardware") ||
        bucketNorm.includes("software") ||
        bucketNorm.includes("semiconductor") ||
        bucketNorm.includes("semis") ||
        bucketNorm.includes("cloud") ||
        bucketNorm.includes("iot") ||
        bucketNorm.includes("telecom") ||
        bucketNorm.includes("quantum") ||
        bucketNorm.includes("robotics") ||
        bucketNorm.includes("automation") ||
        bucketNorm.includes("autonomous") ||
        bucketNorm.includes("cybersecurity") ||
        bucketNorm.includes("nanotech") ||
        bucketNorm.includes("big data") ||
        bucketNorm.includes("wearables") ||
        bucketNorm.includes("vr") ||
        bucketNorm.includes("ar")
      ) {
        return "Technology";
      }
      if (bucketNorm.includes("utilities")) return "Utilities";
      if (bucketNorm.includes("healthcare") || bucketNorm.includes("biometrics")) return "Healthcare";
      return null;
    };

    const sectorKey = bucketToScannerSector(finvizBucketNorm);
    const sectorThemes = sectorKey
      ? scannerPayload.themes.filter((t) => normalizeThemeLabel(t.sector ?? "").includes(normalizeThemeLabel(sectorKey)))
      : [];

    const candidateThemes = sectorThemes.length ? sectorThemes : scannerPayload.themes;
    const stocks = candidateThemes.flatMap((t) => t.stocks ?? []);

    // 3) Final fallback: if still empty, return finviz (which is empty in Themes/Industry anyway).
    return stocks.slice(0, 12);
  }, [scannerPayload, scannerSpotlightTheme, spotlightThemeName, finvizSpotlightTheme]);

  const spotlightConstituentsNote = useMemo(() => {
    if (!finvizLeaderboardOnly) return null;
    if (!spotlightThemeName) return null;
    const bucket = finvizBucketFromThemeLabel(spotlightThemeName);
    if (!bucket) return "Theme constituents are not provided in Themes/Industry mode.";
    return `Theme constituents are approximated from Scanner audit (bucket: ${bucket}).`;
  }, [finvizLeaderboardOnly, spotlightThemeName]);
  const spotlightAvgPrice = useMemo(() => {
    if (spotlightStocks.length === 0) return null;
    return spotlightStocks.reduce((sum, s) => sum + s.close, 0) / spotlightStocks.length;
  }, [spotlightStocks]);
  const spotlightAvgMonth = useMemo(() => {
    const vals = spotlightStocks
      .map((s) => s.month_return_pct)
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
    if (vals.length === 0) return null;
    return vals.reduce((sum, v) => sum + v, 0) / vals.length;
  }, [spotlightStocks]);
  const spotlightTopMovers = useMemo(() => {
    if (leaderView === "themes" && universeSpotlight?.best?.length) return universeSpotlight.best;
    const vals = spotlightStocks
      .filter((s) => typeof s.today_return_pct === "number" && Number.isFinite(s.today_return_pct))
      .slice()
      .sort((a, b) => (b.today_return_pct ?? 0) - (a.today_return_pct ?? 0));
    return vals.slice(0, 3);
  }, [leaderView, spotlightStocks, universeSpotlight]);
  const spotlightWorstMovers = useMemo(() => {
    if (leaderView === "themes" && universeSpotlight?.worst?.length) return universeSpotlight.worst;
    const vals = spotlightStocks
      .filter((s) => typeof s.today_return_pct === "number" && Number.isFinite(s.today_return_pct))
      .slice()
      .sort((a, b) => (a.today_return_pct ?? 0) - (b.today_return_pct ?? 0));
    return vals.slice(0, 3);
  }, [leaderView, spotlightStocks, universeSpotlight]);

  const filteredStockItems = useMemo(() => {
    if (workspaceTab === "gappers") {
      return stockGridItems.filter(({ stock }) => isEpCandidate(stock) || isEpBurst(stock));
    }
    return stockGridItems;
  }, [stockGridItems, workspaceTab]);

  useEffect(() => {
    if (!audioAlertsEnabled || !payload) return;
    const epNew: string[] = [];
    const burstNew: string[] = [];
    for (const { theme, stock } of stockGridItems) {
      const key = `${theme}::${stock.ticker}`;
      if (isEpCandidate(stock) && !epAudioAnnouncedRef.current.has(key)) {
        epAudioAnnouncedRef.current.add(key);
        epNew.push(key);
      }
      if (isEpBurst(stock) && !epBurstAudioAnnouncedRef.current.has(key)) {
        epBurstAudioAnnouncedRef.current.add(key);
        burstNew.push(key);
      }
    }
    epNew.forEach((_, i) => {
      window.setTimeout(() => playSubtleBuzzPing(audioCtxRef), i * 95);
    });
    burstNew.forEach((_, i) => {
      window.setTimeout(() => playSonarPing(audioCtxRef), i * 120);
    });
  }, [audioAlertsEnabled, payload, stockGridItems]);

  const themes = payload?.themes ?? [];

  if (loading) {
    return (
      <div className="flex h-full min-h-0 flex-col items-center justify-center bg-terminal-bg p-8 text-slate-400">
        <div className="flex flex-col items-center gap-3 text-center">
          <div className="h-8 w-8 animate-pulse rounded-full border-2 border-sky-500/30 border-t-sky-400" />
          <p className="text-sm tracking-wide text-slate-300">Loading leaderboard…</p>
          {longScan && (
            <p className="max-w-sm text-xs leading-relaxed text-amber-400/90">
              Still scanning… Finviz + yfinance can take 30–90s on the first run. Cached responses load instantly on
              refresh.
            </p>
          )}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center bg-terminal-bg p-6 text-rose-300">
        Failed to load leaderboard: {error}
      </div>
    );
  }

  if (!payload) {
    return (
      <div className="flex h-full items-center justify-center bg-terminal-bg p-6 text-slate-400">
        No leaderboard data available.
      </div>
    );
  }

  if (themes.length === 0) {
    return (
      <div className="flex h-full items-center justify-center bg-terminal-bg p-6 text-slate-300">
        Searching for A+ Patterns...
      </div>
    );
  }

  const vixClose = typeof payload.vix.close === "number" ? payload.vix.close : 0;
  const vixChange = typeof payload.vix.change_pct === "number" ? payload.vix.change_pct : 0;
  const momentum = payload.market_momentum_score;
  const highVixOverride = vixClose > 25;

  const dailyEm = vixDailyExpectedMovePct(vixClose);
  const autoRefreshLabel = !autoRefreshEnabled
    ? "Auto refresh: OFF"
    : backoffActive
      ? "Auto refresh: BACKOFF"
      : "Auto refresh: ON";
  const autoRefreshDotClass = !autoRefreshEnabled ? "bg-slate-500" : backoffActive ? "bg-amber-400" : "bg-emerald-400";
  const countdownText = (() => {
    if (!autoRefreshEnabled) return "—";
    if (countdownSec == null) return "—";
    const m = Math.floor(countdownSec / 60);
    const s = countdownSec % 60;
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  })();

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden bg-terminal-bg text-slate-200">
      <header className="shrink-0 border-b border-terminal-border bg-terminal-elevated">
        <div className="flex flex-wrap items-center gap-3 px-4 py-2.5">
          <div className="flex min-w-0 items-center gap-2.5">
            <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-accent/15 text-accent">
              <LayoutGrid className="h-4 w-4" aria-hidden />
            </span>
            <div className="min-w-0">
              <h1 className="truncate text-sm font-semibold tracking-tight text-white">Thematic Scanner</h1>
              <p className="truncate text-[11px] text-slate-500">Pure-play clusters · 1M RS · Finviz · yfinance · VIX</p>
            </div>
          </div>
          <div className="fintech-scroll flex min-w-0 flex-1 items-center gap-4 overflow-x-auto border-x border-terminal-border/60 px-3 py-1">
            {(payload.tape ?? []).map((row) => {
              const displayVal = tapeValue(row.close);
              const pct = typeof row.change_pct === "number" ? row.change_pct : 0;
              return (
                <div key={row.label} className="flex shrink-0 flex-col gap-0.5 whitespace-nowrap">
                  <span className="w-full text-center text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                    {row.label}
                  </span>
                  <div className="flex items-baseline justify-between gap-2">
                    <span className="font-mono text-xs text-slate-200">{displayVal}</span>
                    <span className={`font-mono text-[11px] ${pctClass(pct)}`}>
                      {pct === 0 ? "" : `${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%`}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500" aria-hidden />
              <input
                type="search"
                placeholder="Search Ticker…"
                value={tickerQuery}
                onChange={(e) => {
                  setTickerQuery(e.target.value);
                  setTickerIntelOpen(true);
                }}
                onFocus={() => setTickerIntelOpen(true)}
                onBlur={() => {
                  // Delay so clicks inside the card register.
                  window.setTimeout(() => setTickerIntelOpen(false), 120);
                }}
                className="h-9 w-52 rounded-lg border border-terminal-border bg-terminal-bg py-1 pl-8 pr-3 text-xs text-slate-200 placeholder:text-slate-600 focus:border-accent/60 focus:outline-none"
                aria-label="Search Ticker"
              />
              {tickerIntelOpen && (tickerIntelLoading || tickerIntel || tickerSuggestLoading || tickerSuggest.length > 0) ? (
                <div className="absolute left-0 top-[42px] z-50 w-[320px] overflow-hidden rounded-xl border border-terminal-border bg-[#0b1220]/95 shadow-[0_20px_60px_-25px_rgba(0,0,0,0.85)] backdrop-blur">
                  <div className="px-3 py-2.5">
                    {tickerSuggestLoading ? (
                      <div className="mb-2 text-[11px] text-slate-500">Searching…</div>
                    ) : tickerSuggest.length > 0 ? (
                      <div className="mb-2 space-y-1">
                        {tickerSuggest.map((r) => (
                          <button
                            key={`s-${r.ticker}`}
                            type="button"
                            className="flex w-full items-baseline justify-between gap-2 rounded-md border border-terminal-border/50 bg-terminal-bg/40 px-2 py-1 text-left text-[11px] text-slate-200 hover:border-accent/40 hover:bg-terminal-bg/60"
                            onMouseDown={(e) => e.preventDefault()}
                            onClick={() => {
                              setTickerQuery(r.ticker);
                            }}
                            aria-label={`Select ${r.ticker}`}
                          >
                            <span className="font-mono font-semibold text-slate-100">{r.ticker}</span>
                            <span className="min-w-0 flex-1 truncate text-right text-slate-400">{r.name || "—"}</span>
                          </button>
                        ))}
                      </div>
                    ) : null}

                    <div className="flex items-baseline justify-between gap-3">
                      <div className="min-w-0">
                        <div className="flex items-baseline gap-2">
                          <span className="font-mono text-sm font-semibold text-white">{tickerIntel?.ticker ?? tickerQuery.trim().toUpperCase()}</span>
                          {tickerIntel?.close != null ? (
                            <span className="font-mono text-sm text-slate-200">{tickerIntel.close.toFixed(2)}</span>
                          ) : null}
                          {tickerIntel?.today_return_pct != null ? (
                            <span className={`font-mono text-xs ${tickerIntel.today_return_pct >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                              {tickerIntel.today_return_pct >= 0 ? "+" : ""}
                              {tickerIntel.today_return_pct.toFixed(2)}%
                            </span>
                          ) : null}
                        </div>
                        <div className="truncate text-[11px] text-slate-400">{tickerIntelLoading ? "Loading…" : (tickerIntel?.name ?? "—")}</div>
                      </div>
                    </div>

                    <div className="mt-2 border-t border-terminal-border/70 pt-2">
                      <div className="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 text-[11px]">
                        <div className="text-slate-500">Sector</div>
                        <div className="min-w-0 truncate text-slate-200">
                          {tickerIntel?.sector ?? "—"}
                          {tickerIntel?.sector_etf ? <span className="ml-2 font-mono text-slate-400">{tickerIntel.sector_etf}</span> : null}
                        </div>

                        <div className="text-slate-500">Industry</div>
                        <div className="min-w-0 truncate text-slate-200">
                          {tickerIntel?.industry ?? "—"}
                        </div>

                        <div className="text-slate-500">Theme</div>
                        <div className="min-w-0 truncate text-slate-200">{tickerIntel?.theme ?? "—"}</div>

                        <div className="text-slate-500">Sub-Theme</div>
                        <div className="min-w-0 truncate text-slate-200">
                          {tickerIntel?.subtheme ?? "—"}
                          {tickerIntel?.theme_matches?.length ? (
                            <span className="ml-2 text-slate-500">({tickerIntel.theme_matches.length})</span>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
            <button
              type="button"
              className="flex h-9 items-center gap-1.5 rounded-lg border border-terminal-border bg-terminal-card px-3 text-xs font-medium text-slate-300 transition-colors hover:border-accent/40 hover:text-accent"
            >
              <Filter className="h-3.5 w-3.5" aria-hidden />
              Filters
            </button>
          </div>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-terminal-border/80 px-4 py-2">
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setWorkspaceTab("scanner")}
              className={`rounded-full px-4 py-1.5 text-xs font-semibold transition-colors ${
                workspaceTab === "scanner"
                  ? "bg-accent text-white shadow-[0_0_20px_-6px_rgba(91,141,239,0.65)]"
                  : "border border-terminal-border bg-terminal-bg text-slate-400 hover:border-slate-600 hover:text-slate-300"
              }`}
            >
              Thematic Scanner
            </button>
            <button
              type="button"
              onClick={() => setWorkspaceTab("gappers")}
              className={`rounded-full px-4 py-1.5 text-xs font-semibold transition-colors ${
                workspaceTab === "gappers"
                  ? "bg-accent text-white shadow-[0_0_20px_-6px_rgba(91,141,239,0.65)]"
                  : "border border-terminal-border bg-terminal-bg text-slate-400 hover:border-slate-600 hover:text-slate-300"
              }`}
            >
              Pre-Market Gappers
            </button>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setAudioAlertsEnabled((v) => !v)}
              aria-pressed={audioAlertsEnabled}
              className={`rounded-lg border px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider transition-colors ${
                audioAlertsEnabled
                  ? "border-accent/50 bg-accent/15 text-accent"
                  : "border-terminal-border bg-terminal-bg text-slate-500 hover:border-slate-600"
              }`}
              title="EP candidate: buzz ping. EP burst: sonar ping."
            >
              Audio Alerts
            </button>
            <div
              className="flex items-center gap-2 rounded-lg border border-terminal-border bg-terminal-bg px-3 py-1.5"
              title={lastUpdated ? `Last updated ${lastUpdated.toLocaleString()}` : undefined}
            >
              {refreshing ? (
                <span className="relative h-2 w-2 shrink-0 animate-spin rounded-full border-2 border-accent/30 border-t-accent" />
              ) : (
                <span className="relative flex h-2 w-2 shrink-0">
                  <span className="live-dot-ring absolute inline-flex h-full w-full rounded-full bg-emerald-400/40" />
                  <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-400" />
                </span>
              )}
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                {refreshing ? "Updating" : "Live"}
              </span>
              {lastUpdated && !refreshing && (
                <span className="font-mono text-[10px] text-slate-500">{formatUpdatedClock(lastUpdated)}</span>
              )}
            </div>
            <div
              className="flex items-center gap-2 rounded-lg border border-terminal-border bg-terminal-bg px-3 py-1.5"
              title={autoRefreshEnabled ? `Next refresh in ${countdownText}` : "Auto refresh disabled"}
            >
              <span className="relative flex h-2 w-2 shrink-0">
                <span className={`relative inline-flex h-2 w-2 rounded-full ${autoRefreshDotClass}`} />
              </span>
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
                {autoRefreshLabel}
              </span>
              <span className="font-mono text-[10px] text-slate-500">{countdownText}</span>
            </div>
          </div>
        </div>
      </header>

      {highVixOverride && (
        <div className="flex shrink-0 items-center justify-center gap-2 border-b border-rose-500/35 bg-rose-950/40 px-4 py-2 text-center shadow-[0_0_24px_-8px_rgba(244,63,94,0.45)]">
          <Zap className="h-4 w-4 shrink-0 text-amber-400" aria-hidden />
          <p className="text-xs font-semibold text-rose-100">
            <span className="uppercase tracking-wider text-amber-300">Breaking risk · </span>
            VIX elevated — widen stops, reduce size, favor liquid names.
          </p>
        </div>
      )}

      <div className="flex min-h-0 min-w-0 flex-1 flex-row overflow-x-auto overflow-y-hidden">
        <aside className="fintech-scroll flex h-full min-h-0 w-[380px] min-w-[380px] shrink-0 flex-col gap-4 overflow-y-auto border-r border-terminal-border bg-terminal-bg p-4">
          <section className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="border-b border-terminal-border px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              Market regime
            </header>
            <div className={`rounded-b-xl px-4 py-3 text-center ${trendRibbonSurfaceClass(momentum?.state)}`}>
              <p className="text-sm font-semibold leading-snug text-slate-100">
                {momentum?.message ?? "Loading market momentum score…"}
              </p>
            </div>
          </section>

          <section className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="flex items-center justify-between gap-2 border-b border-terminal-border px-4 py-2">
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">VIX fear gauge</span>
              <button
                type="button"
                onClick={() => setVixPanelMinimized((v) => !v)}
                className="rounded border border-terminal-border bg-terminal-bg px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-slate-400 transition-colors hover:border-slate-500 hover:text-slate-200"
              >
                {vixPanelMinimized ? "Expand" : "Minimize"}
              </button>
            </header>
            <div className={`p-4 ${vixPanelMinimized ? "pb-3" : ""}`}>
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start">
                <div className="flex-1">
                  {!vixPanelMinimized ? (
                    <p className="mb-2 text-center text-[10px] font-medium uppercase tracking-wider text-slate-500">{payload.vix.symbol}</p>
                  ) : null}
                  <VixFearGauge close={vixClose} changePct={vixChange} />
                </div>
                {!vixPanelMinimized ? (
                  <div className="flex-1 rounded-lg border border-terminal-border bg-terminal-bg/80 p-3">
                    <p className="text-[10px] font-bold uppercase tracking-wider text-accent">Expected ATR% move</p>
                    <p className="mt-2 font-mono text-lg font-bold text-amber-300">±{dailyEm.toFixed(2)}%</p>
                    <p className="mt-1 text-[11px] leading-relaxed text-slate-400">
                      Elevated VIX = wider swings; prioritize liquidity and defined risk.
                    </p>
                  </div>
                ) : null}
              </div>
            </div>
          </section>

          <section className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="flex items-center justify-between gap-2 border-b border-terminal-border px-4 py-2">
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Macro gauges (TradingView)</span>
              <button
                type="button"
                onClick={() => setMacroGaugesMinimized((v) => !v)}
                className="rounded border border-terminal-border bg-terminal-bg px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-slate-400 transition-colors hover:border-slate-500 hover:text-slate-200"
              >
                {macroGaugesMinimized ? "Expand" : "Minimize"}
              </button>
            </header>
            <div className="p-3">
              {!macroGaugesMinimized ? (
                <>
                  <p className="text-[11px] leading-relaxed text-slate-500">
                    Scroll here for the breadth/credit widgets (kept outside the VIX window to preserve its original size).
                  </p>
                  <div className="mt-3 grid grid-cols-1 gap-2">
                    <TradingViewMiniSymbol symbol="FRED:BAMLH0A0HYM2" title="HY Credit (BAMLH0A0HYM2)" />
                    <TradingViewMiniSymbol symbol="INDEX:S5FI" title="S5FI" />
                    <TradingViewMiniSymbol symbol="INDEX:MMTH" title="MMTH" />
                  </div>
                </>
              ) : (
                <p className="text-[11px] leading-relaxed text-slate-500">
                  Minimized — expand to view HY Credit, S5FI, and MMTH widgets.
                </p>
              )}
            </div>
          </section>

          <section className="rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="border-b border-terminal-border px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              Liquidity & flow
            </header>
            <MarketSummary summary={payload.marketFlowSummary} />
          </section>

          <section className="flex min-h-[200px] flex-1 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
            <header className="shrink-0 border-b border-terminal-border px-4 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              News
            </header>
            <div className="fintech-scroll min-h-0 flex-1 overflow-y-auto p-3">
              {premarketLoading ? (
                <p className="text-[11px] leading-relaxed text-slate-500">Loading pre-market brief…</p>
              ) : premarketBrief?.sections?.length ? (
                <article className="min-h-0 space-y-4">
                  <header className="rounded-lg border border-terminal-border bg-terminal-bg/50 p-3">
                    <div className="flex items-center gap-2">
                      <span className="flex h-7 w-7 items-center justify-center rounded-md bg-amber-400/10 text-amber-300">
                        <Sunrise className="h-4 w-4" aria-hidden />
                      </span>
                      <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-300">Pre‑Market Brief</p>
                    </div>
                    <p className="mt-1 text-[11px] text-slate-500">
                      Released <span className="font-semibold text-slate-300">8:03am ET</span>
                      <span className="text-slate-700"> · </span>
                      Generated <span className="font-mono text-slate-300">{premarketBrief.generated_at_utc ?? "—"}</span>
                    </p>
                  </header>

                  <div className="rounded-lg border border-terminal-border bg-terminal-bg/30 p-3">
                    {premarketBrief.narrative?.length ? (
                      <div className="space-y-4 text-[12px] leading-relaxed text-slate-300">
                        {premarketBrief.narrative.map((para, i) => (
                          <p key={`narr-${i}`} className="text-slate-300">
                            {highlightBriefText(para)}
                          </p>
                        ))}
                      </div>
                    ) : (
                      <div className="space-y-4 text-[12px] leading-relaxed text-slate-300">
                        {premarketBrief.sections.map((sec) => (
                          <section key={sec.title} className="space-y-1.5">
                            <h3 className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{sec.title}</h3>
                            {sec.bullets.map((b, j) => (
                              <p key={`${sec.title}-${j}`} className="text-slate-300">
                                {highlightBriefText(b)}
                              </p>
                            ))}
                          </section>
                        ))}
                      </div>
                    )}
                  </div>

                  {premarketBrief.headlines?.length ? (
                    <section className="rounded-lg border border-terminal-border bg-terminal-bg/30 p-3">
                      <h3 className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Top headlines</h3>
                      <ul className="mt-2 space-y-1.5 text-[11px] leading-snug text-slate-300">
                        {premarketBrief.headlines.slice(0, 8).map((h) => (
                          <li key={h.link || h.title} className="min-w-0">
                            {h.link ? (
                              <a
                                href={h.link}
                                target="_blank"
                                rel="noreferrer"
                                className="block truncate text-slate-300 hover:text-white hover:underline"
                                title={h.title}
                              >
                                {h.title}
                              </a>
                            ) : (
                              <span className="block truncate text-slate-300" title={h.title}>
                                {h.title}
                              </span>
                            )}
                          </li>
                        ))}
                      </ul>
                      <p className="mt-2 text-[10px] text-slate-600">Sources: TradingView (markets), Google News (RSS)</p>
                    </section>
                  ) : null}
                </article>
              ) : (
                <div className="space-y-2">
                  <p className="text-[11px] leading-relaxed text-slate-500">
                    No brief generated yet. It will publish every trading day at <span className="text-slate-300">8:03am ET</span>.
                  </p>
                  <button
                    type="button"
                    className="rounded-md border border-terminal-border bg-terminal-bg px-3 py-2 text-[11px] font-semibold text-slate-300 hover:border-accent/40 hover:text-white"
                    onClick={() => {
                      setPremarketLoading(true);
                      fetch(`${API_BASE_URL}/api/news/premarket/refresh`, { method: "POST" })
                        .then(async (r) => {
                          if (!r.ok) throw new Error(`premarket refresh ${r.status}`);
                          return (await r.json()) as PremarketBrief;
                        })
                        .then((data) => setPremarketBrief(data))
                        .finally(() => setPremarketLoading(false));
                    }}
                  >
                    Generate now
                  </button>
                </div>
              )}
            </div>
          </section>
        </aside>

        <div className="fintech-scroll flex h-full min-h-0 min-w-0 flex-1 basis-0 flex-col overflow-y-auto bg-terminal-bg p-4">
          <div className="mb-4 flex w-full min-w-0 flex-col gap-4 lg:flex-row lg:items-stretch lg:gap-3">
          <section className="flex w-full min-w-0 shrink-0 flex-col rounded-xl border border-terminal-border bg-terminal-card p-2 shadow-sm lg:h-full lg:w-1/4 lg:max-w-[25%]">
            <div className="mb-2 flex items-center justify-between gap-2">
              <p className="shrink-0 text-[10px] font-semibold uppercase tracking-wider text-slate-500">RS snapshot</p>
              <button
                type="button"
                onClick={() => setRsSnapshotMinimized((v) => !v)}
                className="rounded border border-terminal-border bg-terminal-bg px-2 py-0.5 text-[9px] font-semibold uppercase tracking-wider text-slate-400 transition-colors hover:border-slate-500 hover:text-slate-200"
              >
                {rsSnapshotMinimized ? "Expand" : "Minimize"}
              </button>
            </div>
            {!rsSnapshotMinimized ? (
              <p className="mb-2 shrink-0 text-[9px] leading-snug text-slate-600 lg:hidden">
                Hover a bar — strip under chart shows theme and reading
              </p>
            ) : null}
            <div className="flex w-full shrink-0 flex-col overflow-hidden rounded-lg border border-terminal-border/40 bg-terminal-bg/50">
              <div className="w-full min-w-0 max-w-full overflow-x-hidden overflow-y-hidden">
                <div className="flex w-full min-w-0 shrink-0 flex-col">
                  <div
                    className={`relative box-border w-full min-w-0 shrink-0 rounded border border-dashed border-slate-600/50 bg-terminal-bg/40 p-0.5 ${
                      rsSnapshotMinimized ? "h-[82px]" : "h-[136px]"
                    }`}
                  >
                    {rsSnapshotMinimized ? (
                      <div className="pointer-events-none absolute inset-x-1 top-1 z-20 flex justify-center">
                        {rsSnapshotHover ? (
                          <span className="inline-flex max-w-full items-center gap-1 truncate rounded border border-terminal-border/80 bg-terminal-bg/90 px-1.5 py-0.5 font-mono text-[10.5px] leading-none text-slate-300">
                            <span className="truncate text-slate-200">{rsSnapshotHover.theme}</span>
                            <span className="text-slate-600">·</span>
                            <span
                              className={`tabular-nums ${
                                rsSnapshotHover.isGhost
                                  ? "text-slate-400"
                                  : rsSnapshotHover.rsDisplay >= 0
                                    ? "text-emerald-400"
                                    : "text-rose-400"
                              }`}
                            >
                              {rsSnapshotHover.isGhost
                                ? `${rsSnapshotHover.rsDisplay.toFixed(2)}`
                                : `${rsSnapshotHover.rsDisplay >= 0 ? "+" : ""}${rsSnapshotHover.rsDisplay.toFixed(2)}%`}
                            </span>
                          </span>
                        ) : (
                          <span className="rounded border border-terminal-border/70 bg-terminal-bg/85 px-1.5 py-0.5 text-[9.2px] uppercase tracking-wider text-slate-500">
                            Hover bars
                          </span>
                        )}
                      </div>
                    ) : null}
                    <ResponsiveContainer
                      width="100%"
                      height="100%"
                      minWidth={160}
                      minHeight={100}
                      initialDimension={{ width: 400, height: 136 }}
                    >
                      <BarChart
                        data={chartData}
                        margin={{ top: 4, right: 4, left: 2, bottom: 2 }}
                        barCategoryGap={4}
                        barGap={2}
                        onMouseLeave={() => {
                          setRsSnapshotHover(null);
                          setRsSnapshotHoverIndex(null);
                        }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#2c2c32" vertical={false} />
                        <XAxis dataKey="theme" type="category" hide height={0} padding={{ left: 0, right: 0 }} />
                        <YAxis hide domain={rsYDomain} />
                        <Bar
                          dataKey="rsDisplay"
                          maxBarSize={14}
                          radius={[1, 1, 0, 0]}
                          onMouseEnter={(item, index) => {
                            const p = item.payload as RsChartRow | undefined;
                            if (!p?.theme) return;
                            setRsSnapshotHover(p);
                            setRsSnapshotHoverIndex(index);
                          }}
                        >
                          {chartData.map((entry, i) => (
                            <Cell
                              key={`cell-${entry.theme}-${i}`}
                              fill={
                                entry.barFill === "muted"
                                  ? "#47556966"
                                  : entry.barFill === "neg"
                                    ? "#f43f5e"
                                    : entry.barFill === "zero"
                                      ? "#64748b"
                                      : "#5b8def"
                              }
                              fillOpacity={rsSnapshotHoverIndex === null ? 1 : rsSnapshotHoverIndex === i ? 1 : 0.45}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
              {!rsSnapshotMinimized ? (
                <div
                  className="flex min-h-[2rem] min-w-0 shrink-0 items-center justify-center border-t border-terminal-border/50 bg-terminal-bg/90 px-2 py-1.5"
                  aria-live="polite"
                  title={rsSnapshotHover?.theme ?? undefined}
                >
                  {rsSnapshotHover ? (
                    <p className="flex w-full min-w-0 justify-center leading-tight">
                      <span className="inline-flex min-w-0 max-w-full flex-nowrap items-center justify-center gap-x-1.5 overflow-hidden text-center whitespace-nowrap">
                        <span className="min-w-0 shrink truncate text-[11px] font-semibold text-slate-100">
                          {rsSnapshotHover.theme}
                        </span>
                        <span className="shrink-0 font-mono text-[8.8px] text-slate-500">
                          <span className="font-medium text-slate-500">
                            {rsSnapshotHover.isGhost ? "RS" : "Sector 1M"}
                          </span>
                          <span className="text-slate-600"> · </span>
                          <span className="tabular-nums text-slate-400">
                            {rsSnapshotHover.isGhost
                              ? `${rsSnapshotHover.rsDisplay.toFixed(2)} (leaders ratio)`
                              : `${rsSnapshotHover.rsDisplay.toFixed(2)}%`}
                          </span>
                        </span>
                      </span>
                    </p>
                  ) : (
                    <p className="w-full min-w-0 px-1 text-center text-[11px] leading-tight text-slate-600">
                      Hover a bar to show theme and reading
                    </p>
                  )}
                </div>
              ) : null}
            </div>
            {!rsSnapshotMinimized ? (
              <p className="mt-2 shrink-0 text-[9px] uppercase leading-snug tracking-wide text-slate-600">
                Sector 1M % · Blue / red / grey · Hidden axis; strip under chart shows hovered theme
              </p>
            ) : null}

            <section
              className={`flex min-h-0 flex-1 flex-col rounded-lg border border-terminal-border bg-terminal-bg/50 ${
                rsSnapshotMinimized ? "mt-2" : "mt-3 min-h-[220px]"
              }`}
            >
              <header className="flex items-center justify-between gap-2 border-b border-terminal-border px-3 py-2">
                <p className="truncate text-[10px] font-semibold uppercase tracking-wider text-slate-500">Theme spotlight</p>
                {spotlightTheme ? (
                  <span className="font-mono text-[10px] text-slate-400">{spotlightTheme.totalCount} names</span>
                ) : null}
              </header>

              {spotlightTheme ? (
                <div className="fintech-scroll flex min-h-0 flex-1 flex-col overflow-y-auto p-2">
                  <div
                    className={`rounded-md border px-2.5 py-2 ${
                      (spotlightTheme.perf1D ?? 0) >= 0
                        ? "border-emerald-500/40 bg-emerald-500/20"
                        : "border-rose-500/40 bg-rose-500/20"
                    }`}
                  >
                    <p className="truncate text-sm font-semibold text-slate-100">
                      {spotlightThemeName ?? spotlightTheme.theme}
                    </p>
                    <p
                      className={`mt-1 font-mono text-lg font-bold ${
                        (spotlightTheme.perf1D ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300"
                      }`}
                    >
                      {(spotlightTheme.perf1D ?? 0) >= 0 ? "+" : ""}
                      {(spotlightTheme.perf1D ?? 0).toFixed(1)}%
                    </p>
                  </div>

                  <div className="mt-2 grid grid-cols-3 gap-2">
                    <div className="rounded-md border border-terminal-border bg-terminal-card/70 p-2">
                      <p className="text-[9px] uppercase tracking-wide text-slate-500">Avg price</p>
                      <p className="mt-1 font-mono text-sm text-slate-200">{spotlightAvgPrice == null ? "—" : `$${spotlightAvgPrice.toFixed(2)}`}</p>
                    </div>
                    <div className="rounded-md border border-terminal-border bg-terminal-card/70 p-2">
                      <p className="text-[9px] uppercase tracking-wide text-slate-500">Avg 1M</p>
                      <p className={`mt-1 font-mono text-sm ${spotlightAvgMonth == null ? "text-slate-400" : spotlightAvgMonth >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                        {spotlightAvgMonth == null ? "—" : `${spotlightAvgMonth >= 0 ? "+" : ""}${spotlightAvgMonth.toFixed(1)}%`}
                      </p>
                    </div>
                    <div className="rounded-md border border-terminal-border bg-terminal-card/70 p-2">
                      <p className="text-[9px] uppercase tracking-wide text-slate-500">RS 1M</p>
                      <p className={`mt-1 font-mono text-sm ${(spotlightTheme.relativeStrength1M ?? 0) >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                        {spotlightTheme.relativeStrength1M == null
                          ? "—"
                          : `${spotlightTheme.relativeStrength1M >= 0 ? "+" : ""}${spotlightTheme.relativeStrength1M.toFixed(1)}%`}
                      </p>
                    </div>
                  </div>

                  {spotlightConstituentsNote ? (
                    <p className="mt-2 text-[10px] leading-snug text-slate-500">{spotlightConstituentsNote}</p>
                  ) : null}

                  <div className="mt-2 rounded-md border border-terminal-border bg-terminal-card/60 p-2">
                    <p className="text-[9px] uppercase tracking-wide text-slate-500">Today&apos;s movers</p>

                    <div className="mt-1 flex flex-col gap-2">
                      <div className="min-w-0">
                        <p className="text-[9px] text-slate-500">Best</p>
                        <div className="mt-1 flex flex-wrap gap-1">
                          {spotlightTopMovers.length > 0 ? (
                            spotlightTopMovers.map((s) => (
                              <span
                                key={`top-${s.ticker}`}
                                className={`inline-flex items-center gap-1.5 rounded border px-2 py-1 font-mono text-[10px] ${
                                  (s.today_return_pct ?? 0) >= 0
                                    ? "border-emerald-500/35 bg-emerald-500/10 text-emerald-300"
                                    : "border-rose-500/35 bg-rose-500/10 text-rose-300"
                                }`}
                                title={`${s.ticker} ${(s.today_return_pct ?? 0) >= 0 ? "+" : ""}${(s.today_return_pct ?? 0).toFixed(1)}%`}
                              >
                                <span className="font-semibold">{s.ticker}</span>
                                <span className="tabular-nums">
                                  {(s.today_return_pct ?? 0) >= 0 ? "+" : ""}
                                  {(s.today_return_pct ?? 0).toFixed(1)}%
                                </span>
                              </span>
                            ))
                          ) : (
                            <span className="text-[10px] text-slate-600">
                              {scannerLoading
                                ? "Running Scanner audit…"
                                : finvizLeaderboardOnly
                                  ? "No ticker audit in Themes/Industry — switch to Scanner."
                                  : "No ticker movers"}
                            </span>
                          )}
                        </div>
                      </div>

                      <div className="min-w-0">
                        <p className="text-[9px] text-slate-500">Worst</p>
                        <div className="mt-1 flex flex-wrap gap-1">
                          {spotlightWorstMovers.length > 0 ? (
                            spotlightWorstMovers.map((s) => (
                              <span
                                key={`worst-${s.ticker}`}
                                className={`inline-flex items-center gap-1.5 rounded border px-2 py-1 font-mono text-[10px] ${
                                  (s.today_return_pct ?? 0) >= 0
                                    ? "border-emerald-500/35 bg-emerald-500/10 text-emerald-300"
                                    : "border-rose-500/35 bg-rose-500/10 text-rose-300"
                                }`}
                                title={`${s.ticker} ${(s.today_return_pct ?? 0) >= 0 ? "+" : ""}${(s.today_return_pct ?? 0).toFixed(1)}%`}
                              >
                                <span className="font-semibold">{s.ticker}</span>
                                <span className="tabular-nums">
                                  {(s.today_return_pct ?? 0) >= 0 ? "+" : ""}
                                  {(s.today_return_pct ?? 0).toFixed(1)}%
                                </span>
                              </span>
                            ))
                          ) : (
                            <span className="text-[10px] text-slate-600">
                              {scannerLoading
                                ? "Running Scanner audit…"
                                : finvizLeaderboardOnly
                                  ? "No ticker audit in Themes/Industry — switch to Scanner."
                                  : "No ticker movers"}
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  </div>

                  {!finvizLeaderboardOnly ? (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {(spotlightTheme.leaders.length > 0 ? spotlightTheme.leaders : spotlightStocks.map((s) => s.ticker))
                        .slice(0, 12)
                        .map((ticker) => (
                          <span
                            key={`spot-${ticker}`}
                            className="rounded border border-terminal-border bg-terminal-card px-1.5 py-0.5 font-mono text-[10px] text-slate-300"
                          >
                            {ticker}
                          </span>
                        ))}
                    </div>
                  ) : null}
                </div>
              ) : (
                <div className="flex min-h-[170px] items-center justify-center p-3 text-center text-[11px] text-slate-500">
                  No theme data available yet.
                </div>
              )}
            </section>
          </section>

          <section className="flex min-h-0 min-w-0 w-full flex-1 shrink-0 flex-col rounded-xl border border-terminal-border bg-terminal-card shadow-sm lg:min-w-0">
            <div className="flex flex-wrap items-center justify-between gap-2 border-b border-terminal-border px-4 py-3">
              <h2 className="text-sm font-semibold text-white">Leaderboard</h2>
              <div className="flex flex-wrap justify-end gap-0.5 rounded-lg border border-terminal-border bg-terminal-bg p-0.5">
                <button
                  type="button"
                  onClick={() => setLeaderView("themes")}
                  className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
                    leaderView === "themes" ? "bg-accent text-white" : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  Themes
                </button>
                <button
                  type="button"
                  onClick={() => setLeaderView("industry")}
                  className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
                    leaderView === "industry" ? "bg-accent text-white" : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  Industry
                </button>
                <button
                  type="button"
                  onClick={() => setLeaderView("scanner")}
                  className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
                    leaderView === "scanner" ? "bg-accent text-white" : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  Scanner
                </button>
              </div>
            </div>
            <div className="fintech-scroll max-h-[min(29rem,55vh)] min-w-0 overflow-y-auto overflow-x-hidden">
              <table className="w-full table-fixed border-separate border-spacing-0 text-left text-xs">
                <colgroup>
                  <col style={{ width: "3%" }} />
                  <col style={{ width: "26%" }} />
                  <col style={{ width: "7.5%" }} />
                  {Array.from({ length: 9 }, (_, i) => (
                    <col key={i} style={{ width: `${63.5 / 9}%` }} />
                  ))}
                </colgroup>
                <thead>
                  <tr className="border-0">
                    <th className="sticky top-0 z-30 whitespace-nowrap border-b border-terminal-border bg-terminal-bg px-3 py-2.5 text-left align-bottom text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                      #
                    </th>
                    <SortTh label="Theme" colKey="theme" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} thClassName="pl-3 pr-1" />
                    <SortTh
                      label="1D %"
                      colKey="perf1d"
                      activeKey={themeSortKey}
                      dir={themeSortDir}
                      onSort={onThemeSort}
                      align="right"
                      thClassName="pl-1 pr-3"
                    />
                    <SortTh label="1W %" colKey="perf1w" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="1M %" colKey="perf1m" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="3M %" colKey="perf3m" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="6M %" colKey="perf6m" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="1M RS" colKey="rs1m" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="RS ratio" colKey="rsRatio" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="Leaders" colKey="leaders" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="Theme $" colKey="themeVol" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                    <SortTh label="Stage" colKey="stage" activeKey={themeSortKey} dir={themeSortDir} onSort={onThemeSort} align="right" />
                  </tr>
                </thead>
                <tbody>
                  {sortedThemeRows.map((row, idx) => {
                    const rs = row.relativeStrength1M ?? row.relativeStrengthQualifierRatio;
                    const rsNum = typeof rs === "number" && Number.isFinite(rs) ? rs : null;
                    const stage2 =
                      row.accumulation && row.highLiquidity && rsNum != null && rsNum > 0;
                    const n = sortedThemeRows.length;
                    const rankDisplay = themeSortDir === "asc" ? n - idx : idx + 1;
                    return (
                      <tr
                        key={themeRowKey(row)}
                        className={`relative z-0 ${idx % 2 === 1 ? "bg-terminal-bg/35" : ""} hover:bg-terminal-bg/55 [&>td]:border-b [&>td]:border-terminal-border/80`}
                      >
                        <td className="px-3 py-2.5 font-mono text-slate-400">{rankDisplay}</td>
                        <td
                          className="min-w-0 max-w-0 truncate py-2.5 pl-3 pr-1 font-medium text-slate-100"
                          title={row.theme}
                        >
                          <button
                            type="button"
                            className="w-full cursor-pointer truncate text-left hover:text-white"
                            onClick={() => {
                              setSpotlightThemeName(row.theme);
                              if (leaderView === "themes") {
                                void (async () => {
                                  const ok = await loadUniverseSpotlight(row.theme);
                                  if (!ok) {
                                    // Fallback to Scanner audit approximation when the universe
                                    // doesn't have this label (404) or has no tickers/movers yet.
                                    await ensureScannerLoaded();
                                  }
                                })();
                              } else {
                                // If we're in Finviz-only modes, lazily fetch Scanner audit so we can show movers.
                                void ensureScannerLoaded();
                              }
                            }}
                            aria-label={`Show ${row.theme} in Theme spotlight`}
                          >
                            {row.theme}
                          </button>
                        </td>
                        <td className="py-2.5 pl-1 pr-3 text-right">
                          <PerfCell v={row.perf1D} />
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <PerfCell v={row.perf1W} />
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <PerfCell v={row.perf1M} />
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <PerfCell v={row.perf3M} />
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <PerfCell v={row.perf6M} />
                        </td>
                        <td
                          className={`px-3 py-2.5 text-right font-mono text-xs tabular-nums ${
                            rsNum == null ? "text-slate-500" : rsNum >= 0 ? "text-emerald-400" : "text-rose-400"
                          }`}
                        >
                          {rsNum == null ? "—" : `${rsNum >= 0 ? "+" : ""}${rsNum.toFixed(1)}%`}
                        </td>
                        <td className="px-3 py-2.5 text-right font-mono tabular-nums text-slate-300">
                          {row.relativeStrengthQualifierRatio.toFixed(2)}
                        </td>
                        <td className="px-3 py-2.5 text-right font-mono text-slate-400">
                          {row.qualifiedCount}/{row.totalCount}
                        </td>
                        <td
                          className="min-w-0 truncate px-3 py-2.5 text-right font-mono tabular-nums text-slate-300"
                          title={formatMoney(row.themeDollarVolume)}
                        >
                          {formatMoney(row.themeDollarVolume)}
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          {stage2 ? (
                            <span className="inline-flex rounded-full bg-emerald-500/20 px-2 py-0.5 text-[10px] font-semibold text-emerald-300">
                              Stage 2
                            </span>
                          ) : (
                            <span className="text-slate-600">—</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
          </div>

          <div className="min-w-0">
            <h2 className="mb-3 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              Leader constituents{workspaceTab === "gappers" ? " · EP / gap focus" : ""}
            </h2>
            {filteredStockItems.length === 0 ? (
              <p className="rounded-lg border border-dashed border-terminal-border bg-terminal-card/50 py-10 text-center text-sm text-slate-500">
                {workspaceTab === "gappers"
                  ? "No EP / gappers in this scan — switch to Thematic Scanner."
                  : finvizLeaderboardOnly
                    ? "No constituents — Themes / Industry rows are Finviz-only (no ticker audit). Switch to Scanner for graded stock cards."
                    : "No constituents."}
              </p>
            ) : (
            <div className="grid w-full min-w-0 grid-cols-3 gap-3">
            {filteredStockItems.map(({ theme, stock, themeHot }) => {
              const epCandidate = isEpCandidate(stock);
              const epBurst = isEpBurst(stock);
              const buzzTitle =
                epBurst && stock.gap_open_pct != null && stock.or_rvol_ratio != null
                  ? `EP BURST: gap +${stock.gap_open_pct.toFixed(1)}% · OR-RVOL ${stock.or_rvol_ratio.toFixed(2)}×`
                  : epCandidate && stock.gap_open_pct != null && stock.or_rvol_ratio != null
                    ? `EP: gap +${stock.gap_open_pct.toFixed(1)}% · OR-RVOL ${stock.or_rvol_ratio.toFixed(2)}× · 1M ${stock.month_return_pct?.toFixed(1) ?? "—"}%`
                    : `Open ${stock.ticker} on TradingView`;
              return (
              <a
                key={`${theme}-${stock.ticker}`}
                href={`https://www.tradingview.com/chart/?symbol=${encodeURIComponent(stock.ticker)}`}
                target="_blank"
                rel="noopener noreferrer"
                title={buzzTitle}
                className={`group relative flex flex-col overflow-hidden rounded-lg border border-terminal-border bg-terminal-card p-3 transition duration-200 hover:cursor-pointer ${stockCardHoverClass(stock.gradeLabel)} ${epBurst ? "stock-card-ep-burst" : epCandidate ? "stock-card-ep-pulse" : ""} ${epBurst ? "pr-[4.25rem] pt-4" : ""}`}
              >
                {epBurst && (
                  <span
                    className="pointer-events-none absolute right-1 top-1 z-20 font-mono text-[6px] font-bold uppercase leading-none tracking-wide text-grade-aplus"
                    aria-hidden
                  >
                    ⚡ EP BURST
                  </span>
                )}
                <div className="flex min-h-[1.25rem] items-start justify-between gap-1">
                  <div className="flex min-w-0 flex-1 flex-wrap items-center gap-x-1 gap-y-0.5">
                    <span
                      className={`font-mono text-xs font-bold tracking-tight underline-offset-2 group-hover:underline ${
                        stock.gradeLabel === "A+"
                          ? "text-grade-aplus"
                          : stock.gradeLabel === "A"
                            ? "text-grade-a"
                            : "text-slate-200"
                      }`}
                    >
                      {stock.ticker}
                    </span>
                    {epCandidate && (
                      <span className="inline-flex shrink-0 items-center rounded border border-lime-400/60 bg-lime-500/15 px-0.5 py-px font-mono text-[6px] font-bold uppercase tracking-wide text-lime-100">
                        EP
                      </span>
                    )}
                  </div>
                  {stock.gradeLabel === "A+" ? (
                    <span className="badge-a-plus-neon shrink-0 rounded-sm border border-grade-aplus/70 bg-grade-aplus/15 px-1 py-0.5 font-mono text-[8px] font-bold uppercase tracking-wider text-grade-aplus">
                      A+
                    </span>
                  ) : stock.gradeLabel === "A" ? (
                    <span className="shrink-0 rounded-sm border border-grade-a/55 bg-grade-a/12 px-1 py-0.5 font-mono text-[8px] font-bold uppercase tracking-wider text-grade-a">
                      A
                    </span>
                  ) : (
                    <span className="shrink-0 font-mono text-[8px] font-medium uppercase tracking-wider text-slate-600">—</span>
                  )}
                </div>
                <p className="mt-0.5 truncate font-mono text-[8px] font-medium uppercase tracking-wide text-slate-500">
                  {theme}
                  {themeHot ? " · Hot" : ""}
                </p>
                <p className="line-clamp-1 font-sans text-[9px] leading-tight text-slate-500">{stock.name}</p>
                <LineChart
                  className="pointer-events-none absolute bottom-2 right-2 z-10 h-3.5 w-3.5 text-accent opacity-[0.35] transition-opacity duration-200 group-hover:opacity-100"
                  aria-hidden
                />
                <div className="mt-0.5">
                  {epBurst || epCandidate ? (
                    <span className="inline-flex max-w-full flex-wrap items-center gap-x-0.5 rounded border border-lime-500/40 bg-lime-500/10 px-0.5 py-px font-mono text-[7px] font-semibold uppercase tracking-wider text-lime-100">
                      <span>Buzz {stock.volume_buzz_pct.toFixed(0)}%</span>
                      {stock.gap_open_pct != null && (
                        <span className="text-lime-200/90">G+{stock.gap_open_pct.toFixed(1)}%</span>
                      )}
                      {stock.or_rvol_ratio != null && <span>OR×{stock.or_rvol_ratio.toFixed(2)}</span>}
                    </span>
                  ) : (
                    <span
                      className={`inline-flex items-center rounded border px-0.5 py-px font-mono text-[7px] font-semibold uppercase tracking-wider ${
                        stock.volume_buzz_pct > 0
                          ? "border-grade-a/35 bg-grade-a/10 text-grade-a"
                          : "border-terminal-border bg-terminal-bg text-slate-500"
                      }`}
                    >
                      Buzz {stock.volume_buzz_pct.toFixed(0)}%
                    </span>
                  )}
                </div>
                <dl className="mt-1 grid grid-cols-2 gap-x-1 gap-y-0.5 leading-tight text-slate-500">
                  <div>
                    <dt className="font-mono text-[7px] uppercase tracking-wide text-slate-600">ADR</dt>
                    <dd
                      className={`font-mono text-[9px] ${stock.adr_pct >= 4.5 ? "text-grade-aplus/90" : "text-slate-300"}`}
                    >
                      {stock.adr_pct.toFixed(2)}%
                    </dd>
                  </div>
                  <div>
                    <dt className="font-mono text-[7px] uppercase tracking-wide text-slate-600">ADDV</dt>
                    <dd className="font-mono text-[9px] text-slate-300">{formatMoney(stock.avg_dollar_volume)}</dd>
                  </div>
                  <div className="col-span-2">
                    <dt className="font-mono text-[7px] uppercase tracking-wide text-slate-600">Mkt cap</dt>
                    <dd className="font-mono text-[9px] text-slate-300">{formatMoney(stock.market_cap)}</dd>
                  </div>
                </dl>
              </a>
            );
            })}
            </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
