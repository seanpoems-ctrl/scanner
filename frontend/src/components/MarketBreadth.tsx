import { memo, useCallback, useEffect, useRef, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity, RefreshCw, Rocket, TrendingUp, Waves } from "lucide-react";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim() || "http://127.0.0.1:8000";

// Mirrors SPEEDBOAT_BLAST_OFF_THRESHOLD in breadth.py.
// The backend also returns blast_off_threshold so we can use that when available.
const BLAST_OFF_THRESHOLD_FALLBACK = 125;

// ── Types ─────────────────────────────────────────────────────────────────────

type HistoryPoint = { date: string; value: number };

type OceanPayload = {
  s5fi: number | null;
  speedboat_count: number | null;
  is_blast_off: boolean;
  blast_off_threshold?: number;
  s5fi_history: HistoryPoint[];
  speedboat_history: HistoryPoint[];
  universe_size: number;
  fetched_at_utc?: string;
  error?: string;
};

// ── Regime helpers ────────────────────────────────────────────────────────────

type RegimeInfo = {
  label: string;
  directive: string;
  color: string;
  bgClass: string;
  borderClass: string;
  isBlastOff: boolean;
};

/**
 * Blast-off overrides the S5FI regime when the elite speedboat count crosses
 * the institutional-thrust threshold (>= 125).
 */
function getOceanRegime(s5fi: number | null, isBlastOff: boolean): RegimeInfo {
  if (isBlastOff) {
    return {
      label: "BLAST-OFF",
      directive:
        "Elite institutional thrust detected. Heavy accumulation in high-liquidity leadership. Maximize exposure in A+ setups.",
      color: "#2EE59D",
      bgClass: "bg-[#2EE59D]/10",
      borderClass: "border-[#2EE59D]/50",
      isBlastOff: true,
    };
  }
  if (s5fi === null) {
    return {
      label: "Loading…",
      directive: "Awaiting breadth data.",
      color: "#64748b",
      bgClass: "bg-slate-700/20",
      borderClass: "border-slate-600/40",
      isBlastOff: false,
    };
  }
  if (s5fi > 50) {
    return {
      label: "Ocean is Deep",
      directive: "Long breakouts have high probability. Favor A+ EP setups with size.",
      color: "#2EE59D",
      bgClass: "bg-[#2EE59D]/10",
      borderClass: "border-[#2EE59D]/40",
      isBlastOff: false,
    };
  }
  if (s5fi >= 20) {
    return {
      label: "Ocean is Thinning",
      directive: "Reduce position size. Trade only A+ setups. Avoid new sector longs.",
      color: "#B3FF00",
      bgClass: "bg-[#B3FF00]/10",
      borderClass: "border-[#B3FF00]/40",
      isBlastOff: false,
    };
  }
  return {
    label: "Ocean is Dry",
    directive: "Stop long trading. Cash is a position. Watch for a breadth thrust reversal.",
    color: "#f87171",
    bgClass: "bg-red-500/10",
    borderClass: "border-red-500/40",
    isBlastOff: false,
  };
}

/** S5FI color by value — used for bar chart cells. */
function s5fiColor(value: number): string {
  if (value > 50) return "#2EE59D";
  if (value >= 20) return "#B3FF00";
  return "#f87171";
}

function fmtDate(d: string): string {
  try {
    const dt = new Date(d + "T00:00:00");
    return dt.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  } catch {
    return d.slice(5);
  }
}

// ── Sub-components ────────────────────────────────────────────────────────────

function S5FiGauge({ value }: { value: number | null }) {
  const pct = value ?? 0;
  const r = 70;
  const cx = 90;
  const cy = 90;
  const startAngle = Math.PI;
  const endAngle = 0;
  const totalArc = startAngle - endAngle;
  const fillAngle = startAngle - (pct / 100) * totalArc;

  function polarToCart(angle: number) {
    return { x: cx + r * Math.cos(angle), y: cy - r * Math.sin(angle) };
  }

  const arcStart = polarToCart(startAngle);
  const arcEnd = polarToCart(endAngle);
  const needleEnd = polarToCart(fillAngle);
  const arcPath = `M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 0 1 ${arcEnd.x} ${arcEnd.y}`;
  const fillPath = `M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 0 1 ${needleEnd.x} ${needleEnd.y}`;
  const color = s5fiColor(pct);

  return (
    <div className="flex flex-col items-center">
      <svg viewBox="0 0 180 100" className="w-full max-w-[200px]" aria-label={`S5FI gauge: ${pct}%`}>
        <path d={arcPath} fill="none" stroke="#1e293b" strokeWidth={12} strokeLinecap="round" />
        {(() => {
          const band20End = polarToCart(startAngle - 0.2 * totalArc);
          const band50End = polarToCart(startAngle - 0.5 * totalArc);
          return (
            <>
              <path d={`M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 0 1 ${band20End.x} ${band20End.y}`}
                fill="none" stroke="#f87171" strokeWidth={12} strokeLinecap="butt" opacity={0.3} />
              <path d={`M ${band20End.x} ${band20End.y} A ${r} ${r} 0 0 1 ${band50End.x} ${band50End.y}`}
                fill="none" stroke="#B3FF00" strokeWidth={12} strokeLinecap="butt" opacity={0.3} />
              <path d={`M ${band50End.x} ${band50End.y} A ${r} ${r} 0 0 1 ${arcEnd.x} ${arcEnd.y}`}
                fill="none" stroke="#2EE59D" strokeWidth={12} strokeLinecap="butt" opacity={0.3} />
            </>
          );
        })()}
        {value !== null && (
          <path d={fillPath} fill="none" stroke={color} strokeWidth={12} strokeLinecap="round" />
        )}
        {value !== null && (
          <>
            <line x1={cx} y1={cy} x2={needleEnd.x} y2={needleEnd.y}
              stroke={color} strokeWidth={2.5} strokeLinecap="round" />
            <circle cx={cx} cy={cy} r={5} fill={color} />
          </>
        )}
        <text x={cx} y={cy + 32} textAnchor="middle" fill="white" fontSize={22} fontWeight={700} fontFamily="monospace">
          {value !== null ? `${pct.toFixed(1)}%` : "—"}
        </text>
        <text x={arcStart.x - 4} y={arcStart.y + 14} textAnchor="end" fill="#64748b" fontSize={8}>0%</text>
        <text x={arcEnd.x + 4} y={arcEnd.y + 14} textAnchor="start" fill="#64748b" fontSize={8}>100%</text>
        <text x={cx} y={cy - r - 6} textAnchor="middle" fill="#64748b" fontSize={8}>50%</text>
      </svg>
      <p className="mt-1 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
        S&P 500 Stocks Above 50 SMA
      </p>
    </div>
  );
}

type TooltipProps = { active?: boolean; payload?: { value: number }[]; label?: string };

function SpeedboatChartTooltip({ active, payload, label, threshold }: TooltipProps & { threshold: number }) {
  if (!active || !payload?.length) return null;
  const v = payload[0].value;
  const isBlast = v >= threshold;
  return (
    <div className="rounded-lg border border-terminal-border bg-terminal-card px-3 py-2 text-[11px] shadow-xl">
      <p className="font-mono text-slate-400">{label}</p>
      <p className="font-mono font-bold" style={{ color: isBlast ? "#2EE59D" : "#94a3b8" }}>
        {v} stocks{isBlast ? " 🚀" : ""}
      </p>
      {isBlast && <p className="text-[#2EE59D]">Blast-off threshold ({threshold}) reached</p>}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

const MarketBreadth = memo(function MarketBreadth() {
  const [data, setData] = useState<OceanPayload | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastFetchedAt, setLastFetchedAt] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    if (loading) return;
    setLoading(true);
    setError(null);
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    try {
      const r = await fetch(`${API_BASE_URL}/api/market-ocean`, { signal: ctrl.signal });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const json = (await r.json()) as OceanPayload;
      setData(json);
      setLastFetchedAt(json.fetched_at_utc ?? null);
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return;
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [loading]);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 20 * 60_000);
    return () => {
      window.clearInterval(id);
      abortRef.current?.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Use server-sent threshold when available so UI is always in sync with backend constant.
  const blastOffThreshold = data?.blast_off_threshold ?? BLAST_OFF_THRESHOLD_FALLBACK;
  const isBlastOff = data?.is_blast_off ?? false;
  const regime = getOceanRegime(data?.s5fi ?? null, isBlastOff);

  const s5fiHistory = (data?.s5fi_history ?? []).map((p) => ({ date: fmtDate(p.date), value: p.value }));
  const speedboatHistory = (data?.speedboat_history ?? []).map((p) => ({ date: fmtDate(p.date), value: p.value }));

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-4">
      {/* ── Header ── */}
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-accent/15 text-accent">
            <Waves className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <h2 className="text-sm font-semibold text-white">Market Ocean</h2>
            <p className="text-[11px] text-slate-500">Breath regime · S5FI proxy · Elite Speedboats</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {lastFetchedAt && (
            <span className="hidden text-[10px] text-slate-600 sm:block">
              Updated{" "}
              <span className="font-mono">
                {new Date(lastFetchedAt).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}
              </span>
            </span>
          )}
          <button
            type="button"
            onClick={() => void load()}
            disabled={loading}
            title="Refresh breadth data"
            className="flex items-center gap-1.5 rounded-lg border border-terminal-border bg-terminal-bg px-3 py-1.5 text-[11px] font-semibold text-slate-300 transition-colors hover:border-slate-500 hover:text-white disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} aria-hidden />
            {loading ? "Loading…" : "Refresh"}
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-[12px] text-rose-300">
          Failed to load breadth data: {error}
        </div>
      )}

      {/* ── Daily Directive card ── */}
      <div
        className={`rounded-xl border ${regime.borderClass} ${regime.bgClass} px-5 py-4 ${
          regime.isBlastOff ? "shadow-[0_0_24px_rgba(46,229,157,0.18)]" : ""
        }`}
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              Daily Directive
            </p>
            <p
              className={`mt-1 text-base font-bold ${regime.isBlastOff ? "animate-pulse" : ""}`}
              style={{ color: regime.color }}
            >
              {regime.isBlastOff ? "🚀 BLAST-OFF" : regime.label}
            </p>
            <p className="mt-1 max-w-xl text-[12px] leading-relaxed text-slate-300">
              {regime.directive}
            </p>
          </div>
          {/* Live speedboat count badge */}
          {data?.speedboat_count != null && (
            <div
              className={`flex flex-col items-end gap-1 rounded-lg border px-3 py-2 ${
                isBlastOff
                  ? "border-[#2EE59D]/40 bg-[#2EE59D]/10"
                  : "border-terminal-border bg-terminal-bg"
              }`}
              title={`Elite Speedboats: price >$12, 30d avg daily $vol >$100M, change >+4%, ADR% >4%, mkt cap >$2B. Blast-off at ≥${blastOffThreshold}.`}
            >
              <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                Elite Speedboats
              </span>
              <span
                className="font-mono text-2xl font-bold tabular-nums"
                style={{ color: isBlastOff ? "#2EE59D" : "#94a3b8" }}
              >
                {data.speedboat_count}
              </span>
              <span className="text-[10px] text-slate-600">
                {isBlastOff ? (
                  <span className="flex items-center gap-1 text-[#2EE59D]">
                    <Rocket className="h-3 w-3" aria-hidden /> Thrust active
                  </span>
                ) : (
                  `threshold: ${blastOffThreshold}`
                )}
              </span>
            </div>
          )}
        </div>
      </div>

      {/* ── Metrics row ── */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {/* S5FI Gauge */}
        <div className="rounded-xl border border-terminal-border bg-terminal-card p-4">
          <div className="mb-3 flex items-center gap-2">
            <Activity className="h-4 w-4 text-accent" aria-hidden />
            <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
              S5FI — Ocean Depth
            </span>
          </div>
          <S5FiGauge value={data?.s5fi ?? null} />
          <div className="mt-3 flex justify-center gap-4 text-[10px] text-slate-500">
            <span className="flex items-center gap-1">
              <span className="inline-block h-2 w-2 rounded-full bg-red-400" />&lt;20% Dry
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2 w-2 rounded-full bg-[#B3FF00]" />20–50% Thin
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2 w-2 rounded-full bg-[#2EE59D]" />&gt;50% Deep
            </span>
          </div>
        </div>

        {/* Speedboat Count card */}
        <div className="rounded-xl border border-terminal-border bg-terminal-card p-4">
          <div className="mb-1 flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-accent" aria-hidden />
              <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                Elite Speedboats (Up &gt;4%)
              </span>
            </div>
            {data?.speedboat_count != null && (
              <span
                className="font-mono text-xl font-bold tabular-nums"
                style={{ color: isBlastOff ? "#2EE59D" : "#94a3b8" }}
              >
                {data.speedboat_count}
              </span>
            )}
          </div>
          {/* Criteria tooltip row */}
          <p
            className="mb-2 text-[10px] text-slate-600"
            title="All five gates must be met simultaneously for a stock to count."
          >
            Price &gt;$12 · 30d Avg $Vol &gt;$100M · Change &gt;+4% · ADR% (20d) &gt;4% · Mkt Cap &gt;$2B
          </p>
          {/* Blast-off inline banner */}
          {isBlastOff && (
            <div className="mb-3 flex items-center gap-2 rounded-lg border border-[#2EE59D]/35 bg-[#2EE59D]/10 px-3 py-2 text-[11px] font-semibold text-[#2EE59D] shadow-[0_0_12px_rgba(46,229,157,0.15)]">
              <Rocket className="h-3.5 w-3.5 shrink-0" aria-hidden />
              Blast-off signal ({data?.speedboat_count} ≥ {blastOffThreshold}) — broad momentum thrust. Maximize A+ exposure.
            </div>
          )}
          {/* 10-day bar chart */}
          {speedboatHistory.length > 0 ? (
            <ResponsiveContainer width="100%" height={120}>
              <BarChart data={speedboatHistory} margin={{ top: 4, right: 4, bottom: 0, left: -24 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} />
                <Tooltip
                  content={<SpeedboatChartTooltip threshold={blastOffThreshold} />}
                  cursor={{ fill: "rgba(255,255,255,0.04)" }}
                />
                <Bar dataKey="value" radius={[3, 3, 0, 0]}>
                  {speedboatHistory.map((entry) => {
                    const blast = entry.value >= blastOffThreshold;
                    return (
                      <Cell
                        key={entry.date}
                        fill={blast ? "#2EE59D" : "#475569"}
                        opacity={0.9}
                        // SVG filter for glow on blast-off bars applied via style
                        style={blast ? { filter: "drop-shadow(0 0 4px rgba(46,229,157,0.6))" } : undefined}
                      />
                    );
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-[120px] items-center justify-center text-[11px] text-slate-600">
              {loading ? "Computing elite filter…" : "No history yet"}
            </div>
          )}
          <p className="mt-2 text-[10px] text-slate-600">
            Blast-off threshold: ≥{blastOffThreshold} stocks · bars glow on thrust days
          </p>
        </div>
      </div>

      {/* ── S5FI 10-day history bar chart ── */}
      <div className="rounded-xl border border-terminal-border bg-terminal-card p-4">
        <div className="mb-3 flex items-center gap-2">
          <Activity className="h-4 w-4 text-accent" aria-hidden />
          <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
            S5FI — 10-Day Trend
          </span>
          <span className="ml-auto text-[10px] text-slate-600">% of S&P 500 proxy above 50 SMA</span>
        </div>
        {s5fiHistory.length > 0 ? (
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={s5fiHistory} margin={{ top: 4, right: 8, bottom: 0, left: -16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} />
              <YAxis domain={[0, 100]} tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} unit="%" />
              <Tooltip
                content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const val = payload[0].value as number;
                  const r = getOceanRegime(val, false);
                  return (
                    <div className="rounded-lg border border-terminal-border bg-terminal-card px-3 py-2 text-[11px] shadow-xl">
                      <p className="font-mono text-slate-400">{label as string}</p>
                      <p className="font-mono font-bold" style={{ color: r.color }}>{val.toFixed(1)}%</p>
                      <p className="text-slate-500">{r.label}</p>
                    </div>
                  );
                }}
                cursor={{ fill: "rgba(255,255,255,0.04)" }}
              />
              <Bar dataKey="value" radius={[3, 3, 0, 0]}>
                {s5fiHistory.map((entry) => (
                  <Cell key={entry.date} fill={s5fiColor(entry.value)} opacity={0.85} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[160px] items-center justify-center text-[11px] text-slate-600">
            {loading ? "Computing S5FI history…" : "No history data available"}
          </div>
        )}
        <div className="mt-3 flex flex-wrap gap-4 text-[10px] text-slate-500">
          <span>50% = Bull/Bear threshold</span>
          <span>20% = Breadth thrust watch zone</span>
          {data?.universe_size ? <span className="ml-auto">Universe: {data.universe_size} tickers</span> : null}
        </div>
      </div>
    </div>
  );
});

export default MarketBreadth;
