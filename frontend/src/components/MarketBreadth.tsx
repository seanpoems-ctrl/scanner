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
import { Activity, RefreshCw, TrendingUp, Waves, Zap } from "lucide-react";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim() || "http://127.0.0.1:8000";

// ── Types ─────────────────────────────────────────────────────────────────────

type HistoryPoint = { date: string; value: number };

type OceanPayload = {
  s5fi: number | null;
  speedboat_count: number | null;
  s5fi_history: HistoryPoint[];
  speedboat_history: HistoryPoint[];
  universe_size: number;
  fetched_at_utc?: string;
  error?: string;
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function getOceanRegime(s5fi: number | null): {
  label: string;
  directive: string;
  color: string;
  bgClass: string;
  borderClass: string;
} {
  if (s5fi === null) {
    return {
      label: "Loading…",
      directive: "Awaiting breadth data.",
      color: "#64748b",
      bgClass: "bg-slate-700/20",
      borderClass: "border-slate-600/40",
    };
  }
  if (s5fi > 50) {
    return {
      label: "Ocean is Deep",
      directive: "Long breakouts have high probability. Favor A+ EP setups with size.",
      color: "#2EE59D",
      bgClass: "bg-[#2EE59D]/10",
      borderClass: "border-[#2EE59D]/40",
    };
  }
  if (s5fi >= 20) {
    return {
      label: "Ocean is Thinning",
      directive: "Reduce position size. Trade only A+ setups. Avoid new sector longs.",
      color: "#B3FF00",
      bgClass: "bg-[#B3FF00]/10",
      borderClass: "border-[#B3FF00]/40",
    };
  }
  return {
    label: "Ocean is Dry",
    directive: "Stop long trading. Cash is a position. Watch for a breadth thrust reversal.",
    color: "#f87171",
    bgClass: "bg-red-500/10",
    borderClass: "border-red-500/40",
  };
}

function fmtDate(d: string): string {
  // "2025-03-28" -> "Mar 28"
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
  // Semicircle gauge: SVG arc from 180deg to 0deg (left to right).
  const r = 70;
  const cx = 90;
  const cy = 90;
  const startAngle = Math.PI;
  const endAngle = 0;
  const totalArc = startAngle - endAngle; // PI radians
  const fillAngle = startAngle - (pct / 100) * totalArc;

  function polarToCart(angle: number) {
    return {
      x: cx + r * Math.cos(angle),
      y: cy - r * Math.sin(angle),
    };
  }

  const arcStart = polarToCart(startAngle);
  const arcEnd = polarToCart(endAngle);
  const needleEnd = polarToCart(fillAngle);

  const arcPath = `M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 0 1 ${arcEnd.x} ${arcEnd.y}`;
  const fillPath = `M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 ${pct > 50 ? 0 : 0} 1 ${needleEnd.x} ${needleEnd.y}`;

  const color = pct > 50 ? "#2EE59D" : pct >= 20 ? "#B3FF00" : "#f87171";

  return (
    <div className="flex flex-col items-center">
      <svg viewBox="0 0 180 100" className="w-full max-w-[200px]" aria-label={`S5FI gauge: ${pct}%`}>
        {/* Track */}
        <path d={arcPath} fill="none" stroke="#1e293b" strokeWidth={12} strokeLinecap="round" />
        {/* Zone bands */}
        {(() => {
          const band20End = polarToCart(startAngle - 0.2 * totalArc);
          const band50End = polarToCart(startAngle - 0.5 * totalArc);
          return (
            <>
              <path
                d={`M ${arcStart.x} ${arcStart.y} A ${r} ${r} 0 0 1 ${band20End.x} ${band20End.y}`}
                fill="none" stroke="#f87171" strokeWidth={12} strokeLinecap="butt" opacity={0.3}
              />
              <path
                d={`M ${band20End.x} ${band20End.y} A ${r} ${r} 0 0 1 ${band50End.x} ${band50End.y}`}
                fill="none" stroke="#B3FF00" strokeWidth={12} strokeLinecap="butt" opacity={0.3}
              />
              <path
                d={`M ${band50End.x} ${band50End.y} A ${r} ${r} 0 0 1 ${arcEnd.x} ${arcEnd.y}`}
                fill="none" stroke="#2EE59D" strokeWidth={12} strokeLinecap="butt" opacity={0.3}
              />
            </>
          );
        })()}
        {/* Fill arc */}
        {value !== null && (
          <path d={fillPath} fill="none" stroke={color} strokeWidth={12} strokeLinecap="round" />
        )}
        {/* Needle */}
        {value !== null && (
          <>
            <line
              x1={cx}
              y1={cy}
              x2={needleEnd.x}
              y2={needleEnd.y}
              stroke={color}
              strokeWidth={2.5}
              strokeLinecap="round"
            />
            <circle cx={cx} cy={cy} r={5} fill={color} />
          </>
        )}
        {/* Label */}
        <text x={cx} y={cy + 18} textAnchor="middle" fill="white" fontSize={22} fontWeight={700} fontFamily="monospace">
          {value !== null ? `${pct.toFixed(1)}%` : "—"}
        </text>
        {/* Zone labels */}
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

type ChartTooltipProps = { active?: boolean; payload?: { value: number }[]; label?: string };

function OceanChartTooltip({ active, payload, label }: ChartTooltipProps) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-terminal-border bg-terminal-card px-3 py-2 text-[11px] shadow-xl">
      <p className="font-mono text-slate-400">{label}</p>
      <p className="font-mono font-semibold text-white">{payload[0].value}</p>
    </div>
  );
}

function SpeedboatBadge({ count }: { count: number | null }) {
  const isBlastoff = (count ?? 0) > 300;
  if (isBlastoff) {
    return (
      <span className="inline-flex animate-pulse items-center gap-1 rounded-full border border-[#B3FF00]/50 bg-[#B3FF00]/15 px-2.5 py-1 font-mono text-[11px] font-bold text-[#B3FF00]">
        <Zap className="h-3 w-3" aria-hidden />
        Speedboat Blast-off!
      </span>
    );
  }
  return null;
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

  // Load on mount; auto-refresh every 20 min
  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 20 * 60_000);
    return () => {
      window.clearInterval(id);
      abortRef.current?.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const regime = getOceanRegime(data?.s5fi ?? null);

  const s5fiHistory = (data?.s5fi_history ?? []).map((p) => ({
    date: fmtDate(p.date),
    value: p.value,
  }));
  const speedboatHistory = (data?.speedboat_history ?? []).map((p) => ({
    date: fmtDate(p.date),
    value: p.value,
  }));

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-4">
      {/* Header row */}
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

      {/* Daily Directive card */}
      <div className={`rounded-xl border ${regime.borderClass} ${regime.bgClass} px-5 py-4`}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">
              Daily Directive
            </p>
            <p className="mt-1 text-base font-bold" style={{ color: regime.color }}>
              {regime.label}
            </p>
            <p className="mt-1 max-w-xl text-[12px] leading-relaxed text-slate-300">
              {regime.directive}
            </p>
          </div>
          {data?.speedboat_count != null && (
            <SpeedboatBadge count={data.speedboat_count} />
          )}
        </div>
      </div>

      {/* Metrics row */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {/* S5FI Gauge card */}
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
              <span className="inline-block h-2 w-2 rounded-full bg-red-400" />
              &lt;20% Dry
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2 w-2 rounded-full bg-[#B3FF00]" />
              20–50% Thin
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-2 w-2 rounded-full bg-[#2EE59D]" />
              &gt;50% Deep
            </span>
          </div>
        </div>

        {/* Speedboat Count card */}
        <div className="rounded-xl border border-terminal-border bg-terminal-card p-4">
          <div className="mb-2 flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-accent" aria-hidden />
              <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                Elite Speedboats (Up &gt;4%)
              </span>
            </div>
            {data?.speedboat_count != null && (
              <span
                className="font-mono text-xl font-bold tabular-nums"
                style={{ color: (data.speedboat_count ?? 0) > 300 ? "#B3FF00" : "#2EE59D" }}
              >
                {data.speedboat_count}
              </span>
            )}
          </div>
          <p className="mb-3 text-[10px] text-slate-600">
            Price &gt;$12 · Avg $Vol &gt;$100M · Change &gt;4% · ADR% &gt;4% · Mkt Cap &gt;$2B
          </p>
          {data?.speedboat_count != null && data.speedboat_count > 300 && (
            <div className="mb-3 rounded-lg border border-[#B3FF00]/30 bg-[#B3FF00]/10 px-3 py-2 text-[11px] font-semibold text-[#B3FF00]">
              Blast-off signal — broad momentum thrust. High-probability breakout window.
            </div>
          )}
          {speedboatHistory.length > 0 ? (
            <ResponsiveContainer width="100%" height={120}>
              <BarChart data={speedboatHistory} margin={{ top: 4, right: 4, bottom: 0, left: -24 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: "#64748b", fontSize: 9 }} axisLine={false} tickLine={false} />
                <Tooltip content={<OceanChartTooltip />} cursor={{ fill: "rgba(255,255,255,0.04)" }} />
                <Bar dataKey="value" radius={[3, 3, 0, 0]}>
                  {speedboatHistory.map((entry) => (
                    <Cell
                      key={entry.date}
                      fill={entry.value > 300 ? "#B3FF00" : "#2EE59D"}
                      opacity={0.85}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-[120px] items-center justify-center text-[11px] text-slate-600">
              {loading ? "Computing…" : "No history yet"}
            </div>
          )}
        </div>
      </div>

      {/* S5FI 10-day history bar chart */}
      <div className="rounded-xl border border-terminal-border bg-terminal-card p-4">
        <div className="mb-3 flex items-center gap-2">
          <Activity className="h-4 w-4 text-accent" aria-hidden />
          <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
            S5FI — 10-Day Trend
          </span>
          <span className="ml-auto text-[10px] text-slate-600">
            % of S&P 500 proxy above 50 SMA
          </span>
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
                  const r = getOceanRegime(val);
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
                  <Cell
                    key={entry.date}
                    fill={getOceanRegime(entry.value).color}
                    opacity={0.85}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[160px] items-center justify-center text-[11px] text-slate-600">
            {loading ? "Computing S5FI history…" : "No history data available"}
          </div>
        )}
        {/* Threshold reference lines legend */}
        <div className="mt-3 flex flex-wrap gap-4 text-[10px] text-slate-500">
          <span>50% line = Bull/Bear breadth threshold</span>
          <span>20% line = Breadth thrust watch zone</span>
          {data?.universe_size ? (
            <span className="ml-auto">Universe: {data.universe_size} tickers</span>
          ) : null}
        </div>
      </div>
    </div>
  );
});

export default MarketBreadth;
