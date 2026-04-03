import { memo, useMemo } from "react";
import { BarChart3, ExternalLink } from "lucide-react";
import { ImpactBadge } from "./ui/ImpactBadge";
import { PanelLoading } from "./ui/SkeletonRows";

export type StockbeeRow = {
  date: string;
  date_display: string;
  up_4_pct: number | null;
  down_4_pct: number | null;
  ratio_5d: number | null;
  ratio_10d: number | null;
  up_25_q: number | null;
  down_25_q: number | null;
  up_25_m: number | null;
  down_25_m: number | null;
  up_50_m: number | null;
  down_50_m: number | null;
  up_13_34d: number | null;
  down_13_34d: number | null;
  worden_universe: number | null;
  t2108: number | null;
  sp_index: number | null;
};

export type LeadingThemeRow = { theme: string; perf1D: number; sector?: string | null };

export type MarketBreadthPayload = {
  ok: boolean;
  rows: StockbeeRow[];
  sheet_year?: number;
  source_url?: string;
  blog_url?: string;
  detail?: string | null;
  fetched_at_utc?: string;
  leading_themes?: LeadingThemeRow[];
  leading_themes_note?: string;
};

function fmtN(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return Number.isInteger(n) ? String(n) : n.toFixed(2);
}

function t2108Label(v: number | null): { text: string; badge: string } {
  if (v == null || !Number.isFinite(v)) return { text: "—", badge: "Neutral" };
  if (v < 20) return { text: "Oversold zone (<20)", badge: "Oversold" };
  if (v > 70) return { text: "Overbought zone (>70)", badge: "Overbought" };
  return { text: "Neutral band (20–70)", badge: "Neutral" };
}

const MarketBreadthReport = memo(function MarketBreadthReport({ data }: { data: MarketBreadthPayload | null }) {
  const latest = data?.rows?.[0] ?? null;

  const analysis = useMemo(() => {
    if (!latest) return null;
    const up4 = latest.up_4_pct ?? 0;
    const dn4 = latest.down_4_pct ?? 0;
    const upq = latest.up_25_q;
    const dnq = latest.down_25_q;

    const thrust = dn4 <= 0 ? up4 > 0 : up4 > 3 * dn4;
    const shortTerm = thrust
      ? { label: "Extremely Bullish / Thrust", badge: "Extreme Bullish" as const }
      : up4 > dn4
      ? { label: "Bulls leading +4% tape", badge: "Bullish" as const }
      : { label: "Bears competitive on +4% tape", badge: "Caution" as const };

    const quarterly =
      upq != null && dnq != null && dnq > upq
        ? { label: "Quarterly breadth skewed negative (down > up)", badge: "Bearish" as const }
        : { label: "Quarterly breadth not dominated by losers", badge: "Neutral" as const };

    const t = t2108Label(latest.t2108);

    const verdict =
      latest.t2108 != null &&
      latest.t2108 < 25 &&
      latest.up_4_pct != null &&
      latest.up_4_pct > 500
        ? "The Oversold Bounce — Strategy: Aggressive longs in high-volume leaders."
        : null;

    return { shortTerm, quarterly, t2108: t, verdict };
  }, [latest]);

  if (!data) {
    return <PanelLoading label="Loading Stockbee monitor…" />;
  }

  if (!data.ok && !data.rows?.length) {
    return (
      <div className="rounded-xl border border-rose-900/40 bg-rose-950/20 px-4 py-4 text-sm text-rose-200">
        <p className="font-semibold">Stockbee Market Monitor unavailable</p>
        <p className="mt-1 text-xs text-rose-300/80">{data.detail ?? "Unknown error"}</p>
        {data.blog_url ? (
          <a
            href={data.blog_url}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-2 inline-flex items-center gap-1 text-xs text-accent hover:underline"
          >
            Open source page <ExternalLink className="h-3 w-3" />
          </a>
        ) : null}
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-gray-800 bg-terminal-bg">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-gray-800 px-4 py-3">
        <div className="flex items-center gap-2">
          <BarChart3 className="h-4 w-4 text-accent" aria-hidden />
          <div>
            <h3 className="t-page">Stockbee Market Monitor</h3>
            <p className="t-micro text-slate-500">
              Sheet year {data.sheet_year ?? "—"} ·{" "}
              {data.fetched_at_utc
                ? `Updated ${new Date(data.fetched_at_utc).toLocaleString()}`
                : "—"}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {data.blog_url ? (
            <a
              href={data.blog_url}
              target="_blank"
              rel="noopener noreferrer"
              className="t-micro text-slate-500 hover:text-accent"
            >
              stockbee MM ↗
            </a>
          ) : null}
        </div>
      </div>

      <div className="flex flex-col gap-4 p-4 lg:flex-row">
        {/* LEFT: table ~65% */}
        <div className="min-w-0 flex-[1_1_65%] overflow-x-auto">
          <table className="w-full min-w-[720px] border-collapse text-left">
            <caption className="sr-only">Stockbee market monitor historical rows by date.</caption>
            <thead>
              <tr className="border-b border-gray-800">
                <th scope="col" className="sticky left-0 z-10 bg-terminal-bg py-2 pr-2 t-label">Date</th>
                <th scope="col" className="px-1 py-2 t-label">Up 4%+</th>
                <th scope="col" className="px-1 py-2 t-label">Dn 4%+</th>
                <th scope="col" className="px-1 py-2 t-label">5d R</th>
                <th scope="col" className="px-1 py-2 t-label">10d R</th>
                <th scope="col" className="px-1 py-2 t-label">Up 25% Q</th>
                <th scope="col" className="px-1 py-2 t-label">Dn 25% Q</th>
                <th scope="col" className="px-1 py-2 t-label">Up 25% M</th>
                <th scope="col" className="px-1 py-2 t-label">Dn 25% M</th>
                <th scope="col" className="px-1 py-2 t-label">Up 50% M</th>
                <th scope="col" className="px-1 py-2 t-label">Dn 50% M</th>
                <th scope="col" className="px-1 py-2 t-label">Up 13% 34d</th>
                <th scope="col" className="px-1 py-2 t-label">Dn 13% 34d</th>
                <th scope="col" className="px-1 py-2 t-label">Univ</th>
                <th scope="col" className="px-1 py-2 t-label">T2108</th>
                <th scope="col" className="px-1 py-2 t-label">S&amp;P</th>
              </tr>
            </thead>
            <tbody>
              {(data.rows ?? []).map((r) => {
                const bearishRow =
                  r.up_25_q != null && r.down_25_q != null && r.up_25_q < r.down_25_q;
                const up4hi = r.up_4_pct != null && r.up_4_pct > 600;
                const t2108Low = r.t2108 != null && r.t2108 < 20;
                return (
                  <tr
                    key={r.date}
                    className={`border-b border-gray-800/80 t-mono ${
                      bearishRow ? "text-rose-200/70" : "text-slate-300"
                    }`}
                  >
                    <td className="sticky left-0 z-10 bg-terminal-bg py-1.5 pr-2 text-slate-200">{r.date_display}</td>
                    <td
                      className={`px-1 py-1.5 ${
                        up4hi
                          ? "font-bold text-emerald-300 shadow-[0_0_12px_rgba(52,211,153,0.35)]"
                          : ""
                      }`}
                    >
                      {fmtN(r.up_4_pct)}
                    </td>
                    <td className="px-1 py-1.5">{fmtN(r.down_4_pct)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.ratio_5d)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.ratio_10d)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.up_25_q)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.down_25_q)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.up_25_m)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.down_25_m)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.up_50_m)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.down_50_m)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.up_13_34d)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.down_13_34d)}</td>
                    <td className="px-1 py-1.5">{fmtN(r.worden_universe)}</td>
                    <td
                      className={`px-1 py-1.5 ${
                        t2108Low ? "rounded bg-orange-950/50 font-bold text-orange-300" : ""
                      }`}
                    >
                      {r.t2108 != null ? r.t2108.toFixed(2) : "—"}
                      {t2108Low ? <span className="ml-1 text-[10px] text-orange-400">OS</span> : null}
                    </td>
                    <td className="px-1 py-1.5">{r.sp_index != null ? r.sp_index.toLocaleString() : "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* RIGHT: brief ~35% */}
        <div className="flex w-full min-w-[260px] flex-[1_1_35%] flex-col gap-3">
          <p className="t-label">Daily brief</p>

          {analysis ? (
            <>
              <div className="rounded-lg border border-gray-800 bg-terminal-bg/60 p-3">
                <p className="t-section text-slate-500">Short-term momentum</p>
                <p className="mt-1 t-data text-slate-300">{analysis.shortTerm.label}</p>
                <div className="mt-2">
                  <ImpactBadge level={analysis.shortTerm.badge} />
                </div>
              </div>
              <div className="rounded-lg border border-gray-800 bg-terminal-bg/60 p-3">
                <p className="t-section text-slate-500">Quarterly breadth</p>
                <p className="mt-1 t-data text-slate-300">{analysis.quarterly.label}</p>
                <div className="mt-2">
                  <ImpactBadge level={analysis.quarterly.badge} />
                </div>
              </div>
              <div className="rounded-lg border border-gray-800 bg-terminal-bg/60 p-3">
                <p className="t-section text-slate-500">Secondary breadth</p>
                <p className="mt-1 t-mono text-slate-400">
                  25% M: ↑{fmtN(latest?.up_25_m)} ↓{fmtN(latest?.down_25_m)}
                </p>
                <p className="mt-1 t-mono text-slate-400">
                  50% M: ↑{fmtN(latest?.up_50_m)} ↓{fmtN(latest?.down_50_m)}
                </p>
                <p className="mt-1 t-mono text-slate-400">
                  13% / 34d: ↑{fmtN(latest?.up_13_34d)} ↓{fmtN(latest?.down_13_34d)}
                </p>
              </div>
              <div className="rounded-lg border border-gray-800 bg-terminal-bg/60 p-3">
                <p className="t-section text-slate-500">T2108 (external)</p>
                <p className="mt-1 text-lg font-mono font-bold text-white">
                  {latest?.t2108 != null ? latest.t2108.toFixed(2) : "—"}
                </p>
                <p className="mt-1 t-data text-slate-400">{analysis.t2108.text}</p>
                <div className="mt-2">
                  <ImpactBadge level={analysis.t2108.badge} />
                </div>
              </div>
              {analysis.verdict ? (
                <div className="rounded-lg border border-emerald-700/40 bg-emerald-950/30 p-3 shadow-[0_0_16px_rgba(16,185,129,0.12)]">
                  <p className="t-section text-emerald-400">Final verdict</p>
                  <p className="mt-2 text-sm font-semibold leading-snug text-emerald-100">{analysis.verdict}</p>
                </div>
              ) : (
                <div className="rounded-lg border border-gray-800 bg-terminal-bg/40 p-3 t-data text-slate-500">
                  No automated verdict (needs T2108 &lt; 25 and Up 4%+ &gt; 500 on latest row).
                </div>
              )}
            </>
          ) : (
            <p className="t-data text-slate-500">No rows to analyze.</p>
          )}

          <div className="rounded-lg border border-gray-800 bg-terminal-bg/60 p-3">
            <p className="t-section text-slate-500">Leading themes (Finviz 1D proxy)</p>
            <p className="mt-1 t-micro">{data.leading_themes_note}</p>
            <ul className="mt-2 space-y-1.5">
              {(data.leading_themes ?? []).length ? (
                (data.leading_themes ?? []).map((t, i) => (
                  <li key={`${t.theme}-${i}`} className="flex items-baseline justify-between gap-2">
                    <span className="truncate t-data text-slate-300">{t.theme}</span>
                    <span className="t-mono shrink-0 font-semibold text-emerald-300">
                      {t.perf1D >= 0 ? "+" : ""}
                      {t.perf1D.toFixed(2)}%
                    </span>
                  </li>
                ))
              ) : (
                <li className="t-data text-slate-600">No cached Finviz theme snapshot yet.</li>
              )}
            </ul>
          </div>

          <p className="t-micro">
            Source: published Google Sheet linked from Stockbee. For research only — verify before trading.
          </p>
        </div>
      </div>
    </div>
  );
});

export default MarketBreadthReport;
