import { memo, useMemo, useState } from "react";
import { BarChart3, ExternalLink } from "lucide-react";
import { ImpactBadge } from "./ui/ImpactBadge";
import { PanelLoading } from "./ui/SkeletonRows";
import StockListModal, { type FilterKey } from "./ui/StockListModal";

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
  atr_10x_ext: number | null;        // count of up4%+ stocks ≥10×ATR from 50-SMA
  above_50dma_pct: number | null;    // % of up4%+ stocks above their 50-day SMA
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
  const [modalFilter, setModalFilter] = useState<FilterKey | null>(null);
  const [modalLabel, setModalLabel] = useState<string>("");

  const latest = data?.rows?.[0] ?? null;

  const CLICKABLE_COLS: Record<number, { filter: FilterKey; label: string }> = {
    1:  { filter: "up4",     label: "Up 4%+ Today" },
    2:  { filter: "dn4",     label: "Down 4%+ Today" },
    5:  { filter: "up25q",   label: "Up 25%+ Quarterly" },
    6:  { filter: "dn25q",   label: "Down 25%+ Quarterly" },
    7:  { filter: "up25m",   label: "Up 25%+ Monthly" },
    8:  { filter: "dn25m",   label: "Down 25%+ Monthly" },
    9:  { filter: "up50m",   label: "Up 50%+ Monthly" },
    10: { filter: "dn50m",   label: "Down 50%+ Monthly" },
    11: { filter: "up13_34", label: "Up 13%+ 34d" },
    12: { filter: "dn13_34", label: "Down 13%+ 34d" },
  };

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
          <table className="w-full min-w-[1040px] border-collapse text-left">
            <caption className="sr-only">Stockbee market monitor historical rows by date.</caption>
            <thead>
              <tr className="border-b border-gray-800">
                <th scope="col" className="sticky left-0 z-10 bg-terminal-bg py-2 pr-3 t-label text-emerald-400 whitespace-nowrap">Date</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Up 4%+</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Dn 4%+</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">5d R</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">10d R</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Up 25% Q</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Dn 25% Q</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Up 25% M</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Dn 25% M</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Up 50% M</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Dn 50% M</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Up 13% 34d</th>
                <th scope="col" className="px-1 py-2 t-label text-emerald-400 whitespace-nowrap">Dn 13% 34d</th>
                <th scope="col" className="px-1 py-2 t-label text-purple-400 whitespace-nowrap">10x ATR Ext</th>
                <th scope="col" className="px-1 py-2 t-label text-sky-400 whitespace-nowrap">&gt;50 DMA</th>
                <th scope="col" className="px-1 py-2 t-label text-amber-400 whitespace-nowrap">Share Universe</th>
                <th scope="col" className="px-1 py-2 t-label text-amber-400 whitespace-nowrap">T2108</th>
                <th scope="col" className="px-1 py-2 t-label text-amber-400 whitespace-nowrap">S&amp;P</th>
              </tr>
            </thead>
            <tbody>
              {(data.rows ?? []).map((r, rowIndex) => {
                const isLatest = rowIndex === 0;
                const bearishRow =
                  r.up_25_q != null && r.down_25_q != null && r.up_25_q < r.down_25_q;
                const up4hi = r.up_4_pct != null && r.up_4_pct > 600;
                const t2108Low = r.t2108 != null && r.t2108 < 20;

                const clickCls = (colIdx: number, base: string) => {
                  if (!isLatest || !CLICKABLE_COLS[colIdx]) return base;
                  return `${base} cursor-pointer underline decoration-dotted underline-offset-2 hover:text-white`;
                };

                return (
                  <tr
                    key={r.date}
                    className={`border-b border-gray-800/80 t-mono ${
                      bearishRow ? "text-rose-200/70" : "text-slate-300"
                    }`}
                  >
                    {/* Date — sticky */}
                    <td className="sticky left-0 z-10 bg-terminal-bg py-1 pr-3 t-mono whitespace-nowrap">
                      {r.date_display}
                    </td>

                    {/* Up 4%+ — col 1, clickable on latest */}
                    <td
                      className={clickCls(1, `px-1 py-1 text-right t-mono whitespace-nowrap ${up4hi ? "text-emerald-300 font-semibold" : ""}`)}
                      {...(isLatest && CLICKABLE_COLS[1] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[1].filter); setModalLabel(CLICKABLE_COLS[1].label); } } : {})}
                    >
                      {fmtN(r.up_4_pct)}
                    </td>

                    {/* Dn 4%+ — col 2, clickable on latest */}
                    <td
                      className={clickCls(2, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[2] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[2].filter); setModalLabel(CLICKABLE_COLS[2].label); } } : {})}
                    >
                      {fmtN(r.down_4_pct)}
                    </td>

                    {/* 5d R — col 3, not clickable */}
                    <td className="px-1 py-1 text-right t-mono whitespace-nowrap">{fmtN(r.ratio_5d)}</td>

                    {/* 10d R — col 4, not clickable */}
                    <td className="px-1 py-1 text-right t-mono whitespace-nowrap">{fmtN(r.ratio_10d)}</td>

                    {/* Up 25% Q — col 5, clickable on latest */}
                    <td
                      className={clickCls(5, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[5] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[5].filter); setModalLabel(CLICKABLE_COLS[5].label); } } : {})}
                    >
                      {fmtN(r.up_25_q)}
                    </td>

                    {/* Dn 25% Q — col 6, clickable on latest */}
                    <td
                      className={clickCls(6, `px-1 py-1 text-right t-mono whitespace-nowrap ${bearishRow ? "text-rose-400" : ""}`)}
                      {...(isLatest && CLICKABLE_COLS[6] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[6].filter); setModalLabel(CLICKABLE_COLS[6].label); } } : {})}
                    >
                      {fmtN(r.down_25_q)}
                    </td>

                    {/* Up 25% M — col 7, clickable on latest */}
                    <td
                      className={clickCls(7, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[7] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[7].filter); setModalLabel(CLICKABLE_COLS[7].label); } } : {})}
                    >
                      {fmtN(r.up_25_m)}
                    </td>

                    {/* Dn 25% M — col 8, clickable on latest */}
                    <td
                      className={clickCls(8, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[8] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[8].filter); setModalLabel(CLICKABLE_COLS[8].label); } } : {})}
                    >
                      {fmtN(r.down_25_m)}
                    </td>

                    {/* Up 50% M — col 9, clickable on latest */}
                    <td
                      className={clickCls(9, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[9] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[9].filter); setModalLabel(CLICKABLE_COLS[9].label); } } : {})}
                    >
                      {fmtN(r.up_50_m)}
                    </td>

                    {/* Dn 50% M — col 10, clickable on latest */}
                    <td
                      className={clickCls(10, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[10] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[10].filter); setModalLabel(CLICKABLE_COLS[10].label); } } : {})}
                    >
                      {fmtN(r.down_50_m)}
                    </td>

                    {/* Up 13% 34d — col 11, clickable on latest */}
                    <td
                      className={clickCls(11, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[11] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[11].filter); setModalLabel(CLICKABLE_COLS[11].label); } } : {})}
                    >
                      {fmtN(r.up_13_34d)}
                    </td>

                    {/* Dn 13% 34d — col 12, clickable on latest */}
                    <td
                      className={clickCls(12, "px-1 py-1 text-right t-mono whitespace-nowrap")}
                      {...(isLatest && CLICKABLE_COLS[12] ? { onClick: () => { setModalFilter(CLICKABLE_COLS[12].filter); setModalLabel(CLICKABLE_COLS[12].label); } } : {})}
                    >
                      {fmtN(r.down_13_34d)}
                    </td>

                    {/* 10x ATR Ext — col 13, purple; null for historical rows */}
                    <td className="px-1 py-1 text-right t-mono text-purple-300 whitespace-nowrap">
                      {r.atr_10x_ext != null ? fmtN(r.atr_10x_ext) : <span className="text-slate-600">—</span>}
                    </td>

                    {/* >50 DMA — col 14, sky blue; null for historical rows */}
                    <td className="px-1 py-1 text-right t-mono text-sky-300 whitespace-nowrap">
                      {r.above_50dma_pct != null
                        ? `${r.above_50dma_pct.toFixed(1)}%`
                        : <span className="text-slate-600">—</span>}
                    </td>

                    {/* Share Universe — col 15, amber */}
                    <td className="px-1 py-1 text-right t-mono text-amber-300 whitespace-nowrap">
                      {fmtN(r.worden_universe)}
                    </td>

                    {/* T2108 — col 16, amber, red when < 20 */}
                    <td className={`px-1 py-1 text-right t-mono whitespace-nowrap ${t2108Low ? "text-rose-400 font-semibold" : "text-amber-300"}`}>
                      {fmtN(r.t2108)}
                    </td>

                    {/* S&P — col 17, amber */}
                    <td className="px-1 py-1 text-right t-mono text-amber-300 whitespace-nowrap">
                      {fmtN(r.sp_index)}
                    </td>
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

      {/* Drill-down modal */}
      {modalFilter !== null && (
        <StockListModal
          open={true}
          filter={modalFilter}
          filterLabel={modalLabel}
          minCapB={1.0}
          onClose={() => setModalFilter(null)}
        />
      )}
    </div>
  );
});

export default MarketBreadthReport;
