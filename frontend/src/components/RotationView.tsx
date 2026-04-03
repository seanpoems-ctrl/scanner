import { useMemo, useState } from "react";
import { BarChart3, RefreshCw } from "lucide-react";
import { fmtPct, pctClass } from "../lib/formatters";
import { useRotationSnapshot } from "../hooks/useRotationSnapshot";
import { ErrorBanner } from "./ui/ErrorBanner";
import { EmptyState } from "./ui/EmptyState";
import { PanelLoading } from "./ui/SkeletonRows";
import { RefreshRow } from "./ui/RefreshRow";
import { RsSparkline } from "./ui/RsSparkline";

export type { RotationHistoryPoint, RotationThemeRow } from "../hooks/useRotationSnapshot";

export function RotationView() {
  const [days, setDays] = useState(10);
  const { data, loading, error, reload, lastUpdatedAt } = useRotationSnapshot(days);

  const themes = useMemo(() => (Array.isArray(data?.themes) ? data!.themes! : []), [data]);
  const generatedAt = data?.generated_at_utc ?? null;

  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 overflow-hidden p-4">
      <section className="shrink-0 rounded-xl border border-terminal-border bg-terminal-card px-4 py-3 shadow-sm">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0">
            <h2 className="t-page flex items-center gap-2">
              <BarChart3 className="h-4 w-4 text-accent" aria-hidden />
              Sector rotation
            </h2>
            <p className="mt-0.5 max-w-[48rem] t-micro text-slate-500">
              RS momentum vs start of window (ΔRS). History builds as the backend records daily theme snapshots when the scanner refreshes.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-1.5">
              {[7, 10, 14, 21, 30].map((d) => (
                <button
                  key={d}
                  type="button"
                  onClick={() => setDays(d)}
                  className={`rounded-md border px-2 py-1 t-label transition-colors ${
                    days === d
                      ? "border-accent/40 bg-accent/15 text-white"
                      : "border-terminal-border bg-terminal-bg text-slate-400 hover:border-slate-500 hover:text-white"
                  }`}
                >
                  {d}d
                </button>
              ))}
            </div>
            <RefreshRow onRefresh={() => void reload()} loading={loading} lastUpdatedAt={lastUpdatedAt} />
          </div>
        </div>
        {generatedAt ? (
          <p className="mt-2 t-micro text-slate-600">
            Generated <span className="font-mono text-slate-500">{generatedAt}</span>
          </p>
        ) : null}
      </section>

      <section className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden rounded-xl border border-terminal-border bg-terminal-card shadow-sm">
        <div className="fintech-scroll min-h-0 flex-1 overflow-auto p-2 sm:p-3">
          {loading && !themes.length ? (
            <PanelLoading label="Loading rotation snapshot…" />
          ) : error ? (
            <ErrorBanner title="Rotation unavailable" detail={error} onRetry={() => void reload()} />
          ) : !themes.length ? (
            <EmptyState
              icon={RefreshCw}
              title="No rotation history yet"
              subtitle="Open Thematic Scanner once so the backend can record a daily snapshot, then return here."
            />
          ) : (
            <table className="w-full min-w-[900px] border-separate border-spacing-0 text-left t-data">
              <caption className="sr-only">Theme relative strength rotation and momentum over the selected window.</caption>
              <thead>
                <tr>
                  {["#", "Theme", "Sector", "RS 1M", "Trend", "ΔRS", "1D", "1W", "1M"].map((label) => (
                    <th
                      key={label}
                      scope="col"
                      className="sticky top-0 z-10 border-b border-terminal-border bg-terminal-card px-2 py-2 t-label sm:px-3"
                    >
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {themes.map((t, idx) => {
                  const d = t.rs_delta;
                  return (
                    <tr key={`${t.theme}-${idx}`} className="border-b border-terminal-border/50 hover:bg-terminal-elevated/30">
                      <td className="px-2 py-2 font-mono text-slate-500 sm:px-3">{idx + 1}</td>
                      <td className="max-w-[200px] px-2 py-2 font-medium text-slate-100 sm:px-3">
                        <span className="line-clamp-2">
                          {t.theme}
                          {t.rs_delta != null && Number.isFinite(Number(t.rs_delta)) && (
                            <span
                              className={`ml-1.5 t-mono text-[10px] ${
                                t.rs_delta > 0 ? "text-emerald-400" : "text-rose-400"
                              }`}
                            >
                              {t.rs_delta > 0 ? "▲" : "▼"}
                              {Math.abs(t.rs_delta).toFixed(1)}
                            </span>
                          )}
                        </span>
                      </td>
                      <td className="max-w-[140px] px-2 py-2 text-slate-400 sm:px-3">
                        <span className="line-clamp-2">{t.sector ?? "—"}</span>
                      </td>
                      <td className="px-2 py-2 text-right font-mono tabular-nums text-slate-200 sm:px-3">
                        {t.rs1m != null && Number.isFinite(Number(t.rs1m)) ? Number(t.rs1m).toFixed(1) : "—"}
                      </td>
                      <td className="px-2 py-2 sm:px-3">
                        <RsSparkline
                          history={t.history ?? []}
                          positive={(t.rs_delta ?? 0) >= 0}
                        />
                      </td>
                      <td
                        className={`px-2 py-2 text-right font-mono tabular-nums font-semibold sm:px-3 ${
                          d == null || !Number.isFinite(d) ? "text-slate-600" : pctClass(d)
                        }`}
                      >
                        {d == null || !Number.isFinite(d) ? "—" : `${d >= 0 ? "+" : ""}${d.toFixed(2)}`}
                      </td>
                      <td className={`px-2 py-2 text-right font-mono tabular-nums sm:px-3 ${pctClass(t.perf1D ?? 0)}`}>
                        {fmtPct(t.perf1D, 2)}
                      </td>
                      <td className={`px-2 py-2 text-right font-mono tabular-nums sm:px-3 ${pctClass(t.perf1W ?? 0)}`}>
                        {fmtPct(t.perf1W, 2)}
                      </td>
                      <td className={`px-2 py-2 text-right font-mono tabular-nums sm:px-3 ${pctClass(t.perf1M ?? 0)}`}>
                        {fmtPct(t.perf1M, 2)}
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
