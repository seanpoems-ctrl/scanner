import { useMemo } from "react";
import type { RotationThemeRow } from "../hooks/useRotationSnapshot";

function lerp(a: number, b: number, u: number): number {
  return a + (b - a) * u;
}

/** Map normalized strength `t` in [0,1] to RGB (low RS → rose, mid → slate, high → emerald). */
function heatRgb(t: number): string {
  const x = Math.max(0, Math.min(1, t));
  let r: number;
  let g: number;
  let b: number;
  if (x < 0.5) {
    const u = x / 0.5;
    r = lerp(248, 71, u);
    g = lerp(113, 85, u);
    b = lerp(113, 105, u);
  } else {
    const u = (x - 0.5) / 0.5;
    r = lerp(71, 52, u);
    g = lerp(85, 211, u);
    b = lerp(105, 153, u);
  }
  return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
}

function shortDate(iso: string): string {
  const s = String(iso).slice(0, 10);
  if (s.length >= 10) return `${s.slice(5, 7)}/${s.slice(8, 10)}`;
  return iso;
}

function collectDates(themes: RotationThemeRow[]): string[] {
  const set = new Set<string>();
  for (const row of themes) {
    for (const pt of row.history ?? []) {
      const d = pt?.date;
      if (typeof d === "string" && d.length >= 8) set.add(d.slice(0, 10));
    }
  }
  return [...set].sort();
}

function buildMatrix(themes: RotationThemeRow[], dates: string[]) {
  const grid = new Map<string, Map<string, number | null>>();
  for (const row of themes) {
    const m = new Map<string, number | null>();
    for (const d of dates) m.set(d, null);
    for (const pt of row.history ?? []) {
      if (typeof pt?.date !== "string") continue;
      const key = pt.date.slice(0, 10);
      if (!m.has(key)) continue;
      const v = pt.rs1m;
      const n = v == null ? null : Number(v);
      m.set(key, n != null && Number.isFinite(n) ? n : null);
    }
    grid.set(row.theme, m);
  }
  return grid;
}

function minMaxRs(grid: Map<string, Map<string, number | null>>): { min: number; max: number } | null {
  let min = Infinity;
  let max = -Infinity;
  for (const col of grid.values()) {
    for (const v of col.values()) {
      if (v == null || !Number.isFinite(v)) continue;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
  }
  if (!Number.isFinite(min) || !Number.isFinite(max)) return null;
  if (min === max) return { min: min - 1, max: max + 1 };
  return { min, max };
}

type Props = {
  themes: RotationThemeRow[];
};

export function RotationHeatmap({ themes }: Props) {
  const dates = useMemo(() => collectDates(themes), [themes]);
  const grid = useMemo(() => buildMatrix(themes, dates), [themes, dates]);
  const range = useMemo(() => minMaxRs(grid), [grid]);

  if (!dates.length) {
    return (
      <p className="t-data text-slate-500">
        Not enough dated history to draw a heatmap. After a few daily scanner snapshots, dates will appear as columns.
      </p>
    );
  }

  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-3">
      <div className="fintech-scroll min-h-0 flex-1 overflow-auto rounded-lg border border-terminal-border/80 bg-terminal-bg/30">
        <table className="w-max min-w-full border-separate border-spacing-0 text-left t-data">
          <caption className="sr-only">
            Heatmap of one-month relative strength by theme and snapshot date. Darker rose is lower RS; emerald is higher RS.
          </caption>
          <thead>
            <tr>
              <th
                scope="col"
                className="sticky left-0 top-0 z-30 border-b border-r border-terminal-border bg-terminal-card px-2 py-2 t-label"
              >
                Theme
              </th>
              {dates.map((d) => (
                <th
                  key={d}
                  scope="col"
                  className="sticky top-0 z-20 min-w-[40px] border-b border-terminal-border bg-terminal-card px-1 py-2 text-center font-mono text-[10px] font-semibold uppercase tracking-wide text-slate-400"
                  title={d}
                >
                  {shortDate(d)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {themes.map((row) => {
              const rowMap = grid.get(row.theme);
              return (
                <tr key={row.theme} className="border-b border-terminal-border/40">
                  <th
                    scope="row"
                    className="sticky left-0 z-10 max-w-[160px] border-r border-terminal-border bg-terminal-card px-2 py-1.5 text-left font-medium text-slate-200"
                    title={row.theme}
                  >
                    <span className="line-clamp-2 text-xs leading-snug">{row.theme}</span>
                  </th>
                  {dates.map((d) => {
                    const rs = rowMap?.get(d) ?? null;
                    let bg = "rgba(30, 41, 59, 0.85)";
                    if (rs != null && range) {
                      const tNorm = (rs - range.min) / (range.max - range.min);
                      bg = heatRgb(tNorm);
                    }
                    const label =
                      rs != null && Number.isFinite(rs)
                        ? `${row.theme}, ${d}, RS 1M ${rs.toFixed(1)}`
                        : `${row.theme}, ${d}, no RS snapshot`;
                    return (
                      <td
                        key={d}
                        className="h-9 min-w-[36px] cursor-default border-b border-terminal-border/30 p-0 align-middle"
                        style={{ backgroundColor: bg }}
                        title={label}
                        aria-label={label}
                      />
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {range ? (
        <div className="flex flex-wrap items-center gap-3 t-micro text-slate-500">
          <span className="t-label text-slate-400">RS 1M scale</span>
          <div
            className="h-2 flex-1 min-w-[120px] max-w-md rounded-sm border border-terminal-border"
            style={{
              background: `linear-gradient(to right, ${heatRgb(0)}, ${heatRgb(0.5)}, ${heatRgb(1)})`,
            }}
            role="img"
            aria-label={`Relative strength from ${range.min.toFixed(1)} to ${range.max.toFixed(1)}`}
          />
          <span className="font-mono text-slate-400">
            {range.min.toFixed(1)} → {range.max.toFixed(1)}
          </span>
        </div>
      ) : null}
    </div>
  );
}
