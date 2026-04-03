/** Shared display helpers (unit-tested). */

export function formatMoney(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(2)}B`;
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
  return `$${value.toFixed(2)}`;
}

export function pctClass(value: number): string {
  if (!Number.isFinite(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-400";
  if (value < 0) return "text-rose-400";
  return "text-slate-400";
}

export function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const x = Number(v);
  return `${x >= 0 ? "+" : ""}${x.toFixed(digits)}%`;
}

export function fmtPrice(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const x = Number(v);
  if (x >= 1000) return Math.round(x).toLocaleString();
  if (x >= 100) return x.toFixed(1);
  return x.toFixed(2);
}
