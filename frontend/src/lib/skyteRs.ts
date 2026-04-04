/** Normalize labels for matching Finviz industry rows to skyte/rs-log CSV `industry` names. */

export type SkyteIndustryRow = {
  rank: number;
  industry: string;
  sector: string;
  relative_strength: number;
  percentile: number;
  month_1_ago?: number | null;
  month_3_ago?: number | null;
  month_6_ago?: number | null;
  tickers?: string;
};

export type SkyteIndustriesPayload = {
  ok: boolean;
  source?: string;
  kind?: string;
  count?: number;
  rows?: SkyteIndustryRow[];
  detail?: string;
  cache_hit?: boolean;
  ttl_seconds?: number;
  fetched_at_utc?: string;
};

export function normSkyteIndustryKey(name: string): string {
  let s = String(name || "")
    .trim()
    .toLowerCase();
  s = s.replace(/\u2014|\u2013|\u2212|–|—/g, "-");
  // Mojibake when UTF-8 em dash is mis-decoded (seen in skyte CSV in some environments)
  s = s.replace(/â€"/g, "-");
  s = s.replace(/[^a-z0-9]+/g, "");
  return s;
}

export function buildSkyteIndustryMap(rows: SkyteIndustryRow[]): Map<string, { percentile: number; relative_strength: number }> {
  const m = new Map<string, { percentile: number; relative_strength: number }>();
  for (const r of rows) {
    const k = normSkyteIndustryKey(r.industry);
    if (!k) continue;
    if (!m.has(k)) {
      m.set(k, { percentile: r.percentile, relative_strength: r.relative_strength });
    }
  }
  return m;
}

export function lookupSkyteIndustry(
  m: Map<string, { percentile: number; relative_strength: number }>,
  themeLabel: string
): { percentile: number; relative_strength: number } | null {
  const k = normSkyteIndustryKey(themeLabel);
  if (!k) return null;
  return m.get(k) ?? null;
}
