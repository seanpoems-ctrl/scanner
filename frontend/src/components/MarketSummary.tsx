type MarketFlowSummary = {
  aggregateDollarVolume: number;
  previousAggregateDollarVolume: number;
  aggregateAvg20DollarVolume: number;
  flowTrend: "up" | "down";
  conviction: "high" | "low";
};

function formatCompactMoney(value: number): string {
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(1)}B`;
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  return `$${value.toFixed(0)}`;
}

export function MarketSummary({ summary }: { summary: MarketFlowSummary }) {
  const isUp = summary.flowTrend === "up";
  const trendIcon = isUp ? "🟢 ↑" : "🔴 ↓";
  const trendText = isUp ? "Flow Expanding" : "Low Conviction";

  return (
    <div className="px-2 pb-2 pt-1 text-center">
      <p className="font-mono text-[11px] font-semibold leading-tight text-slate-100">
        {formatCompactMoney(summary.aggregateDollarVolume)} <span className="text-slate-500">top themes</span>
      </p>
      <p className="mt-0.5 font-mono text-[9px] leading-tight text-slate-500">
        20D avg {formatCompactMoney(summary.aggregateAvg20DollarVolume)} · Prev {formatCompactMoney(summary.previousAggregateDollarVolume)}
      </p>
      <p className={`mt-1.5 font-mono text-[10px] font-semibold leading-tight ${isUp ? "text-emerald-400" : "text-rose-400"}`}>
        {trendIcon} {trendText}
      </p>
    </div>
  );
}
