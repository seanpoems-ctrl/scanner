type Props = { level: string | null | undefined };

export function ImpactBadge({ level }: Props) {
  const lv = (level || "").toLowerCase();
  const styles =
    lv.includes("extreme") && !lv.includes("bullish")
      ? "text-rose-500 font-black border-rose-500/50 bg-rose-500/10"
      : lv.includes("overbought")
        ? "text-purple-300 font-bold border-purple-400/50 bg-purple-500/10"
        : lv === "high"
          ? "text-orange-400 font-bold border-orange-400/50 bg-orange-400/10"
          : lv.includes("bullish") || lv.includes("thrust")
            ? "text-emerald-400 font-bold border-emerald-400/50 bg-emerald-400/10"
            : lv.includes("oversold")
              ? "text-orange-400 font-semibold border-orange-400/50 bg-orange-400/10"
              : lv.includes("bearish")
                ? "text-rose-400 font-bold border-rose-400/50 bg-rose-400/10"
                : lv === "low"
                  ? "text-emerald-400 font-semibold border-emerald-400/50 bg-emerald-400/10"
                  : "text-amber-200 font-semibold border-amber-200/50 bg-amber-200/10";
  return (
    <span
      className={`inline-block min-w-[80px] rounded border px-1.5 py-0.5 text-center text-[10px] uppercase tracking-widest ${styles}`}
    >
      {level || "—"}
    </span>
  );
}
