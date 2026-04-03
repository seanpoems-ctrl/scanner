import { RefreshCw } from "lucide-react";

type Props = {
  onRefresh: () => void;
  loading: boolean;
  lastUpdatedAt: number | null;
  label?: string;
};

function formatRelative(ms: number | null): string {
  if (ms == null) return "—";
  const s = Math.round((Date.now() - ms) / 1000);
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return (
    new Date(ms).toLocaleTimeString("en-US", {
      timeZone: "America/New_York",
      hour: "2-digit",
      minute: "2-digit",
    }) + " ET"
  );
}

export function RefreshRow({ onRefresh, loading, lastUpdatedAt, label = "Refresh" }: Props) {
  return (
    <div className="flex items-center gap-3">
      {lastUpdatedAt != null && (
        <span className="text-[10px] text-slate-500">
          Updated <span className="font-mono tabular-nums">{formatRelative(lastUpdatedAt)}</span>
        </span>
      )}
      <button
        type="button"
        onClick={onRefresh}
        disabled={loading}
        className="flex items-center gap-1.5 rounded-lg border border-terminal-border bg-terminal-bg px-3 py-1.5 text-[11px] font-semibold text-slate-300 transition-colors hover:border-slate-500 hover:text-white disabled:opacity-40"
        aria-label={loading ? "Updating..." : label}
      >
        <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} aria-hidden />
        {loading ? "Updating..." : label}
      </button>
    </div>
  );
}
