/**
 * StockListModal — drill-down stock list for a Stockbee breadth filter cell.
 *
 * Two views:
 *   List    — flat table of individual stocks (ticker, company, price, change%, ADR%, mkt-cap, $vol)
 *   Grouped — two-level accordion matching the Leaderboard Industry tab:
 *               PARENT ROW  "Technology"  (87)   [copy all ↗]
 *                 CHILD ROW "Semiconductors · AI & Semiconductors" | 34 | [copy]
 *                 CHILD ROW "Software - Application · AI & SaaS"   | 21 | [copy]
 *
 * Copy icon on parent → copies ALL tickers in that parent (hover-only).
 * Copy icon on child  → copies that sub-industry's tickers (always visible).
 */

import {
  memo,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  ChevronDown,
  ChevronRight,
  Copy,
  Check,
  X,
  LayoutList,
  Layers,
  Loader2,
} from "lucide-react";
import { API_BASE_URL } from "../lib/apiBase";

// ── Types ──────────────────────────────────────────────────────────────────

export type BreadthStock = {
  ticker: string;
  company: string;
  market_cap_b: number | null;
  price: number | null;
  change_pct: number | null;
  dollar_volume: string;
  adr_pct: number | null;
  industry: string | null;
  thematic_label: string;
  leaderboard_parent: string;
};

export type BreadthSubIndustry = {
  industry: string;
  thematic_label: string;
  count: number;
  tickers: string[];
};

export type BreadthGroup = {
  group_name: string;
  count: number;
  avg_change_pct: number | null;
  tickers: string[];
  sub_industries: BreadthSubIndustry[];
};

export type BreadthStocksPayload = {
  ok: boolean;
  filter: string;
  min_cap_b: number;
  count: number;
  stocks: BreadthStock[];
  fetched_at_utc: string;
};

export type BreadthGroupedPayload = {
  ok: boolean;
  filter: string;
  min_cap_b: number;
  total_count: number;
  groups: BreadthGroup[];
  fetched_at_utc: string;
};

export type StockListModalProps = {
  /** Breadth filter key, e.g. "up4", "dn4", "up25q" */
  filter: string;
  /** Human-readable label shown in the modal title */
  filterLabel: string;
  /** Minimum market cap in billions (default 1.0) */
  minCapB?: number;
  onClose: () => void;
};

// ── Helpers ────────────────────────────────────────────────────────────────

function fmtPct(v: number | null): string {
  if (v == null) return "—";
  const s = v.toFixed(2);
  return v > 0 ? `+${s}%` : `${s}%`;
}

function fmtPrice(v: number | null): string {
  if (v == null) return "—";
  return `$${v.toFixed(2)}`;
}

function fmtCap(v: number | null): string {
  if (v == null) return "—";
  if (v >= 1000) return `$${(v / 1000).toFixed(1)}T`;
  if (v >= 1) return `$${v.toFixed(1)}B`;
  return `$${(v * 1000).toFixed(0)}M`;
}

function changeCls(v: number | null): string {
  if (v == null) return "text-slate-400";
  return v > 0 ? "text-emerald-400" : v < 0 ? "text-rose-400" : "text-slate-400";
}

// ── CopyButton ─────────────────────────────────────────────────────────────

const CopyButton = memo(function CopyButton({
  tickers,
  className = "",
}: {
  tickers: string[];
  className?: string;
}) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleCopy = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      void navigator.clipboard.writeText(tickers.join(",")).then(() => {
        setCopied(true);
        if (timerRef.current) clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => setCopied(false), 1800);
      });
    },
    [tickers],
  );

  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current); }, []);

  return (
    <button
      onClick={handleCopy}
      title={`Copy ${tickers.length} tickers`}
      className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-xs transition-colors
        hover:bg-slate-700 ${copied ? "text-emerald-400" : "text-slate-500 hover:text-slate-300"} ${className}`}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
      <span>{copied ? "Copied" : tickers.length}</span>
    </button>
  );
});

// ── List view ──────────────────────────────────────────────────────────────

const ListView = memo(function ListView({ stocks }: { stocks: BreadthStock[] }) {
  if (stocks.length === 0) {
    return (
      <p className="py-12 text-center text-sm text-slate-500">
        No stocks match the current filters.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[640px] border-collapse text-left text-xs">
        <thead>
          <tr className="border-b border-gray-800 text-slate-400">
            <th className="py-2 pr-3 font-medium">Ticker</th>
            <th className="px-2 py-2 font-medium">Company</th>
            <th className="px-2 py-2 text-right font-medium">Price</th>
            <th className="px-2 py-2 text-right font-medium">Chg%</th>
            <th className="px-2 py-2 text-right font-medium">ADR%</th>
            <th className="px-2 py-2 text-right font-medium">Mkt Cap</th>
            <th className="px-2 py-2 text-right font-medium">$Vol</th>
            <th className="px-2 py-2 font-medium">Industry</th>
          </tr>
        </thead>
        <tbody>
          {stocks.map((s) => (
            <tr
              key={s.ticker}
              className="border-b border-gray-800/60 hover:bg-slate-800/40"
            >
              <td className="py-1.5 pr-3 font-mono font-semibold text-slate-100">
                {s.ticker}
              </td>
              <td className="max-w-[180px] truncate px-2 py-1.5 text-slate-300">
                {s.company || "—"}
              </td>
              <td className="px-2 py-1.5 text-right font-mono text-slate-300">
                {fmtPrice(s.price)}
              </td>
              <td className={`px-2 py-1.5 text-right font-mono font-semibold ${changeCls(s.change_pct)}`}>
                {fmtPct(s.change_pct)}
              </td>
              <td className="px-2 py-1.5 text-right font-mono text-slate-300">
                {s.adr_pct != null ? `${s.adr_pct.toFixed(1)}%` : "—"}
              </td>
              <td className="px-2 py-1.5 text-right font-mono text-slate-400">
                {fmtCap(s.market_cap_b)}
              </td>
              <td className="px-2 py-1.5 text-right font-mono text-slate-400">
                {s.dollar_volume || "—"}
              </td>
              <td className="max-w-[160px] truncate px-2 py-1.5 text-slate-500">
                {s.industry || "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
});

// ── Grouped view ───────────────────────────────────────────────────────────

const GroupRow = memo(function GroupRow({ group }: { group: BreadthGroup }) {
  const [open, setOpen] = useState(false);

  return (
    <div className="border-t border-terminal-border first:border-t-0">
      {/* Parent row */}
      <div
        className="group flex cursor-pointer items-center gap-2 bg-terminal-elevated/20 px-3 py-2
          hover:bg-terminal-elevated/40"
        onClick={() => setOpen((o) => !o)}
        role="button"
        aria-expanded={open}
      >
        <span className="text-slate-400">
          {open
            ? <ChevronDown className="h-4 w-4" />
            : <ChevronRight className="h-4 w-4" />}
        </span>
        <span className="flex-1 text-sm font-semibold text-slate-100">
          {group.group_name}
        </span>
        <span className="text-xs text-slate-400">{group.count}</span>
        {group.avg_change_pct != null && (
          <span className={`text-xs font-mono ${changeCls(group.avg_change_pct)}`}>
            {fmtPct(group.avg_change_pct)}
          </span>
        )}
        {/* Copy all tickers in parent — hover-only */}
        <span className="opacity-0 transition-opacity group-hover:opacity-100">
          <CopyButton tickers={group.tickers} />
        </span>
      </div>

      {/* Sub-industry child rows */}
      {open && (
        <div className="ml-4 border-l border-slate-700">
          {group.sub_industries.map((sub) => (
            <div
              key={`${sub.industry}|${sub.thematic_label}`}
              className="flex items-center gap-2 px-3 py-1.5 text-sm
                hover:bg-terminal-elevated/30"
            >
              <span className="flex-1 pl-4 text-slate-300">
                <span className="font-mono">{sub.industry}</span>
                {sub.thematic_label && (
                  <span className="ml-2 text-xs text-slate-500">
                    · {sub.thematic_label}
                  </span>
                )}
              </span>
              {/* Copy sub-industry tickers — always visible */}
              <CopyButton tickers={sub.tickers} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
});

const GroupedView = memo(function GroupedView({ groups }: { groups: BreadthGroup[] }) {
  if (groups.length === 0) {
    return (
      <p className="py-12 text-center text-sm text-slate-500">
        No groups to display.
      </p>
    );
  }

  return (
    <div>
      {/* Column header */}
      <div className="flex items-center gap-2 border-b border-gray-800 px-3 py-1.5 text-xs font-medium text-slate-500">
        <span className="flex-1">Industry Group</span>
        <span>Count</span>
      </div>
      {groups.map((g) => (
        <GroupRow key={g.group_name} group={g} />
      ))}
    </div>
  );
});

// ── Main modal ─────────────────────────────────────────────────────────────

const StockListModal = memo(function StockListModal({
  filter,
  filterLabel,
  minCapB = 1.0,
  onClose,
}: StockListModalProps) {
  const [view, setView] = useState<"list" | "grouped">("grouped");
  const [stocks, setStocks] = useState<BreadthStock[]>([]);
  const [groups, setGroups] = useState<BreadthGroup[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [totalCount, setTotalCount] = useState(0);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;

    const qs = `filter=${encodeURIComponent(filter)}&min_cap_b=${minCapB}`;

    try {
      const [listRes, groupedRes] = await Promise.all([
        fetch(`${API_BASE_URL}/api/market-breadth/stocks?${qs}`, { signal: ctrl.signal }),
        fetch(`${API_BASE_URL}/api/market-breadth/stocks/grouped?${qs}`, { signal: ctrl.signal }),
      ]);

      const [listBody, groupedBody] = await Promise.all([
        listRes.json() as Promise<BreadthStocksPayload>,
        groupedRes.json() as Promise<BreadthGroupedPayload>,
      ]);

      if (listBody.ok) {
        setStocks(listBody.stocks ?? []);
        setTotalCount(listBody.count ?? 0);
      }
      if (groupedBody.ok) {
        setGroups(groupedBody.groups ?? []);
        if (!listBody.ok) setTotalCount(groupedBody.total_count ?? 0);
      }
      if (!listBody.ok && !groupedBody.ok) {
        setError("Failed to load stocks.");
      }
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return;
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [filter, minCapB]);

  useEffect(() => {
    void load();
    return () => { abortRef.current?.abort(); };
  }, [load]);

  // Close on Escape key
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      {/* Panel */}
      <div className="relative flex h-[85vh] w-full max-w-3xl flex-col rounded-xl border border-gray-700
        bg-terminal-bg shadow-2xl">

        {/* Header */}
        <div className="flex items-center gap-3 border-b border-gray-800 px-4 py-3">
          <div className="flex-1">
            <h2 className="text-sm font-semibold text-slate-100">{filterLabel}</h2>
            {!loading && (
              <p className="text-xs text-slate-500">
                {totalCount} stocks · min ${minCapB}B mkt cap
              </p>
            )}
          </div>

          {/* View toggle */}
          <div className="flex items-center gap-1 rounded-lg border border-gray-700 p-0.5">
            <button
              onClick={() => setView("grouped")}
              title="Grouped view"
              className={`flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs transition-colors
                ${view === "grouped"
                  ? "bg-slate-700 text-slate-100"
                  : "text-slate-400 hover:text-slate-200"}`}
            >
              <Layers className="h-3.5 w-3.5" />
              Grouped
            </button>
            <button
              onClick={() => setView("list")}
              title="List view"
              className={`flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs transition-colors
                ${view === "list"
                  ? "bg-slate-700 text-slate-100"
                  : "text-slate-400 hover:text-slate-200"}`}
            >
              <LayoutList className="h-3.5 w-3.5" />
              List
            </button>
          </div>

          {/* Copy all */}
          {stocks.length > 0 && (
            <CopyButton tickers={stocks.map((s) => s.ticker)} className="text-xs" />
          )}

          {/* Close */}
          <button
            onClick={onClose}
            className="rounded p-1 text-slate-400 hover:bg-slate-700 hover:text-slate-200"
            aria-label="Close"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Body */}
        <div className="min-h-0 flex-1 overflow-y-auto">
          {loading ? (
            <div className="flex h-full items-center justify-center">
              <Loader2 className="h-6 w-6 animate-spin text-slate-500" />
            </div>
          ) : error ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-sm text-rose-400">
              <p>{error}</p>
              <button
                onClick={() => void load()}
                className="text-xs text-slate-400 underline hover:text-slate-200"
              >
                Retry
              </button>
            </div>
          ) : view === "list" ? (
            <ListView stocks={stocks} />
          ) : (
            <GroupedView groups={groups} />
          )}
        </div>
      </div>
    </div>
  );
});

export default StockListModal;
