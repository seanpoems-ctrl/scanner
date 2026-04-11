/**
 * StockListModal — drill-down stock list for Stockbee Market Monitor breadth cells.
 *
 * Two views:
 *   List    — sortable flat table with virtual chunking (50 rows at a time via IntersectionObserver)
 *   Grouped — two-level accordion matching the Leaderboard Industry tab
 *
 * Data is fetched once on open via Promise.all and cached in state — no refetch on view toggle.
 */

import {
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Clipboard,
  X,
} from "lucide-react";
import { ErrorBanner } from "./ErrorBanner";
import { SkeletonRows } from "./SkeletonRows";
import { API_BASE_URL } from "../../lib/apiBase";

// ── Exported types ─────────────────────────────────────────────────────────

export type FilterKey =
  | "up4" | "dn4"
  | "up25q" | "dn25q"
  | "up25m" | "dn25m"
  | "up50m" | "dn50m"
  | "up13_34" | "dn13_34";

export interface StockRow {
  ticker: string;
  company: string;
  market_cap_b: number;
  price: number;
  change_pct: number;
  dollar_volume: string;
  adr_pct: number | null;
  industry: string;
  thematic_label: string;
  leaderboard_parent: string;
}

export interface SubIndustry {
  industry: string;
  thematic_label: string;
  count: number;
  tickers: string[];
}

export interface IndustryGroup {
  group_name: string;
  count: number;
  avg_change_pct: number;
  tickers: string[];
  sub_industries: SubIndustry[];
}

export interface StockListModalProps {
  open: boolean;
  onClose: () => void;
  filter: FilterKey;
  filterLabel: string;
  minCapB?: number;
}

// ── Internal API response shapes ───────────────────────────────────────────

interface _StocksResponse {
  ok: boolean;
  stocks: StockRow[];
  count: number;
  detail?: string;
}

interface _GroupedResponse {
  ok: boolean;
  groups: IndustryGroup[];
  total_count: number;
  detail?: string;
}

// ── Sort types ─────────────────────────────────────────────────────────────

type SortKey = "ticker" | "dollar_volume" | "adr_pct" | "change_pct";
type SortDir = "asc" | "desc";

interface SortState {
  key: SortKey;
  dir: SortDir;
}

// ── Helpers ────────────────────────────────────────────────────────────────

const DN_FILTERS: ReadonlySet<string> = new Set([
  "dn4", "dn25q", "dn25m", "dn50m", "dn13_34",
]);

function defaultSort(filter: FilterKey): SortState {
  return {
    key: "change_pct",
    dir: DN_FILTERS.has(filter) ? "asc" : "desc",
  };
}

function fmtPct(v: number | null | undefined): string {
  if (v == null) return "—";
  const fixed = Math.abs(v).toFixed(2);
  return v >= 0 ? `+${fixed}%` : `-${fixed}%`;
}

function sortRows(rows: StockRow[], sort: SortState): StockRow[] {
  return [...rows].sort((a, b) => {
    let av: number | string | null;
    let bv: number | string | null;

    switch (sort.key) {
      case "ticker":
        av = a.ticker;
        bv = b.ticker;
        break;
      case "dollar_volume":
        // Sort by dollar_volume string lexicographically — reasonable proxy
        av = a.dollar_volume ?? "";
        bv = b.dollar_volume ?? "";
        break;
      case "adr_pct":
        av = a.adr_pct ?? -Infinity;
        bv = b.adr_pct ?? -Infinity;
        break;
      case "change_pct":
      default:
        av = a.change_pct ?? -Infinity;
        bv = b.change_pct ?? -Infinity;
        break;
    }

    if (av === null || av === undefined) return 1;
    if (bv === null || bv === undefined) return -1;
    if (av < bv) return sort.dir === "asc" ? -1 : 1;
    if (av > bv) return sort.dir === "asc" ? 1 : -1;
    return 0;
  });
}

function copyTickers(tickers: string[]): Promise<void> {
  return navigator.clipboard.writeText(tickers.join(","));
}

// ── SortIcon ───────────────────────────────────────────────────────────────

function SortIcon({ col, sort }: { col: SortKey; sort: SortState }) {
  if (sort.key !== col) {
    return <span className="ml-0.5 text-slate-600 text-[10px] leading-none">⇅</span>;
  }
  return sort.dir === "desc"
    ? <ChevronDown className="ml-0.5 inline h-3 w-3 text-accent" />
    : <ChevronUp   className="ml-0.5 inline h-3 w-3 text-accent" />;
}

// ── CopyIconButton ─────────────────────────────────────────────────────────

function CopyIconButton({
  tickers,
  id,
  copiedMap,
  onCopy,
  className = "",
}: {
  tickers: string[];
  id: string;
  copiedMap: Map<string, boolean>;
  onCopy: (id: string, tickers: string[]) => void;
  className?: string;
}) {
  const wasCopied = copiedMap.get(id) === true;
  return (
    <div className="relative flex items-center">
      {wasCopied && (
        <span
          className="absolute -top-6 left-1/2 -translate-x-1/2 whitespace-nowrap
            rounded bg-slate-700 px-1.5 py-0.5 text-[10px] text-slate-200 shadow"
        >
          Copied!
        </span>
      )}
      <button
        onClick={(e) => { e.stopPropagation(); onCopy(id, tickers); }}
        title={`Copy ${tickers.length} tickers`}
        className={`flex items-center justify-center rounded p-0.5 transition-colors
          hover:bg-slate-700 ${wasCopied ? "text-emerald-400" : "text-slate-500 hover:text-slate-300"}
          ${className}`}
      >
        <Clipboard className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}

// ── Stock list (virtualized in chunks) ────────────────────────────────────

const CHUNK = 50;

function StockListView({
  rows,
  sort,
  onSort,
}: {
  rows: StockRow[];
  sort: SortState;
  onSort: (key: SortKey) => void;
}) {
  const [visibleCount, setVisibleCount] = useState(CHUNK);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  // Reset chunk size when rows change (e.g. sort change)
  useEffect(() => { setVisibleCount(CHUNK); }, [rows]);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) setVisibleCount((n) => n + CHUNK); },
      { threshold: 0.1 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  const visible = rows.slice(0, visibleCount);

  if (rows.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-slate-500">
        <p className="t-label">No stocks match this filter today</p>
      </div>
    );
  }

  const thCls =
    "px-3 py-2 text-left text-[11px] font-medium text-slate-400 cursor-pointer select-none whitespace-nowrap hover:text-slate-200 transition-colors";

  return (
    <table className="w-full border-collapse text-xs">
      <thead className="sticky top-0 z-10 bg-terminal-bg border-b border-terminal-border">
        <tr>
          <th className={`${thCls} w-8 text-center`}>#</th>
          <th className={thCls} onClick={() => onSort("ticker")}>
            Ticker <SortIcon col="ticker" sort={sort} />
          </th>
          <th className={`${thCls} max-w-[180px]`}>Company</th>
          <th className={`${thCls} text-right`} onClick={() => onSort("dollar_volume")}>
            $ Vol <SortIcon col="dollar_volume" sort={sort} />
          </th>
          <th className={`${thCls} text-right`} onClick={() => onSort("adr_pct")}>
            ADR% <SortIcon col="adr_pct" sort={sort} />
          </th>
          <th className={`${thCls} text-right`} onClick={() => onSort("change_pct")}>
            Change% <SortIcon col="change_pct" sort={sort} />
          </th>
        </tr>
      </thead>
      <tbody>
        {visible.map((row, i) => {
          const chgPos = row.change_pct > 0;
          const chgNeg = row.change_pct < 0;
          return (
            <tr
              key={row.ticker}
              className="border-b border-gray-800/60 hover:bg-terminal-elevated/50"
            >
              <td className="px-3 py-1.5 text-center text-slate-600 tabular-nums">{i + 1}</td>
              <td className="px-3 py-1.5 font-semibold text-sky-400">{row.ticker}</td>
              <td className="max-w-[180px] truncate px-3 py-1.5 text-slate-300">
                {row.company || "—"}
              </td>
              <td className="px-3 py-1.5 text-right font-mono tabular-nums text-slate-400">
                {row.dollar_volume || "—"}
              </td>
              <td className="px-3 py-1.5 text-right font-mono tabular-nums text-slate-300">
                {row.adr_pct != null ? `${row.adr_pct.toFixed(1)}%` : "—"}
              </td>
              <td
                className={`px-3 py-1.5 text-right font-mono tabular-nums font-semibold
                  ${chgPos ? "text-emerald-400" : chgNeg ? "text-rose-400" : "text-slate-400"}`}
              >
                {fmtPct(row.change_pct)}
              </td>
            </tr>
          );
        })}
      </tbody>
      {/* Sentinel for IntersectionObserver — triggers next chunk load */}
      <tfoot>
        <tr>
          <td colSpan={6}>
            <div ref={sentinelRef} className="h-1" />
          </td>
        </tr>
      </tfoot>
    </table>
  );
}

// ── Grouped view ───────────────────────────────────────────────────────────

function GroupedView({
  groups,
  copiedMap,
  onCopy,
}: {
  groups: IndustryGroup[];
  copiedMap: Map<string, boolean>;
  onCopy: (id: string, tickers: string[]) => void;
}) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggle(name: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  }

  if (groups.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-slate-500">
        <p className="t-label">No stocks match this filter today</p>
      </div>
    );
  }

  return (
    <div>
      {/* Column header */}
      <div className="flex items-center border-b border-terminal-border px-4 py-1.5 text-[11px] font-medium text-slate-500">
        <span className="flex-1">Industry Group</span>
        <span>Count</span>
      </div>

      {groups.map((group) => {
        const isOpen = expanded.has(group.group_name);
        return (
          <div key={group.group_name} className="border-t border-terminal-border">
            {/* Parent row */}
            <div
              role="button"
              aria-expanded={isOpen}
              onClick={() => toggle(group.group_name)}
              className="group flex cursor-pointer items-center gap-2 bg-terminal-elevated/20
                px-4 py-2 hover:bg-terminal-elevated/40"
            >
              <span className="text-slate-400 shrink-0">
                {isOpen
                  ? <ChevronDown className="h-4 w-4" />
                  : <ChevronRight className="h-4 w-4" />}
              </span>
              <span className="flex-1 text-sm font-semibold text-slate-100">
                {group.group_name}
              </span>
              <span className="text-xs font-semibold text-slate-300 tabular-nums">
                {group.count}
              </span>
              {group.avg_change_pct != null && (
                <span
                  className={`text-xs font-mono tabular-nums ${
                    group.avg_change_pct > 0
                      ? "text-emerald-400"
                      : group.avg_change_pct < 0
                        ? "text-rose-400"
                        : "text-slate-400"
                  }`}
                >
                  {fmtPct(group.avg_change_pct)}
                </span>
              )}
              {/* Copy all — hover-only */}
              <span className="opacity-0 transition-opacity group-hover:opacity-100">
                <CopyIconButton
                  tickers={group.tickers}
                  id={`parent:${group.group_name}`}
                  copiedMap={copiedMap}
                  onCopy={onCopy}
                />
              </span>
            </div>

            {/* Child rows */}
            {isOpen && (
              <div className="ml-4 border-l border-slate-700">
                {group.sub_industries.map((sub) => (
                  <div
                    key={`${sub.industry}|${sub.thematic_label}`}
                    className="flex items-center gap-2 py-1.5 pl-4 pr-4
                      text-sm hover:bg-terminal-elevated/30"
                  >
                    <span className="flex-1 text-slate-300">
                      {sub.industry}
                      {sub.thematic_label && (
                        <span className="ml-2 text-xs text-slate-500">
                          · {sub.thematic_label}
                        </span>
                      )}
                    </span>
                    <span className="text-xs tabular-nums text-slate-400">
                      {sub.count}
                    </span>
                    {/* Copy sub-industry — always visible */}
                    <CopyIconButton
                      tickers={sub.tickers}
                      id={`sub:${sub.industry}`}
                      copiedMap={copiedMap}
                      onCopy={onCopy}
                    />
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ── Main modal ─────────────────────────────────────────────────────────────

export default function StockListModal({
  open,
  onClose,
  filter,
  filterLabel,
  minCapB = 1.0,
}: StockListModalProps) {
  const [view, setView] = useState<"list" | "grouped">("list");
  const [stockRows, setStockRows] = useState<StockRow[]>([]);
  const [groups, setGroups] = useState<IndustryGroup[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sort, setSort] = useState<SortState>(() => defaultSort(filter));

  // Header copy-all state
  const [headerCopied, setHeaderCopied] = useState(false);
  const headerCopyTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Per-item copied state: Map<id, boolean>
  const [copiedMap, setCopiedMap] = useState<Map<string, boolean>>(new Map());
  const copiedTimers = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());

  const abortRef = useRef<AbortController | null>(null);

  // Sorted rows memo-equivalent
  const [sortedRows, setSortedRows] = useState<StockRow[]>([]);
  useEffect(() => {
    setSortedRows(sortRows(stockRows, sort));
  }, [stockRows, sort]);

  // Reset sort when filter changes
  useEffect(() => {
    setSort(defaultSort(filter));
  }, [filter]);

  // Fetch on open
  const fetchData = useCallback(async () => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    setError(null);

    const qs = `filter=${encodeURIComponent(filter)}&min_cap_b=${minCapB}`;
    try {
      const [listRes, groupedRes] = await Promise.all([
        fetch(`${API_BASE_URL}/api/market-breadth/stocks?${qs}`, { signal: ctrl.signal }),
        fetch(`${API_BASE_URL}/api/market-breadth/stocks/grouped?${qs}`, { signal: ctrl.signal }),
      ]);
      const [listBody, groupedBody]: [_StocksResponse, _GroupedResponse] = await Promise.all([
        listRes.json() as Promise<_StocksResponse>,
        groupedRes.json() as Promise<_GroupedResponse>,
      ]);

      if (!listBody.ok && !groupedBody.ok) {
        setError(listBody.detail ?? groupedBody.detail ?? "Failed to load stocks.");
        return;
      }
      if (listBody.ok) {
        setStockRows(listBody.stocks ?? []);
        setTotalCount(listBody.count ?? 0);
      }
      if (groupedBody.ok) {
        setGroups(groupedBody.groups ?? []);
        if (!listBody.ok) setTotalCount(groupedBody.total_count ?? 0);
      }
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return;
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [filter, minCapB]);

  useEffect(() => {
    if (!open) return;
    void fetchData();
    return () => { abortRef.current?.abort(); };
  }, [open, fetchData]);

  // Body scroll lock
  useEffect(() => {
    if (open) {
      document.body.classList.add("overflow-hidden");
    } else {
      document.body.classList.remove("overflow-hidden");
    }
    return () => { document.body.classList.remove("overflow-hidden"); };
  }, [open]);

  // Escape key
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  // Per-item copy with tooltip
  const handleItemCopy = useCallback((id: string, tickers: string[]) => {
    void copyTickers(tickers).then(() => {
      setCopiedMap((prev) => new Map(prev).set(id, true));
      const existing = copiedTimers.current.get(id);
      if (existing) clearTimeout(existing);
      const t = setTimeout(() => {
        setCopiedMap((prev) => {
          const next = new Map(prev);
          next.delete(id);
          return next;
        });
        copiedTimers.current.delete(id);
      }, 1500);
      copiedTimers.current.set(id, t);
    });
  }, []);

  // Header copy-all
  const handleHeaderCopy = useCallback(() => {
    const tickers =
      view === "list"
        ? stockRows.map((s) => s.ticker)
        : groups.flatMap((g) => g.tickers);
    void copyTickers(tickers).then(() => {
      setHeaderCopied(true);
      if (headerCopyTimer.current) clearTimeout(headerCopyTimer.current);
      headerCopyTimer.current = setTimeout(() => setHeaderCopied(false), 1500);
    });
  }, [view, stockRows, groups]);

  // Sort toggle
  const handleSort = useCallback((key: SortKey) => {
    setSort((prev) =>
      prev.key === key
        ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key, dir: key === "change_pct" ? (DN_FILTERS.has(filter) ? "asc" : "desc") : "desc" },
    );
  }, [filter]);

  // Cleanup timers on unmount
  useEffect(() => {
    return () => {
      if (headerCopyTimer.current) clearTimeout(headerCopyTimer.current);
      copiedTimers.current.forEach((t) => clearTimeout(t));
    };
  }, []);

  if (!open) return null;

  return (
    /* Overlay */
    <div
      className="fixed inset-0 z-50 bg-black/60"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      {/* Panel */}
      <div
        className="fixed left-1/2 top-1/2 flex max-h-[85vh] w-[min(92vw,700px)]
          -translate-x-1/2 -translate-y-1/2 flex-col rounded-xl border
          border-terminal-border bg-terminal-bg shadow-2xl"
      >
        {/* ── Header ── */}
        <div className="flex shrink-0 items-center gap-3 border-b border-terminal-border px-4 py-3">
          <div className="flex-1 min-w-0">
            <p className="truncate text-sm font-semibold text-slate-100">{filterLabel}</p>
            {!loading && (
              <p className="text-[11px] text-slate-500">
                {totalCount} stocks · min ${minCapB}B mkt cap
              </p>
            )}
          </div>

          {/* Group toggle */}
          <button
            onClick={() => setView((v) => (v === "list" ? "grouped" : "list"))}
            title={view === "list" ? "Switch to grouped view" : "Switch to list view"}
            className={`flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs transition-colors
              ${view === "grouped"
                ? "border-accent/40 bg-accent/10 text-accent"
                : "border-gray-700 text-slate-400 hover:border-gray-600 hover:text-slate-200"}`}
          >
            <ChevronDown className="h-3.5 w-3.5" />
            Group
          </button>

          {/* Copy all */}
          <button
            onClick={handleHeaderCopy}
            title="Copy all tickers"
            className={`flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs transition-colors
              ${headerCopied
                ? "border-emerald-500/40 text-emerald-400"
                : "border-gray-700 text-slate-400 hover:border-gray-600 hover:text-slate-200"}`}
          >
            <Clipboard className="h-3.5 w-3.5" />
            {headerCopied ? "Copied!" : "Copy"}
          </button>

          {/* Close */}
          <button
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-400 hover:bg-slate-700 hover:text-slate-200 transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* ── Body ── */}
        <div className="flex-1 overflow-y-auto">
          {loading ? (
            <SkeletonRows count={3} />
          ) : error ? (
            <div className="p-4">
              <ErrorBanner
                title="Failed to load stocks"
                detail={error}
                onRetry={() => void fetchData()}
              />
            </div>
          ) : view === "list" ? (
            <StockListView rows={sortedRows} sort={sort} onSort={handleSort} />
          ) : (
            <GroupedView
              groups={groups}
              copiedMap={copiedMap}
              onCopy={handleItemCopy}
            />
          )}
        </div>
      </div>
    </div>
  );
}
