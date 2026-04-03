type Props = { count?: number; className?: string };

export function SkeletonRows({ count = 5, className = "" }: Props) {
  return (
    <div className={`animate-pulse space-y-2 p-4 ${className}`}>
      {Array.from({ length: count }).map((_, i) => (
        <div
          key={i}
          className="h-8 rounded-md bg-terminal-elevated/40"
          style={{ opacity: 1 - i * 0.1 }}
        />
      ))}
    </div>
  );
}

export function PanelLoading({ label = "Loading..." }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-10 text-[11px] text-slate-500">
      <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-slate-600 border-t-accent" />
      {label}
    </div>
  );
}
